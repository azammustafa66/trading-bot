from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from dotenv import load_dotenv
from telethon import TelegramClient, events

try:
    from core.dhan_bridge import DhanBridge
    from core.signal_parser import process_and_save
except ImportError as e:
    sys.stderr.write(f'Import Error: {e}\n')
    sys.exit(1)

load_dotenv()
SESSION_NAME = os.getenv('SESSION_NAME', 'telegram_session')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('LiveListener')


class SignalBatcher:
    def __init__(self, bridge: DhanBridge):
        self.bridge = bridge
        self.tm = bridge.trade_manager
        self.active_monitors: Set[str] = set()
        self.retry_tasks: Set[asyncio.Task] = set()
        self.paused = False
        self.batch_msgs: List[str] = []
        self.batch_dates: List[datetime] = []
        self._timer: Optional[asyncio.Task] = None
        self._resume_active_trades()

    def _resume_active_trades(self):
        trades = self.tm.get_all_open_trades()
        for t in trades:
            sid = str(t['security_id'])
            sym = str(t['symbol'])
            logger.info(f'🔄 Resuming Monitor: {sym}')
            fut_id, _ = self.bridge.mapper.get_underlying_future_id(sym)
            asyncio.create_task(self._exit_monitor(sym, sid, fut_id))

    async def add_message(self, text: str, dt: datetime):
        if 'PAUSE' in text:
            self.paused = True
        if 'RESUME' in text:
            self.paused = False
        if self.paused:
            return
        self.batch_msgs.append(text)
        self.batch_dates.append(dt)
        if self._timer:
            self._timer.cancel()
        self._timer = asyncio.create_task(self._process_batch())

    async def _process_batch(self):
        await asyncio.sleep(2)
        res = process_and_save(self.batch_msgs, self.batch_dates)
        self.batch_msgs, self.batch_dates = [], []

        for sig in res:
            sym = str(sig.get('trading_symbol', ''))
            if sym in self.active_monitors:
                continue

            # Initial Attempt
            ltp, status = await asyncio.to_thread(self.bridge.execute_super_order, sig)

            if status == 'SUCCESS':
                sid, _, _, _ = self.bridge.mapper.get_security_id(sym)
                fut_id, _ = self.bridge.mapper.get_underlying_future_id(sym)
                if sid:
                    asyncio.create_task(self._exit_monitor(sym, str(sid), fut_id))

            # RETRY LOGIC (Price High/Low)
            elif status in ['PRICE_HIGH', 'PRICE_LOW']:
                logger.info(f'⏳ Price {status} for {sym}. Starting 15m Retry Monitor.')
                task = asyncio.create_task(self._retry_monitor(sig))
                self.retry_tasks.add(task)
                task.add_done_callback(self.retry_tasks.discard)

    async def _retry_monitor(self, sig: Dict[str, Any]):
        sym = str(sig.get('trading_symbol', ''))
        entry = float(sig.get('trigger_above', 0))
        sid, _, _, _ = self.bridge.mapper.get_security_id(sym)
        if not sid:
            return
        sid_str = str(sid)

        if sym in self.active_monitors:
            return

        await self.bridge.feed.subscribe([{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid_str}])
        self.active_monitors.add(sym)

        try:
            cnt = 0
            # 15 MINUTE LOOP (1800 * 0.5s = 900s)
            for _ in range(1800):
                if self.paused or self.bridge.kill_switch_triggered:
                    return
                await asyncio.sleep(0.5)

                ltp = self.bridge.get_live_ltp(sid_str)
                if ltp == 0:
                    continue

                # Check Entry Condition
                if ltp >= entry:
                    cnt += 1
                else:
                    cnt = 0

                # Trigger if price sustains for 1.5s (3 ticks)
                if cnt >= 3:
                    stat = await asyncio.to_thread(self.bridge.execute_super_order, sig)
                    if stat == 'SUCCESS':
                        fut_id, _ = self.bridge.mapper.get_underlying_future_id(sym)
                        asyncio.create_task(self._exit_monitor(sym, sid_str, fut_id))
                    return
        finally:
            self.active_monitors.discard(sym)

    async def _exit_monitor(self, sym: str, sid: str, fut_id: Optional[str] = None):
        logger.info(f'🛡️ SMART GUARD: {sym}')

        # Subscribe to Option AND Future (if available)
        subs = [{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid}]
        if fut_id:
            subs.append({'ExchangeSegment': 'NSE_FNO', 'SecurityId': fut_id})
        await self.bridge.feed.subscribe(subs)

        try:
            while True:
                await asyncio.sleep(1)

                # Retrieve fresh state from JSON
                t_data = self.tm.get_trade(sid)
                if not t_data:
                    break  # Trade closed

                is_call = t_data.get('is_call')
                is_put = t_data.get('is_put')

                # 1. OPTION PANIC (Local Depth)
                opt_imb = self.bridge.get_order_imbalance(sid)
                if opt_imb < 0.3:
                    logger.critical(f'⚠️ LIQUIDITY DUMP: {sym} (Imb {opt_imb})')
                    self.bridge.square_off_single(sid)
                    break

                # 2. FUTURES DIRECTION (Global Depth)
                if fut_id:
                    fut_imb = self.bridge.get_order_imbalance(fut_id)
                    # Crash: Exit Calls
                    if fut_imb < 0.4 and is_call:
                        logger.critical(f'📉 FUTURES CRASH ({fut_imb}): Exiting CALL {sym}')
                        self.bridge.square_off_single(sid)
                        break
                    # Rocket: Exit Puts
                    elif fut_imb > 2.5 and is_put:
                        logger.critical(f'🚀 FUTURES RALLY ({fut_imb}): Exiting PUT {sym}')
                        self.bridge.square_off_single(sid)
                        break

                if self.bridge.kill_switch_triggered:
                    break
        except Exception as e:
            logger.error(f'Exit Guard Err: {e}')


async def main():
    api_id = int(os.getenv('TELEGRAM_API_ID', 0))
    api_hash = os.getenv('TELEGRAM_API_HASH', '')
    if not api_id or not api_hash:
        logger.critical('❌ Missing TELEGRAM_API_ID')
        return

    bridge = DhanBridge()
    if bridge.feed:
        asyncio.create_task(bridge.feed.connect())
    batcher = SignalBatcher(bridge)
    client = TelegramClient(SESSION_NAME, api_id, api_hash)
    await client.start()  # type: ignore

    @client.on(events.NewMessage)  # type: ignore
    async def handler(event):
        if event.message and event.message.message:
            await batcher.add_message(event.message.message, event.message.date)

    await client.run_until_disconnected()  # type: ignore


if __name__ == '__main__':

    def signal_handler(sig, frame):
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    asyncio.run(main())
