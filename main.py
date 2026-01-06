from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Set

from dotenv import load_dotenv
from telethon import TelegramClient, events

# --- IMPORT CORE MODULES ---
try:
    from core.dhan_bridge import DhanBridge
    from core.notifier import Notifier
    from core.signal_parser import process_and_save
except ImportError as e:
    sys.stderr.write(f'Import Error: {e}. Ensure you are running from the root directory.\n')
    sys.exit(1)

# --- CONFIGURATION ---
load_dotenv()

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
MAX_LOG_SIZE = int(os.getenv('MAX_LOG_SIZE_MB', '50')) * 1024 * 1024
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))

TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME', 'telegram_session')
ADMIN_ID = int(os.getenv('ADMIN_ID', ''))

RAW_CHANNELS = os.getenv('TARGET_CHANNELS', os.getenv('TARGET_CHANNEL', ''))
TARGET_CHANNELS = [x.strip() for x in RAW_CHANNELS.split(',') if x.strip()]

SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')
BATCH_DELAY_SECONDS = 2.0

os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)


# --- LOGGING ---
def setup_logging():
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = RotatingFileHandler(
        'logs/trade.log', maxBytes=MAX_LOG_SIZE, backupCount=LOG_BACKUP_COUNT, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    root.addHandler(file_handler)
    root.addHandler(console_handler)


setup_logging()
logger = logging.getLogger('LiveListener')


# --- SIGNAL HANDLING ---
def handle_shutdown_signal(signum, frame):
    logger.info(f'Received signal {signum}. Shutting down.')
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# --- CHANNEL STATE ---
class ChannelState:
    def __init__(self):
        self._paused_until: Dict[int, datetime] = {}

    def pause(self, channel_id: int):
        self._paused_until[channel_id] = datetime.now().replace(hour=23, minute=59, second=59)

    def resume(self, channel_id: int):
        self._paused_until.pop(channel_id, None)

    def is_paused(self, channel_id: int) -> bool:
        until = self._paused_until.get(channel_id)
        if not until:
            return False
        if datetime.now() > until:
            del self._paused_until[channel_id]
            return False
        return True


# --- SIGNAL BATCHER ---
class SignalBatcher:
    def __init__(self, bridge: DhanBridge, notifier: Notifier):
        self.bridge = bridge
        self.notifier = notifier
        self.tm = bridge.trade_manager
        self.active_monitors: Set[str] = set()
        self.channel_state = ChannelState()
        self.batch_msgs: List[str] = []
        self.batch_dates: List[datetime] = []
        self._timer: Optional[asyncio.Task] = None
        self._resume_active_trades()
        self._subscribed_sids: Set[str] = set()

    def _resume_active_trades(self):
        trades = self.tm.get_all_open_trades()
        loop = asyncio.get_running_loop()

        for t in trades:
            sym = str(t['symbol'])
            sid = str(t['security_id'])
            logger.info(f'🔄 Resuming Exit Monitor: {sym}')
            self.active_monitors.add(sym)
            loop.create_task(self._exit_monitor(sym, sid))

    async def add_message(self, text: str, dt: datetime, channel_id: int):
        if self.channel_state.is_paused(channel_id):
            return

        self.batch_msgs.append(text)
        self.batch_dates.append(dt)

        if self._timer:
            self._timer.cancel()

        loop = asyncio.get_running_loop()
        self._timer = loop.create_task(self._process_batch())

    async def _process_batch(self):
        await asyncio.sleep(BATCH_DELAY_SECONDS)

        try:
            signals = process_and_save(
                self.batch_msgs, self.batch_dates, SIGNALS_JSONL, SIGNALS_JSON
            )
        except Exception as e:
            logger.error(f'Parser error: {e}')
            signals = []

        self.batch_msgs.clear()
        self.batch_dates.clear()

        loop = asyncio.get_running_loop()

        for sig in signals:
            sym = sig.get('trading_symbol')
            if not isinstance(sym, str):
                continue

            if sym in self.active_monitors:
                continue

            # 1. First Execution Attempt
            ltp, status = await asyncio.to_thread(self.bridge.execute_super_order, sig)

            # 2. Success Case
            if status == 'SUCCESS':
                await self.notifier.order_placed(sym, 0, ltp)
                sid, _, _, _ = self.bridge.mapper.get_security_id(
                    sym, ltp, self.bridge.get_live_ltp
                )
                if sid:
                    self.active_monitors.add(sym)
                    loop.create_task(self._exit_monitor(sym, str(sid)))

            # 3. RETRY LOGIC (Restored)
            # If price is too low (waiting for breakout) or too high (waiting for pullback)
            elif status in ['PRICE_LOW', 'PRICE_HIGH']:
                await self.notifier.retrying(sym, status)
                logger.info(f'⏳ Price {status} for {sym}. Starting 15m Retry Monitor.')
                self.active_monitors.add(sym)
                loop.create_task(self._retry_monitor(sig))
            elif status == 'ERROR':
                await self.notifier.order_failed(sym, 'Execution error')

    async def _retry_monitor(self, sig: Dict[str, Any]):
        sym = str(sig.get('trading_symbol', ''))
        entry = float(sig.get('trigger_above', 0))

        sid, _, _, _ = self.bridge.mapper.get_security_id(sym, entry, self.bridge.get_live_ltp)
        if not sid:
            self.active_monitors.discard(sym)
            return

        sid_str = str(sid)
        liquidity_sids = self.bridge.get_liquidity_sids(sym, sid_str)
        subs = [{'ExchangeSegment': 'NSE_FNO', 'SecurityId': s} for s in liquidity_sids]
        self.bridge.subscribe(subs)

        try:
            cnt = 0
            for _ in range(2400):
                if self.bridge.kill_switch_triggered:
                    return

                await asyncio.sleep(10.0)

                ltp = self.bridge.get_live_ltp(sid_str)
                if ltp == 0:
                    continue

                if ltp >= entry:
                    cnt += 1
                else:
                    cnt = 0

                if cnt >= 3:
                    logger.info(f'⚡ Trigger Hit: {sym} ({ltp} >= {entry}). Executing!')
                    _, status = await asyncio.to_thread(self.bridge.execute_super_order, sig)

                    if status == 'SUCCESS':
                        asyncio.get_running_loop().create_task(self._exit_monitor(sym, sid_str))
                    return
        finally:
            if not self.tm.get_trade(sid_str):
                self.active_monitors.discard(sym)

    def get_imbalance_rules(self, sym: str):
        if 'NIFTY' in sym.upper() or 'BANKNIFTY' in sym.upper():
            return {'bad_imb': 0.20, 'good_imb': 2.8, 'bad_ticks': 4}
        else:
            return {'bad_imb': 0.35, 'good_imb': 2.2, 'bad_ticks': 6}

    async def _exit_monitor(self, sym: str, sid: str):
        rules = self.get_imbalance_rules(sym)
        bad_imb = rules['bad_imb']
        good_imb = rules['good_imb']
        bad_ticks_required = rules['bad_ticks']

        bad_tick_count = 0

        await asyncio.sleep(3)

        liquidity_sids = self.bridge.get_liquidity_sids(sym, sid)
        new_subs = []
        for s in liquidity_sids:
            if s not in self._subscribed_sids:
                new_subs.append({'ExchangeSegment': 'NSE_FNO', 'SecurityId': s})
                self._subscribed_sids.add(s)

        if new_subs:
            self.bridge.subscribe(new_subs)

        try:
            while True:
                await asyncio.sleep(1)

                trade = self.tm.get_trade(sid)
                if not trade:
                    break

                imb = self.bridge.get_combined_imbalance(liquidity_sids)

                if imb >= good_imb:
                    if bad_tick_count > 0:
                        logger.info(f'💧 {sym} liquidity recovered ({imb}), resetting counter')
                    bad_tick_count = 0
                    continue

                if imb < bad_imb:
                    bad_tick_count += 1
                    logger.warning(
                        f'{sym} bad imbalance {imb:.2f} ({bad_tick_count}/{bad_ticks_required})'
                    )
                else:
                    bad_tick_count = max(0, bad_tick_count - 1)

                if bad_tick_count >= bad_ticks_required:
                    await self.notifier.squared_off(sym, f'Liquidity collapse ({imb:.2f})')
                    logger.critical(f'🧯 Liquidity collapse: {sym}')
                    self.bridge.square_off_single(sid)
                    break

        finally:
            self.active_monitors.discard(sym)


# --- MAIN ---
async def main():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical('Telegram credentials missing')
        return

    client = TelegramClient(SESSION_NAME, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
    await client.start()  # pyright: ignore[reportGeneralTypeIssues]
    logger.info('Telegram connected')

    notifier = Notifier(client, ADMIN_ID)
    bridge = DhanBridge()
    batcher = SignalBatcher(bridge, notifier)

    resolved = []
    for ch in TARGET_CHANNELS:
        try:
            resolved.append(await client.get_entity(ch))
        except Exception as e:
            logger.error(f'Failed to resolve channel {ch}: {e}')

    @client.on(events.NewMessage(chats=resolved))
    async def handler(event):
        if event.message and event.message.message:
            await batcher.add_message(event.message.message, event.message.date, event.chat_id)

    await client.run_until_disconnected()  # pyright: ignore[reportGeneralTypeIssues]


if __name__ == '__main__':
    asyncio.run(main())
