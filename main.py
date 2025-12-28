from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, time
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv
from telethon import TelegramClient, events

# --- IMPORT CORE MODULES ---
try:
    from core.dhan_bridge import DhanBridge
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
ADMIN_ID = os.getenv('ADMIN_ID')

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


# --- MARKET HOURS ---
async def check_market_hours(client: TelegramClient, bridge: DhanBridge):
    SHUTDOWN_TIME = time(15, 30)
    logger.info('Market monitor started')

    while True:
        now = datetime.now()
        if now.time() >= SHUTDOWN_TIME:
            logger.info('Market closed. Disconnecting Telegram.')
            await client.disconnect() # pyright: ignore[reportGeneralTypeIssues]
            return
        await asyncio.sleep(30)


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
    def __init__(self, bridge: DhanBridge):
        self.bridge = bridge
        self.tm = bridge.trade_manager
        self.active_monitors: Set[str] = set()
        self.channel_state = ChannelState()
        self.batch_msgs: List[str] = []
        self.batch_dates: List[datetime] = []
        self._timer: Optional[asyncio.Task] = None

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

            ltp, status = await asyncio.to_thread(self.bridge.execute_super_order, sig)

            if status == 'SUCCESS':
                sid, _, _, _ = self.bridge.mapper.get_security_id(
                    sym, ltp, self.bridge.get_live_ltp
                )
                if sid:
                    self.active_monitors.add(sym)
                    loop.create_task(self._exit_monitor(sym, str(sid)))

    async def _exit_monitor(self, sym: str, sid: str):
        self.bridge.subscribe([{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid}])

        try:
            while True:
                await asyncio.sleep(1)
                trade = self.tm.get_trade(sid)
                if not trade:
                    break

                if self.bridge.get_order_imbalance(sid) < 0.3:
                    logger.critical(f'Liquidity dump: {sym}')
                    self.bridge.square_off_single(sid)
                    break
        finally:
            self.active_monitors.discard(sym)


# --- MAIN ---
async def main():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical('Telegram credentials missing')
        return

    bridge = DhanBridge()
    batcher = SignalBatcher(bridge)

    client = TelegramClient(SESSION_NAME, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
    await client.start()  # pyright: ignore[reportGeneralTypeIssues]
    logger.info('Telegram connected')

    loop = asyncio.get_running_loop()
    loop.create_task(check_market_hours(client, bridge))

    resolved = []
    for ch in TARGET_CHANNELS:
        resolved.append(await client.get_entity(ch))

    @client.on(events.NewMessage(chats=resolved))
    async def handler(event):
        if event.message and event.message.message:
            await batcher.add_message(event.message.message, event.message.date, event.chat_id)

    await client.run_until_disconnected()  # pyright: ignore[reportGeneralTypeIssues]


if __name__ == '__main__':
    asyncio.run(main())
