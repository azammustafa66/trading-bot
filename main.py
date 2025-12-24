from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, time
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Set

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

# System Config
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
MAX_LOG_SIZE = int(os.getenv('MAX_LOG_SIZE_MB', '50')) * 1024 * 1024
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))

# Telegram Config
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME', 'telegram_session')
ADMIN_ID = os.getenv('ADMIN_ID')
LOSS_LIMIT = float(os.getenv('LOSS_LIMIT', '8000.0'))

# Target Channels
RAW_CHANNELS = os.getenv('TARGET_CHANNELS', os.getenv('TARGET_CHANNEL', ''))
TARGET_CHANNELS = [x.strip() for x in RAW_CHANNELS.split(',') if x.strip()]

# Data Paths
SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')
BATCH_DELAY_SECONDS = 60.0

# --- SAFETY KEYWORDS ---
PAUSE_KEYWORDS = [
    'SAFE AVOID',
    'SAFE DONT TRADE',
    'NO TRADE',
    'AVOID TRADING',
    'CLOSE FOR TODAY',
    'SAFE AVOID TODAY',
    'MARKET CHOPPY',
    'TRAPPING',
    'NON DIRECTIONAL',
    'SIDEWAYS',
    'SAFE TRADERS STAY AWAY',
    'BEGINNERS AVOID',
    'ONLY PRO',
    'ONLY RISK',
    'SCALP ONLY',
]

RESUME_KEYWORDS = [
    'SAFE NOW',
    'RESUME',
    'GOOD TO GO',
    'HERO ZERO',
    'JACKPOT',
    'ROCKET',
    'RECOVERY',
    'ENTER NOW',
    'MARKET GOOD',
    'BTST',
]

# Ensure directories exist
os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)


# --- LOGGING SETUP ---
def setup_logging():
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler = RotatingFileHandler(
        'logs/trade_logs.log',
        mode='a',
        maxBytes=MAX_LOG_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8',
    )
    file_handler.setFormatter(formatter)
    error_handler = RotatingFileHandler(
        'logs/errors.log',
        mode='a',
        maxBytes=MAX_LOG_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8',
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        handlers=[file_handler, error_handler, console_handler],
    )


setup_logging()
logger = logging.getLogger('LiveListener')


# --- EXCEPTION HANDLING ---
def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical('Uncaught exception:', exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_uncaught_exception


def handle_shutdown_signal(signum, frame):
    logger.info('Shutting down gracefully...')
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# --- BACKGROUND TASKS ---
async def check_market_hours(client: TelegramClient, bridge: DhanBridge):
    SHUTDOWN_TIME = time(15, 30)
    logger.info('Market Monitor Started (Fri SqOff: 15:18 | Stop: 15:30)')

    while True:
        now = datetime.now()
        current_time = now.time()

        if now.weekday() == 4 and current_time.hour == 15 and current_time.minute == 18:
            logger.warning('Friday 3:18 PM: Triggering Auto-Square Off.')
            bridge.square_off_all()
            if ADMIN_ID:
                await client.send_message(int(ADMIN_ID), '**Friday Square-Off Executed**')
            await asyncio.sleep(65)

        if current_time >= SHUTDOWN_TIME:
            logger.info('Market Closed (3:30 PM). Shutting down...')
            sys.exit(0)

        await asyncio.sleep(60)


# --- TELEGRAM HELPER FUNCTIONS ---
async def resolve_channel(client: TelegramClient, target: str):
    if str(target).lstrip('-').isdigit():
        try:
            return await client.get_entity(int(target))
        except ValueError:
            pass
    try:
        return await client.get_entity(target)
    except ValueError:
        pass

    async for d in client.iter_dialogs(limit=500):
        title = getattr(d.entity, 'title', '')
        if title and title.lower() == target.lower():
            return d.entity
    raise ValueError(f'Could not resolve channel: {target}')


class ChannelState:
    def __init__(self):
        self._paused_until: Dict[int, datetime] = {}

    def pause(self, channel_id: int):
        now = datetime.now()
        self._paused_until[channel_id] = now.replace(hour=23, minute=59, second=59)
        logger.warning(f'⛔ Channel {channel_id} PAUSED until End of Day')

    def resume(self, channel_id: int):
        if channel_id in self._paused_until:
            del self._paused_until[channel_id]
            logger.info(f'✅ Channel {channel_id} RESUMED manually.')

    def is_paused(self, channel_id: int) -> bool:
        if channel_id not in self._paused_until:
            return False
        if datetime.now() > self._paused_until[channel_id]:
            del self._paused_until[channel_id]
            return False
        return True


class SignalBatcher:
    def __init__(self, bridge_instance: DhanBridge):
        self.batch_messages: List[str] = []
        self.batch_dates: List[datetime] = []
        self._timer_task: Optional[asyncio.Task] = None
        self.bridge = bridge_instance
        self.active_monitors: Set[str] = set()
        self.channel_state = ChannelState()

    def kill_switch_hit(self) -> bool:
        realized = self.bridge.get_realized_pnl()
        if realized <= -LOSS_LIMIT:
            logger.critical(f'KILL SWITCH: Realized Loss {realized} hits limit {LOSS_LIMIT}')
            self.bridge.check_kill_switch(LOSS_LIMIT)
            return True
        return False

    async def add_message(self, text: str, dt: datetime, channel_id: int):
        clean_text = text.upper()
        if any(k in clean_text for k in RESUME_KEYWORDS):
            self.channel_state.resume(channel_id)
        if any(k in clean_text for k in PAUSE_KEYWORDS):
            self.channel_state.pause(channel_id)
            return
        if self.channel_state.is_paused(channel_id):
            return

        self.batch_messages.append(text)
        self.batch_dates.append(dt)

        if self._timer_task:
            self._timer_task.cancel()
        self._timer_task = asyncio.create_task(self._process_after_delay())

    async def _retry_monitor(self, res: Dict[str, Any], reason: str, ltp: float):
        symbol = res.get('trading_symbol', '')
        entry = res.get('trigger_above', 0.0)
        POLL_INTERVAL = 60
        MAX_RETRIES = 15

        try:
            self.active_monitors.add(symbol)
            logger.info(f'Monitor Started: {symbol} | Reason: {reason} | Entry: {entry}')

            for attempt in range(1, MAX_RETRIES + 1):
                if self.bridge.kill_switch_triggered:
                    return

                await asyncio.sleep(POLL_INTERVAL)

                # --- SAFETY CHECK: Verify Position Before Polling ---
                sec_id, _, _, _ = self.bridge.mapper.get_security_id(symbol)
                if sec_id and self.bridge.has_open_position(sec_id):
                    logger.warning(f'🛑 Monitor Stopped: Position for {symbol} detected!')
                    self.active_monitors.discard(symbol)
                    return
                # ----------------------------------------------------

                logger.info(f'Polling {symbol} ({attempt}/{MAX_RETRIES})  LTP {ltp}...')

                status = await asyncio.to_thread(self.bridge.execute_super_order, res)

                if status == 'SUCCESS':
                    logger.info(f'Trigger Hit! {symbol} Executed.')
                    self.active_monitors.discard(symbol)
                    return
                elif status == 'ERROR':
                    return

            logger.warning(f'Monitor Timed Out (15m): {symbol}. Signal Expired.')

        finally:
            self.active_monitors.discard(symbol)

    async def _process_after_delay(self):
        try:
            await asyncio.sleep(BATCH_DELAY_SECONDS)
            if self.kill_switch_hit():
                return

            logger.info(f'Processing batch ({len(self.batch_messages)} msgs)...')

            try:
                results = process_and_save(
                    self.batch_messages,
                    self.batch_dates,
                    jsonl_path=SIGNALS_JSONL,
                    json_path=SIGNALS_JSON,
                )
            except Exception as e:
                logger.error(f'Parsing Error: {e}', exc_info=True)
                results = []

            if results:
                logger.info(f'Found {len(results)} valid signals')
                for idx, res in enumerate(results, 1):
                    symbol = res.get('trading_symbol')
                    if symbol in self.active_monitors:
                        logger.warning(f'Duplicate: {symbol} is already being monitored.')
                        continue

                    try:
                        ltp, status = await asyncio.to_thread(self.bridge.execute_super_order, res)
                        if status in ['PRICE_HIGH', 'PRICE_LOW']:
                            asyncio.create_task(self._retry_monitor(res, status, ltp))
                    except Exception as e:
                        logger.error(f'Execution Failed: {e}', exc_info=True)
            else:
                logger.info('No actionable signals in batch.')

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f'Batch Logic Error: {e}', exc_info=True)
        finally:
            self.batch_messages = []
            self.batch_dates = []
            self._timer_task = None


# --- MAIN ENTRY POINT ---
async def main():
    logger.info('=' * 60)
    logger.info('Trading Bot Starting')
    logger.info('=' * 60)

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TARGET_CHANNELS:
        logger.critical('Missing Environment Variables')
        return

    bridge = DhanBridge()
    batcher = SignalBatcher(bridge)

    try:
        client = TelegramClient(SESSION_NAME, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
        await client.start() # pyright: ignore[reportGeneralTypeIssues]
        logger.info('Telegram Connected')
        asyncio.create_task(check_market_hours(client, bridge))
    except Exception as e:
        logger.critical(f'Connection Failed: {e}')
        return

    resolved_chats = []
    for target in TARGET_CHANNELS:
        try:
            resolved_chats.append(await resolve_channel(client, target))
        except Exception as e:
            logger.error(f"Failed to resolve '{target}': {e}")

    if not resolved_chats:
        logger.critical('No valid channels found. Exiting.')
        return

    if ADMIN_ID:

        @client.on(events.NewMessage(from_users=[int(ADMIN_ID)]))
        async def admin_handler(event: events.NewMessage.Event):
            text = event.raw_text.lower().strip()
            if text == '/status':
                await event.reply(f'Funds: {bridge.get_funds()}\nP&L: {bridge.get_realized_pnl()}')
            elif text == '/force_sqoff':
                bridge.square_off_all()
                await event.reply('Triggered Square-off')

    @client.on(events.NewMessage(chats=resolved_chats))
    async def handler(event: events.NewMessage.Event):
        if event.message.message and event.chat_id:
            await batcher.add_message(event.message.message, event.message.date, event.chat_id)
            logger.info('Message Received')

    await client.run_until_disconnected() # pyright: ignore[reportGeneralTypeIssues]


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f'Fatal Error: {e}', exc_info=True)
