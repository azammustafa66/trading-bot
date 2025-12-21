from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, time, timedelta
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
BATCH_DELAY_SECONDS = 2.0

# --- SAFETY KEYWORDS ---
PAUSE_KEYWORDS = [
    'SAFE AVOID',
    'SAFE DONT TRADE',
    'NO TRADE',
    'AVOID TRADING',
    'CLOSE FOR TODAY',
    'SAFE AVOID TODAY',
    'SAFE TRADERS AVOID TODAY',
    'MARKET CHOPPY',
    'TRAPPING',
    'NON DIRECTIONAL',
    'SIDEWAYS',
    'SAFE TRADERS STAY AWAY',
    'BEGINNERS AVOID',
    'BEGINNER AVOID',
    'ONLY PRO',
    'ONLY RISK',
    'RISK TRADERS ONLY',
    'SCALP ONLY',
    'SCALP NOT TRADE',
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


# --- LOGGING SETUP (Restored) ---
def setup_logging():
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Main Trade Log
    file_handler = RotatingFileHandler(
        'logs/trade_logs.log',
        mode='a',
        maxBytes=MAX_LOG_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8',
    )
    file_handler.setFormatter(formatter)

    # 2. Error Log
    error_handler = RotatingFileHandler(
        'logs/errors.log',
        mode='a',
        maxBytes=MAX_LOG_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8',
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # 3. Console Output (Essential for Systemd/Journalctl)
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
    sig_name = signal.Signals(signum).name
    logger.info('=' * 60)
    logger.info(f'Received {sig_name} - Shutting down gracefully...')
    logger.info('=' * 60)
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# --- BACKGROUND TASKS (Fixed with Sleep Logic) ---
async def check_market_hours(client: TelegramClient, bridge: DhanBridge):
    """
    Monitors time:
    - 3:18 PM Friday: Auto Square-off
    - 3:30 PM Daily: Sleeps until 9:00 AM next day (Prevents Systemd Restart Loops)
    """
    SHUTDOWN_TIME = time(15, 30)
    START_TIME = time(9, 0)

    logger.info('Market Monitor Started (Fri SqOff: 15:18 | Sleep: 15:30)')

    while True:
        now = datetime.now()
        current_time = now.time()

        # 1. Friday Auto Square-off
        if now.weekday() == 4:  # Friday
            if current_time.hour == 15 and current_time.minute == 18:
                logger.warning('Friday 3:18 PM: Triggering Auto-Square Off.')
                try:
                    bridge.square_off_all()
                    if ADMIN_ID:
                        await client.send_message(int(ADMIN_ID), '**Friday Square-Off Executed**')
                except Exception as e:
                    logger.error(f'Square Off Failed: {e}')
                await asyncio.sleep(65)

        # 2. Overnight Sleep Logic
        if current_time >= SHUTDOWN_TIME or current_time < START_TIME:
            target_time = datetime.combine(now.date(), START_TIME)
            if current_time >= SHUTDOWN_TIME:
                target_time += timedelta(days=1)

            sleep_seconds = (target_time - now).total_seconds()
            logger.info(
                f'Market Closed. Sleeping for {sleep_seconds / 3600:.2f} hours until {target_time}'
            )

            while sleep_seconds > 0:
                await asyncio.sleep(min(300, sleep_seconds))
                sleep_seconds -= 300
            logger.info('Good Morning! Resuming Market Monitoring...')

        else:
            await asyncio.sleep(60)


# --- TELEGRAM HELPER FUNCTIONS ---
async def resolve_channel(client: TelegramClient, target: str):
    logger.info(f'Resolving channel: {target}')

    if str(target).lstrip('-').isdigit():
        try:
            entity = await client.get_entity(int(target))
            logger.info(f'Resolved by ID: {getattr(entity, "title", target)}')
            return entity
        except ValueError:
            pass

    try:
        entity = await client.get_entity(target)
        title = getattr(entity, 'title', getattr(entity, 'username', target))
        logger.info(f'Resolved by username: {title}')
        return entity
    except ValueError:
        pass

    logger.info(f"Searching dialogs for title: '{target}'...")
    async for d in client.iter_dialogs(limit=500):
        title = getattr(d.entity, 'title', '')
        if title and title.lower() == target.lower():
            logger.info(f'Found by title: {title} (ID: {d.entity.id})')
            return d.entity

    raise ValueError(f'Could not resolve channel: {target}')


class ChannelState:
    def __init__(self):
        self._paused_until: Dict[int, datetime] = {}

    def pause(self, channel_id: int):
        now = datetime.now()
        end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        self._paused_until[channel_id] = end_of_day
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

    # --- SAFETY CHECK ---
    def kill_switch_hit(self) -> bool:
        """Layer 1 Safety Check before parsing."""
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
            logger.info(f'⚠️ Pause Keyword Detected in Channel {channel_id}')
            self.channel_state.pause(channel_id)
            return

        if self.channel_state.is_paused(channel_id):
            return

        self.batch_messages.append(text)
        self.batch_dates.append(dt)

        if self._timer_task:
            self._timer_task.cancel()

        self._timer_task = asyncio.create_task(self._process_after_delay())

    async def _retry_monitor(self, res: Dict[str, Any], reason: str):        
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
                logger.info(f'Polling {symbol} ({attempt}/{MAX_RETRIES})...')

                status = await asyncio.to_thread(self.bridge.execute_super_order, res)

                if status == 'SUCCESS':
                    logger.info(f'Trigger Hit! {symbol} Executed.')
                    return
                elif status == 'ERROR':
                    return

            logger.warning(f'Monitor Timed Out (15m): {symbol}. Signal Expired.')

        finally:
            self.active_monitors.discard(symbol)

    async def _process_after_delay(self):
        try:
            await asyncio.sleep(BATCH_DELAY_SECONDS)

            # Check Kill Switch before processing
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
                    action = res.get('action')
                    entry = res.get('trigger_above', 'N/A')

                    logger.info(
                        f'Signal {idx}/{len(results)}: {symbol} | {action} | Entry: {entry}'
                    )

                    if symbol in self.active_monitors:
                        logger.warning(f'Duplicate: {symbol} is already being monitored.')
                        continue

                    try:
                        status = await asyncio.to_thread(self.bridge.execute_super_order, res)
                        if status in ['PRICE_HIGH', 'PRICE_LOW']:
                            asyncio.create_task(self._retry_monitor(res, status))

                    except Exception as e:
                        logger.error(f'Execution Failed: {e}', exc_info=True)
            else:
                logger.info('No actionable signals in batch.')

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f'Batch Logic Error: {e}', exc_info=True)
        finally:
            current_task = asyncio.current_task()
            if current_task and not current_task.cancelled():
                self.batch_messages = []
                self.batch_dates = []
                self._timer_task = None


# --- MAIN ENTRY POINT ---
async def main():
    logger.info('=' * 60)
    logger.info('Trading Bot Starting')
    logger.info(f'Mode: {"Production" if not __debug__ else "Debug"}')
    logger.info('=' * 60)

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical('Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env')
        return
    if not TARGET_CHANNELS:
        logger.critical('Missing TARGET_CHANNELS in .env')
        return

    logger.info('Initializing Dhan Bridge...')
    try:
        bridge = DhanBridge()
        logger.info('Bridge Initialized Successfully')
    except Exception as e:
        logger.critical(f'Bridge Initialization Failed: {e}', exc_info=True)
        return

    batcher = SignalBatcher(bridge)

    logger.info('Connecting to Telegram...')
    try:
        client = TelegramClient(
            session=SESSION_NAME,
            api_id=int(TELEGRAM_API_ID),
            api_hash=TELEGRAM_API_HASH,
        )
        await client.start()  # pyright: ignore[reportGeneralTypeIssues]
        logger.info('Telegram Connected')

        # Start Market Monitor (with Sleep logic)
        asyncio.create_task(check_market_hours(client, bridge))

    except Exception as e:
        logger.critical(f'Telegram Connection Failed: {e}', exc_info=True)
        return

    resolved_chats = []
    logger.info('Resolving Target Channels...')

    for target in TARGET_CHANNELS:
        try:
            entity = await resolve_channel(client, target)
            resolved_chats.append(entity)
            logger.info(f'Listening to: {getattr(entity, "title", target)}')
        except Exception as e:
            logger.error(f"Failed to resolve '{target}': {e}")

    if not resolved_chats:
        logger.critical('No valid channels found. Exiting.')
        return

    # Admin Commands
    if ADMIN_ID:
        try:
            admin_id_int = int(ADMIN_ID)
            logger.info(f'Admin Commands Enabled for: {admin_id_int}')

            @client.on(events.NewMessage(from_users=[admin_id_int]))
            async def admin_handler(event: events.NewMessage.Event):
                text = event.raw_text.lower().strip()

                if text == '/status':
                    funds = bridge.get_funds()
                    pnl = bridge.get_realized_pnl()
                    await event.reply(
                        f'**Bot Status**\n'
                        f'Funds: {funds:.2f}\n'
                        f'Day P&L: {pnl:.2f}\n'
                        f'Loss Limit: {LOSS_LIMIT}\n'
                        f'Time: {datetime.now().strftime("%H:%M:%S")}'
                    )
                elif text == '/logs':
                    await event.reply('Uploading logs...')
                    files = [
                        f for f in ['logs/trade_logs.log', 'logs/errors.log'] if os.path.exists(f)
                    ]
                    await event.reply(file=files) if files else await event.reply('No logs found.')
                elif text == '/force_sqoff':
                    await event.reply('⚠️ Force Square-off Triggered!')
                    bridge.square_off_all()

        except ValueError:
            logger.error('ADMIN_ID is not a valid integer.')

    logger.info('=' * 60)
    logger.info(f'Active Listeners: {len(resolved_chats)}')
    logger.info('=' * 60)

    @client.on(events.NewMessage(chats=resolved_chats))
    async def handler(event: events.NewMessage.Event):
        try:
            text = event.message.message
            if text:
                if event.chat_id:
                    await batcher.add_message(text, event.message.date, event.chat_id)
                    logger.info(f'Received message ({len(text)} chars)')
        except Exception as e:
            logger.error(f'Handler Error: {e}', exc_info=True)

    try:
        await client.run_until_disconnected()  # pyright: ignore[reportGeneralTypeIssues]
    except Exception as e:
        logger.critical(f'Client Disconnected: {e}', exc_info=True)
        raise


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except asyncio.CancelledError:
        logger.info('Tasks Cancelled')
    except Exception as e:
        logger.critical(f'Fatal Error: {e}', exc_info=True)
        sys.exit(1)
