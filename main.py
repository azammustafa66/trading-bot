import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, time
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from telethon import TelegramClient, events

load_dotenv()

# ... (Logging setup remains the same) ...
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
MAX_LOG_SIZE = int(os.getenv('MAX_LOG_SIZE_MB', '50')) * 1024 * 1024
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))

os.makedirs('logs', exist_ok=True)

file_handler = RotatingFileHandler(
    'logs/trade_logs.log',
    mode='a',
    maxBytes=MAX_LOG_SIZE,
    backupCount=LOG_BACKUP_COUNT,
    encoding='utf-8',
)
file_handler.setFormatter(
    logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
)

error_handler = RotatingFileHandler(
    'logs/errors.log',
    mode='a',
    maxBytes=MAX_LOG_SIZE,
    backupCount=LOG_BACKUP_COUNT,
    encoding='utf-8',
)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(
    logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] [%(filename)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(
    logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'
    )
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[file_handler, error_handler, console_handler],
)

logger = logging.getLogger('LiveListener')


def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical(
        'Uncaught exception:', exc_info=(exc_type, exc_value, exc_traceback)
    )


sys.excepthook = handle_uncaught_exception

try:
    from core.dhan_bridge import DhanBridge
    from core.signal_parser import process_and_save
except ImportError as e:
    logger.critical(
        f'Import Error: {e}. Ensure you are running from the root directory.'
    )
    sys.exit(1)

# --- CONFIGURATION ---
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME', 'telegram_session')

RAW_CHANNELS = os.getenv('TARGET_CHANNELS', os.getenv('TARGET_CHANNEL', ''))
TARGET_CHANNELS = [x.strip() for x in RAW_CHANNELS.split(',') if x.strip()]

SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')
BATCH_DELAY_SECONDS = 5

os.makedirs('data', exist_ok=True)


# --- SHUTDOWN HANDLING ---
def handle_shutdown_signal(signum, frame):
    """Handle shutdown signals (SIGTERM, SIGINT) with proper logging and clean exit"""
    sig_name = signal.Signals(signum).name
    logger.info('=' * 60)
    logger.info(f'Received {sig_name} - Shutting down gracefully...')
    logger.info(f'Stopped at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('=' * 60)
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# --- NEW: MARKET HOURS MONITOR ---
async def check_market_hours(client: TelegramClient):
    """Checks every minute if market is closed (3:30 PM IST)."""
    logger.info('⏰ Market Hours Monitor Started (Auto-Stop at 15:30)')

    stop_time = time(15, 30)  # 3:30 PM

    while True:
        now = datetime.now()

        # If current time is past 3:30 PM
        if now.time() >= stop_time:
            logger.info('🛑 Market Closed (3:30 PM). Stopping Bot...')
            await client.disconnect()  # pyright: ignore[reportGeneralTypeIssues]
            sys.exit(0)  # Exit with success code

        # Wait 60 seconds before checking again
        await asyncio.sleep(60)


# --- HELPER FUNCTIONS ---
async def resolve_channel(client: TelegramClient, target: str):
    """
    Robust channel resolution:
    1. Numeric ID (e.g., -1001234567890)
    2. Username (e.g., @channelname)
    3. Search by exact title match (case-insensitive)
    """
    logger.info(f'🔍 Resolving channel: {target}')

    # 1) Try numeric ID
    if str(target).lstrip('-').isdigit():
        try:
            entity = await client.get_entity(int(target))
            logger.info(f'Resolved by ID: {getattr(entity, "title", target)}')
            return entity
        except Exception as e:
            logger.debug(f'Failed to resolve by ID: {e}')

    # 2) Try username / raw get_entity (handles @username)
    try:
        entity = await client.get_entity(target)
        title = getattr(entity, 'title', getattr(entity, 'username', target))
        logger.info(f'Resolved by username/entity: {title}')
        return entity
    except Exception as e:
        logger.debug(f'Failed to resolve by username: {e}')

    # 3) Search by title exact match (case-insensitive)
    logger.info(f"🔍 Searching dialogs for title: '{target}'...")
    async for d in client.iter_dialogs(limit=500):
        title = getattr(d.entity, 'title', '')
        if title and title.lower() == target.lower():
            logger.info(f'Found by title: {title} (ID: {d.entity.id})')
            return d.entity

    raise ValueError(f'Could not resolve channel: {target}')


class SignalBatcher:
    def __init__(self, bridge_instance: DhanBridge):
        self.batch_messages = []
        self.batch_dates = []
        self._timer_task = None
        self.bridge = bridge_instance

    async def add_message(self, text: str, dt: datetime):
        self.batch_messages.append(text)
        self.batch_dates.append(dt)

        if self._timer_task:
            self._timer_task.cancel()

        self._timer_task = asyncio.create_task(self._process_after_delay())

    async def _process_after_delay(self):
        try:
            await asyncio.sleep(BATCH_DELAY_SECONDS)

            logger.info(
                f'⚡ Processing batch of {len(self.batch_messages)} messages...'
            )

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Batch content: {self.batch_messages}')

            # 1. PARSE & SAVE
            try:
                results = process_and_save(
                    self.batch_messages,
                    self.batch_dates,
                    jsonl_path=SIGNALS_JSONL,
                    json_path=SIGNALS_JSON,
                )
                logger.debug(
                    f'Parser returned {len(results) if results else 0} signals'
                )
            except Exception as e:
                logger.error(f'❌ Signal parsing failed: {e}', exc_info=True)
                results = []

            # 2. EXECUTE TRADES
            if results:
                logger.info(f'Found {len(results)} valid signal(s)')
                for idx, res in enumerate(results, 1):
                    logger.info(
                        f'Signal {idx}/{len(results)}: {res["trading_symbol"]} | '
                        f'{res["action"]} | Entry: {res.get("trigger_above", "N/A")} | '
                        f'SL: {res.get("stop_loss", "N/A")}'
                    )

                    try:
                        self.bridge.execute_super_order(res)
                    except Exception as e:
                        logger.error(
                            f'Order execution failed for {res["trading_symbol"]}: {e}',
                            exc_info=True,
                        )
            else:
                logger.info('No valid signals found in batch.')

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f'Batch processing error: {e}', exc_info=True)
        finally:
            current_task = asyncio.current_task()
            if current_task and not current_task.cancelled():
                self.batch_messages = []
                self.batch_dates = []
                self._timer_task = None
                logger.debug('Batch buffer cleared')


async def main():
    logger.info('=' * 60)
    logger.info('Trading Bot Starting (Multi-Channel Support)...')
    logger.info('=' * 60)

    # 1. Validation
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical('Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env')
        return
    if not TARGET_CHANNELS:
        logger.critical('Missing TARGET_CHANNELS in .env')
        return

    logger.info('Configuration loaded')
    logger.info(f'   - Channels Target: {len(TARGET_CHANNELS)}')
    logger.info(f'   - Log Level: {LOG_LEVEL}')

    # 2. Initialize Bridge
    logger.info('Initializing Dhan Bridge...')
    try:
        bridge = DhanBridge()
        logger.info('Dhan Bridge initialized')
    except Exception as e:
        logger.critical(f'Failed to initialize Dhan Bridge: {e}', exc_info=True)
        return

    # 3. Initialize Batcher
    batcher = SignalBatcher(bridge)

    # 4. Start Telegram Client
    logger.info('Connecting to Telegram...')
    try:
        client = TelegramClient(
            session=SESSION_NAME,
            api_id=int(TELEGRAM_API_ID),
            api_hash=TELEGRAM_API_HASH,
        )
        await client.start()  # pyright: ignore
        logger.info('✅ Connected to Telegram')

        # --- NEW: START MARKET HOURS CHECKER ---
        asyncio.create_task(check_market_hours(client))

    except Exception as e:
        logger.critical(f'Failed to connect to Telegram: {e}', exc_info=True)
        return

    # 5. Resolve ALL target channels
    resolved_chats = []
    logger.info('Resolving target channels...')

    for target in TARGET_CHANNELS:
        try:
            entity = await resolve_channel(client, target)
            resolved_chats.append(entity)
            logger.info(
                f'Added listener for: {getattr(entity, "title", target)}'
            )
        except Exception as e:
            logger.error(f"   Failed to resolve '{target}': {e}")
            logger.error('    Check permissions or channel name spelling.')

    if not resolved_chats:
        logger.critical('No channels could be resolved. Exiting.')
        return

    logger.info('=' * 60)
    logger.info(f'Listening to {len(resolved_chats)} channel(s)')
    logger.info(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('=' * 60)

    # 6. Event Loop (Listening to ALL resolved chats)
    @client.on(events.NewMessage(chats=resolved_chats))
    async def handler(event):
        try:
            text = event.message.message
            if not text:
                return

            chat_title = 'Unknown'
            try:
                chat = await event.get_chat()
                chat_title = getattr(
                    chat, 'title', getattr(chat, 'username', 'Unknown')
                )
            except Exception as e:
                pass

            # Preview
            preview = text.replace('\n', ' ')[:50]
            logger.info(f'[{chat_title}] Received: {preview}...')

            await batcher.add_message(text, event.message.date)

        except Exception as e:
            logger.error(f'Handler Error: {e}', exc_info=True)

    try:
        await client.run_until_disconnected()  # pyright: ignore
    except Exception as e:
        logger.critical(f'Client disconnected: {e}', exc_info=True)
        raise


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('=' * 60)
        logger.info('Bot Stopped (Keyboard Interrupt)')
        logger.info(
            f'Stopped at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        )
        logger.info('=' * 60)
    except SystemExit:
        pass  # Clean exit for sys.exit(0)
    except Exception as e:
        logger.critical(f'Critical Crash: {e}', exc_info=True)
        sys.exit(1)
