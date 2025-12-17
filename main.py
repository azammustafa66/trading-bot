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

# --- LOGGING SETUP ---
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
    logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
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
    logger.critical('Uncaught exception:', exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_uncaught_exception

try:
    from core.dhan_bridge import DhanBridge
    from core.signal_parser import process_and_save
except ImportError as e:
    logger.critical(f'Import Error: {e}. Ensure you are running from the root directory.')
    sys.exit(1)

# --- CONFIGURATION ---
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME', 'telegram_session')
ADMIN_ID = os.getenv('ADMIN_ID')
LOSS_LIMIT = float(os.getenv('LOSS_LIMIT', '8000.0'))

RAW_CHANNELS = os.getenv('TARGET_CHANNELS', os.getenv('TARGET_CHANNEL', ''))
TARGET_CHANNELS = [x.strip() for x in RAW_CHANNELS.split(',') if x.strip()]

SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')

BATCH_DELAY_SECONDS = 2.0

os.makedirs('data', exist_ok=True)


# --- SHUTDOWN HANDLING ---
def handle_shutdown_signal(signum, frame):
    sig_name = signal.Signals(signum).name
    logger.info('=' * 60)
    logger.info(f'Received {sig_name} - Shutting down gracefully...')
    logger.info(f'Stopped at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('=' * 60)
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# --- BACKGROUND TASKS ---

# async def risk_monitor_task(client: TelegramClient, bridge: DhanBridge):
#     """
#     Checks P&L every 10 seconds.
#     If triggered, Sends Message -> Disconnects -> Terminates Program (Success Code 0).
#     """
#     logger.info(f'Risk Monitor Started (Max Loss Limit: {LOSS_LIMIT})')

#     while True:
#         try:
#             # Check kill switch logic
#             killed = bridge.check_kill_switch(loss_limit=LOSS_LIMIT)

#             if killed:
#                 logger.critical('KILL SWITCH TRIGGERED! TERMINATING PROGRAM.')

#                 if ADMIN_ID:
#                     try:
#                         await client.send_message(
#                             int(ADMIN_ID),
#                             f'**KILL SWITCH ACTIVATED**\n\n'
#                             f'Loss limit of {LOSS_LIMIT} breached.\n'
#                             f'Positions closed & API disabled.\n'
#                             f'**Bot is shutting down now.**',
#                         )
#                     except Exception:
#                         pass

#                 await client.disconnect()

#                 logger.info('Shutdown complete.')
#                 sys.exit(0)

#         except Exception as e:
#             logger.error(f'Risk Monitor Error: {e}')
#             await asyncio.sleep(5)

#         await asyncio.sleep(10)


async def check_market_hours(client: TelegramClient, bridge: DhanBridge):
    """
    1. Checks for Friday Square-off (15:18).
    2. Checks for Daily Shutdown (15:30).
    """
    stop_time = time(15, 30)

    logger.info('Market Monitor Started (Fri SqOff: 15:18 | Stop: 15:30)')

    while True:
        now = datetime.now()

        # --- FRIDAY 15:18 LOGIC ---
        if now.weekday() == 4:  # Friday
            if now.time().hour == 15 and now.time().minute == 18:
                logger.warning('Friday 3:18 PM: Auto-Square Off Triggered.')
                try:
                    bridge.square_off_all()
                    if ADMIN_ID:
                        await client.send_message(int(ADMIN_ID), '**Friday Square-Off Executed**')
                except Exception as e:
                    logger.error(f'Square Off Failed: {e}')

                await asyncio.sleep(65)  # Avoid double trigger

        # --- DAILY SHUTDOWN ---
        if now.time() >= stop_time:
            logger.info('Market Closed (3:30 PM). Stopping Bot...')
            await client.disconnect()  # pyright: ignore[reportGeneralTypeIssues]
            return

        await asyncio.sleep(30)


# --- HELPER FUNCTIONS ---
async def resolve_channel(client: TelegramClient, target: str):
    logger.info(f'Resolving channel: {target}')

    if str(target).lstrip('-').isdigit():
        try:
            entity = await client.get_entity(int(target))
            logger.info(f'Resolved by ID: {getattr(entity, "title", target)}')
            return entity
        except Exception:
            pass

    try:
        entity = await client.get_entity(target)
        title = getattr(entity, 'title', getattr(entity, 'username', target))
        logger.info(f'Resolved by username/entity: {title}')
        return entity
    except Exception:
        pass

    logger.info(f"Searching dialogs for title: '{target}'...")
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
        self.active_monitors = set()

    async def add_message(self, text: str, dt: datetime):
        self.batch_messages.append(text)
        self.batch_dates.append(dt)

        if self._timer_task:
            self._timer_task.cancel()

        self._timer_task = asyncio.create_task(self._process_after_delay())

    async def _retry_monitor(self, signal: dict, reason: str):
        """
        Background task: Polls price every 60s for 45 mins.
        Handles both PRICE_LOW (Waiting for Breakout) and PRICE_HIGH (Waiting for Pullback).
        """
        symbol = signal.get('trading_symbol')
        entry = signal.get('trigger_above')

        try:
            self.active_monitors.add(symbol)
            logger.info(f'Monitor Started for {symbol} | Reason: {reason} | Entry: {entry}')

            for attempt in range(1, 61):
                if self.bridge.kill_switch_triggered:
                    logger.warning(f'Monitor Stopped for {symbol} (Kill Switch Active)')
                    return

                await asyncio.sleep(60)

                logger.info(f'Check {attempt}/45: {symbol}...')

                # Check Price via Bridge (Runs in Thread to avoid blocking)
                # execute_super_order internally checks LTP vs Entry
                status = await asyncio.to_thread(self.bridge.execute_super_order, signal)

                if status == 'SUCCESS':
                    logger.info(f'Trigger Hit! {symbol} Executed.')
                    return

                elif status == 'ERROR':
                    logger.error(f'Monitor aborted for {symbol} due to error.')
                    return

                # If status is still PRICE_LOW or PRICE_HIGH, continue loop.

            logger.warning(f'Monitor Timed Out (45m) for {symbol}. Signal Expired.')

        finally:
            self.active_monitors.discard(symbol)

    async def _process_after_delay(self):
        try:
            await asyncio.sleep(BATCH_DELAY_SECONDS)
            logger.info(f'Processing batch of {len(self.batch_messages)} messages...')

            try:
                results = process_and_save(
                    self.batch_messages,
                    self.batch_dates,
                    jsonl_path=SIGNALS_JSONL,
                    json_path=SIGNALS_JSON,
                )
            except Exception as e:
                logger.error(f'Signal parsing failed: {e}', exc_info=True)
                results = []

            if results:
                logger.info(f'Found {len(results)} valid signal(s)')
                for idx, res in enumerate(results, 1):
                    symbol = res.get('trading_symbol')
                    logger.info(
                        f'Signal {idx}/{len(results)}: {symbol} | '
                        f'{res["action"]} | Entry: {res.get("trigger_above", "N/A")}'
                    )

                    # DUPLICATE GUARD
                    if symbol in self.active_monitors:
                        logger.warning(f'Already monitoring {symbol}. Ignoring duplicate signal.')
                        continue

                    # EXECUTE AND CHECK STATUS
                    try:
                        status = await asyncio.to_thread(self.bridge.execute_super_order, res)

                        if status in ['PRICE_HIGH', 'PRICE_LOW']:
                            asyncio.create_task(self._retry_monitor(res, status))

                    except Exception as e:
                        logger.error(f'Order execution failed: {e}', exc_info=True)
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


async def main():
    logger.info('=' * 60)
    logger.info('Trading Bot Starting (Features: ATR Trail, Risk Monitor, Friday SqOff)...')
    logger.info('=' * 60)

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical('Missing TELEGRAM_API_ID or TELEGRAM_API_HASH')
        return
    if not TARGET_CHANNELS:
        logger.critical('Missing TARGET_CHANNELS')
        return

    logger.info('Configuration loaded')
    logger.info(f'   - Channels: {len(TARGET_CHANNELS)}')

    logger.info('Initializing Dhan Bridge...')
    try:
        bridge = DhanBridge()
        logger.info('Dhan Bridge initialized')
    except Exception as e:
        logger.critical(f'Failed to initialize Dhan Bridge: {e}', exc_info=True)
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
        logger.info('Connected to Telegram')

        # --- START BACKGROUND TASKS ---
        asyncio.create_task(check_market_hours(client, bridge))
        # asyncio.create_task(risk_monitor_task(client, bridge))

    except Exception as e:
        logger.critical(f'Failed to connect to Telegram: {e}', exc_info=True)
        return

    resolved_chats = []
    logger.info('Resolving target channels...')

    try:
        for target in TARGET_CHANNELS:
            try:
                entity = await resolve_channel(client, target)
                resolved_chats.append(entity)
                logger.info(f'Added listener for: {getattr(entity, "title", target)}')
            except Exception as e:
                logger.error(f"   Failed to resolve '{target}': {e}")

    except asyncio.CancelledError:
        logger.info('Bot stopped during startup (Kill Switch active). Exiting.')
        sys.exit(0)

    if not resolved_chats:
        logger.critical('No channels resolved. Exiting.')
        return

    # --- ADMIN COMMANDS ---
    if ADMIN_ID:
        try:
            admin_id_int = int(ADMIN_ID)
            logger.info(f'Admin Commands Enabled for User ID: {admin_id_int}')

            @client.on(events.NewMessage(from_users=[admin_id_int]))
            async def admin_handler(event: events.NewMessage.Event):
                text = event.raw_text.lower().strip()

                if text == '/status':
                    funds = bridge.get_funds()
                    pnl = bridge.get_total_pnl()
                    fund_str = f'Rs.{funds:.2f}' if funds is not None else 'Error'
                    pnl_str = f'Rs.{pnl:.2f}'

                    msg = (
                        f'**Bot Status**\n'
                        f'Funds: {fund_str}\n'
                        f'Day P&L: {pnl_str}\n'
                        f'Loss Limit: {LOSS_LIMIT}\n'
                        f'Time: {datetime.now().strftime("%H:%M:%S")}'
                    )
                    await event.reply(msg)

                elif text == '/logs':
                    await event.reply('Uploading logs...')
                    files = [
                        f for f in ['logs/trade_logs.log', 'logs/errors.log'] if os.path.exists(f)
                    ]
                    if files:
                        await event.reply(file=files)
                    else:
                        await event.reply('No logs found.')

                elif text == '/tail':
                    try:
                        with open('logs/trade_logs.log', 'r') as f:
                            lines = f.readlines()[-15:]
                        await event.reply(f'```{"".join(lines)}```')
                    except Exception as e:
                        await event.reply(f'Error reading logs {e}')

                elif text == '/force_sqoff':
                    await event.reply('Force Square-off Triggered!')
                    bridge.square_off_all()

        except ValueError:
            logger.error('ADMIN_ID in .env is not a valid number')

    logger.info('=' * 60)
    logger.info(f'Listening to {len(resolved_chats)} channel(s)')
    logger.info('=' * 60)

    @client.on(events.NewMessage(chats=resolved_chats))
    async def handler(event: events.NewMessage.Event):
        try:
            text = event.message.message
            if text:
                await batcher.add_message(text, event.message.date)
                logger.info(f'Received msg (len {len(text)})')
        except Exception as e:
            logger.error(f'Handler Error: {e}', exc_info=True)

    try:
        await client.run_until_disconnected()  # pyright: ignore[reportGeneralTypeIssues]
    except Exception as e:
        logger.critical(f'Client disconnected: {e}', exc_info=True)
        raise


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Bot Stopped (Keyboard Interrupt)')
    except asyncio.CancelledError:
        logger.info('Bot Stopped (Task Cancelled)')
    except SystemExit:
        pass
    except Exception as e:
        logger.critical(f'Critical Crash: {e}', exc_info=True)
        sys.exit(1)
