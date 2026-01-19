"""
Trading Bot - Main Entry Point

Listens to Telegram channels for trading signals and executes trades on Dhan.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from telethon import TelegramClient, events

try:
    from core.dhan_bridge import DhanBridge
    from core.notifier import Notifier
    from core.signal_batcher import SignalBatcher
except ImportError as e:
    sys.stderr.write(f'Import Error: {e}. Ensure you are running from the root directory.\n')
    sys.exit(1)

load_dotenv()

# --- CONFIG --- #
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
MAX_LOG_SIZE = int(os.getenv('MAX_LOG_SIZE_MB', '50')) * 1024 * 1024
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))

TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME', 'telegram_session')
ADMIN_ID = int(os.getenv('ADMIN_ID', ''))

RAW_CHANNELS = os.getenv('TARGET_CHANNELS', os.getenv('TARGET_CHANNEL', ''))
TARGET_CHANNELS = [x.strip() for x in RAW_CHANNELS.split(',') if x.strip()]

# Add numeric ID support
TARGET_CHANNEL_ID = os.getenv('TARGET_CHANNEL_ID', '')
if TARGET_CHANNEL_ID:
    TARGET_CHANNELS.append(int(TARGET_CHANNEL_ID))

os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)


# --- LOGGING --- #
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
logger = logging.getLogger('Main')


# --- SIGNAL HANDLING --- #
def handle_shutdown_signal(signum, frame):
    logger.info(f'Received signal {signum}. Shutting down.')
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)


# --- RECONCILIATION --- #
async def reconciliation_loop(bridge: DhanBridge, interval: int = 1000):
    """Periodically reconciles local trades with broker positions."""
    logger.info('Reconciliation loop started')

    while True:
        try:
            await asyncio.to_thread(bridge.reconcile_positions)
        except Exception as e:
            logger.error(f'Reconciliation loop error: {e}', exc_info=True)

        await asyncio.sleep(interval)


# --- MAIN --- #
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

    await notifier.started_bot()

    asyncio.create_task(reconciliation_loop(bridge, 300))  # Every 5 minutes

    resolved = []
    for ch in TARGET_CHANNELS:
        try:
            # If channel is a string but looks like an ID, convert it
            if isinstance(ch, str) and ch.lstrip('-').isdigit():
                ch = int(ch)

            resolved.append(await client.get_entity(ch))
        except Exception as e:
            logger.error(f'Failed to resolve channel {ch}: {e}')

    @client.on(events.NewMessage(chats=resolved))
    async def handler(event):
        if event.message and event.message.message:
            chat = await event.get_chat()
            chat_name = getattr(chat, 'title', getattr(chat, 'username', 'Unknown'))
            text = event.message.message.replace('\n', ' ')[:50]
            logger.info(f'📩 Received from [{chat_name}]: {text}...')
            await batcher.add_message(event.message.message, event.message.date, event.chat_id)

    await client.run_until_disconnected()  # pyright: ignore[reportGeneralTypeIssues]


if __name__ == '__main__':
    asyncio.run(main())
