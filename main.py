import asyncio
import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from telethon import TelegramClient, events

from core.dhan_bridge import DhanBridge
from core.signal_parser import process_and_save

# --- LOGGING SETUP ---
# Load environment variables first
load_dotenv()

# Get log configuration from environment
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_LOG_SIZE = int(os.getenv("MAX_LOG_SIZE_MB", "50")) * 1024 * 1024  # Convert to bytes
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Setup rotating file handler for main logs
file_handler = RotatingFileHandler(
    "logs/trade_logs.log",
    mode="a",
    maxBytes=MAX_LOG_SIZE,
    backupCount=LOG_BACKUP_COUNT,
    encoding="utf-8"
)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Setup error-only log file
error_handler = RotatingFileHandler(
    "logs/errors.log",
    mode="a",
    maxBytes=MAX_LOG_SIZE,
    backupCount=LOG_BACKUP_COUNT,
    encoding="utf-8"
)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(name)s] [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Setup console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
))

# Configure root logger
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[file_handler, error_handler, console_handler]
)

logger = logging.getLogger("LiveListener")

# --- CONFIGURATION ---
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_session")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")

BATCH_DELAY_SECONDS = 2.0


class SignalBatcher:
    def __init__(self, bridge_instance: DhanBridge):
        self.batch_messages = []
        self.batch_dates = []
        self._timer_task = None
        self.bridge = bridge_instance

    async def add_message(self, text: str, dt: datetime):
        """Adds message to buffer and resets the processing timer."""
        self.batch_messages.append(text)
        self.batch_dates.append(dt)

        if self._timer_task:
            self._timer_task.cancel()

        self._timer_task = asyncio.create_task(self._process_after_delay())

    async def _process_after_delay(self):
        """Waits for silence (completing split messages) then processes."""
        try:
            await asyncio.sleep(BATCH_DELAY_SECONDS)

            logger.info(
                f"⚡ Processing batch of {len(self.batch_messages)} messages..."
            )

            # Log the batch content for debugging
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Batch content: {self.batch_messages}")

            # 1. PARSE & SAVE
            try:
                results = process_and_save(
                    self.batch_messages,
                    self.batch_dates,
                    jsonl_path="signals.jsonl",
                    json_path="signals.json",
                )
                logger.debug(f"Parser returned {len(results) if results else 0} signals")
            except Exception as e:
                logger.error(f"❌ Signal parsing failed: {e}", exc_info=True)
                results = []

            # 2. EXECUTE TRADES
            if results:
                logger.info(f"✅ Found {len(results)} valid signal(s)")
                for idx, res in enumerate(results, 1):
                    logger.info(
                        f"📊 Signal {idx}/{len(results)}: {res['trading_symbol']} | "
                        f"{res['action']} | Entry: {res.get('trigger_above', 'N/A')} | "
                        f"SL: {res.get('stop_loss', 'N/A')} | "
                        f"Positional: {res.get('is_positional', False)}"
                    )

                    # The Magic Line: Fires the Super Order
                    try:
                        self.bridge.execute_super_order(res)
                    except Exception as e:
                        logger.error(
                            f"❌ Order execution failed for {res['trading_symbol']}: {e}",
                            exc_info=True
                        )
            else:
                logger.info("ℹ️  No valid signals found in batch.")

        except asyncio.CancelledError:
            logger.debug("Batch processing cancelled (timer reset)")
            pass  # Timer reset, normal behavior
        except Exception as e:
            logger.error(f"❌ Batch processing error: {e}", exc_info=True)
        finally:
            # Clean up buffer only if this specific task wasn't cancelled
            current_task = asyncio.current_task()
            if current_task and not current_task.cancelled():
                self.batch_messages = []
                self.batch_dates = []
                self._timer_task = None
                logger.debug("Batch buffer cleared")


async def main():
    # Print startup banner
    logger.info("=" * 60)
    logger.info("🤖 Trading Bot Starting...")
    logger.info("=" * 60)

    # 1. Validation
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical("❌ Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env")
        logger.critical("💡 Please copy .env.example to .env and configure it")
        return
    if not TARGET_CHANNEL:
        logger.critical("❌ Missing TARGET_CHANNEL in .env")
        logger.critical("💡 Please add TARGET_CHANNEL to your .env file")
        return

    logger.info(f"📋 Configuration loaded successfully")
    logger.info(f"   - Session: {SESSION_NAME}")
    logger.info(f"   - Channel: {TARGET_CHANNEL}")
    logger.info(f"   - Log Level: {LOG_LEVEL}")

    # 2. Initialize Bridge (Downloads CSV & Connects Dhan)
    logger.info("🌉 Initializing Dhan Bridge...")
    try:
        bridge = DhanBridge()
        logger.info("✅ Dhan Bridge initialized successfully")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize Dhan Bridge: {e}", exc_info=True)
        return

    # 3. Initialize Batcher
    logger.info("📦 Initializing Signal Batcher...")
    batcher = SignalBatcher(bridge)
    logger.info(f"✅ Batcher configured (delay: {BATCH_DELAY_SECONDS}s)")

    # 4. Start Telegram Client
    logger.info("🔌 Connecting to Telegram...")
    try:
        client = TelegramClient(
            session=SESSION_NAME, api_id=int(TELEGRAM_API_ID), api_hash=TELEGRAM_API_HASH
        )
        await client.start()  # pyright: ignore[reportGeneralTypeIssues]
        logger.info("✅ Connected to Telegram successfully")
    except Exception as e:
        logger.critical(f"❌ Failed to connect to Telegram: {e}", exc_info=True)
        return

    logger.info("=" * 60)
    logger.info(f"👀 Listening to: {TARGET_CHANNEL}")
    logger.info(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Press Ctrl+C to stop.")
    logger.info("=" * 60)

    # 5. Event Loop
    @client.on(events.NewMessage(chats=TARGET_CHANNEL))
    async def handler(event):
        try:
            text = event.message.message
            if not text:
                logger.debug("Received empty message, skipping")
                return

            # Preview log
            preview = text.replace("\n", " ")[:50]
            logger.info(f"📥 Received: {preview}...")

            # Send to Batcher
            await batcher.add_message(text, event.message.date)

        except Exception as e:
            logger.error(f"❌ Handler Error: {e}", exc_info=True)

    try:
        await client.run_until_disconnected()  # pyright: ignore[reportGeneralTypeIssues]
    except Exception as e:
        logger.critical(f"❌ Client disconnected with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot Stopped.")
    except Exception as e:
        logger.critical(f"🔥 Critical Crash: {e}")
