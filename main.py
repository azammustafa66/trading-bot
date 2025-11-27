import asyncio
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from telethon import TelegramClient, events

from core.dhan_bridge import DhanBridge
from core.signal_parser import process_and_save

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            "trade_logs.log", mode="a", encoding="utf-8"
        ),  # Log to file
        logging.StreamHandler(sys.stdout),  # Log to console
    ],
)
logger = logging.getLogger("LiveListener")

# --- CONFIGURATION ---
load_dotenv()
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

            # 1. PARSE & SAVE
            results = process_and_save(
                self.batch_messages,
                self.batch_dates,
                jsonl_path="signals.jsonl",
                json_path="signals.json",
            )

            # 2. EXECUTE TRADES
            if results:
                for res in results:
                    logger.info(
                        f"✅ SIGNAL SAVED: {res['trading_symbol']} | {res['action']}"
                    )

                    # The Magic Line: Fires the Super Order
                    self.bridge.execute_super_order(res)
            else:
                logger.info("ℹ️  No valid signals found in batch.")

        except asyncio.CancelledError:
            pass  # Timer reset, normal behavior
        except Exception as e:
            logger.error(f"❌ Batch Error: {e}")
        finally:
            # Clean up buffer only if this specific task wasn't cancelled
            current_task = asyncio.current_task()
            if current_task and not current_task.cancelled():
                self.batch_messages = []
                self.batch_dates = []
                self._timer_task = None


async def main():
    # 1. Validation
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        logger.critical("❌ Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env")
        return
    if not TARGET_CHANNEL:
        logger.critical("❌ Missing TARGET_CHANNEL in .env")
        return

    # 2. Initialize Bridge (Downloads CSV & Connects Dhan)
    logger.info("🌉 Initializing Dhan Bridge...")
    bridge = DhanBridge()

    # 3. Initialize Batcher
    batcher = SignalBatcher(bridge)

    # 4. Start Telegram Client
    logger.info("🔌 Connecting to Telegram...")
    client = TelegramClient(
        session=SESSION_NAME, api_id=int(TELEGRAM_API_ID), api_hash=TELEGRAM_API_HASH
    )
    await client.start()  # pyright: ignore[reportGeneralTypeIssues]

    logger.info(f"👀 Listening to: {TARGET_CHANNEL}")
    logger.info("Press Ctrl+C to stop.")

    # 5. Event Loop
    @client.on(events.NewMessage(chats=TARGET_CHANNEL))
    async def handler(event):
        try:
            text = event.message.message
            if not text:
                return

            # Preview log
            preview = text.replace("\n", " ")[:50]
            logger.info(f"📥 Received: {preview}...")

            # Send to Batcher
            await batcher.add_message(text, event.message.date)

        except Exception as e:
            logger.error(f"Handler Error: {e}")

    await client.run_until_disconnected()  # pyright: ignore[reportGeneralTypeIssues]


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot Stopped.")
    except Exception as e:
        logger.critical(f"🔥 Critical Crash: {e}")
