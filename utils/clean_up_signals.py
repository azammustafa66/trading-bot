import logging
import os
import shutil
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Cleanup")

SIGNALS_FILE = "signals.jsonl"
ARCHIVE_DIR = "archive"


def clean_signals():
    if not os.path.exists(SIGNALS_FILE):
        logger.info("No signals file to clean.")
        return

    # 1. Create Archive Folder
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # 2. Generate Backup Name (e.g., signals_2025-11-27.jsonl)
    today_str = datetime.now().strftime("%Y-%m-%d")
    backup_path = os.path.join(ARCHIVE_DIR, f"signals_{today_str}.jsonl")

    try:
        # 3. Copy current file to archive
        shutil.copy2(SIGNALS_FILE, backup_path)
        logger.info(f"✅ Backup created: {backup_path}")

        # 4. Wipe original file
        with open(SIGNALS_FILE, "w") as f:
            f.truncate(0)  # Makes size 0
        logger.info(f"🧹 {SIGNALS_FILE} wiped clean for tomorrow.")

        # 5. Optional: Clear Scrip Master Cache to force re-download tomorrow
        # os.remove("cache/dhan_master.csv")
        # logger.info("🗑️  Stale CSV cache removed.")

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")


if __name__ == "__main__":
    clean_signals()
