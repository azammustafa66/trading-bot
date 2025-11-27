import logging
import os
from datetime import date, datetime

import polars as pl
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DhanMapper")


class DhanMapper:
    def __init__(self):
        self.base_dir = "cache"
        self.file_name = "dhan_master.csv"
        self.csv_path = os.path.join(self.base_dir, self.file_name)
        self.url = "https://images.dhan.co/api-data/api-scrip-master.csv"

        # Ensure cache directory exists
        os.makedirs(self.base_dir, exist_ok=True)

    def _is_file_fresh(self):
        """Returns True if file exists AND was modified today."""
        if not os.path.exists(self.csv_path):
            return False

        # Get file modification timestamp
        mtime = os.path.getmtime(self.csv_path)
        file_date = datetime.fromtimestamp(mtime).date()
        today = date.today()

        return file_date == today

    def _ensure_csv(self):
        """Downloads the master CSV if it doesn't exist or is old."""
        if self._is_file_fresh():
            logger.debug("CSV file is fresh, no download needed")
            return

        logger.info(f"⬇️ Downloading Dhan Scrip Master (~500MB)...")
        logger.info("This may take a few minutes depending on your connection...")

        try:
            # Add timeout and better error handling
            with requests.get(self.url, stream=True, timeout=60) as r:
                r.raise_for_status()

                # Get total size if available
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0

                with open(self.csv_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Log progress every 50MB
                        if total_size > 0 and downloaded % (50 * 1024 * 1024) < 8192:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"📥 Downloaded: {progress:.1f}%")

            logger.info("✅ Download complete.")
        except requests.exceptions.Timeout:
            logger.error("❌ Download failed: Connection timeout")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Download failed: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error during download: {e}", exc_info=True)
            raise

    def get_security_id(self, trading_symbol):
        """
        Maps 'BANKNIFTY 30 DEC 69700 CALL' -> ('35000', 'NSE', 30)
        Returns: (security_id, exchange_segment, lot_size)
        """
        self._ensure_csv()

        try:
            # POLARS LAZY SCAN
            # We filter for the exact string match.
            # We also ensure it's an Option (OPTIDX) and on a valid exchange.
            q = (
                pl.scan_csv(self.csv_path)
                .filter(
                    (pl.col("SEM_CUSTOM_SYMBOL") == trading_symbol)
                    & (pl.col("SEM_EXM_EXCH_ID").is_in(["NSE", "BSE"]))
                    & (pl.col("SEM_INSTRUMENT_NAME") == "OPTIDX")
                )
                .select(
                    [
                        "SEM_SMST_SECURITY_ID",
                        "SEM_EXM_EXCH_ID",
                        "SEM_LOT_UNITS",
                    ]
                )
            )

            # Collect result (should be 1 row)
            df = q.collect()

            if not df.is_empty():
                sec_id = str(df.item(0, "SEM_SMST_SECURITY_ID"))
                exch = str(df.item(0, "SEM_EXM_EXCH_ID"))

                # Safe Lot Size Conversion
                try:
                    lot_size = int(float(df.item(0, "SEM_LOT_UNITS")))
                except:
                    lot_size = 1

                return sec_id, exch, lot_size

            logger.warning(f"❌ ID Not Found for: {trading_symbol}")
            return None, None, None

        except Exception as e:
            logger.error(f"Mapping Error: {e}")
            return None, None, None


# --- TEST SUITE ---
if __name__ == "__main__":
    print("\n🔬 INSTANTIATING MAPPER & RUNNING TESTS")
    mapper = DhanMapper()

    # 1. Define Test Symbols
    # (NOTE: These dates MUST match current active contracts in the market)
    test_cases = [
        "BANKNIFTY 30 DEC 69700 CALL",
        "NIFTY 02 DEC 24500 CALL",
        "SENSEX 06 DEC 86000 CALL",
        "NIFTY 32 DEC 99000 CALL",  # Invalid
    ]

    print(f"\n📊 Testing {len(test_cases)} symbols against Dhan Master CSV...")
    print("-" * 65)
    print(f"{'SYMBOL':<30} | {'ID':<10} | {'EXCH':<5} | {'LOT'}")
    print("-" * 65)

    for symbol in test_cases:
        sec_id, exch, lot = mapper.get_security_id(symbol)

        # Formatting for print
        id_str = sec_id if sec_id else "---"
        exch_str = exch if exch else "---"
        lot_str = str(lot) if lot else "---"

        print(f"{symbol:<30} | {id_str:<10} | {exch_str:<5} | {lot_str}")

    print("-" * 65)
    print("✅ Done.")
