from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Tuple

import polars as pl
import requests

# --- CONFIGURATION ---
logger = logging.getLogger('DhanMapper')


class DhanMapper:
    """
    Handles mapping of trading symbols (e.g., 'NIFTY 25 DEC 24000 CALL') to
    Dhan-specific Security IDs, Exchange IDs, and Lot Sizes.
    Downloads and caches the master scrip CSV daily.
    """

    # Constants
    CACHE_DIR = 'cache'
    CSV_FILENAME = 'dhan_master.csv'
    MASTER_URL = 'https://images.dhan.co/api-data/api-scrip-master.csv'

    # CSV Column Headers
    COL_SYMBOL = 'SEM_CUSTOM_SYMBOL'
    COL_INSTRUMENT = 'SEM_INSTRUMENT_NAME'
    COL_EXCHANGE = 'SEM_EXM_EXCH_ID'
    COL_SEC_ID = 'SEM_SMST_SECURITY_ID'
    COL_LOT_SIZE = 'SEM_LOT_UNITS'

    def __init__(self):
        """Initializes the mapper and ensures the cache directory exists."""
        self.csv_path = os.path.join(self.CACHE_DIR, self.CSV_FILENAME)
        os.makedirs(self.CACHE_DIR, exist_ok=True)

    def _is_file_fresh(self) -> bool:
        """Returns True if the master CSV exists and was modified today."""
        if not os.path.exists(self.csv_path):
            return False

        mtime = os.path.getmtime(self.csv_path)
        file_date = datetime.fromtimestamp(mtime).date()
        return file_date == date.today()

    def _ensure_csv(self):
        """Downloads the master CSV if it does not exist or is outdated."""
        if self._is_file_fresh():
            return

        logger.info('Downloading Dhan Scrip Master...')
        logger.info('This may take a few minutes depending on connection speed...')

        try:
            with requests.get(self.MASTER_URL, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(self.csv_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.info('Download complete.')

        except requests.exceptions.Timeout:
            logger.error('Download failed: Connection timeout.')
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f'Download failed: {e}')
            raise
        except Exception as e:
            logger.error(f'Unexpected error during download: {e}', exc_info=True)
            raise

    def get_security_id(self, trading_symbol: str) -> Tuple[str, str, int]:
        """
        Maps a trading symbol to (Security ID, Exchange ID, Lot Size).

        Bidirectional Matching Logic:
        1. Input 'EICHER'       matches CSV 'EICHERMOT'  (CSV starts with Input)
        2. Input 'EICHERMOTORS' matches CSV 'EICHERMOT'  (Input starts with CSV)
        """
        self._ensure_csv()

        try:
            parts = trading_symbol.split(' ', 1)

            if len(parts) == 2:
                user_root, suffix = parts

                csv_root_col = pl.col(self.COL_SYMBOL).str.split(' ').list.get(0)
                root_match = csv_root_col.str.starts_with(user_root) | pl.lit(
                    user_root
                ).str.starts_with(csv_root_col)
                symbol_condition = root_match & pl.col(self.COL_SYMBOL).str.ends_with(suffix)
            else:
                symbol_condition = pl.col(self.COL_SYMBOL).str.starts_with(trading_symbol) | pl.lit(
                    trading_symbol
                ).str.starts_with(pl.col(self.COL_SYMBOL))

            q = (
                pl.scan_csv(self.csv_path)
                .filter(
                    symbol_condition
                    & (
                        (
                            (pl.col(self.COL_INSTRUMENT) == 'OPTSTK')
                            & (pl.col(self.COL_EXCHANGE) == 'NSE')
                        )
                        | (
                            (pl.col(self.COL_INSTRUMENT) == 'OPTIDX')
                            & (pl.col(self.COL_EXCHANGE).is_in(['NSE', 'BSE']))
                        )
                    )
                )
                .select([self.COL_SYMBOL, self.COL_SEC_ID, self.COL_EXCHANGE, self.COL_LOT_SIZE])
            )

            df = q.collect()

            if not df.is_empty():
                if len(df) > 1:
                    logger.info(
                        f"Fuzzy match found multiple results for '{trading_symbol}'. Using: {
                            df.item(0, self.COL_SYMBOL)
                        }"
                    )

                sec_id = str(df.item(0, self.COL_SEC_ID))
                exch = str(df.item(0, self.COL_EXCHANGE))

                try:
                    lot_size = int(float(df.item(0, self.COL_LOT_SIZE)))
                    lot_size = max(1, lot_size)
                except (ValueError, TypeError):
                    logger.warning(f'Invalid lot size for {trading_symbol}, defaulting to 1.')
                    lot_size = 1

                return sec_id, exch, lot_size

            logger.warning(f'Security ID Not Found for: {trading_symbol}')
            return '', '', -1

        except Exception as e:
            logger.error(f'Mapping Error for {trading_symbol}: {e}', exc_info=True)
            return '', '', -1


if __name__ == '__main__':
    print('\n--- RUNNING DHAN MAPPER DIAGNOSTICS ---\n')
    mapper = DhanMapper()

    test_cases = [
        'BANKNIFTY 30 DEC 69700 CALL',
        'SENSEX 24 DEC 85500 CALL',
        'RELIANCE 30 DEC 1500 CALL',
        'EICHER 30 DEC 8000 PUT',
        'TATASTEE 30 DEC 100 PUT',
    ]

    print(f'Testing {len(test_cases)} symbols...')
    print('-' * 70)
    print(f'{"SYMBOL":<35} | {"ID":<10} | {"EXCH":<6} | {"LOT"}')
    print('-' * 70)

    for symbol in test_cases:
        sec_id, exch, lot = mapper.get_security_id(symbol)

        id_str = sec_id if sec_id else '---'
        exch_str = exch if exch else '---'
        lot_str = str(lot) if lot != -1 else '---'

        print(f'{symbol:<35} | {id_str:<10} | {exch_str:<6} | {lot_str}')

    print('-' * 70)
