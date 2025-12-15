import logging
import os
from datetime import date, datetime

import polars as pl
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('DhanMapper')


class DhanMapper:
    def __init__(self):
        self.base_dir = 'cache'
        self.file_name = 'dhan_master.csv'
        self.csv_path = os.path.join(self.base_dir, self.file_name)
        self.url = 'https://images.dhan.co/api-data/api-scrip-master.csv'

        # Ensure cache directory exists
        os.makedirs(self.base_dir, exist_ok=True)

    def _is_file_fresh(self):
        """Returns True if file exists AND was modified today."""
        if not os.path.exists(self.csv_path):
            return False

        mtime = os.path.getmtime(self.csv_path)
        file_date = datetime.fromtimestamp(mtime).date()
        today = date.today()

        return file_date == today

    def _ensure_csv(self):
        """Downloads the master CSV if it doesn't exist or is old."""
        if self._is_file_fresh():
            return

        logger.info('Downloading Dhan Scrip Master...')
        logger.info('This may take a few minutes depending on your connection...')

        try:
            with requests.get(self.url, stream=True, timeout=60) as r:
                r.raise_for_status()

                with open(self.csv_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            logger.info('Download complete.')
        except requests.exceptions.Timeout:
            logger.error('Download failed: Connection timeout')
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f'Download failed: {e}')
            raise
        except Exception as e:
            logger.error(f'Unexpected error during download: {e}', exc_info=True)
            raise

    def get_security_id(self, trading_symbol):
        """
        Maps 'RELIANCE 26 DEC 2500 CALL' -> ('12345', 'NSE', 250)
        Enforces NSE for Stocks.
        """
        self._ensure_csv()

        try:
            q = (
                pl.scan_csv(self.csv_path)
                .filter(
                    (pl.col('SEM_CUSTOM_SYMBOL') == trading_symbol)
                    & (
                        (
                            (pl.col('SEM_INSTRUMENT_NAME') == 'OPTSTK')
                            & (pl.col('SEM_EXM_EXCH_ID') == 'NSE')
                        )
                        | (
                            (pl.col('SEM_INSTRUMENT_NAME') == 'OPTIDX')
                            & (pl.col('SEM_EXM_EXCH_ID').is_in(['NSE', 'BSE']))
                        )
                    )
                )
                .select(
                    [
                        'SEM_SMST_SECURITY_ID',
                        'SEM_EXM_EXCH_ID',
                        'SEM_LOT_UNITS',
                    ]
                )
            )

            df = q.collect()

            if not df.is_empty():
                sec_id = str(df.item(0, 'SEM_SMST_SECURITY_ID'))
                exch = str(df.item(0, 'SEM_EXM_EXCH_ID'))

                try:
                    lot_size = int(float(df.item(0, 'SEM_LOT_UNITS')))
                except Exception as e:
                    logger.info(f'{e}')
                    lot_size = 1

                return sec_id, exch, lot_size

            logger.warning(f'ID Not Found for: {trading_symbol}')
            return '', '', -1

        except Exception as e:
            logger.error(f'Mapping Error: {e}')
            return '', '', -1


if __name__ == '__main__':
    print('\nINSTANTIATING MAPPER & RUNNING TESTS')
    mapper = DhanMapper()

    test_cases = [
        'BANKNIFTY 30 DEC 69700 CALL',
        'SENSEX 11 DEC 85500 CALL',
        'RELIANCE 30 DEC 1500 CALL',
    ]

    print(f'Testing {len(test_cases)} symbols...')
    print('-' * 65)
    print(f'{"SYMBOL":<30} | {"ID":<10} | {"EXCH":<5} | {"LOT"}')
    print('-' * 65)

    for symbol in test_cases:
        sec_id, exch, lot = mapper.get_security_id(symbol)
        id_str = sec_id if sec_id else '---'
        exch_str = exch if exch else '---'
        lot_str = str(lot) if lot != -1 else '---'
        print(f'{symbol:<30} | {id_str:<10} | {exch_str:<5} | {lot_str}')

    print('-' * 65)
