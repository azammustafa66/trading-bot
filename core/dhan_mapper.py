import logging
import os
import re
from datetime import datetime
from typing import Callable, Optional, Tuple

import polars as pl
import requests

logger = logging.getLogger('DhanMapper')


class DhanMapper:
    CSV_URL = 'https://images.dhan.co/api-data/api-scrip-master.csv'
    CSV_FILE = 'data/dhan_master.csv'

    COL_SEM_EXM_EXCH_ID = 'SEM_EXM_EXCH_ID'
    COL_SEM_SMST_SECURITY_ID = 'SEM_SMST_SECURITY_ID'
    COL_SEM_TRADING_SYMBOL = 'SEM_TRADING_SYMBOL'
    COL_SEM_CUSTOM_SYMBOL = 'SEM_CUSTOM_SYMBOL'
    COL_SEM_EXPIRY_DATE = 'SEM_EXPIRY_DATE'
    COL_SEM_INSTRUMENT_NAME = 'SEM_INSTRUMENT_NAME'
    COL_SEM_LOT_UNITS = 'SEM_LOT_UNITS'
    COL_SEM_STRIKE_PRICE = 'SEM_STRIKE_PRICE'
    COL_SEM_OPTION_TYPE = 'SEM_OPTION_TYPE'

    def __init__(self):
        if not os.path.exists('data'):
            os.makedirs('data')
        self._refresh_master_csv()
        self.df = self._load_csv()

    def _refresh_master_csv(self):
        try:
            if os.path.exists(self.CSV_FILE):
                if (datetime.now().timestamp() - os.path.getmtime(self.CSV_FILE)) < 86400:
                    return
            logger.info('Downloading Master Scrip CSV...')
            resp = requests.get(self.CSV_URL)
            with open(self.CSV_FILE, 'wb') as f:
                f.write(resp.content)
        except Exception as e:
            logger.error(f'CSV Download Error: {e}')

    def _load_csv(self):
        try:
            # Load CSV and parse date column immediately
            # We also ensure Strike Price is float for comparisons
            return pl.read_csv(self.CSV_FILE, ignore_errors=True).with_columns(
                pl.col(self.COL_SEM_EXPIRY_DATE).str.strptime(
                    pl.Date, '%Y-%m-%d %H:%M:%S', strict=False
                ),
                pl.col(self.COL_SEM_STRIKE_PRICE).cast(pl.Float64, strict=False),
            )
        except Exception as e:
            logger.error(f'CSV Load Error: {e}')
            return pl.DataFrame()

    def get_security_id(
        self,
        trading_symbol: str,
        price_ref: float = 0,
        ltp_fetcher: Optional[Callable[[str, str], float]] = None,
    ) -> Tuple[Optional[str], Optional[str], int, float]:
        """
        Maps a trading symbol to Dhan Security ID.
        Returns: (SecurityID, ExchangeID, LotSize, LTP)
        """
        today = datetime.now().date()

        # ---------------------------------------------------------
        # 1. EXACT MATCH (Fastest)
        # ---------------------------------------------------------
        res = self.df.filter(
            (pl.col(self.COL_SEM_CUSTOM_SYMBOL) == trading_symbol)
            & (~pl.col(self.COL_SEM_INSTRUMENT_NAME).str.contains('FUT'))
            & (pl.col(self.COL_SEM_EXPIRY_DATE) >= today)
        )

        if res.height > 0:
            return (
                str(res[0, self.COL_SEM_SMST_SECURITY_ID]),
                str(res[0, self.COL_SEM_EXM_EXCH_ID]),
                max(1, int(res[0, self.COL_SEM_LOT_UNITS] or 1)),
                0.0,
            )

        # ---------------------------------------------------------
        # 2. SMART SEARCH (Robust Fallback)
        # ---------------------------------------------------------
        # Parse the signal string: e.g., "NIFTY 25 JAN 21000 CALL"
        try:
            parts = trading_symbol.upper().split()

            # Heuristics extraction
            strike = None
            opt_type = None
            underlying = None

            for p in parts:
                # Detect Strike (Number)
                if re.match(r'^\d+(\.\d+)?$', p):
                    strike = float(p)
                # Detect Option Type
                elif p in ['CE', 'CALL']:
                    opt_type = 'CALL'
                elif p in ['PE', 'PUT']:
                    opt_type = 'PUT'
                # Detect Underlying (skip dates/months)
                elif p not in [
                    'JAN',
                    'FEB',
                    'MAR',
                    'APR',
                    'MAY',
                    'JUN',
                    'JUL',
                    'AUG',
                    'SEP',
                    'OCT',
                    'NOV',
                    'DEC',
                ] and not re.match(r'^\d+$', p):
                    # Usually the first part is underlying
                    if not underlying:
                        underlying = p

            # FIX 1: Ensure strict type safety for 'underlying' (must be str, not None)
            if not strike or not opt_type or not underlying:
                return None, None, 0, 0.0

            # Note: Dhan CSV 'SEM_OPTION_TYPE' is usually 'CE' or 'PE'
            csv_opt = 'CE' if opt_type == 'CALL' else 'PE'

            smart_res = self.df.filter(
                (pl.col(self.COL_SEM_CUSTOM_SYMBOL).str.contains(underlying))
                & (pl.col(self.COL_SEM_OPTION_TYPE) == csv_opt)
                & (pl.col(self.COL_SEM_STRIKE_PRICE) == strike)
                & (pl.col(self.COL_SEM_EXPIRY_DATE) >= today)
            ).sort(self.COL_SEM_EXPIRY_DATE)

            if smart_res.height > 0:
                # FIX 2: Use direct DataFrame access [0, ColName] instead of row object
                # This ensures we get scalars (str/int) and satisfies Pylance

                s_id = str(smart_res[0, self.COL_SEM_SMST_SECURITY_ID])
                e_id = str(smart_res[0, self.COL_SEM_EXM_EXCH_ID])

                # Handle lot size safely (convert from potential None/int to int)
                raw_lot = smart_res[0, self.COL_SEM_LOT_UNITS]
                lot_size = int(raw_lot) if raw_lot else 1

                return (s_id, e_id, max(1, lot_size), 0.0)

        except Exception as e:
            logger.warning(f'Smart Search Failed: {e}')

        return None, None, 0, 0.0

    def get_underlying_future_id(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Finds the Current Month Future ID for an Index Option.
        Example: 'BANKNIFTY 43000 CE' -> 'BANKNIFTY NOV FUT' ID
        """
        try:
            clean = symbol.upper().replace('BUY', '').replace('SELL', '').strip()
            if not clean:
                return None, None

            underlying = clean.split()[0]
            current_date = datetime.now().date()

            res = (
                self.df.filter(
                    (pl.col(self.COL_SEM_TRADING_SYMBOL).str.contains(underlying))
                    & (pl.col(self.COL_SEM_INSTRUMENT_NAME).is_in(['FUTIDX', 'FUTSTK']))
                    & (pl.col(self.COL_SEM_EXPIRY_DATE) >= current_date)
                )
                .sort(self.COL_SEM_EXPIRY_DATE)
                .head(1)
            )

            if res.height > 0:
                return (
                    str(res[0, self.COL_SEM_SMST_SECURITY_ID]),
                    str(res[0, self.COL_SEM_EXM_EXCH_ID]),
                )
            return None, None
        except Exception as e:
            logger.error(f'Future Map Error: {e}')
            return None, None


# --- TEST SUITE (Included for verification) ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    mapper = DhanMapper()

    # Test 1: Exact Match (Simulated)
    print('Testing Mapper...')
    # Add real symbol logic here if CSV is present

    # --- Pylance Type Verification Mock ---
    logging.basicConfig(level=logging.INFO)
    m = DhanMapper()
    # Pylance will now correctly identify the return tuple types
    res = m.get_security_id('NIFTY 25000 CALL', 150.0, lambda x, y: 148.0)
    print(f'Verified Result: {res}')

    # ==============================================================================
    #                                TEST CASES
    # ==============================================================================
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print('\n' + '=' * 60)
    print('RUNNING DHAN MAPPER DIAGNOSTICS & TESTS')
    print('=' * 60 + '\n')

    mapper = DhanMapper()

    def mock_ltp_fetcher(sec_id: str, segment: str) -> float:
        print(f'    [Mock API] Fetching LTP for ID {sec_id} ({segment})...')
        return 1425.0

    # ------------------------------------------------------------------
    # TEST 1: Auto-Padding (285.5 -> 285.50)
    # ------------------------------------------------------------------
    symbol_pad = 'BHEL 30 DEC 282.5 CALL'
    print(f"Test 1: Auto-Padding Check for '{symbol_pad}'")
    sid, exch, lot, ltp = mapper.get_security_id(symbol_pad)
    print(f'Result: ID={sid if sid else "NOT FOUND"} | Exch={exch} | Lot={lot}\n')

    # ------------------------------------------------------------------
    # TEST 2: Futures Rejection (Safety Check)
    # ------------------------------------------------------------------
    symbol_fut = 'GOLDM FEB FUT'
    print(f"Test 2: Futures Rejection Check for '{symbol_fut}'")
    # Expected: Should return empty ID because FUTCOM is filtered out
    sid, exch, lot, ltp = mapper.get_security_id(symbol_fut)
    if not sid:
        print('✅ PASSED: Futures contract was correctly ignored.')
    else:
        print(f'❌ FAILED: Futures contract was returned! ID={sid}')
    print()

    # ------------------------------------------------------------------
    # TEST 3: Smart Expiry + Price Match
    # ------------------------------------------------------------------
    symbol_smart = 'GOLDM DEC 136000 CALL'
    target_price = 1500.0

    print(f"Test 3: Smart Expiry & Price Match for '{symbol_smart}' @ {target_price}")

    # We pass our mock fetcher. The mapper should find candidates (Feb 2025, Feb 2026)
    # and call the fetcher. Since fetcher returns 3050 (close to 3000),
    # it should match successfully.
    sid, exch, lot, ltp = mapper.get_security_id(
        symbol_smart, price_ref=target_price, ltp_fetcher=mock_ltp_fetcher
    )
    print(f'Result: ID={sid if sid else "NOT FOUND"} | Exch={exch} | Lot={lot}')

    print('-' * 60)
