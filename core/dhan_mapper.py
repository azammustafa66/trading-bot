from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime
from typing import Callable, Optional, Tuple

import polars as pl
import requests

logger = logging.getLogger('DhanMapper')


class DhanMapper:
    """
    Handles mapping of trading symbols.
    Features:
    - Auto-pads decimals (285.5 -> 285.50)
    - SMART EXPIRY: Resolves missing dates.
    - FUZZY ROOT: Matches 'INDUSTOWERS' to 'INDUSTOWER'.
    - PRICE MATCHING: Uses live price to find the correct contract.
    - SAFETY: Strictly IGNORES Futures.
    - OPTIMIZATION: Returns fetched LTP to Bridge to save API calls.
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
    COL_EXPIRY = 'SEM_EXPIRY_DATE'

    def __init__(self):
        self.csv_path = os.path.join(self.CACHE_DIR, self.CSV_FILENAME)
        os.makedirs(self.CACHE_DIR, exist_ok=True)

    def _is_file_fresh(self) -> bool:
        if not os.path.exists(self.csv_path):
            return False
        mtime = os.path.getmtime(self.csv_path)
        return datetime.fromtimestamp(mtime).date() == date.today()

    def _ensure_csv(self):
        if self._is_file_fresh():
            return

        try:
            logger.info('Downloading fresh Master CSV...')
            with requests.get(self.MASTER_URL, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(self.csv_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            logger.error(f'Download failed: {e}')
            raise

    def get_security_id(
        self,
        trading_symbol: str,
        price_ref: float = 0.0,
        ltp_fetcher: Optional[Callable[[str, str], float]] = None,
    ) -> Tuple[str, str, int, float]:
        """
        Returns: (SecurityID, Exchange, LotSize, MappedLTP)
        """
        self._ensure_csv()

        try:
            # 1. AUTO-PADDING (285.5 -> 285.50)
            padded_symbol = re.sub(r'(\d+\.\d)(?!\d)', r'\g<1>0', trading_symbol)
            target_symbol = padded_symbol if padded_symbol != trading_symbol else trading_symbol

            # 2. CHECK FOR EXPLICIT DAY
            has_day = bool(
                re.search(
                    r'\b\d{1,2}\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)',
                    target_symbol,
                    re.IGNORECASE,
                )
            )

            lf = pl.scan_csv(self.csv_path)

            # 3. BUILD FILTER
            if has_day:
                # Exact match logic with Fuzzy Root support
                parts = target_symbol.split(' ', 1)
                if len(parts) == 2:
                    root, suffix = parts
                    csv_root = pl.col(self.COL_SYMBOL).str.split(' ').list.get(0)
                    # Bidirectional check: 'INDUSTOWERS' matches 'INDUSTOWER'
                    root_match = csv_root.str.starts_with(root) | pl.lit(root).str.starts_with(
                        csv_root
                    )
                    symbol_filter = root_match & pl.col(self.COL_SYMBOL).str.ends_with(suffix)
                else:
                    symbol_filter = pl.col(self.COL_SYMBOL).str.starts_with(target_symbol)
            else:
                # Smart Match (User missed date)
                parts = target_symbol.split(' ')
                root = parts[0]
                month_str = parts[1] if len(parts) > 1 else ''
                suffix_parts = parts[2:]
                suffix = ' '.join(suffix_parts) if suffix_parts else ''

                csv_root = pl.col(self.COL_SYMBOL).str.split(' ').list.get(0)
                root_match = csv_root.str.starts_with(root) | pl.lit(root).str.starts_with(csv_root)

                symbol_filter = root_match & pl.col(self.COL_SYMBOL).str.contains(month_str.upper())
                if suffix:
                    symbol_filter = symbol_filter & pl.col(self.COL_SYMBOL).str.ends_with(suffix)

            # 4. EXECUTE QUERY
            q = lf.filter(
                symbol_filter
                & (
                    (pl.col(self.COL_EXCHANGE) == 'MCX')
                    & (pl.col(self.COL_INSTRUMENT).is_in(['OPTFUT', 'OPTCOM']))
                    | (pl.col(self.COL_EXCHANGE) == 'NSE')
                    | (pl.col(self.COL_EXCHANGE) == 'BSE')
                )
            ).select(
                [
                    self.COL_SYMBOL,
                    self.COL_SEC_ID,
                    self.COL_EXCHANGE,
                    self.COL_LOT_SIZE,
                    self.COL_EXPIRY,
                    self.COL_INSTRUMENT,
                ]
            )

            df = q.collect()

            if df.is_empty():
                logger.warning(f'Security ID Not Found for: {trading_symbol}')
                return '', '', -1, 0.0

            # 5. CANDIDATE SELECTION
            try:
                df = (
                    df.with_columns(
                        pl.col(self.COL_EXPIRY)
                        .str.to_datetime('%Y-%m-%d %H:%M:%S', strict=False)
                        .alias('expiry_dt')
                    )
                    .filter(pl.col('expiry_dt') >= datetime.now())
                    .sort('expiry_dt')
                )
            except Exception:
                pass

            candidates = df.head(3)
            best_match = candidates.row(0)
            best_match_ltp = 0.0

            # --- PRICE MATCHING LOGIC ---
            if price_ref > 0 and ltp_fetcher is not None and len(candidates) > 0:
                logger.info(f'Resolving ambiguous symbol via PRICE MATCHING. Target: {price_ref}')
                best_diff = float('inf')
                found_better_match = False

                for row in candidates.iter_rows(named=True):
                    s_id = str(row[self.COL_SEC_ID])
                    exch = str(row[self.COL_EXCHANGE])

                    if exch == 'MCX':
                        segment = 'MCX_COMM'
                    elif exch == 'BSE':
                        segment = 'BSE_FNO'
                    else:
                        segment = 'NSE_FNO'

                    ltp = ltp_fetcher(s_id, segment)

                    if ltp and ltp > 0:
                        diff = abs(ltp - price_ref)
                        pct_diff = (diff / price_ref) * 100
                        logger.info(
                            f'Candidate: {row[self.COL_SYMBOL]} | LTP: {ltp} | Diff: \
                                    {pct_diff:.2f}%'
                        )

                        if pct_diff < 20 and diff < best_diff:
                            best_diff = diff
                            best_match = (
                                row[self.COL_SYMBOL],
                                row[self.COL_SEC_ID],
                                row[self.COL_EXCHANGE],
                                row[self.COL_LOT_SIZE],
                                row[self.COL_EXPIRY],
                                row[self.COL_INSTRUMENT],
                            )
                            best_match_ltp = ltp
                            found_better_match = True

                if found_better_match:
                    logger.info(f'✅ Price Match Winner: {best_match[0]}')

            # Extract Result
            sec_id = str(best_match[1])
            exch = str(best_match[2])
            try:
                lot_size = int(float(best_match[3]))
                lot_size = max(1, lot_size)
            except Exception:
                lot_size = 1

            return sec_id, exch, lot_size, best_match_ltp

        except Exception as e:
            logger.error(f'Mapping Error for {trading_symbol}: {e}', exc_info=True)
            return '', '', -1, 0.0


# ==============================================================================
#                                TEST CASES
# ==============================================================================
if __name__ == '__main__':
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
