from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from typing import Callable, Optional, Tuple

import polars as pl
import requests

# Ensure this util returns date in IST
from utils.generate_expiry_dates import get_today, select_expiry_date

logger = logging.getLogger('DhanMapper')


class DhanMapper:
    CSV_URL = 'https://images.dhan.co/api-data/api-scrip-master.csv'
    CSV_FILE = 'cache/dhan_master.csv'

    # Column Constants
    COL_SEM_SM_ID = 'SEM_SMST_SECURITY_ID'
    COL_SEM_EXM_EXCH_ID = 'SEM_EXM_EXCH_ID'
    COL_SEM_TRADING_SYMBOL = 'SEM_TRADING_SYMBOL'
    COL_SEM_CUSTOM_SYMBOL = 'SEM_CUSTOM_SYMBOL'
    COL_SEM_EXPIRY_DATE = 'SEM_EXPIRY_DATE'
    COL_SEM_INSTRUMENT_NAME = 'SEM_INSTRUMENT_NAME'
    COL_SEM_LOT_UNITS = 'SEM_LOT_UNITS'
    COL_SEM_STRIKE_PRICE = 'SEM_STRIKE_PRICE'
    COL_SEM_OPTION_TYPE = 'SEM_OPTION_TYPE'
    COL_SEM_TICK_SIZE = 'SEM_TICK_SIZE'

    def __init__(self):
        if not os.path.exists('cache'):
            os.makedirs('cache', exist_ok=True)
        self._refresh_master_csv()
        self.df = self._load_csv()

    def _refresh_master_csv(self):
        """
        Refreshes CSV if it wasn't downloaded 'today'.
        This prevents using yesterday's master file during morning trading.
        """
        try:
            should_download = True
            if os.path.exists(self.CSV_FILE):
                # Get file modification time
                mtime = os.path.getmtime(self.CSV_FILE)
                file_date = datetime.fromtimestamp(mtime).date()

                # Compare with current trading date (IST)
                # If file matches today's date, we don't need to download
                if file_date == get_today():
                    should_download = False

            if should_download:
                logger.info('⬇️ Downloading Master Scrip CSV (New Day Detected)...')
                resp = requests.get(self.CSV_URL, timeout=30)
                with open(self.CSV_FILE, 'wb') as f:
                    f.write(resp.content)
                logger.info('✅ Download Complete.')
            else:
                logger.info("✅ Master CSV is fresh (Today's Cache).")

        except Exception as e:
            logger.error(f'CSV Download Error: {e}')

    def _load_csv(self) -> pl.DataFrame:
        try:
            return pl.read_csv(
                self.CSV_FILE, ignore_errors=True, infer_schema_length=10000
            ).with_columns(
                pl.col(self.COL_SEM_EXPIRY_DATE).str.to_date(
                    format='%Y-%m-%d %H:%M:%S', strict=False
                ),
                pl.col(self.COL_SEM_STRIKE_PRICE).cast(pl.Float64, strict=False),
                pl.col(self.COL_SEM_SM_ID).cast(pl.Utf8, strict=False),
            )
        except Exception as e:
            logger.error(f'CSV Load Error: {e}')
            return pl.DataFrame()

    def get_instrument_type(self, security_id: str) -> str | None:
        try:
            row = self.df.filter(pl.col(self.COL_SEM_SM_ID) == str(security_id)).select(
                self.COL_SEM_INSTRUMENT_NAME
            )

            if row.is_empty():
                return None

            inst = row.item()
            if inst in ('OPTIDX', 'OPTSTK'):
                return inst

        except Exception as e:
            logger.warning(f'{e}')

        return None

    def get_security_id(
        self,
        trading_symbol: str,
        price_ref: float = 0.0,
        ltp_fetcher: Optional[Callable[[str], float]] = None,
    ) -> Tuple[Optional[str], Optional[str], int, float]:
        """
        Maps symbol to ID with detailed logging of the decision process.
        """
        if self.df.is_empty():
            return None, None, 0, 0.0

        # FIX: Use get_today() for consistency with IST
        today = get_today()
        logger.info(f"🔍 Mapping: '{trading_symbol}' | Ref Price: {price_ref}")

        # 1. EXACT MATCH
        res = self.df.filter(
            (pl.col(self.COL_SEM_CUSTOM_SYMBOL) == trading_symbol)
            & (pl.col(self.COL_SEM_EXPIRY_DATE) >= today)
        )

        if not res.is_empty():
            row = res.row(0, named=True)
            sid = str(row[self.COL_SEM_SM_ID])
            logger.info(
                f'✅ Exact Match Found: ID {sid} | Symbol: {row[self.COL_SEM_TRADING_SYMBOL]}'
            )
            return (
                sid,
                str(row[self.COL_SEM_EXM_EXCH_ID]),
                int(row[self.COL_SEM_LOT_UNITS] or 1),
                0.0,
            )

        MONTHS = {
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
        }

        # 2. SMART REGEX SEARCH
        try:
            parts = trading_symbol.upper().split()
            strike = None
            opt_type = None
            underlying = None

            for p in parts:
                if re.match(r'^\d+(\.\d+)?$', p):
                    strike = float(p)
                elif p in ['CE', 'CALL']:
                    opt_type = 'CE'
                elif p in ['PE', 'PUT']:
                    opt_type = 'PE'
                elif p not in MONTHS and not re.match(r'^\d+$', p):
                    if not underlying:
                        underlying = p

            logger.info(f'🧩 Regex Parsed: {underlying} | Strike: {strike} | Type: {opt_type}')

            if not strike or not opt_type or not underlying:
                logger.warning(f'❌ Regex failed to extract full details from {trading_symbol}')
                return None, None, 0, 0.0

            smart_res = self.df.filter(
                (
                    pl.col(self.COL_SEM_CUSTOM_SYMBOL).str.contains(
                        rf'\b{underlying}\b', literal=False
                    )
                )
                & (pl.col(self.COL_SEM_OPTION_TYPE) == opt_type)
                & (pl.col(self.COL_SEM_STRIKE_PRICE).round(2) == round(strike, 2))
                & (pl.col(self.COL_SEM_EXPIRY_DATE) >= today)
            ).sort(self.COL_SEM_EXPIRY_DATE)

            if smart_res.is_empty():
                logger.warning(f'No candidates found in CSV for {underlying} {strike} {opt_type}')
                return None, None, 0, 0.0

            logger.info(f'Found {smart_res.height} candidates. Analyzing...')

            # 3. SMART PRICE MATCHING
            final_row = None
            if smart_res.height > 1 and price_ref > 0 and ltp_fetcher:
                best_diff = float('inf')
                candidates = smart_res.head(3)

                for i in range(candidates.height):
                    row = candidates.row(i, named=True)
                    sid = str(row[self.COL_SEM_SM_ID])
                    expiry = row[self.COL_SEM_EXPIRY_DATE]

                    try:
                        live_price = ltp_fetcher(sid)
                    except Exception:
                        live_price = 0.0

                    logger.info(
                        f'   ⚖️ Candidate {i + 1}: Expiry {expiry} | ID {sid} \
                        | Live: {live_price} vs Ref: {price_ref}'
                    )

                    if live_price > 0:
                        diff = abs(live_price - price_ref)
                        if diff < best_diff and diff < (price_ref * 0.20):
                            best_diff = diff
                            final_row = row

                if not final_row:
                    logger.warning(
                        'Price match failed (No price within 20%). Reverting to nearest.'
                    )
                else:
                    logger.info(
                        f'Smart Match Selected: ID {final_row[self.COL_SEM_SM_ID]} (Best Price Fit)'
                    )

            if not final_row:
                final_row = smart_res.row(0, named=True)
                logger.info(
                    f'📍 Default Selection (Nearest Expiry): {final_row[self.COL_SEM_EXPIRY_DATE]}'
                )

            return (
                str(final_row[self.COL_SEM_SM_ID]),
                str(final_row[self.COL_SEM_EXM_EXCH_ID]),
                int(final_row[self.COL_SEM_LOT_UNITS] or 1),
                0.0,
            )

        except Exception as e:
            logger.error(f'Smart Search Error: {e}', exc_info=True)

        return None, None, 0, 0.0

    def get_underlying_future_id(self, symbol: str) -> Tuple[Optional[str], float]:
        if self.df.is_empty():
            return None, 0.0

        underlying = symbol.split()[0].upper()
        # Clean up Indices names if needed
        if underlying == 'BANKNIFTY':
            underlying = 'BANKNIFTY'

        logger.info(f'🔮 Finding Future for {underlying}...')

        try:
            target_expiry = select_expiry_date(underlying)
            target_month = target_expiry.month
            target_year = target_expiry.year
            logger.info(f'   📅 Target Rollover Date: {target_expiry}')

            res = self.df.filter(
                (pl.col(self.COL_SEM_TRADING_SYMBOL).str.starts_with(underlying))
                & (pl.col(self.COL_SEM_INSTRUMENT_NAME).is_in(['FUTIDX', 'FUTSTK']))
            ).sort(self.COL_SEM_EXPIRY_DATE)

            if res.is_empty():
                return None, 0.0

            candidate = None
            target_future = res.filter(
                (pl.col(self.COL_SEM_EXPIRY_DATE).dt.month() == target_month)
                & (pl.col(self.COL_SEM_EXPIRY_DATE).dt.year() == target_year)
            )

            if not target_future.is_empty():
                candidate = target_future.row(0, named=True)
                logger.info(
                    f'Found Target Future: {candidate[self.COL_SEM_TRADING_SYMBOL]} \
                    (ID: {candidate[self.COL_SEM_SM_ID]})'
                )
            else:
                # FIX: Use get_today() here as well
                today = get_today()
                fallback_res = res.filter(pl.col(self.COL_SEM_EXPIRY_DATE) >= today)
                if not fallback_res.is_empty():
                    candidate = fallback_res.row(0, named=True)
                    logger.warning(
                        f'   Target Future not found. Using Nearest: \
                            {candidate[self.COL_SEM_TRADING_SYMBOL]}'
                    )

            if candidate:
                return (
                    str(candidate[self.COL_SEM_SM_ID]),
                    float(candidate[self.COL_SEM_TICK_SIZE] or 0.05),
                )

        except Exception as e:
            logger.error(f'Future Map Error: {e}')

        return None, 0.0
