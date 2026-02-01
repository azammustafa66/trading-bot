"""
Dhan Security Mapper Module.

Maps trading symbols (e.g., "NIFTY 24500 CE") to Dhan security IDs by querying
the daily master CSV. Handles expiry selection, strike matching, and futures lookup.

Typical usage:
    mapper = DhanMapper()
    sid, exchange, lot_size, _ = mapper.get_security_id("NIFTY 24500 CE", price_ref=125.0)
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime
from typing import Callable, Optional, Tuple

import polars as pl
import requests

from utils.generate_expiry_dates import get_today, select_expiry_date

logger = logging.getLogger('DhanMapper')

# Type aliases for clarity
SecurityId = str
ExchangeId = str
LotSize = int
TickSize = float


class DhanMapper:
    """
    Maps trading symbols to Dhan security IDs using the daily master CSV.

    The mapper downloads the master CSV once per trading day and provides
    methods to look up options and futures by symbol, strike, and expiry.

    Attributes:
        CSV_URL: URL to download the Dhan master scrip CSV.
        CSV_FILE: Local cache path for the CSV file.
        df: Polars DataFrame containing the loaded CSV data.
    """

    CSV_URL = 'https://images.dhan.co/api-data/api-scrip-master.csv'
    CSV_FILE = 'cache/dhan_master.csv'

    # Column name constants for type safety and refactoring ease
    COL_SECURITY_ID = 'SEM_SMST_SECURITY_ID'
    COL_EXCHANGE_ID = 'SEM_EXM_EXCH_ID'
    COL_TRADING_SYMBOL = 'SEM_TRADING_SYMBOL'
    COL_CUSTOM_SYMBOL = 'SEM_CUSTOM_SYMBOL'
    COL_EXPIRY_DATE = 'SEM_EXPIRY_DATE'
    COL_INSTRUMENT_NAME = 'SEM_INSTRUMENT_NAME'
    COL_LOT_UNITS = 'SEM_LOT_UNITS'
    COL_STRIKE_PRICE = 'SEM_STRIKE_PRICE'
    COL_OPTION_TYPE = 'SEM_OPTION_TYPE'
    COL_TICK_SIZE = 'SEM_TICK_SIZE'

    # Month abbreviations for symbol parsing
    _MONTHS = frozenset(
        {'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'}
    )

    def __init__(self) -> None:
        """Initialize the mapper and load the master CSV."""
        os.makedirs('cache', exist_ok=True)
        self._refresh_master_csv()
        self.df = self._load_csv()

    def _refresh_master_csv(self) -> None:
        """
        Download the master CSV if stale.

        The CSV is considered fresh if it was modified today (IST).
        This ensures we always have the latest contract data during
        morning trading sessions.
        """
        try:
            if os.path.exists(self.CSV_FILE):
                mtime = os.path.getmtime(self.CSV_FILE)
                file_date = datetime.fromtimestamp(mtime).date()
                if file_date == get_today():
                    logger.info("✅ Master CSV is fresh (today's cache)")
                    return

            logger.info('⬇️ Downloading Master Scrip CSV...')
            resp = requests.get(self.CSV_URL, timeout=60)
            resp.raise_for_status()

            with open(self.CSV_FILE, 'wb') as f:
                f.write(resp.content)
            logger.info('✅ Download complete')

        except requests.RequestException as e:
            logger.error(f'CSV download failed: {e}')
        except OSError as e:
            logger.error(f'CSV file error: {e}')

    def _load_csv(self) -> pl.DataFrame:
        """
        Load and preprocess the master CSV.

        Returns:
            Polars DataFrame with parsed dates and normalized types.
            Returns empty DataFrame on failure.
        """
        try:
            df = pl.read_csv(self.CSV_FILE, ignore_errors=True, infer_schema_length=10000)

            return df.with_columns(
                pl.col(self.COL_EXPIRY_DATE).str.to_date(format='%Y-%m-%d %H:%M:%S', strict=False),
                pl.col(self.COL_STRIKE_PRICE).cast(pl.Float64, strict=False),
                pl.col(self.COL_SECURITY_ID).cast(pl.Utf8, strict=False),
            )

        except Exception as e:
            logger.error(f'CSV load failed: {e}')
            return pl.DataFrame()

    def get_instrument_type(self, security_id: str) -> Optional[str]:
        """
        Get the instrument type for a security ID.

        Args:
            security_id: The Dhan security ID.

        Returns:
            'OPTIDX' for index options, 'OPTSTK' for stock options,
            or None if not found or not an option.
        """
        try:
            row = self.df.filter(pl.col(self.COL_SECURITY_ID) == str(security_id)).select(
                self.COL_INSTRUMENT_NAME
            )

            if row.is_empty():
                return None

            inst = row.item()
            return inst if inst in ('OPTIDX', 'OPTSTK') else None

        except Exception as e:
            logger.warning(f'Instrument type lookup failed: {e}')
            return None

    def get_exchange_segment(self, security_id: str) -> Optional[str]:
        """
        Get the exchange segment for a security ID.

        Args:
            security_id: The Dhan security ID.

        Returns:
            Exchange segment string (e.g., 'NSE_FNO', 'BSE_FNO') or None.
        """
        try:
            row = self.df.filter(pl.col(self.COL_SECURITY_ID) == str(security_id)).select(
                self.COL_EXCHANGE_ID
            )

            if row.is_empty():
                return None

            exch_id = str(row.item()).strip().upper()

            # Map exchange ID to segment
            if exch_id in ('NSE', 'NFO'):
                return 'NSE_FNO'
            elif exch_id in ('BSE', 'BFO'):
                return 'BSE_FNO'
            elif exch_id == 'NSE_EQ':
                return 'NSE_EQ'
            elif exch_id == 'BSE_EQ':
                return 'BSE_EQ'
            else:
                return None

        except Exception as e:
            logger.debug(f'Exchange segment lookup failed: {e}')
            return None

    def get_security_id(
        self,
        trading_symbol: str,
        price_ref: float = 0.0,
        ltp_fetcher: Optional[Callable[[str], float]] = None,
    ) -> Tuple[Optional[SecurityId], Optional[ExchangeId], LotSize, TickSize]:
        """
        Map a trading symbol to its Dhan security ID.

        Performs a multi-step lookup:
        1. Exact match on custom symbol
        2. Regex parsing for underlying/strike/type
        3. Price-based disambiguation for multiple expiries

        Args:
            trading_symbol: Symbol like "NIFTY 24500 CE" or "BANKNIFTY 52000 PE".
            price_ref: Reference price for smart expiry selection (optional).
            ltp_fetcher: Callback to fetch live prices for disambiguation.

        Returns:
            Tuple of (security_id, exchange_id, lot_size, tick_size).
            Returns (None, None, 0, 0.0) if not found.

        Example:
            >>> sid, exch, lot, tick = mapper.get_security_id("NIFTY 24500 CE", 125.0)
            >>> print(f"Security ID: {sid}, Lot Size: {lot}")
        """
        if self.df.is_empty():
            return None, None, 0, 0.0

        today = get_today()
        symbol_upper = trading_symbol.upper().strip()
        logger.info(f"🔍 Mapping: '{symbol_upper}' | Ref Price: {price_ref}")

        # Step 1: Try exact match first (fastest path)
        exact_match = self._find_exact_match(symbol_upper, today)
        if exact_match:
            return exact_match

        # Step 2: Parse symbol components and search
        parsed = self._parse_trading_symbol(symbol_upper)
        if not parsed:
            return None, None, 0, 0.0

        underlying, strike, opt_type, target_month = parsed

        # Step 3: Find candidates matching parsed criteria
        candidates = self._find_candidates(underlying, strike, opt_type, today, target_month)
        if candidates.is_empty():
            logger.warning(f'No candidates found for {underlying} {strike} {opt_type}')
            return None, None, 0, 0.0

        # Step 4: Select best candidate (by price or nearest expiry)
        best_row = self._select_best_candidate(candidates, price_ref, ltp_fetcher)

        return (
            str(best_row[self.COL_SECURITY_ID]),
            str(best_row[self.COL_EXCHANGE_ID]),
            int(best_row[self.COL_LOT_UNITS] or 1),
            0.0,
        )

    def _find_exact_match(
        self, symbol: str, today: date
    ) -> Optional[Tuple[SecurityId, ExchangeId, LotSize, TickSize]]:
        """Find an exact match for the trading symbol."""
        result = self.df.filter(
            (pl.col(self.COL_CUSTOM_SYMBOL) == symbol) & (pl.col(self.COL_EXPIRY_DATE) >= today)
        )

        if result.is_empty():
            return None

        row = result.row(0, named=True)
        sid = str(row[self.COL_SECURITY_ID])
        logger.info(f'✅ Exact match: ID {sid}')

        return (sid, str(row[self.COL_EXCHANGE_ID]), int(row[self.COL_LOT_UNITS] or 1), 0.0)

    def _parse_trading_symbol(self, symbol: str) -> Optional[Tuple[str, float, str, Optional[str]]]:
        """
        Parse a trading symbol into components.

        Args:
            symbol: Uppercase trading symbol.

        Returns:
            Tuple of (underlying, strike, option_type, target_month).
        """
        parts = symbol.split()
        strike: Optional[float] = None
        opt_type: Optional[str] = None
        underlying: Optional[str] = None
        target_month: Optional[str] = None

        for part in parts:
            if re.match(r'^\d+(\.\d+)?$', part):
                strike = float(part)
            elif part in ('CE', 'CALL'):
                opt_type = 'CE'
            elif part in ('PE', 'PUT'):
                opt_type = 'PE'
            elif part in self._MONTHS:
                target_month = part
            elif not re.match(r'^\d+$', part):
                if not underlying:
                    underlying = part

        if not all([strike, opt_type, underlying]):
            logger.warning(f'❌ Failed to parse: {symbol}')
            return None

        logger.info(
            f'🧩 Parsed: {underlying} | Strike: {strike} | Type: {opt_type} | Month: {target_month}'
        )
        return underlying, strike, opt_type, target_month  # type: ignore

    def _find_candidates(
        self,
        underlying: str,
        strike: float,
        opt_type: str,
        today: date,
        target_month: Optional[str] = None,
    ) -> pl.DataFrame:
        """Find all option contracts matching the criteria."""
        candidates = self.df.filter(
            pl.col(self.COL_CUSTOM_SYMBOL).str.contains(rf'\b{underlying}\b', literal=False)
            & (pl.col(self.COL_OPTION_TYPE) == opt_type)
            & (pl.col(self.COL_STRIKE_PRICE).round(2) == round(strike, 2))
            & (pl.col(self.COL_EXPIRY_DATE) >= today)
        )

        if target_month:
            # Map month name (FEB) to month number, fallback to ignore if unknown
            # Standard month map
            m_map = {
                'JAN': 1,
                'FEB': 2,
                'MAR': 3,
                'APR': 4,
                'MAY': 5,
                'JUN': 6,
                'JUL': 7,
                'AUG': 8,
                'SEP': 9,
                'OCT': 10,
                'NOV': 11,
                'DEC': 12,
            }
            month_num = m_map.get(target_month)
            if month_num:
                candidates = candidates.filter(pl.col(self.COL_EXPIRY_DATE).dt.month() == month_num)

        return candidates.sort(self.COL_EXPIRY_DATE)

    def _select_best_candidate(
        self,
        candidates: pl.DataFrame,
        price_ref: float,
        ltp_fetcher: Optional[Callable[[str], float]],
    ) -> dict:
        """
        Select the best candidate from multiple matches.

        Uses price matching if reference price and LTP fetcher are available,
        otherwise falls back to nearest expiry.
        """
        logger.info(f'Found {candidates.height} candidates')

        # Try price-based selection if we have tools for it
        if candidates.height > 1 and price_ref > 0 and ltp_fetcher:
            best_row = self._match_by_price(candidates, price_ref, ltp_fetcher)
            if best_row:
                return best_row

        # Default: nearest expiry
        row = candidates.row(0, named=True)
        logger.info(f'📍 Selected nearest expiry: {row[self.COL_EXPIRY_DATE]}')
        return row

    def _match_by_price(
        self, candidates: pl.DataFrame, price_ref: float, ltp_fetcher: Callable[[str], float]
    ) -> Optional[dict]:
        """Match candidates by comparing live prices to reference."""
        best_diff = float('inf')
        best_row = None
        max_deviation = price_ref * 0.20  # 20% tolerance

        for i in range(min(3, candidates.height)):
            row = candidates.row(i, named=True)
            sid = str(row[self.COL_SECURITY_ID])

            try:
                live_price = ltp_fetcher(sid)
            except Exception:
                live_price = 0.0

            logger.info(
                f'   ⚖️ Candidate {i + 1}: Expiry {row[self.COL_EXPIRY_DATE]} | '
                f'Live: {live_price} vs Ref: {price_ref}'
            )

            if live_price > 0:
                diff = abs(live_price - price_ref)
                if diff < best_diff and diff < max_deviation:
                    best_diff = diff
                    best_row = row

        if best_row:
            logger.info(f'✅ Price match: ID {best_row[self.COL_SECURITY_ID]}')

        return best_row

    def get_underlying_future_id(self, symbol: str) -> Tuple[Optional[SecurityId], TickSize]:
        """
        Find the appropriate futures contract for an underlying.

        Uses smart expiry selection to handle monthly rollovers correctly.

        Args:
            symbol: Underlying symbol (e.g., "NIFTY", "BANKNIFTY", "RELIANCE").

        Returns:
            Tuple of (security_id, tick_size) or (None, 0.0) if not found.
        """
        if self.df.is_empty():
            return None, 0.0

        underlying = symbol.split()[0].upper()
        logger.info(f'🔮 Finding future for: {underlying}')

        try:
            target_expiry = select_expiry_date(underlying)
            logger.info(f'   📅 Target expiry: {target_expiry}')

            # Filter to matching futures
            futures = self.df.filter(
                pl.col(self.COL_TRADING_SYMBOL).str.starts_with(underlying)
                & pl.col(self.COL_INSTRUMENT_NAME).is_in(['FUTIDX', 'FUTSTK'])
            ).sort(self.COL_EXPIRY_DATE)

            if futures.is_empty():
                return None, 0.0

            # Try to find exact month match
            candidate = self._find_target_month_future(
                futures, target_expiry.month, target_expiry.year
            )

            # Fallback to nearest available future
            if not candidate:
                candidate = self._find_nearest_future(futures, get_today())

            if candidate:
                return (
                    str(candidate[self.COL_SECURITY_ID]),
                    float(candidate[self.COL_TICK_SIZE] or 0.05),
                )

        except Exception as e:
            logger.error(f'Future lookup failed: {e}')

        return None, 0.0

    def _find_target_month_future(
        self, futures: pl.DataFrame, month: int, year: int
    ) -> Optional[dict]:
        """Find future expiring in the target month."""
        result = futures.filter(
            (pl.col(self.COL_EXPIRY_DATE).dt.month() == month)
            & (pl.col(self.COL_EXPIRY_DATE).dt.year() == year)
        )

        if result.is_empty():
            return None

        row = result.row(0, named=True)
        logger.info(f'✅ Found target future: {row[self.COL_TRADING_SYMBOL]}')
        return row

    def _find_nearest_future(self, futures: pl.DataFrame, today: date) -> Optional[dict]:
        """Find the nearest non-expired future."""
        result = futures.filter(pl.col(self.COL_EXPIRY_DATE) >= today)

        if result.is_empty():
            return None

        row = result.row(0, named=True)
        logger.warning(f'⚠️ Using nearest future: {row[self.COL_TRADING_SYMBOL]}')
        return row
