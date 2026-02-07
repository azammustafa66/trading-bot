"""
Positional Scanner Module

Scans F&O watchlist for Writers Trap signals and executes positional trades
between 2:00-3:25 PM when price confirms above day's high.

Entry Rules:
- PUT Writers Trap → BUY CALL (bullish)
- CALL Writers Trap → BUY PUT (bearish)
- Time Window: 2:00 PM - 3:25 PM
- Max Trades: 3 per day
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional
from zoneinfo import ZoneInfo

import polars as pl

from utils.generate_expiry_dates import select_expiry_date

if TYPE_CHECKING:
    from core.dhan_bridge import DhanBridge
    from core.trade_manager import TradeManager

logger = logging.getLogger('PositionalScanner')
IST = ZoneInfo('Asia/Kolkata')

# Time Window
SCAN_START = time(14, 0)
SCAN_END = time(15, 28)
MAX_TRADES_PER_DAY = 3

WATCHLIST_PATH = Path(__file__).parent.parent / 'cache/fno_watchlist.csv'


class PositionalScanner:
    """
    Scans for Writers Trap signals and executes positional trades at EOD.
    """

    def __init__(self, bridge: DhanBridge, trade_manager: TradeManager, dry_run: bool = False):
        self.bridge = bridge
        self.tm = trade_manager
        self.dry_run = dry_run

        # Trap signals detected during the day
        # {symbol: {'trap_type': 'PUT'|'CALL', 'detected_at': datetime, 'day_high': float}}
        self.trap_signals: Dict[str, Dict] = {}

        # Trades executed today
        self.trades_today: List[str] = []
        self._last_reset_date: Optional[datetime] = None

        # Load watchlist
        self.watchlist = self._load_watchlist()

    def _load_watchlist(self) -> List[Dict]:
        """Load F&O watchlist from CSV."""
        if not WATCHLIST_PATH.exists():
            logger.warning(f'Watchlist not found: {WATCHLIST_PATH}')
            return []

        try:
            df = pl.read_csv(WATCHLIST_PATH)
            watchlist = df.to_dicts()
            logger.info(f'📋 Loaded {len(watchlist)} symbols from watchlist')
            return watchlist
        except Exception as e:
            logger.error(f'Failed to load watchlist: {e}')
            return []

    def _reset_daily_state(self):
        """Reset state at start of new trading day."""
        today = datetime.now(IST).date()
        if self._last_reset_date != today:
            self.trap_signals.clear()
            self.trades_today.clear()
            self._last_reset_date = today
            logger.info('🔄 Daily state reset')

    def register_trap_signal(self, symbol: str, trap_type: str, day_high: float):
        """
        Called by TrapMonitor when a trap is detected.

        Args:
            symbol: Stock/Index symbol (e.g., 'RELIANCE', 'NIFTY')
            trap_type: 'PUT' or 'CALL' (which writers are trapped)
            day_high: Current day's high price
        """
        self._reset_daily_state()

        # Skip if not in watchlist
        if not any(w['SYMBOL'] == symbol for w in self.watchlist):
            return

        # Skip if already have max trades
        if len(self.trades_today) >= MAX_TRADES_PER_DAY:
            return

        # Skip if already have signal for this symbol
        if symbol in self.trap_signals:
            return

        self.trap_signals[symbol] = {'trap_type': trap_type, 'detected_at': datetime.now(IST), 'day_high': day_high}
        logger.info(f'Trap Signal Registered: {symbol} | {trap_type} writers trapped | Day High: {day_high}')

    def _is_scan_window(self) -> bool:
        """Check if current time is within scan window (2 PM - 3:25 PM)."""
        now = datetime.now(IST).time()
        return SCAN_START <= now <= SCAN_END

    def _get_day_high(self, security_id: str) -> Optional[float]:
        """Get day's high price for a security."""
        try:
            # Use OHLC from Dhan API
            ohlc = self.bridge.get_ohlc(security_id)
            if ohlc:
                return ohlc.get('high', 0)
        except Exception as e:
            logger.error(f'Failed to get day high for {security_id}: {e}')
        return None

    def _get_ltp(self, security_id: str) -> float:
        """Get live LTP for a security."""
        return self.bridge.get_live_ltp(security_id)

    async def check_and_execute(self):
        """
        Check registered trap signals and execute if conditions met.
        Called periodically during scan window.
        """
        self._reset_daily_state()

        if not self._is_scan_window():
            return

        if len(self.trades_today) >= MAX_TRADES_PER_DAY:
            logger.info(f'Max trades ({MAX_TRADES_PER_DAY}) reached for today')
            return

        if not self.trap_signals:
            return

        logger.info(f'🔍 Checking {len(self.trap_signals)} trap signals for confirmation...')

        for symbol, signal in list(self.trap_signals.items()):
            if symbol in self.trades_today:
                continue

            if len(self.trades_today) >= MAX_TRADES_PER_DAY:
                break

            trap_type = signal['trap_type']
            stored_day_high = signal['day_high']

            # Get security ID for underlying
            sec_id = self._resolve_security_id(symbol)
            if not sec_id:
                continue

            # Get current LTP
            ltp = self._get_ltp(sec_id)
            if ltp <= 0:
                continue

            # Get current day high (may have updated since trap detection)
            current_day_high = self._get_day_high(sec_id)
            if not current_day_high:
                current_day_high = stored_day_high

            # CONFIRMATION: Price > Day High
            if ltp > current_day_high:
                logger.info(f'✅ CONFIRMED: {symbol} | LTP {ltp} > Day High {current_day_high}')

                # Execute the trade
                await self._execute_positional_trade(symbol, trap_type)
            else:
                logger.debug(f'{symbol}: LTP {ltp} <= Day High {current_day_high}, waiting...')

    def _resolve_security_id(self, symbol: str) -> Optional[str]:
        """Resolve security ID for a symbol from watchlist."""
        for w in self.watchlist:
            if w['SYMBOL'] == symbol:
                return str(w['SECURITY_ID'])
        return None

    async def _execute_positional_trade(self, symbol: str, trap_type: str):
        """
        Execute the positional option trade.

        Args:
            symbol: Underlying symbol
            trap_type: 'PUT' or 'CALL' (which writers are trapped)
        """
        # Determine option type to buy (opposite of trapped writers)
        # PUT writers trapped → Price going UP → BUY CALL
        # CALL writers trapped → Price going DOWN → BUY PUT
        option_type = 'CE' if trap_type == 'PUT' else 'PE'

        try:
            # 1. Get underlying security ID
            sec_id = self._resolve_security_id(symbol)
            if not sec_id:
                logger.error(f'Could not resolve security ID for {symbol}')
                return

            # 2. Get LTP for ATM strike calculation
            ltp = self._get_ltp(sec_id)
            if ltp <= 0:
                logger.error(f'Could not get LTP for {symbol}')
                return

            # 3. Get expiry date using existing utility
            expiry = select_expiry_date(symbol)
            expiry_str = expiry.strftime('%Y-%m-%d')

            # 4. Get ATM strike (round to nearest option strike)
            atm_strike = self._get_atm_strike(symbol, ltp)

            # 5. Build trading symbol
            # Format: "NIFTY 25600 CE" or "RELIANCE 1500 PE"
            trading_symbol = f'{symbol} {atm_strike} {option_type}'

            logger.info(f'🎯 POSITIONAL ENTRY: {trading_symbol} | Trap: {trap_type} writers | Expiry: {expiry_str}')

            if self.dry_run:
                logger.warning(f'[DRY RUN] Would buy: {trading_symbol}')
                self.trades_today.append(symbol)
                del self.trap_signals[symbol]
                return

            # 6. Resolve option security ID
            opt_sec_id, _, _, _ = self.bridge.mapper.get_security_id(trading_symbol, ltp, self.bridge.get_live_ltp)

            if not opt_sec_id:
                logger.error(f'Could not resolve option security ID for {trading_symbol}')
                return

            # 7. Get lot size
            lot_size = self.bridge.mapper.get_lot_size(str(opt_sec_id)) or 1

            # 8. Execute order
            signal = {
                'trading_symbol': trading_symbol,
                'quantity': lot_size,  # 1 lot
                'trigger_above': 0,  # Market order
            }

            result, status = await asyncio.to_thread(self.bridge.execute_super_order, signal)

            if status == 'SUCCESS':
                logger.info(f'✅ POSITIONAL ORDER PLACED: {trading_symbol}')
                self.trades_today.append(symbol)
                del self.trap_signals[symbol]

                # Register with trade manager
                self.tm.add_trade(signal, result, str(opt_sec_id))
            else:
                logger.error(f'❌ Order failed for {trading_symbol}: {result}')

        except Exception as e:
            logger.error(f'Positional trade execution failed for {symbol}: {e}', exc_info=True)

    def _get_atm_strike(self, symbol: str, ltp: float) -> int:
        """
        Get ATM strike for an underlying.
        Rounds to nearest valid strike interval.
        """
        # Strike intervals
        if symbol in ['NIFTY', 'FINNIFTY']:
            interval = 50
        elif symbol in ['BANKNIFTY', 'MIDCPNIFTY']:
            interval = 100
        elif symbol == 'SENSEX':
            interval = 100
        else:
            # Stocks - varies by price
            if ltp > 5000:
                interval = 100
            elif ltp > 1000:
                interval = 50
            elif ltp > 500:
                interval = 25
            else:
                interval = 10

        # Round to nearest interval
        atm = round(ltp / interval) * interval
        return int(atm)
