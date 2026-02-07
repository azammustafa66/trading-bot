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

    async def execute_order_async(self, func, *args):
        """Run blocking order execution in thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    def register_trap_signal(self, symbol: str, trap_type: str, day_high: float, sentiment_score: float = 0.0):
        """
        Register a potential EOD setup from TrapMonitor.
        Args:
            symbol: Underlying symbol (e.g., 'NIFTY')
            trap_type: 'PUT' or 'CALL'
            day_high: Current day high (or spot price at trigger)
            sentiment_score: Global OI Sentiment (Put Δ - Call Δ)
        """
        if not self._is_scan_window():
            logger.info(f'⚠️ Ignoring signal for {symbol} outside scan window')
            return

        # Skip if already have max trades
        if len(self.trades_today) >= MAX_TRADES_PER_DAY:
            return

        # Update if exists
        self.trap_signals[symbol] = {
            'trap_type': trap_type,
            'detected_at': datetime.now(IST),
            'day_high': day_high,
            'sentiment': sentiment_score
        }
        logger.info(f'Trap Signal Registered: {symbol} | {trap_type} (Sent: {sentiment_score}) | Ref: {day_high}')

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

        # --- Index Divergence Check ---
        skip_indices = False
        nifty = self.trap_signals.get('NIFTY')
        banknifty = self.trap_signals.get('BANKNIFTY')

        if nifty and banknifty:
            n_sent = nifty.get('sentiment', 0)
            bn_sent = banknifty.get('sentiment', 0)
            
            # Divergence: Signs are opposite (and both non-zero)
            if (n_sent > 0 and bn_sent < 0) or (n_sent < 0 and bn_sent > 0):
                logger.warning(f'⚠️ Index Divergence: NIFTY ({n_sent}) vs BANKNIFTY ({bn_sent}). Skipping Indices.')
                skip_indices = True

        logger.info(f'🔍 Checking {len(self.trap_signals)} trap signals for confirmation...')

        # Sort signals by Global Sentiment Magnitude (Descending)
        sorted_signals = sorted(
            self.trap_signals.items(),
            key=lambda item: abs(item[1].get('sentiment', 0)),
            reverse=True
        )

        for symbol, signal in sorted_signals:
            if symbol in self.trades_today:
                continue

            if len(self.trades_today) >= MAX_TRADES_PER_DAY:
                break
            
            # Skip if Divergence detected
            if skip_indices and symbol in ['NIFTY', 'BANKNIFTY']:
                logger.debug(f'Skipping {symbol} due to Index Divergence')
                continue

            trap_type = signal['trap_type']
            sentiment = signal.get('sentiment', 0)
            
            # --- DIRECTIONAL FILTER (Global Sentiment Validation) ---
            # Call Trap (Buy CE) -> Requires Bullish Sentiment (Sentiment > 0)
            if trap_type == 'CALL' and sentiment <= 0:
                logger.debug(f'Skipping {symbol}: Call Trap but Sentiment Bearish ({sentiment})')
                continue
                
            # Put Trap (Buy PE) -> Requires Bearish Sentiment (Sentiment < 0)
            if trap_type == 'PUT' and sentiment >= 0:
                logger.debug(f'Skipping {symbol}: Put Trap but Sentiment Bullish ({sentiment})')
                continue
            stored_day_high = signal['day_high']

            # Get security ID for underlying
            sec_id = self._resolve_security_id(symbol)
            if not sec_id:
                continue

            # Get current LTP
            ltp = self._get_ltp(sec_id)
            if ltp <= 0:
                continue

            # Execute immediately based on High OI Change confidence
            logger.info(f'🚀 EXECUTING: {symbol} | Global Sentiment: {signal.get("sentiment", 0)} | LTP: {ltp}')
            await self._execute_positional_trade(symbol, trap_type)

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
        # Determine option type to buy (SAME as trapped writers because they cover = price moves in that direction)
        # PUT writers trapped (Support Broken) → Price going DOWN → BUY PUT
        # CALL writers trapped (Resistance Broken) → Price going UP → BUY CALL
        option_type = 'PE' if trap_type == 'PUT' else 'CE'

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
                'is_positional': True,
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
