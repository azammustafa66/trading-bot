"""
Dhan Bridge Module.

Provides the core trading interface to Dhan's API, including order execution,
position management, real-time data feeds, and risk management.

This module handles:
- Super order execution with dynamic stop-loss and targets
- Real-time depth feed via WebSocket
- Order book imbalance calculations
- Position reconciliation
- Kill switch for risk management
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import threading
import time
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import requests
import talib
from dotenv import load_dotenv

from core.depth_feed import DepthFeed
from core.dhan_mapper import DhanMapper
from core.trade_manager import TradeManager

logger = logging.getLogger('DhanBridge')
load_dotenv()

# Type aliases
SecurityId = str
OrderStatus = str


class DhanBridge:
    """
    Bridge between trading signals and Dhan's trading API.

    Provides methods for order execution, position monitoring, and risk
    management. Maintains a real-time depth feed for order book analysis.

    Attributes:
        RISK_PER_TRADE_INTRA: Fraction of capital to risk per intraday trade.
        ATR_PERIOD: Period for ATR calculation.
        FUNDS_CACHE_TTL: Seconds to cache funds value.
        kill_switch_triggered: Whether the daily loss limit was hit.

    Example:
        >>> bridge = DhanBridge()
        >>> ltp, status = bridge.execute_super_order(signal)
        >>> if status == 'SUCCESS':
        ...     print(f"Order placed at {ltp}")
    """

    # Risk and position sizing constants
    RISK_PER_TRADE_INTRA = 0.0125  # 1.25% of capital per trade
    ATR_PERIOD = 14
    FUNDS_CACHE_TTL = 30  # seconds
    ATR_INTERVAL_INTRA = 5  # minutes
    ATR_INTERVAL_POS = 15  # minutes

    def __init__(self) -> None:
        """Initialize the Dhan bridge with API credentials and data feed."""
        logger.info('Initializing DhanBridge...')

        # API configuration
        self.client_id = os.getenv('DHAN_CLIENT_ID', '')
        self.access_token = os.getenv('DHAN_ACCESS_TOKEN', '')
        self.base_url = 'https://api.dhan.co/v2'

        # State
        self.kill_switch_triggered = False
        self._funds_cache: Tuple[float, float] = (0.0, 0.0)
        self._pending_orders: set[str] = set()
        self._pending_lock = Lock()
        self._imbalance_log_ts: Dict[str, float] = {}

        # HTTP session
        self.session = requests.Session()

        # Components
        self.mapper = DhanMapper()
        self.trade_manager = TradeManager()

        # Depth feed
        self.depth_cache: Dict[str, Dict[str, Any]] = {}
        self.depth_updated = asyncio.Event()
        self.feed: Optional[DepthFeed] = None
        self.feed_loop = asyncio.new_event_loop()
        self.feed_thread = threading.Thread(target=self._run_feed_loop, daemon=True)

        self._initialize_session()

    def _initialize_session(self) -> None:
        """Configure HTTP session and start depth feed."""
        if not self.access_token or not self.client_id:
            logger.critical('⚠️ Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN')
            return

        self.session.headers.update(
            {
                'access-token': self.access_token,
                'client-id': self.client_id,
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }
        )

        try:
            logger.info('Connecting to Depth Feed...')
            self.feed = DepthFeed(self.access_token, self.client_id)
            self.feed.register_callback(self._on_depth_update)
            self.feed_thread.start()
            logger.info('✅ DhanBridge ready, feed thread started')
        except Exception as e:
            logger.error(f'❌ Failed to initialize feed: {e}', exc_info=True)

    def _run_feed_loop(self) -> None:
        """Run the async depth feed in a background thread."""
        asyncio.set_event_loop(self.feed_loop)
        if self.feed:
            self.feed_loop.run_until_complete(self.feed.connect())

    # =========================================================================
    # Subscription Management
    # =========================================================================

    def subscribe(self, symbols: List[Dict[str, str]]) -> None:
        """
        Subscribe to depth data for given instruments.

        Args:
            symbols: List of instrument specs, each with 'ExchangeSegment' and
                     'SecurityId' keys.

        Example:
            >>> bridge.subscribe([
            ...     {'ExchangeSegment': 'NSE_FNO', 'SecurityId': '45000'}
            ... ])
        """
        if not self.feed:
            logger.warning('Cannot subscribe: Feed not initialized')
            return

        logger.info(f'Subscribing to {len(symbols)} symbols...')

        if self.feed_loop.is_running():
            asyncio.run_coroutine_threadsafe(self.feed.subscribe(symbols), self.feed_loop)
        else:
            logger.error('Feed loop not running')

    def unsubscribe_sid(self, sid: str) -> None:
        """
        Unsubscribe from depth data for a security.

        Args:
            sid: Security ID to unsubscribe from.
        """
        if not self.feed or not self.feed_loop.is_running():
            return

        payload = [{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid}]
        asyncio.run_coroutine_threadsafe(self.feed.unsubscribe(payload), self.feed_loop)
        self.depth_cache.pop(sid, None)

    # =========================================================================
    # Depth Feed Processing
    # =========================================================================

    def _on_depth_update(self, data: Dict[str, Any]) -> None:
        """
        Handle incoming depth feed updates.

        Updates the local cache with bid/ask levels and calculates mid-price.
        """
        try:
            sid = str(data['security_id'])
            side = str(data.get('side', ''))
            levels = data.get('levels', [])
            now = time.monotonic()

            if sid not in self.depth_cache:
                self.depth_cache[sid] = {
                    'bid': [],
                    'ask': [],
                    'ltp': 0.0,
                    'bid_ts': now,
                    'ask_ts': now,
                }

            self.depth_cache[sid][side] = levels
            self.depth_cache[sid][f'{side}_ts'] = now

            # Calculate mid-price
            bids = self.depth_cache[sid]['bid']
            asks = self.depth_cache[sid]['ask']
            if bids and asks:
                self.depth_cache[sid]['ltp'] = (bids[0]['price'] + asks[0]['price']) / 2

            # Signal that depth has been updated (for event-driven monitors)
            self.depth_updated.set()

        except Exception as e:
            logger.error(f'Depth update error: {e}', exc_info=True)

    def get_live_ltp(self, security_id: str) -> float:
        """
        Get the last traded price.

        Uses depth cache for NSE instruments. Falls back to broker API
        for BSE instruments (SENSEX) which don't have depth data.

        Args:
            security_id: The security to get price for.

        Returns:
            Current LTP or 0.0 if not available.
        """
        # Try depth cache first
        ltp = float(self.depth_cache.get(security_id, {}).get('ltp', 0.0))
        if ltp > 0:
            return ltp

        # Fallback to broker API for BSE instruments
        return self._fetch_ltp_from_api(security_id)

    # =========================================================================
    # Order Book Imbalance
    # =========================================================================

    def get_order_imbalance(self, security_id: str) -> float:
        """
        Calculate order book imbalance ratio.

        Measures buying vs selling pressure using depth-weighted volume.
        Includes anti-spoofing logic to discount suspiciously large orders.

        Args:
            security_id: Security to analyze.

        Returns:
            Ratio of buy volume to sell volume. Values > 1 indicate buying
            pressure, < 1 indicate selling pressure. Returns 1.0 if data
            is stale or unavailable.
        """
        data = self.depth_cache.get(security_id)
        if not data:
            return 1.0

        bids = data.get('bid')
        asks = data.get('ask')
        if not bids or not asks:
            # Warn periodically if depth is consistently empty
            now = time.monotonic()
            last = self._imbalance_log_ts.get(security_id, 0)
            if now - last >= 10:
                logger.warning(
                    f'⚠️ Empty Depth for {security_id}. \
                    Bids: {len(bids or [])}, \
                    Asks: {len(asks or [])}'
                )
                self._imbalance_log_ts[security_id] = now
            return 1.0

        # FIX: Relaxed time check from 0.25s to 2.0s
        # Check for stale data (bid/ask timestamps too far apart)
        time_diff = abs(data['bid_ts'] - data['ask_ts'])
        if time_diff > 2.0:
            self._log_stale_data_warning(security_id, time_diff)
            return 1.0

        # Sum top 20 levels
        buy_vol = sum(int(x['qty']) for x in bids[:20])
        sell_vol = sum(int(x['qty']) for x in asks[:20])

        if buy_vol <= 0 or sell_vol <= 0:
            return 1.0

        # Anti-spoofing: discount orders that are >70% of total volume
        buy_vol, sell_vol = self._apply_anti_spoofing(bids, asks, buy_vol, sell_vol)

        if sell_vol <= 0:
            return 5.0

        imb = round(buy_vol / sell_vol, 2)
        self._log_imbalance(security_id, imb, buy_vol, sell_vol, time_diff)

        return imb

    def _log_stale_data_warning(self, security_id: str, time_diff: float) -> None:
        """Log warning for stale depth data (rate-limited)."""
        now = time.monotonic()
        last = self._imbalance_log_ts.get(security_id, 0)
        if now - last >= 10:
            logger.warning(f'⚠️ Stale data for {security_id}: lag {time_diff:.3f}s')
            self._imbalance_log_ts[security_id] = now

    def _apply_anti_spoofing(
        self, bids: List[Dict], asks: List[Dict], buy_vol: int, sell_vol: int
    ) -> Tuple[int, int]:
        """Discount suspiciously large orders that may be spoofing."""
        for i in range(min(2, len(bids), len(asks))):
            if int(bids[i]['qty']) >= buy_vol * 0.7:
                buy_vol -= int(bids[i]['qty'])
            if int(asks[i]['qty']) >= sell_vol * 0.7:
                sell_vol -= int(asks[i]['qty'])
        return buy_vol, sell_vol

    def _log_imbalance(
        self, security_id: str, imb: float, buy_vol: int, sell_vol: int, time_diff: float
    ) -> None:
        """Log imbalance calculation (rate-limited to every 5s)."""
        now = time.monotonic()
        last = self._imbalance_log_ts.get(security_id, 0)
        if now - last >= 60:
            logger.info(
                f'⚖️ IMB {security_id} = {imb} | '
                f'Buy={buy_vol} Sell={sell_vol} | Lag={time_diff:.4f}s'
            )
            self._imbalance_log_ts[security_id] = now

    def get_liquidity_sids(self, sym: str, option_sid: str) -> List[str]:
        """
        Get security IDs for liquidity analysis (option + underlying future).

        Args:
            sym: Trading symbol (e.g., "NIFTY 24500 CE").
            option_sid: Security ID of the option.

        Returns:
            List containing [option_sid, futures_sid] if available,
            otherwise just [option_sid].
        """
        sids = [option_sid]
        sym_upper = sym.upper()

        # Determine underlying
        if 'BANKNIFTY' in sym_upper:
            underlying = 'BANKNIFTY'
        elif 'NIFTY' in sym_upper or 'SENSEX' in sym_upper:
            underlying = 'NIFTY'
        else:
            underlying = sym.split()[0]

        fut_sid, _ = self.mapper.get_underlying_future_id(underlying)
        if fut_sid:
            sids.append(str(fut_sid))
            logger.info(f'Liquidity proxy added: FUT {fut_sid}')

        return sids

    def get_combined_imbalance(self, sids: List[str]) -> float:
        """
        Calculate combined imbalance using futures-first logic.

        Institutional approach:
        - Futures order flow dominates direction signaling
        - Options only confirm or provide additional warning
        - Prevents false exits during controlled option selling

        Args:
            sids: List of [option_sid, futures_sid].

        Returns:
            Combined imbalance ratio prioritizing futures signal.
        """
        if not sids:
            return 1.0

        if len(sids) == 1:
            return self.get_order_imbalance(sids[0])

        opt_imb = self.get_order_imbalance(sids[0])
        fut_imb = self.get_order_imbalance(sids[1])

        # logger.info(f'Fut IMB: {fut_imb}, Opt IMB: {opt_imb}')

        # Futures healthy → ignore option selling pressure
        if fut_imb >= 1.0:
            return fut_imb

        # Both weak → real danger
        if fut_imb < 0.7 and opt_imb < 0.7:
            return min(fut_imb, opt_imb)

        # Futures weak but options stable → warning, not exit
        return fut_imb

    # =========================================================================
    # Position Management
    # =========================================================================

    def reconcile_positions(self) -> List[Dict[str, Any]]:
        """
        Sync local trade records with broker positions.
        Returns: List of new manual positions found.
        """
        new_positions = []
        try:
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            positions = resp if isinstance(resp, list) else resp.get('data', [])

            # Build map of live positions with non-zero quantity
            live_map = {str(p['securityId']): p for p in positions if int(p.get('netQty', 0)) != 0}
            live_sids = set(live_map.keys())

            # 1. Clean up stale trades
            for sid in self.trade_manager.get_all_sids():
                if sid not in live_sids:
                    logger.warning(f'🧹 Cleaning stale trade: {sid}')
                    self._cleanup_trade(sid)

            # 2. Identify new manual trades
            for sid, pos in live_map.items():
                if not self.trade_manager.get_trade(sid):
                    new_positions.append(pos)

        except Exception as e:
            logger.error(f'Reconciliation failed: {e}', exc_info=True)

        return new_positions

    def _cleanup_trade(self, sid: str) -> None:
        """
        Remove a trade and clean up associated resources.

        Args:
            sid: Security ID of the trade to clean up.
        """
        try:
            trade = self.trade_manager.get_trade(sid)
            if not trade:
                return

            self.trade_manager.remove_trade(sid)
            self.unsubscribe_sid(sid)

            # Also unsubscribe futures if present
            fut_sid = trade.get('fut_sid')
            if fut_sid:
                self.unsubscribe_sid(fut_sid)

            self.depth_cache.pop(sid, None)
            logger.info(f'✅ Cleanup complete: {sid}')

        except Exception as e:
            logger.error(f'Cleanup failed for {sid}: {e}')

    # =========================================================================
    # Risk Management
    # =========================================================================

    def get_funds(self) -> float:
        """
        Get available trading funds with caching.

        Returns:
            Available funds in INR. Returns cached value if fresh.
        """
        now = time.time()
        cached, ts = self._funds_cache

        if now - ts < self.FUNDS_CACHE_TTL:
            return cached

        try:
            data = self.session.get(f'{self.base_url}/fundlimit', timeout=5).json()
            funds = float(data.get('sodLimit', 0.0))
            self._funds_cache = (funds, now)
            logger.info(f'Funds available: ₹{funds:,.0f}')
            return funds
        except Exception as e:
            logger.error(f'Funds fetch failed: {e}')
            return cached

    def check_kill_switch(self) -> bool:
        """
        Check if daily loss limit has been exceeded.

        If triggered, squares off all positions and blocks further trading.

        Returns:
            True if kill switch is active, False otherwise.
        """
        if self.kill_switch_triggered:
            return True

        limit = 500000 * 0.02  # 2% of 5L = 10K

        try:
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            positions = resp if isinstance(resp, list) else resp.get('data', [])

            pnl = sum(
                float(p.get('realizedProfit', 0)) + float(p.get('unrealizedProfit', 0))
                for p in positions
                if isinstance(p, dict)
            )

            if pnl <= -abs(limit):
                self.kill_switch_triggered = True
                logger.critical(f'🚨 KILL SWITCH: PnL {pnl:.0f} exceeded limit {limit:.0f}')
                self.square_off_all()
                return True

        except Exception as e:
            logger.error(f'Kill switch check failed: {e}')

        return False

    def fetch_atr(self, sec_id: str, segment: str, symbol: str, is_positional: bool) -> float:
        """
        Fetch Average True Range for position sizing.

        Args:
            sec_id: Security ID.
            segment: Exchange segment (NSE_FNO/BSE_FNO).
            symbol: Trading symbol for logging.
            is_positional: Whether this is a positional trade.

        Returns:
            ATR value, or conservative fallback if data unavailable.
        """
        try:
            inst_type = self.mapper.get_instrument_type(sec_id)
            if not inst_type:
                return self._atr_fallback(symbol)

            interval = '10' if is_positional else '5'
            to_date = datetime.now()
            from_date = to_date - timedelta(days=7)

            payload = {
                'securityId': str(sec_id),
                'exchangeSegment': segment,
                'instrument': inst_type,
                'interval': interval,
                'oi': False,
                'fromDate': from_date.strftime('%Y-%m-%d'),
                'toDate': to_date.strftime('%Y-%m-%d'),
            }

            resp = self.session.post(f'{self.base_url}/charts/intraday', json=payload, timeout=10)
            data = resp.json()

            highs = np.array(data.get('high', []), dtype=float)
            lows = np.array(data.get('low', []), dtype=float)
            closes = np.array(data.get('close', []), dtype=float)

            if len(highs) < self.ATR_PERIOD + 1:
                logger.warning(f'ATR: Insufficient data for {symbol}')
                return self._atr_fallback(symbol)

            atr_series = talib.ATR(highs, lows, closes, timeperiod=self.ATR_PERIOD)
            atr_series = atr_series[:-1]  # Drop forming candle

            if len(atr_series) == 0 or np.isnan(atr_series[-1]):
                return self._atr_fallback(symbol)

            atr = float(atr_series[-1])

            # Clamp ATR to reasonable range
            ltp = self.get_live_ltp(str(sec_id))
            if ltp > 0:
                atr = min(atr, ltp * 0.25)
                atr = max(atr, ltp * 0.01)

            logger.info(f'ATR for {symbol}: {atr:.2f}')
            return atr

        except Exception as e:
            logger.error(f'ATR fetch error for {symbol}: {e}')
            return self._atr_fallback(symbol)

    def _atr_fallback(self, symbol: str) -> float:
        """
        Conservative fallback ATR values per instrument type.

        Args:
            symbol: Trading symbol.

        Returns:
            Default ATR value based on underlying.
        """
        sym = symbol.upper()
        if 'BANKNIFTY' in sym:
            return 20.0
        if 'NIFTY' in sym or 'SENSEX' in sym:
            return 10.0
        return 15.0

    # =========================================================================
    # Order Execution
    # =========================================================================

    def get_super_orders(self) -> List[Dict[str, Any]]:
        """
        Fetch all active super orders.

        Returns:
            List of super order dictionaries from Dhan API.
        """
        try:
            resp = self.session.get(f'{self.base_url}/super/orders', timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f'Fetch super orders failed: {e}')
        return []

    def cancel_super_leg(self, order_id: str, leg: str) -> None:
        """
        Cancel a specific leg of a super order.

        Args:
            order_id: The super order ID.
            leg: Leg name ('ENTRY_LEG', 'STOP_LOSS_LEG', 'TARGET_LEG').
        """
        try:
            url = f'{self.base_url}/super/orders/{order_id}/{leg}'
            resp = self.session.delete(url, timeout=3)

            try:
                data = resp.json()
                status = data.get('orderStatus', '') if isinstance(data, dict) else ''
            except Exception:
                status = ''

            if resp.status_code == 202 or status in ('CANCELLED', 'CLOSED', 'TRADED'):
                logger.info(f'{leg} cancelled for order {order_id}')
            else:
                logger.debug(f'Cancel ignored: {leg} | HTTP {resp.status_code}')

        except Exception as e:
            logger.error(f'Cancel leg error [{order_id}/{leg}]: {e}')

    def square_off_single(self, security_id: str) -> None:
        """
        Square off a single position with market order.

        Cancels pending super order legs and exits the position.

        Args:
            security_id: Security to exit.
        """
        sid = str(security_id)

        if not self.trade_manager.get_trade(sid):
            logger.info(f'Exit already processed: {sid}')
            return

        try:
            # Attempt market exit up to 5 times
            for _ in range(5):
                resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()
                positions = resp if isinstance(resp, list) else resp.get('data', [])

                for p in positions:
                    if str(p.get('securityId')) == sid:
                        qty = abs(int(p.get('netQty', 0)))
                        if qty == 0:
                            break

                        action = 'SELL' if int(p['netQty']) > 0 else 'BUY'
                        payload = {
                            'dhanClientId': self.client_id,
                            'transactionType': action,
                            'exchangeSegment': p['exchangeSegment'],
                            'productType': p['productType'],
                            'orderType': 'MARKET',
                            'securityId': sid,
                            'quantity': qty,
                            'validity': 'DAY',
                        }

                        self.session.post(f'{self.base_url}/orders', json=payload, timeout=3)
                        logger.critical(f'MARKET EXIT: {sid}')
                        time.sleep(1)
                        break
                else:
                    break

        except Exception as e:
            logger.error(f'Square off failed: {e}')

        # Clean up super order legs
        self._cancel_super_order_legs(sid)
        self.trade_manager.remove_trade(sid)

    def _cancel_super_order_legs(self, sid: str) -> None:
        """Cancel pending legs of super orders for a security."""
        super_orders = self.get_super_orders()

        for so in super_orders:
            if str(so.get('securityId')) != sid:
                continue

            order_id = so['orderId']
            for leg in so.get('legDetails', []):
                if leg.get('orderStatus') == 'PENDING':
                    self.cancel_super_leg(order_id, leg['legName'])

            logger.info(f'Super order cleaned: {order_id}')
            break

    def square_off_all(self) -> None:
        """
        Emergency exit: Square off ALL positions.

        Called when kill switch triggers or on manual intervention.
        """
        logger.warning('☢️ GLOBAL SQUARE OFF INITIATED ☢️')

        # First cancel all pending super order entries
        super_orders = self.get_super_orders()
        for so in super_orders:
            order_id = so.get('orderId', '')
            status = so.get('orderStatus', '')

            if status in ('PENDING', 'PART_TRADED'):
                self.cancel_super_leg(order_id, 'ENTRY_LEG')
            elif status in ('TRADED', 'CLOSED'):
                for leg in so.get('legDetails', []):
                    if leg['orderStatus'] == 'PENDING':
                        self.cancel_super_leg(order_id, leg['legName'])

        time.sleep(0.5)

        # Then exit all positions
        try:
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            positions = resp if isinstance(resp, list) else resp.get('data', [])

            for p in positions:
                sid = str(p.get('securityId'))
                self._square_off_position_market(sid)
                self.trade_manager.remove_trade(sid)

        except Exception as e:
            logger.error(f'Square off all failed: {e}')

    def _square_off_position_market(self, security_id: str) -> None:
        """Execute market order to close a position."""
        try:
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            positions = resp if isinstance(resp, list) else resp.get('data', [])

            for p in positions:
                if str(p['securityId']) == str(security_id):
                    qty = abs(int(p.get('netQty', 0)))
                    if qty == 0:
                        return

                    action = 'SELL' if int(p['netQty']) > 0 else 'BUY'
                    payload = {
                        'dhanClientId': self.client_id,
                        'transactionType': action,
                        'exchangeSegment': p['exchangeSegment'],
                        'productType': p['productType'],
                        'orderType': 'MARKET',
                        'securityId': security_id,
                        'quantity': qty,
                        'validity': 'DAY',
                    }

                    self.session.post(f'{self.base_url}/orders', json=payload)
                    logger.warning(f'🔫 Market exit: {security_id}')

        except Exception as e:
            logger.error(f'Market square off error: {e}')

    # =========================================================================
    # Super Order Execution
    # =========================================================================

    def execute_super_order(self, signal: Dict[str, Any]) -> Tuple[float, OrderStatus]:
        """
        Execute a super order based on a trading signal.

        Super orders combine entry, stop-loss, and target into a single
        bracket order with trailing stop capability.

        Args:
            signal: Trading signal dictionary containing:
                - trading_symbol: Symbol like "NIFTY 24500 CE"
                - trigger_above: Entry trigger price
                - stop_loss: Stop loss price (optional)
                - target: Target price (optional)
                - is_positional: Whether positional trade

        Returns:
            Tuple of (ltp, status) where status is one of:
            - 'SUCCESS': Order placed successfully
            - 'ERROR': Execution failed
            - 'PRICE_HIGH': Current price too far above trigger
            - 'PRICE_LOW': Current price below trigger (waiting)
            - 'KILL_SWITCH': Trading halted due to losses
            - 'ALREADY_OPEN': Position already exists
        """
        if not self.access_token:
            logger.error('Token missing')
            return 0.0, 'ERROR'

        if self.check_kill_switch():
            return 0.0, 'KILL_SWITCH'

        sym = signal.get('trading_symbol', '')
        logger.info(f'Processing: {sym}')

        # Extract signal parameters
        entry = float(signal.get('trigger_above') or 0.0)
        parsed_sl = float(signal.get('stop_loss') or 0.0)
        parsed_target = float(signal.get('target') or 0.0)
        is_positional = signal.get('is_positional', False)

        # Map symbol to security ID
        sec_id, exch, lot, _ = self.mapper.get_security_id(sym, entry, self.get_live_ltp)
        if not sec_id:
            logger.error(f'Security ID not found: {sym}')
            return 0.0, 'ERROR'

        sid_str = str(sec_id)

        # Determine exchange segment
        exch_seg, has_depth = self._get_exchange_segment(sym, exch)

        # Check for duplicate
        if self.trade_manager.get_trade(sid_str):
            logger.info(f'Duplicate signal ignored: {sym}')
            return 0.0, 'ALREADY_OPEN'

        # Acquire pending lock
        with self._pending_lock:
            if sid_str in self._pending_orders:
                return 0.0, 'ERROR'
            self._pending_orders.add(sid_str)

        try:
            # Get current price
            curr_ltp = self._get_current_price(sid_str, exch_seg, entry, has_depth)
            if curr_ltp == 0:
                return 0.0, 'ERROR'

            # Check price conditions
            anchor = entry if entry > 0 else curr_ltp
            atr = self.fetch_atr(sid_str, exch_seg, sym, is_positional)

            price_status = self._check_price_conditions(curr_ltp, entry, atr, anchor)
            if price_status:
                return curr_ltp, price_status

            # Active Greeks Validation
            if not self.validate_trade_greeks(signal, curr_ltp):
                logger.warning(f'⛔ Trade Rejected by Greeks Validation: {sym}')
                return 0.0, 'GREEKS_REJECT'

            # Calculate order parameters
            # Updated signature: parsed (dict), anchor, atr
            final_sl, final_target, trailing_jump = self._calculate_order_params(
                signal, anchor, atr
            )

            qty = self._calculate_quantity(anchor, final_sl, lot, sid_str)
            prod_type = 'MARGIN' if is_positional else 'INTRADAY'

            # Build and send order
            payload = self._build_super_order_payload(
                sid_str, exch_seg, prod_type, qty, final_sl, final_target, trailing_jump
            )

            logger.info(f'EXECUTING: {sym} | LTP: {curr_ltp} | Qty: {qty}')

            return self._send_super_order(payload, signal, sid_str, sym)

        except Exception as e:
            logger.error(f'Execution error: {e}', exc_info=True)
            return 0.0, 'ERROR'

        finally:
            with self._pending_lock:
                self._pending_orders.discard(sid_str)

    def _get_exchange_segment(self, sym: str, exch: Optional[str]) -> Tuple[str, bool]:
        """Determine exchange segment and depth feed availability."""
        sym_upper = sym.upper()
        if 'SENSEX' in sym_upper or exch == 'BSE':
            return 'BSE_FNO', False
        return 'NSE_FNO', True

    def _get_current_price(self, sid: str, exch_seg: str, entry: float, has_depth: bool) -> float:
        """Get current price via WebSocket or API fallback."""
        curr_ltp = self.get_live_ltp(sid)

        if curr_ltp == 0:
            logger.info('Cold start: fetching price...')

            # Try WebSocket first
            if has_depth:
                self.subscribe([{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid}])
                for _ in range(10):
                    time.sleep(0.05)
                    curr_ltp = self.get_live_ltp(sid)
                    if curr_ltp > 0:
                        break

            # API fallback
            if curr_ltp == 0:
                curr_ltp = self._fetch_ltp_from_api(sid, exch_seg)

            # Use signal entry as last resort
            if curr_ltp == 0 and entry > 0:
                logger.warning(f'Using signal entry as anchor: {entry}')
                curr_ltp = entry

        return curr_ltp

    def _fetch_ltp_from_api(self, sid: str, exch_seg: str = None) -> float:
        """Fetch LTP via Dhan ticker API."""
        try:
            if not exch_seg:
                exch_seg = self.mapper.get_exchange_segment(sid) or 'NSE_FNO'

            url = f'{self.base_url}/marketfeed/ltp'
            payload = {exch_seg: [int(sid)]}
            resp = self.session.post(url, json=payload, timeout=2).json()

            if resp.get('status') == 'success' and 'data' in resp:
                item = resp['data'].get(exch_seg, {}).get(sid, {})
                ltp = float(item.get('last_price', 0))
                if ltp > 0:
                    # Cache the value
                    if sid not in self.depth_cache:
                        self.depth_cache[sid] = {
                            'bid': [],
                            'ask': [],
                            'ltp': 0.0,
                            'bid_ts': 0,
                            'ask_ts': 0,
                        }
                    self.depth_cache[sid]['ltp'] = ltp
                    logger.info(f'API price: {ltp}')
                return ltp
        except Exception as e:
            logger.error(f'API fetch failed: {e}')
        return 0.0

    def get_vix(self) -> float:
        """
        Fetch India VIX value (cached for 5 minutes).

        Returns:
            VIX value (e.g. 14.5) or 0.0 if fetch fails.
        """
        now = time.monotonic()
        # Return cached VIX if fresh (< 300s)
        if hasattr(self, '_vix_cache') and (now - self._vix_cache_ts < 300):
            return self._vix_cache

        try:
            # India VIX SID 21 (NSE) - fetched via IDX_I Chart API
            # Note: 21 is typically India VIX.
            # We use a 1-minute chart request for today to get the latest close.
            payload = {
                'securityId': '21',
                'exchangeSegment': 'IDX_I',
                'instrument': 'INDEX',
                'interval': '1',
                'fromDate': datetime.now().strftime('%Y-%m-%d'),
                'toDate': datetime.now().strftime('%Y-%m-%d'),
            }
            url = f'{self.base_url}/charts/intraday'
            resp = self.session.post(url, json=payload, timeout=2).json()

            # Chart API sometimes returns data directly, sometimes wrapped in 'data'
            data = resp.get('data', resp)

            if 'close' in data and data['close']:
                closes = data['close']
                if closes:
                    vix = float(closes[-1])
                    self._vix_cache = vix
                    self._vix_cache_ts = now
                    logger.info(f'India VIX fetched: {vix}')
                    return vix
        except Exception as e:
            logger.error(f'VIX fetch failed: {e}')

        return 15.0  # Fallback default

    def fetch_option_chain(
        self, underlying_sid: int, segment: str = 'IDX_I', expiry: str = None
    ) -> dict:
        """
        Fetch Option Chain data including Greeks.

        Args:
            underlying_sid: Security ID of underlying (e.g. 13 for Nifty).
            segment: Segment of underlying (IDX_I, NSE_EQ).
            expiry: Optional Expiry date (YYYY-MM-DD). If None, fetches nearest.

        Returns:
            Dict containing option chain data (strike -> ce/pe -> greeks).
        """
        try:
            # 1. Get Expiry if not provided
            if not expiry:
                url_exp = f'{self.base_url}/optionchain/expirylist'
                pay_exp = {'UnderlyingScrip': underlying_sid, 'UnderlyingSeg': segment}
                resp_exp = self.session.post(url_exp, json=pay_exp, timeout=2).json()
                if resp_exp.get('status') == 'success' and resp_exp.get('data'):
                    expiry = resp_exp['data'][0]  # Nearest
                else:
                    return {}

            # 2. Get Chain
            url_oc = f'{self.base_url}/optionchain'
            pay_oc = {'UnderlyingScrip': underlying_sid, 'UnderlyingSeg': segment, 'Expiry': expiry}
            resp_oc = self.session.post(url_oc, json=pay_oc, timeout=2).json()
            return resp_oc.get('data', {}).get('oc', {})

        except Exception as e:
            logger.error(f'Option Chain fetch failed: {e}')
            return {}

    def _check_price_conditions(
        self, curr_ltp: float, entry: float, atr: float, anchor: float
    ) -> Optional[str]:
        """Check if price conditions (Above/Below) are met."""
        # Simple check for now (only handling BUY for trigger ABOVE)
        if curr_ltp >= anchor:
            return 'ABOVE'
        return None

        if curr_ltp >= anchor:
            return 'ABOVE'
        return None

    def validate_trade_greeks(self, signal: dict, curr_ltp: float) -> bool:
        """
        Validate trade entry based on Greeks and Liquidity.

        Rules:
        1. Delta >= range (0.3 to 0.9). Avoid deep OTM.
        2. Spread <= 2% (Ask-Bid).
        3. If VIX > 25, require Delta > 0.5 (ITM).
        """
        try:
            # Only validate Options
            if not signal.get('strike'):
                return True

            und_sym = signal.get('underlying')
            strike = signal.get('strike')
            otype = signal.get('option_type')

            # Resolve Underlying SID (Simplified map)
            und_sid = 0
            if 'NIFTY' in und_sym:
                und_sid = 13
            elif 'BANK' in und_sym:
                und_sid = 25
            elif 'FIN' in und_sym:
                und_sid = 27
            # Sensex options not fully supported for greeks yet if mapping is complex, skip
            if not und_sid:
                return True

            # Fetch Chain
            oc = self.fetch_option_chain(und_sid)
            if not oc:
                return True  # Fail open if API fails? Or fail closed? Open for now.

            # Find Strike
            target_strike_str = f'{float(strike):.6f}'  # API format often 6 decimals

            # Fuzzy match strike if exact string fails
            details = oc.get(target_strike_str)
            if not details:
                # Try finding closest key
                for k in oc.keys():
                    if abs(float(k) - float(strike)) < 0.1:
                        details = oc[k]
                        break

            if details:
                key = 'ce' if otype == 'CE' else 'pe'
                if key in details:
                    data = details[key]
                    greeks = data.get('greeks', {})
                    delta = abs(float(greeks.get('delta', 0)))

                    # 1. Delta Check
                    if delta < 0.30:
                        logger.warning(f'⛔ Reject: Delta {delta:.2f} < 0.30')
                        return False

                    # 2. VIX Check
                    vix = self.get_vix()
                    if vix > 25 and delta < 0.50:
                        logger.warning(f'⛔ Reject: High VIX ({vix}) requires ITM (Delta > 0.5)')
                        return False

                    # 3. Spread Check
                    bid = float(data.get('top_bid_price', 0))
                    ask = float(data.get('top_ask_price', 0))
                    if bid > 0:
                        spread = (ask - bid) / bid
                        if spread > 0.02:
                            logger.warning(f'⛔ Reject: Spread {spread * 100:.1f}% > 2%')
                            return False

            return True

        except Exception as e:
            logger.error(f'Greeks validation error: {e}')
            return True  # Fail open safely

    def _calculate_order_params(
        self, parsed: dict, anchor: float, atr: float
    ) -> Tuple[float, float, float]:
        """
        Calculate Stop Loss, Target, and Trailing Jump.

        Args:
            parsed: Parsed signal data.
            anchor: Entry price (anchor).
            atr: ATR value for volatility-based sizing.

        Returns:
            Tuple of (sl, target, trailing_jump)
        """
        # Base multiplier
        vix = self.get_vix()
        scale = 1.0

        if vix < 13:
            scale = 0.8  # Tighter SL/Target in low vol
        elif vix > 18:
            scale = 1.2  # Wider SL/Target in high vol

        final_sl = 0.0
        final_target = 0.0
        trailing_jump = 0.0

        # --- STOP LOSS ---
        if parsed.get('stop_loss'):
            final_sl = parsed['stop_loss']
        else:
            # Auto-SL: Default 10% below anchor, scaled by VIX
            sl_dist = (anchor * 0.10) * scale
            final_sl = anchor - sl_dist

        # --- TRAILING JUMP ---
        if parsed.get('is_positional'):
            trailing_jump = 1.0 * scale
        else:
            trailing_jump = 0.5 * scale

        # --- TARGET ---
        target_dist = parsed.get('target', anchor * 10.0) - anchor
        if target_dist > 0:
            final_target = anchor + (target_dist * scale)
        else:
            final_target = anchor * 10.0

        return final_sl, final_target, trailing_jump

    def _calculate_quantity(self, anchor: float, final_sl: float, lot: int, sid: str) -> int:
        """Calculate position size based on risk."""
        risk_per_share = max(anchor - final_sl, 1.0)
        risk_amount = self.get_funds() * self.RISK_PER_TRADE_INTRA
        qty = math.floor(math.floor(risk_amount / risk_per_share) / lot) * lot

        if qty <= 0:
            qty = lot

        return qty

    def _build_super_order_payload(
        self,
        sid: str,
        exch_seg: str,
        prod_type: str,
        qty: int,
        final_sl: float,
        final_target: float,
        trailing_jump: float,
    ) -> Dict[str, Any]:
        """Build the super order request payload."""
        return {
            'dhanClientId': self.client_id,
            'transactionType': 'BUY',
            'exchangeSegment': exch_seg,
            'productType': prod_type,
            'orderType': 'MARKET',
            'securityId': sid,
            'quantity': qty,
            'price': 0.0,
            'validity': 'DAY',
            'stopLossPrice': round(final_sl, 2),
            'targetPrice': round(final_target, 2),
            'trailingJump': trailing_jump,
        }

    def _send_super_order(
        self, payload: Dict[str, Any], signal: Dict[str, Any], sid: str, sym: str
    ) -> Tuple[float, str]:
        """Send super order to Dhan API."""
        resp = self.session.post(f'{self.base_url}/super/orders', json=payload, timeout=5)

        if resp.status_code not in (200, 201):
            logger.error(f'API error: {resp.text}')
            return self.get_live_ltp(sid), 'ERROR'

        raw_data = resp.json()
        order_data = raw_data.get('data', {})
        if not order_data and 'orderId' in raw_data:
            order_data = raw_data

        if order_data.get('orderId'):
            liquidity_sids = self.get_liquidity_sids(sym, sid)
            fut_sid = liquidity_sids[1] if len(liquidity_sids) > 1 else None
            self.trade_manager.add_trade(signal, order_data, sid, fut_sid)
            return self.get_live_ltp(sid), 'SUCCESS'

        logger.error(f'No order ID in response: {raw_data}')
        return self.get_live_ltp(sid), 'ERROR'
