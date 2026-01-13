from __future__ import annotations

import asyncio
import logging
import math
import os
import threading
import time
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, List, Tuple

import numpy as np
import requests
import talib
from dotenv import load_dotenv

# Ensure these imports point to your actual file locations
from core.depth_feed import DepthFeed
from core.dhan_mapper import DhanMapper
from core.trade_manager import TradeManager

# --- LOGGING SETUP ---
logger = logging.getLogger('DhanBridge')
load_dotenv()


class DhanBridge:
    RISK_PER_TRADE_INTRA = 0.015
    ATR_PERIOD = 14
    FUNDS_CACHE_TTL = 30
    ATR_INTERVAL_INTRA = 5
    ATR_INTERVAL_POS = 15

    def __init__(self):
        logger.info('Initializing DhanBridge...')
        self.client_id = os.getenv('DHAN_CLIENT_ID', '')
        self.access_token = os.getenv('DHAN_ACCESS_TOKEN', '')
        self.base_url = 'https://api.dhan.co/v2'
        self.kill_switch_triggered = False
        self.session = requests.Session()
        self.mapper = DhanMapper()
        self.trade_manager = TradeManager()
        self._funds_cache: Tuple[float, float] = (0.0, 0.0)
        self._pending_orders: set[str] = set()
        self._pending_lock = Lock()
        self.depth_cache: Dict[str, Dict[str, Any]] = {}
        self.feed_loop = asyncio.new_event_loop()
        self.feed_thread = threading.Thread(target=self._start_feed_thread, daemon=True)
        self._imbalance_log_ts: Dict[str, float] = {}

        if self.access_token and self.client_id:
            self.session.headers.update(
                {
                    'access-token': self.access_token,
                    'client-id': self.client_id,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                }
            )
            try:
                # Initialize Feed
                logger.info('Connecting to Depth Feed...')
                self.feed = DepthFeed(self.access_token, self.client_id)
                self.feed.register_callback(self._on_depth_update)

                # Start the Feed Thread
                self.feed_thread.start()
                logger.info('✅ Dhan Bridge Ready & Feed Thread Started')
            except Exception as e:
                logger.error(f'❌ Failed to init Dhan/Feed: {e}', exc_info=True)
                self.dhan = None
        else:
            logger.critical('⚠️ Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN')
            self.dhan = None

    def _start_feed_thread(self):
        """Internal method to run the async feed in a background thread."""
        asyncio.set_event_loop(self.feed_loop)
        self.feed_loop.run_until_complete(self.feed.connect())

    def subscribe(self, symbols: List[Dict[str, str]]):
        """
        Thread-safe subscription method.
        Usage: bridge.subscribe([{'ExchangeSegment': 'NSE_FNO', 'SecurityId': '45000'}])
        """
        if not self.feed:
            logger.warning('Cannot subscribe: Feed is not initialized.')
            return

        logger.info(f'Sending subscription request for {len(symbols)} symbols...')

        # Schedule the subscribe task on the feed's event loop
        if self.feed_loop.is_running():
            asyncio.run_coroutine_threadsafe(self.feed.subscribe(symbols), self.feed_loop)
        else:
            logger.error('Feed loop is not running. Is the thread started?')

    def unsubscribe_sid(self, sid: str):
        if not self.feed or not self.feed_loop.is_running():
            return

        payload = [{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid}]

        asyncio.run_coroutine_threadsafe(self.feed.unsubscribe(payload), self.feed_loop)

        self.depth_cache.pop(sid, None)

    # --- DATA FEED ---
    def _on_depth_update(self, data: Dict[str, Any]):
        try:
            sid = str(data['security_id'])
            side = str(data.get('side', ''))
            levels = data.get('levels', [])

            if sid not in self.depth_cache:
                self.depth_cache[sid] = {'bid': [], 'ask': [], 'ltp': 0.0}

            self.depth_cache[sid][side] = levels

            bids = self.depth_cache[sid].get('bid')
            asks = self.depth_cache[sid].get('ask')

            if bids and asks:
                ltp = (float(bids[0]['price']) + float(asks[0]['price'])) / 2
                self.depth_cache[sid]['ltp'] = ltp
                # logger.debug(f"Tick {sid}: {ltp}")
        except Exception as e:
            logger.error(f'Error parsing depth update: {e}')

    def reconcile_positions(self):
        """
        Periodically sync local trades with live Dhan positions.
        Cleans stale trades + unsubscribes feeds.
        """
        try:
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()

            if isinstance(resp, list):
                live_positions = resp
            else:
                live_positions = resp.get('data', [])

            # Build live SID → netQty map
            live_map = {
                str(p['securityId']): int(p.get('netQty', 0))
                for p in live_positions
                if int(p.get('netQty', 0)) != 0
            }

            active_sids = self.trade_manager.get_all_sids()

            for sid in active_sids:
                if sid not in live_map:
                    logger.warning(f'🧹 Stale trade detected. Cleaning up SID {sid}')
                    self._cleanup_trade(sid)

        except Exception as e:
            logger.error(f'Reconcile Positions Failed: {e}', exc_info=True)

    def _cleanup_trade(self, sid: str):
        """
        Removes trade + unsubscribes feeds safely.
        """
        try:
            trade = self.trade_manager.get_trade(sid)
            if not trade:
                return

            # 1. Remove trade
            self.trade_manager.remove_trade(sid)

            # 2. Unsubscribe option
            self.unsubscribe_sid(sid)

            # 3. Unsubscribe futures if present
            fut_sid = trade.get('fut_sid')
            if fut_sid:
                self.unsubscribe_sid(fut_sid)

            # 4. Clear cache
            self.depth_cache.pop(sid, None)

            logger.info(f'✅ Cleanup complete for {sid}')

        except Exception as e:
            logger.error(f'Trade Cleanup Failed for {sid}: {e}')

    def get_live_ltp(self, security_id: str) -> float:
        ltp = float(self.depth_cache.get(security_id, {}).get('ltp', 0.0))
        if ltp == 0.0:
            logger.debug(f'Zero LTP returned for {security_id}')
        return ltp

    def get_liquidity_sids(self, sym: str, option_sid: str) -> List[str]:
        sids = [option_sid]
        sym_u = sym.upper()

        if 'BANKNIFTY' in sym_u:
            fut_sid, _ = self.mapper.get_underlying_future_id('BANKNIFTY')
        elif 'NIFTY' in sym_u or 'SENSEX' in sym_u:
            fut_sid, _ = self.mapper.get_underlying_future_id('NIFTY')
        else:
            underlying = sym.split()[0]
            fut_sid, _ = self.mapper.get_underlying_future_id(underlying)

        if fut_sid:
            sids.append(str(fut_sid))
            logger.info(f'Liquidity proxy added: FUT SID {fut_sid}')

        return sids

    def get_order_imbalance(self, security_id: str) -> float:
        if security_id not in self.depth_cache:
            return 1.0

        data = self.depth_cache[security_id]
        bids, asks = data.get('bid', []), data.get('ask', [])

        if not bids or not asks:
            return 1.0

        buy_vol = sum(int(x['qty']) for x in bids[:20])
        sell_vol = sum(int(x['qty']) for x in asks[:20])

        # Anti-spoofing
        if buy_vol > 0 and int(bids[0]['qty']) >= buy_vol * 0.70:
            buy_vol -= int(bids[0]['qty'])
        if sell_vol > 0 and int(asks[0]['qty']) >= sell_vol * 0.70:
            sell_vol -= int(asks[0]['qty'])

        if sell_vol <= 0:
            return 5.0

        imb = round(buy_vol / sell_vol, 2)

        now = time.time()
        last_ts = self._imbalance_log_ts.get(security_id, 0)
        if now - last_ts >= 45:
            logger.info(f'⚖️ Imbalance {security_id}: {imb} (Buy: {buy_vol} | Sell: {sell_vol})')
            self._imbalance_log_ts[security_id] = now

        return imb

    def get_combined_imbalance(self, sids: List[str]) -> float:
        """
        Futures-first imbalance logic.
        - Futures dominate direction
        - Options only confirm or warn
        - Prevents false exits during controlled option selling
        """

        if not sids:
            return 1.0

        # Single instrument (no futures available)
        if len(sids) == 1:
            return self.get_order_imbalance(sids[0])

        option_sid = sids[0]
        fut_sid = sids[1]

        opt_imb = self.get_order_imbalance(option_sid)
        fut_imb = self.get_order_imbalance(fut_sid)

        # --- INSTITUTIONAL LOGIC ---

        # Futures are healthy → IGNORE option selling
        if fut_imb >= 1.0:
            return fut_imb

        # Futures weak + options also weak → REAL danger
        if fut_imb < 0.7 and opt_imb < 0.7:
            return min(fut_imb, opt_imb)

        # Futures weak but options not confirming → warning, not exit
        return fut_imb

    def get_futures_imbalance(self, sym: str, option_sid: str) -> float:
        sids = self.get_liquidity_sids(sym, option_sid)

        if len(sids) > 1:
            return self.get_order_imbalance(sids[1])

        return self.get_order_imbalance(option_sid)

    # --- UTILS ---
    def get_funds(self) -> float:
        # now = time.time()
        # cached, ts = self._funds_cache
        # if now - ts < self.FUNDS_CACHE_TTL:
        #     return cached
        # try:
        #     # LIVE FUNDS
        #     logger.debug('Fetching live funds...')
        #     data = self.session.get(f'{self.base_url}/fundlimit', timeout=5).json()
        #     funds = float(data.get('sodLimit', 0.0))
        #     self._funds_cache = (funds, now)
        #     logger.info(f'Funds Available: {funds}')
        #     return funds
        # except Exception as e:
        #     logger.error(f'Failed to fetch funds: {e}')
        #     return cached
        return 500000.0

    def fetch_atr(self, sec_id: str, segment: str, symbol: str, is_pos: bool) -> float:
        """
        Robust ATR fetcher for options trading.

        Guarantees:
        - Correct instrument type
        - Drops forming candle
        - Handles insufficient data
        - Applies conservative fallbacks
        """
        try:
            sym_u = symbol.upper()

            if any(idx in sym_u for idx in ('NIFTY', 'BANKNIFTY', 'SENSEX')):
                inst_type = 'OPTIDX'
            else:
                inst_type = 'OPTSTK'

            if not inst_type:
                logger.warning(f'ATR: Unknown instrument for {symbol}')
                return self._atr_fallback(symbol)

            interval = '10' if is_pos else '5'

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

            highs = data.get('high', [])
            lows = data.get('low', [])
            closes = data.get('close', [])

            if len(highs) < self.ATR_PERIOD + 1:
                logger.warning(f'ATR: Insufficient candles ({len(highs)}) for {symbol}')
                return self._atr_fallback(symbol)

            highs = np.array(highs, dtype=float)
            lows = np.array(lows, dtype=float)
            closes = np.array(closes, dtype=float)

            atr_series = talib.ATR(highs, lows, closes, timeperiod=self.ATR_PERIOD)

            # Drop last candle (still forming)
            atr_series = atr_series[:-1]

            if len(atr_series) == 0 or np.isnan(atr_series[-1]):
                logger.warning(f'ATR NaN for {symbol}')
                return self._atr_fallback(symbol)

            atr = float(atr_series[-1])

            ltp = self.get_live_ltp(str(sec_id))
            if ltp > 0:
                atr = min(atr, ltp * 0.25)
                atr = max(atr, ltp * 0.01)

            logger.info(f'ATR for {symbol}: {atr:.2f}')
            return atr

        except Exception as e:
            logger.error(f'ATR Fetch Error for {symbol}: {e}', exc_info=True)
            return self._atr_fallback(symbol)

    def _atr_fallback(self, symbol: str) -> float:
        """
        Conservative fallback ATR when data is missing or invalid.
        Designed for options safety.
        """
        # Index options
        sym = symbol.upper()
        if 'BANKNIFTY' in sym:
            return 120.0
        if 'NIFTY' in sym or 'SENSEX' in sym:
            return 60.0

        return 40.0

    # def check_kill_switch(self) -> bool:
    #     if self.kill_switch_triggered:
    #         return True

    #     limit = 8000.0

    #     try:
    #         # 1. Get raw JSON response
    #         resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()

    #         # 2. Handle both response formats:
    #         # Format A: {'status': 'success', 'data': [...]}
    #         # Format B: [...] (List directly)
    #         if isinstance(resp, list):
    #             pos_data = resp
    #         else:
    #             pos_data = resp.get('data', [])

    #         # 3. Calculate PnL safely
    #         pnl = 0.0
    #         for p in pos_data:
    #             # Ensure p is a dict before accessing
    #             if isinstance(p, dict):
    #                 pnl += float(p.get('realizedProfit', 0)) + float(p.get('unrealizedProfit', 0))

    #         # 4. Check Limit
    #         if pnl <= -abs(limit):
    #             self.kill_switch_triggered = True
    #             logger.critical(f'KILL SWITCH TRIGGERED: PnL {pnl} exceeded limit {limit}')
    #             self.square_off_all()
    #             return True

    #     except Exception as e:
    #         logger.error(f'Kill Switch Check Failed: {e}')
    #         # Do not crash the bot, just assume False so trading continues
    #         return False

    #     return False

    def get_super_orders(self) -> List[Dict[str, Any]]:
        try:
            resp = self.session.get(f'{self.base_url}/super/orders', timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f'Fetch Super Orders Failed: {e}')
        return []

    def cancel_super_leg(self, order_id: str, leg: str):
        try:
            url = f'{self.base_url}/super/orders/{order_id}/{leg}'
            resp = self.session.delete(url, timeout=3)

            try:
                data = resp.json()
            except Exception:
                data = {}

            if isinstance(data, list):
                status = ''
            else:
                status = data.get('orderStatus', '')

            if resp.status_code == 202 or status in ('CANCELLED', 'CLOSED', 'TRADED'):
                logger.info(f'{leg} cleaned for Super Order {order_id}')
            else:
                logger.debug(
                    f'Cancel ignored for {leg} | HTTP {resp.status_code} | Status: {status}'
                )
        except Exception as e:
            logger.error(f'Cancel Super Leg Error [{order_id} | {leg}]: {e}')

    def _square_off_position_market(self, security_id: str):
        try:
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            if isinstance(resp, list):
                positions = resp
            else:
                positions = resp.get('data', [])

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
                    logger.warning(f'🔫 Market SqOff executed for {security_id}')
        except Exception as e:
            logger.error(f'Market SqOff Error: {e}')

    def square_off_single(self, security_id: str):
        sid = str(security_id)

        if not self.trade_manager.get_trade(sid):
            logger.info(f'Exit already processed for {sid}')
            return

        try:
            for _ in range(5):
                resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()

                if isinstance(resp, list):
                    positions = resp
                else:
                    positions = resp.get('data', [])

                for p in positions:
                    if str(p.get('securityId')) == sid:
                        qty = abs(int(p.get('netQty', 0)))
                        if qty == 0:
                            break

                        action = 'SELL' if int(p['netQty']) > 0 else 'BUY'

                        payload: Dict[str, (str | int | None)] = {
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

                        logger.critical(f'MARKET EXIT EXECUTED for {sid}')
                        time.sleep(1)
                        break
                else:
                    break
        except Exception as e:
            logger.error(f'Position square-off failed: {e}')

        super_orders = self.get_super_orders()

        for so in super_orders:
            if str(so.get('securityId')) != sid:
                continue

            order_id = so['orderId']
            legs = so.get('legDetails', [])

            for leg in legs:
                if leg.get('orderStatus') == 'PENDING':
                    self.cancel_super_leg(order_id, leg['legName'])

            logger.info(f'Super Order cleaned: {order_id}')
            break

        self.trade_manager.remove_trade(sid)

    def square_off_all(self):
        logger.warning('☢️ GLOBAL SQUARE OFF INITIATED ☢️')

        super_orders = self.get_super_orders()

        for so in super_orders:
            order_id = so.get('orderId', '')
            status = so.get('orderStatus', '')
            sec_id = str(so.get('securityId', ''))

            logger.warning(f'Handling Super Order {order_id} | {sec_id} | Status: {status}')

            if status in ('PENDING', 'PART_TRADED'):
                self.cancel_super_leg(order_id, 'ENTRY_LEG')
                continue

            if status in ('TRADED', 'CLOSED'):
                for leg in so.get('legDetails', []):
                    if leg['legName'] in ('STOP_LOSS_LEG', 'TARGET_LEG'):
                        if leg['orderStatus'] == 'PENDING':
                            self.cancel_super_leg(order_id, leg['legName'])

        time.sleep(0.5)

        try:
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            if isinstance(resp, List):
                positions = resp
            else:
                positions = resp.get('data', [])

            for p in positions:
                sid = str(p.get('securityId'))
                self._square_off_position_market(sid)
                self.trade_manager.remove_trade(sid)

        except Exception as e:
            logger.error(f'Square Off All Failed: {e}')

    def execute_super_order(self, signal: Dict[str, Any]) -> Tuple[float, str]:
        if not self.access_token:
            logger.error('Token missing in execute_super_order')
            return 0.0, 'ERROR'

        # if self.check_kill_switch():
        #     return 0.0, 'KILL_SWITCH'

        sym = signal.get('trading_symbol', '')
        logger.info(f'Processing Signal: {sym} | Type: {signal.get("type")}')

        entry = float(signal.get('trigger_above') or 0.0)
        parsed_sl = float(signal.get('stop_loss') or 0.0)
        parsed_target = float(signal.get('target') or 0.0)
        is_pos = signal.get('is_positional', False)

        # 1. Map Symbol to Security ID
        sec_id, exch, lot, ltp = self.mapper.get_security_id(sym, entry, self.get_live_ltp)
        if not sec_id:
            logger.error(f'Security ID not found for {sym}')
            return 0.0, 'ERROR'

        sid_str = str(sec_id)

        sym_upper = sym.upper()

        if exch == 'MCX':
            exch_seg = 'MCX_COMM'
            has_depth_feed = False
        elif 'SENSEX' in sym_upper or exch == 'BSE':
            exch_seg = 'BSE_FNO'
            has_depth_feed = False
        else:
            exch_seg = 'NSE_FNO'
            has_depth_feed = True

        existing_trade = self.trade_manager.get_trade(sid_str)
        if existing_trade:
            logger.info(f'Duplicate signal ignored for {sym}')
            return 0.0, 'ALREADY_OPEN'

        with self._pending_lock:
            if sid_str in self._pending_orders:
                return 0.0, 'ERROR'
            self._pending_orders.add(sid_str)

        try:
            curr_ltp = self.get_live_ltp(sid_str)

            if curr_ltp == 0:
                logger.info(f'Cold Start: Fetching Price for {sym}...')

                if has_depth_feed:
                    self.subscribe([{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid_str}])
                    for _ in range(10):
                        time.sleep(0.05)
                        curr_ltp = self.get_live_ltp(sid_str)
                        if curr_ltp > 0:
                            logger.info(f'WebSocket Tick Received: {curr_ltp}')
                            break
                else:
                    logger.info(f'Skipping WebSocket wait for {exch_seg} (No Depth Support)')

                # B. API Fallback (Ticker Data)
                if curr_ltp == 0:
                    try:
                        logger.info('Fetching via Ticker API...')
                        ticker_url = f'{self.base_url}/marketfeed/ltp'
                        ticker_payload = {exch_seg: [int(sid_str)]}

                        resp = self.session.post(ticker_url, json=ticker_payload, timeout=2).json()

                        if resp.get('status') == 'success' and 'data' in resp:
                            seg_data = resp['data'].get(exch_seg, {})
                            item_data = seg_data.get(sid_str, {})
                            if 'last_price' in item_data:
                                curr_ltp = float(item_data['last_price'])
                                # Update Cache
                                if sid_str not in self.depth_cache:
                                    self.depth_cache[sid_str] = {}
                                self.depth_cache[sid_str]['ltp'] = curr_ltp
                                logger.info(f'✅ API Price Fetched: {curr_ltp}')
                    except Exception as e:
                        logger.error(f'API Fetch Failed: {e}')

            # C. Final Safety Check
            if curr_ltp == 0:
                if entry > 0:
                    logger.warning(f'No Live Price. Using Signal Entry {entry} as Anchor.')
                    curr_ltp = entry
                else:
                    logger.error(f'Failed to get any price for {sym}. Aborting.')
                    return 0.0, 'ERROR'

            # --- EXECUTION LOGIC ---
            anchor = entry if entry > 0 else curr_ltp
            atr = self.fetch_atr(sid_str, exch_seg, sym, is_pos)

            if atr <= 0:
                trailing_jump = max(round(anchor * 0.05, 1), 1.0)
            else:
                trailing_jump = (
                    max(round(atr * 0.6, 1), 1.0) if not is_pos else max(round(atr * 1.2, 1), 2.0)
                )

            entry_limit = anchor + min(atr * 1.5, anchor * 0.15) if atr > 0 else anchor * 1.10

            if entry and curr_ltp > entry_limit:
                logger.warning(f'Price Too High: {curr_ltp} > Limit {entry_limit:.2f}')
                return curr_ltp, 'PRICE_HIGH'

            if entry and curr_ltp < entry:
                logger.info(f'Price Below Trigger: {curr_ltp} < {entry}')
                return curr_ltp, 'PRICE_LOW'

            # SL/Target Calculation
            if parsed_sl > 0 and parsed_sl < anchor:
                final_sl = parsed_sl
            else:
                if atr > 0:
                    final_sl = anchor - (atr * 0.8) if not is_pos else anchor - (atr * 1)
                else:
                    final_sl = anchor * (0.93 if not is_pos else 0.90)

            final_target = parsed_target if parsed_target > anchor else (anchor * 5.0)

            risk_per_share = max(anchor - final_sl, 1.0)
            risk_amount = self.get_funds() * 0.0125
            qty = math.floor(math.floor(risk_amount / risk_per_share) / lot) * lot
            if qty <= 0:
                qty = lot

            prod_type = 'MARGIN' if is_pos else 'INTRADAY'

            payload = {
                'dhanClientId': self.client_id,
                'transactionType': 'BUY',
                'exchangeSegment': exch_seg,
                'productType': prod_type,
                'orderType': 'MARKET',
                'securityId': sid_str,
                'quantity': qty,
                'price': 0.0,
                'validity': 'DAY',
                'stopLossPrice': round(final_sl, 2),
                'targetPrice': round(final_target, 2),
                'trailingJump': trailing_jump,
            }

            logger.info(f'EXECUTING: {sym} | LTP: {curr_ltp} | Qty: {qty}')

            resp = self.session.post(f'{self.base_url}/super/orders', json=payload, timeout=5)

            # --- RESPONSE PARSING FIX ---
            if resp.status_code in (200, 201):
                raw_data = resp.json()

                # Check 1: Wrapper
                order_data = raw_data.get('data', {})

                # Check 2: Root level
                if not order_data and 'orderId' in raw_data:
                    order_data = raw_data

                liquidity_sids = self.get_liquidity_sids(sym, sid_str)
                fut_sid = liquidity_sids[1] if len(liquidity_sids) > 1 else None

                if order_data.get('orderId'):
                    self.trade_manager.add_trade(signal, order_data, sid_str, fut_sid)
                    return curr_ltp, 'SUCCESS'

            logger.error(f'Dhan API Fail: {resp.text}')
            return curr_ltp, 'ERROR'

        except Exception as e:
            logger.error(f'Execution Exception: {e}', exc_info=True)
            return 0.0, 'ERROR'
        finally:
            with self._pending_lock:
                self._pending_orders.discard(sid_str)
