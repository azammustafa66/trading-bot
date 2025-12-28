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
from dhanhq import dhanhq
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

    def __init__(self):
        logger.info('Initializing DhanBridge...')
        self.client_id = os.getenv('DHAN_CLIENT_ID')
        self.access_token = os.getenv('DHAN_ACCESS_TOKEN')
        self.base_url = 'https://api.dhan.co/v2'
        self.kill_switch_triggered = False
        self.session = requests.Session()

        # Helper classes
        self.mapper = DhanMapper()
        self.trade_manager = TradeManager()

        # State management
        self._funds_cache: Tuple[float, float] = (0.0, 0.0)
        self._pending_orders: set[str] = set()
        self._pending_lock = Lock()
        self.depth_cache: Dict[str, Dict[str, Any]] = {}

        # Async Feed Management
        self.feed_loop = asyncio.new_event_loop()
        self.feed_thread = threading.Thread(target=self._start_feed_thread, daemon=True)

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
                self.dhan = dhanhq(self.client_id, self.access_token)

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
                # Simple Mid-Price calculation for LTP
                ltp = (float(bids[0]['price']) + float(asks[0]['price'])) / 2
                self.depth_cache[sid]['ltp'] = ltp
                # logger.debug(f"Tick {sid}: {ltp}") # Uncomment if you want spammy logs
        except Exception as e:
            logger.error(f'Error parsing depth update: {e}')

    def get_live_ltp(self, security_id: str) -> float:
        ltp = float(self.depth_cache.get(security_id, {}).get('ltp', 0.0))
        if ltp == 0.0:
            logger.debug(f'⚠️ Zero LTP returned for {security_id}')
        return ltp

    def get_order_imbalance(self, security_id: str) -> float:
        if security_id not in self.depth_cache:
            return 1.0

        data = self.depth_cache[security_id]
        bids, asks = data.get('bid', []), data.get('ask', [])

        if not bids or not asks:
            return 1.0

        buy_vol = sum(int(x['qty']) for x in bids[:10])
        sell_vol = sum(int(x['qty']) for x in asks[:10])

        # Anti-Spoofing: Reduce weight of top level if it's too large (>60% of total)
        if buy_vol > 0 and int(bids[0]['qty']) > (buy_vol * 0.60):
            buy_vol -= int(bids[0]['qty'])
        if sell_vol > 0 and int(asks[0]['qty']) > (sell_vol * 0.60):
            sell_vol -= int(asks[0]['qty'])

        if sell_vol <= 0:
            return 5.0

        imb = round(buy_vol / sell_vol, 2)
        logger.info(f'⚖️ Imbalance {security_id}: {imb} (Buy: {buy_vol} | Sell: {sell_vol})')
        return imb

    # --- UTILS ---
    def get_funds(self) -> float:
        now = time.time()
        cached, ts = self._funds_cache
        if now - ts < self.FUNDS_CACHE_TTL:
            return cached
        try:
            # LIVE FUNDS
            logger.debug('Fetching live funds...')
            data = self.session.get(f'{self.base_url}/fundlimit', timeout=5).json()
            funds = float(data.get('sodLimit', 0.0))

            self._funds_cache = (funds, now)
            logger.info(f'💰 Funds Available: {funds}')
            return funds
        except Exception as e:
            logger.error(f'Failed to fetch funds: {e}')
            return cached

    def fetch_atr(self, sec_id: str, segment: str, symbol: str) -> float:
        try:
            is_index = any(x in symbol.upper() for x in ['NIFTY', 'BANKNIFTY', 'SENSEX'])
            inst_type = 'OPTIDX' if is_index else 'OPTSTK'
            to_date = datetime.now()
            from_date = to_date - timedelta(days=5)

            payload = {
                'securityId': str(sec_id),
                'exchangeSegment': segment,
                'instrument': inst_type,
                'interval': self.ATR_INTERVAL_INTRA,
                'fromDate': from_date.strftime('%Y-%m-%d'),
                'toDate': to_date.strftime('%Y-%m-%d'),
            }
            resp = self.session.post(f'{self.base_url}/charts/intraday', json=payload, timeout=4)
            data = resp.json()

            if 'high' in data and len(data['high']) > self.ATR_PERIOD:
                highs = np.array(data['high'], dtype=float)
                lows = np.array(data['low'], dtype=float)
                closes = np.array(data['close'], dtype=float)
                atr_vals = talib.ATR(highs, lows, closes, timeperiod=self.ATR_PERIOD)
                val = float(atr_vals[-1]) if not np.isnan(atr_vals[-1]) else 0.0
                logger.info(f'📊 ATR for {symbol}: {val:.2f}')
                return val
            else:
                logger.warning(f'⚠️ Insufficient ATR data for {symbol}')
        except Exception as e:
            logger.error(f'ATR Fetch Error for {symbol}: {e}')
            return 0.0
        return 0.0

    def check_kill_switch(self) -> bool:
        if self.kill_switch_triggered:
            return True

        limit = float(os.getenv('LOSS_LIMIT', '8000.0'))

        try:
            # 1. Get raw JSON response
            resp = self.session.get(f'{self.base_url}/positions', timeout=5).json()

            # 2. Handle both response formats:
            # Format A: {'status': 'success', 'data': [...]}
            # Format B: [...] (List directly)
            if isinstance(resp, list):
                pos_data = resp
            else:
                pos_data = resp.get('data', [])

            # 3. Calculate PnL safely
            pnl = 0.0
            for p in pos_data:
                # Ensure p is a dict before accessing
                if isinstance(p, dict):
                    pnl += float(p.get('realizedProfit', 0)) + float(p.get('unrealizedProfit', 0))

            # 4. Check Limit
            if pnl <= -abs(limit):
                self.kill_switch_triggered = True
                logger.critical(f'🚨 KILL SWITCH TRIGGERED: PnL {pnl} exceeded limit {limit}')
                self.square_off_all()
                return True

        except Exception as e:
            logger.error(f'Kill Switch Check Failed: {e}')
            # Do not crash the bot, just assume False so trading continues
            return False

        return False

    def square_off_single(self, security_id: str):
        target = str(security_id)
        found = False
        try:
            positions = self.session.get(f'{self.base_url}/positions').json().get('data', [])
            for p in positions:
                if str(p['securityId']) == target:
                    qty = abs(int(p.get('netQty', 0)))
                    if qty == 0:
                        continue
                    found = True
                    action = 'SELL' if int(p.get('netQty', 0)) > 0 else 'BUY'

                    logger.warning(f'🔫 Exiting Position: {p["tradingSymbol"]} Qty: {qty}')

                    self.session.post(
                        f'{self.base_url}/orders',
                        json={
                            'dhanClientId': self.client_id,
                            'transactionType': action,
                            'exchangeSegment': p['exchangeSegment'],
                            'productType': p['productType'],
                            'orderType': 'MARKET',
                            'securityId': target,
                            'quantity': qty,
                            'validity': 'DAY',
                        },
                    )
                    break

            if found:
                self.trade_manager.remove_trade(target)
            else:
                logger.info(
                    f'ℹ️ Position {target} not found in open positions. Removing from manager.'
                )
                self.trade_manager.remove_trade(target)

        except Exception as e:
            logger.error(f'Single SqOff Error for {target}: {e}')

    def square_off_all(self):
        logger.warning('☢️ SQUARING OFF ALL POSITIONS ☢️')
        try:
            positions = self.session.get(f'{self.base_url}/positions').json().get('data', [])
            for p in positions:
                self.square_off_single(str(p['securityId']))
        except Exception as e:
            logger.error(f'Square Off All Failed: {e}')

    # --- CORE EXECUTION ---
    def execute_super_order(self, signal: Dict[str, Any]) -> Tuple[float, str]:
        if not self.access_token:
            logger.error('Token missing in execute_super_order')
            return 0.0, 'ERROR'

        if self.check_kill_switch():
            return 0.0, 'KILL_SWITCH'

        sym = signal.get('trading_symbol', '')
        logger.info(f'🔔 Processing Signal: {sym} | Type: {signal.get("type")}')

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

        # 2. Duplicate Check
        existing_trade = self.trade_manager.get_trade(sid_str)
        if existing_trade:
            logger.info(f'🚫 Duplicate signal ignored for {sym}')
            return 0.0, 'ALREADY_OPEN'

        with self._pending_lock:
            if sid_str in self._pending_orders:
                return 0.0, 'ERROR'
            self._pending_orders.add(sid_str)

        try:
            # --- INTELLIGENT PRICE FETCHING ---
            curr_ltp = self.get_live_ltp(sid_str)

            if curr_ltp == 0:
                logger.info(f'Cold Start: Fetching Price for {sym}...')

                if has_depth_feed:
                    self.subscribe([{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid_str}])
                    for _ in range(10):
                        time.sleep(0.05)
                        curr_ltp = self.get_live_ltp(sid_str)
                        if curr_ltp > 0:
                            logger.info(f'✅ WebSocket Tick Received: {curr_ltp}')
                            break
                else:
                    logger.info(f'ℹ️ Skipping WebSocket wait for {exch_seg} (No Depth Support)')

                # B. API Fallback (Ticker Data)
                # If WS failed OR if we skipped it because it's BSE/MCX
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
            atr = self.fetch_atr(sid_str, exch_seg, sym)

            entry_limit = anchor + min(atr * 1.5, anchor * 0.15) if atr > 0 else anchor * 1.10
            # Price Logic Checks
            if entry and curr_ltp > entry_limit:
                logger.warning(f'Price Too High: {curr_ltp} > Limit {entry_limit:.2f}')
                return curr_ltp, 'PRICE_HIGH'

            if entry and curr_ltp < entry:
                logger.info(f'Price Below Trigger: {curr_ltp} < {entry}')
                return curr_ltp, 'PRICE_LOW'

            # SL/Target Calculation
            final_sl = (
                parsed_sl
                if (parsed_sl > 0 and parsed_sl < anchor)
                else (anchor - max(atr * 2.0, anchor * 0.06, 15) if atr > 0 else anchor * 0.90)
            )

            final_target = (
                parsed_target
                if parsed_target > anchor
                else (anchor + min((anchor - final_sl) * 3, anchor * 0.1))
            )

            risk_per_share = max(anchor - final_sl, 1.0)
            risk_amount = self.get_funds() * 0.02
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
                'quantity': int(qty),
                'price': 0.0,
                'validity': 'DAY',
                'stopLossPrice': round(final_sl, 2),
                'targetPrice': round(final_target, 2),
                'trailingJump': max(round(anchor * 0.05, 1), 1.0),
            }

            logger.info(f'🚀 EXECUTING: {sym} | LTP: {curr_ltp} | Qty: {qty}')

            resp = self.session.post(f'{self.base_url}/super/orders', json=payload, timeout=5)

            if resp.status_code in (200, 201):
                od = resp.json().get('data', {})
                if od.get('orderId'):
                    self.trade_manager.add_trade(signal, od, sid_str)
                    return curr_ltp, 'SUCCESS'

            logger.error(f'Dhan API Fail: {resp.text}')
            return curr_ltp, 'ERROR'

        except Exception as e:
            logger.error(f'Execution Exception: {e}', exc_info=True)
            return 0.0, 'ERROR'
        finally:
            with self._pending_lock:
                self._pending_orders.discard(sid_str)
