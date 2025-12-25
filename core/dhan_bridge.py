from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, Tuple

import numpy as np
import requests
import talib
from dhanhq import dhanhq
from dotenv import load_dotenv

from core.depth_feed import DepthFeed
from core.dhan_mapper import DhanMapper
from core.trade_manager import TradeManager

logger = logging.getLogger('DhanBridge')
load_dotenv()


class DhanBridge:
    RISK_PER_TRADE_INTRA = 0.015
    ATR_PERIOD = 14
    FUNDS_CACHE_TTL = 30
    ATR_INTERVAL_INTRA = 5

    def __init__(self):
        self.client_id = os.getenv('DHAN_CLIENT_ID')
        self.access_token = os.getenv('DHAN_ACCESS_TOKEN')
        self.base_url = 'https://api.dhan.co/v2'
        self.kill_switch_triggered = False
        self.session = requests.Session()
        self.mapper = DhanMapper()
        self.trade_manager = TradeManager()
        self._funds_cache: Tuple[float, float] = (0.0, 0.0)
        self._pending_orders: set[str] = set()
        self._pending_lock = Lock()
        self.depth_cache: Dict[str, Dict[str, Any]] = {}

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
                self.feed = DepthFeed(self.access_token, self.client_id)
                self.feed.register_callback(self._on_depth_update)
                logger.info('✅ Dhan Bridge Ready')
            except Exception:
                self.dhan = None
        else:
            self.dhan = None

    # --- DATA FEED ---
    def _on_depth_update(self, data: Dict[str, Any]):
        sid = str(data['security_id'])
        side = str(data.get('side', ''))
        levels = data.get('levels', [])
        if sid not in self.depth_cache:
            self.depth_cache[sid] = {'bid': [], 'ask': [], 'ltp': 0.0}
        self.depth_cache[sid][side] = levels

        bids, asks = self.depth_cache[sid].get('bid'), self.depth_cache[sid].get('ask')
        if bids and asks:
            self.depth_cache[sid]['ltp'] = (float(bids[0]['price']) + float(asks[0]['price'])) / 2

    def get_live_ltp(self, security_id: str) -> float:
        return float(self.depth_cache.get(security_id, {}).get('ltp', 0.0))

    def get_order_imbalance(self, security_id: str) -> float:
        """Calculates Imbalance with ANTI-SPOOFING Logic."""
        if security_id not in self.depth_cache:
            return 1.0
        data = self.depth_cache[security_id]
        bids, asks = data.get('bid', []), data.get('ask', [])
        if not bids or not asks:
            return 1.0

        buy_vol = sum(int(x['qty']) for x in bids[:10])
        sell_vol = sum(int(x['qty']) for x in asks[:10])

        # Ignore Fake Walls (>60% volume at top level)
        if buy_vol > 0 and int(bids[0]['qty']) > (buy_vol * 0.60):
            buy_vol -= int(bids[0]['qty'])
        if sell_vol > 0 and int(asks[0]['qty']) > (sell_vol * 0.60):
            sell_vol -= int(asks[0]['qty'])

        if sell_vol == 0:
            return 5.0
        return round(buy_vol / sell_vol, 2)

    # --- UTILS ---
    def get_funds(self) -> float:
        now = time.time()
        cached, ts = self._funds_cache
        if now - ts < self.FUNDS_CACHE_TTL:
            return cached
        try:
            # LIVE FUNDS (Uncomment for Production)
            data = self.session.get(f'{self.base_url}/fundlimit', timeout=5).json()
            funds = float(data.get('sodLimit', 0.0))

            # MOCK FUNDS (Comment out for Production)
            # funds = 150000.0

            self._funds_cache = (funds, now)
            return funds
        except Exception:
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
            data = self.session.post(
                f'{self.base_url}/charts/intraday', json=payload, timeout=4
            ).json()
            if 'high' in data and len(data['high']) > self.ATR_PERIOD:
                highs = np.array(data['high'], dtype=float)
                lows = np.array(data['low'], dtype=float)
                closes = np.array(data['close'], dtype=float)
                atr_vals = talib.ATR(highs, lows, closes, timeperiod=self.ATR_PERIOD)
                return float(atr_vals[-1]) if not np.isnan(atr_vals[-1]) else 0.0
        except Exception:
            return 0.0
        return 0.0

    def check_kill_switch(self) -> bool:
        if self.kill_switch_triggered:
            return True
        limit = float(os.getenv('LOSS_LIMIT', '8000.0'))
        try:
            pos = self.session.get(f'{self.base_url}/positions', timeout=5).json().get('data', [])
            pnl = sum(
                float(p.get('realizedProfit', 0)) + float(p.get('unrealizedProfit', 0)) for p in pos
            )
            if pnl <= -abs(limit):
                self.kill_switch_triggered = True
                logger.critical(f'🚨 KILL SWITCH: {pnl}')
                self.square_off_all()
                return True
        except Exception:
            pass
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
                    logger.warning(f'🔫 Surgical Exit: {p["tradingSymbol"]}')
                    break

            if found:
                self.trade_manager.remove_trade(target)
            else:
                logger.info(f'⚠️ Position {target} not found (SL Hit?). Cleaning JSON.')
                self.trade_manager.remove_trade(target)

        except Exception as e:
            logger.error(f'Single SqOff Error: {e}')

    def square_off_all(self):
        try:
            positions = self.session.get(f'{self.base_url}/positions').json().get('data', [])
            for p in positions:
                self.square_off_single(str(p['securityId']))
        except Exception:
            pass

    # --- CORE EXECUTION ---
    def execute_super_order(self, signal: Dict[str, Any]) -> Tuple[float, str]:
        if not self.access_token:
            return 0.0, 'ERROR'
        if self.check_kill_switch():
            return 0.0, 'KILL_SWITCH'

        sym = signal.get('trading_symbol', '')
        entry = float(signal.get('trigger_above') or 0.0)
        parsed_sl = float(signal.get('stop_loss') or 0.0)
        parsed_target = float(signal.get('target') or 0.0)
        is_pos = signal.get('is_positional', False)

        sec_id, exch, lot, ltp = self.mapper.get_security_id(sym, entry)
        if not sec_id:
            return 0.0, 'ERROR'
        sid_str = str(sec_id)

        # ------------------------------------------------------------------
        # 🛑 RECURSION GUARD (New)
        # Checks if we already have this trade active in our JSON Ledger.
        # This prevents duplicate buys if the Retry Loop fires multiple times.
        # ------------------------------------------------------------------
        existing_trade = self.trade_manager.get_trade(sid_str)
        if existing_trade:
            logger.info(f'🚫 Ignoring duplicate buy signal for {sym} (Trade already active)')
            return 0.0, 'ALREADY_OPEN'

        # Lock to prevent race conditions during rapid retries
        with self._pending_lock:
            if sid_str in self._pending_orders:
                return 0.0, 'ERROR'
            self._pending_orders.add(sid_str)

        try:
            exch_seg = 'MCX_COMM' if exch == 'MCX' else 'NSE_FNO'
            curr_ltp = (self.get_live_ltp(sid_str) if exch != 'MCX' else ltp) or 0.0
            if curr_ltp == 0:
                return 0.0, 'ERROR'

            anchor = entry if entry > 0 else curr_ltp
            if entry and curr_ltp > entry * 1.25:
                return curr_ltp, 'PRICE_HIGH'
            if entry and curr_ltp < entry:
                return curr_ltp, 'PRICE_LOW'

            final_sl = 0.0
            if parsed_sl > 0 and parsed_sl < anchor:
                final_sl = parsed_sl
            else:
                atr = self.fetch_atr(sid_str, exch_seg, sym)
                final_sl = anchor - max(atr * 2.0, anchor * 0.06, 15) if atr > 0 else anchor * 0.90

            final_target = (
                parsed_target
                if parsed_target > anchor
                else anchor + min((anchor - final_sl) * 3, anchor * 0.1)
            )

            risk_per_share = max(anchor - final_sl, 1.0)
            risk_amount = self.get_funds() * 0.02
            qty = math.floor(math.floor(risk_amount / risk_per_share) / lot) * lot
            if qty <= 0:
                return curr_ltp, 'ERROR'

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

            logger.info(
                f'🚀 Order: {sym} ({prod_type}) | Qty: {qty} | Entry: {anchor} | SL: {final_sl}'
            )
            resp = self.session.post(f'{self.base_url}/super/orders', json=payload, timeout=5)

            if resp.status_code in (200, 201):
                od = resp.json().get('data', {})
                if od.get('orderId'):
                    self.trade_manager.add_trade(signal, od, sid_str)
                return curr_ltp, 'SUCCESS'

            logger.error(f'Dhan Fail: {resp.text}')
            return curr_ltp, 'ERROR'
        except Exception as e:
            logger.error(f'Exec Error: {e}')
            return 0.0, 'ERROR'
        finally:
            with self._pending_lock:
                self._pending_orders.discard(sid_str)
