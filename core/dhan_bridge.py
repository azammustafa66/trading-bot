from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import requests
import talib
from dhanhq import dhanhq
from dotenv import load_dotenv

from core.dhan_mapper import DhanMapper

logger = logging.getLogger('DhanBridge')
load_dotenv()


class DhanBridge:
    # --- RISK CONFIG ---
    RISK_PER_TRADE_POSITIONAL = 0.02
    RISK_PER_TRADE_INTRADAY = 0.015
    MIN_SL_POINTS = 5.0

    # --- ATR CONFIG ---
    ATR_PERIOD = 14
    ATR_LOOKBACK_DAYS_POS = 3
    ATR_LOOKBACK_DAYS_INTRA = 3
    ATR_INTERVAL_POS = 15
    ATR_INTERVAL_INTRA = 5

    # --- MULTIPLIERS ---
    SL_MULTIPLIER = 3.0
    TRAIL_MULTIPLIER_POS = 1.5
    TRAIL_MULTIPLIER_INTRA = 1.25
    TARGET_RR_RATIO_POS = 5.0
    TARGET_RR_RATIO_INTRA = 4.0

    def __init__(self):
        self.client_id = os.getenv('DHAN_CLIENT_ID')
        self.access_token = os.getenv('DHAN_ACCESS_TOKEN')
        self.base_url = 'https://api.dhan.co/v2'
        self.kill_switch_triggered = False
        self.session = requests.Session()
        self.mapper = DhanMapper()

        if self.access_token and self.client_id:
            self.session.headers.update(
                {
                    'access-token': self.access_token,
                    'client-id': self.client_id,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            )
            self.dhan = dhanhq(self.client_id, self.access_token)
            logger.info('Dhan Bridge Connected')
        else:
            self.dhan = None
            logger.warning('Running in MOCK mode')

    # ------------------------------------------------------------------
    @staticmethod
    def round_to_tick(price: float, tick: float = 0.05) -> float:
        return round(round(price / tick) * tick, 2)

    # ------------------------------------------------------------------
    def get_funds(self) -> float:
        try:
            data = self.session.get(f'{self.base_url}/fundlimit', timeout=5).json()
            return float(data.get('sodLimit', 0.0))
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    def get_ltp(self, security_id: str, exchange_segment: str) -> Optional[float]:
        try:
            payload = {exchange_segment: [int(security_id)]}
            data = self.session.post(
                f'{self.base_url}/marketfeed/ltp',
                json=payload,
                timeout=5,
            ).json()

            return float(data['data'][exchange_segment][str(security_id)]['last_price'])
        except Exception:
            return None

    # ------------------------------------------------------------------
    def calculate_quantity(
        self, entry: float, sl: float, is_positional: bool, lot_size: int
    ) -> int:
        try:
            funds = self.get_funds()
            risk_pct = (
                self.RISK_PER_TRADE_POSITIONAL if is_positional else self.RISK_PER_TRADE_INTRADAY
            )

            risk_amount = funds * risk_pct
            sl_points = abs(entry - sl)

            if sl_points <= 0.05:
                return 0

            # Calculate max quantity allowed by Risk
            max_qty_by_risk = math.floor(risk_amount / sl_points)

            # Convert to lots
            num_lots = math.floor(max_qty_by_risk / lot_size)

            if num_lots < 1:
                logger.warning(
                    f'Risk Too High: Cap {risk_amount:.2f} < Risk {sl_points * lot_size:.2f}'
                )
                return 0

            return int(num_lots * lot_size)
        except Exception as e:
            logger.error(f'Qty Calc Error: {e}')
            return 0

    # ------------------------------------------------------------------
    def fetch_atr(self, sec_id: str, segment: str, symbol: str, is_positional: bool) -> float:
        try:
            is_index = any(x in symbol.upper() for x in ['NIFTY', 'BANKNIFTY', 'SENSEX'])
            inst_type = 'OPTIDX' if is_index else 'OPTSTK'

            interval = self.ATR_INTERVAL_POS if is_positional else self.ATR_INTERVAL_INTRA
            lookback = self.ATR_LOOKBACK_DAYS_POS if is_positional else self.ATR_LOOKBACK_DAYS_INTRA

            to_date = datetime.now()
            from_date = to_date - timedelta(days=lookback)

            payload = {
                'securityId': str(sec_id),
                'exchangeSegment': segment,
                'instrument': inst_type,
                'interval': interval,
                'fromDate': from_date.strftime('%Y-%m-%d'),
                'toDate': to_date.strftime('%Y-%m-%d'),
            }

            data = self.session.post(
                f'{self.base_url}/charts/intraday',
                json=payload,
                timeout=4,
            ).json()

            if len(data.get('high', [])) > self.ATR_PERIOD:
                atr = talib.ATR(
                    np.array(data['high'], float),
                    np.array(data['low'], float),
                    np.array(data['close'], float),
                    timeperiod=self.ATR_PERIOD,
                )[-1]
                return float(atr) if not np.isnan(atr) else 0.0

        except Exception as e:
            logger.error(f'ATR Error: {e}')
        return 0.0

    # ------------------------------------------------------------------
    def get_realized_pnl(self) -> float:
        try:
            data = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            return sum(float(p.get('realizedProfit', 0)) for p in data)
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    def check_kill_switch(self, loss_limit: float = 8000.0) -> bool:
        if self.kill_switch_triggered:
            return True

        realized = self.get_realized_pnl()

        if realized <= -abs(loss_limit):
            self.kill_switch_triggered = True
            logger.critical(f'REALIZED LOSS LIMIT HIT: {realized}')

            self.square_off_all()
            try:
                self.session.post(
                    f'{self.base_url}/killswitch?killSwitchStatus=ACTIVATE',
                    timeout=10,
                )
            except Exception:
                pass

            return True

        return False

    # ------------------------------------------------------------------
    def square_off_all(self):
        try:
            positions = self.session.get(f'{self.base_url}/positions', timeout=10).json()

            for p in positions:
                qty = abs(int(p.get('netQty', 0)))
                if qty == 0:
                    continue

                action = 'SELL' if p['netQty'] > 0 else 'BUY'

                payload = {
                    'dhanClientId': self.client_id,
                    'transactionType': action,
                    'exchangeSegment': p['exchangeSegment'],
                    'productType': p['productType'],
                    'orderType': 'MARKET',
                    'securityId': p['securityId'],
                    'quantity': qty,
                    'validity': 'DAY',
                }
                self.session.post(
                    f'{self.base_url}/orders',
                    json=payload,
                    timeout=5,
                )
        except Exception as e:
            logger.error(f'Square Off Error: {e}')

    # ------------------------------------------------------------------
    def execute_super_order(self, signal: Dict[str, Any]) -> str:
        if not self.dhan:
            return 'ERROR'

        if self.check_kill_switch():
            return 'ERROR'

        try:
            sym = signal.get('underlying', '')
            trade_sym = signal.get('trading_symbol', '')
            action = signal.get('action', 'BUY')
            is_pos = signal.get('is_positional', False)
            entry = float(signal.get('trigger_above') or 0)
            sl_price = float(signal.get('stop_loss') or 0)
            raw_target = float(signal.get('target') or 0)

            sec_id, exch_id, lot_size = self.mapper.get_security_id(trade_sym)
            if not sec_id:
                return 'ERROR'

            exch_seg = 'BSE_FNO' if exch_id == 'BSE' else 'NSE_FNO'

            ltp = self.get_ltp(sec_id, exch_seg)
            if not ltp:
                return 'ERROR'

            if entry and ltp < entry:
                return 'PRICE_LOW'
            if entry and ltp > entry * 1.05:
                return 'PRICE_HIGH'

            order_type = 'MARKET' if ltp >= entry else 'LIMIT'
            anchor = entry or ltp

            atr = self.fetch_atr(str(sec_id), exch_seg, sym, is_pos)

            # --- SL LOGIC (FIXED) ---
            fb_sl = max(self.MIN_SL_POINTS, 5.0)
            sl_dist = (atr * self.SL_MULTIPLIER) if atr else fb_sl
            sl_dist = max(sl_dist, fb_sl)
            sl_price = anchor - sl_dist

            # --- TARGET ---
            rr = self.TARGET_RR_RATIO_POS if is_pos else self.TARGET_RR_RATIO_INTRA
            target_price = raw_target or (anchor + rr * sl_dist)

            # --- TRAILING ---
            trail_mult = self.TRAIL_MULTIPLIER_POS if is_pos else self.TRAIL_MULTIPLIER_INTRA
            trailing_jump = max(self.round_to_tick(atr * trail_mult), 0.05)

            qty = self.calculate_quantity(anchor, sl_price, is_pos, lot_size)
            if qty <= 0:
                return 'ERROR'

            payload = {
                'dhanClientId': self.client_id,
                'transactionType': action,
                'exchangeSegment': exch_seg,
                'productType': 'MARGIN' if is_pos else 'INTRADAY',
                'orderType': order_type,
                'securityId': str(sec_id),
                'quantity': qty,
                'price': 0.0,
                'validity': 'DAY',
                'targetPrice': self.round_to_tick(target_price),
                'stopLossPrice': self.round_to_tick(sl_price),
                'trailingJump': trailing_jump,
            }

            resp = self.session.post(
                f'{self.base_url}/super/orders',
                json=payload,
                timeout=10,
            )

            if resp.status_code in (200, 201):
                return 'SUCCESS'

            return 'ERROR'

        except Exception as e:
            logger.critical(f'Execution Error: {e}', exc_info=True)
            return 'ERROR'
