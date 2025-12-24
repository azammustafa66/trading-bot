from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

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
    # TRAIL_MULTIPLIER not used in fixed % logic, kept for fallback
    TRAIL_MULTIPLIER_POS = 1.3
    TRAIL_MULTIPLIER_INTRA = 1.1

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
                    'Accept': 'application/json',
                }
            )
            try:
                self.dhan = dhanhq(self.client_id, self.access_token)
                logger.info('Dhan Bridge Connected')
            except Exception as e:
                logger.error(f'DhanHQ Init Failed: {e}')
                self.dhan = None
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
        """
        Fetches the Last Traded Price (LTP) with retries.
        """
        # Try 3 times (Total wait ~1.5s max)
        for attempt in range(1, 4):
            try:
                payload = {exchange_segment: [int(security_id)]}
                response = self.session.post(
                    f'{self.base_url}/marketfeed/ltp', json=payload, timeout=5
                )
                data = response.json()

                # Check validity
                if 'data' not in data or exchange_segment not in data['data']:
                    # logger.warning(f"LTP Empty (ID: {security_id}, Att: {attempt})")
                    time.sleep(0.5)
                    continue

                price = float(data['data'][exchange_segment][str(security_id)]['last_price'])
                return price

            except Exception as e:
                logger.warning(f'LTP Error (ID: {security_id}, Att: {attempt}): {e}')
                time.sleep(0.5)

        logger.error(f'❌ Failed to fetch LTP for ID {security_id} after retries.')
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

            max_qty_by_risk = math.floor(risk_amount / sl_points)
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
            # --- Instrument Type Logic ---
            if segment == 'MCX_COMM':
                inst_type = 'OPTFUT'
            else:
                is_index = any(
                    x in symbol.upper() for x in ['NIFTY', 'BANKNIFTY', 'SENSEX', 'FINNIFTY']
                )
                inst_type = 'OPTIDX' if is_index else 'OPTSTK'
            # -----------------------------

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
                f'{self.base_url}/charts/intraday', json=payload, timeout=4
            ).json()

            if isinstance(data, dict) and len(data.get('high', [])) > self.ATR_PERIOD:
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
            if isinstance(data, dict):
                data = data.get('data', [])
            return sum(float(p.get('realizedProfit', 0)) for p in data)
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    def has_open_position(self, security_id: str) -> bool:
        try:
            response = self.session.get(f'{self.base_url}/positions', timeout=5)
            data = response.json()

            positions_list = []
            if isinstance(data, list):
                positions_list = data
            elif isinstance(data, dict):
                positions_list = data.get('data', [])

            for p in positions_list:
                if str(p.get('securityId')) == str(security_id):
                    net_qty = int(p.get('netQty', 0))
                    pos_type = p.get('positionType', 'CLOSED')

                    if net_qty != 0 and pos_type != 'CLOSED':
                        logger.info(
                            f'Existing Position Found: {p.get("tradingSymbol")} | Qty: {net_qty}'
                        )
                        return True
            return False
        except Exception as e:
            logger.error(f'Position Check Error: {e}')
            return False

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
                    f'{self.base_url}/killswitch?killSwitchStatus=ACTIVATE', timeout=10
                )
            except Exception:
                pass
            return True

        return False

    # ------------------------------------------------------------------
    def square_off_all(self):
        try:
            positions = self.session.get(f'{self.base_url}/positions', timeout=10).json()
            if isinstance(positions, dict):
                positions = positions.get('data', [])

            for p in positions:
                qty = abs(int(p.get('netQty', 0)))
                if qty == 0:
                    continue

                action = 'SELL' if int(p.get('netQty', 0)) > 0 else 'BUY'

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

                self.session.post(f'{self.base_url}/orders', json=payload, timeout=5)

        except Exception as e:
            logger.error(f'Square Off Error: {e}', exc_info=True)

    # ------------------------------------------------------------------
    def execute_super_order(self, signal: Dict[str, Any]) -> Tuple[float, str]:
        if not self.access_token:
            return 0.0, 'ERROR'

        if self.check_kill_switch():
            return 0.0, 'ERROR'

        try:
            try:
                trade_sym = signal.get('trading_symbol', '')
                action = signal.get('action', 'BUY')
                is_pos = signal.get('is_positional', False)
                sym = signal.get('underlying', '')

                entry = float(signal.get('trigger_above') or 0)
                sl_price = float(signal.get('stop_loss') or 0)
                raw_target = float(signal.get('target') or 0)

                def ltp_wrapper(security_id: str, exchange_segment: str) -> float:
                    # Uses robust get_ltp inside mapper
                    result = self.get_ltp(security_id, exchange_segment)
                    return result if result is not None else 0.0

                # [FIX] Unpack 4 values including LTP
                sec_id, exch_id, lot_size, mapped_ltp = self.mapper.get_security_id(
                    trading_symbol=trade_sym, price_ref=entry, ltp_fetcher=ltp_wrapper
                )
            except Exception as e:
                logger.critical(f'Mapping Error: {e}')
                return 0.0, 'ERROR'

            if not sec_id:
                return 0.0, 'ERROR'

            if self.has_open_position(sec_id):
                logger.info(f'Skipping {trade_sym}: Position already active.')
                return 0.0, 'SUCCESS'

            # --- Exchange Logic ---
            if exch_id == 'MCX':
                exch_seg = 'MCX_COMM'
            elif exch_id == 'BSE':
                exch_seg = 'BSE_FNO'
            else:
                exch_seg = 'NSE_FNO'

            # --- [OPTIMIZATION] Use Mapped LTP if available ---
            if mapped_ltp > 0:
                ltp = mapped_ltp
                # logger.info(f"Using Cached LTP: {ltp}")
            else:
                ltp = self.get_ltp(sec_id, exch_seg)

            if not ltp:
                logger.error(f'Could not fetch LTP for {trade_sym}')
                return 0.0, 'ERROR'

            # --- Price Validation ---
            if entry and ltp < entry:
                return ltp, 'PRICE_LOW'
            if entry and ltp > entry * 1.20:
                return ltp, 'PRICE_HIGH'

            # --- Dynamic Shift ---
            drift = 0.0
            if entry and ltp > entry:
                drift = ltp - entry
                if sl_price > 0:
                    sl_price += drift
                    logger.info(f'Late Entry! Shifting SL up by {drift:.2f}')

            order_type = 'MARKET' if ltp >= entry else 'LIMIT'
            anchor = entry or ltp

            # Fetch ATR for Target Calculation (NOT used for Trailing Jump anymore)
            atr = self.fetch_atr(str(sec_id), exch_seg, sym, is_pos)

            # --- SL CALCULATION ---
            if sl_price == 0:
                if atr:
                    sl_dist = atr * 2.0
                else:
                    sl_dist = anchor * 0.10
                sl_dist = max(sl_dist, self.MIN_SL_POINTS)
                sl_price = anchor - sl_dist

            # --- TARGET ---
            atr_mult = 30.0 if is_pos else 20.0
            if raw_target == 0:
                target_price = (anchor + (atr * atr_mult)) if atr else (anchor * 3.5)
            else:
                target_price = raw_target * 2.0

            # --- [FIXED % TRAILING] ---
            jump_amount = anchor * 0.05
            trailing_jump = max(self.round_to_tick(jump_amount), 1.0)

            logger.info(f'Trailing Config: Price {anchor} | Jump {trailing_jump} (5%)')

            qty = self.calculate_quantity(anchor, sl_price, is_pos, lot_size)
            if qty <= 0:
                return ltp, 'ERROR'

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

            resp = self.session.post(f'{self.base_url}/super/orders', json=payload, timeout=10)

            if resp.status_code in (200, 201):
                logger.info(f'Order Placed Successfully: {trade_sym}')
                return ltp, 'SUCCESS'

            logger.error(f'Order Failed: {resp.text}')
            return ltp, 'ERROR'

        except Exception as e:
            logger.critical(f'Execution Error: {e}', exc_info=True)
            return 0.0, 'ERROR'
