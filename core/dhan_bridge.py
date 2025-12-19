from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import requests
import talib
from dhanhq import dhanhq
from dotenv import load_dotenv

from core.dhan_mapper import DhanMapper

# Setup Logger
logger = logging.getLogger('DhanBridge')
load_dotenv()


class DhanBridge:
    # --- CONFIGURATION CONSTANTS ---
    # Risk Management
    RISK_PER_TRADE_POSITIONAL = 0.02  # 2% of capital
    RISK_PER_TRADE_INTRADAY = 0.015  # 1.5% of capital
    MIN_SL_POINTS = 5.0

    # ATR & Technicals
    ATR_PERIOD = 14
    ATR_LOOKBACK_DAYS_POS = 3
    ATR_LOOKBACK_DAYS_INTRA = 3
    ATR_INTERVAL_POS = 15
    ATR_INTERVAL_INTRA = 5

    # Multipliers
    SL_MULTIPLIER = 3.0
    TRAIL_MULTIPLIER_POS = 1.5
    TRAIL_MULTIPLIER_INTRA = 1.25
    TARGET_RR_RATIO_POS = 5.0
    TARGET_RR_RATIO_INTRA = 4.0

    def __init__(self):
        """Initializes Dhan client, session, and mapper."""
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
            self.dhan = dhanhq(self.client_id, self.access_token)
            logger.info('Dhan Bridge Connected')
        else:
            self.dhan = None
            logger.warning('Running in Mock Mode')

    @staticmethod
    def round_to_tick(price: float, tick: float = 0.05) -> float:
        """Rounds price to the nearest tick size."""
        return round(round(price / tick) * tick, 2)

    def get_funds(self) -> float:
        """Fetches available fund limit from Dhan."""
        if not self.access_token:
            return 0.0
        try:
            response = self.session.get(f'{self.base_url}/fundlimit', timeout=5)
            return float(response.json().get('sodLimit', 0.0))
        except Exception as e:
            logger.error(f'Fund Fetch Error: {e}')
            return 0.0

    def get_ltp(self, security_id: str, exchange_segment: str) -> Optional[float]:
        """
        Fetches the Last Traded Price (LTP) for a security.
        """
        if not self.access_token:
            return None

        try:
            payload = {exchange_segment: [int(security_id)]}

            response = self.session.post(f'{self.base_url}/marketfeed/ltp', json=payload, timeout=5)
            data = response.json()

            if not data.get('data'):
                return None

            segment_data = data['data'].get(exchange_segment, {})
            instrument_data = segment_data.get(str(security_id), {})

            price = instrument_data.get('last_price')

            if price is not None:
                return float(price)

            return None

        except Exception as e:
            logger.error(f'LTP Fetch Error for {security_id} [{exchange_segment}]: {e}')
            return None

    def calculate_quantity(
        self, entry: float, sl: float, is_positional: bool, lot_size: int
    ) -> int:
        """Calculates position size based on risk percentage and available funds."""
        try:
            funds = self.get_funds()
            risk_pct = (
                self.RISK_PER_TRADE_POSITIONAL if is_positional else self.RISK_PER_TRADE_INTRADAY
            )
            risk_capital = math.ceil(funds * risk_pct)

            sl_gap = abs(entry - sl)
            if sl_gap <= 1.0:
                return 0

            raw_qty = math.ceil(risk_capital / sl_gap)
            num_lots = max(1, math.ceil(raw_qty / lot_size))
            return int(num_lots * lot_size)
        except Exception:
            return lot_size

    # --- TECHNICALS ---
    def fetch_atr(self, sec_id: str, segment: str, symbol: str, is_positional: bool) -> float:
        """Calculates ATR for dynamic SL and Target."""
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

            response = self.session.post(
                f'{self.base_url}/charts/intraday', json=payload, timeout=4
            )
            data = response.json()

            if data and 'high' in data and len(data['high']) > self.ATR_PERIOD:
                highs = np.array(data['high'], dtype=float)
                lows = np.array(data['low'], dtype=float)
                closes = np.array(data['close'], dtype=float)

                atr = talib.ATR(highs, lows, closes, timeperiod=self.ATR_PERIOD)[-1]
                return float(atr) if not np.isnan(atr) else 0.0

        except Exception as e:
            logger.error(f'ATR Error: {e}')
        return 0.0

    # --- ORDER MANAGEMENT ---
    def cancel_all_pending_orders(self):
        """Cancels all open orders to prevent unintended executions."""
        try:
            response = self.session.get(f'{self.base_url}/orders', timeout=10)
            orders = response.json()
            if not isinstance(orders, list):
                return

            pending = [
                o for o in orders if o.get('orderStatus') in ['PENDING', 'TRANSIT', 'PART_TRADED']
            ]

            if pending:
                logger.info(f'Cancelling {len(pending)} pending orders...')
                for o in pending:
                    self.session.delete(f'{self.base_url}/orders/{o["orderId"]}', timeout=5)
        except Exception:
            pass

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Fetches active open positions."""
        try:
            response = self.session.get(f'{self.base_url}/positions', timeout=10)
            data = response.json()
            if not isinstance(data, list):
                return []
            return [
                p for p in data if p.get('netQty', 0) != 0 and p.get('positionType') != 'CLOSED'
            ]
        except Exception:
            return []

    def square_off_all(self):
        """Emergency function to close all positions and cancel orders."""
        logger.info('SQUARE OFF INITIATED')
        self.cancel_all_pending_orders()

        try:
            positions = self.get_open_positions()
            if not positions:
                logger.info('No open positions.')
                return

            for pos in positions:
                net_qty = pos.get('netQty', 0)
                if net_qty == 0:
                    continue

                action = 'SELL' if net_qty > 0 else 'BUY'
                qty = abs(net_qty)

                payload = {
                    'dhanClientId': self.client_id,
                    'correlationId': f'SQOFF-{int(time.time())}',
                    'transactionType': action,
                    'exchangeSegment': pos.get('exchangeSegment'),
                    'productType': pos.get('productType'),
                    'orderType': 'MARKET',
                    'securityId': pos.get('securityId'),
                    'quantity': qty,
                    'validity': 'DAY',
                }
                self.session.post(f'{self.base_url}/orders', json=payload, timeout=5)
                logger.info(f'Closed {pos.get("tradingSymbol")}: {action} {qty}')

        except Exception as e:
            logger.error(f'Square Off Failed: {e}')

    # --- KILL SWITCH ---
    def get_total_pnl(self) -> float:
        """Calculates realized + unrealized PnL for the day."""
        try:
            data = self.session.get(f'{self.base_url}/positions', timeout=5).json()
            if not isinstance(data, list):
                return 0.0

            return sum(
                float(p.get('realizedProfit', 0)) + float(p.get('unrealizedProfit', 0))
                for p in data
            )
        except Exception:
            return 0.0

    def check_kill_switch(self, loss_limit: float = 8000.0) -> bool:
        """Checks if PnL breaches limit; triggers Square Off & Kill Switch if true."""
        if self.kill_switch_triggered:
            return True

        pnl = self.get_total_pnl()
        if pnl <= -abs(loss_limit):
            self.kill_switch_triggered = True
            logger.critical(f'LOSS LIMIT BREACHED: {pnl} <= -{loss_limit}')

            self.square_off_all()
            time.sleep(2)

            try:
                self.session.post(
                    f'{self.base_url}/killswitch?killSwitchStatus=ACTIVATE', timeout=10
                )
                logger.info('KILL SWITCH ACTIVATED ON BROKER')
            except Exception as e:
                logger.error(f'Kill Switch API Failed: {e}')

            return True
        return False

    # --- EXECUTION LOGIC ---
    def execute_super_order(self, signal: Dict[str, Any]) -> str:
        """Parses signal, calculates safe SL/Target, and executes Bracket Order."""
        if not self.dhan:
            return 'ERROR'
        if self.kill_switch_triggered:
            return 'ERROR'

        logger.info('=' * 60)
        logger.info('EXECUTING SUPER ORDER')

        try:
            # 1. Parse Signal
            sym = signal.get('underlying', '')
            trade_sym = signal.get('trading_symbol', '')
            action = signal.get('action', 'BUY')
            is_pos = signal.get('is_positional', False)
            entry = float(signal.get('trigger_above') or 0)
            sl_price = float(signal.get('stop_loss') or 0)
            raw_target = float(signal.get('target') or 0)

            # 2. Map Security
            sec_id, exch_id, lot_size = self.mapper.get_security_id(trade_sym)
            if not sec_id or lot_size == -1:
                logger.error(f'Security ID Not Found: {trade_sym}')
                return 'ERROR'

            exch_seg = 'BSE_FNO' if (exch_id == 'BSE' or sym == 'SENSEX') else 'NSE_FNO'

            # 3. LTP & Entry Logic
            ltp = self.get_ltp(sec_id, exch_seg)
            if not ltp:
                logger.error('Could not fetch LTP')
                return 'ERROR'

            logger.info(f'Entry Check: LTP {ltp} vs Trigger {entry}')

            order_type = 'LIMIT'
            if entry == 0:
                entry = ltp
                order_type = 'MARKET'
            elif ltp > (entry * 1.05):
                logger.warning('Price High (>5%). Waiting for pullback.')
                return 'PRICE_HIGH'
            elif ltp >= entry:
                logger.info('Breakout Triggered! Switching to MARKET.')
                order_type = 'MARKET'
            else:
                logger.info('Waiting for breakout.')
                return 'PRICE_LOW'

            anchor = entry if entry > 0 else ltp

            # 4. Technical Calculations (ATR & Fallbacks)
            atr = self.fetch_atr(str(sec_id), exch_seg, sym, is_pos)

            # Define Price-Based Fallbacks
            if anchor < 50:
                fb_sl, fb_trail = 5.0, 0.5
            elif anchor < 150:
                fb_sl, fb_trail = 15.0, 2.0
            elif anchor < 500:
                fb_sl, fb_trail = 30.0, 5.0
            else:
                fb_sl, fb_trail = 50.0, 10.0

            # 5. Calculate Stop Loss & Trail
            trail_mult = self.TRAIL_MULTIPLIER_POS if is_pos else self.TRAIL_MULTIPLIER_INTRA
            trailing_jump = max((atr * trail_mult) if atr else fb_trail, fb_trail)

            if sl_price == 0:
                sl_dist = (atr * self.SL_MULTIPLIER) if atr else fb_sl
                sl_price = anchor - sl_dist

            # Clamp SL to ensure it's valid
            sl_price = max(sl_price, anchor - fb_sl, 5.0)
            if action == 'BUY' and sl_price >= anchor:
                sl_price = max(anchor - 1.0, 5.0)

            sl_dist = abs(anchor - sl_price)

            # 6. Calculate Target (Dynamic RR)
            target_price = 0.0
            if raw_target > 0:
                target_price = raw_target
            else:
                rr_mult = self.TARGET_RR_RATIO_POS if is_pos else self.TARGET_RR_RATIO_INTRA
                tgt_dist = max(rr_mult * sl_dist, 10.0 * atr) if atr else (5.0 * sl_dist)
                target_price = anchor + tgt_dist

            # 7. Rounding & Quantity
            trailing_jump = self.round_to_tick(trailing_jump)
            sl_price = math.floor(self.round_to_tick(sl_price))
            target_price = self.round_to_tick(target_price)
            price_to_send = self.round_to_tick(entry) if order_type == 'LIMIT' else 0.0

            qty = self.calculate_quantity(entry, sl_price, is_pos, lot_size)
            prod_type = 'MARGIN' if is_pos else 'INTRADAY'

            # 8. Send Order
            payload = {
                'dhanClientId': self.client_id,
                'correlationId': f'BOT-{int(time.time())}',
                'transactionType': 'BUY',
                'exchangeSegment': exch_seg,
                'productType': prod_type,
                'orderType': order_type,
                'securityId': str(sec_id),
                'quantity': int(qty),
                'price': float(price_to_send),
                'triggerPrice': 0.0,
                'validity': 'DAY',
                'targetPrice': float(target_price),
                'stopLossPrice': float(sl_price),
                'trailingJump': float(trailing_jump),
            }

            logger.info(f'Sending {order_type} | Qty: {qty} | TGT: {target_price} | SL: {sl_price}')

            response = self.session.post(f'{self.base_url}/super/orders', json=payload, timeout=10)
            data = response.json()

            if response.status_code in [200, 201] and data.get('orderStatus') in [
                'PENDING',
                'TRADED',
                'TRANSIT',
            ]:
                logger.info(f'SUCCESS: Order ID {data.get("orderId")}')
                return 'SUCCESS'
            else:
                logger.error(f'FAILED: {data}')
                return 'ERROR'

        except Exception as e:
            logger.critical(f'Execution Exception: {e}', exc_info=True)
            return 'ERROR'
