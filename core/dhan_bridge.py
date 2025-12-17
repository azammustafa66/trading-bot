import logging
import math
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict

import numpy as np
import requests
import talib
from dhanhq import dhanhq
from dotenv import load_dotenv

from core.dhan_mapper import DhanMapper

logger = logging.getLogger('DhanBridge')
load_dotenv()


class DhanBridge:
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
            self.dhan = dhanhq(self.client_id, self.access_token)
            logger.info('Dhan Bridge Connected')
        else:
            self.dhan = None
            logger.warning('Running in Mock Mode')

    @staticmethod
    def round_to_tick(price: float, tick: float = 0.05) -> float:
        return round(round(price / tick) * tick, 2)

    def get_funds(self) -> float | None:
        if not self.access_token:
            return None
        try:
            url = f'{self.base_url}/fundlimit'
            response = self.session.get(url=url, timeout=10)
            data = response.json()
            return float(data.get('sodLimit', 0.0))
        except Exception as e:
            logger.error(f'Failure in fetching funds: {e}')
            return None

    def get_ltp(self, security_id: str, exchange_segment: str) -> float | None:
        if not self.access_token:
            return None
        try:
            url = f'{self.base_url}/marketfeed/ltp'
            payload = {exchange_segment: [int(security_id)]}

            if exchange_segment == 'BSE_FNO':
                payload['BSE'] = [int(security_id)]

            response = self.session.post(url, json=payload, timeout=5)
            data = response.json()

            if not data.get('data'):
                return None

            str_id = str(security_id)
            for seg, items in data['data'].items():
                if str_id in items:
                    price = float(items[str_id].get('last_price', 0))
                    if price > 0:
                        return price
            return None
        except Exception as e:
            logger.error(f'Something went wrong while fetching LTP {e}')
            return None

    def calculate_quantity(
        self, entry_price: float, sl_price: float, is_positional: bool, lot_size: int
    ) -> int:
        try:
            available_funds = self.get_funds() or 0
            risk_capital = math.ceil(
                available_funds * 0.02 if is_positional else available_funds * 0.015
            )

            sl_gap = abs(entry_price - sl_price)
            if sl_gap <= 1.0:
                return 0

            raw_qty = math.ceil(risk_capital / sl_gap)
            num_lots = max(1, math.ceil(raw_qty / lot_size))
            return math.ceil(num_lots * lot_size)
        except Exception:
            return lot_size

    # --- UNIFIED ATR MODULE ---
    def fetch_atr(
        self, security_id: str, exchange_segment: str, symbol: str, is_positional: bool
    ) -> float:
        try:
            is_index = any(x in symbol.upper() for x in ['NIFTY', 'BANKNIFTY', 'SENSEX'])
            instrument_type = 'OPTIDX' if is_index else 'OPTSTK'

            if is_positional:
                interval = 15
                lookback_days = 3
            else:
                interval = 5
                lookback_days = 3

            to_date = datetime.now()
            from_date = to_date - timedelta(days=lookback_days)

            url = f'{self.base_url}/charts/intraday'
            payload = {
                'securityId': str(security_id),
                'exchangeSegment': exchange_segment,
                'instrument': instrument_type,
                'interval': interval,
                'fromDate': from_date.strftime('%Y-%m-%d'),
                'toDate': to_date.strftime('%Y-%m-%d'),
            }

            response = self.session.post(url, json=payload, timeout=4)
            data = response.json()

            if data and 'high' in data and len(data['high']) > 15:
                highs = np.array(data['high'], dtype=float)
                lows = np.array(data['low'], dtype=float)
                closes = np.array(data['close'], dtype=float)

                atr_array = talib.ATR(highs, lows, closes)
                current_atr = atr_array[-1]

                if not np.isnan(current_atr) and current_atr > 0:
                    return float(current_atr)

        except Exception as e:
            logger.error(f'ATR Fetch Failed: {e}')

        return 0.0

    # --- POSITION & ORDER MANAGEMENT ---
    def cancel_all_pending_orders(self):
        try:
            url = f'{self.base_url}/orders'
            response = self.session.get(url, timeout=10)
            orders = response.json()
            if not isinstance(orders, list):
                return

            pending = [
                o for o in orders if o.get('orderStatus') in ['PENDING', 'TRANSIT', 'PART_TRADED']
            ]
            if not pending:
                return

            logger.info(f'⚠️ Cancelling {len(pending)} pending orders...')
            for o in pending:
                self.session.delete(f'{self.base_url}/orders/{o["orderId"]}', timeout=5)
        except Exception:
            pass

    def get_open_positions(self):
        try:
            url = f'{self.base_url}/positions'
            response = self.session.get(url, timeout=10)
            data = response.json()
            if not isinstance(data, list):
                return []
            return [
                p for p in data if p.get('netQty', 0) != 0 and p.get('positionType') != 'CLOSED'
            ]
        except Exception:
            return []

    def square_off_all(self):
        logger.info('SQUARE OFF SEQUENCE INITIATED')

        # 1. Cancel Pending Orders First (Super Orders)
        self.cancel_all_pending_orders()

        # 2. Close Positions
        positions = self.get_open_positions()
        if not positions:
            logger.info('No open positions.')
            return

        for pos in positions:
            try:
                net_qty = pos.get('netQty', 0)
                action = 'SELL' if net_qty > 0 else 'BUY'
                qty = abs(net_qty)

                payload = {
                    'dhanClientId': self.client_id,
                    'correlationId': f'SQOFF-{int(datetime.now().timestamp())}',
                    'transactionType': action,
                    'exchangeSegment': pos.get('exchangeSegment'),
                    'productType': pos.get('productType'),
                    'orderType': 'MARKET',
                    'securityId': pos.get('securityId'),
                    'quantity': qty,
                    'validity': 'DAY',
                }
                self.session.post(f'{self.base_url}/orders', json=payload, timeout=5)
                logger.info(f'CLOSED {pos.get("tradingSymbol")}: {action} {qty}')
            except Exception as e:
                logger.error(f'Failed to close {pos.get("tradingSymbol")}: {e}')

    # --- KILL SWITCH MODULE ---
    def get_total_pnl(self) -> float:
        """
        Calculates Total Day P&L (Realized + Unrealized) from positions.
        """
        try:
            url = f'{self.base_url}/positions'
            response = self.session.get(url, timeout=5)
            data = response.json()

            if not isinstance(data, list):
                return 0.0

            total_pnl = 0.0
            for p in data:
                realized = float(p.get('realizedProfit', 0.0))
                unrealized = float(p.get('unrealizedProfit', 0.0))
                total_pnl += realized + unrealized

            return total_pnl
        except Exception as e:
            logger.error(f'Error fetching PnL: {e}')
            return 0.0

    def activate_kill_switch(self):
        """
        Activates Kill Switch via API.
        Requires all positions to be closed first.
        """
        logger.critical('ACTIVATING KILL SWITCH')
        try:
            url = f'{self.base_url}/killswitch?killSwitchStatus=ACTIVATE'
            response = self.session.post(url, timeout=10)

            if response.status_code == 200:
                logger.info(f'KILL SWITCH ACTIVATED: {response.json()}')
                return True
            else:
                logger.error(f'Kill Switch Failed: {response.text}')
                return False
        except Exception as e:
            logger.error(f'Kill Switch Exception: {e}')
            return False

    def check_kill_switch(self, loss_limit: float = 8000.0) -> bool:
        """
        Checks P&L. If loss >= limit:
        1. Squares off positions
        2. Activates Kill Switch
        3. Returns True (Signal to stop bot)
        """
        if self.kill_switch_triggered:
            return True

        current_pnl = self.get_total_pnl()

        if current_pnl <= -abs(loss_limit):
            self.kill_switch_triggered = True  # <--- SET FLAG IMMEDIATELY

            logger.critical('=' * 60)
            logger.critical(f'LOSS LIMIT BREACHED: {current_pnl} <= -{loss_limit}')
            logger.critical('=' * 60)

            # 1. Close Everything
            self.square_off_all()

            # 2. Wait for executions
            time.sleep(2)

            # 3. Activate Kill Switch
            self.activate_kill_switch()

            return True

        return False

    # --- MAIN EXECUTION ---
    def execute_super_order(self, signal: Dict[str, Any]) -> str:
        if not self.dhan:
            logger.warning('Dhan client not initialized')
            return 'ERROR'

        if self.kill_switch_triggered:
            logger.warning('Kill Switch Active. Ignoring Signal.')
            return 'ERROR'

        logger.info('=' * 60)
        logger.info('EXECUTING SUPER ORDER')

        try:
            sym = signal.get('underlying', '')
            trade_sym = signal.get('trading_symbol', '')
            action = signal.get('action', '')
            is_positional = signal.get('is_positional', False)

            # Safe float conversion
            entry_price = float(signal.get('trigger_above') or 0)
            sl_price = float(signal.get('stop_loss') or 0)

            sec_id, exch_id, lot_size = self.mapper.get_security_id(trade_sym)

            if not sec_id or lot_size == -1:
                logger.error(f'Security ID not found for {trade_sym}')
                return 'ERROR'

            exch_seg = 'BSE_FNO' if (exch_id == 'BSE' or sym == 'SENSEX') else 'NSE_FNO'
            current_ltp = self.get_ltp(sec_id, exch_seg)
            order_type = 'LIMIT'

            # --- CLIENT SIDE TRIGGER LOGIC ---
            if current_ltp:
                logger.info(f'LTP Check: {current_ltp} vs Entry {entry_price}')

                if entry_price == 0:
                    entry_price = current_ltp
                    order_type = 'MARKET'

                elif current_ltp > (entry_price * 1.05):
                    logger.warning(f'Price {current_ltp} > 5% above entry. Waiting (PRICE_HIGH).')
                    # FIXED: Added underscore to match main.py
                    return 'PRICE_HIGH'

                elif current_ltp >= entry_price:
                    logger.info(f'Breakout! {current_ltp} >= {entry_price}. FIRE MARKET ORDER.')
                    order_type = 'MARKET'

                else:
                    logger.info(
                        f'Price {current_ltp} < {entry_price}. Waiting for breakout (PRICE_LOW).'
                    )
                    # FIXED: Added underscore to match main.py
                    return 'PRICE_LOW'
            else:
                logger.error('Could not fetch LTP.')
                return 'ERROR'

            anchor_price = entry_price if entry_price > 0 else current_ltp
            atr_val = self.fetch_atr(str(sec_id), exch_seg, sym, is_positional)

            # Fallback logic
            if anchor_price < 50:
                fallback_sl_pts, fallback_trail = 5.0, 0.5
            elif 50 <= anchor_price < 150:
                fallback_sl_pts, fallback_trail = 15.0, 2.0
            elif 150 <= anchor_price < 500:
                fallback_sl_pts, fallback_trail = 30.0, 5.0
            else:
                fallback_sl_pts, fallback_trail = 50.0, 10.0

            sl_mult = 2.25
            trail_mult = 1.75 if is_positional else 1.5
            trailing_jump = 0.0

            if atr_val:
                trailing_jump = math.ceil((atr_val * trail_mult))
            else:
                trailing_jump = fallback_trail

            if sl_price == 0:
                raw_text = signal.get('raw', '').upper()
                if 'HERO' in raw_text:
                    sl_price = 5.0
                elif atr_val:
                    sl_price = anchor_price - (atr_val * sl_mult)
                else:
                    sl_price = anchor_price - fallback_sl_pts

            sl_price = max(sl_price, 5.0)
            if action == 'BUY' and sl_price >= anchor_price:
                sl_price = max(anchor_price - 1.0, 5.0)

            trailing_jump = math.ceil(self.round_to_tick(trailing_jump))
            target_price = self.round_to_tick(entry_price * 10.0)
            sl_price = math.floor(self.round_to_tick(sl_price))
            price_to_send = self.round_to_tick(entry_price) if order_type == 'LIMIT' else 0.0

            qty = self.calculate_quantity(entry_price, sl_price, is_positional, lot_size)
            product_type = 'MARGIN' if is_positional else 'INTRADAY'

            payload = {
                'dhanClientId': self.client_id,
                'correlationId': f'BOT-{int(datetime.now().timestamp())}',
                'transactionType': 'BUY',
                'exchangeSegment': exch_seg,
                'productType': product_type,
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

            logger.info(f'SENDING {order_type} | Trail: {trailing_jump} | SL: {sl_price}')

            url = f'{self.base_url}/super/orders'
            response = self.session.post(url, json=payload, timeout=10)
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
            logger.critical(f'Order Execution Error: {e}', exc_info=True)
            return 'ERROR'
