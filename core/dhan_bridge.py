import logging
import math
import os
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
                available_funds * 0.02 if is_positional else available_funds * 0.0125
            )

            sl_gap = abs(entry_price - sl_price)
            if sl_gap < 1.0:
                sl_gap = 1.0

            raw_qty = math.ceil(risk_capital / sl_gap)
            num_lots = max(1, round(raw_qty / lot_size))
            return math.ceil(num_lots * lot_size)
        except Exception:
            return lot_size

    def get_dynamic_trail(
        self,
        security_id: str,
        exchange_segment: str,
        entry_price: float,
        symbol: str,
        is_positional: bool,
    ) -> float:
        """
        Calculates ATR based Trailing Jump.
        - Intraday: 5-min candles, 1.5x ATR
        - Positional: 60-min candles, 2.5x ATR
        """
        atr_trail = None

        try:
            is_index = any(x in symbol.upper() for x in ['NIFTY', 'BANKNIFTY', 'SENSEX'])
            instrument_type = 'OPTIDX' if is_index else 'OPTSTK'

            # --- ADAPTIVE TIMEFRAME LOGIC ---
            if is_positional:
                interval = 60
                lookback_days = 15
                multiplier = 2.5
                logger.info('Using POSITIONAL settings: 60min candles | 2.5x ATR')
            else:
                interval = 5
                lookback_days = 3
                multiplier = 1.5
                logger.info('Using INTRADAY settings: 5min candles | 1.5x ATR')

            # Calculate Date Range
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

            response = self.session.post(url, json=payload, timeout=5)
            data = response.json()

            if data and 'high' in data and len(data['high']) > 15:
                highs = np.array(data['high'], dtype=float)
                lows = np.array(data['low'], dtype=float)
                closes = np.array(data['close'], dtype=float)

                atr_array = talib.ATR(highs, lows, closes, timeperiod=14)
                current_atr = atr_array[-1]

                if not np.isnan(current_atr) and current_atr > 0:
                    atr_trail = current_atr * multiplier
                    logger.info(
                        f'ATR({interval}m): {current_atr:.2f} | Trail ({multiplier}x): \
                        {atr_trail:.2f}'
                    )

        except Exception as e:
            logger.error(f'TA-Lib Calc Failed: {e}')

        # Fallback Logic (Tiered System)
        if atr_trail is None:
            if entry_price < 50:
                fallback = 0.5
            elif 50 <= entry_price < 150:
                fallback = 2.0
            elif 150 <= entry_price < 500:
                fallback = 5.0
            else:
                fallback = 10.0
            logger.info(f'API/Data Issue - Using Tiered Fallback: {fallback}')
            return fallback

        return atr_trail

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

            logger.info(f'Cancelling {len(pending)} pending orders...')
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
        logger.info('🚨 FRIDAY SQUARE OFF TRIGGERED 🚨')
        self.cancel_all_pending_orders()
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

    def execute_super_order(self, signal: Dict[str, Any]):
        if not self.dhan:
            logger.warning('Dhan client not initialized')
            return

        logger.info('=' * 60)
        logger.info('EXECUTING SUPER ORDER')

        try:
            sym = signal.get('underlying', '')
            trade_sym = signal.get('trading_symbol')
            action = signal.get('action')
            is_positional = signal.get('is_positional', False)
            entry_price = float(signal.get('trigger_above') or 0)
            sl_price = float(signal.get('stop_loss') or 0)

            sec_id, exch_id, lot_size = self.mapper.get_security_id(trade_sym)

            if not sec_id or lot_size == -1:
                logger.error(f'Security ID not found for {trade_sym}')
                return

            exch_seg = 'BSE_FNO' if (exch_id == 'BSE' or sym == 'SENSEX') else 'NSE_FNO'
            current_ltp = self.get_ltp(sec_id, exch_seg)
            order_type = 'LIMIT'

            # LTP Checks
            if current_ltp:
                logger.info(f'LTP Check: {current_ltp} vs Entry {entry_price}')
                if entry_price == 0:
                    entry_price = current_ltp
                    order_type = 'MARKET'
                elif current_ltp > (entry_price * 1.05):
                    logger.warning(f'Price {current_ltp} > 5% above entry. SKIPPING.')
                    return
                elif current_ltp >= entry_price:
                    logger.info('Breakout confirmed. Switching to MARKET.')
                    order_type = 'MARKET'
                else:
                    logger.info(f'Price {current_ltp} < {entry_price}. Sending LIMIT.')
                    order_type = 'LIMIT'
            else:
                logger.error('Could not fetch LTP.')
                return

            # Auto-SL Logic
            anchor_price = entry_price if entry_price > 0 else current_ltp
            if sl_price == 0 and anchor_price > 0:
                raw = signal.get('raw', '').upper()
                if 'HERO' in raw:
                    sl_price = 0.05
                else:
                    sl_price = max(anchor_price - 15.0, 0.05)

            if action == 'BUY' and sl_price >= entry_price:
                sl_price = max(entry_price - 1.0, 0.05)

            # --- DYNAMIC TRAILING CALCULATION ---
            # Pass is_positional flag to adapt settings
            trailing_jump = self.get_dynamic_trail(
                sec_id, exch_seg, entry_price, sym, is_positional
            )

            # Rounding
            trailing_jump = self.round_to_tick(trailing_jump)
            target_price = self.round_to_tick(entry_price * 10.0)
            sl_price = self.round_to_tick(sl_price)

            trigger_price = self.round_to_tick(entry_price) if order_type == 'LIMIT' else 0.0
            price_to_send = self.round_to_tick(entry_price + 0.5) if order_type == 'LIMIT' else 0.0

            qty = self.calculate_quantity(entry_price, sl_price, is_positional, lot_size)
            product_type = 'MARGIN' if is_positional else 'INTRADAY'

            payload = {
                'dhanClientId': self.client_id,
                'correlationId': f'BOT-{int(datetime.now().timestamp())}',
                'transactionType': 'BUY' if action == 'BUY' else 'SELL',
                'exchangeSegment': exch_seg,
                'productType': product_type,
                'orderType': order_type,
                'securityId': str(sec_id),
                'quantity': int(qty),
                'price': float(price_to_send),
                'triggerPrice': float(trigger_price),
                'validity': 'DAY',
                'targetPrice': float(target_price),
                'stopLossPrice': float(sl_price),
                'trailingJump': float(trailing_jump),
            }

            logger.info(
                f'SENDING {order_type} | Trail: {trailing_jump} | Positional: {is_positional}'
            )
            logger.debug(f'Payload: {payload}')

            url = f'{self.base_url}/super/orders'
            response = self.session.post(url, json=payload, timeout=10)
            data = response.json()

            if response.status_code in [200, 201] and data.get('orderStatus') in [
                'PENDING',
                'TRADED',
                'TRANSIT',
            ]:
                logger.info(f'SUCCESS: Order ID {data.get("orderId")}')
            else:
                logger.error(f'FAILED: {data}')

        except Exception as e:
            logger.critical(f'Order Execution Error: {e}', exc_info=True)
