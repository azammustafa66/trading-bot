import logging
import math
import os
from datetime import datetime
from typing import Any, Dict

import requests
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

        # FIX: Add headers including client-id for LTP calls
        if self.access_token and self.client_id:
            self.session.headers.update(
                {
                    'access-token': self.access_token,
                    'client-id': self.client_id,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                }
            )

        if self.client_id and self.access_token:
            self.dhan = dhanhq(self.client_id, self.access_token)
            logger.info('Dhan Bridge Connected')
        else:
            self.dhan = None
            logger.warning('Running in Mock Mode')

        self.mapper = DhanMapper()

    @staticmethod
    def round_to_tick(price: float, tick: float = 0.05) -> float:
        """
        CRITICAL: Rounds price to nearest 0.05.
        """
        return round(round(price / tick) * tick, 2)

    def get_funds(self) -> float | None:
        """
        Fetches available funds in trading account
        """
        if not self.access_token:
            return None
        try:
            url = f'{self.base_url}/fundlimit'
            response = self.session.get(url=url, timeout=10)
            data = response.json()
            total_funds = float(data.get('sodLimit', 0.0))
            return total_funds
        except Exception as e:
            logger.error(f'Failure in fetching funds: {e}')
            return None

    def get_ltp(self, security_id: str, exchange_segment: str) -> float | None:
        """
        Fetches the latest market price using the active session.
        Matches API Structure: { "NSE_FNO": [49081] } (Int in List)
        """
        if not self.access_token:
            return None
        try:
            url = f'{self.base_url}/marketfeed/ltp'

            # Payload must be INTEGER for ID
            payload = {exchange_segment: [int(security_id)]}

            # Fallback for BSE Options (Try both BSE_FNO and BSE)
            if exchange_segment == 'BSE_FNO':
                payload['BSE'] = [int(security_id)]

            response = self.session.post(url, json=payload, timeout=5)
            data = response.json()

            if not data.get('data'):
                logger.error(f'LTP Failed. API Response: {data}')
                return None

            # Check all returned segments for the ID (String key in response)
            str_id = str(security_id)
            for seg, items in data['data'].items():
                if str_id in items:
                    price = float(items[str_id].get('last_price', 0))
                    if price > 0:
                        return price

            logger.warning(f'LTP key missing for {exchange_segment}:{security_id}')
            return None

        except Exception as e:
            logger.error(f'LTP fetch error: {e}')
            return None

    def calculate_quantity(
        self,
        entry_price: float,
        sl_price: float,
        is_positional: bool,
        lot_size: int,
    ) -> int:
        """
        Calculates position size based on defined risk per trade.
        """
        try:
            available_funds = self.get_funds()
            total_funds = available_funds if available_funds is not None else 0
            risk_capital = math.ceil(
                total_funds * 0.02 if is_positional else total_funds * 0.0125
            )

            sl_gap = abs(entry_price - sl_price)
            if sl_gap < 1.0:
                sl_gap = 1.0

            raw_qty = math.ceil(risk_capital / sl_gap)

            num_lots = max(1, round(raw_qty / lot_size))
            final_qty = math.ceil(num_lots * lot_size)

            logger.info(
                f'Qty Calc: Risk INR {risk_capital} | Gap {sl_gap:.2f} | {num_lots} Lots -> {final_qty}'
            )
            return final_qty
        except Exception:
            return lot_size

    def execute_super_order(self, signal: Dict[str, Any]):
        """
        Executes Super Order (LIMIT/MARKET) with Trigger Price for Limit orders.
        """
        if not self.dhan:
            logger.warning('Dhan client not initialized, order skipped')
            return

        logger.info('=' * 60)
        logger.info('EXECUTING SUPER ORDER')

        try:
            # 1. Unpack Signal
            sym = signal.get('underlying')
            trade_sym = signal.get('trading_symbol')
            action = signal.get('action')
            is_positional = signal.get('is_positional', False)
            entry_price = float(signal.get('trigger_above') or 0)

            # Default to 0 so we can detect missing SL later
            sl_price = float(signal.get('stop_loss') or 0)

            # 2. Map Security
            sec_id, exch_id, lot_size = self.mapper.get_security_id(trade_sym)

            if not sec_id or lot_size == -1:
                logger.error(f'Security ID not found for {trade_sym}')
                return

            # 3. Determine Exchange Segment
            exch_seg = 'BSE_FNO' if (exch_id == 'BSE' or sym == 'SENSEX') else 'NSE_FNO'

            # 4. LTP Logic & Order Type
            current_ltp = self.get_ltp(sec_id, exch_seg)
            order_type = 'LIMIT'

            if current_ltp:
                logger.info(f'LTP Check: {current_ltp} vs Entry {entry_price}')

                # CASE A: Market Order requested explicitly (Entry=0)
                if entry_price == 0:
                    entry_price = current_ltp
                    order_type = 'MARKET'

                # CASE B: Price is TOO HIGH (> 5% buffer) - SKIP TRADE
                elif current_ltp > (entry_price * 1.05):
                    logger.warning(
                        f'Price {current_ltp} is > 5% above entry {entry_price}. SKIPPING trade.'
                    )
                    return

                # CASE C: Momentum Breakout (LTP >= Entry) - EXECUTE NOW
                elif current_ltp >= entry_price:
                    logger.info(
                        f'Breakout confirmed ({current_ltp} >= {entry_price}). Switching to MARKET.'
                    )
                    order_type = 'MARKET'

                # CASE D: Price is below Entry - PLACE LIMIT
                else:
                    logger.info(
                        f'Price {current_ltp} < {entry_price}. Sending LIMIT order with Trigger.'
                    )
                    order_type = 'LIMIT'
            else:
                logger.error('Could not fetch LTP. Skipping trade for safety.')
                return

            # 5. Auto-SL Logic
            anchor_price = entry_price
            if anchor_price == 0 and current_ltp:
                anchor_price = current_ltp

            if sl_price == 0 and anchor_price > 0:
                raw_text = signal.get('raw', '').upper()
                hero_keywords = {'HERO ZERO', 'HEROZERO', 'ZERO HERO', 'ZEROHERO'}

                if any(k in raw_text for k in hero_keywords):
                    sl_price = 0.05
                    logger.info('Hero Zero Detected: SL set to 0.05')
                else:
                    sl_price = anchor_price - 15.0
                    logger.info(f'SL Missing. Applying Auto-SL: {sl_price}')

                sl_price = max(sl_price, 0.05)

            # Ensure SL is strictly below Entry for BUY orders
            if action == 'BUY' and sl_price >= entry_price:
                adjusted_sl = entry_price - 1.0
                logger.warning(f'SL ({sl_price}) >= Entry. Adjusting to {adjusted_sl}')
                sl_price = max(adjusted_sl, 0.05)

            # 6. Price Calculations
            target_price = self.round_to_tick(entry_price * 10.0)
            sl_price = self.round_to_tick(sl_price)
            trailing_jump = self.round_to_tick(entry_price * 0.05)

            # --- TRIGGER PRICE LOGIC ---
            # If LIMIT: Trigger = Entry (80), Price = Entry + 0.5 (80.5)
            # If MARKET: Trigger = 0, Price = 0
            if order_type == 'LIMIT':
                trigger_price = self.round_to_tick(entry_price)
                price_to_send = self.round_to_tick(entry_price + 0.5)
            else:
                trigger_price = 0.0
                price_to_send = 0.0

            qty = self.calculate_quantity(entry_price, sl_price, is_positional, lot_size)
            product_type = 'MARGIN' if is_positional else 'INTRADAY'

            # 7. Payload
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

            logger.info(f'SENDING {order_type}: {trade_sym} on {exch_seg}')
            logger.debug(f'Payload: {payload}')

            # 8. Execute
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
