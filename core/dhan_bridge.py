import logging
import math
import os
from datetime import datetime

import requests
from dhanhq import dhanhq

from core.dhan_mapper import DhanMapper

# Setup Logger
logger = logging.getLogger("DhanBridge")


class DhanBridge:
    def __init__(self):
        self.client_id = os.getenv("DHAN_CLIENT_ID")
        self.access_token = os.getenv("DHAN_ACCESS_TOKEN")
        self.base_url = "https://api.dhan.co/v2"

        if self.client_id and self.access_token:
            self.dhan = dhanhq(self.client_id, self.access_token)
            logger.info("✅ Dhan Bridge Connected")
        else:
            self.dhan = None
            logger.warning("⚠️ Running in Mock Mode")

        self.mapper = DhanMapper()

        self.lot_sizes = {"NIFTY": 75, "BANKNIFTY": 35, "SENSEX": 20}

        # Risk per trade
        self.RISK_INTRADAY = 3500
        self.RISK_POSITIONAL = 5000

    def parse_date_label(self, label):
        try:
            if not label:
                return None
            parts = label.split()
            day, mon_str = int(parts[0]), parts[1]
            month_map = {
                "JAN": 1,
                "FEB": 2,
                "MAR": 3,
                "APR": 4,
                "MAY": 5,
                "JUN": 6,
                "JUL": 7,
                "AUG": 8,
                "SEP": 9,
                "OCT": 10,
                "NOV": 11,
                "DEC": 12,
            }

            now = datetime.now()
            month = month_map[mon_str.upper()]
            year = now.year if month >= now.month else now.year + 1
            return datetime(year, month, day).date()
        except Exception:
            return None

    def get_ltp(self, security_id, exchange_segment):
        if not self.access_token:
            return None
        try:
            url = f"{self.base_url}/marketfeed/ltp"
            payload = {
                "instruments": [
                    {
                        "exchangeSegment": exchange_segment,
                        "securityId": str(security_id),
                    }
                ]
            }
            headers = {
                "access-token": self.access_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            response = requests.post(url, headers=headers, json=payload)
            data = response.json()

            if data.get("data"):
                key = f"{exchange_segment}:{security_id}"
                item = data["data"].get(key)
                if item:
                    return float(item.get("last_price", 0))
            return None
        except Exception as e:
            logger.error(f"LTP Fetch Error: {e}")
            return None

    def calculate_quantity(self, entry_price, sl_price, is_positional, lot_size):
        try:
            risk_capital = self.RISK_POSITIONAL if is_positional else self.RISK_INTRADAY
            sl_gap = abs(entry_price - sl_price)

            if sl_gap < 1:
                return lot_size

            raw_qty = risk_capital / sl_gap
            num_lots = round(raw_qty / lot_size)
            if num_lots < 1:
                num_lots = 1

            final_qty = math.ceil(num_lots * lot_size)
            logger.info(
                f"🧮 Qty Calc: Risk ₹{risk_capital} | Gap {sl_gap:.1f} | {num_lots} Lots -> {final_qty}"
            )
            return final_qty
        except Exception:
            return lot_size

    def execute_super_order(self, signal):
        if not self.dhan:
            return

        try:
            # 1. Unpack Signal
            sym = signal.get("underlying")
            trade_sym = signal.get("trading_symbol")
            label = signal.get("expiry_label")
            action = signal.get("action")
            is_positional = signal.get("is_positional", False)
            entry_price = float(signal.get("trigger_above") or 0)

            target_date = self.parse_date_label(label)
            sec_id, exch_id, lot_size = self.mapper.get_security_id(trade_sym)

            if not sec_id:
                logger.error(f"❌ ID Missing for {trade_sym}")
                return

            exch_seg_str = (
                "BSE_FNO" if (exch_id == "BSE" or sym == "SENSEX") else "NSE_FNO"
            )

            # 2. LTP Logic (Market vs Limit)
            current_ltp = self.get_ltp(sec_id, exch_seg_str)

            order_type = "LIMIT"

            if current_ltp:
                if entry_price == 0:
                    entry_price = current_ltp

                threshold = entry_price + (entry_price * 0.03)

                if current_ltp > threshold:
                    logger.warning(f"⚠️ SKIPPING: Price flew >3%. LTP: {current_ltp}")
                    return
                elif current_ltp >= entry_price:
                    logger.info(
                        f"⚡ BREAKOUT ({current_ltp} > {entry_price}). MARKET Order."
                    )
                    order_type = "MARKET"
                else:
                    logger.info(f"⏳ Waiting for trigger. LIMIT Order.")
                    order_type = "LIMIT"
            else:
                if entry_price == 0:
                    return

            # 3. Strategy Params
            sl_price = float(signal.get("stop_loss") or (entry_price * 0.90))
            target_price = round(entry_price * 5.0, 2)
            trailing_jump = round(entry_price * 0.05, 2)
            qty = self.calculate_quantity(
                entry_price, sl_price, is_positional, lot_size
            )
            product_type = "MARGIN" if is_positional else "INTRADAY"

            # 4. PRICE FIX: 0 for Market, Entry for Limit
            price_to_send = entry_price if order_type == "LIMIT" else 0

            # 5. Payload
            payload = {
                "dhanClientId": self.client_id,
                "correlationId": f"BOT-{int(datetime.now().timestamp())}",
                "transactionType": "BUY" if action == "BUY" else "SELL",
                "exchangeSegment": exch_seg_str,
                "productType": product_type,
                "orderType": order_type,
                "securityId": str(sec_id),
                "quantity": int(qty),
                "price": float(price_to_send),  # <--- 0 if Market, Entry if Limit
                "targetPrice": float(target_price),
                "stopLossPrice": float(sl_price),
                "trailingJump": float(trailing_jump),
            }

            logger.info(f"🚀 FIRING {order_type} ({product_type}): {trade_sym}")

            # 6. Execute
            url = f"{self.base_url}/super/orders"
            headers = {
                "access-token": self.access_token,
                "Content-Type": "application/json",
            }

            response = requests.post(url, headers=headers, json=payload)
            data = response.json()

            if response.status_code == 200 or data.get("orderStatus") == "PENDING":
                logger.info(f"🎉 SUCCESS! ID: {data.get('orderId')}")
            else:
                logger.error(f"⚠️ REJECTED: {data}")

        except Exception as e:
            logger.critical(f"Execution Error: {e}")
