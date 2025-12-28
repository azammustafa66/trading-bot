import json
import logging
import os
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List, Optional

logger = logging.getLogger('TradeManager')

TRADES_FILE = 'data/active_trades.json'


class TradeManager:
    def __init__(self):
        self.file_path = TRADES_FILE
        self._lock = Lock()
        self._ensure_file()
        self.active_trades = self._load_trades()

    def _ensure_file(self):
        if not os.path.exists('data'):
            os.makedirs('data', exist_ok=True)
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                json.dump({}, f)

    def _load_trades(self) -> Dict[str, Dict[str, Any]]:
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_trades(self):
        tmp = f'{self.file_path}.tmp'
        with open(tmp, 'w') as f:
            json.dump(self.active_trades, f, indent=2)
        os.replace(tmp, self.file_path)

    def add_trade(self, signal: Dict[str, Any], order_data: Dict[str, Any], sec_id: str):
        """
        Registers a new executed trade.
        """
        # Determine direction for easier exit logic later
        symbol = signal.get('trading_symbol', '').upper()
        is_call = 'CE' in symbol or 'CALL' in symbol
        is_put = 'PE' in symbol or 'PUT' in symbol

        trade_id = str(sec_id)  # Use Security ID as key for easy lookup

        entry_data = {
            'symbol': symbol,
            'security_id': str(sec_id),
            'order_id': order_data.get('orderId'),
            'entry_price': (
                order_data.get('averagePrice')
                or order_data.get('tradedPrice')
                or signal.get('trigger_above', 0.0)
            ),
            'is_call': is_call,
            'is_put': is_put,
            'entry_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'signal_details': signal,
        }

        with self._lock:
            self.active_trades[trade_id] = entry_data
            self._save_trades()
        logger.info(f'📝 Trade Logged: {symbol} (ID: {sec_id})')

    def remove_trade(self, sec_id: str):
        """Removes a trade from the ledger (after exit)."""
        sid = str(sec_id)
        if sid in self.active_trades:
            with self._lock:
                del self.active_trades[sid]
                self._save_trades()
            logger.info(f'🗑️ Trade Removed: {sid}')

    def get_all_open_trades(self) -> List[Dict[str, Any]]:
        with self._lock:
            self.active_trades = self._load_trades()
            return list(self.active_trades.values())

    def get_trade(self, sec_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            self.active_trades = self._load_trades()
            return self.active_trades.get(str(sec_id))
