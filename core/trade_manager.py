"""
Trade Manager Module.

Maintains a persistent ledger of active trades, supporting crash recovery
and position reconciliation with the broker.

Trades are stored in JSON format and include:
- Entry details (price, time, order ID)
- Option direction (call/put)
- Associated futures SID for liquidity monitoring
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List, Optional

logger = logging.getLogger('TradeManager')

TRADES_FILE = 'data/active_trades.json'


class TradeManager:
    """
    Thread-safe manager for active trade records.

    Persists trade state to disk for crash recovery. Supports reconciliation
    with broker positions to clean up stale trades.

    Attributes:
        file_path: Path to the trades JSON file.
        active_trades: In-memory dictionary of active trades.

    Example:
        >>> tm = TradeManager()
        >>> tm.add_trade(signal, order_data, "12345", "67890")
        >>> trade = tm.get_trade("12345")
        >>> tm.remove_trade("12345")
    """

    def __init__(self) -> None:
        """Initialize the trade manager and load existing trades."""
        self.file_path = TRADES_FILE
        self._lock = Lock()
        self._ensure_file()
        self.active_trades = self._load_trades()

    def _ensure_file(self) -> None:
        """Ensure the data directory and trades file exist."""
        os.makedirs('data', exist_ok=True)
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                json.dump({}, f)

    def _load_trades(self) -> Dict[str, Dict[str, Any]]:
        """Load trades from disk."""
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def _save_trades(self) -> None:
        """Atomically save trades to disk."""
        tmp_path = f'{self.file_path}.tmp'
        with open(tmp_path, 'w') as f:
            json.dump(self.active_trades, f, indent=2)
        os.replace(tmp_path, self.file_path)

    def add_trade(
        self,
        signal: Dict[str, Any],
        order_data: Dict[str, Any],
        sec_id: str,
        fut_sid: Optional[str] = None,
    ) -> None:
        """
        Register a new executed trade.

        Args:
            signal: Original trading signal dictionary.
            order_data: Response from broker order placement.
            sec_id: Security ID of the traded option.
            fut_sid: Security ID of underlying future (optional).
        """
        symbol = signal.get('trading_symbol', '').upper()

        entry_data = {
            'symbol': symbol,
            'security_id': str(sec_id),
            'order_id': order_data.get('orderId'),
            'entry_price': (
                order_data.get('averagePrice')
                or order_data.get('tradedPrice')
                or signal.get('trigger_above', 0.0)
            ),
            'is_call': 'CE' in symbol or 'CALL' in symbol,
            'is_put': 'PE' in symbol or 'PUT' in symbol,
            'fut_sid': str(fut_sid) if fut_sid else None,
            'status': 'OPEN',
            'entry_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'signal_details': signal,
        }

        with self._lock:
            self.active_trades[str(sec_id)] = entry_data
            self._save_trades()

        logger.info(f'Trade logged: {symbol} (ID: {sec_id})')

    def remove_trade(self, sec_id: str) -> None:
        """
        Remove a trade after exit.

        Args:
            sec_id: Security ID of the trade to remove.
        """
        sid = str(sec_id)
        with self._lock:
            if sid in self.active_trades:
                del self.active_trades[sid]
                self._save_trades()
                logger.info(f'Trade removed: {sid}')

    def get_trade(self, sec_id: str) -> Optional[Dict[str, Any]]:
        """
        Get trade details by security ID.

        Args:
            sec_id: Security ID to look up.

        Returns:
            Trade dictionary or None if not found.
        """
        with self._lock:
            self.active_trades = self._load_trades()
            return self.active_trades.get(str(sec_id))

    def get_all_open_trades(self) -> List[Dict[str, Any]]:
        """
        Get all currently open trades.

        Returns:
            List of trade dictionaries.
        """
        with self._lock:
            self.active_trades = self._load_trades()
            return list(self.active_trades.values())

    def get_all_sids(self) -> List[str]:
        """
        Get all security IDs (options + futures) for active trades.

        Returns:
            List of unique security ID strings.
        """
        sids: set[str] = set()

        with self._lock:
            self.active_trades = self._load_trades()
            for trade in self.active_trades.values():
                if trade.get('security_id'):
                    sids.add(str(trade['security_id']))
                if trade.get('fut_sid'):
                    sids.add(str(trade['fut_sid']))

        return list(sids)

    def reconcile_with_positions(self, open_positions: List[str]) -> List[Dict[str, Any]]:
        """
        Remove trades not present in broker positions.

        Used to clean up ghost trades after manual exits or system issues.

        Args:
            open_positions: List of security IDs with open positions at broker.

        Returns:
            List of removed trade dictionaries (for cleanup actions).
        """
        removed: List[Dict[str, Any]] = []

        with self._lock:
            for sid, trade in list(self.active_trades.items()):
                if sid not in open_positions:
                    removed.append(trade)
                    del self.active_trades[sid]

            if removed:
                self._save_trades()

        return removed
