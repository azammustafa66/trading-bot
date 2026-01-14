"""
Signal Batcher Module

Batches incoming Telegram messages and coordinates signal processing,
order execution, and exit monitoring.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from core.dhan_bridge import DhanBridge
from core.exit_monitor import ExitMonitor, RetryMonitor
from core.notifier import Notifier
from core.signal_parser import process_and_save

logger = logging.getLogger('SignalBatcher')

SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')
BATCH_DELAY_SECONDS = 1.5


class ChannelState:
    """Tracks paused channels (pause until end of day)."""

    def __init__(self):
        self._paused_until: Dict[int, datetime] = {}

    def pause(self, channel_id: int):
        self._paused_until[channel_id] = datetime.now().replace(
            hour=23, minute=59, second=59
        )

    def resume(self, channel_id: int):
        self._paused_until.pop(channel_id, None)

    def is_paused(self, channel_id: int) -> bool:
        until = self._paused_until.get(channel_id)
        if not until:
            return False
        if datetime.now() > until:
            del self._paused_until[channel_id]
            return False
        return True


class SignalBatcher:
    """
    Batches Telegram messages and coordinates the trading pipeline:
    1. Collects messages with delay for multi-part signals
    2. Parses signals
    3. Executes orders
    4. Starts exit monitors
    """

    def __init__(self, bridge: DhanBridge, notifier: Notifier):
        self.bridge = bridge
        self.notifier = notifier
        self.tm = bridge.trade_manager
        self.active_monitors: Set[str] = set()
        self.channel_state = ChannelState()
        self.batch_msgs: List[str] = []
        self.batch_dates: List[datetime] = []
        self._timer: Optional[asyncio.Task] = None
        self._subscribed_sids: Set[str] = set()
        self._resume_active_trades()

    def _resume_active_trades(self):
        """Resume exit monitors for trades that survived a restart."""
        trades = self.tm.get_all_open_trades()
        loop = asyncio.get_running_loop()

        for t in trades:
            sym = str(t['symbol'])
            sid = str(t['security_id'])
            logger.info(f'🔄 Resuming Exit Monitor: {sym}')
            self.active_monitors.add(sym)
            loop.create_task(self._start_exit_monitor(sym, sid))

    async def add_message(self, text: str, dt: datetime, channel_id: int):
        """Add a message to the batch for processing."""
        if self.channel_state.is_paused(channel_id):
            return

        self.batch_msgs.append(text)
        self.batch_dates.append(dt)

        if self._timer:
            self._timer.cancel()

        loop = asyncio.get_running_loop()
        self._timer = loop.create_task(self._process_batch())

    async def _process_batch(self):
        """Process batched messages after delay."""
        await asyncio.sleep(BATCH_DELAY_SECONDS)

        try:
            signals = process_and_save(
                self.batch_msgs, self.batch_dates, SIGNALS_JSONL, SIGNALS_JSON
            )
        except Exception as e:
            logger.error(f'Parser error: {e}')
            signals = []

        self.batch_msgs.clear()
        self.batch_dates.clear()

        loop = asyncio.get_running_loop()

        for sig in signals:
            sym = sig.get('trading_symbol', '')
            if not isinstance(sym, str):
                continue

            if sym in self.active_monitors:
                continue

            try:
                # 1. First Execution Attempt
                ltp, status = await asyncio.to_thread(
                    self.bridge.execute_super_order, sig
                )

                # 2. Success Case
                if status == 'SUCCESS':
                    await self.notifier.order_placed(sym, 0, ltp)
                    sid, _, _, _ = self.bridge.mapper.get_security_id(
                        sym, ltp, self.bridge.get_live_ltp
                    )
                    if sid:
                        self.active_monitors.add(sym)
                        loop.create_task(self._start_exit_monitor(sym, str(sid)))

                # 3. Retry if price not at trigger yet
                elif status in ['PRICE_LOW', 'PRICE_HIGH']:
                    await self.notifier.retrying(sym, status)
                    logger.info(f'⏳ Price {status} for {sym}. Starting Retry Monitor.')
                    self.active_monitors.add(sym)
                    loop.create_task(self._start_retry_monitor(sig))

                elif status == 'ERROR':
                    await self.notifier.order_failed(sym, 'Execution error')
                else:
                    logger.warning(f'Unexpected status: {status}')

            except Exception as e:
                logger.warning(f'Error processing signal: {e}')

    async def _start_exit_monitor(self, sym: str, sid: str):
        """Create and run an exit monitor for a trade."""
        monitor = ExitMonitor(
            bridge=self.bridge,
            notifier=self.notifier,
            trade_manager=self.tm,
            active_monitors=self.active_monitors,
            subscribed_sids=self._subscribed_sids,
        )
        await monitor.run(sym, sid)

    async def _start_retry_monitor(self, sig: Dict[str, Any]):
        """Create and run a retry monitor for a pending signal."""
        monitor = RetryMonitor(
            bridge=self.bridge,
            trade_manager=self.tm,
            active_monitors=self.active_monitors,
        )

        async def on_success(sym: str, sid: str):
            asyncio.get_running_loop().create_task(
                self._start_exit_monitor(sym, sid)
            )

        await monitor.run(sig, on_success)
