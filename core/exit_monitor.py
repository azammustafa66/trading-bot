"""
Exit and Retry Monitor Module

Handles direction-aware exit logic for options trades based on order book imbalance.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, Set

if TYPE_CHECKING:
    from core.dhan_bridge import DhanBridge
    from core.notifier import Notifier
    from core.trade_manager import TradeManager

logger = logging.getLogger('ExitMonitor')


def get_imbalance_rules(sym: str) -> Dict[str, float]:
    """
    Returns imbalance thresholds based on instrument type.
    Index options have tighter thresholds due to higher liquidity.
    """
    if 'NIFTY' in sym.upper() or 'BANKNIFTY' in sym.upper() or 'SENSEX' in sym.upper():
        return {'bad_imb': 0.20, 'good_imb': 2.8, 'bad_ticks': 4}
    else:
        return {'bad_imb': 0.35, 'good_imb': 2.2, 'bad_ticks': 6}


class ExitMonitor:
    """
    Monitors active trades and exits based on order book imbalance.
    Direction-aware: CALLs exit on seller dominance, PUTs exit on buyer dominance.
    """

    def __init__(
        self,
        bridge: DhanBridge,
        notifier: Notifier,
        trade_manager: TradeManager,
        active_monitors: Set[str],
        subscribed_sids: Set[str],
    ):
        self.bridge = bridge
        self.notifier = notifier
        self.tm = trade_manager
        self.active_monitors = active_monitors
        self._subscribed_sids = subscribed_sids

    async def run(self, sym: str, sid: str):
        """Main exit monitor loop for a single trade."""
        rules = get_imbalance_rules(sym)
        bad_imb = rules['bad_imb']
        good_imb = rules['good_imb']
        bad_ticks_required = int(rules['bad_ticks'])

        bad_tick_count = 0

        await asyncio.sleep(3)

        # Determine direction from trade data
        trade = self.tm.get_trade(sid)
        if not trade:
            return
        is_put = trade.get('is_put', False)
        direction = 'PUT' if is_put else 'CALL'

        # --- WICK DETECTION: Check if entry price is below trigger ---
        entry_price = float(trade.get('entry_price') or 0)
        signal_details = trade.get('signal_details', {})
        trigger_price = float(signal_details.get('trigger_above') or 0)

        if trigger_price > 0 and entry_price > 0 and entry_price < trigger_price:
            logger.warning(
                f'⚠️ WICK DETECTED: {sym} entry={entry_price:.2f} < trigger={trigger_price:.2f}'
            )
            await self.notifier.squared_off(
                sym, f'Wick entry (avg {entry_price:.2f} < {trigger_price:.2f})'
            )
            self.bridge.square_off_single(sid)
            self.active_monitors.discard(sym)
            return

        # --- WICK DETECTION: First 10 seconds - ensure price sustains above trigger ---
        if trigger_price > 0:
            logger.info(f'{sym}: Monitoring wick protection for 10s (trigger={trigger_price:.2f})')
            for _ in range(5):  # 5 checks × 2s = 10s
                await asyncio.sleep(2)
                ltp = self.bridge.get_live_ltp(sid)
                if ltp > 0 and ltp < trigger_price * 0.995:  # Allow 0.5% tolerance
                    logger.warning(
                        f'⚠️ WICK EXIT: {sym} price={ltp:.2f} fell below trigger={trigger_price:.2f}'
                    )
                    await self.notifier.squared_off(
                        sym, f'Wick (price {ltp:.2f} < {trigger_price:.2f})'
                    )
                    self.bridge.square_off_single(sid)
                    self.active_monitors.discard(sym)
                    return
            logger.info(f'{sym}: Wick protection passed, continuing normal monitoring')

        liquidity_sids = self.bridge.get_liquidity_sids(sym, sid)
        new_subs = []
        new_subs = []
        for liq_sid in liquidity_sids:
            if liq_sid not in self._subscribed_sids:
                # Dynamic segment resolution
                seg = self.bridge.mapper.get_exchange_segment(liq_sid) or 'NSE_FNO'
                new_subs.append({'ExchangeSegment': seg, 'SecurityId': liq_sid})
                self._subscribed_sids.add(liq_sid)

        if new_subs:
            self.bridge.subscribe(new_subs)

        logger.info(
            f'🎯 Exit Monitor Started: {sym} ({direction}) | '
            f'Thresholds: bad<{bad_imb}, good>={good_imb}'
        )

        last_log_time = 0

        try:
            while True:
                # Wait for depth update (event-driven) with timeout
                try:
                    await asyncio.wait_for(
                        self.bridge.depth_updated.wait(),
                        timeout=2.0,  # Fallback if no updates
                    )
                    self.bridge.depth_updated.clear()
                except asyncio.TimeoutError:
                    pass  # Continue to check even if no update

                trade = self.tm.get_trade(sid)
                if not trade:
                    break

                raw_imb = self.bridge.get_combined_imbalance(liquidity_sids)

                # DIRECTION-AWARE IMBALANCE:
                # - CALL: We want buyers (high imb = good). Use raw imbalance.
                # - PUT: We want sellers (low imb = good). Invert: effective_imb = 1/raw_imb
                if is_put:
                    effective_imb = round(1.0 / raw_imb, 2) if raw_imb > 0 else 0.0
                else:
                    effective_imb = raw_imb

                now = asyncio.get_event_loop().time()
                if now - last_log_time >= 60:
                    last_log_time = now
                    logger.info(
                        f'📊 IMB {sym} ({direction}): raw={raw_imb:.2f} eff={effective_imb:.2f} | '
                        f'bad_ticks={bad_tick_count}/{bad_ticks_required}'
                    )

                # Use effective_imb for threshold checks
                if effective_imb >= good_imb:
                    if bad_tick_count > 0:
                        logger.info(f'{sym} liquidity recovered ({effective_imb:.2f}), resetting')
                    bad_tick_count = 0
                    continue

                if effective_imb < bad_imb:
                    bad_tick_count += 1
                    logger.warning(
                        f'{sym} ({direction}) bad imbalance eff={effective_imb:.2f} '
                        f'raw={raw_imb:.2f} ({bad_tick_count}/{bad_ticks_required})'
                    )
                else:
                    bad_tick_count = max(0, bad_tick_count - 1)

                if bad_tick_count >= bad_ticks_required:
                    reason = f'{"Buyer" if is_put else "Seller"} dominance ({raw_imb:.2f})'

                    if trade.get('is_manual', False):
                        # Manual Trade: ALERT ONLY
                        logger.critical(f'🚨 MANUAL TRADE ALERT: {sym} ({direction}) - {reason}')
                        # Reset counter to avoid spamming every update, or keep warning?
                        # Let's reset purely to allow "re-alerting" later if it persists
                        bad_tick_count = 0
                    else:
                        # Auto Trade: EXECUTE EXIT
                        await self.notifier.squared_off(sym, reason)
                        logger.critical(f'⚠️ Exit Triggered: {sym} ({direction}) - {reason}')
                        self.bridge.square_off_single(sid)
                        break

        finally:
            self.active_monitors.discard(sym)


class RetryMonitor:
    """
    Monitors price and retries order execution when breakout trigger is hit.
    """

    def __init__(self, bridge: DhanBridge, trade_manager: TradeManager, active_monitors: Set[str]):
        self.bridge = bridge
        self.tm = trade_manager
        self.active_monitors = active_monitors

    async def run(self, sig: Dict[str, Any], on_success_callback):
        """
        Waits for price to hit trigger, then executes order.

        Args:
            sig: Signal dictionary with trading_symbol, trigger_above, etc.
            on_success_callback: Async function(sym, sid) to call on successful execution.
        """
        sym = str(sig.get('trading_symbol', ''))
        entry = float(sig.get('trigger_above', 0))

        sid, _, _, _ = self.bridge.mapper.get_security_id(sym, entry, self.bridge.get_live_ltp)
        if not sid:
            self.active_monitors.discard(sym)
            return

        sid_str = str(sid)
        liquidity_sids = self.bridge.get_liquidity_sids(sym, sid_str)
        subs = [{'ExchangeSegment': 'NSE_FNO', 'SecurityId': s} for s in liquidity_sids]
        self.bridge.subscribe(subs)

        try:
            cnt = 0
            for _ in range(1800):  # 5 hours max (1800 * 10s)
                if self.bridge.kill_switch_triggered:
                    return

                await asyncio.sleep(5.0)

                ltp = self.bridge.get_live_ltp(sid_str)
                if ltp == 0:
                    continue

                if ltp >= entry:
                    cnt += 1
                else:
                    cnt = 0

                if cnt >= 3:
                    logger.info(f'⚡ Trigger Hit: {sym} ({ltp} >= {entry}). Executing!')
                    _, status = await asyncio.to_thread(self.bridge.execute_super_order, sig)

                    if status == 'SUCCESS':
                        await on_success_callback(sym, sid_str)
                    return
        finally:
            if not self.tm.get_trade(sid_str):
                self.active_monitors.discard(sym)
