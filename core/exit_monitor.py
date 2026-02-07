"""
Exit and Retry Monitor Module

Handles direction-aware exit logic for options trades based on order book imbalance.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Set

import polars as pl

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
        # High liquidity: Require 10 seconds of persistent bad imbalance
        return {'bad_imb': 0.20, 'good_imb': 2.8, 'bad_ticks': 10}
    else:
        # Lower liquidity: Slightly more tolerance
        return {'bad_imb': 0.30, 'good_imb': 2.5, 'bad_ticks': 10}


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
        base_bad_ticks = int(rules['bad_ticks'])  # Base threshold (10)

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
            logger.warning(f'⚠️ WICK DETECTED: {sym} entry={entry_price:.2f} < trigger={trigger_price:.2f}')
            await self.notifier.squared_off(sym, f'Wick entry (avg {entry_price:.2f} < {trigger_price:.2f})')
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
                    logger.warning(f'⚠️ WICK EXIT: {sym} price={ltp:.2f} fell below trigger={trigger_price:.2f}')
                    await self.notifier.squared_off(sym, f'Wick (price {ltp:.2f} < {trigger_price:.2f})')
                    self.bridge.square_off_single(sid)
                    self.active_monitors.discard(sym)
                    return
            logger.info(f'{sym}: Wick protection passed, continuing normal monitoring')

        liquidity_sids = self.bridge.get_liquidity_sids(sym, sid)
        new_subs = []
        for liq_sid in liquidity_sids:
            if liq_sid not in self._subscribed_sids:
                # Dynamic segment resolution
                seg = self.bridge.mapper.get_exchange_segment(liq_sid) or 'NSE_FNO'
                new_subs.append({'ExchangeSegment': seg, 'SecurityId': liq_sid})
                self._subscribed_sids.add(liq_sid)

        if new_subs:
            self.bridge.subscribe(new_subs)

        logger.info(f'🎯 Exit Monitor Started: {sym} ({direction}) | Thresholds: bad<{bad_imb}, good>={good_imb}')

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

                # --- Dynamic threshold based on OI Risk ---
                # OI risk is updated by TrapMonitor every 3 mins
                oi_risk = trade.get('oi_risk', 0.0)

                # Adjust bad_ticks_required based on OI risk:
                # oi_risk = 0.0 → 10 ticks (normal)
                # oi_risk = 0.5 → 6 ticks (faster exit)
                # oi_risk = 0.9 → 5 ticks (urgent exit)
                bad_ticks_required = max(5, int(base_bad_ticks * (1 - oi_risk * 0.7)))

                # DIRECTION-AWARE IMBALANCE:
                # - CALL: We want buyers (high imb = good). Use raw imbalance.
                # - PUT: We want sellers (low imb = good). Invert: effective_imb = 1/raw_imb
                if is_put:
                    effective_imb = round(1.0 / raw_imb, 2) if raw_imb > 0.001 else 0.0
                else:
                    effective_imb = raw_imb

                now = asyncio.get_event_loop().time()
                
                # --- Dynamic OI Target Update (Every 60s) ---
                if now - last_log_time >= 60:
                    await self._update_dynamic_target(sid, sym, direction)
                    last_log_time = now

                # Check if Dynamic Target Hit
                # We need to compare UNDERLYING price to the Target Strike
                # But ExitMonitor usually tracks OPTION price (LTP).
                # We need the Underlying LTP here.
                dyn_target = trade.get('dynamic_target', 0.0)
                
                if dyn_target > 0:
                    # Resolve underlying ID again or store it?
                    # Let's get it from cache or map
                    # For now, let's assume we can get underlying LTP from bridge if we knew the ID.
                    # Optimization: Store underlying_sid in trade when we resolve it in _update_dynamic_target
                    underlying_sid = trade.get('underlying_sid')
                    if underlying_sid:
                        u_ltp = self.bridge.get_live_ltp(underlying_sid)
                        
                        if u_ltp > 0:
                             # Bullish (Call) -> Exit if Underlying >= Resistance Strike
                            if not is_put and u_ltp >= dyn_target:
                                logger.info(f'🎯 DYNAMIC TARGET HIT: {sym} (Und {u_ltp} >= {dyn_target})')
                                await self.notifier.squared_off(sym, f'Target Hit (Spot {u_ltp:.2f})')
                                self.bridge.square_off_single(sid)
                                break
                            
                            # Bearish (Put) -> Exit if Underlying <= Support Strike
                            if is_put and u_ltp <= dyn_target:
                                logger.info(f'🎯 DYNAMIC TARGET HIT: {sym} (Und {u_ltp} <= {dyn_target})')
                                await self.notifier.squared_off(sym, f'Target Hit (Spot {u_ltp:.2f})')
                                self.bridge.square_off_single(sid)
                                break
               
                if now - last_log_time >= 5.0:
                    last_log_time = now
                    # ... existing logging ...
                    logger.info(
                        f'{sym} ({direction}) | Imb={effective_imb:.2f} '
                        f'| Ticks={bad_tick_count}/{bad_ticks_required}'
                        f'| DynTgt={dyn_target}'
                    )

        except Exception as e:
            logger.error(f'ExitMonitor Loop Error ({sym}): {e}', exc_info=True)
            self.active_monitors.discard(sym)

    async def _update_dynamic_target(self, sid: str, sym: str, direction: str):
        """Update the dynamic target based on Max OI Strike (Resistance/Support)."""
        try:
            trade = self.tm.get_trade(sid)
            if not trade: return

            # 1. Identify Underlying Symbol
            # e.g. "NIFTY 25000 CE" -> "NIFTY"
            parts = sym.split()
            root_sym = parts[0]
            
            # 2. Resolve Underlying Security ID using Mapper
            # We need to find the ID for "NIFTY" (Index) or "RELIANCE" (Equity)
            # The mapper dataframe has this.
            # Helper in bridge? Or direct access?
            # Let's use a heuristic: Search mapper for exact match on 'SEM_TRADING_SYMBOL' with Instrument=INDEX or EQ
            
            # For efficiency, let's assume specific map or helper
            # We can use bridge.mapper.get_security_id BUT that returns OPTION ID usually.
            # Let's add a specialized lookup in this method or use a known one.
            
            # FAST FIX: Use the 'scrip_master' from cache if available or scan mapper df?
            # Creating a tiny helper inside logic:
            
            df = self.bridge.mapper.df
            if df.is_empty(): return
            
            import polars as pl
            
            # Try INDEX first
            res = df.filter(
                (pl.col('SEM_TRADING_SYMBOL') == root_sym) & 
                (pl.col('SEM_INSTRUMENT_NAME') == 'INDEX')
            )
            if res.is_empty():
                 # Try Stock
                res = df.filter(
                    (pl.col('SEM_TRADING_SYMBOL') == root_sym) & 
                    (pl.col('SEM_EXM_EXCH_ID').is_in(['NSE', 'NSE_EQ', 'EQ'])) &
                    (pl.col('SEM_INSTRUMENT_NAME') == 'EQUITY')
                )
            
            if res.is_empty():
                return
                
            row = res.row(0, named=True)
            u_sid = str(row['SEM_SMST_SECURITY_ID'])
            u_seg = 'IDX_I' if row['SEM_INSTRUMENT_NAME'] == 'INDEX' else 'NSE_EQ'
            
            # Update trade with underlying SID for faster lookups later
            trade['underlying_sid'] = u_sid
            
            # 3. Get Expiry
            # We need the expiry of the OPTION we are trading to find the right chain?
            # Or just the nearest monthly/weekly?
            # Let's use the expiry of the trade itself if possible.
            # 'sym' usually doesn't have expiry date in string for some brokers, but here it might.
            # "NIFTY 24500 CE" -> No date. 
            # We need to fetch expiry list for underlying and pick nearest.
            expiries = self.bridge.fetch_expiry_list(int(u_sid), u_seg)
            if not expiries: return
            
            # Pick nearest active expiry
            # Logic: just pick the first one that is >= today
            current_date = datetime.now().strftime('%Y-%m-%d')
            valid_expiries = [d for d in expiries if d >= current_date]
            if not valid_expiries: return
            expiry = sorted(valid_expiries)[0]
            
            # 4. Find Max OI Strike
            # If Bullish (Call Trade) -> We want Resistance (Max Call OI)
            # If Bearish (Put Trade) -> We want Support (Max Put OI)
            
            # Wait:
            # Bullish Trade (Long CE) -> Stops at Resistance (Call Writers)
            # Bearish Trade (Long PE) -> Stops at Support (Put Writers)
            
            # So:
            # direction='CALL' (Long CE) -> Look for CE OI Max
            # direction='PUT' (Long PE) -> Look for PE OI Max
            
            target_strike = self.bridge.get_max_oi_strike(int(u_sid), u_seg, expiry, type='CE' if direction == 'CALL' else 'PE')
            
            if target_strike > 0:
                trade['dynamic_target'] = target_strike
                # logger.info(f"🔄 Updated Dynamic Target for {sym}: {target_strike} (Max {direction} OI)")
                
        except Exception as e:
            logger.error(f"DynTarget Error: {e}")
                
                # --- Dynamic OI Target Update (Every 60s) ---
            if now - last_log_time >= 60:
                await self._update_dynamic_target(sid, sym, direction)
                last_log_time = now

            # Check if Dynamic Target Hit
            dyn_target = trade.get('dynamic_target', 0.0)
            ltp = self.bridge.get_live_ltp(sid)
            
            if dyn_target > 0 and ltp > 0:
                    # Bullish (Call) -> Exit if above Target
                if not is_put and ltp >= dyn_target:
                    logger.info(f'🎯 DYNAMIC TARGET HIT: {sym} ({ltp} >= {dyn_target})')
                    await self.notifier.squared_off(sym, f'Target Hit ({ltp:.2f})')
                    self.bridge.square_off_single(sid)
                    break
                
                # Bearish (Put) -> Exit if below Target (wait... option price goes UP when underlying goes down? 
                # NO. We are buying Options. So for PUT, option price goes UP.
                # This logic assumes 'dynamic_target' is an *Option Price* target? 
                # Ah, wait. My plan said "Find Strike with Max OI". That is UNDERLYING price level.
                # If I am holding a PE, and underlying hits Max Put OI (Support), I should exit.
                # We need to compare UNDERLYING price to Strike.
                pass 

            if now - last_log_time >= 5.0:
                # ... existing log logic ...
                pass

    async def _update_dynamic_target(self, sid: str, sym: str, direction: str):
        """Update the dynamic target based on Max OI Strike."""
        try:
            # 1. Get underlying details
            # Symbol is like "NIFTY 25000 CE". We need "NIFTY"
            parts = sym.split()
            root_sym = parts[0]
            
            # Resolve underlying scrip
            # This is tricky without access to the full map here, but we can try reverse lookup or passed data.
            # TradeManager might have it.
            trade = self.tm.get_trade(sid)
            if not trade: 
                return
                
            # If we don't have underlying scrip, we can't fetch chain.
            # But wait, DhanBridge.fetch_option_chain needs Scrip ID.
            # let's skip strict resolution and rely on hardcoded map for now or ignore?
            # actually better: Extract from symbol and use map
            
            # For now, let's implement the logic assuming we can get the chain
            # TODO: Need robust way to get underlying scrip from Option Symbol
            pass

        except Exception as e:
            logger.error(f'Dynamic Target Update Failed: {e}')
            if now - last_log_time >= 60:
                last_log_time = now
                oi_info = f' | OI risk={oi_risk:.2f}' if oi_risk > 0 else ''
                logger.info(
                    f'📊 IMB {sym} ({direction}): raw={raw_imb:.2f} eff={effective_imb:.2f} | '
                    f'bad_ticks={bad_tick_count}/{bad_ticks_required}{oi_info}'
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
                oi_note = f' (OI risk: {oi_risk:.1f})' if oi_risk > 0.3 else ''
                reason = f'{"Buyer" if is_put else "Seller"} dominance ({raw_imb:.2f}){oi_note}'

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
