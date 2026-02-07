"""
Trap Monitor Service.

Orchestrates the "Writers Trap" strategy by periodically scanning option chains,
validating signals with Wick/Momentum checks, and executing trades via DhanBridge.
"""

import logging
import time
from datetime import datetime
from threading import Event, Thread
from typing import Dict

import polars as pl

from core.dhan_bridge import DhanBridge
from core.strategies.writers_trap import WritersTrapStrategy

logger = logging.getLogger('TrapMonitor')


class TrapMonitor:
    def __init__(self, bridge: DhanBridge, trade_manager=None, positional_scanner=None, dry_run: bool = False):
        self.bridge = bridge
        self.trade_manager = trade_manager
        self.positional_scanner = positional_scanner
        self.strategy = WritersTrapStrategy()
        self.dry_run = dry_run
        self.running = False
        self.stop_event = Event()
        self._thread = None

        # State tracking for Wick Protection
        # { 'SID_TRAPTYPE': {'start_time': float, 'notified': bool} }
        self.pending_traps = {}
        self._last_pos_check = 0

        # Configuration
        self.WICK_TIME_SEC = 5
        self.IMBALANCE_THRESHOLD = 2.0
        self.POS_CHECK_INTERVAL = 180  # 3 Minutes for OI delta tracking

        # Scrips will be resolved dynamically
        self.MONITORED_SCRIPS = []

    def start(self):
        """Start the monitor loop in a background thread."""
        if self.running:
            return

        # Resolve instruments before starting
        self.resolve_instruments()

        if not self.MONITORED_SCRIPS:
            logger.error('❌ No instruments resolved. Monitor will not start.')
            return

        self.running = True
        self.stop_event.clear()
        self._thread = Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info('🕸️ Trap Monitor Started')

    def stop(self):
        """Stop the monitor loop."""
        self.running = False
        self.stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        logger.info('🕸️ Trap Monitor Stopped')

    def resolve_instruments(self):
        """Dynamically resolve Security IDs for monitored instruments."""
        logger.info('🔍 Resolving target instruments...')
        targets = []
        mapper = self.bridge.mapper

        try:
            # 1. NIFTY
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == 'NIFTY').filter(
                pl.col('SEM_INSTRUMENT_NAME') == 'INDEX'
            )
            if not res.is_empty():
                row = res.row(0, named=True)
                targets.append({'name': 'NIFTY', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': 'IDX_I'})

            # 2. BANKNIFTY
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == 'BANKNIFTY').filter(
                pl.col('SEM_INSTRUMENT_NAME') == 'INDEX'
            )
            if not res.is_empty():
                row = res.row(0, named=True)
                targets.append({'name': 'BANKNIFTY', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': 'IDX_I'})

            # 3. SENSEX
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == 'SENSEX').filter(
                pl.col('SEM_INSTRUMENT_NAME') == 'INDEX'
            )
            if not res.is_empty():
                row = res.row(0, named=True)
                targets.append({'name': 'SENSEX', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': 'IDX_I'})

            # 4. FINNIFTY
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == 'FINNIFTY').filter(
                pl.col('SEM_INSTRUMENT_NAME') == 'INDEX'
            )
            if not res.is_empty():
                row = res.row(0, named=True)
                targets.append({'name': 'FINNIFTY', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': 'IDX_I'})

            # 5. MIDCPNIFTY
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == 'MIDCPNIFTY').filter(
                pl.col('SEM_INSTRUMENT_NAME') == 'INDEX'
            )
            if not res.is_empty():
                row = res.row(0, named=True)
                targets.append({'name': 'MIDCPNIFTY', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': 'IDX_I'})

            # --- STOCKS (5 liquid F&O stocks) ---
            stock_symbols = ['RELIANCE', 'ICICIBANK', 'HDFCBANK', 'INFY', 'SBIN']

            for sym in stock_symbols:
                res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == sym)
                candidate = res.filter(pl.col('SEM_EXM_EXCH_ID').is_in(['NSE_EQ', 'NSE', 'EQ']))
                if not candidate.is_empty():
                    row = candidate.row(0, named=True)
                    seg = row['SEM_EXM_EXCH_ID']
                    if seg == 'NSE':
                        seg = 'NSE_EQ'
                    targets.append({'name': sym, 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': seg})

            self.MONITORED_SCRIPS = targets
            logger.info(f'✅ Resolved Targets: {[t["name"] for t in targets]}')

        except Exception as e:
            logger.error(f'Instrument Resolution Failed: {e}', exc_info=True)

    def _resolve_single_instrument(self, symbol: str) -> Dict | None:
        """Dynamically resolve a single instrument by symbol name."""
        mapper = self.bridge.mapper

        try:
            # Try as INDEX first
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == symbol).filter(
                pl.col('SEM_INSTRUMENT_NAME') == 'INDEX'
            )
            if not res.is_empty():
                row = res.row(0, named=True)
                return {'name': symbol, 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': 'IDX_I'}

            # Try as EQUITY (Stock)
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL') == symbol)
            candidate = res.filter(pl.col('SEM_EXM_EXCH_ID').is_in(['NSE_EQ', 'NSE', 'EQ']))
            if not candidate.is_empty():
                row = candidate.row(0, named=True)
                seg = row['SEM_EXM_EXCH_ID']
                if seg == 'NSE':
                    seg = 'NSE_EQ'
                return {'name': symbol, 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': seg}

            return None
        except Exception as e:
            logger.error(f'Failed to resolve {symbol}: {e}')
            return None

    def _run_loop(self):
        """Main monitoring loop."""
        # Rate limit compliance (Dhan Data API: 100k/day)
        # With 5 targets, scanning every 3 mins = 5 * 20 * 8 = 800 calls/day (safe margin)
        SCAN_INTERVAL = 180  # 3 minutes
        last_scan = 0

        while not self.stop_event.is_set():
            try:
                now = time.time()

                # Scan Targets (Every 3 mins, batched: 3 at a time, 5s wait)
                if now - last_scan > SCAN_INTERVAL:
                    logger.info('🔍 Scanning Option Chains...')
                    batch_size = 3
                    for i in range(0, len(self.MONITORED_SCRIPS), batch_size):
                        batch = self.MONITORED_SCRIPS[i : i + batch_size]
                        for target in batch:
                            self._process_target(target)
                        if i + batch_size < len(self.MONITORED_SCRIPS):
                            time.sleep(5)  # Wait 5s between batches
                    last_scan = now

                if now - self._last_pos_check > self.POS_CHECK_INTERVAL:
                    self._monitor_open_positions()
                    self._last_pos_check = now

                time.sleep(5.0)

            except Exception as e:
                logger.error(f'Monitor Loop Error: {e}', exc_info=True)
                time.sleep(30)

    def _monitor_open_positions(self):
        """Check health of all open positions - dynamically resolves instruments."""
        logger.info('🕵️ Checking Position Health...')
        try:
            # 1. Get ALL Positions from Dhan
            positions = self.bridge.get_positions()
            if not positions:
                return

            open_positions = [p for p in positions if p.get('positionType') != 'CLOSED' and p.get('netQty', 0) != 0]
            if not open_positions:
                logger.info('No open positions to check.')
                return

            # 2. Extract unique underlyings from positions
            # Trading symbol format: "NIFTY 25600 CE" or "RELIANCE 1450 PE"
            underlyings = {}
            for pos in open_positions:
                sym = pos.get('tradingSymbol', '')
                parts = sym.split()
                if len(parts) >= 3:
                    underlying = parts[0]  # e.g., "NIFTY", "RELIANCE"
                    if underlying not in underlyings:
                        underlyings[underlying] = []
                    underlyings[underlying].append(pos)

            logger.info(f'📊 Monitoring {len(underlyings)} underlyings: {list(underlyings.keys())}')

            # 3. Process each underlying
            for underlying, pos_list in underlyings.items():
                # Try to find in predefined list first
                target = next((t for t in self.MONITORED_SCRIPS if t['name'] == underlying), None)

                # If not in predefined list, resolve dynamically
                if not target:
                    target = self._resolve_single_instrument(underlying)
                    if not target:
                        logger.warning(f'⚠️ Could not resolve instrument: {underlying}')
                        continue

                logger.info(f'Checking {len(pos_list)} positions for {underlying}')

                # Fetch Option Chain
                expiries = self.bridge.fetch_expiry_list(target['scrip'], target['seg'])
                if not expiries:
                    continue

                current_date_str = datetime.now().strftime('%Y-%m-%d')
                valid_expiries = [d for d in expiries if d >= current_date_str]
                if not valid_expiries:
                    continue
                expiry = sorted(valid_expiries)[0]

                data = self.bridge.fetch_option_chain(target['scrip'], target['seg'], expiry)
                if not data or 'oc' not in data:
                    continue

                oc_data = data['oc']

                for pos in pos_list:
                    p_adapter = {
                        'trading_symbol': pos.get('tradingSymbol'),
                        'quantity': pos.get('netQty', 0),
                        'type': 'BUY' if pos.get('netQty', 0) > 0 else 'SELL',
                    }

                    # Only protect LONG positions
                    if p_adapter['quantity'] <= 0:
                        continue

                    health = self.strategy.check_position_health(p_adapter, oc_data)
                    logger.info(f'Health Result [{p_adapter["trading_symbol"]}]: {health}')

                    if health['status'] == 'DANGER':
                        logger.warning(f'⚠️ POS DANGER: {p_adapter["trading_symbol"]} | {health["reason"]}')

                    # --- AGGREGATED OI ANALYSIS (±5 Strikes) ---
                    # Institutional Logic:
                    # - Long CALL safe when: Call OI ↓ (resistance weakening) + Put OI ↑ (support building)
                    # - Long PUT safe when: Put OI ↓ (support weakening) + Call OI ↑ (resistance building)

                    sym = p_adapter['trading_symbol']
                    sec_id = pos.get('securityId')
                    parts = sym.split()

                    if len(parts) >= 3:
                        p_strike = float(parts[-2])
                        p_type = parts[-1]  # CE or PE

                        # Get all strikes and sort them
                        all_strikes = sorted([float(s) for s in oc_data.keys() if s.replace('.', '').isdigit()])

                        # Find index of our strike
                        try:
                            strike_idx = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - p_strike))
                        except ValueError:
                            continue

                        # Get ±5 strikes around our position
                        start_idx = max(0, strike_idx - 5)
                        end_idx = min(len(all_strikes), strike_idx + 6)
                        nearby_strikes = all_strikes[start_idx:end_idx]

                        # Aggregate OI changes
                        total_call_delta = 0
                        total_put_delta = 0
                        total_call_oi = 0
                        total_put_oi = 0

                        for strike in nearby_strikes:
                            strike_key = str(strike) if str(strike) in oc_data else str(int(strike))
                            s_data = oc_data.get(strike_key, {})

                            ce_data = s_data.get('ce', {})
                            pe_data = s_data.get('pe', {})

                            ce_oi = ce_data.get('oi', 0)
                            ce_prev = ce_data.get('previous_oi', 0)
                            pe_oi = pe_data.get('oi', 0)
                            pe_prev = pe_data.get('previous_oi', 0)

                            total_call_oi += ce_oi
                            total_put_oi += pe_oi
                            total_call_delta += (ce_oi - ce_prev) if ce_prev > 0 else 0
                            total_put_delta += (pe_oi - pe_prev) if pe_prev > 0 else 0

                        # Get current strike's price data
                        strike_key = str(p_strike) if str(p_strike) in oc_data else str(int(p_strike))
                        my_strike_data = oc_data.get(strike_key, {})
                        opt_data = my_strike_data.get('ce' if p_type == 'CE' else 'pe', {})
                        ltp = opt_data.get('last_price', 0)
                        prev_close = opt_data.get('previous_close_price', 0)
                        price_falling = ltp < prev_close if prev_close > 0 else False

                        logger.info(
                            f'📊 OI Summary [{sym}] ±5 Strikes: '
                            f'Call Δ={total_call_delta:+,} | Put Δ={total_put_delta:+,} | '
                            f'Total CE={total_call_oi:,} | Total PE={total_put_oi:,}'
                        )

                        # --- EXIT DECISION LOGIC (Institutional Perspective) ---
                        # Thresholds (Indian market - aggregated across 10 strikes)
                        OI_BUILDUP_THRESHOLD = 300000  # 3 lakh (aggregated)
                        OI_SUPPORT_THRESHOLD = 100000  # 1 lakh support building

                        should_exit = False
                        should_hold = False
                        exit_reason = ''
                        hold_reason = ''

                        if p_type == 'CE':  # LONG CALL
                            # EXIT: Call OI increasing (resistance) + Put OI NOT increasing (no support)
                            if total_call_delta > OI_BUILDUP_THRESHOLD and total_put_delta < OI_SUPPORT_THRESHOLD:
                                should_exit = True
                                exit_reason = (
                                    f'Resistance Building: Call Δ +{total_call_delta:,}, Put Δ {total_put_delta:+,}'
                                )

                            # EXIT: Call OI surge + Price falling
                            elif total_call_delta > OI_BUILDUP_THRESHOLD and price_falling:
                                should_exit = True
                                exit_reason = f'Short Buildup: Call Δ +{total_call_delta:,} & Price ↓'

                            # HOLD: Call OI decreasing (resistance weakening) + Put OI increasing (support)
                            elif total_call_delta < -100000 and total_put_delta > OI_SUPPORT_THRESHOLD:
                                should_hold = True
                                hold_reason = f'Bullish: Call Δ {total_call_delta:,}, Put Δ +{total_put_delta:,}'

                        elif p_type == 'PE':  # LONG PUT
                            # EXIT: Put OI increasing (support building = bad for long put)
                            if total_put_delta > OI_BUILDUP_THRESHOLD and total_call_delta < OI_SUPPORT_THRESHOLD:
                                should_exit = True
                                exit_reason = (
                                    f'Support Building: Put Δ +{total_put_delta:,}, Call Δ {total_call_delta:+,}'
                                )

                            # EXIT: Put OI surge + Price falling (put premium eroding)
                            elif total_put_delta > OI_BUILDUP_THRESHOLD and price_falling:
                                should_exit = True
                                exit_reason = f'Put Writers Winning: Put Δ +{total_put_delta:,} & Price ↓'

                            # HOLD: Put OI decreasing + Call OI increasing (bearish = good for long put)
                            elif total_put_delta < -100000 and total_call_delta > OI_SUPPORT_THRESHOLD:
                                should_hold = True
                                hold_reason = f'Bearish: Put Δ {total_put_delta:,}, Call Δ +{total_call_delta:,}'

                        # --- Calculate OI Risk Score (0.0 to 1.0) ---
                        oi_risk = 0.0

                        if should_exit:
                            # High risk - signal to ExitMonitor to lower bad_ticks threshold
                            oi_risk = 0.9
                            logger.warning(f'⚠️ OI RISK HIGH: {sym} | {exit_reason}')
                        elif should_hold:
                            # OI is favorable, no rush to exit
                            oi_risk = 0.0
                            logger.info(f'✅ OI FAVORABLE: {sym} | {hold_reason}')
                        else:
                            # Neutral - calculate proportional risk based on delta magnitude
                            if p_type == 'CE':
                                risk_delta = total_call_delta
                            else:
                                risk_delta = total_put_delta

                            if risk_delta > 0:
                                # Gradual risk: 0.1 at 50k, 0.5 at 150k, 0.7 at 250k
                                oi_risk = min(0.7, risk_delta / 350000)

                        # Update TradeManager with OI risk score
                        if sec_id and self.trade_manager:
                            self.trade_manager.update_oi_risk(sec_id, oi_risk)
                            if oi_risk > 0.5:
                                logger.info(f'📊 OI Risk [{sym}]: {oi_risk:.2f} → ExitMonitor will use faster exit')

        except Exception as e:
            logger.error(f'Pos Monitor Error: {e}', exc_info=True)

    def _process_target(self, target: Dict):
        """Process a single underlying."""
        name = target['name']
        scrip = target['scrip']
        seg = target['seg']

        # 1. Fetch Option Chain
        # We fetch the expiry list from Dhan to ensure validity (avoiding Holiday/Weekday logic mismatch)
        expiries = self.bridge.fetch_expiry_list(scrip, seg)
        if not expiries:
            logger.warning(f'Could not fetch expiries for {name}')
            return

        # Select nearest expiry (first in list usually, but sort to be safe)
        # API returns YYYY-MM-DD strings.
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        valid_expiries = [d for d in expiries if d >= current_date_str]

        if not valid_expiries:
            return

        expiry = sorted(valid_expiries)[0]

        data = self.bridge.fetch_option_chain(scrip, seg, expiry)
        if not data:
            return

        last_price = data.get('last_price', 0)
        oc_data = data.get('oc', {})

        if last_price == 0 or not oc_data:
            return

        # 2. Analyze
        traps = self.strategy.analyze_chain(name, last_price, oc_data)

        # 3. Validate & Execute
        for trap in traps:
            self._handle_trap_signal(trap)

    def _handle_trap_signal(self, trap: Dict):
        """Handle a detected potential trap signal."""
        key = f'{trap["sid"]}_{trap["type"]}'
        now = time.time()

        # Check Imbalance (Momentum Override)
        imbalance = self.bridge.get_order_imbalance(str(trap['sid']))

        # Decision Logic
        should_fire = False
        reason = ''

        if imbalance >= self.IMBALANCE_THRESHOLD:
            should_fire = True
            reason = f'Momentum Override (Imb: {imbalance:.2f})'
        else:
            # Wick Protection Check
            if key not in self.pending_traps:
                logger.info(f'⏳ Potential Trap Detected: {trap["symbol"]} @ {trap["spot"]} (Waiting 5s...)')  # noqa: E501
                self.pending_traps[key] = {'start_time': now, 'notified': False}
                return  # Wait for next tick

            # Check elapsed time
            start_time = self.pending_traps[key]['start_time']
            elapsed = now - start_time

            if elapsed >= self.WICK_TIME_SEC:
                should_fire = True
                reason = f'Wick Validation Passed ({elapsed:.1f}s)'
            else:
                return  # Keep waiting

        # Execution
        if should_fire:
            # Prevent double firing for same instance (basic debounce)
            # In real system, TradeManager prevents duplicates, but here we clear state
            if key in self.pending_traps and self.pending_traps[key].get('fired'):
                return

            self._execute_trap(trap, reason)

            # Mark as fired or remove
            if key in self.pending_traps:
                self.pending_traps[key]['fired'] = True
                # Cleanup old keys? logic for another day.
                # For now just clear it to allow re-entry if it breaks again later?
                # Or keep it to prevent spam. Let's keep it 'fired'.

    def _execute_trap(self, trap: Dict, reason: str):
        """Execute (or Log) the trap trade."""
        logger.info(
            f'🚀 TRAP TRIGGERED: {trap["symbol"]} | Reason: {reason} | Spot: {trap["spot"]} | Strike: {trap["strike"]}'
        )  # noqa: E501

        # --- Register with Positional Scanner for EOD Entry ---
        # Extract underlying from symbol (e.g., "NIFTY 25600 CE" -> "NIFTY")
        if self.positional_scanner:
            try:
                symbol_parts = trap['symbol'].split()
                underlying = symbol_parts[0] if symbol_parts else ''
                trap_type = 'PUT' if trap.get('type') == 'PUT_TRAP' else 'CALL'
                day_high = trap.get('spot', 0)
                sentiment = trap.get('sentiment', 0)

                self.positional_scanner.register_trap_signal(underlying, trap_type, day_high, sentiment)
            except Exception as e:
                logger.error(f'Failed to register trap with PositionalScanner: {e}')

        # Signal Construction
        signal = {
            'trading_symbol': trap['symbol'],
            'trigger_above': trap['spot'],  # Market entry effectively
            'stop_loss': 0,  # Auto-calc by Bridge
            'target': 0,  # Auto-calc by Bridge
            'strategy': 'WRITERS_TRAP',
            'expiry': 'WEEKLY',  # Placeholder
            'segment': 'NSE',
        }

        if self.dry_run:
            logger.info(f'[DRY RUN] Would execute: {signal}')
        else:
            # self.bridge.execute_super_order(...)
            # Since user asked for Mock Order Print in Console, we just log.
            # But the 'execute_super_order' method is what we usually call.
            # For this specific task, "Mock Order" implies we shouldn't send it to Dhan.
            logger.warning(f'⭐⭐ MOCK ORDER GENERATED: Buy {trap["symbol"]} at {trap["spot"]} ⭐⭐')
