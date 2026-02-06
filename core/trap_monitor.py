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
    def __init__(self, bridge: DhanBridge, dry_run: bool = False):
        self.bridge = bridge
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
        self.POS_CHECK_INTERVAL = 90  # 1.5 Minutes

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
                # SENSEX usually on BSE, but check exchange if needed. Assuming IDX_I for now or API handling.
                targets.append({'name': 'SENSEX', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': 'IDX_I'})

            # 4. TVSMOTOR
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL').str.contains('TVSMOTOR'))
            candidate = res.filter(pl.col('SEM_EXM_EXCH_ID').is_in(['NSE_EQ', 'NSE', 'EQ']))
            if not candidate.is_empty():
                row = candidate.row(0, named=True)
                seg = row['SEM_EXM_EXCH_ID']
                if seg == 'NSE':
                    seg = 'NSE_EQ'
                targets.append({'name': 'TVSMOTOR', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': seg})

            # 5. IEX
            res = mapper.df.filter(pl.col('SEM_TRADING_SYMBOL').str.contains('IEX'))
            candidate = res.filter(pl.col('SEM_EXM_EXCH_ID').is_in(['NSE_EQ', 'NSE', 'EQ']))
            if not candidate.is_empty():
                # Prefer exact match if multiple
                exact = candidate.filter(pl.col('SEM_TRADING_SYMBOL') == 'IEX')
                if not exact.is_empty():
                    row = exact.row(0, named=True)
                else:
                    row = candidate.row(0, named=True)
                seg = row['SEM_EXM_EXCH_ID']
                if seg == 'NSE':
                    seg = 'NSE_EQ'
                targets.append({'name': 'IEX', 'scrip': int(row['SEM_SMST_SECURITY_ID']), 'seg': seg})

            self.MONITORED_SCRIPS = targets
            logger.info(f'✅ Resolved Targets: {[t["name"] for t in targets]}')

        except Exception as e:
            logger.error(f'Instrument Resolution Failed: {e}', exc_info=True)

    def _run_loop(self):
        """Main monitoring loop."""
        while not self.stop_event.is_set():
            try:
                # Scan Targets
                for target in self.MONITORED_SCRIPS:
                    self._process_target(target)
                    time.sleep(3)  # Rate limit

                # Check Positions (Every 1.5 mins)
                if time.time() - self._last_pos_check > self.POS_CHECK_INTERVAL:
                    self._monitor_open_positions()
                    self._last_pos_check = time.time()

            except Exception as e:
                logger.error(f'Monitor Loop Error: {e}', exc_info=True)
                time.sleep(5)

    def _monitor_open_positions(self):
        """Check health of all open positions."""
        logger.info('🕵️ Checking Position Health...')
        try:
            # 1. Get Positions
            positions = self.bridge.get_positions()
            if not positions:
                return

            open_positions = [p for p in positions if p.get('positionType') != 'CLOSED' and p.get('quantity', 0) != 0]
            if not open_positions:
                logger.info('No open positions to check.')
                return

            # Group by Underlying (Symbol string matching)
            # We iterate through our monitored chains effectively
            for target in self.MONITORED_SCRIPS:
                t_name = target['name']  # e.g. BANKNIFTY

                # Find positions for this symbol
                relevant_pos = [p for p in open_positions if p.get('tradingSymbol', '').startswith(t_name)]

                if not relevant_pos:
                    continue

                logger.info(f'Checking {len(relevant_pos)} positions for {t_name}')

                # Fetch Chain (Cached ideally, but here we might re-fetch if not recent)
                # TODO: Optimization - Share chain data between Trap Scan and Position Check?
                # For now, explicit fetch to ensure freshness.
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

                for pos in relevant_pos:
                    # Rename keys to match strategy expectation
                    # API returns 'tradingSymbol', strategy expects 'trading_symbol'
                    p_adapter = {
                        'trading_symbol': pos.get('tradingSymbol'),
                        # Positive for Buy?? Check API
                        'quantity': pos.get('netQty', 0),
                        'type': 'BUY' if pos.get('netQty', 0) > 0 else 'SELL',
                    }

                    # We typically only protect BUYS (Long Options) in this strategy
                    if p_adapter['quantity'] <= 0:
                        continue

                    health = self.strategy.check_position_health(p_adapter, oc_data)
                    logger.info(f'Health Result [{p_adapter["trading_symbol"]}]: {health}')

                    if health['status'] == 'DANGER':
                        logger.warning(f'⚠️ POS DANGER: {p_adapter["trading_symbol"]} | {health["reason"]}')
                        # TODO: Auto-Exit Logic?
                        # For now, just Log as requested "Review"

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
