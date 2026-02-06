"""
Writers Trap Strategy Module.

Identifies potential "Trap" scenarios where High-OI Option Writers are forced to cover,
leading to sharp price movements.

Strategy Logic:
1. Identify Top 3 Strikes with Highest Call/Put OI (Resistance/Support).
2. Check if Spot Price has breached these levels.
3. Filter by 'Time Hold' (Wick Protection) or 'Imbalance' (Momentum Override).
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger('WritersTrap')


class WritersTrapStrategy:
    def __init__(self):
        self.active_traps = {}  # Format: {symbol_strike: {'start_time': ts, 'type': 'CALL/PUT'}}

    def analyze_chain(
        self, symbol: str, spot_price: float, chain_data: Dict[str, Any], top_n: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Analyze Option Chain to find active traps.

        Args:
            symbol: Underlying symbol name (e.g., 'NIFTY').
            spot_price: Current spot price of underlying.
            chain_data: 'oc' dictionary from Dhan API.
            top_n: Number of top OI strikes to monitor.

        Returns:
            List of detected TRAP signals ready for validation/execution.
        """
        candidates = []

        # 1. Parse Chain Data
        # Flatten the nested structure: {strike: {ce: {...}, pe: {...}}}
        calls = []
        puts = []

        for strike_str, data in chain_data.items():
            try:
                strike = float(strike_str)

                # Calls
                if 'ce' in data:
                    ce = data['ce']
                    calls.append(
                        {
                            'strike': strike,
                            'oi': ce.get('oi', 0),
                            'ltp': ce.get('last_price', 0),
                            'sid': ce.get('security_id'),
                        }
                    )

                # Puts
                if 'pe' in data:
                    pe = data['pe']
                    puts.append(
                        {
                            'strike': strike,
                            'oi': pe.get('oi', 0),
                            'ltp': pe.get('last_price', 0),
                            'sid': pe.get('security_id'),
                        }
                    )
            except ValueError:
                continue

        # 2. Sort by OI (Descending)
        top_calls = sorted(calls, key=lambda x: x['oi'], reverse=True)[:top_n]
        top_puts = sorted(puts, key=lambda x: x['oi'], reverse=True)[:top_n]

        # Log the "Walls" for visibility
        c_walls = [f'{int(c["strike"])} ({c["oi"]})' for c in top_calls]
        p_walls = [f'{int(p["strike"])} ({p["oi"]})' for p in top_puts]
        logger.info(f'📊 {symbol} Walls | CE: {c_walls} | PE: {p_walls}')

        # 3. Check for Traps
        # CALL TRAP: Price Breaks ABOVE Resistance (Call Writers Panicking)
        for c in top_calls:
            strike = c['strike']
            # Relevancy Filter: Only consider if within 2% of spot
            if abs(spot_price - strike) / strike > 0.02:
                continue

            # TRIGGER: Spot > Strike + Buffer (0.1%)
            # We use a small buffer to avoid exact-touch noise
            buffer = strike * 0.0005
            if spot_price > (strike + buffer):
                candidates.append(
                    {
                        'type': 'CALL_TRAP',
                        'symbol': f'{symbol} {int(strike)} CE',
                        'strike': strike,
                        'sid': c['sid'],
                        'spot': spot_price,
                        'oi': c['oi'],
                        'diff': spot_price - strike,
                    }
                )

        # PUT TRAP: Price Breaks BELOW Support (Put Writers Panicking)
        for p in top_puts:
            strike = p['strike']
            # Relevancy Filter
            if abs(spot_price - strike) / strike > 0.02:
                continue

            # TRIGGER: Spot < Strike - Buffer
            buffer = strike * 0.0005
            if spot_price < (strike - buffer):
                candidates.append(
                    {
                        'type': 'PUT_TRAP',
                        'symbol': f'{symbol} {int(strike)} PE',
                        'strike': strike,
                        'sid': p['sid'],
                        'spot': spot_price,
                        'oi': p['oi'],
                        'diff': strike - spot_price,
                    }
                )

        return candidates

    def check_position_health(self, position: Dict[str, Any], chain_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate if an open position is facing dangerous OI buildup.

        Args:
            position: Dict containing 'security_id', 'buy_sell', 'quantity', 'trading_symbol'.
            chain_data: Option Chain data.

        Returns:
            Dict with 'status': 'SAFE'|'DANGER', 'reason': str
        """
        # 1. Parse Symbol to get Strike & Type
        # Schema: "BANKNIFTY 60000 CE"
        sym = position.get('trading_symbol', '')
        if not sym:
            return {'status': 'UNKNOWN', 'reason': 'No Symbol'}

        parts = sym.split()
        if len(parts) < 3:
            return {'status': 'UNKNOWN', 'reason': 'Parse Error'}

        p_strike = float(parts[-2])
        p_type = parts[-1]  # CE or PE

        # 2. Extract specific chain data for this strike
        strike_data = chain_data.get(str(p_strike), {})
        if not strike_data:
            # Try formatting float 60000.0 vs 60000
            strike_data = chain_data.get(str(int(p_strike)), {})

        if not strike_data:
            return {'status': 'UNKNOWN', 'reason': 'Strike Data Missing'}

        # 3. Logic: Are we fighting a Wall?
        # If we are Long CE, we don't want massive Call OI at our strike or slightly above.

        # Get Top walls to compare against
        # Simple heuristic: If OI at our strike > 2x OI of opposite side?
        # Or simply, if OI is very high.

        status = 'SAFE'
        reason = ''

        try:
            if p_type == 'CE':
                ce_oi = strike_data.get('ce', {}).get('oi', 0)
                pe_oi = strike_data.get('pe', {}).get('oi', 0)

                # Danger: Call OI is massive (Resistance) and > Put OI
                if ce_oi > 100000 and ce_oi > (pe_oi * 1.5):
                    status = 'DANGER'
                    reason = f'Fighting Wall: Call OI ({ce_oi}) > 1.5x Put OI ({pe_oi})'

            elif p_type == 'PE':
                ce_oi = strike_data.get('ce', {}).get('oi', 0)
                pe_oi = strike_data.get('pe', {}).get('oi', 0)

                # Danger: Put OI is massive (Support) and > Call OI
                if pe_oi > 100000 and pe_oi > (ce_oi * 1.5):
                    status = 'DANGER'
                    reason = f'Fighting Wall: Put OI ({pe_oi}) > 1.5x Call OI ({ce_oi})'

        except Exception as e:
            logger.error(f'Health Check Error: {e}')
            return {'status': 'ERROR', 'reason': str(e)}

        return {'status': status, 'reason': reason}
