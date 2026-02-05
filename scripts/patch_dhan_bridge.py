import os

target_file = '/opt/trading_bot/core/dhan_bridge.py'

new_method = '''    def _get_current_price(self, sid: str, exch_seg: str, entry: float, has_depth: bool) -> float:
        """
        Get current price. 
        Prioritizes WebSocket Depth feed. Falls back to 10-tick API polling if depth is unavailable.
        """
        # 1. Check Cache DIRECTLY (Avoid get_live_ltp implicit API call)
        curr_ltp = float(self.depth_cache.get(sid, {}).get('ltp', 0.0))

        if curr_ltp == 0:
            logger.info('Cold start: fetching price...')

            # 2. Try WebSocket (Fastest - if available)
            if has_depth:
                self.subscribe(
                    [{'ExchangeSegment': 'NSE_FNO', 'SecurityId': sid}])
                for _ in range(10):
                    time.sleep(0.05)
                    curr_ltp = float(self.depth_cache.get(sid, {}).get('ltp', 0.0))
                    if curr_ltp > 0:
                        break

            # 3. API Fallback with 10-tick Polling (Strict Requirement)
            if curr_ltp == 0:
                logger.info(f'Switching to API Polling (10 ticks) for {sid}...')
                for i in range(10):
                    # This fetches AND updates the cache
                    ltp = self._fetch_ltp_from_api(sid, exch_seg)
                    if ltp > 0:
                        curr_ltp = ltp
                        logger.info(f'Tick {i+1}/10: LTP {curr_ltp}')
                    else:
                        logger.warning(f'Tick {i+1}/10: LTP 0')
                    
                    time.sleep(1)

        # Use signal entry as last resort
        if curr_ltp == 0 and entry > 0:
            logger.warning(f'Using signal entry as anchor: {entry}')
            curr_ltp = entry

        return curr_ltp
'''

with open(target_file, 'r') as f:
    lines = f.readlines()

start_index = -1
end_index = -1

for i, line in enumerate(lines):
    if 'def _get_current_price' in line:
        start_index = i
        break

if start_index != -1:
    # Find end of function (start of next method)
    for i in range(start_index + 1, len(lines)):
        if 'def _fetch_ltp_from_api' in line:  # Or whatever is next, checking indentation
            pass
        # Heuristic: verify indentation. Method is indented with 4 spaces.
        if lines[i].strip() and not lines[i].startswith('    ') and not lines[i].startswith(')'):
            # This heuristic might fail on comments/blank lines.
            # Better to find the NEXT def
            pass

    # Actually, simpler: finding the next def is safer.
    for i in range(start_index + 1, len(lines)):
        if lines[i].strip().startswith('def '):
            end_index = i
            break

    # If no next def, assumes it's till end (not true here).
    # Based on previous readings, _fetch_ltp_from_api follows.

    if end_index != -1:
        new_lines = lines[:start_index] + [new_method] + lines[end_index:]
        with open(target_file, 'w') as f:
            f.writelines(new_lines)
        print('Successfully patched file.')
    else:
        print('Could not find end of method.')

else:
    print('Could not find start of method.')
