import json
import logging
import os
from datetime import date, datetime
from typing import Any, Dict

# 1. Setup Environment & Logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('MegaTest')

# 2. Import Core Modules
try:
    from core.dhan_bridge import DhanBridge
    from core.dhan_mapper import DhanMapper
    from core.signal_parser import parse_single_block
except ImportError as e:
    logger.critical(f'❌ Import Failed: {e}')
    logger.critical('⚠️  Ensure you are running this from the root folder.')
    exit(1)

# --- 3. SAFETY: Mock Session (Prevents Real Money Loss) ---


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


class MockSession:
    """Intercepts API calls to print payloads instead of trading."""

    def __init__(self):
        self.headers = {}

    def post(self, url, json=None, timeout=None):
        # A. Intercept LTP Calls
        if 'marketfeed/ltp' in url:
            sec_id = json['instruments'][0]['securityId']  # pyright: ignore[reportOptionalSubscript]
            exch = json['instruments'][0]['exchangeSegment']  # pyright: ignore[reportOptionalSubscript]
            fake_ltp = 155.05
            logger.info(
                f'🔮 [MOCK API] Fetching LTP for {exch}:{sec_id}... Returning {fake_ltp}'
            )
            return MockResponse(
                {'data': {f'{exch}:{sec_id}': {'last_price': fake_ltp}}}
            )

        # B. Intercept Order Calls
        if 'orders' in url:
            print('\n' + '=' * 60)
            print(f'📦 [MOCK API] ORDER INTERCEPTED: {url}')
            print(f'   Security ID: {json.get("securityId")} (VERIFY THIS!)')
            print(f'   Exchange   : {json.get("exchangeSegment")}')  # type: ignore
            print(f'📤 PAYLOAD:\n{json.dumps(json, indent=2)}')  # pyright: ignore[reportOptionalMemberAccess]
            print('=' * 60 + '\n')
            return MockResponse(
                {'orderStatus': 'PENDING', 'orderId': 'TEST-ORDER-123'}
            )

        return MockResponse({})


# --- 4. The Test Runner ---


def run_mega_test():
    print('\n🧪 STARTING MEGA PIPELINE TEST (PARSE -> MAP -> TRADE)')
    print('=' * 60)

    # Initialize Components
    try:
        mapper = DhanMapper()
        bridge = DhanBridge()
        bridge.session = MockSession()  # 🛡️ ENABLE SAFETY MODE
        logger.warning('🛡️  SAFETY MODE: API calls are mocked.')
    except Exception as e:
        logger.error(f'Failed to init components: {e}')
        return

    # Define Test Scenarios
    test_messages = [
        # 1. Standard Index (Checks NSE_FNO mapping)
        'BUY BANKNIFTY 60000 CE ABOVE 320 SL 280',
        # 2. Stock Option (CRITICAL: Must map to NSE, NOT BSE)
        'BUY RELIANCE 1500 CALL ABOVE 40 SL 30',
        # 3. Sensex (Checks BSE_FNO mapping & Holiday logic if active)
        'SENSEX 86000 PE BUY ABOVE 150 SL 100',
        # 4. Positional Stock
        'POSITIONAL RISKY\
        BUY TATASTEEL 160 CE ABOVE 5\
        SL 2',
    ]

    for i, msg in enumerate(test_messages, 1):
        print(f'\n🔹 TEST CASE {i}:')
        print(f'   Input: {msg}')

        # --- STEP 1: PARSE ---
        try:
            signal = parse_single_block(msg, reference_date=date.today())
            if signal.get('ignore'):
                logger.warning('   ⚠️  Parser ignored this message.')
                continue

            tsym = signal['trading_symbol']
            print(f"   ✅ Step 1 (Parse): Symbol generated -> '{tsym}'")
        except Exception as e:
            logger.error(f'   ❌ Step 1 Crashed: {e}')
            continue

        # --- STEP 2: VERIFY MAPPING (CRITICAL CHECK) ---
        print(f"   🔍 Step 2 (Map): Querying CSV for '{tsym}'...")
        sec_id, exch, lot = mapper.get_security_id(tsym)

        if sec_id and lot != -1:
            print(f'      🎉 SUCCESS: Security ID Found!')
            print(f'      🆔 ID      : {sec_id}')
            print(
                f'      🏛️ Exchange: {exch} (Should be NSE for Stocks, BSE for Sensex)'
            )
            print(f'      📦 Lot Size: {lot}')
        else:
            print(f'      ❌ FAILURE: Security ID NOT FOUND in CSV.')
            print(
                '      💡 Tip: Check if CSV is downloaded and dates match active contracts.'
            )
            continue  # Stop if mapping fails

        # --- STEP 3: EXECUTE (MOCKED) ---
        try:
            print(f'   🚀 Step 3 (Trade): Sending to Bridge...')
            # Bridge will call mapper again internally, but that's fine.
            # We watch the 'MOCK API' output to see the final JSON payload.
            bridge.execute_super_order(signal)

        except Exception as e:
            logger.error(f'   ❌ Step 3 Crashed: {e}')

    print('\n✅ MEGA TEST COMPLETE.')


if __name__ == '__main__':
    run_mega_test()
