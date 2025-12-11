import json
import logging
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

# --- MOCK ENVIRONMENT ---
os.environ['TELEGRAM_API_ID'] = '12345'
os.environ['TELEGRAM_API_HASH'] = 'fake_hash'
os.environ['SESSION_NAME'] = 'test_session'
os.environ['TARGET_CHANNEL'] = 'test_channel'
os.environ['DHAN_CLIENT_ID'] = '1000000000'
os.environ['DHAN_ACCESS_TOKEN'] = 'fake_jwt_token'

# Ensure we can import from local folders
sys.path.append(os.path.abspath('.'))

# Import your actual modules
try:
    from core.dhan_bridge import DhanBridge
    from core.signal_parser import process_and_save
except ImportError as e:
    print(f'❌ IMPORT ERROR: {e}')
    print(
        "👉 Did you run the 'mkdir' and 'mv' commands to organize 'core/' and 'utils/'?"
    )
    sys.exit(1)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(message)s')


def run_full_simulation():
    print('\n🧪 --- STARTING FULL DEPLOYMENT SIMULATION ---')

    # 1. MOCK EXTERNAL DEPENDENCIES
    # We verify that your code handles the signal correctly without actually hitting Dhan/Telegram

    with (
        patch('core.dhan_mapper.DhanMapper') as MockMapper,
        patch('requests.post') as mock_post,
        patch('core.dhan_bridge.DhanBridge.get_ltp') as mock_ltp,
    ):
        # --- A. SETUP MOCKS (The "Fake World") ---

        # 1. Mapper: Should return ID '50000', Exchange 'NSE', Lot Size 75 (Nifty)
        mapper_instance = MockMapper.return_value
        mapper_instance.get_security_id.return_value = ('50000', 'NSE', 75)

        # 2. LTP: Price is 24100. Entry is 24050.
        # Logic Check: 24100 > 24050 but < (24050 + 3%). Should trigger MARKET order.
        mock_ltp.return_value = 24100.0

        # 3. Dhan API: Returns Success
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'status': 'success',
            'data': {'orderId': 'SANDBOX_ORDER_999', 'orderStatus': 'PENDING'},
        }
        mock_post.return_value = mock_response

        # --- B. EXECUTE FLOW ---

        print('🔹 Initializing Bridge...')
        bridge = DhanBridge()

        # Simulate a "Positional" Telegram Message
        print('🔹 Receiving Telegram Signal...')
        raw_msg = 'Positional Risky\nBUY NIFTY 02 DEC 24000 CE ABOVE 24050\nSL 23950\nTARGET 25000'
        print(f'   📩 Input: {raw_msg.replace("\n", " ")}')

        # Run Parser
        print('🔹 Parsing...')
        # Clean previous test files
        if os.path.exists('signals.jsonl'):
            os.remove('signals.jsonl')

        signals = process_and_save([raw_msg], [datetime.now()])

        if not signals:
            print('❌ PARSER FAILED. No signals extracted.')
            return

        signal = signals[0]
        print(f'   ✅ Parsed: {signal["trading_symbol"]}')
        print(f'   ✅ Positional Flag: {signal["is_positional"]}')

        # Run Bridge
        print('🔹 Executing Bridge Logic...')
        bridge.execute_super_order(signal)

        # --- C. VALIDATE FINAL PAYLOAD ---

        # Capture what your code sent to Dhan
        if not mock_post.called:
            print('❌ BRIDGE FAILED: No order request was sent!')
            return

        args, kwargs = mock_post.call_args
        payload = kwargs['json']

        print('\n📋 --- FINAL ORDER JSON (To Dhan) ---')
        print(json.dumps(payload, indent=2))

        print('\n🔎 DIAGNOSTIC RESULTS:')
        errors = []

        # Check 1: Positional Logic
        if payload['productType'] == 'MARGIN':
            print('✅ Product Type: MARGIN (Correct for Positional)')
        else:
            errors.append(f'Product Type: Expected MARGIN, got {payload["productType"]}')

        # Check 2: Smart Entry Logic
        # LTP (24100) > Entry (24050). Should be Market Order.
        if payload['orderType'] == 'MARKET' and payload['price'] == 0:
            print('✅ Order Type: MARKET @ 0 (Smart Breakout Entry Working)')
        else:
            errors.append(
                f'Order Type: Expected MARKET @ 0, got {payload["orderType"]} @ {payload["price"]}'
            )

        # Check 3: Risk Management & Quantity
        # Risk = 5000 (Positional). Gap = 24050 - 23950 = 100 pts.
        # Qty Needed = 5000 / 100 = 50.
        # Lot Size = 75.
        # 50/75 = 0.66 -> Round to nearest Lot -> 1 Lot (75).
        if payload['quantity'] == 75:
            print(
                f'✅ Quantity: {payload["quantity"]} (Correctly calculated 1 Lot from Risk)'
            )
        else:
            errors.append(f'Quantity: Expected 75, got {payload["quantity"]}')

        if not errors:
            print('\n🎉 ALL SYSTEMS GO. CODE IS READY FOR DEPLOYMENT.')
        else:
            print('\n❌ DEPLOYMENT ABORTED. ERRORS FOUND:')
            for e in errors:
                print(f'   - {e}')


if __name__ == '__main__':
    run_full_simulation()
