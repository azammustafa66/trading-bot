import logging
import os
import sys
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# --- MOCK ENVIRONMENT BEFORE IMPORTS ---
os.environ['TELEGRAM_API_ID'] = '12345'
os.environ['TELEGRAM_API_HASH'] = 'fake_hash'
os.environ['SESSION_NAME'] = 'test_session'
os.environ['TARGET_CHANNEL'] = 'test_channel'
os.environ['DHAN_CLIENT_ID'] = '1000000000'
os.environ['DHAN_ACCESS_TOKEN'] = 'fake_jwt_token'

# Setup paths to find your modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.dhan_bridge import DhanBridge
from core.signal_parser import process_and_save

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger('Sandbox')


def run_sandbox():
    print('\n🧪 --- STARTING SANDBOX SIMULATION ---')

    # 1. MOCK THE MAPPER (So we don't download CSV)
    print('🔹 Mocking DhanMapper...')
    with patch('core.dhan_mapper.DhanMapper') as MockMapper:
        mapper_instance = MockMapper.return_value
        # Simulate finding ID '5000' for Nifty, Lot Size 75
        mapper_instance.get_security_id.return_value = ('5000', 'NSE', 75)

        # 2. MOCK THE BRIDGE NETWORK CALLS
        print('🔹 Mocking Dhan API Network Calls...')

        # Mock LTP to simulate different market conditions
        # Scenario: Entry is 100. We simulate LTP at 101 (Chase Zone).
        with patch.object(DhanBridge, 'get_ltp', return_value=101.0) as mock_ltp, patch('requests.post') as mock_post:
            # Setup Mock Response for Order
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'status': 'success',
                'data': {'orderId': 'TEST_ORDER_123', 'orderStatus': 'PENDING'},
            }
            mock_post.return_value = mock_response

            # 3. INITIALIZE BRIDGE
            bridge = DhanBridge()

            # 4. SIMULATE TELEGRAM SIGNALS
            # We simulate a batch of messages arriving
            print('🔹 Injecting Test Signal...')
            mock_messages = [
                'Positional Risky',
                'BUY NIFTY 02 DEC 24500 CE ABOVE 100',
                'SL 80',
                'TARGET 200',
            ]
            mock_dates = [datetime.now() for _ in mock_messages]

            # 5. RUN PARSER
            print('🔹 Running Parser...')
            parsed_signals = process_and_save(
                mock_messages,
                mock_dates,
                jsonl_path='sandbox_signals.jsonl',
                json_path='sandbox_signals.json',
            )

            if not parsed_signals:
                print('❌ PARSER FAILED: No signals extracted.')
                return

            print(f'✅ Parser extracted: {parsed_signals[0]["trading_symbol"]}')

            # 6. RUN EXECUTION (The moment of truth)
            print('🔹 Executing Order through Bridge...')
            bridge.execute_super_order(parsed_signals[0])

            # 7. VERIFY PAYLOAD
            # We check what arguments requests.post was called with
            args, kwargs = mock_post.call_args
            payload = kwargs['json']

            print('\n📋 --- FINAL ORDER PAYLOAD INSPECTION ---')
            print(json.dumps(payload, indent=2))

            # Assertions / Validation
            print('\n🔎 Validating Logic...')

            # Check 1: Did it switch to MARKET because LTP (101) > Entry (100)?
            if payload['orderType'] == 'MARKET':
                print('✅ Logic Check: Order converted to MARKET (Chase Zone) - PASSED')
            else:
                print(f'❌ Logic Check: Expected MARKET, got {payload["orderType"]} - FAILED')

            # Check 2: Is Price 0 for Market Order?
            if payload['price'] == 0:
                print('✅ Logic Check: Price set to 0 for Market Order - PASSED')
            else:
                print(f'❌ Logic Check: Price is {payload["price"]}, expected 0 - FAILED')

            # Check 3: Positional Flag
            if payload['productType'] == 'MARGIN':
                print('✅ Logic Check: Product set to MARGIN (Positional) - PASSED')
            else:
                print(f'❌ Logic Check: Expected MARGIN, got {payload["productType"]} - FAILED')

            # Check 4: Quantity Calculation
            # Risk 5000 (Positional). Gap = 100-80 = 20.
            # Raw Qty = 5000 / 20 = 250.
            # Lot Size = 75.
            # 250 / 75 = 3.33 -> Rounds to 3 lots -> 225 Qty.
            expected_qty = 225
            if payload['quantity'] == expected_qty:
                print(f'✅ Math Check: Qty {payload["quantity"]} is correct (3 Lots) - PASSED')
            else:
                print(f'❌ Math Check: Expected {expected_qty}, got {payload["quantity"]} - FAILED')

    print('\n🎉 SANDBOX TEST COMPLETE')


if __name__ == '__main__':
    # Cleanup old test files
    if os.path.exists('sandbox_signals.jsonl'):
        os.remove('sandbox_signals.jsonl')
    if os.path.exists('sandbox_signals.json'):
        os.remove('sandbox_signals.json')

    try:
        run_sandbox()
    except Exception as e:
        print(f'\n❌ CRITICAL TEST FAILURE: {e}')
        import traceback

        traceback.print_exc()
