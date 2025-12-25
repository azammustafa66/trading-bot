import logging

from core.dhan_bridge import DhanBridge

# Setup simple logging
logging.basicConfig(level=logging.INFO)

print('\n--- 1. INITIALIZING BRIDGE ---')
bridge = DhanBridge()

if not bridge.dhan:
    print('❌ Connection Failed! Check .env file.')
    exit()
else:
    print('✅ Dhan Bridge Connected!')

print('\n--- 2. FETCHING TEST SYMBOLS ---')

# Test Case A: NSE Stock (e.g., RELIANCE or HDFC)
# We use the mapper to get the ID first to ensure end-to-end flow works
print('Searching for RELIANCE...')
sec_id, exch, lot = bridge.mapper.get_security_id('RELIANCE 27 JAN 1500 CALL')
print(sec_id, exch, lot)
# Note: Use a valid far OTM or current contract just to get an ID.
# If exact symbol doesn't exist, mapper logs warning.
# For safety, let's just fetch a generic NIFTY index price manually if mapper fails or pick a known liquid one.

# Let's try NIFTY 50 Index (Usually ID 13 for NSE_IDX, but for FNO let's try a contract)
# Better approach: Let the mapper find a valid Gold option we saw earlier
print('Searching for GOLD Option...')
mcx_id, mcx_exch, mcx_lot = bridge.mapper.get_security_id('GOLDM DEC 136000 CALL')

# --- EXECUTE LTP CHECKS ---

# 1. Check NSE_FNO (using whatever ID the mapper found for Reliance, or hardcode a known one if that failed)
# If reliance mapper failed above, let's use a hardcoded common ID for testing:
# (NIFTY 50 Index is '13' on NSE_IDX, but get_ltp expects exchange segments like NSE_FNO)
# Let's rely on the MCX ID we confirmed earlier (486471)

if mcx_id:
    print(f'\n--- TEST MCX (ID: {mcx_id}) ---')
    price = bridge.get_ltp(mcx_id, 'MCX_COMM')
    if price:
        print(f'✅ SUCCESS! Live Price for GOLDM: {price}')
    else:
        print('❌ FAILED to fetch MCX Price.')
else:
    print('⚠️ Could not find MCX ID in CSV to test price.')

# 2. Check NSE (Nifty Bank Index usually has ID '25' on NSE_IDX, let's try that context)
# OR just check funds to verify API key validity
print('\n--- TEST FUNDS ---')
funds = bridge.get_funds()
print(f'💰 Available Funds: {funds}')

if funds >= 0:
    print('\n✅ SYSTEM GREEN. You are ready to trade.')
else:
    print('\n❌ SYSTEM RED. Fund fetch failed.')
