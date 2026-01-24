import logging

from core.dhan_mapper import DhanMapper
from core.signal_parser import parse_single_block

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('VerifyMonth')


def verify():
    print('=' * 50)
    print(f'🚀 VERIFYING MONTH PARSING')
    print('=' * 50)

    # 1. Test Signal Parser
    print('\n--- Test 1: Signal Parser ---')
    raw_text = 'Buy VOLTAS FEB 1340 CE ABOVE 64.50'
    print(f'Input: {raw_text}')

    parsed = parse_single_block(raw_text)
    print(f'Parsed Result: {parsed["trading_symbol"]}')

    expected = 'VOLTAS FEB 1340 CE'
    if parsed['trading_symbol'] == expected:
        print('✅ PASS: Parser constructed correct symbol')
    else:
        print(f"❌ FAIL: Expected '{expected}', got '{parsed['trading_symbol']}'")

    # 2. Test Dhan Mapper
    print('\n--- Test 2: Dhan Mapper resolution ---')
    mapper = DhanMapper()

    # Wait for CSV load
    if mapper.df.is_empty():
        mapper._refresh_master_csv()
        mapper.df = mapper._load_csv()

    # Try to map the symbol
    print(f'Mapping: {parsed["trading_symbol"]}')
    sid, exch, lot, tick = mapper.get_security_id(parsed['trading_symbol'])

    if sid:
        print(f'✅ PASS: Resolved to SID {sid} (Exch: {exch})')

        # Verify it's actually a Feb expiry
        row = mapper.df.filter(pl.col(mapper.COL_SECURITY_ID) == sid).row(0, named=True)
        expiry = row[mapper.COL_EXPIRY_DATE]
        print(f'   Expiry Date: {expiry}')

        if expiry.month == 2:
            print('   ✅ Validated Expiry Month is FEB (2)')
        else:
            print(f'   ❌ Expiry Month Mismatch: Got {expiry.month}')

    else:
        print('❌ FAIL: Could not resolve symbol')

    # 3. Test Default (No Month)
    print('\n--- Test 3: Default (No Month) ---')
    raw_no_month = 'Buy VOLTAS 1340 CE ABOVE 64.50'
    parsed_nm = parse_single_block(raw_no_month)
    print(f'Parsed: {parsed_nm["trading_symbol"]}')

    sid_nm, _, _, _ = mapper.get_security_id(parsed_nm['trading_symbol'])
    if sid_nm:
        row = mapper.df.filter(pl.col(mapper.COL_SECURITY_ID) == sid_nm).row(0, named=True)
        print(f'✅ Resolved Default to: {row[mapper.COL_EXPIRY_DATE]}')
    else:
        print('❌ FAIL: Could not resolve default')


if __name__ == '__main__':
    # Need polars for type hint in mapper, imported inside mapper but script needs generic setup
    import polars as pl

    verify()
