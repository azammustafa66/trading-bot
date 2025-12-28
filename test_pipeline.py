import logging
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

# --- Import your modules ---
from core.dhan_mapper import DhanMapper
from core.signal_parser import parse_single_block

# Import your Expiry Logic
try:
    from utils.generate_expiry_dates import select_expiry_label
except ImportError:
    print('⚠️  Utils not found, using fallback date logic.')

    def select_expiry_label(underlying, reference_dt=None):
        dt = reference_dt or datetime.now()
        days_ahead = 3 - dt.weekday()
        if days_ahead < 0:
            days_ahead += 7
        next_thurs = dt + timedelta(days=days_ahead)
        return next_thurs.strftime('%d %b').upper()


logging.basicConfig(level=logging.INFO, format='%(name)s - %(message)s')
logger = logging.getLogger('TestDynamic')


class TestDynamicSystem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.now = datetime.now()
        cls.dynamic_expiry = select_expiry_label('NIFTY', cls.now)

        # Define the symbols we expect
        cls.test_strike = 25000
        cls.test_opt_type = 'CALL'
        cls.expected_symbol = f'NIFTY {cls.dynamic_expiry} {cls.test_strike} {cls.test_opt_type}'

        logger.info(f'🗓️  Dynamic Test Date: {cls.dynamic_expiry}')
        logger.info(f'🎯 Expected Symbol: {cls.expected_symbol}')

        # Create the Mock DataFrame ONCE here
        cls.mock_df = pd.DataFrame(
            {
                'SEM_SMST_SECURITY_ID': ['1337'],
                'SEM_TRADING_SYMBOL': [cls.expected_symbol],
                'SEM_CUSTOM_SYMBOL': [cls.expected_symbol],
                'SEM_EXM_EXCH_ID': ['NSE_FNO'],
                'SEM_INSTRUMENT_NAME': ['OPTIDX'],
                'SEM_EXPIRY_DATE': [str(cls.dynamic_expiry)],
                'SEM_STRIKE_PRICE': [float(cls.test_strike)],
                'SEM_OPTION_TYPE': ['CE'],
            }
        )

    def setUp(self):
        # --- THE FIX: PATCH read_csv ---
        # This prevents DhanMapper from loading the real 500MB file.
        # Instead, it instantly gets our 'cls.mock_df'.
        with patch('pandas.read_csv', return_value=self.mock_df):
            # When we init Mapper now, it loads mock_df internally
            self.mapper = DhanMapper()

            # If your Mapper calculates maps in __init__, we are good.
            # If it does it lazily or needs a refresh, force it here:
            if hasattr(self.mapper, '_create_maps'):
                self.mapper._create_maps()  # type: ignore
            else:
                # Fallback manual map injection if method doesn't exist
                # Try to guess the variable name (usually self.data or self.df)
                if hasattr(self.mapper, 'data'):
                    self.mapper.data = self.mock_df  # type: ignore
                elif hasattr(self.mapper, 'df'):
                    self.mapper.df = self.mock_df  # type: ignore

    # --- TEST 1: Signal Parser ---
    def test_parser_generates_dynamic_symbol(self):
        """Verify parser outputs correct dynamic date string."""
        raw_msg = f'Buy Nifty {self.test_strike} CE above 100'
        result = parse_single_block(raw_msg, reference_date=self.now.date())

        self.assertFalse(result.get('ignore'), 'Parser ignored a valid message')
        self.assertEqual(result['trading_symbol'], self.expected_symbol)
        logger.info(f'✅ Parser correctly generated: {result["trading_symbol"]}')

    # --- TEST 2: Mapper Lookup ---
    def test_mapper_finds_dynamic_symbol(self):
        """Verify Mapper retrieves the MOCK ID (1337), not real ID."""
        # Note: If your mapper logic creates a custom format like "NIFTY-Dec..."
        # ensure your mock data 'SEM_TRADING_SYMBOL' matches what get_security_id expects
        # OR ensure get_security_id logic can handle the standard format.

        result = self.mapper.get_security_id(self.expected_symbol, 100.0, lambda x: 100.0)

        # Handle tuple vs single return
        sec_id = result[0] if isinstance(result, tuple) else result

        # This assert failed before because it found the REAL ID (71395)
        # Now it should find our MOCK ID (1337)
        self.assertEqual(str(sec_id), '1337')
        logger.info(f'✅ Mapper found Mock Security ID {sec_id}')

    # --- TEST 3: Range Price Logic ---
    def test_parser_range_price(self):
        """Test 'Target 150....200' logic"""
        full_msg = f'Buy Nifty {self.test_strike} CE above 100\nSL 80\nTarget 150....200'
        result = parse_single_block(full_msg, reference_date=self.now.date())

        self.assertTrue(result['target'] in [175.0, 200.0])
        logger.info(f'✅ Range Target parsed as: {result["target"]}')

    # --- TEST 4: Noise Removal ---
    def test_noise_removal(self):
        """Ensure 'Risky Nifty' becomes 'NIFTY'"""
        raw_msg = f'Risky Nifty {self.test_strike} CE above 100'
        result = parse_single_block(raw_msg, reference_date=self.now.date())

        self.assertEqual(result['underlying'], 'NIFTY')
        self.assertNotIn('RISKY', result['trading_symbol'])  # type: ignore
        logger.info("✅ 'Risky' prefix removed")


if __name__ == '__main__':
    unittest.main()
