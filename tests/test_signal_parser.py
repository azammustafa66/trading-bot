from datetime import datetime

import pytest

from core.signal_parser import parse_single_block, process_and_save


class TestSignalParser:
    def test_explicit_buy_signal(self, dynamic_dates):
        """Test a clear BUY signal with standard formatting."""
        msg = f'BUY NIFTY 24000 CE ABOVE 120 SL 80 TARGET 160'
        signals = process_and_save([msg], [dynamic_dates['today']])

        assert len(signals) == 1
        sig = signals[0]
        assert sig['action'] == 'BUY'
        assert sig['underlying'] == 'NIFTY'
        assert sig['strike'] == 24000
        assert sig['option_type'] == 'CALL'
        assert sig['trigger_above'] == 120.0
        assert sig['stop_loss'] == 80.0

    def test_implied_buy_signal(self, dynamic_dates):
        """Test a defined signal missing the 'BUY' keyword (defaulting to BUY)."""
        msg = f'NIFTY 24500 PE ABOVE 200'  # No 'BUY'
        signals = process_and_save([msg], [dynamic_dates['today']])

        assert len(signals) == 1
        sig = signals[0]
        assert sig['action'] == 'BUY'  # Should default to BUY
        assert sig['underlying'] == 'NIFTY'
        assert sig['strike'] == 24500
        assert sig['option_type'] == 'PUT'

    def test_3_digit_strike(self, dynamic_dates):
        """Test regex support for 3-digit strikes (e.g. Stocks)."""
        msg = 'BHEL 255 PE'
        signals = process_and_save([msg], [dynamic_dates['today']])

        assert len(signals) == 1
        assert signals[0]['strike'] == 255
        assert signals[0]['underlying'] == 'BHEL'

    def test_decimal_strike(self, dynamic_dates):
        """Test regex support for decimal strikes."""
        msg = 'TATAPOWER 252.5 CE'
        signals = process_and_save([msg], [dynamic_dates['today']])

        assert len(signals) == 1
        assert signals[0]['strike'] == 252.5
        assert signals[0]['underlying'] == 'TATAPOWER'

    def test_explicit_expiry_date(self, dynamic_dates):
        """Test parsing of explicit expiry dates in the specific format found in signals."""
        # Using the dynamic next Thursday string (e.g. '27 JAN')
        expiry_str = dynamic_dates['next_thursday_str']
        msg = f'NIFTY {expiry_str} 25000 CE'

        signals = process_and_save([msg], [dynamic_dates['today']])
        assert len(signals) == 1
        assert signals[0]['expiry_label'] == expiry_str

    def test_ignore_keywords(self, dynamic_dates):
        """Test that ignored keywords result in no signal."""
        msg = 'SAFE TRADERS BOOK PROFIT'
        signals = process_and_save([msg], [dynamic_dates['today']])
        assert len(signals) == 0  # Should be filtered out
