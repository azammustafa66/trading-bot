from unittest.mock import MagicMock, patch

import pytest

from core.dhan_mapper import DhanMapper
from core.signal_parser import process_and_save


class TestIntegration:
    @pytest.fixture
    def mock_mapper(self):
        """Mock DhanMapper to avoid real CSV downloads."""
        with patch('core.dhan_mapper.DhanMapper._download_master_csv') as mock_dl:
            mapper = DhanMapper()
            # Mock the internal dataframe lookup
            # We simulate a dataframe by mocking the search methods directly
            mapper.lookup = MagicMock()

            # Setup mock return for get_atm_strike or similar if needed
            # But the parser mostly relies on finding a match
            return mapper

    def test_parsing_to_execution_flow(self, dynamic_dates):
        """
        E2E-like test: Raw Text -> Parsed Signal -> Mocked Execution Check.
        This verifies that the parser output is compatible with what the system expects.
        """
        raw_msg = 'BANKNIFTY 48000 CE ABOVE 450'

        # 1. Parse
        signals = process_and_save([raw_msg], [dynamic_dates['today']])
        assert len(signals) == 1
        sig = signals[0]

        # 2. Verify Structured Data
        assert sig['underlying'] == 'BANKNIFTY'
        assert sig['strike'] == 48000
        assert sig['option_type'] == 'CALL'
        # Default action implied
        assert sig['action'] == 'BUY'

        # 3. Simulate Logic Layer checks
        # The logic layer uses these keys: 'action', 'trigger_above', 'stop_loss'
        assert 'trigger_above' in sig
        assert sig['trigger_above'] == 450.0

    def test_positional_logic(self, dynamic_dates):
        """Test that POSITIONAL keywords correctly flag the signal."""
        raw_msg = 'POSITIONAL BUY RELIANCE 2500 CA @ 40'
        # Note: 'CA' is sometimes used for CALL, let's verify regex handles standard 'CE'/'PE' strictly
        # or if we need to expand it. The current regex expects CE/PE/CALL/PUT.
        # Let's stick to supported formats for now.

        raw_msg_valid = 'POSITIONAL BUY RELIANCE 2500 CE @ 40'

        signals = process_and_save([raw_msg_valid], [dynamic_dates['today']])
        assert len(signals) == 1
        assert signals[0]['is_positional'] is True
