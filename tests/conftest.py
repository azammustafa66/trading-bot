import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture
def mock_dhan():
    """Mock the Dhan client."""
    mock = MagicMock()
    mock.session = MagicMock()
    return mock


@pytest.fixture
def dynamic_dates():
    """
    Returns a dictionary of dynamic dates for testing.
    - next_thursday: The next occurring Thursday (for weekly expiry)
    - next_month_expiry: The last Thursday of next month
    """
    today = datetime.now()

    # Calculate next Thursday
    days_ahead = 3 - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    next_thursday = today + timedelta(days=days_ahead)

    return {
        'today': today,
        'next_thursday': next_thursday,
        'next_thursday_str': next_thursday.strftime('%d %b').upper(),  # e.g. "27 JAN"
        'year': today.year,
    }
