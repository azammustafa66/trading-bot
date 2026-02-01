"""
Signal Parser Module.

Parses trading signals from Telegram messages into structured data that can
be used for order execution. Handles multi-part messages, noise removal,
deduplication, and expiry date resolution.

Typical message formats:
    "BANKNIFTY 43500 PE BUY ABOVE 320 SL 280 TARGET 400"
    "Buy Nifty 26100 PE above 70\\nSL 55\\nTarget 100 120"
"""

from __future__ import annotations

import json
import logging
import os
import re
import statistics
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger('SignalParser')

# Timezone setup
try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo('Asia/Kolkata')
except Exception:
    IST = None

# Configuration
MARKET_OPEN_TIME = time(9, 15)
DEDUPE_WINDOW_MINUTES = 15
SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')

# Keywords to ignore - these indicate non-actionable messages
IGNORE_KEYWORDS = frozenset(
    {
        'RISK TRAIL',
        'SAFE BOOK',
        'IGNORE',
        'BOOK PROFIT',
        'EXIT',
        'AVOID',
        'FUT',
        'FUTURE',
        'FUTURES',
        'CLOSE',
        'WATCHLIST',
        'WATCH',
    }
)

# Noise words to strip from symbol detection
NOISE_WORDS = frozenset(
    {
        'RISKY',
        'SAFE',
        'HERO',
        'ZERO',
        'JACKPOT',
        'MOMENTUM',
        'TRADE',
        'EXPIRY',
        'SPECIAL',
        'TODAY',
        'MORNING',
        'ROCKET',
        'BTST',
        'POSITIONAL',
    }
)

# Compiled regex patterns
RE_POSITIONAL = re.compile(r'\bPOSITION(AL)?|HOLD|LONG\s*TERM\b', re.I)
# Regex to handle:
# 1. Optional Action (BUY/SELL) - potentially attached to name (e.g. BUYPOLYCAB)
# 2. Underlying Name (A-Z & -)
# 3. Optional Date (e.g. 27 JAN) - non-capturing group for skipping
# 4. Strike Price (digits or decimals)
# 5. Option Type (CE/PE/CALL/PUT)
RE_STOCK_NAME = re.compile(
    r'\b(?:(?:BUY|SELL)\s*)?([A-Z0-9\s&\-]+?)(?:\s*\d+\s+(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))?\s+(\d+(?:\.\d+)?)\s*(CE|PE|CALL|PUT)\b',
    re.I,
)
RE_DATE = re.compile(
    r'\b(\d{1,2})(?:st|nd|rd|th)?\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b', re.I
)
RE_STRIKE = re.compile(r'\b(\d+(?:\.\d+)?)\s*(CE|PE|CALL|PUT)\b', re.I)
RE_TRIGGER = re.compile(r'\b(?:ABOVE|AT|CMP|RANGE)\s+([\d.\-\s]+)', re.I)
RE_SL = re.compile(r'\b(?:SL|STOP\s*LOSS)\s*[:\-]?\s*([\d.\-\s]+)', re.I)
RE_TARGET = re.compile(r'\bTARGETS?\s+([\d.\-\s]+)', re.I)
RE_DIGITS_ONLY = re.compile(r'^[\d.\-\s]+$')


# =============================================================================
# Time Utilities
# =============================================================================


def now_ist() -> datetime:
    """Get current datetime in IST."""
    return datetime.now(IST) if IST else datetime.now()


def to_ist(dt: Any) -> Optional[datetime]:
    """
    Convert a datetime to IST timezone.

    Args:
        dt: Datetime object or string to convert.

    Returns:
        Datetime in IST, or None if conversion fails.
    """
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return None

    if isinstance(dt, datetime):
        if IST and dt.tzinfo is None:
            return dt.replace(tzinfo=IST)
        return dt.astimezone(IST) if IST else dt

    return None


# =============================================================================
# Text Processing
# =============================================================================


def remove_noise(text: str) -> str:
    """Remove noise words from text for cleaner parsing."""
    clean = text.upper()
    for word in NOISE_WORDS:
        clean = clean.replace(word, '')
    return clean.strip()


def parse_price(text: str) -> Optional[float]:
    """
    Extract price value(s) from text.

    If multiple values found (e.g., "100-110"), returns the mean.

    Args:
        text: Text containing price information.

    Returns:
        Extracted price or None if not found.
    """
    nums = re.findall(r'(\d+(?:\.\d+)?)', text)
    if not nums:
        return None
    values = list(map(float, nums))
    return statistics.mean(values) if len(values) > 1 else values[0]


def is_price_only(text: str) -> bool:
    """Check if text contains only price/numeric data."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return bool(lines) and all(RE_DIGITS_ONLY.fullmatch(line) for line in lines)


# =============================================================================
# Symbol Detection
# =============================================================================


def detect_underlying(text: str) -> Optional[str]:
    """
    Detect the underlying instrument from signal text.

    Handles index names (NIFTY, BANKNIFTY, SENSEX) and stock names.

    Args:
        text: Signal text to parse.

    Returns:
        Underlying symbol (e.g., "BANKNIFTY", "RELIANCE") or None.
    """
    clean = remove_noise(text)

    # Check for known indices first
    if 'BANKNIFTY' in clean or 'BANK NIFTY' in clean:
        return 'BANKNIFTY'
    if 'FINNIFTY' in clean:
        return 'FINNIFTY'
    if 'SENSEX' in clean or 'SNSEX' in clean:
        return 'SENSEX'
    if re.search(r'\bNIFTY\b', clean):
        return 'NIFTY'

    # Try to extract stock name
    match = RE_STOCK_NAME.search(clean)
    if match:
        raw_name = match.group(1)
        # Fix: Regex greedily captures month (e.g. VOLTAS FEB -> VOLTASFEB)
        # Remove any month names from the end of the detected name
        for m in [
            'JAN',
            'FEB',
            'MAR',
            'APR',
            'MAY',
            'JUN',
            'JUL',
            'AUG',
            'SEP',
            'OCT',
            'NOV',
            'DEC',
        ]:
            if raw_name.endswith(f' {m}'):
                raw_name = raw_name[: -len(m) - 1]
            elif raw_name.endswith(m):  # merged
                raw_name = raw_name[: -len(m)]

        return raw_name.replace(' ', '').upper()

    return None


RE_MONTH_ONLY = re.compile(r'\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b', re.I)


def extract_explicit_date(text: str) -> Optional[str]:
    """
    Extract explicit expiry date or month from text.

    Args:
        text: Signal text to parse.

    Returns:
        Label like "25 DEC" or "FEB" or None.
    """
    # 1. Try full date (25 DEC)
    match = RE_DATE.search(text)
    if match:
        return f'{int(match.group(1)):02d} {match.group(2).upper()}'

    # 2. Try month only (FEB)
    match = RE_MONTH_ONLY.search(text)
    if match:
        return match.group(1).upper()

    return None


# =============================================================================
# Core Parsing
# =============================================================================


def parse_single_block(text: str, ref_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Parse a single signal message block into structured data.

    Args:
        text: Raw signal text.
        ref_date: Reference date for expiry calculation.

    Returns:
        Dictionary with parsed signal data including:
        - action: 'BUY' or 'SELL'
        - underlying: Instrument symbol
        - strike: Strike price
        - option_type: 'CALL' or 'PUT'
        - trigger_above: Entry trigger price
        - stop_loss: Stop loss price
        - target: Target price
        - trading_symbol: Full symbol for trading
        - ignore: Whether to skip this signal
    """
    raw = text.strip()
    clean = raw.upper()

    result: Dict[str, Any] = {
        'raw': raw,
        'action': '',
        'underlying': '',
        'strike': None,
        'option_type': '',
        'trigger_above': None,
        'stop_loss': None,
        'target': None,
        'is_positional': bool(RE_POSITIONAL.search(clean)),
        'expiry_label': '',
        'trading_symbol': '',
        'ignore': False,
    }

    # Quick rejection checks
    if not clean or is_price_only(clean):
        result['ignore'] = True
        return result

    if any(keyword in clean for keyword in IGNORE_KEYWORDS):
        result['ignore'] = True
        return result

    # Extract core signal components
    result['action'] = 'SELL' if 'SELL' in clean else 'BUY'
    result['underlying'] = detect_underlying(clean)

    strike_match = RE_STRIKE.search(clean)
    if strike_match:
        result['strike'] = int(strike_match.group(1))
        result['option_type'] = 'CALL' if strike_match.group(2).startswith('C') else 'PUT'

    # Extract prices
    if match := RE_TRIGGER.search(clean):
        result['trigger_above'] = parse_price(match.group(1))

    if match := RE_SL.search(clean):
        result['stop_loss'] = parse_price(match.group(1))

    if match := RE_TARGET.search(clean):
        values = re.findall(r'\d+(?:\.\d+)?', match.group(1))
        result['target'] = max(map(float, values)) if values else None

    # Validate required fields
    required = [result['action'], result['underlying'], result['strike'], result['option_type']]
    if not all(required):
        result['ignore'] = True
        return result

    # Generate trading symbol with expiry
    try:
        expiry_label = extract_explicit_date(clean)
        if not expiry_label:
            from utils.generate_expiry_dates import select_expiry_label

            ref = ref_date or date.today()
            expiry_label = select_expiry_label(
                result['underlying'], datetime.combine(ref, time(9, 15))
            )

        result['expiry_label'] = expiry_label
        result['trading_symbol'] = (
            f'{result["underlying"]} {expiry_label} {result["strike"]} {result["option_type"]}'
        )

    except Exception as e:
        logger.warning(f'Symbol generation failed: {e}')
        result['ignore'] = True

    return result


# =============================================================================
# Stream Processing
# =============================================================================


def process_and_save(
    messages: List[str],
    dates: List[datetime],
    jsonl_path: str = SIGNALS_JSONL,
    json_path: str = SIGNALS_JSON,
) -> List[Dict[str, Any]]:
    """
    Process a batch of Telegram messages into trading signals.

    Handles:
    - Multi-part message batching
    - Deduplication within time window
    - Persistence to JSONL and JSON files

    Args:
        messages: List of message texts.
        dates: Corresponding message timestamps.
        jsonl_path: Path for append-only signal log.
        json_path: Path for current signals JSON.

    Returns:
        List of new (non-duplicate) parsed signals.
    """
    if len(messages) != len(dates):
        return []

    # Parse messages with batching for multi-part signals
    parsed = _parse_message_batch(messages, dates)

    # Load existing signals for deduplication
    existing = _load_existing_signals(jsonl_path)

    # Filter duplicates
    new_signals = _filter_duplicates(parsed, existing)

    # Persist new signals
    if new_signals:
        _save_signals(new_signals, existing, jsonl_path, json_path)

    return new_signals


def _parse_message_batch(messages: List[str], dates: List[datetime]) -> List[Dict[str, Any]]:
    """Parse messages, batching multi-part signals together."""
    parsed: List[Dict[str, Any]] = []
    buffer = ''
    buffer_dates: List[datetime] = []

    def flush_buffer():
        nonlocal buffer, buffer_dates
        if not buffer:
            return

        ref_dt = to_ist(buffer_dates[-1]) or now_ist()
        sig = parse_single_block(buffer, ref_dt.date())

        if not sig['ignore'] and ref_dt.time() >= MARKET_OPEN_TIME:
            sig['timestamp'] = ref_dt.isoformat()
            parsed.append(sig)

        buffer = ''
        buffer_dates = []

    for msg, dt in zip(messages, dates):
        text = msg.strip()
        if not text:
            continue

        # Check if this starts a new signal
        is_new_signal = bool(re.search(r'\b(BUY|SELL)\b', text.upper()) and detect_underlying(text))

        curr_ts = to_ist(dt)
        last_ts = to_ist(buffer_dates[-1]) if buffer_dates else None
        is_stale = curr_ts and last_ts and (curr_ts - last_ts).total_seconds() > 300

        # Flush buffer if new signal, stale, or price-only followup
        if buffer and (is_new_signal or is_stale or is_price_only(text)):
            flush_buffer()

        buffer = f'{buffer}\n{text}' if buffer else text
        buffer_dates.append(dt)

    flush_buffer()
    return parsed


def _load_existing_signals(jsonl_path: str) -> Dict[tuple, Dict[str, Any]]:
    """Load existing signals keyed by (symbol, action)."""
    existing: Dict[tuple, Dict[str, Any]] = {}

    if os.path.exists(jsonl_path):
        with open(jsonl_path) as f:
            for line in f:
                try:
                    sig = json.loads(line)
                    key = (sig['trading_symbol'], sig['action'])
                    existing[key] = sig
                except (json.JSONDecodeError, KeyError):
                    continue

    return existing


def _filter_duplicates(
    parsed: List[Dict[str, Any]], existing: Dict[tuple, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Filter out duplicate signals within dedupe window."""
    new_signals = []

    for sig in parsed:
        key = (sig['trading_symbol'], sig['action'])
        old = existing.get(key)

        if not old:
            new_signals.append(sig)
            existing[key] = sig
            continue

        # Check if enough time has passed
        curr_ts = to_ist(sig['timestamp'])
        old_ts = to_ist(old['timestamp'])

        if curr_ts and old_ts:
            elapsed = (curr_ts - old_ts).total_seconds()
            if elapsed > DEDUPE_WINDOW_MINUTES * 60:
                new_signals.append(sig)
                existing[key] = sig

    return new_signals


def _save_signals(
    new_signals: List[Dict[str, Any]],
    all_signals: Dict[tuple, Dict[str, Any]],
    jsonl_path: str,
    json_path: str,
) -> None:
    """Persist signals to disk."""
    # Append to JSONL
    with open(jsonl_path, 'a') as f:
        for sig in new_signals:
            f.write(json.dumps(sig) + '\n')

    # Overwrite JSON with all signals
    with open(json_path, 'w') as f:
        json.dump(list(all_signals.values()), f, indent=2)


# =============================================================================
# Test Suite
# =============================================================================

if __name__ == '__main__':
    print(f'\nRunning Test Suite [Time: {now_ist()}]')

    mock_now = datetime(2025, 12, 30, 10, 0, 0)
    if IST:
        mock_now = mock_now.replace(tzinfo=IST)

    test_stream = [
        'BANKNIFTY 43500 PE BUY ABOVE 320 SL 280 TARGET 400',
        'SENSEX 87500 CE BUY ABOVE 420 SL 380 TARGET 500',
        'Hero Zero Risky',
        'Buy Risky Nifty 26100 pe above 70',
        'SL 115',
        'Target 160.... 250',
        'Positional',
        'Buy Sensex 25 Dec 85000 CE Above 1000',
        'SL 800',
        'Target 1200 1400',
        'Risky',
        'Buy Banknifty 58700 ce above 420',
        'sl 385',
        'target 550.... 650',
    ]

    test_dates = [mock_now for _ in test_stream]
    results = process_and_save(
        test_stream, test_dates, jsonl_path='test_signals.jsonl', json_path='test_signals.json'
    )

    print(f'\nProcessed {len(results)} signals.\n')
    print(f'{"SYMBOL":<35} | {"ACTION":<5} | {"TRIG":<5} | {"SL":<5} | TARGET')
    print('-' * 80)

    for r in results:
        trig = str(r['trigger_above']) if r['trigger_above'] else '---'
        sl = str(r['stop_loss']) if r['stop_loss'] else '---'
        target = str(r['target']) if r['target'] else '---'
        print(f'{r["trading_symbol"]:<35} | {r["action"]:<5} | {trig:<5} | {sl:<5} | {target}')
