from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Ensure this utility exists in your project structure
try:
    from utils.generate_expiry_dates import select_expiry_label
except ImportError:
    # Fallback if file is missing (mostly for testing isolation)
    def select_expiry_label(underlying, reference_dt):
        return datetime.now().strftime('%d %b').upper()


load_dotenv()

IST = ZoneInfo('Asia/Kolkata')
logger = logging.getLogger('Parser')
MARKET_OPEN_TIME = time(9, 15)
DEDUPE_WINDOW_MINUTES = 15
SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')

# --- CONSTANTS & PATTERNS ---
IGNORE_PATTERNS = [
    re.compile(r'\bRISK\s+TRAIL\b', re.IGNORECASE),
    re.compile(r'\bSAFE\s+BOOK\b', re.IGNORECASE),
    re.compile(r'\bIGNORE\b', re.IGNORECASE),
    re.compile(r'\bBOOK\s+PROFIT\b', re.IGNORECASE),
    re.compile(r'\bEXIT\b', re.IGNORECASE),
    re.compile(r'\bAVOID\b', re.IGNORECASE),
    re.compile(r'\bSAFE\s+TRADERS\b', re.IGNORECASE),
    re.compile(r'\bFUT\b', re.IGNORECASE),
    re.compile(r'\bFUTURES?\b', re.IGNORECASE),
    re.compile(r'\bVERY\s+RISKY\b', re.IGNORECASE),
    re.compile(r'HERO\s*ZERO|JACKPOT|ROCKET|EXPIRY\s*SPECIAL', re.IGNORECASE),
]

# Pre-compiled Regex Patterns for Performance
RE_POSITIONAL = re.compile(
    r'\b(POSITIONAL|POSTIONAL|POSITION|LONG TERM|HOLD|POS|BTST)\b', re.IGNORECASE
)
RE_STOCK_NAME = re.compile(
    r'(?:BUY|SELL)\s+([A-Z0-9\s\&\-]+?)\s+(\d+(?:\.\d+)?)\s*(?:CE|PE|C|P|CALL|PUT)', re.IGNORECASE
)
RE_NIFTY = re.compile(r'\bNIFTY\b', re.IGNORECASE)
RE_DATE = re.compile(
    r'\b(\d{1,2})(?:st|nd|rd|th)?\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)',
    re.IGNORECASE,
)
RE_STRIKE = re.compile(r'\b(\d{4,6}|\d{2,5}(?:\.\d+)?)\s*(CE|PE|C|P|CALL|PUT)?\b', re.IGNORECASE)
RE_TRIGGER = re.compile(r'\bABOVE\s+(\d+(?:\.\d+)?)', re.IGNORECASE)
RE_SL = re.compile(r'\b(?:SL|STOP\s*LOSS)[\s:\-]*(\d+(?:\.\d+)?)', re.IGNORECASE)
RE_TARGET = re.compile(r'TARGETS?[\s:\-]+([\d\.\s…,\-]+)', re.IGNORECASE)
RE_DIGITS_ONLY = re.compile(r'[\d\.\-\s]+')


# --- 3. Helpers ---
def to_ist(dt_input: Any) -> Optional[datetime]:
    """Converts input to IST datetime."""
    if not dt_input:
        return None
    if isinstance(dt_input, str):
        try:
            return to_ist(datetime.fromisoformat(dt_input))
        except ValueError:
            return None
    if isinstance(dt_input, datetime):
        if IST and dt_input.tzinfo is None:
            return dt_input.replace(tzinfo=IST)
        if IST:
            return dt_input.astimezone(IST)
        return dt_input
    return None


def now_ist() -> datetime:
    """Returns current time in IST."""
    return datetime.now(IST) if IST else datetime.now()


# --- 4. Core Parsing Logic ---
def detect_positional(text: str) -> bool:
    """Checks if the signal is positional."""
    return bool(RE_POSITIONAL.search(text))


def extract_stock_name(text: str) -> Optional[str]:
    """Captures stock name between Action and Strike Price."""
    match = RE_STOCK_NAME.search(text)
    if match:
        raw_name = match.group(1).strip()
        if raw_name.upper() in ['ABOVE', 'BELOW', 'AT', 'NEAR', 'THE', 'RANGE']:
            return None
        return raw_name
    return None


def detect_underlying(text: str) -> Optional[str]:
    """Identifies the underlying asset (Index or Stock)."""
    up = text.upper()
    if 'BANKNIFTY' in up or 'BANK NIFTY' in up:
        return 'BANKNIFTY'
    if RE_NIFTY.search(up):
        return 'NIFTY'
    if 'SENSEX' in up or 'SNSEX' in up:
        return 'SENSEX'

    stock_name = extract_stock_name(up)
    if stock_name:
        return stock_name.replace(' ', '')
    return None


def extract_explicit_date(text: str) -> Optional[str]:
    """Detects explicit dates (e.g., '25 DEC')."""
    match = RE_DATE.search(text)
    if match:
        day = int(match.group(1))
        month = match.group(2).upper()
        return f'{day:02d} {month}'
    return None


def is_price_only(text: str) -> bool:
    """True if text contains only numbers/prices (Noise)."""
    up = text.upper()
    if any(k in up for k in ['TARGET', 'TGT', 'SL', 'STOP']):
        return False

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return True
    return all(RE_DIGITS_ONLY.fullmatch(ln) for ln in lines)


def should_ignore(text: str) -> bool:
    """Checks text against compiled ignore patterns."""
    for pattern in IGNORE_PATTERNS:
        if pattern.search(text):
            return True
    return False


def parse_single_block(text: str, reference_date: Optional[date] = None) -> Dict[str, Any]:
    """Parses a single block of text into a structured signal dictionary."""
    clean_text = text.strip().upper()

    out: Dict[str, Any] = {
        'raw': text.strip(),
        'action': None,
        'underlying': None,
        'strike': None,
        'option_type': None,
        'stop_loss': None,
        'trigger_above': None,
        'target': None,
        'is_positional': False,
        'trading_symbol': None,
        'ignore': False,
        'expiry_label': None,
    }

    # 1. Fast Fail Filters (Including Futures Check)
    if not clean_text or is_price_only(clean_text) or should_ignore(clean_text):
        out['ignore'] = True
        return out

    # 2. Extract Basic Info
    if detect_positional(clean_text):
        out['is_positional'] = True

    if 'BUY' in clean_text:
        out['action'] = 'BUY'
    elif 'SELL' in clean_text:
        out['action'] = 'SELL'

    out['underlying'] = detect_underlying(text)

    # 3. Extract Strike & Option Type
    strike_match = RE_STRIKE.search(clean_text)
    if strike_match:
        raw_strike = strike_match.group(1)
        out['strike'] = float(raw_strike) if '.' in raw_strike else int(raw_strike)

        opt_str = strike_match.group(2) or ''
        if opt_str.startswith(('C', 'CALL')):
            out['option_type'] = 'CALL'
        elif opt_str.startswith(('P', 'PUT')):
            out['option_type'] = 'PUT'

    # 4. Extract Levels
    trigger_match = RE_TRIGGER.search(clean_text)
    if trigger_match:
        out['trigger_above'] = float(trigger_match.group(1))

    sl_match = RE_SL.search(clean_text)
    if sl_match:
        out['stop_loss'] = float(sl_match.group(1))

    # 5. Extract Targets (Takes the last value from range)
    target_match = RE_TARGET.search(clean_text)
    if target_match:
        raw_target_string = target_match.group(1)
        all_numbers = re.findall(r'\d+(?:\.\d+)?', raw_target_string)

        found_values = []
        for n in all_numbers:
            try:
                found_values.append(float(n))
            except ValueError:
                continue

        if found_values:
            out['target'] = found_values[-1]

    # 6. Generate Trading Symbol
    if out['underlying'] and out['strike'] and out['option_type']:
        try:
            manual_date = extract_explicit_date(clean_text)
            if manual_date:
                label = manual_date
            else:
                ref_d = reference_date or date.today()
                ref_dt = datetime.combine(ref_d, time(9, 15))
                label = select_expiry_label(underlying=out['underlying'], reference_dt=ref_dt)

            out['expiry_label'] = label
            out['trading_symbol'] = (
                f'{out["underlying"]} {label} {out["strike"]} {out["option_type"]}'
            )

        except Exception as e:
            logger.warning(f'Symbol Gen Failed: {e}')
            out['ignore'] = True
    else:
        # If we couldn't parse Strike/Option Type, it's ignored anyway.
        # This handles Futures implicitly (no strike), but explicit ignore is safer.
        out['ignore'] = True

    return out


# --- 5. Stitching Logic ---
def is_partial_signal(text: str) -> bool:
    """Checks if buffer has partial signal components."""
    up = text.upper()
    has_action = 'BUY' in up or 'SELL' in up
    has_symbol = detect_underlying(text) is not None
    has_digits = bool(re.search(r'\d', text))
    return (has_action and has_symbol) and not has_digits


def process_and_save(
    messages: List[str],
    dates: List[datetime],
    jsonl_path: str = SIGNALS_JSONL,
    json_path: str = SIGNALS_JSON,
) -> List[Dict[str, Any]]:
    """Processes a batch of messages, parses signals, and saves unique ones."""
    if len(messages) != len(dates):
        return []

    parsed_signals: List[Dict[str, Any]] = []
    buffer = ''
    buffer_dates: List[datetime] = []

    def flush_buffer():
        nonlocal buffer, buffer_dates
        if not buffer.strip():
            return

        ref_date = buffer_dates[-1].date() if buffer_dates else date.today()
        temp_time = to_ist(buffer_dates[-1]) if buffer_dates else now_ist()
        ref_time = temp_time if temp_time is not None else now_ist()

        signal = parse_single_block(buffer, reference_date=ref_date)

        if signal['trading_symbol'] and not signal['ignore']:
            if signal['trigger_above'] or signal['stop_loss']:
                if ref_time.time() >= MARKET_OPEN_TIME:
                    signal['timestamp'] = ref_time.isoformat()
                    parsed_signals.append(signal)

        buffer = ''
        buffer_dates = []

    # Stream Processing
    for msg, dt in zip(messages, dates):
        text = msg.strip()
        if not text:
            continue

        # Check ignore list early to avoid buffering garbage
        if should_ignore(text):
            # If the current line is garbage, we still might be in the middle of a buffer.
            # But if the line says "AVOID", we usually want to kill the whole buffer.
            # For now, we treat it as a flush trigger to clear context.
            flush_buffer()
            continue

        is_start_keyword = detect_positional(text)
        has_action = bool(re.search(r'\b(BUY|SELL)\b', text.upper()))
        has_symbol = detect_underlying(text) is not None
        is_strong_new = is_start_keyword or (has_action and has_symbol)

        is_price_noise = is_price_only(text)
        buffer_is_partial = is_partial_signal(buffer) if buffer else False

        current_ts = to_ist(dt)
        last_ts = to_ist(buffer_dates[-1]) if buffer_dates else current_ts
        is_stale = (current_ts and last_ts) and (current_ts - last_ts).total_seconds() > 300

        should_flush = False
        if not buffer:
            should_flush = False
        elif is_stale:
            should_flush = True
        elif is_strong_new:
            # Don't flush if it's just a positional tag following a strong signal
            if buffer.strip().upper() in ['POSITIONAL', 'RISKY', 'POSITIONAL RISKY']:
                should_flush = False
            else:
                should_flush = True
        elif buffer_is_partial:
            should_flush = False
        else:
            should_flush = is_price_noise

        if should_flush:
            flush_buffer()

        buffer = (buffer + '\n' + text) if buffer else text
        buffer_dates.append(dt)

    flush_buffer()

    # Deduplication & Persistence
    new_unique = []
    existing = []

    if os.path.exists(jsonl_path):
        try:
            with open(jsonl_path, 'r') as f:
                existing = [json.loads(line) for line in f if line.strip()]
        except Exception as e:
            logger.info(f'Failed to read existing signals: {e}')

    for new_sig in parsed_signals:
        is_dupe = False
        new_ts = to_ist(new_sig.get('timestamp'))
        if new_ts is None:
            continue

        for old_sig in existing:
            if (
                old_sig.get('trading_symbol') == new_sig['trading_symbol']
                and old_sig.get('action') == new_sig['action']
            ):
                old_ts = to_ist(old_sig.get('timestamp'))
                if old_ts and (new_ts - old_ts).total_seconds() < (DEDUPE_WINDOW_MINUTES * 60):
                    is_dupe = True
                    logger.info(f'Duplicate detected: {new_sig["trading_symbol"]}')
                    break

        if not is_dupe:
            new_unique.append(new_sig)
            existing.append(new_sig)

    if new_unique:
        with open(jsonl_path, 'a') as f:
            for sig in new_unique:
                f.write(json.dumps(sig) + '\n')
        with open(json_path, 'w') as f:
            json.dump(existing, f, indent=2)

    return new_unique


# --- 6. Test Suite ---
if __name__ == '__main__':
    print(f'\nRunning Test Suite [Time: {now_ist()}]')

    mock_now = datetime(2025, 12, 30, 10, 0, 0)
    if IST:
        mock_now = mock_now.replace(tzinfo=IST)

    test_stream = [
        'BANKNIFTY 43500 PE BUY ABOVE 320 SL 280 TARGET 400',
        'SENSEX 87500 CE BUY ABOVE 420 SL 380 TARGET 500',
        'Hero Zero Risky',
        'Buy Sensex 84500 CE ABOVE 140',
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
        'BUY GOLDM FEB FUT',
    ]

    test_dates = [mock_now for _ in test_stream]

    results = process_and_save(
        test_stream, test_dates, jsonl_path='test_signals.jsonl', json_path='test_signals.json'
    )

    print(f'\nProcessed {len(results)} signals.\n')
    print(f'{"SYMBOL":<35} | {"ACTION":<5} | {"TRIG":<5} | {"SL":<5} | {"TARGET"}')
    print('-' * 80)

    for r in results:
        trig = str(r['trigger_above']) if r['trigger_above'] is not None else '---'
        sl = str(r['stop_loss']) if r['stop_loss'] is not None else '---'
        target = str(r['target'])
        print(f'{r["trading_symbol"]:<35} | {r["action"]:<5} | {trig:<5} | {sl:<5} | {target}')
