from __future__ import annotations

import json
import logging
import os
import re
import statistics
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------
load_dotenv()
logger = logging.getLogger('SignalParser')

try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo('Asia/Kolkata')
except Exception:
    IST = None

MARKET_OPEN_TIME = time(9, 15)
DEDUPE_WINDOW_MINUTES = 15

SIGNALS_JSONL = os.getenv('SIGNALS_JSONL', 'data/signals.jsonl')
SIGNALS_JSON = os.getenv('SIGNALS_JSON', 'data/signals.json')

# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------
IGNORE_KEYWORDS = {
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
}

NOISE_WORDS = {
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
}

# -------------------------------------------------------------------
# Regex
# -------------------------------------------------------------------
RE_POSITIONAL = re.compile(r'\bPOSITION(AL)?|HOLD|LONG\s*TERM\b', re.I)

RE_STOCK_NAME = re.compile(
    r'\b(?:BUY|SELL)\s+([A-Z0-9\s&\-]+?)\s+\d{4,6}\s*(CE|PE|CALL|PUT)\b', re.I
)

RE_DATE = re.compile(
    r'\b(\d{1,2})(?:st|nd|rd|th)?\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\b', re.I
)

RE_STRIKE = re.compile(r'\b(\d{4,6})\s*(CE|PE|CALL|PUT)\b', re.I)

RE_TRIGGER = re.compile(r'\b(?:ABOVE|AT|CMP|RANGE)\s+([\d.\-\s]+)', re.I)
RE_SL = re.compile(r'\b(?:SL|STOP\s*LOSS)\s*[:\-]?\s*([\d.\-\s]+)', re.I)
RE_TARGET = re.compile(r'\bTARGETS?\s+([\d.\-\s]+)', re.I)

RE_DIGITS_ONLY = re.compile(r'^[\d.\-\s]+$')


# -------------------------------------------------------------------
# Time Helpers
# -------------------------------------------------------------------
def now_ist() -> datetime:
    return datetime.now(IST) if IST else datetime.now()


def to_ist(dt: Any) -> Optional[datetime]:
    if isinstance(dt, datetime):
        if IST and dt.tzinfo is None:
            return dt.replace(tzinfo=IST)
        return dt.astimezone(IST) if IST else dt
    return None


# -------------------------------------------------------------------
# Text Helpers
# -------------------------------------------------------------------
def remove_noise(text: str) -> str:
    clean = text.upper()
    for w in NOISE_WORDS:
        clean = clean.replace(w, '')
    return clean.strip()


def parse_price(text: str) -> Optional[float]:
    nums = re.findall(r'(\d+(?:\.\d+)?)', text)
    if not nums:
        return None
    values = list(map(float, nums))
    return statistics.mean(values) if len(values) > 1 else values[0]


# -------------------------------------------------------------------
# Detection
# -------------------------------------------------------------------
def detect_underlying(text: str) -> Optional[str]:
    up = remove_noise(text)

    if 'BANKNIFTY' in up or 'BANK NIFTY' in up:
        return 'BANKNIFTY'
    if 'FINNIFTY' in up:
        return 'FINNIFTY'
    if 'SENSEX' in up or 'SNSEX' in up:
        return 'SENSEX'
    if re.search(r'\bNIFTY\b', up):
        return 'NIFTY'

    match = RE_STOCK_NAME.search(text)
    if match:
        return match.group(1).replace(' ', '').upper()

    return None


def extract_explicit_date(text: str) -> Optional[str]:
    m = RE_DATE.search(text)
    if m:
        return f'{int(m.group(1)):02d} {m.group(2).upper()}'
    return None


def is_price_only(text: str) -> bool:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return bool(lines) and all(RE_DIGITS_ONLY.fullmatch(line) for line in lines)


# -------------------------------------------------------------------
# Core Parsing
# -------------------------------------------------------------------
def parse_single_block(text: str, ref_date: Optional[date] = None) -> Dict[str, Any]:
    raw = text.strip()
    clean = raw.upper()

    out = {
        'raw': raw,
        'action': None,
        'underlying': None,
        'strike': None,
        'option_type': None,
        'trigger_above': None,
        'stop_loss': None,
        'target': None,
        'is_positional': bool(RE_POSITIONAL.search(clean)),
        'expiry_label': None,
        'trading_symbol': None,
        'ignore': False,
    }

    if not clean or is_price_only(clean) or any(k in clean for k in IGNORE_KEYWORDS):
        out['ignore'] = True
        return out

    out['action'] = 'BUY' if 'BUY' in clean else 'SELL' if 'SELL' in clean else None
    out['underlying'] = detect_underlying(clean)

    strike_match = RE_STRIKE.search(clean)
    if strike_match:
        out['strike'] = int(strike_match.group(1))
        out['option_type'] = 'CALL' if strike_match.group(2).startswith('C') else 'PUT'

    if m := RE_TRIGGER.search(clean):
        out['trigger_above'] = parse_price(m.group(1))

    if m := RE_SL.search(clean):
        out['stop_loss'] = parse_price(m.group(1))

    if m := RE_TARGET.search(clean):
        values = re.findall(r'\d+(?:\.\d+)?', m.group(1))
        out['target'] = max(map(float, values)) if values else None

    if not (out['action'] and out['underlying'] and out['strike'] and out['option_type']):
        out['ignore'] = True
        return out

    try:
        manual = extract_explicit_date(clean)
        if manual:
            label = manual
        else:
            from utils.generate_expiry_dates import select_expiry_label

            ref = ref_date or date.today()
            label = select_expiry_label(out['underlying'], datetime.combine(ref, time(9, 15)))

        out['expiry_label'] = label
        out['trading_symbol'] = f'{out["underlying"]} {label} {out["strike"]} {out["option_type"]}'
    except Exception as e:
        logger.warning(f'Symbol generation failed: {e}')
        out['ignore'] = True

    return out


# -------------------------------------------------------------------
# Stream Processing
# -------------------------------------------------------------------
def process_and_save(
    messages: List[str],
    dates: List[datetime],
    jsonl_path: str = SIGNALS_JSONL,
    json_path: str = SIGNALS_JSON,
) -> List[Dict[str, Any]]:
    if len(messages) != len(dates):
        return []

    parsed, buffer, buffer_dates = [], '', []

    def flush():
        nonlocal buffer, buffer_dates
        if not buffer:
            return

        ref_dt = to_ist(buffer_dates[-1]) or now_ist()
        sig = parse_single_block(buffer, ref_dt.date())

        if not sig['ignore'] and ref_dt.time() >= MARKET_OPEN_TIME:
            sig['timestamp'] = ref_dt.isoformat()
            parsed.append(sig)

        buffer, buffer_dates = '', []

    for msg, dt in zip(messages, dates):
        text = msg.strip()
        if not text:
            continue

        strong_start = bool(re.search(r'\b(BUY|SELL)\b', text.upper()) and detect_underlying(text))
        cur_ts = to_ist(dt)
        last_ts = to_ist(buffer_dates[-1]) if buffer_dates else None

        stale = (
            cur_ts is not None and last_ts is not None and (cur_ts - last_ts).total_seconds() > 300
        )

        if buffer and (strong_start or stale or is_price_only(text)):
            flush()

        buffer = f'{buffer}\n{text}' if buffer else text
        buffer_dates.append(dt)

    flush()

    existing = {}
    if os.path.exists(SIGNALS_JSONL):
        with open(SIGNALS_JSONL) as f:
            for line in f:
                s = json.loads(line)
                key = (s['trading_symbol'], s['action'])
                existing[key] = s

    new = []
    for s in parsed:
        key = (s['trading_symbol'], s['action'])
        ts = to_ist(s['timestamp'])
        old = existing.get(key)

        if not old:
            new.append(s)
            existing[key] = s
            continue

        ts = to_ist(s['timestamp'])
        old_ts = to_ist(old['timestamp'])
        if (ts and old_ts) and (ts - old_ts).total_seconds() > DEDUPE_WINDOW_MINUTES * 60:
            new.append(s)
            existing[key] = s

    if new:
        with open(SIGNALS_JSONL, 'a') as f:
            for s in new:
                f.write(json.dumps(s) + '\n')
        with open(SIGNALS_JSON, 'w') as f:
            json.dump(list(existing.values()), f, indent=2)

    return new


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
        'Buy Risky Nifty 26100 pe above 70',  # TEST 1: Noise Removal
        'SL 115',
        'Target 160.... 250',  # TEST 2: Price Range Average
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
    print(f'{"SYMBOL":<35} | {"ACTION":<5} | {"TRIG":<5} | {"SL":<5} | {"TARGET"}')
    print('-' * 80)

    for r in results:
        trig = str(r['trigger_above']) if r['trigger_above'] is not None else '---'
        sl = str(r['stop_loss']) if r['stop_loss'] is not None else '---'
        target = str(r['target'])
        print(f'{r["trading_symbol"]:<35} | {r["action"]:<5} | {trig:<5} | {sl:<5} | {target}')
