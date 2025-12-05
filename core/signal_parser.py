from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# --- 1. Robust Import Setup ---
try:
    from utils.generate_expiry_dates import (select_expiry_date,
                                             select_expiry_label)
except ImportError:
    try:
        sys.path.append(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))
        from utils.generate_expiry_dates import (select_expiry_date,
                                                 select_expiry_label)
    except ImportError:
        # Fallback dummy functions (Matches signature for Pylance safety)
        def select_expiry_date(
            underlying: str, reference_dt: Optional[datetime] = None
        ) -> date:
            return date.today()

        def select_expiry_label(
            underlying: str, reference_dt: Optional[datetime] = None
        ) -> str:
            return "TEST-EXPIRY"


# --- 2. Configuration & Logging ---
load_dotenv()

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except ImportError:
    IST = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Parser")

MARKET_OPEN_TIME = time(9, 15)
DEDUPE_WINDOW_MINUTES = int(os.getenv("DEDUPE_WINDOW_MINUTES", "60"))
SIGNALS_JSONL = '../data/signals.jsonl'
SIGNALS_JSON = '../data/signals.json'

IGNORE_KEYWORDS = [
    "RISK TRAIL",
    "SAFE BOOK",
    "IGNORE",
    "BOOK PROFIT",
    "EXIT",
    "AVOID",
    "SAFE TRADERS",
]


# --- 3. Helpers ---


def to_ist(dt_input: Any) -> Optional[datetime]:
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
    return datetime.now(IST) if IST else datetime.now()


# --- 4. Core Parsing Logic ---


def detect_positional(text: str) -> bool:
    # Improved regex to catch variations like "Positional", "Hold", "Long Term"
    return bool(
        re.search(r"POSITIONAL|POSTIONAL|POSITION|LONG TERM|HOLD", text.upper())
    )


def extract_stock_name(text: str) -> Optional[str]:
    """
    Captures text between BUY/SELL and the Strike Price for Stocks.
    Pattern: BUY <STOCK NAME> <STRIKE> <CE/PE>
    """
    # 1. (?:BUY|SELL)       -> Start with Action
    # 2. \s+                -> Space
    # 3. ([A-Z0-9\s\&\-]+?) -> CAPTURE GROUP 1: The Stock Name (Non-greedy)
    # 4. \s+                -> Space
    # 5. (\d+(?:\.\d+)?)    -> The Strike Price (Integer or Float)
    # 6. \s* -> Optional Space
    # 7. (?:CE|PE...)       -> Lookahead for Option Type validation

    pattern = r"(?:BUY|SELL)\s+([A-Z0-9\s\&\-]+?)\s+(\d+(?:\.\d+)?)\s*(?:CE|PE|C|P|CALL|PUT)"
    match = re.search(pattern, text.upper())

    if match:
        raw_name = match.group(1).strip()
        # Safety Filter: Ensure captured name isn't a keyword
        if raw_name in ["ABOVE", "BELOW", "AT", "NEAR", "THE", "RANGE"]:
            return None
        return raw_name
    return None


def detect_underlying(text: str) -> Optional[str]:
    up = text.upper()

    # STRICT BLOCK: Explicitly ignore unsupported indices
    if "FINNIFTY" in up or "MIDCP" in up or "MIDCAP" in up:
        return None

    # 1. Priority: Hardcoded Indices
    if "BANKNIFTY" in up or "BANK NIFTY" in up:
        return "BANKNIFTY"
    # Use word boundary to ensure NIFTY doesn't match NIFTYBEES or FINNIFTY
    if re.search(r"\bNIFTY\b", up):
        return "NIFTY"
    if "SENSEX" in up or "SNSEX" in up:
        return "SENSEX"

    # 2. Fallback: Generic Stock Extraction
    stock_name = extract_stock_name(text)
    if stock_name:
        # Clean up common spaces in stock names (e.g., "TATA STEEL" -> "TATASTEEL")
        # Dhan usually uses condensed names, but mapper handles exact matches.
        return stock_name.replace(" ", "")

    return None


def extract_explicit_date(text: str) -> Optional[str]:
    """Detects if user provided a specific date like '25 DEC'."""
    # Matches: 25 DEC, 25 DEC 2025, 2nd DEC
    match = re.search(
        r"\b(\d{1,2})(?:st|nd|rd|th)?\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)",
        text.upper(),
    )
    if match:
        day = int(match.group(1))
        month = match.group(2)
        return f"{day:02d} {month}"
    return None


def is_price_only(text: str) -> bool:
    """
    True if text is JUST numbers/prices (Noise).
    False if it contains words like 'TARGET', 'SL', 'TGT'.
    """
    up = text.upper()
    if "TARGET" in up or "TGT" in up or "SL" in up or "STOP" in up:
        return False
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return True
    # Regex matches lines that are purely numbers/ranges like "180", "180-190"
    return all(re.fullmatch(r"[\d\.\-\s]+", ln) for ln in lines)


def parse_single_block(
    text: str, reference_date: Optional[date] = None
) -> Dict[str, Any]:
    clean_text = text.strip().upper()

    out: Dict[str, Any] = {
        "raw": text.strip(),
        "action": None,
        "underlying": None,
        "strike": None,
        "option_type": None,
        "stop_loss": None,
        "trigger_above": None,
        "is_positional": False,
        "trading_symbol": None,
        "ignore": False,
        "expiry_label": None,
    }

    # 1. Fast Fail Filters
    if (
        not clean_text
        or is_price_only(clean_text)
        or any(k in clean_text for k in IGNORE_KEYWORDS)
    ):
        out["ignore"] = True
        return out

    # 2. Extract Positional Flag
    if detect_positional(clean_text):
        out["is_positional"] = True

    # 3. Extract Action
    if "BUY" in clean_text:
        out["action"] = "BUY"
    elif "SELL" in clean_text:
        out["action"] = "SELL"

    # 4. Extract Underlying (Indices OR Stocks)
    out["underlying"] = detect_underlying(text)

    # 5. Extract Strike + Option Type
    # Updated Regex to support FLOAT strikes (e.g. 157.5) common in stocks
    strike_match = re.search(
        r"\b(\d{4,6}|\d{2,5}(?:\.\d+)?)\s*(CE|PE|C|P|CALL|PUT)?\b", clean_text)
    if strike_match:
        raw_strike = strike_match.group(1)
        # Store as float if decimal, else int
        if '.' in raw_strike:
            out["strike"] = float(raw_strike)
        else:
            out["strike"] = int(raw_strike)

        opt_str = strike_match.group(2) or ""
        if opt_str.startswith(("C", "CALL")):
            out["option_type"] = "CALL"
        if opt_str.startswith(("P", "PUT")):
            out["option_type"] = "PUT"

    # 6. Extract Trigger Price ("Above 120.5")
    trigger_match = re.search(r"\bABOVE\s+(\d+(?:\.\d+)?)", clean_text)
    if trigger_match:
        out["trigger_above"] = float(trigger_match.group(1))

    # 7. Extract Stop Loss ("SL 80.5")
    sl_match = re.search(
        r"(?:SL|STOP\s*LOSS)[\s:\-]*(\d+(?:\.\d+)?)", clean_text)
    if sl_match:
        out["stop_loss"] = float(sl_match.group(1))

    # 8. Generate Trading Symbol
    if out["underlying"] and out["strike"] and out["option_type"]:
        try:
            # Check for Manual Date Override in text (e.g. "25 DEC")
            manual_date = extract_explicit_date(clean_text)

            if manual_date:
                label = manual_date
            else:
                # Use Auto-Calculation (Indices=Weekly, Stocks=Monthly)
                # Logic resides in utils/generate_expiry_dates.py
                ref_d = reference_date or date.today()
                ref_dt = datetime.combine(ref_d, time(9, 15))
                label = select_expiry_label(
                    underlying=out["underlying"], reference_dt=ref_dt
                )

            out["expiry_label"] = label

            # Formatting: "BANKNIFTY 05 DEC 45000 CALL" or "RELIANCE 29 DEC 2500 CALL"
            out["trading_symbol"] = (
                f"{out['underlying']} {label} {out['strike']} {out['option_type']}"
            )

        except Exception as e:
            logger.warning(f"Symbol Gen Failed: {e}")
            out["ignore"] = True
    else:
        # Missing critical components -> Ignore
        out["ignore"] = True

    return out


# --- 5. Intelligent Stitching & Processing ---


def is_partial_signal(text: str) -> bool:
    """True if text has Action + Symbol but NO numbers (Waiting for price)."""
    up = text.upper()
    has_action = "BUY" in up or "SELL" in up
    has_symbol = detect_underlying(text) is not None
    has_digits = bool(re.search(r"\d", text))
    return (has_action and has_symbol) and not has_digits


def process_and_save(
    messages: List[str],
    dates: List[datetime],
    jsonl_path: str = SIGNALS_JSONL,
    json_path: str = SIGNALS_JSON,
) -> List[Dict[str, Any]]:

    if len(messages) != len(dates):
        return []

    parsed_signals: List[Dict[str, Any]] = []
    buffer = ""
    buffer_dates: List[datetime] = []

    def flush_buffer():
        nonlocal buffer, buffer_dates
        if not buffer.strip():
            return

        # Use date of the LAST message in the block for reference
        ref_date = buffer_dates[-1].date() if buffer_dates else date.today()
        temp_time = to_ist(buffer_dates[-1]) if buffer_dates else now_ist()
        ref_time = temp_time if temp_time is not None else now_ist()

        signal = parse_single_block(buffer, reference_date=ref_date)

        # Validation: Must have Symbol AND (Trigger OR SL)
        if signal["trading_symbol"] and not signal["ignore"]:
            if signal["trigger_above"] or signal["stop_loss"]:

                if ref_time.time() >= MARKET_OPEN_TIME:
                    signal["timestamp"] = ref_time.isoformat()
                    parsed_signals.append(signal)
                else:
                    logger.debug(
                        f"Skipped Pre-market: {signal['trading_symbol']}")

        buffer = ""
        buffer_dates = []

    for msg, dt in zip(messages, dates):
        text = msg.strip()
        if not text:
            continue
        if "FUTURES" in text.upper():
            continue

        # --- Stitching Logic ---

        # 1. Detect Start of New Signal
        is_start_keyword = detect_positional(text)
        has_action = bool(re.search(r"\b(BUY|SELL)\b", text.upper()))
        has_symbol = detect_underlying(text) is not None

        # Strong Start: Keyword OR (Action + Symbol)
        is_strong_new = is_start_keyword or (has_action and has_symbol)

        # 2. Detect Separators
        is_ignore_line = any(k in text.upper() for k in IGNORE_KEYWORDS)
        is_price_noise = is_price_only(text)

        # 3. Partial Logic
        buffer_is_partial = is_partial_signal(buffer) if buffer else False

        # 4. Stale Check (> 5 mins)
        current_ts = to_ist(dt)
        last_ts = to_ist(buffer_dates[-1]) if buffer_dates else current_ts
        is_stale = (current_ts and last_ts) and (
            current_ts - last_ts
        ).total_seconds() > 300

        should_flush = False

        if not buffer:
            should_flush = False
        elif is_stale:
            should_flush = True
        elif is_strong_new:
            # Exception: "Positional" tag followed by signal
            if buffer.strip().upper() in ["POSITIONAL", "RISKY", "POSITIONAL RISKY"]:
                should_flush = False
            else:
                should_flush = True
        elif buffer_is_partial:
            should_flush = is_ignore_line
        else:
            should_flush = is_ignore_line or is_price_noise

        if should_flush:
            flush_buffer()

        buffer = (buffer + "\n" + text) if buffer else text
        buffer_dates.append(dt)

    flush_buffer()  # Final flush

    # --- Deduplication & Saving ---
    new_unique = []
    existing = []

    if os.path.exists(jsonl_path):
        try:
            with open(jsonl_path, "r") as f:
                existing = [json.loads(line) for line in f if line.strip()]
        except:
            pass

    for new_sig in parsed_signals:
        is_dupe = False
        new_ts = to_ist(new_sig.get("timestamp"))
        if new_ts is None:
            continue

        for old_sig in existing:
            if (
                old_sig.get("trading_symbol") == new_sig["trading_symbol"]
                and old_sig.get("action") == new_sig["action"]
            ):
                old_ts = to_ist(old_sig.get("timestamp"))
                if old_ts is not None and (new_ts - old_ts).total_seconds() < (
                    DEDUPE_WINDOW_MINUTES * 60
                ):
                    is_dupe = True
                    break

        if not is_dupe:
            new_unique.append(new_sig)
            existing.append(new_sig)

    if new_unique:
        with open(jsonl_path, "a") as f:
            for sig in new_unique:
                f.write(json.dumps(sig) + "\n")
        with open(json_path, "w") as f:
            json.dump(existing, f, indent=2)

    return new_unique


# --- 6. Robust Test Suite ---

if __name__ == "__main__":
    print(f"\n🧪 Running Test Suite [Time: {now_ist()}]")

    mock_now = datetime(2025, 12, 30, 10, 0, 0)
    if IST:
        mock_now = mock_now.replace(tzinfo=IST)

    test_stream = [
        # 1. Messy One-Liner
        "BANKNIFTY 43500 PE BUY ABOVE 320 SL 280 TARGET 400",
        "SENSEX 87500 CE BUY ABOVE 420 SL 380 TARGET 500",
        # 2. Scrambled
        "SENSEX 86000 CE",
        "ABOVE 500",
        "TARGET 600",
        "BUY",
        # 3. Hero Zero (No Trigger)
        "HERO ZERO",
        "Buy Nifty 24200 CE at market",
        "SL 10",
        "Target Open",
        # 4. Explicit Date (25 DEC)
        "Positional",
        "Buy Sensex 25 Dec 85000 CE Above 1000",
        "SL 800",
        # 5. Noise Handling
        "BUY NIFTY 26000 CE",
        "Above 150",
        "SL 120",
        "160",
        "170",
        "Safe traders book",
        "180.... 🚀",
        # 6. FINNIFTY (Should be IGNORED now)
        "Buy FINNIFTY 21000 CE Above 50 SL 30",
        "Watch Nifty 25000 PE",
        "Keep on radar",
        # 7. Split Signal Outlier
        "Buy Nifty 24000 CE",  # Msg 1
        "Above 120",  # Msg 2 (Looks like noise, but isn't)
        "SL 80",  # Msg 3
        # 8. Force Flush New Signal
        "Buy Sensex 86000 PE",
        "100",
        "SL 50",
        # 9. Multi-line Positional
        "Risky",
        "Buy Banknifty 58700 ce above 420",
        "sl 385",
        "target 5%...550",
        # 10. Stocks
        "Risky",
        "BUY RELIANCE 2500 CE above 80",
        "SL 60",
        "target 5%....50%"
    ]

    test_dates = [mock_now for _ in test_stream]

    results = process_and_save(
        test_stream,
        test_dates,
        jsonl_path="test_signals.jsonl",
        json_path="test_signals.json",
    )

    print(f"\n📊 Processed {len(results)} signals.\n")
    print(f"{'SYMBOL':<35} | {'ACTION':<5} | {'TRIG':<5} | {'SL':<5} | {'POS'}")
    print("-" * 65)

    for r in results:
        trig = str(r["trigger_above"]
                   ) if r["trigger_above"] is not None else "---"
        sl = str(r["stop_loss"]) if r["stop_loss"] is not None else "---"
        pos = "YES" if r["is_positional"] else "NO"
        print(
            f"{r['trading_symbol']:<35} | {r['action']:<5} | {trig:<5} | {sl:<5} | {pos}"
        )
