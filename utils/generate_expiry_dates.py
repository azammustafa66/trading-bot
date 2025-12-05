import calendar
from datetime import date, datetime, timedelta
from typing import Final, Optional, Set
from zoneinfo import ZoneInfo

# --- CONFIGURATION ---
KOLKATA: Final = ZoneInfo("Asia/Kolkata")
TUESDAY: Final[int] = 1
THURSDAY: Final[int] = 3

TRADING_HOLIDAYS: Final[Set[date]] = {
    # --- 2025 ---
    date(2025, 12, 25),  # Christmas

    # --- 2026 (Projected) ---
    date(2026, 1, 26),  # Republic Day
    date(2026, 3, 3),   # Holi (Approx)
    date(2026, 3, 27),  # Ram Navami
    date(2026, 4, 3),   # Good Friday
    date(2026, 4, 14),  # Ambedkar Jayanti
    date(2026, 5, 1),   # Maharashtra Day
}


def _is_trading_holiday(d: date) -> bool:
    """Checks if a date is a known trading holiday."""
    return d in TRADING_HOLIDAYS


def _adjust_for_holiday(expiry_date: date) -> date:
    """
    If expiry_date is a holiday, move to the PREVIOUS trading day.
    Recursively checks if the previous day is also a holiday.
    """
    while _is_trading_holiday(expiry_date):
        expiry_date -= timedelta(days=1)
        # Ensure we don't accidentally land on a weekend after shifting
        # (Though exchange holidays usually account for this, logic makes it robust)
        if expiry_date.weekday() >= 5:  # 5=Sat, 6=Sun
            expiry_date -= timedelta(days=(expiry_date.weekday() - 4))
    return expiry_date


def _next_weekday_on_or_after(start: date, weekday: int) -> date:
    """Return the next date >= start that matches the given weekday."""
    days_ahead = (weekday - start.weekday()) % 7
    return start + timedelta(days=days_ahead)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last specific weekday of a given month."""
    last_day_num = calendar.monthrange(year, month)[1]
    last_date = date(year, month, last_day_num)
    offset = (last_date.weekday() - weekday) % 7
    return last_date - timedelta(days=offset)


def select_expiry_date(underlying: str, reference_dt: Optional[datetime] = None) -> date:
    """
    Calculates expiry based on User Rules + Holiday Logic:
      - NIFTY     : Every Tuesday
      - SENSEX    : Every Thursday (Holiday Logic ACTIVE)
      - BANKNIFTY : Last Tuesday of Month
      - STOCKS    : Last Tuesday of Month
    """
    if reference_dt is None:
        now = datetime.now(KOLKATA) if KOLKATA else datetime.now()
    else:
        now = reference_dt
        if KOLKATA and now.tzinfo is None:
            now = now.replace(tzinfo=KOLKATA)
        elif KOLKATA:
            now = now.astimezone(KOLKATA)

    today = now.date()
    u = underlying.strip().upper()

    calculated_date = today  # Default

    # --- RULE 1: WEEKLY INDICES ---
    if u == "NIFTY":
        calculated_date = _next_weekday_on_or_after(today, TUESDAY)
        # UNCOMMENT TO ACTIVATE HOLIDAY LOGIC FOR NIFTY:
        # calculated_date = _adjust_for_holiday(calculated_date)
        return calculated_date

    if u == "SENSEX":
        calculated_date = _next_weekday_on_or_after(today, THURSDAY)
        # --- ACTIVE HOLIDAY LOGIC (USER REQUEST) ---
        # Checks if Thursday is a holiday (e.g., 25 Dec 2025).
        # If yes, moves to previous trading day (24 Dec).
        calculated_date = _adjust_for_holiday(calculated_date)
        return calculated_date

    # --- RULE 2: MONTHLY (BANKNIFTY vs STOCKS) ---

    # 1. Calculate THIS month's expiry (Last Tuesday)
    current_month_exp = _last_weekday_of_month(
        today.year, today.month, TUESDAY)

    # 2. Define Rollover Condition
    should_rollover = False

    if u == "BANKNIFTY":
        if today > current_month_exp:
            should_rollover = True
    else:
        # STOCKS
        if today >= current_month_exp:
            should_rollover = True

    # 3. Determine Base Date
    if should_rollover:
        next_month = today.month + 1
        next_year = today.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        calculated_date = _last_weekday_of_month(
            next_year, next_month, TUESDAY)
    else:
        calculated_date = current_month_exp

    # UNCOMMENT TO ACTIVATE HOLIDAY LOGIC FOR MONTHLY EXPIRIES:
    # calculated_date = _adjust_for_holiday(calculated_date)

    return calculated_date


def select_expiry_label(underlying: str, reference_dt: Optional[datetime] = None) -> str:
    """Returns expiry in 'DD MMM' format (e.g., '27 NOV')."""
    d = select_expiry_date(underlying, reference_dt)
    return f"{d.day:02d} {d.strftime('%b').upper()}"


# --- Testing Block ---
if __name__ == "__main__":
    print("\n🗓️  EXPIRY DATE CALCULATOR (STOCKS vs BANKNIFTY)")
    print("=" * 65)

    # TEST CASE: Today is Tuesday, 25 Nov 2025 (The Last Tuesday/Expiry Day)
    mock_today = datetime(2025, 11, 25, 9, 15)

    print(
        f"Testing Reference Date: {mock_today.strftime('%Y-%m-%d %A')} (EXPIRY DAY)\n")

    test_cases = ["NIFTY", "SENSEX", "BANKNIFTY", "RELIANCE"]

    print(f"{'SYMBOL':<12} | {'EXPIRY DATE':<12} | {'STATUS'}")
    print("-" * 65)

    for sym in test_cases:
        exp_date = select_expiry_date(sym, mock_today)
        label = select_expiry_label(sym, mock_today)

        status = "Normal"
        if exp_date == mock_today.date():
            status = "Today (0DTE)"
        elif exp_date.month > mock_today.month:
            status = "Rolled Over (Next Month)"

        print(f"{sym:<12} | {label:<12} | {status}")

    print("=" * 65)
    print("Notice: BANKNIFTY trades today, RELIANCE moved to Dec.")
