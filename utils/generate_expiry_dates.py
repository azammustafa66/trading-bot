from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Final, Literal, Optional
from zoneinfo import ZoneInfo

KOLKATA: Final = ZoneInfo("Asia/Kolkata")
# Days of week (Monday=0 ... Sunday=6)
TUESDAY: Final[int] = 1
THURSDAY: Final[int] = 3

def _next_weekday_on_or_after(start: date, weekday: int) -> date:
    """Return the next date >= start that matches the given weekday."""
    days_ahead = (weekday - start.weekday()) % 7
    return start + timedelta(days=days_ahead)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last specific weekday (e.g., last Tuesday) of a given month."""
    # Get the last day of the month
    last_day_num = calendar.monthrange(year, month)[1]
    last_date = date(year, month, last_day_num)

    # Calculate offset to the desired weekday
    offset = (last_date.weekday() - weekday) % 7
    return last_date - timedelta(days=offset)


def select_expiry_date(
    underlying: str, reference_dt: Optional[datetime] = None
) -> date:
    """
    Calculates expiry date based on rules:
      - NIFTY: Coming Tuesday (Weekly)
      - SENSEX: Coming Thursday (Weekly)
      - BANKNIFTY: Last Tuesday of current month (Monthly)
      - STOCKS: Last Thursday of current month (Monthly) - same as BANKNIFTY monthly expiry

    Args:
        underlying: Stock symbol (e.g., 'NIFTY', 'BANKNIFTY', 'RELIANCE', 'TCS', etc.)
        reference_dt: Awareness-agnostic datetime. Defaults to current IST if None.
    """
    if not isinstance(underlying, str):
        raise TypeError("underlying must be a string")

    u = underlying.strip().upper()

    # 1. Normalize Reference Date to IST
    if reference_dt is None:
        now = datetime.now(KOLKATA) if KOLKATA else datetime.now()
    else:
        now = reference_dt
        # If naive, assume IST; if aware, convert to IST
        if KOLKATA and now.tzinfo is None:
            now = now.replace(tzinfo=KOLKATA)
        elif KOLKATA:
            now = now.astimezone(KOLKATA)

    today = now.date()

    # 2. Apply Expiry Rules
    if u == "NIFTY":
        # Rule: Every Tuesday (Weekly)
        return _next_weekday_on_or_after(today, TUESDAY)

    if u == "SENSEX":
        # Rule: Every Thursday (Weekly)
        return _next_weekday_on_or_after(today, THURSDAY)

    if u == "BANKNIFTY":
        # Rule: Last Tuesday of the month (Monthly)
        current_month_last_tue = _last_weekday_of_month(
            today.year, today.month, TUESDAY
        )

        if current_month_last_tue >= today:
            return current_month_last_tue

        # Move to next month's last Tuesday
        next_month = today.month + 1
        next_year = today.year
        if next_month > 12:
            next_month = 1
            next_year += 1

        return _last_weekday_of_month(next_year, next_month, TUESDAY)

    # Default: Stock options (Monthly - Last Thursday, same as BANKNIFTY monthly expiry)
    # This handles RELIANCE, TCS, INFY, and any other stock options dynamically
    current_month_last_thu = _last_weekday_of_month(
        today.year, today.month, THURSDAY
    )

    if current_month_last_thu >= today:
        return current_month_last_thu

    # Move to next month's last Thursday
    next_month = today.month + 1
    next_year = today.year
    if next_month > 12:
        next_month = 1
        next_year += 1

    return _last_weekday_of_month(next_year, next_month, THURSDAY)


def select_expiry_label(
    underlying: str, reference_dt: Optional[datetime] = None
) -> str:
    """Returns expiry in 'DD MMM' format (e.g., '27 NOV')."""
    d = select_expiry_date(underlying, reference_dt)
    return f"{d.day:02d} {d.strftime('%b').upper()}"


# --- Testing Block ---
if __name__ == "__main__":
    # Test with the current time
    now = datetime.now(KOLKATA) if KOLKATA else datetime.now()
    print(f"Current Reference (IST): {now}\n")

    indices = ["NIFTY", "SENSEX", "BANKNIFTY"]

    print(f"{'INDEX':<12} | {'RULE':<15} | {'EXPIRY DATE':<12} | {'LABEL'}")
    print("-" * 55)

    for ind in indices:
        exp_date = select_expiry_date(ind, now)
        label = select_expiry_label(ind, now)

        rule_desc = "Weekly Tue"
        if ind == "SENSEX":
            rule_desc = "Weekly Thu"
        if ind == "BANKNIFTY":
            rule_desc = "Monthly Tue"

        print(f"{ind:<12} | {rule_desc:<15} | {exp_date}   | {label}")
