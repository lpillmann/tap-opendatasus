import calendar
from datetime import datetime


def get_month_end_date(year_month: str) -> str:
    """Receives year month e.g. 2021-01-01 and returns 2021-01-31"""
    year, month, _ = year_month.split("-")
    _, last_day = calendar.monthrange(int(year), int(month))
    return f"{year}-{month}-{last_day}"
