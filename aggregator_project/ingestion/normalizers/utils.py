from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from django.utils.dateparse import parse_datetime


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # Assume milliseconds if larger than year 3000 in seconds
        seconds = value / 1000 if value > 32503680000 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if isinstance(value, str):
        parsed = parse_datetime(value)
        if parsed:
            return parsed
    return None
