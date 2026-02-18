from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import re

_OFFSET_RE = re.compile(r"^(?:UTC)?\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?$", re.IGNORECASE)
_ALIAS_MAP: dict[str, timezone] = {
    "asia/shanghai": timezone(timedelta(hours=8)),
    "beijing": timezone(timedelta(hours=8)),
    "prc": timezone(timedelta(hours=8)),
    "utc+8": timezone(timedelta(hours=8)),
    "utc+08:00": timezone(timedelta(hours=8)),
    "+08:00": timezone(timedelta(hours=8)),
}


def parse_timezone(value: str) -> tzinfo:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("timezone cannot be empty")

    lower = raw.lower()
    if lower in {"utc", "z"}:
        return timezone.utc

    alias = _ALIAS_MAP.get(lower)
    if alias is not None:
        return alias

    match = _OFFSET_RE.match(raw)
    if match:
        sign = 1 if match.group(1) == "+" else -1
        hours = int(match.group(2))
        minutes = int(match.group(3) or "0")
        if hours > 23 or minutes > 59:
            raise ValueError(f"invalid timezone offset: {value}")
        delta = timedelta(hours=hours, minutes=minutes) * sign
        return timezone(delta)

    try:
        return ZoneInfo(raw)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid timezone: {value}") from exc


def date_key_for_timezone(value: datetime, tz: tzinfo) -> str:
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).strftime("%Y-%m-%d")
