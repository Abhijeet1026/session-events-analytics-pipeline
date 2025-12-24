from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone, date


def parse_iso_ts(ts_str: Optional[str]) -> Optional[datetime]:
    if not ts_str:
        return None
    s = ts_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


@dataclass
class NormalizedEvent:
    event_id: str
    session_id: str
    username: Optional[str]
    event_type: str
    platform: str
    event_ts: datetime
    event_date: date
    batch_id: str
    ingested_at: datetime
    device: Optional[str]
    os: Optional[str]
    country: Optional[str]
    url: Optional[str]
    browser_version: Optional[str]
    screen: Optional[str]
    screen_size: Optional[str]
    app_version: Optional[str]


def normalize_event(
    e: Dict[str, Any],
    batch_id: str,
    default_event_date: date,
    ingested_at: datetime,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (row_dict, error_reason). If invalid, row_dict=None and error_reason is set.
    """
    try:
        attrs = e.get("attributes") or {}

        event_id = e.get("id")
        session_id = e.get("session_id")
        event_type = e.get("event_type")
        platform = e.get("platform")
        dt_str = e.get("dt")
        ts_str = e.get("event_timestamp")

        if not event_id:
            return None, "missing_id"
        if not session_id:
            return None, "missing_session_id"
        if not event_type:
            return None, "missing_event_type"
        if not platform:
            return None, "missing_platform"

        event_ts = parse_iso_ts(ts_str)
        if event_ts is None:
            return None, "invalid_event_timestamp"

        event_date = default_event_date
        if dt_str:
            try:
                event_date = date.fromisoformat(dt_str)
            except Exception:
                return None, "invalid_dt"

        row = {
            "event_id": event_id,
            "session_id": session_id,
            "username": attrs.get("username"),
            "event_type": event_type,
            "platform": platform,
            "event_ts": event_ts,
            "event_date": event_date,
            "batch_id": batch_id,
            "ingested_at": ingested_at,
            "device": attrs.get("device"),
            "os": attrs.get("os"),
            "country": attrs.get("geo"),
            "url": e.get("url"),
            "browser_version": e.get("browser_version"),
            "screen": e.get("screen"),
            "screen_size": e.get("screen_size"),
            "app_version": e.get("app_version"),
        }
        return row, None

    except Exception:
        return None, "unexpected_normalize_error"
