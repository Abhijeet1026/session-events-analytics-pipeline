from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone, timedelta


ALLOWED_PLATFORMS = {"web", "mobile"}


def dq_check_row(row: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Lightweight sanity rules. Returns (is_valid, reason_if_invalid).
    """
    # platform allowlist
    plat = row.get("platform")
    if plat and plat not in ALLOWED_PLATFORMS:
        return False, "invalid_platform"

    # timestamp not too far in future
    ts = row.get("event_ts")
    if ts:
        now = datetime.now(timezone.utc)
        if ts > now + timedelta(minutes=5):
            return False, "event_ts_in_future"

    return True, ""


def split_good_bad(
    normalized_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """
    Returns (good_rows, bad_rows, reason_counts)
    bad_rows include a 'dq_reason' field.
    """
    good: List[Dict[str, Any]] = []
    bad: List[Dict[str, Any]] = []
    reason_counts: Dict[str, int] = {}

    for r in normalized_rows:
        ok, reason = dq_check_row(r)
        if ok:
            good.append(r)
        else:
            br = dict(r)
            br["dq_reason"] = reason
            bad.append(br)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    return good, bad, reason_counts
