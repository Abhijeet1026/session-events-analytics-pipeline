# glue/silver/watermark.py

import json
from datetime import datetime, timezone
import boto3


_EPOCH_TS = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _parse_iso_ts(ts: str) -> datetime:
    """
    Parse ISO timestamp like:
    - 2025-01-01T10:15:30Z
    - 2025-01-01T10:15:30+00:00
    Returns UTC datetime.
    """
    ts = ts.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def read_watermark(bucket: str, key: str) -> datetime:
    """
    Reads watermark JSON from S3:
      { "last_processed_event_ts": "..." }

    Returns:
      UTC datetime for last_processed_event_ts
      Epoch if file is missing / invalid
    """
    s3 = boto3.client("s3")

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        payload = json.loads(obj["Body"].read().decode("utf-8"))

        ts_str = payload.get("last_processed_event_ts")
        if not ts_str:
            return _EPOCH_TS

        return _parse_iso_ts(ts_str)

    except Exception:
        # Any error → safe default
        return _EPOCH_TS
