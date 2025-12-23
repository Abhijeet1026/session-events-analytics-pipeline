import sys
import json
import time
import logging
from datetime import datetime, timezone, date
from io import BytesIO
from typing import Any, Dict, List, Tuple, Optional

import boto3
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError

# Glue arg parser (optional). If not running on Glue, we fall back to argparse.
try:
    from awsglue.utils import getResolvedOptions  # type: ignore
    GLUE_AVAILABLE = True
except Exception:
    GLUE_AVAILABLE = False


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Stable Parquet schema (based on your real Lambda payload)
# -------------------------------------------------------------------
PARQUET_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("session_id", pa.string()),
    ("username", pa.string()),
    ("event_type", pa.string()),
    ("platform", pa.string()),
    ("event_ts", pa.timestamp("ms")),
    ("event_date", pa.date32()),
    ("batch_id", pa.string()),
    ("ingested_at", pa.timestamp("ms")),

    ("device", pa.string()),
    ("os", pa.string()),
    ("country", pa.string()),

    ("url", pa.string()),
    ("browser_version", pa.string()),

    ("screen", pa.string()),
    ("screen_size", pa.string()),
    ("app_version", pa.string()),
])


# -------------------------------------------------------------------
# Args
# -------------------------------------------------------------------
def read_args() -> Dict[str, str]:
    """
    Supports both Glue job args and local CLI args.

    Glue:
      --S3_BUCKET <...> --S3_PREFIX <...> --API_URL <...> --COUNT <...> ...

    Local:
      python ingest.py --S3_BUCKET ... --S3_PREFIX ... --API_URL ...
    """
    if GLUE_AVAILABLE:
        required = ["S3_BUCKET", "S3_PREFIX", "API_URL"]
        optional = ["COUNT", "PLATFORM", "NUM_BATCHES", "TIMEOUT_SECS", "MAX_RETRIES"]
        # Glue will throw if optional are missing; so we try required-only first
        try:
            args = getResolvedOptions(sys.argv, required + optional)
        except Exception:
            args = getResolvedOptions(sys.argv, required)
            # fill defaults
            args["COUNT"] = "1000"
            args["PLATFORM"] = "web"
            args["NUM_BATCHES"] = "1"
            args["TIMEOUT_SECS"] = "30"
            args["MAX_RETRIES"] = "3"
        return args

    # Local argparse fallback
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--S3_BUCKET", required=True)
    p.add_argument("--S3_PREFIX", required=True)
    p.add_argument("--API_URL", required=True)
    p.add_argument("--COUNT", default="1000")
    p.add_argument("--PLATFORM", default="web")
    p.add_argument("--NUM_BATCHES", default="1")
    p.add_argument("--TIMEOUT_SECS", default="30")
    p.add_argument("--MAX_RETRIES", default="3")
    ns = p.parse_args()

    return vars(ns)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def parse_iso_ts(ts_str: Optional[str]) -> Optional[datetime]:
    """
    Parse timestamps like:
      2025-12-23T02:57:57.793404+00:00
      2025-12-23T02:57:57Z
    Returns UTC datetime or None.
    """
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


def http_post_with_retries(
    url: str,
    payload: Dict[str, Any],
    timeout_secs: int,
    max_retries: int
) -> Dict[str, Any]:
    """
    Calls API Gateway with simple exponential backoff.
    Retries on 429/5xx and transient errors.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, json=payload, timeout=timeout_secs)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Retryable status={resp.status_code}, body={resp.text}", response=resp)

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            last_err = e
            if attempt == max_retries:
                break
            sleep_s = min(2 ** (attempt - 1), 8)
            logger.warning("API call failed (attempt %d/%d). Sleeping %ds. Error=%s", attempt, max_retries, sleep_s, str(e))
            time.sleep(sleep_s)

    raise RuntimeError(f"API call failed after {max_retries} attempts") from last_err


def normalize_events(api_resp: Dict[str, Any]) -> Tuple[date, str, List[Dict[str, Any]]]:
    """
    Converts API response into row dicts matching PARQUET_SCHEMA.
    """
    batch_id = api_resp.get("batch_id") or "unknown"
    dt_str = api_resp.get("dt")  # "YYYY-MM-DD"
    if not dt_str:
        event_date = datetime.now(timezone.utc).date()
    else:
        event_date = date.fromisoformat(dt_str)

    ingested_at = datetime.now(timezone.utc)

    events = api_resp.get("events", [])
    if not isinstance(events, list):
        raise ValueError("API response 'events' is not a list")

    rows: List[Dict[str, Any]] = []
    for e in events:
        attrs = e.get("attributes") or {}

        row = {
            "event_id": e.get("id"),
            "session_id": e.get("session_id"),
            "username": attrs.get("username"),
            "event_type": e.get("event_type"),
            "platform": e.get("platform"),

            "event_ts": parse_iso_ts(e.get("event_timestamp")),
            "event_date": date.fromisoformat(e.get("dt")) if e.get("dt") else event_date,

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
        rows.append(row)

    return event_date, batch_id, rows


def write_parquet_to_s3(
    s3_client,
    bucket: str,
    prefix: str,
    event_date: date,
    batch_id: str,
    rows: List[Dict[str, Any]]
) -> str:
    """
    Writes one Parquet file to S3. Returns the S3 key.
    """
    if not rows:
        raise ValueError("No rows to write")

    table = pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA)

    buf = BytesIO()
    pq.write_table(
        table,
        buf,
        compression="snappy",
        use_dictionary=True
    )
    buf.seek(0)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dt_part = event_date.isoformat()
    prefix = prefix.rstrip("/")

    key = f"{prefix}/event_date={dt_part}/batch_id={batch_id}/part-{ts}.parquet"

    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.read())

    logger.info("Wrote %d rows to s3://%s/%s", table.num_rows, bucket, key)
    return key


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    args = read_args()

    s3_bucket = args["S3_BUCKET"]
    s3_prefix = args["S3_PREFIX"]
    api_url = args["API_URL"]

    count = int(args.get("COUNT", "1000"))
    platform = args.get("PLATFORM", "web")
    num_batches = int(args.get("NUM_BATCHES", "1"))
    timeout_secs = int(args.get("TIMEOUT_SECS", "30"))
    max_retries = int(args.get("MAX_RETRIES", "3"))

    logger.info(
        "Starting ingestion: bucket=%s prefix=%s api_url=%s count=%d platform=%s batches=%d",
        s3_bucket, s3_prefix, api_url, count, platform, num_batches
    )

    s3 = boto3.client("s3")

    total = 0
    keys: List[str] = []

    for i in range(num_batches):
        payload = {"count": count, "platform": platform}
        logger.info("Fetching batch %d/%d from API", i + 1, num_batches)

        api_resp = http_post_with_retries(
            url=api_url,
            payload=payload,
            timeout_secs=timeout_secs,
            max_retries=max_retries
        )

        event_date, batch_id, rows = normalize_events(api_resp)
        key = write_parquet_to_s3(s3, s3_bucket, s3_prefix, event_date, batch_id, rows)

        keys.append(key)
        total += len(rows)

    logger.info("Ingestion complete. Total events=%d files_written=%d", total, len(keys))
    for k in keys:
        logger.info("  s3://%s/%s", s3_bucket, k)


if __name__ == "__main__":
    try:
        main()
    except (BotoCoreError, ClientError):
        logger.error("AWS S3 error", exc_info=True)
        raise
    except Exception:
        logger.error("Job failed", exc_info=True)
        raise
