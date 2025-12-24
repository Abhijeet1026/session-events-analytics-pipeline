import sys
import time
import logging
from datetime import datetime, timezone, date
from io import BytesIO
from typing import Any, Dict, List

import boto3
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from awsglue.utils import getResolvedOptions

# Glue arg parser (optional)


# ---- Modular ingestion utilities ----
from ingestion.logging_utils import get_logger, log_batch_summary
from ingestion.schema import normalize_event
from ingestion.dq import split_good_bad

# -------------------------------------------------------------------
# Logger
# -------------------------------------------------------------------
logger = get_logger("api_to_s3_ingestion")

# -------------------------------------------------------------------
# Stable Parquet schema
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
def read_args():
    required = ["S3_BUCKET", "S3_PREFIX", "API_URL"]
    optional = ["COUNT", "PLATFORM", "NUM_BATCHES", "TIMEOUT_SECS", "MAX_RETRIES"]

    try:
        args = getResolvedOptions(sys.argv, required + optional)
    except Exception:
        args = getResolvedOptions(sys.argv, required)

    # Enforce defaults if optional args not passed from Glue/Terraform
    args.setdefault("COUNT", "20000")
    args.setdefault("PLATFORM", "web")
    args.setdefault("NUM_BATCHES", "5")
    args.setdefault("TIMEOUT_SECS", "30")
    args.setdefault("MAX_RETRIES", "3")

    return args

# -------------------------------------------------------------------
# HTTP with retries
# -------------------------------------------------------------------
def http_post_with_retries(
    url: str,
    payload: Dict[str, Any],
    timeout_secs: int,
    max_retries: int,
) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, json=payload, timeout=timeout_secs)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(
                    f"Retryable status={resp.status_code}, body={resp.text}",
                    response=resp,
                )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            if attempt == max_retries:
                break
            sleep_s = min(2 ** (attempt - 1), 8)
            logger.warning(
                "API call failed (attempt %d/%d). Sleeping %ds. Error=%s",
                attempt, max_retries, sleep_s, str(e)
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"API call failed after {max_retries} attempts") from last_err

# -------------------------------------------------------------------
# Parquet writer
# -------------------------------------------------------------------
def write_parquet_to_s3(
    s3_client,
    bucket: str,
    prefix: str,
    event_date: date,
    batch_id: str,
    rows: List[Dict[str, Any]],
) -> str:
    if not rows:
        raise ValueError("No rows to write")

    table = pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy", use_dictionary=True)
    buf.seek(0)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = (
        f"{prefix.rstrip('/')}/event_date={event_date.isoformat()}"
        f"/batch_id={batch_id}/part-{ts}.parquet"
    )
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

    count = int(args.get("COUNT", "10000"))
    platform = args.get("PLATFORM", "web")
    num_batches = int(args.get("NUM_BATCHES", "1"))
    timeout_secs = int(args.get("TIMEOUT_SECS", "30"))
    max_retries = int(args.get("MAX_RETRIES", "3"))

    logger.info(
        "Starting ingestion: bucket=%s prefix=%s api_url=%s count=%d platform=%s batches=%d",
        s3_bucket, s3_prefix, api_url, count, platform, num_batches
    )

    s3 = boto3.client("s3")

    total_good = 0
    total_bad = 0

    for i in range(num_batches):
        payload = {"count": count, "platform": platform}
        logger.info("Fetching batch %d/%d from API", i + 1, num_batches)

        api_resp = http_post_with_retries(
            url=api_url,
            payload=payload,
            timeout_secs=timeout_secs,
            max_retries=max_retries,
        )

        ingested_at = datetime.now(timezone.utc)
        batch_id = api_resp.get("batch_id", "unknown")
        dt_str = api_resp.get("dt")
        event_date = date.fromisoformat(dt_str) if dt_str else ingested_at.date()
        events = api_resp.get("events", [])

        normalized_rows = []
        bad_schema = []

        for e in events:
            row, err = normalize_event(
                e=e,
                batch_id=batch_id,
                default_event_date=event_date,
                ingested_at=ingested_at,
            )
            if row:
                normalized_rows.append(row)
            else:
                bad_schema.append({"raw": e, "dq_reason": err})

        good_rows, bad_dq, dq_reason_counts = split_good_bad(normalized_rows)

        if good_rows:
            write_parquet_to_s3(
                s3_client=s3,
                bucket=s3_bucket,
                prefix=s3_prefix,
                event_date=event_date,
                batch_id=batch_id,
                rows=good_rows,
            )

        if bad_schema or bad_dq:
            bad_rows = bad_dq + bad_schema
            write_parquet_to_s3(
                s3_client=s3,
                bucket=s3_bucket,
                prefix=f"{s3_prefix}_bad",
                event_date=event_date,
                batch_id=batch_id,
                rows=bad_rows,
            )

        log_batch_summary(
            logger,
            {
                "batch_id": batch_id,
                "event_date": event_date.isoformat(),
                "received": len(events),
                "good": len(good_rows),
                "bad_schema": len(bad_schema),
                "bad_dq": len(bad_dq),
                "dq_reasons": dq_reason_counts,
            },
        )

        total_good += len(good_rows)
        total_bad += len(bad_schema) + len(bad_dq)

    logger.info(
        "Ingestion complete. total_good=%d total_bad=%d",
        total_good, total_bad
    )

# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except (BotoCoreError, ClientError):
        logger.error("AWS error", exc_info=True)
        raise
    except Exception:
        logger.error("Job failed", exc_info=True)
        raise
