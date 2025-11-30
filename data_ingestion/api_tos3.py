import sys
import json
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions

# Glue job arguments: we pass these from the console
args = getResolvedOptions(sys.argv, ["S3_BUCKET", "S3_PREFIX"])

s3_bucket = args["S3_BUCKET"]
s3_prefix = args["S3_PREFIX"]


def fetch_data():
    """
    TEMP: Fake API response so we can test the Glue job.
    """
    dummy_data = [
        {"user_id": 1, "session_id": "sess-123", "event": "login"},
        {"user_id": 2, "session_id": "sess-456", "event": "view_page"},
    ]
    return dummy_data


def write_to_s3(data):
    s3 = boto3.client("s3")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{s3_prefix}/events_{ts}.json"

    body = json.dumps(data).encode("utf-8")
    s3.put_object(Bucket=s3_bucket, Key=key, Body=body)
    print(f"Wrote {len(data)} records to s3://{s3_bucket}/{key}")


def main():
    data = fetch_data()
    write_to_s3(data)


if __name__ == "__main__":
    main()
