import sys
import json
import logging
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import BotoCoreError, ClientError

# -------------------------------------------------------------------
# Logging setup (Glue-friendly)
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Read Glue job arguments
# -------------------------------------------------------------------
try:
    args = getResolvedOptions(sys.argv, ["S3_BUCKET", "S3_PREFIX"])
    s3_bucket = args["S3_BUCKET"]
    s3_prefix = args["S3_PREFIX"]
except Exception as e:
    logger.error("Failed to read Glue job arguments", exc_info=True)
    raise

# -------------------------------------------------------------------
# Fetch data
# -------------------------------------------------------------------
def fetch_data():
    """
    TEMP: Fake API response so we can test the Glue job.
    """
    try:
        dummy_data = [
            {"user_id": 1, "session_id": "sess-123", "event": "login"},
            {"user_id": 2, "session_id": "sess-456", "event": "view_page"},
        ]
        logger.info("Fetched %d records", len(dummy_data))
        return dummy_data
    except Exception:
        logger.error("Error while fetching data", exc_info=True)
        raise

# -------------------------------------------------------------------
# Write to S3
# -------------------------------------------------------------------
def write_to_s3(data):
    try:
        s3 = boto3.client("s3")

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{s3_prefix}/events_{ts}.json"

        body = json.dumps(data).encode("utf-8")

        s3.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=body
        )

        logger.info(
            "Successfully wrote %d records to s3://%s/%s",
            len(data), s3_bucket, key
        )

    except (BotoCoreError, ClientError):
        logger.error("AWS S3 error while writing data", exc_info=True)
        raise
    except Exception:
        logger.error("Unexpected error while writing to S3", exc_info=True)
        raise

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    try:
        data = fetch_data()
        write_to_s3(data)
        logger.info("Glue job completed successfully")
    except Exception:
        logger.error("Glue job failed", exc_info=True)
        raise  # Important: fail the Glue job


if __name__ == "__main__":
    main()
