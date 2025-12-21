import os
import json
import boto3
from datetime import datetime, timezone

glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
BASE_PREFIX = os.environ.get("BASE_PREFIX", "lakehouse/user_events")  # dt-only

def _resp(code: int, body: dict):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }

def lambda_handler(event, context):
    try:
        body = event.get("body") or "{}"
        if isinstance(body, str):
            body = json.loads(body)

        dt = body.get("dt") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        target_gb = int(body.get("target_gb", 5))

        # keep it safe
        if target_gb < 1 or target_gb > 10:
            return _resp(400, {"error": "target_gb must be between 1 and 10"})

        s3_prefix = f"{BASE_PREFIX}/dt={dt}"

        run = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--S3_BUCKET": S3_BUCKET,
                "--S3_PREFIX": s3_prefix,
                "--TARGET_GB": str(target_gb),  # ok if Glue ignores for now
                "--DT": dt,
            },
        )

        return _resp(200, {
            "message": "Glue job started",
            "job_name": GLUE_JOB_NAME,
            "job_run_id": run["JobRunId"],
            "s3_output": f"s3://{S3_BUCKET}/{s3_prefix}/",
        })

    except Exception as e:
        return _resp(500, {"error": str(e)})
