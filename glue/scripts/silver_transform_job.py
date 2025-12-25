# glue/scripts/silver_transform_job.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
import json
import boto3
from datetime import timezone

from silver.logger import get_logger
from silver.watermark import read_watermark
from silver.reader import read_raw_events
from silver.stats import compute_stats
from silver.writer import merge_into_iceberg



logger = get_logger("silver_transform")


# --- Telecom-ish business mappings ---
ATLANTIC = {"Nova Scotia", "New Brunswick", "Prince Edward Island", "Newfoundland and Labrador"}
PRAIRIES = {"Alberta", "Saskatchewan", "Manitoba"}


def add_telecom_transforms(df):
    """
    Keep these lightweight + realistic:
    - normalize enums
    - derive time features
    - device family bucket
    - geo region bucket (Canada provinces)
    - event category bucket (telecom style: engagement/account/billing/support)
    """
    # Normalize strings
    df = df.withColumn("platform", F.lower(F.trim(F.col("platform"))))
    df = df.withColumn("event_type", F.lower(F.trim(F.col("event_type"))))

    # Derived time columns
    df = df.withColumn("event_hour", F.hour(F.col("event_ts")))

    # Device family bucketing
    df = df.withColumn(
        "device_family",
        F.when(F.col("device").rlike("(?i)iphone|ipad"), F.lit("apple_mobile"))
         .when(F.col("device").rlike("(?i)pixel|samsung|android"), F.lit("android_mobile"))
         .when(F.col("device").rlike("(?i)macbook|mac"), F.lit("apple_desktop"))
         .when(F.col("device").rlike("(?i)windows"), F.lit("windows_desktop"))
         .otherwise(F.lit("other"))
    )

    # Province → region (your data now has provinces; in your schema it lands in `country`)
    df = df.withColumn(
        "geo_region",
        F.when(F.col("country").isin(list(ATLANTIC)), F.lit("atlantic"))
         .when(F.col("country").isin(list(PRAIRIES)), F.lit("prairies"))
         .when(F.col("country") == "Ontario", F.lit("ontario"))
         .when(F.col("country") == "Quebec", F.lit("quebec"))
         .when(F.col("country") == "British Columbia", F.lit("bc"))
         .otherwise(F.lit("unknown"))
    )

    # Event category (telecom-friendly grouping)
    df = df.withColumn(
        "event_category",
        F.when(F.col("event_type").isin("pageview", "clicks", "referral"), F.lit("engagement"))
         .when(F.col("event_type").isin("log_in", "log_off", "signups"), F.lit("account"))
         .when(F.col("event_type").isin("delete_account"), F.lit("churn_signal"))
         .otherwise(F.lit("other"))
    )

    return df


def main():
    # keep args minimal; LOOKBACK_DAYS optional
    arg_keys = ["S3_BUCKET", "RAW_PREFIX", "WATERMARK_KEY"]
    args = getResolvedOptions(sys.argv, arg_keys + ["LOOKBACK_DAYS"]) if "LOOKBACK_DAYS" in " ".join(sys.argv) else getResolvedOptions(sys.argv, arg_keys)

    bucket = args["S3_BUCKET"]
    raw_prefix = args["RAW_PREFIX"]
    watermark_key = args["WATERMARK_KEY"]
    lookback_days = int(args.get("LOOKBACK_DAYS", "3"))

    spark = SparkSession.builder.getOrCreate()

    # 1) Read watermark
    watermark_ts = read_watermark(bucket=bucket, key=watermark_key)
    logger.info("watermark_last_processed_event_ts=%s", watermark_ts.isoformat())

    # 2) Read raw (partition pruned by event_date)
    df_raw = read_raw_events(
        spark=spark,
        bucket=bucket,
        raw_prefix=raw_prefix,
        watermark_ts=watermark_ts,
        lookback_days=lookback_days
    )

    # 3) Incremental filter by event_ts
    df_new = df_raw.where(F.col("event_ts") > F.lit(watermark_ts.replace(tzinfo=None)))

    # 4) Apply telecom-ish transforms
    df_tx = add_telecom_transforms(df_new)

    merge_into_iceberg(
    spark=spark,
    df=df_tx,
    full_table_name="lakehouse.silver_session_events",
    merge_key="event_id",
        )
    
    max_ts = df_new.select(F.max("event_ts").alias("mx")).collect()[0]["mx"]
    if max_ts is not None:
        new_wm = {"last_processed_event_ts": max_ts.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")}
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=watermark_key,
            Body=json.dumps(new_wm).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("watermark_updated_to=%s", new_wm["last_processed_event_ts"])


    # 5) Stats (for CloudWatch)
    stats = compute_stats(df_raw=df_raw, df_new=df_new)
    logger.info("rows_raw_scanned=%s rows_new=%s max_event_ts_new=%s",
                stats["rows_raw_scanned"], stats["rows_new"], stats["max_event_ts_new"])

    # Small sanity sample in logs
    logger.info("sample_rows:")
    for r in df_tx.select("event_id", "event_type", "platform", "country", "geo_region", "device_family", "event_hour", "event_category").limit(5).collect():
        logger.info(str(r))


if __name__ == "__main__":
    main()
