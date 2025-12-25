# glue/scripts/silver_transform_job.py

import sys
import json
import boto3
from datetime import timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions

from silver.logger import get_logger
from silver.watermark import read_watermark
from silver.reader import read_raw_events
from silver.stats import compute_stats
from silver.writer import merge_into_iceberg

logger = get_logger("silver_transform")

# --- Telecom-ish business mappings ---
ATLANTIC = {"Nova Scotia", "New Brunswick", "Prince Edward Island", "Newfoundland and Labrador"}
PRAIRIES = {"Alberta", "Saskatchewan", "Manitoba"}


def _ensure_timestamp(df, colname: str) -> "DataFrame":
    """
    Ensure df[colname] is Spark TimestampType.
    """
    if colname not in df.columns:
        return df
    # handle string timestamps safely
    return df.withColumn(colname, F.to_timestamp(F.col(colname)))


def _ensure_date(df, colname: str) -> "DataFrame":
    """
    Ensure df[colname] is Spark DateType.
    """
    if colname not in df.columns:
        return df
    return df.withColumn(colname, F.to_date(F.col(colname)))


def _safe_add_batch_id(df, batch_id: str) -> "DataFrame":
    """
    Add batch_id ONLY if it doesn't already exist (prevents COLUMN_ALREADY_EXISTS warning).
    """
    if "batch_id" in df.columns:
        return df
    return df.withColumn("batch_id", F.lit(batch_id))


def add_telecom_transforms(df):
    """
    Lightweight + realistic:
    - normalize enums
    - derive time features
    - device family bucket
    - geo region bucket (Canada provinces)
    - event category bucket
    """
    # Normalize strings
    if "platform" in df.columns:
        df = df.withColumn("platform", F.lower(F.trim(F.col("platform"))))
    if "event_type" in df.columns:
        df = df.withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    if "device" in df.columns:
        df = df.withColumn("device", F.trim(F.col("device")))
    if "country" in df.columns:
        df = df.withColumn("country", F.trim(F.col("country")))

    # Derived time columns
    if "event_ts" in df.columns:
        df = df.withColumn("event_hour", F.hour(F.col("event_ts")))

    # Device family bucketing
    if "device" in df.columns:
        df = df.withColumn(
            "device_family",
            F.when(F.col("device").rlike("(?i)iphone|ipad"), F.lit("apple_mobile"))
             .when(F.col("device").rlike("(?i)pixel|samsung|android"), F.lit("android_mobile"))
             .when(F.col("device").rlike("(?i)macbook|\\bmac\\b"), F.lit("apple_desktop"))
             .when(F.col("device").rlike("(?i)windows"), F.lit("windows_desktop"))
             .otherwise(F.lit("other"))
        )
    else:
        df = df.withColumn("device_family", F.lit("unknown"))

    # Province → region (your data now has provinces; in your schema it lands in `country`)
    if "country" in df.columns:
        df = df.withColumn(
            "geo_region",
            F.when(F.col("country").isin(list(ATLANTIC)), F.lit("atlantic"))
             .when(F.col("country").isin(list(PRAIRIES)), F.lit("prairies"))
             .when(F.col("country") == "Ontario", F.lit("ontario"))
             .when(F.col("country") == "Quebec", F.lit("quebec"))
             .when(F.col("country") == "British Columbia", F.lit("bc"))
             .otherwise(F.lit("unknown"))
        )
    else:
        df = df.withColumn("geo_region", F.lit("unknown"))

    # Event category (telecom-friendly grouping)
    if "event_type" in df.columns:
        df = df.withColumn(
            "event_category",
            F.when(F.col("event_type").isin("pageview", "clicks", "referral"), F.lit("engagement"))
             .when(F.col("event_type").isin("log_in", "log_off", "signups"), F.lit("account"))
             .when(F.col("event_type").isin("delete_account"), F.lit("churn_signal"))
             .otherwise(F.lit("other"))
        )
    else:
        df = df.withColumn("event_category", F.lit("other"))

    return df


def silver_standardize(df):
    """
    Generic silver cleanup:
    - cast event_ts / event_date
    - drop null event_id/event_ts rows
    - dedupe by event_id keeping latest event_ts
    """
    df = _ensure_timestamp(df, "event_ts")
    df = _ensure_date(df, "event_date")

    # basic row quality
    if "event_id" in df.columns:
        df = df.where(F.col("event_id").isNotNull())
    if "event_ts" in df.columns:
        df = df.where(F.col("event_ts").isNotNull())

    # dedupe
    if "event_id" in df.columns and "event_ts" in df.columns:
        w = (
            F.row_number().over(
                __import__("pyspark").sql.window.Window
                .partitionBy("event_id")
                .orderBy(F.col("event_ts").desc())
            )
        )
        df = df.withColumn("_rn", w).where(F.col("_rn") == 1).drop("_rn")

    return df


def _parse_args():
    base = ["S3_BUCKET", "RAW_PREFIX", "WATERMARK_KEY"]
    optional = ["LOOKBACK_DAYS", "ICEBERG_TABLE"]
    argv = " ".join(sys.argv)

    keys = base[:]
    for k in optional:
        if f"--{k}" in argv:
            keys.append(k)

    return getResolvedOptions(sys.argv, keys)


def main():
    args = _parse_args()

    bucket = args["S3_BUCKET"]
    raw_prefix = args["RAW_PREFIX"]
    watermark_key = args["WATERMARK_KEY"]
    lookback_days = int(args.get("LOOKBACK_DAYS", "3"))

    # ✅ IMPORTANT: default to db.table (NOT glue_catalog.db.table)
    iceberg_table = args.get("ICEBERG_TABLE", "glue_catalog.lakehouse.silver_session_events")

    spark = (
        SparkSession.builder
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{bucket}/iceberg/")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

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

    # Ensure types before filtering
    df_raw = _ensure_timestamp(df_raw, "event_ts")
    df_raw = _ensure_date(df_raw, "event_date")

    # 3) Incremental filter by event_ts
    # watermark_ts is tz-aware; Spark timestamp is tz-naive -> strip tzinfo for literal compare
    wm_naive = watermark_ts.replace(tzinfo=None)
    df_new = df_raw.where(F.col("event_ts") > F.lit(wm_naive))

    # optional batch_id for traceability (won't overwrite if exists)
    batch_id = F.date_format(F.current_timestamp(), "yyyyMMddHHmmss").cast("string")
    df_new = _safe_add_batch_id(df_new, "run_" + spark.range(1).select(batch_id.alias("b")).collect()[0]["b"])

    # 4) Silver standardize + telecom transforms
    df_tx = silver_standardize(df_new)
    df_tx = add_telecom_transforms(df_tx)

    # 5) MERGE into Iceberg (fixed naming)
    logger.info("merging_into_iceberg_table=%s", iceberg_table)
    merge_into_iceberg(
        spark=spark,
        df=df_tx,
        full_table_name=iceberg_table,
        merge_key="event_id",
    )

    # 6) Update watermark ONLY after successful merge
    max_ts = df_tx.select(F.max("event_ts").alias("mx")).collect()[0]["mx"]
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
    else:
        logger.info("no_new_rows_so_watermark_not_updated")

    # 7) Stats (for CloudWatch)
    stats = compute_stats(df_raw=df_raw, df_new=df_new)
    logger.info(
        "rows_raw_scanned=%s rows_new=%s max_event_ts_new=%s",
        stats.get("rows_raw_scanned"), stats.get("rows_new"), stats.get("max_event_ts_new")
    )

    # Small sanity sample in logs
    cols = [c for c in ["event_id", "event_type", "platform", "country", "geo_region", "device_family", "event_hour", "event_category"] if c in df_tx.columns]
    logger.info("sample_rows_cols=%s", cols)
    for r in df_tx.select(*cols).limit(5).collect():
        logger.info(str(r))


if __name__ == "__main__":
    main()
