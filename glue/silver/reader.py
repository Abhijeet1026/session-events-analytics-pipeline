# glue/silver/reader.py

from datetime import timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def read_raw_events(
    spark: SparkSession,
    bucket: str,
    raw_prefix: str,
    watermark_ts,
    lookback_days: int,
) -> DataFrame:
    """
    Reads raw parquet from:
      s3://<bucket>/<raw_prefix>/

    Uses partition pruning on event_date (string or date in parquet) by reading only:
      event_date >= (watermark_ts - lookback_days).date()

    Assumes raw is partitioned as:
      .../event_date=YYYY-MM-DD/...
    """
    raw_path = f"s3://{bucket}/{raw_prefix.rstrip('/')}/"
    start_date = (watermark_ts - timedelta(days=lookback_days)).date().isoformat()

    return (
        spark.read.parquet(raw_path)
        .where(F.col("event_date") >= F.lit(start_date))
    )
