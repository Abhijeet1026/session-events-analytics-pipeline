# glue/silver/stats.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_stats(df_raw: DataFrame, df_new: DataFrame) -> dict:
    """
    Returns basic metrics for logging/validation.
    """
    rows_raw_scanned = df_raw.count()
    rows_new = df_new.count()

    max_ts = df_new.select(F.max("event_ts").alias("mx")).collect()[0]["mx"]

    return {
        "rows_raw_scanned": rows_raw_scanned,
        "rows_new": rows_new,
        "max_event_ts_new": str(max_ts) if max_ts is not None else None,
    }
