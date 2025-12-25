# glue/silver/writer.py

from pyspark.sql import DataFrame, SparkSession
from silver.logger import get_logger

logger = get_logger("silver_writer")


def merge_into_iceberg(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str = "lakehouse.silver_session_events",
    merge_key: str = "event_id",
    temp_view: str = "staging_silver_events",
) -> None:
    """
    Upserts (MERGE) a Spark DataFrame into an existing Iceberg table.

    Assumptions:
    - Iceberg table already exists (lakehouse.silver_session_events)
    - Spark/Glue job is configured to support Iceberg reads/writes (catalog configs)
    - `merge_key` uniquely identifies an event (event_id)

    Behavior:
    - Creates a temp view for the incoming batch
    - MERGE INTO target table on merge_key
    - Raises on failure (fail-fast); logs useful context
    """
    if df is None:
        raise ValueError("Input DataFrame is None")

    if merge_key not in df.columns:
        raise ValueError(f"merge_key '{merge_key}' not found in DataFrame columns: {df.columns}")

    try:
        df.createOrReplaceTempView(temp_view)

        logger.info("Merging into Iceberg table=%s on key=%s", full_table_name, merge_key)

        spark.sql(f"""
            MERGE INTO {full_table_name} t
            USING {temp_view} s
            ON t.{merge_key} = s.{merge_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        logger.info("Merge completed successfully for table=%s", full_table_name)

    except Exception as e:
        logger.exception("Iceberg MERGE failed for table=%s. Error=%s", full_table_name, str(e))
        raise
