# glue/silver/writer.py

from pyspark.sql import DataFrame, SparkSession
from silver.logger import get_logger
import uuid

logger = get_logger("silver_writer")


def _quote_identifier(name: str) -> str:
    """
    Quote a single identifier with backticks, escaping any backticks inside.
    """
    safe = name.replace("`", "``")
    return f"`{safe}`"


def _format_table_name(full_table_name: str) -> str:
    """
    Convert:
      - db.table -> `db`.`table`
      - catalog.db.table -> `catalog`.`db`.`table`
    """
    parts = [p.strip() for p in full_table_name.split(".") if p.strip()]
    if len(parts) not in (2, 3):
        raise ValueError(f"full_table_name must be db.table or catalog.db.table, got: {full_table_name}")

    return ".".join(_quote_identifier(p) for p in parts)


def _candidate_targets(full_table_name: str) -> list[str]:
    """
    Build a list of candidate target names to try.

    If user passes 3-part: catalog.db.table, we will try:
      1) catalog.db.table (as-is)
      2) db.table (drop catalog)

    If user passes 2-part: db.table, we will try only that.
    (If later you introduce a named catalog, you can extend this to prepend it.)
    """
    parts = [p.strip() for p in full_table_name.split(".") if p.strip()]
    if len(parts) == 2:
        return [full_table_name]
    if len(parts) == 3:
        return [full_table_name, ".".join(parts[1:])]
    raise ValueError(f"full_table_name must be db.table or catalog.db.table, got: {full_table_name}")


def merge_into_iceberg(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
    merge_key: str = "event_id",
):
    """
    Merge df into an Iceberg table using Spark SQL MERGE.

    Handles Glue catalog differences by trying both:
      - catalog.db.table (3-part)
      - db.table (2-part) fallback
    """

    if merge_key not in df.columns:
        raise ValueError(f"merge_key='{merge_key}' not found in df columns: {df.columns}")

    # Unique temp view per run
    src_view = f"src_silver_events_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(src_view)

    cols = df.columns

    # SET all columns except merge_key
    set_cols = [c for c in cols if c != merge_key]
    set_clause = ",\n".join([f"t.{_quote_identifier(c)} = s.{_quote_identifier(c)}" for c in set_cols])

    insert_cols = ", ".join([_quote_identifier(c) for c in cols])
    insert_vals = ", ".join([f"s.{_quote_identifier(c)}" for c in cols])

    last_err = None

    for candidate in _candidate_targets(full_table_name):
        target_sql = _format_table_name(candidate)
        logger.info("Attempting MERGE INTO %s (from %s)", candidate, src_view)

        sql_stmt = f"""
            MERGE INTO {target_sql} t
            USING {src_view} s
            ON t.{_quote_identifier(merge_key)} = s.{_quote_identifier(merge_key)}
            WHEN MATCHED THEN UPDATE SET
              {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols})
            VALUES ({insert_vals})
        """

        try:
            spark.sql(sql_stmt)
            logger.info("MERGE succeeded into %s", candidate)
            return
        except Exception as e:
            last_err = e
            msg = str(e)
            logger.warn("MERGE failed for target=%s error=%s", candidate, msg)

            # If it’s the single-part namespace issue, continue to next candidate
            # (the fallback from catalog.db.table -> db.table is in candidates)
            continue

    # If we get here, all candidates failed
    raise RuntimeError(
        f"MERGE failed for all target name candidates derived from '{full_table_name}'. "
        f"Last error: {last_err}"
    ) from last_err
