"""
src/common/gold_process_log.py
================================
Tracks which silver Delta version was last successfully processed
by each gold table — mirrors the pattern of silver_process_log.

Schema
------
  silver_table   – e.g. "fact_transactions" / "dim_customer"
  gold_table     – e.g. "gold_sales_summary"
  silver_version – Delta version number last processed (long)
  processed_at   – ISO-8601 UTC timestamp

Usage
-----
  from common.gold_process_log import get_last_version, get_current_version, write_log

  last    = get_last_version(spark, "fact_transactions", "gold_sales_summary")
  current = get_current_version(spark, SILVER_DIR / "fact_transactions")

  if current > last:
      # run transform
      write_log(spark, "fact_transactions", "gold_sales_summary", current)
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType,
)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
LOG_DIR  = ROOT_DIR / "data" / "log" / "gold_file_process_log"

LOG_SCHEMA = StructType([
    StructField("silver_table",   StringType(), False),
    StructField("gold_table",     StringType(), False),
    StructField("silver_version", LongType(),   False),
    StructField("processed_at",   StringType(), False),
])


def _ensure_log_table(spark: SparkSession) -> None:
    """Create the log Delta table if it doesn't exist yet."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if not (LOG_DIR / "_delta_log").exists():
        empty = spark.createDataFrame([], LOG_SCHEMA)
        empty.write.format("delta").mode("overwrite").save(str(LOG_DIR))


def get_last_version(
    spark: SparkSession,
    silver_table: str,
    gold_table: str,
) -> int:
    """
    Return the last silver Delta version processed for this
    (silver_table, gold_table) combination.
    Returns -1 if never processed.
    """
    _ensure_log_table(spark)

    rows = (
        spark.read.format("delta").load(str(LOG_DIR))
        .filter(
            (F.col("silver_table") == silver_table) &
            (F.col("gold_table")   == gold_table)
        )
        .orderBy(F.col("silver_version").desc())
        .limit(1)
        .collect()
    )
    return rows[0]["silver_version"] if rows else -1


def get_current_version(spark: SparkSession, silver_path: Path) -> int:
    """Return the latest version number of a silver Delta table."""
    dt      = DeltaTable.forPath(spark, str(silver_path))
    history = dt.history(1).select("version").collect()
    return history[0]["version"] if history else 0


def write_log(
    spark: SparkSession,
    silver_table: str,
    gold_table: str,
    silver_version: int,
) -> None:
    """
    Upsert a log entry — one row per (silver_table, gold_table).
    """
    _ensure_log_table(spark)

    processed_at = datetime.now(timezone.utc).isoformat()

    new_row = spark.createDataFrame(
        [(silver_table, gold_table, silver_version, processed_at)],
        LOG_SCHEMA,
    )

    log_table = DeltaTable.forPath(spark, str(LOG_DIR))

    (
        log_table.alias("log")
        .merge(
            new_row.alias("new"),
            "log.silver_table = new.silver_table AND "
            "log.gold_table   = new.gold_table"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"  [LOG] {silver_table} → {gold_table}  "
          f"silver_version={silver_version}  at={processed_at}")