"""
src/common/silver_process_log.py
=================================
Utility for reading and writing the silver_file_process_log Delta table.

The log tracks which bronze Delta version was last successfully processed
by each silver transform, enabling incremental reads.

Schema
------
  client_name    – e.g. "client_a"
  bronze_table   – e.g. "customers" / "txn"
  silver_table   – e.g. "dim_customer" / "fact_transactions"
  bronze_version – Delta table version number last processed (long)
  processed_at   – ISO-8601 UTC timestamp of the silver run

Usage
-----
  from common.silver_process_log import get_last_version, write_log

  last_version = get_last_version(spark, "client_a", "customers", "dim_customer")
  # ... transform ...
  write_log(spark, "client_a", "customers", "dim_customer", new_version)
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
LOG_DIR  = ROOT_DIR / "data" / "log" / "silver_file_process_log"

LOG_SCHEMA = StructType([
    StructField("client_name",    StringType(), False),
    StructField("bronze_table",   StringType(), False),
    StructField("silver_table",   StringType(), False),
    StructField("bronze_version", LongType(),   False),
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
    client_name: str,
    bronze_table: str,
    silver_table: str,
) -> int:
    """
    Return the last bronze Delta version processed for this
    (client, bronze_table, silver_table) combination.
    Returns -1 if never processed — meaning read from version 0.
    """
    _ensure_log_table(spark)

    df = (
        spark.read.format("delta").load(str(LOG_DIR))
        .filter(
            (F.col("client_name")  == client_name)  &
            (F.col("bronze_table") == bronze_table)  &
            (F.col("silver_table") == silver_table)
        )
        .orderBy(F.col("bronze_version").desc())
        .limit(1)
    )

    rows = df.collect()
    return rows[0]["bronze_version"] if rows else -1


def get_current_bronze_version(spark: SparkSession, bronze_path: Path) -> int:
    """Return the latest version number of a bronze Delta table."""
    dt = DeltaTable.forPath(spark, str(bronze_path))
    history = dt.history(1).select("version").collect()
    return history[0]["version"] if history else 0


def write_log(
    spark: SparkSession,
    client_name: str,
    bronze_table: str,
    silver_table: str,
    bronze_version: int,
) -> None:
    """
    Append a new entry to the silver_file_process_log.
    Uses Delta merge to upsert — one row per (client, bronze_table, silver_table).
    """
    _ensure_log_table(spark)

    processed_at = datetime.now(timezone.utc).isoformat()

    new_row = spark.createDataFrame(
        [(client_name, bronze_table, silver_table, bronze_version, processed_at)],
        LOG_SCHEMA,
    )

    log_table = DeltaTable.forPath(spark, str(LOG_DIR))

    (
        log_table.alias("log")
        .merge(
            new_row.alias("new"),
            "log.client_name  = new.client_name  AND "
            "log.bronze_table = new.bronze_table AND "
            "log.silver_table = new.silver_table"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"  [LOG] {client_name}/{bronze_table} → {silver_table}  "
          f"bronze_version={bronze_version}  at={processed_at}")