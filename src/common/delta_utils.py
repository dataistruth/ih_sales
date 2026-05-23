"""
src/common/delta_utils.py
==========================
Reusable Delta merge utility for SCD Type 1 dimensions.

Functions
---------
  merge_dim()   – dedup source, then MERGE into target Delta table
                  MATCHED     → UPDATE all cols except SK
                  NOT MATCHED → INSERT with new monotonically_increasing_id SK

Usage
-----
  from common.delta_utils import merge_dim

  merge_dim(
      spark       = spark,
      source_df   = df,
      target_path = SILVER_PATH,
      merge_keys  = ["client_id", "source_customer_id"],
      sequence_col= "_ingested_at",
      sk_col      = "customer_sk",
  )
"""

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType


def _dedup(df: DataFrame, merge_keys: list[str], sequence_col: str) -> DataFrame:
    """
    Keep the latest row per merge key combination before merging.
    Latest = highest sequence_col value (e.g. most recent _ingested_at).
    Uses dropDuplicates after sorting so no window shuffle warning.
    """
    return (
        df
        .orderBy(F.col(sequence_col).desc())
        .dropDuplicates(merge_keys)
    )


def merge_dim(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str | Path,
    merge_keys: list[str],
    sequence_col: str,
    sk_col: str,
) -> None:
    """
    SCD Type 1 merge into a Delta dimension table.

    Parameters
    ----------
    spark        : active SparkSession
    source_df    : incoming DataFrame (already conformed to canonical schema)
    target_path  : path to the target Delta table
    merge_keys   : columns that uniquely identify a dimension member
                   e.g. ["client_id", "source_customer_id"]
    sequence_col : column used to pick latest row on dedup before merge
                   e.g. "_ingested_at"
    sk_col       : surrogate key column name — only assigned on INSERT,
                   never overwritten on UPDATE
                   e.g. "customer_sk"
    """
    target_path = Path(target_path)

    # ── Dedup source before merge ──────────────────────────────────────────────
    source_df = _dedup(source_df, merge_keys, sequence_col)

    # ── Build merge condition ──────────────────────────────────────────────────
    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in merge_keys]
    )

    # ── All columns except SK — used for UPDATE and INSERT ────────────────────
    all_cols     = source_df.columns
    non_sk_cols  = [c for c in all_cols if c != sk_col]

    update_map = {c: f"source.{c}" for c in non_sk_cols}
    insert_map = {c: f"source.{c}" for c in non_sk_cols}

    # ── First run — target doesn't exist yet, just write ──────────────────────
    if not (target_path / "_delta_log").exists():
        print(f"  [CREATE] Target not found — creating: {target_path}")
        target_path.mkdir(parents=True, exist_ok=True)

        # Assign SK on first insert
        source_df = source_df.withColumn(
            sk_col,
            (F.monotonically_increasing_id() + 1).cast(LongType())
        )
        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(target_path))
        )
        inserted = source_df.count()
        print(f"  [INSERT] {inserted:,} rows inserted  →  {target_path}")
        return

    # ── Subsequent runs — merge ────────────────────────────────────────────────
    target = DeltaTable.forPath(spark, str(target_path))

    # For new rows (NOT MATCHED), generate SK from max existing SK + offset
    # so new SKs don't collide with existing ones
    max_sk = (
        target.toDF()
        .agg(F.max(sk_col))
        .collect()[0][0] or 0
    )

    source_df = source_df.withColumn(
        sk_col,
        (F.monotonically_increasing_id() + max_sk + 1).cast(LongType())
    )

    insert_map[sk_col] = f"source.{sk_col}"   # include SK only on INSERT

    (
        target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_map)       # Type 1 — overwrite all non-SK cols
        .whenNotMatchedInsert(values=insert_map) # new row — assign new SK
        .execute()
    )

    # Print merge stats from Delta history
    history = target.history(1).select("operationMetrics").collect()[0][0]
    updated  = history.get("numTargetRowsUpdated", "?")
    inserted = history.get("numTargetRowsInserted", "?")
    print(f"  [MERGE]  updated={updated}  inserted={inserted}  →  {target_path}")