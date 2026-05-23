"""
src/common/bronze_df_utils.py
==============================
Reusable DataFrame utilities shared across all pipeline layers.

Functions
---------
  add_audit_columns()   – attaches _client, _source_file, _ingested_at
  write_batch()         – writes a static DataFrame to delta
"""

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Audit columns
# ─────────────────────────────────────────────────────────────────────────────

def add_audit_columns(
    df: DataFrame,
    client: str,
    source_file: str,
    ingested_at: str | None = None,
) -> DataFrame:
    """
    Attach standard provenance columns to any DataFrame.

    Columns added
    -------------
      _client       – source client identifier (e.g. "client_a")
      _source_file  – originating filename (e.g. "customers_2024_01.txt")
      _ingested_at  – ISO-8601 UTC timestamp of the pipeline run

    Parameters
    ----------
    df           : input DataFrame (batch or streaming)
    client       : client name string
    source_file  : filename or path string
    ingested_at  : optional timestamp string; defaults to now() if not supplied
    """
    if ingested_at is None:
        ingested_at = datetime.now(timezone.utc).isoformat()

    return (
        df
        .withColumn("_client",      F.lit(client))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.lit(ingested_at))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Batch write – Delta only
# ─────────────────────────────────────────────────────────────────────────────

def write_batch(
    df: DataFrame,
    out_path: str | Path,
    mode: str = "overwrite",
) -> None:
    """
    Write a static (batch) DataFrame to Delta format and print a summary line.

    Parameters
    ----------
    df       : static DataFrame to write
    out_path : destination directory
    mode     : "overwrite" (default) or "append"
    """
    out_path = Path(out_path)
    out_path.mkdir(parents=True, exist_ok=True)

    df.write.format("delta").mode(mode).save(str(out_path))

    row_count = df.count()
    print(f"  [WRITE] {out_path}  ({row_count:,} rows, {len(df.columns)} cols)")