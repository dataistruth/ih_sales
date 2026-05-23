"""
src/bronze/ingest_to_bronze.py
==============================
Reads every active entry from client JSON configs in /config,
monitors /data/raw_landing/<client>/<wildcard> via Structured Streaming,
and writes new files incrementally to /data/bronze/<client>/<bronze_table>/
as Delta.

Key behaviours
--------------
  - Wildcard file patterns   : customers_*.txt picks up every monthly drop
  - Checkpoint tracking      : only new files processed on each run
  - availableNow trigger     : processes all pending files then shuts down cleanly
  - Schema inference         : inferred from first matching file
  - mergeSchema              : new columns added by client are accepted automatically
  - Bad row handling         : PERMISSIVE mode, corrupt rows flagged with _is_corrupt
  - Audit columns            : _client, _source_file, _ingested_at on every row

Run
---
  cd src
  python bronze/ingest_to_bronze.py
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField
from common.spark_session import get_spark
from common.bronze_df_utils import add_audit_columns

# ── Project paths ──────────────────────────────────────────────────────────────
ROOT_DIR   = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = ROOT_DIR / "config"
RAW_DIR    = ROOT_DIR / "data" / "raw_landing"
BRONZE_DIR = ROOT_DIR / "data" / "bronze"
CHK_DIR    = ROOT_DIR / "data" / "checkpoints" / "bronze"


# ── Config loader ──────────────────────────────────────────────────────────────

def load_client_configs(config_dir: Path) -> list[dict]:
    """
    Scan config_dir for all *.json files and return a flat list of
    file entries, each merged with its parent client_name.
    """
    configs = []
    for config_file in sorted(config_dir.glob("*.json")):
        with open(config_file) as f:
            client_cfg = json.load(f)
        for file_entry in client_cfg["files"]:
            configs.append({
                "client_name": client_cfg["client_name"],
                **file_entry,
            })
    return configs


# ── Stream ingestion ───────────────────────────────────────────────────────────

def ingest_stream(spark, entry: dict, ingested_at: str):
    """
    Start one streaming query for a single client file pattern.
    Returns the StreamingQuery or None if no matching files found.
    """
    client   = entry["client_name"]
    filename = entry["file_name"]                        # e.g. customers_*.txt
    sep      = entry["separator"].replace("\\t", "\t")   # handle tab escape
    table_nm = entry["bronze_table"]                     # e.g. customers / txn
    src_path = RAW_DIR   / client / filename
    out_path = BRONZE_DIR / client / table_nm
    chk_path = CHK_DIR   / client / table_nm            # checkpoint per table

    # ── Schema inference ───────────────────────────────────────────────────────
    # Find first matching file to infer column names — all as StringType
    # since bronze is a faithful landing zone (no casting here)
    first_file = next(RAW_DIR.joinpath(client).glob(filename), None)
    if first_file is None:
        print(f"  [SKIP] No files matching: {src_path}")
        return None

    base_schema = (
        spark.read
        .option("header",      str(entry["is_header"]).lower())
        .option("sep",         sep)
        .option("inferSchema", "false")                  # keep everything as string
        .csv(str(first_file))
        .schema
    )

    # Append _corrupt_record explicitly so PERMISSIVE mode can populate it
    schema = base_schema.add(StructField("_corrupt_record", StringType(), True))

    print(f"  [STREAM] {src_path}")
    print(f"           sink : {out_path}")
    print(f"           chk  : {chk_path}")
    print(f"           cols : {[f.name for f in base_schema.fields]}")

    # ── Read stream ────────────────────────────────────────────────────────────
    df = (
        spark.readStream
        .schema(schema)
        .option("header",                    str(entry["is_header"]).lower())
        .option("sep",                       sep)
        .option("mode",                      "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(str(src_path))
        .withColumn("is_header",   F.lit(False))
        .withColumn("_is_corrupt", F.col("_corrupt_record").isNotNull())
    )

    # ── Audit columns ──────────────────────────────────────────────────────────
    df = add_audit_columns(df, client, filename, ingested_at)

    # ── Write stream ───────────────────────────────────────────────────────────
    chk_path.mkdir(parents=True, exist_ok=True)
    out_path.mkdir(parents=True, exist_ok=True)

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(chk_path))
        .option("mergeSchema",        "true")            # accept new columns
        .trigger(availableNow=True)                      # process all pending, then stop
        .start(str(out_path))
    )

    return query


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark       = get_spark("bronze-ingestion")
    ingested_at = datetime.now(timezone.utc).isoformat()

    print(f"Spark UI   : {spark.sparkContext.uiWebUrl}")
    print(f"Config dir : {CONFIG_DIR}")
    print(f"Raw dir    : {RAW_DIR}")
    print(f"Bronze dir : {BRONZE_DIR}")
    print()

    # ── Load and filter configs ────────────────────────────────────────────────
    all_entries = load_client_configs(CONFIG_DIR)
    active      = [e for e in all_entries if e["is_active"]]
    skipped     = [e for e in all_entries if not e["is_active"]]

    print(f"Registry   : {len(all_entries)} files total  "
          f"({len(active)} active, {len(skipped)} inactive)\n")

    # ── Start one stream per active entry ──────────────────────────────────────
    queries        = []
    current_client = None

    for entry in active:
        if entry["client_name"] != current_client:
            current_client = entry["client_name"]
            print(f"── {current_client} ──────────────────────────────")

        q = ingest_stream(spark, entry, ingested_at)
        if q:
            queries.append(q)

    if not queries:
        print("No streams started — check that raw_landing files exist.")
        spark.stop()
        return

    # ── Wait for all streams to finish ─────────────────────────────────────────
    # availableNow=True means each stream self-terminates after processing
    # all pending files. awaitTermination() blocks until each one is done.
    print(f"\n{len(queries)} stream(s) running:")
    for q in queries:
        print(f"  id={q.id}  name={q.name}  status={q.status['message']}")

    for q in queries:
        q.awaitTermination()

    print("\nAll streams complete. Spark shutting down.")
    spark.stop()


if __name__ == "__main__":
    main()