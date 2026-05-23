"""
src/gold/build_gold.py
=======================
Builds gold summary tables by:
  1. Checking silver Delta version against gold_process_log
  2. Registering silver Delta tables as Spark temp views
  3. Reading SQL files from src/sql/gold/
  4. Executing via spark.sql()
  5. Writing results to data/gold/ as Delta
  6. Updating gold_process_log

Gold tables
-----------
  gold/gold_sales_summary      – monthly revenue per client/currency
  gold/gold_customer_summary   – customer lifetime value

Run
---
  cd src
  python gold/build_gold.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from common.spark_session import get_spark
from common.config import SILVER_DIR, GOLD_DIR
from common.gold_process_log import (
    get_last_version,
    get_current_version,
    write_log,
)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
SQL_DIR  = ROOT_DIR / "src" / "sql" / "gold"

# ── Each gold table declares which silver table drives its version check ───────
# If a gold table joins multiple silver tables, use the primary/largest one
GOLD_TABLES = {
    "gold_sales_summary":    "fact_transactions",   # driven by fact
    "gold_customer_summary": "fact_transactions",   # driven by fact
}


# ── Register silver temp views ─────────────────────────────────────────────────

def register_views(spark: SparkSession) -> None:
    views = {
        "fact_transactions": SILVER_DIR / "fact_transactions",
        "dim_customer":      SILVER_DIR / "dim_customer",
        "dim_date":          SILVER_DIR / "dim_date",
        "dim_channel":       SILVER_DIR / "dim_channel",
        "dim_currency":      SILVER_DIR / "dim_currency",
    }

    for view_name, path in views.items():
        if not path.exists():
            print(f"  [WARN] Silver table not found, skipping: {path}")
            continue
        spark.read.format("delta").load(str(path)) \
            .createOrReplaceTempView(view_name)
        print(f"  [VIEW] {view_name:<25} ← {path}")


# ── SQL file loader ────────────────────────────────────────────────────────────

def load_sql(table_name: str) -> str:
    sql_file = SQL_DIR / f"{table_name}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    return sql_file.read_text()


# ── Build one gold table ───────────────────────────────────────────────────────

def build_table(spark: SparkSession, gold_table: str, silver_table: str) -> None:
    print(f"\n── {gold_table} ──────────────────────────────")

    # ── Version check ──────────────────────────────────────────────────────────
    last_version    = get_last_version(spark, silver_table, gold_table)
    current_version = get_current_version(spark, SILVER_DIR / silver_table)

    print(f"  silver last processed version : {last_version}")
    print(f"  silver current version        : {current_version}")

    if current_version <= last_version:
        print(f"  [SKIP] No new silver data since last run.")
        return

    # ── Execute SQL ────────────────────────────────────────────────────────────
    sql = load_sql(gold_table)
    df  = spark.sql(sql)

    # ── Write to gold ──────────────────────────────────────────────────────────
    out_path = GOLD_DIR / gold_table
    out_path.mkdir(parents=True, exist_ok=True)

    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(str(out_path))

    print(f"  [WRITE] {out_path}  ({df.count():,} rows)")

    # ── Update log ─────────────────────────────────────────────────────────────
    write_log(spark, silver_table, gold_table, current_version)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("gold-build")

    print(f"Silver dir : {SILVER_DIR}")
    print(f"Gold dir   : {GOLD_DIR}")
    print(f"SQL dir    : {SQL_DIR}")

    print(f"\nRegistering silver temp views …")
    register_views(spark)

    print(f"\nBuilding gold tables …")
    for gold_table, silver_table in GOLD_TABLES.items():
        build_table(spark, gold_table, silver_table)

    print("\nGold build complete.")
    spark.stop()


if __name__ == "__main__":
    main()