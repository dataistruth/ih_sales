"""
src/silver/client_a/transform.dim.py
======================================
Reads bronze customers for client_a, applies column mapping,
conforms to canonical dim_customer schema, and merges into the
CONFORMED silver dim_customer table shared across all clients.

Merge keys : ["client_id", "source_customer_id"]
SK          : assigned on INSERT only, never overwritten on UPDATE
SCD Type 1  : matched rows fully updated (no history)

Run
---
  cd src
  python silver/client_a/transform.dim.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F
from common.spark_session import get_spark
from common.config import BRONZE_DIR, SILVER_DIR
from common.silver_process_log import (
    get_last_version,
    get_current_bronze_version,
    write_log,
)
from common.delta_utils import merge_dim
from schema.silver_dim_customer import DIM_CUSTOMER_SCHEMA, conform_to_schema

CLIENT       = "client_a"
BRONZE_TABLE = "customers"
SILVER_TABLE = "dim_customer"

BRONZE_PATH = BRONZE_DIR / CLIENT / BRONZE_TABLE
SILVER_PATH = SILVER_DIR / SILVER_TABLE          # ← conformed, no client subfolder

# ── Column mapping: bronze → canonical ────────────────────────────────────────
# client_a delivers: customer_id, first_name, last_name, email, phone,
#                    signup_ts, status, loyalty_points
# NOT delivered:     country, address, zip_code, vip_flag,
#                    marketing_opt_in, age → NULL via conform_to_schema
COLUMN_MAP = {
    "customer_id":    "source_customer_id",
    "first_name":     "first_name",
    "last_name":      "last_name",
    "email":          "email",
    "phone":          "phone",
    "signup_ts":      "signup_ts",
    "status":         "status",
    "loyalty_points": "loyalty_points",
}


def transform(spark):
    # ── Version check ──────────────────────────────────────────────────────────
    last_version    = get_last_version(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE)
    current_version = get_current_bronze_version(spark, BRONZE_PATH)

    print(f"  bronze last processed version : {last_version}")
    print(f"  bronze current version        : {current_version}")

    if current_version <= last_version:
        print("  [SKIP] No new bronze data since last run.")
        return

    # ── Read bronze — selective select ────────────────────────────────────────
    df = (
        spark.read.format("delta")
        .option("versionAsOf", current_version)
        .load(str(BRONZE_PATH))
        .filter(F.col("_is_corrupt") == False)
        .select(list(COLUMN_MAP.keys()) + ["_ingested_at"])
    )

    # ── Rename to canonical names ──────────────────────────────────────────────
    for src, tgt in COLUMN_MAP.items():
        df = df.withColumnRenamed(src, tgt)

    # ── Derive attributes ──────────────────────────────────────────────────────
    df = (
        df
        .withColumn("client_id", F.lit(CLIENT))
        .withColumn("full_name",
            F.trim(F.concat_ws(" ",
                F.coalesce(F.col("first_name"), F.lit("")),
                F.coalesce(F.col("last_name"),  F.lit("")),
            ))
        )
        .withColumn("signup_date",
            F.to_date(
                F.try_to_timestamp(F.col("signup_ts"), F.lit("yyyy-MM-dd'T'HH:mm:ss"))
            )
        )
        .withColumn("loyalty_points", F.expr("try_cast(loyalty_points as long)"))
        .withColumn("email",  F.lower(F.trim(F.col("email"))))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .drop("signup_ts")
    )

    # ── Conform to canonical schema ────────────────────────────────────────────
    # Adds NULL for columns not delivered by client_a:
    # country, address, zip_code, vip_flag, marketing_opt_in, age, age_band
    df = conform_to_schema(df, DIM_CUSTOMER_SCHEMA)

    # ── Merge into conformed dim_customer ──────────────────────────────────────
    merge_dim(
        spark        = spark,
        source_df    = df,
        target_path  = SILVER_PATH,
        merge_keys   = ["client_id", "source_customer_id"],
        sequence_col = "_ingested_at",
        sk_col       = "customer_sk",
    )

    # ── Update process log ─────────────────────────────────────────────────────
    write_log(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE, current_version)


def main():
    spark = get_spark("silver-dim-client_a")
    print(f"\n── {CLIENT} / {SILVER_TABLE} ──────────────────────────────")
    transform(spark)
    spark.stop()


if __name__ == "__main__":
    main()