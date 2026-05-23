"""
src/silver/client_b/transform.dim.py
======================================
Reads bronze customers for client_b, applies column mapping,
conforms to canonical dim_customer schema, and merges into the
CONFORMED silver dim_customer table shared across all clients.

Bronze columns (client_b)
--------------------------
  cust_id, full_name, contact_email, country, address,
  zip, created_date, vip_flag

NOT delivered
-------------
  first_name, last_name (derived from full_name split)
  phone, status, loyalty_points, marketing_opt_in, age → NULL

Run
---
  cd src
  python silver/client_b/transform.dim.py
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

CLIENT       = "client_b"
BRONZE_TABLE = "customers"
SILVER_TABLE = "dim_customer"

BRONZE_PATH = BRONZE_DIR / CLIENT / BRONZE_TABLE
SILVER_PATH = SILVER_DIR / SILVER_TABLE          # conformed — no client subfolder

COLUMN_MAP = {
    "cust_id":       "source_customer_id",
    "full_name":     "_full_name",               # split into first/last below
    "contact_email": "email",
    "country":       "country",
    "address":       "address",
    "zip":           "zip_code",
    "created_date":  "signup_ts",
    "vip_flag":      "vip_flag",
}


def transform(spark):
    last_version    = get_last_version(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE)
    current_version = get_current_bronze_version(spark, BRONZE_PATH)

    print(f"  bronze last processed version : {last_version}")
    print(f"  bronze current version        : {current_version}")

    if current_version <= last_version:
        print("  [SKIP] No new bronze data since last run.")
        return

    bool_map = F.create_map(
        F.lit("y"), F.lit(True),  F.lit("n"), F.lit(False),
        F.lit("1"), F.lit(True),  F.lit("0"), F.lit(False),
        F.lit("true"), F.lit(True), F.lit("false"), F.lit(False),
    )

    df = (
        spark.read.format("delta")
        .option("versionAsOf", current_version)
        .load(str(BRONZE_PATH))
        .filter(F.col("_is_corrupt") == False)
        .select(list(COLUMN_MAP.keys()) + ["_ingested_at"])
    )

    for src, tgt in COLUMN_MAP.items():
        df = df.withColumnRenamed(src, tgt)

    # ── Split full_name → first_name / last_name ───────────────────────────────
    df = (
        df
        .withColumn("_name_parts", F.split(F.trim(F.col("_full_name")), r"\s+", 2))
        .withColumn("first_name",
            F.when(F.size("_name_parts") > 1, F.col("_name_parts")[0])
             .otherwise(F.lit(None)))
        .withColumn("last_name",
            F.when(F.size("_name_parts") > 1, F.col("_name_parts")[1])
             .otherwise(F.col("_name_parts")[0]))
        .drop("_full_name", "_name_parts")
    )

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
        .withColumn("vip_flag",  bool_map[F.lower(F.trim(F.col("vip_flag")))])
        .withColumn("email",     F.lower(F.trim(F.col("email"))))
        .withColumn("country",   F.upper(F.trim(F.col("country"))))
        .drop("signup_ts")
    )

    df = conform_to_schema(df, DIM_CUSTOMER_SCHEMA)

    merge_dim(
        spark        = spark,
        source_df    = df,
        target_path  = SILVER_PATH,
        merge_keys   = ["client_id", "source_customer_id"],
        sequence_col = "_ingested_at",
        sk_col       = "customer_sk",
    )

    write_log(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE, current_version)


def main():
    spark = get_spark("silver-dim-client_b")
    print(f"\n── {CLIENT} / {SILVER_TABLE} ──────────────────────────────")
    transform(spark)
    spark.stop()


if __name__ == "__main__":
    main()