"""
src/silver/client_c/transform.dim.py
======================================
Reads bronze customers for client_c, applies column mapping,
conforms to canonical dim_customer schema, and merges into the
CONFORMED silver dim_customer table shared across all clients.

Bronze columns (client_c)
--------------------------
  id, fname, lname, signup_date, age, marketing_opt_in

NOT delivered
-------------
  email, phone, country, address, zip_code, status,
  loyalty_points, vip_flag → NULL via conform_to_schema

Run
---
  cd src
  python silver/client_c/transform.dim.py
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

CLIENT       = "client_c"
BRONZE_TABLE = "customers"
SILVER_TABLE = "dim_customer"

BRONZE_PATH = BRONZE_DIR / CLIENT / BRONZE_TABLE
SILVER_PATH = SILVER_DIR / SILVER_TABLE          # conformed — no client subfolder

COLUMN_MAP = {
    "id":               "source_customer_id",
    "fname":            "first_name",
    "lname":            "last_name",
    "signup_date":      "signup_ts",
    "age":              "age",
    "marketing_opt_in": "marketing_opt_in",
}


def transform_customer(spark):
    last_version    = get_last_version(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE)
    current_version = get_current_bronze_version(spark, BRONZE_PATH)

    print(f"  bronze last processed version : {last_version}")
    print(f"  bronze current version        : {current_version}")

    if current_version <= last_version:
        print("  [SKIP] No new bronze data since last run.")
        return

    bool_map = F.create_map(
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
        .withColumn("age",        F.expr("try_cast(age as long)"))
        .withColumn("age_band",
            F.when(F.col("age") < 25,  F.lit("18-24"))
             .when(F.col("age") < 35,  F.lit("25-34"))
             .when(F.col("age") < 45,  F.lit("35-44"))
             .when(F.col("age") < 55,  F.lit("45-54"))
             .when(F.col("age") < 65,  F.lit("55-64"))
             .when(F.col("age") >= 65, F.lit("65+"))
             .otherwise(F.lit(None))
        )
        .withColumn("marketing_opt_in",
            bool_map[F.lower(F.trim(F.col("marketing_opt_in")))])
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
    spark = get_spark("silver-dim-client_c")
    print(f"\n── {CLIENT} / {SILVER_TABLE} ──────────────────────────────")
    transform_customer(spark)
    spark.stop()


if __name__ == "__main__":
    main()