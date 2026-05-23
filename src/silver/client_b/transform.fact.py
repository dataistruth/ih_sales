"""
src/silver/client_b/transform.fact.py
=======================================
Reads bronze txn for client_b, resolves dimension SKs via broadcast joins,
conforms to canonical fact_transactions schema, and appends into the
CONFORMED silver fact_transactions table partitioned by client_id.

Bronze columns (client_b)
--------------------------
  txn_id, cust_id, total, currency, txn_ts, items, notes

NOT delivered
-------------
  channel, discount_code, is_refund, tax, final_amount,
  payment_type → NULL via conform_to_schema

Run
---
  cd src
  python silver/client_b/transform.fact.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from common.spark_session import get_spark
from common.config import BRONZE_DIR, SILVER_DIR
from common.silver_process_log import (
    get_last_version,
    get_current_bronze_version,
    write_log,
)
from schema.silver_fact_transactions import FACT_TRANSACTIONS_SCHEMA, conform_to_schema

CLIENT = "client_b"
BRONZE_TABLE = "txn"
SILVER_TABLE = "fact_transactions"

BRONZE_PATH = BRONZE_DIR / CLIENT / BRONZE_TABLE
SILVER_PATH = SILVER_DIR / SILVER_TABLE  # conformed — no client subfolder

COLUMN_MAP = {
    "txn_id": "source_transaction_id",
    "cust_id": "source_customer_id",
    "total": "amount",
    "currency": "currency",
    "txn_ts": "created_ts",
    "items": "item_count",
    "notes": "notes",
}


def transform(spark):
    last_version = get_last_version(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE)
    current_version = get_current_bronze_version(spark, BRONZE_PATH)

    print(f"  bronze last processed version : {last_version}")
    print(f"  bronze current version        : {current_version}")

    if current_version <= last_version:
        print("  [SKIP] No new bronze data since last run.")
        return

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
        .withColumn("amount", F.expr("try_cast(amount as double)"))
        .withColumn("gross_amount", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn("net_amount", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn("item_count", F.expr("try_cast(item_count as long)"))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("created_at",
                    F.try_to_timestamp(F.col("created_ts"), F.lit("yyyy-MM-dd'T'HH:mm:ss")))
        .withColumn("txn_date", F.to_date("created_at"))
        .withColumn("date_sk",
                    F.when(F.col("txn_date").isNotNull(),
                           (F.year("txn_date") * 10000
                            + F.month("txn_date") * 100
                            + F.dayofmonth("txn_date")).cast("int")
                           ).otherwise(F.lit(-1).cast("int"))
                    )
        .drop("txn_date")
    )

    df = df.withColumn(
        "transaction_sk",
        (F.monotonically_increasing_id() + 1).cast(LongType())
    )

    # dim_customer — filter to client_b rows only before broadcast
    dim_cust = F.broadcast(
        spark.read.format("delta")
        .load(str(SILVER_DIR / "dim_customer"))
        .filter(F.col("client_id") == CLIENT)
        .select("customer_sk", "source_customer_id")
    )
    df = (
        df.join(dim_cust, on="source_customer_id", how="left")
        .withColumn("customer_sk",
                    F.coalesce(F.col("customer_sk"), F.lit(-1).cast(LongType())))
    )

    # dim_currency
    dim_cur = F.broadcast(
        spark.read.format("delta")
        .load(str(SILVER_DIR / "dim_currency"))
        .select("currency_sk", "currency_code")
    )
    df = (
        df.join(dim_cur, df["currency"] == F.col("currency_code"), "left")
        .withColumn("currency_sk",
                    F.coalesce(F.col("currency_sk"), F.lit(-1).cast(LongType())))
        .drop("currency_code")
    )

    df = conform_to_schema(df, FACT_TRANSACTIONS_SCHEMA)

    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("client_id")
        .option("mergeSchema", "true")
        .save(str(SILVER_PATH))
    )
    print(f"  [APPEND] {SILVER_PATH}  partition=client_id={CLIENT}  ({df.count():,} rows)")

    write_log(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE, current_version)


def main():
    spark = get_spark("silver-fact-client_b")
    print(f"\n── {CLIENT} / {SILVER_TABLE} ──────────────────────────────")
    transform(spark)
    spark.stop()


if __name__ == "__main__":
    main()
from pyspark.sql import functions as F
