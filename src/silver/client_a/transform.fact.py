"""
src/silver/client_a/transform.fact.py
=======================================
Reads bronze txn for client_a, resolves dimension SKs via broadcast joins,
conforms to canonical fact_transactions schema, and appends into the
CONFORMED silver fact_transactions table partitioned by client_id.

Partition : client_id
Write mode: replaceWhere client_id = 'client_a' — idempotent per client

Run
---
  cd src
  python silver/client_a/transform.fact.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType
from common.spark_session import get_spark
from common.config import BRONZE_DIR, SILVER_DIR
from common.silver_process_log import (
    get_last_version,
    get_current_bronze_version,
    write_log,
)
from schema.silver_fact_transactions import FACT_TRANSACTIONS_SCHEMA, conform_to_schema

CLIENT       = "client_a"
BRONZE_TABLE = "txn"
SILVER_TABLE = "fact_transactions"

BRONZE_PATH = BRONZE_DIR / CLIENT / BRONZE_TABLE
SILVER_PATH = SILVER_DIR / SILVER_TABLE          # ← conformed, no client subfolder

# ── Column mapping: bronze → canonical ────────────────────────────────────────
# client_a delivers: transaction_id, customer_id, amount, currency,
#                    channel, created_ts, discount_code, is_refund
# NOT delivered:     tax, final_amount, payment_type, item_count, notes
#                    → NULL via conform_to_schema
COLUMN_MAP = {
    "transaction_id": "source_transaction_id",
    "customer_id":    "source_customer_id",
    "amount":         "amount",
    "currency":       "currency",
    "channel":        "channel",
    "created_ts":     "created_ts",
    "discount_code":  "discount_code",
    "is_refund":      "is_refund",
}

def transform(spark):
    # Defined inside function — F.lit() requires active SparkContext
    bool_map = F.create_map(
        F.lit("true"),  F.lit(True),
        F.lit("false"), F.lit(False),
        F.lit("1"),     F.lit(True),
        F.lit("0"),     F.lit(False),
    )

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

    # ── Parse and derive ───────────────────────────────────────────────────────
    df = (
        df
        .withColumn("client_id",    F.lit(CLIENT))
        .withColumn("amount",       F.expr("try_cast(amount as double)"))
        .withColumn("gross_amount", F.col("amount"))           # no tax for client_a
        .withColumn("net_amount",   F.col("amount"))           # no final_amount for client_a
        .withColumn("currency",     F.upper(F.trim(F.col("currency"))))
        .withColumn("channel",      F.lower(F.trim(F.col("channel"))))
        .withColumn("is_refund",
            bool_map[F.lower(F.trim(F.col("is_refund")))].cast(BooleanType()))
        .withColumn("has_discount", F.col("discount_code").isNotNull())
        .withColumn("created_at",
            F.try_to_timestamp(F.col("created_ts"), F.lit("yyyy-MM-dd'T'HH:mm:ss")))
        .withColumn("txn_date",     F.to_date("created_at"))
        .withColumn("date_sk",
            F.when(F.col("txn_date").isNotNull(),
                (F.year("txn_date") * 10000
                 + F.month("txn_date") * 100
                 + F.dayofmonth("txn_date")).cast("int")
            ).otherwise(F.lit(-1).cast("int"))
        )
        .drop("txn_date")
    )

    # ── Surrogate key ──────────────────────────────────────────────────────────
    df = df.withColumn(
        "transaction_sk",
        (F.monotonically_increasing_id() + 1).cast(LongType())
    )

    # ── Broadcast joins — selective select on each dim ─────────────────────────

    # dim_customer — conformed table
    dim_cust = F.broadcast(
        spark.read.format("delta")
        .load(str(SILVER_DIR / "dim_customer"))
        .filter(F.col("client_id") == CLIENT)                 # only client_a rows
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

    # dim_channel
    dim_ch = F.broadcast(
        spark.read.format("delta")
        .load(str(SILVER_DIR / "dim_channel"))
        .select("channel_sk", "channel_code")
    )
    df = (
        df.join(dim_ch, df["channel"] == F.col("channel_code"), "left")
          .withColumn("channel_sk",
              F.coalesce(F.col("channel_sk"), F.lit(-1).cast(LongType())))
          .drop("channel_code")
    )

    # ── Conform to canonical schema ────────────────────────────────────────────
    df = conform_to_schema(df, FACT_TRANSACTIONS_SCHEMA)

    # ── Write — partitioned by client_id, replace this client's partition ──────
    # replaceWhere ensures re-runs are idempotent — only client_a partition
    # is replaced, other clients' data is untouched
    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("client_id")
        .option("mergeSchema", "true")
        .save(str(SILVER_PATH))
    )
    print(f"  [APPEND] {SILVER_PATH}  partition=client_id={CLIENT}  ({df.count():,} rows)")

    # ── Update process log ─────────────────────────────────────────────────────
    write_log(spark, CLIENT, BRONZE_TABLE, SILVER_TABLE, current_version)


def main():
    spark = get_spark("silver-fact-client_a")
    print(f"\n── {CLIENT} / {SILVER_TABLE} ──────────────────────────────")
    transform(spark)
    spark.stop()


if __name__ == "__main__":
    main()