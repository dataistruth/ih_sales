"""
src/common/setup_type_0_dims.py
================================
Builds the three static (Type 0) silver dimensions that are shared
across all clients. Run once, or re-run idempotently at any time.

Tables written
--------------
  data/silver/dim_date        – calendar spine 2020-01-01 → 2030-12-31
  data/silver/dim_channel     – web / mobile / store
  data/silver/dim_currency    – ISO currency codes with region

Type 0 = attributes never change once loaded.
SK generated via row_number() over a deterministic orderBy — stable
and sequential from 1. Sentinel row SK = -1 for unresolvable FKs.

Run
---
  cd src
  python common/setup_type_0_dims.py
"""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, LongType,
    StringType, StructField, StructType,
)
from common.spark_session import get_spark
from common.config import SILVER_DIR


# ── Helpers ────────────────────────────────────────────────────────────────────

def write_dim(df, path: Path, name: str):
    path.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))
    print(f"  [OK] {name:<20} → {path}  ({df.count():,} rows)")


def add_sk(df, sk_col: str, order_col: str):
    """
    Generate a sequential surrogate key starting from 1
    using row_number() over a deterministic orderBy.
    Sentinel row (SK = -1) is prepended separately.
    """
    w = Window.orderBy(order_col)
    return df.withColumn(sk_col, F.row_number().over(w))


# ── dim_date ───────────────────────────────────────────────────────────────────

DIM_DATE_SCHEMA = StructType([
    StructField("date_sk",        IntegerType(), False),
    StructField("full_date",      DateType(),    True),
    StructField("year",           IntegerType(), True),
    StructField("quarter",        IntegerType(), True),
    StructField("month",          IntegerType(), True),
    StructField("month_name",     StringType(),  True),
    StructField("week_of_year",   IntegerType(), True),
    StructField("day_of_month",   IntegerType(), True),
    StructField("day_of_week",    IntegerType(), True),
    StructField("day_name",       StringType(),  True),
    StructField("is_weekend",     BooleanType(), True),
    StructField("is_month_start", BooleanType(), True),
    StructField("is_month_end",   BooleanType(), True),
    StructField("quarter_label",  StringType(),  True),
])


def build_dim_date(spark: SparkSession, start: date, end: date) -> None:
    days = []
    d = start
    while d <= end:
        days.append((d,))
        d += timedelta(days=1)

    df = spark.createDataFrame(days, ["full_date"])
    df = df.withColumn("full_date", F.col("full_date").cast(DateType()))

    df = (
        df
        .withColumn("year",         F.year("full_date"))
        .withColumn("quarter",      F.quarter("full_date"))
        .withColumn("month",        F.month("full_date"))
        .withColumn("month_name",   F.date_format("full_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week",  F.dayofweek("full_date"))
        .withColumn("day_name",     F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend",   F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_month_start", F.dayofmonth("full_date") == 1)
        .withColumn("is_month_end",
            F.dayofmonth("full_date") == F.dayofmonth(F.last_day("full_date")))
        .withColumn("quarter_label",
            F.concat(F.lit("Q"), F.quarter("full_date"),
                     F.lit(" "),  F.year("full_date")))
    )

    # SK = YYYYMMDD integer — naturally unique and meaningful for dates
    df = df.withColumn("date_sk",
        (F.year("full_date") * 10000
         + F.month("full_date") * 100
         + F.dayofmonth("full_date")).cast(IntegerType())
    )

    # Sentinel row
    sentinel = spark.createDataFrame(
        [(-1, None, None, None, None, None, None,
          None, None, None, None, None, None, None)],
        DIM_DATE_SCHEMA,
    )

    dim = sentinel.union(df.select([f.name for f in DIM_DATE_SCHEMA.fields]))
    write_dim(dim, SILVER_DIR / "dim_date", "dim_date")


# ── dim_channel ────────────────────────────────────────────────────────────────

DIM_CHANNEL_SCHEMA = StructType([
    StructField("channel_sk",    LongType(),    False),
    StructField("channel_code",  StringType(),  True),
    StructField("channel_name",  StringType(),  True),
    StructField("channel_type",  StringType(),  True),
])

CHANNEL_ROWS = [
    ("web",    "Web",    "Online"),
    ("mobile", "Mobile", "Online"),
    ("store",  "Store",  "In-Person"),
]


def build_dim_channel(spark: SparkSession) -> None:
    df = spark.createDataFrame(CHANNEL_ROWS, ["channel_code", "channel_name", "channel_type"])

    # SK via monotonically_increasing_id — no window, no shuffle warning
    df = df.withColumn("channel_sk", (F.monotonically_increasing_id() + 1))

    sentinel = spark.createDataFrame(
        [(-1, "unknown", "Unknown", "Unknown")], DIM_CHANNEL_SCHEMA
    )

    dim = sentinel.union(df.select([f.name for f in DIM_CHANNEL_SCHEMA.fields]))
    write_dim(dim, SILVER_DIR / "dim_channel", "dim_channel")


# ── dim_currency ───────────────────────────────────────────────────────────────

DIM_CURRENCY_SCHEMA = StructType([
    StructField("currency_sk",     LongType(),    False),
    StructField("currency_code",   StringType(),  True),
    StructField("currency_name",   StringType(),  True),
    StructField("currency_region", StringType(),  True),
])

CURRENCY_ROWS = [
    ("AUD", "Australian Dollar", "Oceania"),
    ("CAD", "Canadian Dollar",   "North America"),
    ("EUR", "Euro",              "Europe"),
    ("GBP", "British Pound",     "Europe"),
    ("USD", "US Dollar",         "North America"),
]


def build_dim_currency(spark: SparkSession) -> None:
    df = spark.createDataFrame(CURRENCY_ROWS, ["currency_code", "currency_name", "currency_region"])

    # SK via monotonically_increasing_id — no window, no shuffle warning
    df = df.withColumn("currency_sk", (F.monotonically_increasing_id() + 1))

    sentinel = spark.createDataFrame(
        [(-1, "unknown", "Unknown", "Unknown")], DIM_CURRENCY_SCHEMA
    )

    dim = sentinel.union(df.select([f.name for f in DIM_CURRENCY_SCHEMA.fields]))
    write_dim(dim, SILVER_DIR / "dim_currency", "dim_currency")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("setup-type0-dims")
    print(f"Silver dir : {SILVER_DIR}\n")

    print("Building Type 0 dimensions …")
    build_dim_date(spark,    date(2020, 1, 1), date(2030, 12, 31))
    build_dim_channel(spark)
    build_dim_currency(spark)

    print("\nType 0 dimensions complete.")
    spark.stop()


if __name__ == "__main__":
    main()