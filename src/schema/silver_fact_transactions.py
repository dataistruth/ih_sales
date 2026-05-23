"""
src/schema/silver_fact_transactions.py
========================================
Canonical schema for silver fact_transactions — shared across all clients.

Every client's fact_transform must call conform_to_schema() before writing
so the output Delta table is always identical in structure regardless of
what columns a specific client delivers.

Columns not delivered by a client land as NULL.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

FACT_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_sk",        LongType(),      False),  # PK
    StructField("client_id",             StringType(),    False),
    StructField("source_transaction_id", StringType(),    True),
    StructField("customer_sk",           LongType(),      True),   # FK → dim_customer
    StructField("date_sk",               IntegerType(),   True),   # FK → dim_date
    StructField("currency_sk",           LongType(),      True),   # FK → dim_currency
    StructField("channel_sk",            LongType(),      True),   # FK → dim_channel
    StructField("source_customer_id",    StringType(),    True),
    StructField("amount",                DoubleType(),    True),
    StructField("tax",                   DoubleType(),    True),
    StructField("gross_amount",          DoubleType(),    True),
    StructField("net_amount",            DoubleType(),    True),
    StructField("currency",              StringType(),    True),
    StructField("channel",               StringType(),    True),
    StructField("payment_type",          StringType(),    True),
    StructField("discount_code",         StringType(),    True),
    StructField("has_discount",          BooleanType(),   True),
    StructField("is_refund",             BooleanType(),   True),
    StructField("item_count",            LongType(),      True),
    StructField("notes",                 StringType(),    True),
    StructField("created_ts",            StringType(),    True),
    StructField("created_at",            TimestampType(), True),
    StructField("_ingested_at",          StringType(),    True),
])


def conform_to_schema(df: DataFrame, schema: StructType = FACT_TRANSACTIONS_SCHEMA) -> DataFrame:
    """
    Enforce the canonical schema on a DataFrame:
      1. Add any missing columns as NULL cast to the declared type
      2. Cast existing columns to the declared type
      3. Select in canonical order

    Parameters
    ----------
    df     : client-specific transformed DataFrame
    schema : canonical StructType (defaults to FACT_TRANSACTIONS_SCHEMA)
    """
    for field in schema.fields:
        if field.name not in df.columns:
            # Column not delivered by this client — add as NULL
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            # Column exists — cast to canonical type
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    # Select in canonical order only
    return df.select([field.name for field in schema.fields])