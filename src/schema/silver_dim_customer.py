"""
src/schema/silver_dim_customer.py
==================================
Canonical schema for silver dim_customer — shared across all clients.

Every client's dim_transform must call conform_to_schema() before writing
so the output Delta table is always identical in structure regardless of
what columns a specific client delivers.

Columns not delivered by a client land as NULL.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, LongType,
    StringType, StructField, StructType,
)

DIM_CUSTOMER_SCHEMA = StructType([
    StructField("customer_sk",          LongType(),    False),  # PK
    StructField("client_id",            StringType(),  False),
    StructField("source_customer_id",   StringType(),  True),
    StructField("first_name",           StringType(),  True),
    StructField("last_name",            StringType(),  True),
    StructField("full_name",            StringType(),  True),
    StructField("email",                StringType(),  True),
    StructField("phone",                StringType(),  True),
    StructField("country",              StringType(),  True),
    StructField("address",              StringType(),  True),
    StructField("zip_code",             StringType(),  True),
    StructField("signup_date",          DateType(),    True),
    StructField("status",               StringType(),  True),
    StructField("loyalty_points",       LongType(),    True),
    StructField("vip_flag",             BooleanType(), True),
    StructField("marketing_opt_in",     BooleanType(), True),
    StructField("age",                  LongType(),    True),
    StructField("age_band",             StringType(),  True),
    StructField("_ingested_at",         StringType(),  True),
])


def conform_to_schema(df: DataFrame, schema: StructType = DIM_CUSTOMER_SCHEMA) -> DataFrame:
    """
    Enforce the canonical schema on a DataFrame:
      1. Add any missing columns as NULL cast to the declared type
      2. Cast existing columns to the declared type
      3. Select in canonical order

    Parameters
    ----------
    df     : client-specific transformed DataFrame
    schema : canonical StructType (defaults to DIM_CUSTOMER_SCHEMA)
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