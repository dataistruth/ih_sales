"""
silver_schemas.py
=================
StructType definitions for every table in the simplified Silver Star Schema.

Tables
------
  fact_transactions    – central fact, one row per transaction
  dim_customer         – who transacted
  dim_date             – when it happened
  dim_transaction_type – how (channel + payment method combined)
  dim_source           – where the data came from (client + currency)
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ─────────────────────────────────────────────────────────────────────────────
# dim_customer
# ─────────────────────────────────────────────────────────────────────────────
DIM_CUSTOMER_SCHEMA = StructType([
    StructField("customer_sk",        LongType(),    False),  # PK  (-1 = Unknown)
    StructField("client_id",          StringType(),  True),
    StructField("source_customer_id", StringType(),  True),
    StructField("first_name",         StringType(),  True),
    StructField("last_name",          StringType(),  True),
    StructField("full_name",          StringType(),  True),   # derived
    StructField("email",              StringType(),  True),
    StructField("phone",              StringType(),  True),
    StructField("country",            StringType(),  True),   # ISO uppercased
    StructField("address",            StringType(),  True),
    StructField("zip_code",           StringType(),  True),
    StructField("signup_date",        DateType(),    True),   # parsed from string
    StructField("status",             StringType(),  True),   # active/pending/inactive
    StructField("loyalty_points",     LongType(),    True),
    StructField("vip_flag",           BooleanType(), True),
    StructField("marketing_opt_in",   BooleanType(), True),
    StructField("age",                LongType(),    True),
    StructField("age_band",           StringType(),  True),   # 18-24, 25-34 …
])

# ─────────────────────────────────────────────────────────────────────────────
# dim_date
# ─────────────────────────────────────────────────────────────────────────────
DIM_DATE_SCHEMA = StructType([
    StructField("date_sk",        IntegerType(), False),  # PK  YYYYMMDD (-1 = Unknown)
    StructField("full_date",      DateType(),    True),
    StructField("year",           IntegerType(), True),
    StructField("quarter",        IntegerType(), True),   # 1–4
    StructField("month",          IntegerType(), True),   # 1–12
    StructField("month_name",     StringType(),  True),   # January …
    StructField("week_of_year",   IntegerType(), True),
    StructField("day_of_month",   IntegerType(), True),
    StructField("day_of_week",    IntegerType(), True),   # 1=Sun … 7=Sat
    StructField("day_name",       StringType(),  True),   # Monday …
    StructField("is_weekend",     BooleanType(), True),
    StructField("is_month_start", BooleanType(), True),
    StructField("is_month_end",   BooleanType(), True),
    StructField("quarter_label",  StringType(),  True),   # Q1 2024
])

# ─────────────────────────────────────────────────────────────────────────────
# dim_transaction_type  (collapsed: dim_channel + dim_payment)
# ─────────────────────────────────────────────────────────────────────────────
DIM_TRANSACTION_TYPE_SCHEMA = StructType([
    StructField("transaction_type_sk",  IntegerType(), False),  # PK  (-1 = Unknown)
    StructField("channel_code",         StringType(),  True),   # web / mobile / store
    StructField("channel_name",         StringType(),  True),   # Web / Mobile / Store
    StructField("channel_type",         StringType(),  True),   # Online / In-Person
    StructField("payment_code",         StringType(),  True),   # card / cash / paypal
    StructField("payment_name",         StringType(),  True),   # Card / Cash / PayPal
    StructField("payment_category",     StringType(),  True),   # Digital / Physical
])

# ─────────────────────────────────────────────────────────────────────────────
# dim_source  (collapsed: dim_client + dim_currency)
# ─────────────────────────────────────────────────────────────────────────────
DIM_SOURCE_SCHEMA = StructType([
    StructField("source_sk",       IntegerType(), False),  # PK  (-1 = Unknown)
    StructField("client_id",       StringType(),  True),   # client_a / client_b / client_c
    StructField("client_name",     StringType(),  True),   # human-readable
    StructField("currency_code",   StringType(),  True),   # USD / GBP / EUR …
    StructField("currency_name",   StringType(),  True),   # US Dollar …
    StructField("currency_region", StringType(),  True),   # North America / Europe …
])

# ─────────────────────────────────────────────────────────────────────────────
# fact_transactions
# ─────────────────────────────────────────────────────────────────────────────
FACT_TRANSACTIONS_SCHEMA = StructType([
    # ── Keys ──────────────────────────────────────────────────────────────
    StructField("transaction_sk",       LongType(),    False),  # PK
    StructField("customer_sk",          LongType(),    True),   # FK → dim_customer
    StructField("date_sk",              IntegerType(), True),   # FK → dim_date
    StructField("transaction_type_sk",  IntegerType(), True),   # FK → dim_transaction_type
    StructField("source_sk",            IntegerType(), True),   # FK → dim_source
    # ── Degenerate dimensions ──────────────────────────────────────────────
    StructField("source_transaction_id", StringType(), True),
    StructField("discount_code",         StringType(), True),
    # ── Measures ──────────────────────────────────────────────────────────
    StructField("amount",               DoubleType(),  True),
    StructField("tax_amount",           DoubleType(),  True),
    StructField("gross_amount",         DoubleType(),  True),   # amount + coalesce(tax,0)
    StructField("net_amount",           DoubleType(),  True),   # final_amount or gross
    StructField("item_count",           LongType(),    True),
    # ── Flags ─────────────────────────────────────────────────────────────
    StructField("is_refund",            BooleanType(), True),
    StructField("has_discount",         BooleanType(), True),
    # ── Audit ─────────────────────────────────────────────────────────────
    StructField("created_ts",           StringType(),  True),   # raw source string
    StructField("_ingested_at",         StringType(),  True),
])


# ── Convenience export ────────────────────────────────────────────────────────
ALL_SCHEMAS = {
    "fact_transactions":   FACT_TRANSACTIONS_SCHEMA,
    "dim_customer":        DIM_CUSTOMER_SCHEMA,
    "dim_date":            DIM_DATE_SCHEMA,
    "dim_transaction_type":DIM_TRANSACTION_TYPE_SCHEMA,
    "dim_source":          DIM_SOURCE_SCHEMA,
}


if __name__ == "__main__":
    for name, schema in ALL_SCHEMAS.items():
        print(f"\n{'='*55}")
        print(f"  {name}  ({len(schema.fields)} fields)")
        print(f"{'='*55}")
        for f in schema.fields:
            nullable = "    " if f.nullable else " NOT"
            print(f"  {f.name:<28} {str(f.dataType):<16} {nullable} NULL")