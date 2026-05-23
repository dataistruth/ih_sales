# (Interwell Health )- ih_sales — Multi-Client Sales Data Pipeline

A production-grade PySpark pipeline that ingests heterogeneous flat-file deliveries
from multiple external clients, normalises them through a layered Lakehouse architecture
(Bronze → Silver → Gold), and produces analytics-ready Delta tables.

---

## Project Structure

```
ih_sales/
├── config/
│   ├── dir_config.yaml                        # centralised path config — edit this to migrate
│   ├── client_a_bronze_resgistry.json          # client_a file registry
│   ├── client_b_schema_registry.json           # client_b file registry
│   └── client_c_schema_registry.json           # client_c file registry
│
├── data/                                       # gitignored — created at runtime
│   ├── raw_landing/                            # drop client files here
│   │   ├── client_a/
│   │   ├── client_b/
│   │   └── client_c/
│   ├── bronze/                                 # Delta — raw landed data
│   ├── silver/                                 # Delta — normalised star schema
│   ├── gold/                                   # Delta — business summary tables
│   ├── log/
│   │   ├── silver_file_process_log/            # tracks bronze → silver versions
│   │   └── gold_file_process_log/              # tracks silver → gold versions
│   └── checkpoints/bronze/                     # Spark Structured Streaming checkpoints
│
├── src/
│   ├── bronze/
│   │   └── ingest_to_bronze.py                 # streaming ingestion — raw_landing → bronze
│   ├── common/
│   │   ├── spark_session.py                    # shared SparkSession with Delta config
│   │   ├── config.py                           # reads dir_config.yaml, exposes path constants
│   │   ├── bronze_df_utils.py                  # add_audit_columns()
│   │   ├── delta_utils.py                      # merge_dim() — SCD Type 1 merge
│   │   ├── silver_process_log.py               # bronze version tracking for silver
│   │   ├── gold_process_log.py                 # silver version tracking for gold
│   │   └── tranform.setup_type_0_dims.py       # one-time: dim_date, dim_channel, dim_currency
│   ├── schema/
│   │   ├── silver_dim_customer.py              # canonical dim_customer StructType
│   │   └── silver_fact_transactions.py         # canonical fact_transactions StructType
│   ├── silver/
│   │   ├── client_a/
│   │   │   ├── transform.dim.py                # bronze → dim_customer (client_a)
│   │   │   └── transform.fact.py               # bronze → fact_transactions (client_a)
│   │   ├── client_b/
│   │   │   ├── transform.dim.py                # bronze → dim_customer (client_b)
│   │   │   └── transform.fact.py               # bronze → fact_transactions (client_b)
│   │   └── client_c/
│   │       ├── transform.dim.py                # bronze → dim_customer (client_c)
│   │       └── transform.fact.py               # bronze → fact_transactions (client_c)
│   ├── gold/
│   │   └── transform.build_gold.py             # registers temp views, executes SQL, writes gold
│   └── sql/gold/
│       ├── gold_sales_summary.sql              # monthly revenue per client/currency
│       └── gold_customer_summary.sql           # customer lifetime value
│
├── tests/                                      # test stubs — bronze, silver, gold
├── pyproject.toml                              # Poetry dependencies
├── .gitignore
└── README.md
```

---

## Prerequisites

- Python 3.12
- Java 11+ (required by Spark)
- Poetry

```bash
# Install Java (macOS)
brew install openjdk@11

# Install dependencies
poetry install
```

Dependencies (from `pyproject.toml`):

| Package | Version | Purpose |
|---|---|---|
| `pyspark` | 4.1.1 | Distributed compute engine |
| `delta-spark` | 4.1.0 | Open-source Delta Lake |
| `pyyaml` | ^6.0.3 | Config file parsing |
| `faker` | ^40.12.0 | Test data generation |
| `pytest` | ^8.0.0 | Testing |

---

## Configuration

Edit `config/dir_config.yaml` — **only one line needs to change to migrate to a new machine**:

```yaml
project:
  root_dir: /Users/yourname/path/to/ih_sales   # ← change this only

paths:
  raw_landing: data/raw_landing
  bronze:      data/bronze
  silver:      data/silver
  gold:        data/gold
  log:         data/log
  checkpoints: data/checkpoints
  config:      config
```

All path constants (`BRONZE_DIR`, `SILVER_DIR`, `GOLD_DIR` etc.) are imported from
`src/common/config.py` which reads this file — no hardcoded paths anywhere in the codebase.

---

## How to Run End-to-End

### Step 0 — Drop input files

```
data/raw_landing/
  client_a/   customers_2024_01.txt, transactions_2024_01.txt
  client_b/   customer_dump.txt, txn_history.txt
  client_c/   customers.txt, transactions.txt
```

### Step 1 — Bronze ingestion

Reads client JSON registries, streams files from `raw_landing/` to `bronze/` as Delta.

```bash
cd src
python bronze/ingest_to_bronze.py
```

### Step 2 — Setup Type 0 dimensions (run once)

Builds static shared dimensions: `dim_date`, `dim_channel`, `dim_currency`.

```bash
cd src
python common/tranform.setup_type_0_dims.py
```

### Step 3 — Silver transforms (dim before fact)

Each client's `transform.dim.py` must run before `transform.fact.py` since fact joins against dim.

```bash
cd src

# dims — all merge into conformed silver/dim_customer
python silver/client_a/transform.dim.py
python silver/client_b/transform.dim.py
python silver/client_c/transform.dim.py

# facts — all append to conformed silver/fact_transactions partitioned by client_id
python silver/client_a/transform.fact.py
python silver/client_b/transform.fact.py
python silver/client_c/transform.fact.py
```

### Step 4 — Gold build

```bash
cd src
python gold/transform.build_gold.py
```

### Query results

```bash
# Open PySpark shell from project root
pyspark \
  --packages io.delta:delta-spark_2.13:4.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Query any layer
spark.read.format("delta").load("data/gold/gold_sales_summary").show(10, truncate=False)
spark.read.format("delta").load("data/silver/fact_transactions").show(10, truncate=False)
spark.read.format("delta").load("data/silver/dim_customer").show(10, truncate=False)
```

---

## Architecture — Layered Lakehouse

```
raw_landing/     flat files (.txt — CSV, TSV, pipe-delimited)
     ↓
  BRONZE          faithful landing zone — no transforms, all strings
     ↓
  SILVER          normalised star schema — typed, conformed, merged
     ↓
  GOLD            business summaries — spark.sql() over temp views
```

### Bronze — Kappa-Style Streaming Ingestion

The bronze layer uses **Spark Structured Streaming** with `availableNow=True` trigger,
giving it the operational simplicity of batch (runs on demand, exits cleanly) while
retaining the checkpoint tracking of a streaming system.

- **Wildcard file patterns** (`customers_*.txt`) mean new monthly drops are picked up
  automatically without any config changes
- **Checkpoints** at `data/checkpoints/bronze/<client>/<table>/` track exactly which
  files have been processed — re-running never duplicates data
- **`PERMISSIVE` mode** with `_corrupt_record` ensures malformed rows are flagged with
  `_is_corrupt=true` and preserved rather than silently dropped
- **`mergeSchema=true`** on write means new columns added by a client are accepted
  automatically without pipeline failures
- Everything lands as **`StringType`** — no casting at bronze, preserving data
  exactly as delivered

### Silver — Delta Version-Based Incremental Processing

Rather than reprocessing all bronze data on every run, the silver layer uses
**Delta Lake's version tracking** to read only what is new:

```python
last_version    = get_last_version(spark, client, bronze_table, silver_table)
current_version = get_current_bronze_version(spark, bronze_path)

if current_version <= last_version:
    return  # skip — nothing new

df = spark.read.format("delta")
    .option("versionAsOf", current_version)
    .load(bronze_path)
```

This eliminates redundant compute — silver only runs when bronze has actually changed.
The same pattern applies at gold: `gold_process_log` tracks which silver version was
last processed so gold rebuilds only when silver changes.

### Silver — SCD Type 1 Merge via Open-Source Delta Lake

Dimension tables use **Delta Lake's `MERGE`** operation for Type 1 slowly changing
dimension updates:

```
WHEN MATCHED     → UPDATE all columns except customer_sk  (preserve SK)
WHEN NOT MATCHED → INSERT with new monotonically_increasing_id SK
```

We chose **open-source Delta Lake** (`delta-spark 4.1.0`) over proprietary alternatives
because it runs entirely locally without cloud dependencies, while providing full ACID
transactions, schema evolution, time travel, and native Spark integration.

### Silver — Conformed Dimensions & Partitioned Fact

- **`silver/dim_customer`** — one conformed table across all clients, keyed by
  `(client_id, source_customer_id)`. Enables cross-client customer analysis.
- **`silver/fact_transactions`** — one conformed fact table partitioned by `client_id`.
  Each client's transform appends only to its own partition — re-runs are safe and
  partition pruning keeps queries fast.

### Canonical Schema Enforcement

Every client's transform calls `conform_to_schema()` before writing — defined once in `src/schema/`:

```python
from schema.silver_dim_customer import DIM_CUSTOMER_SCHEMA, conform_to_schema
df = conform_to_schema(df, DIM_CUSTOMER_SCHEMA)
```

This adds any column the client didn't deliver as `NULL` with the correct type,
casts existing columns to canonical types, and selects in canonical order —
guaranteeing identical schemas regardless of what each client provides.

### Gold — Spark SQL over Temp Views

Gold tables are defined as pure SQL files in `src/sql/gold/`. Silver Delta tables
are registered as Spark temp views and SQL is executed via `spark.sql()`:

```python
spark.read.format("delta").load(silver_path).createOrReplaceTempView("fact_transactions")
df = spark.sql(open("sql/gold/gold_sales_summary.sql").read())
```

Adding a new gold table requires only a new `.sql` file — no Python code changes.

---

## Key Design Decisions & Tradeoffs

| Decision | Rationale | Tradeoff |
|---|---|---|
| **Client JSON registries** | New client = one JSON file, zero code changes | Requires a code deploy to change |
| **Open-source Delta Lake** | ACID, merge, schema evolution, time travel — locally | Requires Java; heavier than plain Parquet |
| **Kappa streaming for bronze** | Checkpoint tracks files; wildcards; no manual dedup | Requires Spark Streaming setup |
| **`availableNow` trigger** | Runs like batch, exits cleanly, retains checkpoint tracking | Not truly real-time |
| **Delta version tracking** | Only process new data; skips unchanged tables | One Delta history query per run per table |
| **Conformed dim + partitioned fact** | Cross-client queries; partition pruning | `client_id` must be on every row |
| **`conform_to_schema()`** | Identical schemas regardless of client delivery | NULL-fills may hide data quality issues |
| **SCD Type 1** | Simple, fast, no history overhead | Cannot answer historical "what was this value last month?" questions |
| **`monotonically_increasing_id` for SK** | No shuffle, no window partition warning | Not sequential across partitions; gaps possible |
| **Broadcast joins in silver** | Dims are small; avoids shuffle on the large fact side | Dims must fit in executor memory |
| **SQL files for gold** | Readable by analysts; no Python needed for new tables | Cannot use complex Python UDFs inline |

---

## How to Handle New Clients

Adding `client_d` requires **no code changes to the pipeline** — only config and one new transform:

1. Create `config/client_d_registry.json`:
```json
{
  "client_name": "client_d",
  "files": [
    {
      "file_name": "accounts_*.csv",
      "bronze_table": "customers",
      "separator": ",",
      "is_active": true,
      "is_header": true
    }
  ]
}
```

2. Drop files into `data/raw_landing/client_d/`

3. Create `src/silver/client_d/transform.dim.py` and `transform.fact.py` with the
   client-specific `COLUMN_MAP` — call `conform_to_schema()` to enforce canonical schema

4. Run the pipeline — bronze picks up the new registry automatically

---

## How to Handle New File Formats

| Scenario | How to handle |
|---|---|
| New delimiter (e.g. `;`) | Set `"separator": ";"` in client JSON — Spark CSV handles any single character |
| New column from existing client | `mergeSchema=true` on bronze accepts it; `conform_to_schema` handles it in silver |
| Column removed by client | Delta adds `NULL` for missing columns automatically; no pipeline failure |
| New business entity (e.g. `products`) | Add entry to client JSON with new `bronze_table`; create new silver transform |
| Fixed-width or JSON source files | Add a `format` field to the client registry and a custom reader in `ingest_to_bronze.py` |

---

## Assumptions & Limitations

| Area | Detail |
|---|---|
| **Scale** | Designed for local `local[*]` Spark. Moving to a cluster requires only changing `.master(...)` in `spark_session.py` |
| **Timestamps** | Parsed via `try_to_timestamp`. Timezones assumed UTC. Kept as strings in bronze. |
| **Currency** | Amounts stored as-is per client. No cross-currency conversion. |
| **SCD Type 1 only** | No historical tracking of dimension changes. Updated values overwrite previous ones. |
| **Surrogate keys** | `monotonically_increasing_id` produces unique but non-sequential IDs across partitions. |
| **Gold is full rebuild** | Gold tables are fully overwritten each run. Incremental gold would be needed at larger scale. |
| **No orchestration** | Steps must be run manually in order. Airflow or Databricks Workflows would be the natural next step. |
| **client_c timestamps** | `transactions.txt` from client_c has no timestamp — `created_at` and `date_sk` are NULL/−1. |
| **Config file naming** | `client_a_bronze_resgistry.json` has a typo ("resgistry") — harmless since `*.json` glob picks it up, but worth standardising. |
