# End-to-End Retail Sales Data Pipeline (Medallion Architecture)

An end-to-end batch data engineering pipeline built on **Databricks** and **Unity Catalog**, implementing the medallion architecture (Bronze → Silver → Gold) with incremental loading, slowly changing dimensions, a dimensional star schema, data quality gating, and job orchestration.

The pipeline ingests raw retail data (customers, orders, products, regions), cleans and conforms it, and serves a governed star schema for analytics and BI.

---

## Architecture

```
                          ┌─────────────────────────────────────────────┐
  Source (Parquet)        │              MEDALLION PIPELINE               │
  ┌──────────────┐        │                                               │
  │ customers    │        │   BRONZE        SILVER         GOLD           │
  │ orders       │───────▶│  (raw +        (cleaned,     (star schema:    │
  │ products     │        │   audit)        typed,        facts + dims,   │
  │ regions      │        │                 deduped,      SCD2, keys)      │
  └──────────────┘        │                 upserted)                     │
                          │      │             │             │            │
                          │      ▼             ▼             ▼            │
                          │   Data Quality gate between Silver and Gold   │
                          └─────────────────────────────────────────────┘
                                              │
                                              ▼
                                    Serving: SQL / Dashboards / BI
```

Orchestrated as a Databricks Job (DAG):

```
bronze  →  silver  →  data_quality  →  gold
```

Each stage runs only after its upstream dependency succeeds. The `data_quality` task is a gate: if a hard check fails, `gold` never runs.

---

## Tech stack

| Layer            | Technology                                  |
|------------------|---------------------------------------------|
| Platform         | Databricks (serverless compute)             |
| Governance       | Unity Catalog                               |
| Storage format   | Delta Lake                                  |
| Languages        | SQL, PySpark                                |
| Orchestration    | Databricks Jobs (multi-task DAG)            |
| Source format    | Parquet                                     |

---

## Data model (Gold — star schema)

**Fact table**
- `fact_orders` — one row per order; measures (`quantity`, `total_amount`) + surrogate foreign keys (`customer_key`, `product_key`, `date_key`)

**Dimension tables**
- `dim_customer` — **SCD Type 2** (tracks history via `effective_from`, `effective_to`, `is_current`) with a per-version surrogate key
- `dim_product` — product attributes + `product_key`
- `dim_region` — region lookup + `region_key`
- `dim_date` — calendar dimension with `date_key` (yyyyMMdd), year/quarter/month/day

**Control**
- `control.batch_log` — watermark table tracking loaded batches for idempotent incremental loads

---

## Pipeline stages

### 1. Bronze — raw ingestion
- Loads source Parquet files as-is (no transformation), preserving raw fidelity.
- Adds **audit/lineage columns**: `_batch_id`, `_ingested_at`, `_source_file`.
- Append-only for the fact table (orders), so full history is retained.
- **Incremental loads** are guarded against duplicates via the control table (idempotency).

### 2. Silver — cleaned & conformed
- Casts raw types to proper types (dates, integers, decimals for money).
- Filters invalid rows (null keys, non-positive quantities, negative amounts).
- **Deduplication** using `ROW_NUMBER()` partitioned by business key, ordered by batch then ingest time (latest wins).
- **Incremental upsert (MERGE)** into `silver.orders`: existing orders are updated, new orders inserted — idempotent and re-runnable.

### 3. Data quality — gate
- **Hard checks** (block the pipeline): null keys, negative amounts, duplicate keys.
- **Warn checks** (log, don't block): orphan orders (no matching customer).
- Raises an exception on any hard failure, preventing bad data from reaching Gold.

### 4. Gold — dimensional model
- Builds dimensions with **surrogate keys**.
- Maintains `dim_customer` as **SCD Type 2** via a two-step MERGE (close changed versions, insert new versions with continuing surrogate keys).
- Builds `fact_orders` via **surrogate-key lookups** (joins natural keys to dimensions, stores surrogate keys).
- Builds business aggregates (e.g. `sales_by_category`).

---

## Key data engineering concepts demonstrated

| Concept                    | Where / how                                                        |
|----------------------------|-------------------------------------------------------------------|
| Medallion architecture     | Bronze / Silver / Gold schemas                                    |
| Incremental loading        | Append + MERGE upsert; only new/changed data processed            |
| Idempotency                | Control table guard + `CREATE OR REPLACE` / MERGE semantics       |
| Deduplication              | `ROW_NUMBER()` latest-wins within business key                    |
| Slowly Changing Dimension  | SCD Type 2 on `dim_customer` with effective dating                |
| Surrogate keys             | Generated integer keys; fact references them via lookup           |
| Star schema                | Fact surrounded by conformed dimensions                           |
| Data quality               | Tiered checks (hard-fail vs warn) as a pipeline gate              |
| Audit / lineage            | Batch id, ingest timestamp, source file on every bronze row       |
| Orchestration              | Multi-task Job DAG with dependencies                              |

---

## How to run

**Prerequisites:** a Databricks workspace with Unity Catalog and serverless compute; source Parquet files uploaded to a Unity Catalog volume.

**Notebooks** (run in order, or via the orchestrated Job):
1. `01_bronze_ingest` — schemas, control table, bronze loads
2. `02_silver_transform` — silver MERGE + conformed dimensions
3. `03_data_quality` — quality checks (gate)
4. `04_gold_build` — star schema with SCD2 + surrogate keys

**Orchestrated:** a Databricks Job chains the four notebooks as tasks
(`bronze → silver → data_quality → gold`), each depending on the previous,
with an optional daily schedule and failure notifications.

---

## Example analytics (star-schema queries)

Revenue by product category:
```sql
SELECT p.category, SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units
FROM gold.fact_orders f
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY revenue DESC;
```

Revenue trend by month:
```sql
SELECT d.year, d.month_name, SUM(f.total_amount) AS revenue
FROM gold.fact_orders f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

---

## Known limitations & future work

Documented deliberately — these are the deltas between a strong learning
project and a production system:

- **Ingestion**: uses batch file loads; production would use **Auto Loader**
  (incremental file detection with checkpointing) or streaming.
- **Surrogate keys on rebuild**: keys are stable within incremental runs but a
  full dimension rebuild regenerates them; a production warehouse would persist
  keys in a durable key table.
- **SCD2 fact join**: fact currently joins to the *current* customer version;
  a fully time-aware model would join to the version active on the order date.
- **Config**: paths and table names are hard-coded; production would
  parameterize per environment (dev/test/prod).
- **CI/CD**: no automated tests or deployment bundles yet; would add Git,
  unit/integration tests, and Databricks Asset Bundles.
- **Data quality**: hand-written checks; production would use a framework
  (DLT expectations / Great Expectations) with metrics history.
- **Scale**: validated on ~10K rows; large-scale tuning (partitioning,
  Z-order / liquid clustering, `OPTIMIZE`) not yet applied.

---

## Project structure

```
├── 01_bronze_ingest        # raw ingestion + audit columns + control table
├── 02_silver_transform     # clean, type, dedup, incremental MERGE
├── 03_data_quality         # tiered quality checks (gate)
├── 04_gold_build           # star schema: SCD2 dims + surrogate keys + fact
└── README.md
```
