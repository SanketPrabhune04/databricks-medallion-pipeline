v<h1 align="center">🛒 Retail Sales Data Pipeline</h1>

<p align="center">
  <b>An end-to-end data engineering project built on Databricks</b><br>
  Raw files ➜ cleaned data ➜ business-ready analytics — fully automated.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=apache&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Unity_Catalog-FF6F00?style=for-the-badge" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/architecture-medallion-blue" />
  <img src="https://img.shields.io/badge/pattern-star_schema-orange" />
  <img src="https://img.shields.io/badge/loading-incremental-green" />
  <img src="https://img.shields.io/badge/dimension-SCD_Type_2-purple" />
</p>

---

## 📖 What is this project? (in plain English)

Imagine a store gets messy raw data files every day — customers, orders, products, regions.
This project is an **automated assembly line** that takes those raw files and turns them into
**clean, organized, ready-to-analyze data** — the kind you'd build a sales dashboard on.

It does this in **three stages**, a design called the **Medallion Architecture**:

| Stage | Nickname | What it does | Real-world analogy |
|:-----:|:---------|:-------------|:-------------------|
| 🥉 **Bronze** | Raw | Stores the data exactly as it arrives | Unloading boxes into the warehouse |
| 🥈 **Silver** | Clean | Fixes types, removes duplicates, drops bad rows | Sorting and cleaning the goods |
| 🥇 **Gold** | Business | Organizes into analytics tables (star schema) | Putting products on the shelves, ready to sell |

Everything runs **automatically, in the right order, on a schedule** — like a real data pipeline in a company.

---

## 🗺️ How the data flows

```mermaid
flowchart LR
    S[("📁 Source<br/>Parquet files")] --> B

    subgraph Pipeline
      B["🥉 Bronze<br/>raw + audit columns"] --> SI["🥈 Silver<br/>clean · dedup · upsert"]
      SI --> DQ{"✅ Data Quality<br/>gate"}
      DQ -->|pass| G["🥇 Gold<br/>star schema"]
      DQ -.->|fail| X["🛑 Stop pipeline"]
    end

    G --> SV["📊 Serving<br/>SQL · Dashboards · BI"]

    style B fill:#cd7f32,color:#fff
    style SI fill:#9ca3af,color:#fff
    style G fill:#d4af37,color:#000
    style DQ fill:#3b82f6,color:#fff
    style X fill:#ef4444,color:#fff
```

> 💡 **Key idea:** the Data Quality step is a *gate*. If the data is bad, the pipeline **stops**
> and never lets bad data reach the Gold (business) layer.

---

## ⭐ The Gold layer: a Star Schema

The Gold layer is organized as a **star schema** — one central **fact** table (the events)
surrounded by **dimension** tables (the descriptive context).

```mermaid
flowchart TD
    DC["👤 dim_customer<br/>(SCD Type 2)"] --> F
    DP["📦 dim_product"] --> F
    F["⭐ fact_orders<br/>quantity · amount<br/>+ surrogate keys"]
    DD["📅 dim_date"] --> F
    DR["🌍 dim_region"] --> F

    style F fill:#d4af37,color:#000
    style DC fill:#a78bfa,color:#000
    style DP fill:#5dcaa5,color:#000
    style DD fill:#f0997b,color:#000
    style DR fill:#85b7eb,color:#000
```

| Table | Type | Description |
|:------|:-----|:------------|
| `fact_orders` | Fact | One row per order — the measurable events (quantity, amount) |
| `dim_customer` | Dimension (SCD2) | Who bought — **keeps full history** when a customer changes |
| `dim_product` | Dimension | What was bought — category, brand, price |
| `dim_date` | Dimension | When — year, quarter, month, day |
| `dim_region` | Dimension | Where — region lookup |

---

## 🛠️ Tech stack

<table>
<tr>
<td align="center">🧱<br><b>Platform</b><br>Databricks</td>
<td align="center">🌊<br><b>Storage</b><br>Delta Lake</td>
<td align="center">🔥<br><b>Engine</b><br>PySpark</td>
<td align="center">🗃️<br><b>Query</b><br>SQL</td>
<td align="center">🔐<br><b>Governance</b><br>Unity Catalog</td>
<td align="center">⚙️<br><b>Orchestration</b><br>Databricks Jobs</td>
</tr>
</table>

---

## 🧩 Concepts demonstrated

These are the core skills a data engineer is expected to know — all implemented here:

| ✅ Concept | What it means | Where it lives |
|:----------|:--------------|:---------------|
| **Medallion architecture** | Bronze → Silver → Gold layering | Whole pipeline |
| **Incremental loading** | Only process *new* data, not everything | Bronze append + Silver `MERGE` |
| **Idempotency** | Safe to re-run — never duplicates data | Control table guard |
| **Deduplication** | Keep only the latest version of each record | `ROW_NUMBER()` in Silver |
| **SCD Type 2** | Track history when a dimension changes | `dim_customer` |
| **Surrogate keys** | System-generated integer keys for dimensions | All dims + fact lookups |
| **Star schema** | Fact + dimensions for fast analytics | Gold layer |
| **Data quality gate** | Block bad data from reaching Gold | `03_data_quality` |
| **Audit / lineage** | Track batch, time, and source of every row | Bronze columns |
| **Orchestration** | Auto-run steps in order, on a schedule | Databricks Job DAG |

---

## ⚙️ The automated pipeline (Job DAG)

The four notebooks are wired into a single **Databricks Job** that runs them in order:

```mermaid
flowchart LR
    A["1️⃣ bronze"] --> B["2️⃣ silver"] --> C["3️⃣ data_quality"] --> D["4️⃣ gold"]
    style A fill:#cd7f32,color:#fff
    style B fill:#9ca3af,color:#fff
    style C fill:#3b82f6,color:#fff
    style D fill:#d4af37,color:#000
```

Each task waits for the one before it. If `data_quality` fails, `gold` never runs. ✅

---

## 🚀 How to run

<details>
<summary><b>Click to expand run instructions</b></summary>

**Prerequisites**
- Databricks workspace with Unity Catalog + serverless compute
- Source Parquet files uploaded to a Unity Catalog volume

**Run the notebooks in order** (or trigger the Job):

| # | Notebook | Does |
|:-:|:---------|:-----|
| 1 | `01_bronze_ingest` | Loads raw data + adds audit columns |
| 2 | `02_silver_transform` | Cleans, types, dedups, incremental MERGE |
| 3 | `03_data_quality` | Runs quality checks (the gate) |
| 4 | `04_gold_build` | Builds the star schema (SCD2 + surrogate keys) |

**Or run the orchestrated Job**: `bronze → silver → data_quality → gold`,
optionally on a daily schedule with failure email alerts.

</details>

---

## 📊 Example analytics

<details>
<summary><b>Revenue by product category</b></summary>

```sql
SELECT p.category, SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units
FROM gold.fact_orders f
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY revenue DESC;
```
</details>

<details>
<summary><b>Revenue trend by month</b></summary>

```sql
SELECT d.year, d.month_name, SUM(f.total_amount) AS revenue
FROM gold.fact_orders f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```
</details>

---

## 🔮 Known limitations & future work

> _Listed on purpose — knowing the gap to production is a senior-engineer habit._

- 📥 **Ingestion** — uses batch file loads; production would use **Auto Loader** (streaming, checkpointed).
- 🔑 **Surrogate keys** — regenerated on full rebuild; production persists them in a durable key table.
- 🕐 **SCD2 fact join** — joins to the *current* customer version; a time-aware model joins to the version active on the order date.
- ⚙️ **Config** — paths/names are hard-coded; production parameterizes per environment.
- 🧪 **CI/CD** — no automated tests yet; would add Git, tests, and Databricks Asset Bundles.
- 📈 **Scale** — validated on ~10K rows; large-scale tuning (partitioning, Z-order, `OPTIMIZE`) pending.

---

## 📁 Project structure

```
retail-sales-pipeline/
├── README.md
├── notebooks/
│   ├── 01_bronze_ingest.py      # 🥉 raw ingestion + audit + control table
│   ├── 02_silver_transform.py   # 🥈 clean, dedup, incremental MERGE
│   ├── 03_data_quality.py       # ✅ quality checks (gate)
│   └── 04_gold_build.py         # 🥇 star schema: SCD2 + surrogate keys + fact
└── docs/
    └── job_dag.png              # 📸 screenshot of the pipeline DAG
```

---

<p align="center">
  <i>Built as an end-to-end data engineering project.</i><br>
  <b>Bronze → Silver → Gold ⭐</b>
</p>
