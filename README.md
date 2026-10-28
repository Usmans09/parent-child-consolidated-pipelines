# FMCG Parent-Child Consolidated Pipeline

A complete Databricks medallion-architecture ETL pipeline for an FMCG (Fast-Moving Consumer Goods) parent-child company scenario. This project ingests raw data from a child company ("Sports Bar") via S3, processes it through bronze → silver → gold layers with extensive data quality transformations, and merges the curated child data into parent company dimension and fact tables using Delta MERGE operations.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Data Model & Schema Layout](#data-model--schema-layout)
4. [Pipeline Stages](#pipeline-stages)
   - [Stage 1: Setup](#stage-1-setup)
   - [Stage 2: Dimension Data Processing](#stage-2-dimension-data-processing)
   - [Stage 3: Fact Data Processing](#stage-3-fact-data-processing)
   - [Stage 4: Dashboard Insights](#stage-4-dashboard-insights)
5. [Data Quality & Transformations](#data-quality--transformations)
6. [Key Design Patterns](#key-design-patterns)
7. [Usage Instructions](#usage-instructions)
8. [Repository Structure](#repository-structure)

---

## Project Overview

This pipeline consolidates a child company's FMCG data (nutrition bars, protein products) into a parent company's unified data model. The child company ("Sports Bar") operates in India and sells through an acquisition channel. Raw CSV files land in an S3 bucket (`s3://usman-sportsbar-dp/`), and the pipeline processes them through a three-layer medallion architecture before merging curated results into parent company tables.

### Business Context

| Attribute | Value |
| --- | --- |
| Industry | FMCG (Nutrition/Protein Bars) |
| Child Company | Sports Bar |
| Market | India |
| Platform | Sports Bar |
| Channel | Acquisition |
| Data Period | January 2024 – December 2025 |
| Source System | Amazon S3 (CSV files) |

---

## Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              S3 Source (CSV Files)                                  │
│                    s3://usman-sportsbar-dp/{data_source}/                             │
│                          /landing/  ──▶  /processed/                                 │
└──────────────────────────┬──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  BRONZE (Raw Landing)                                                               │
│  • Read CSV from S3 with inferred schema                                            │
│  • Append metadata: read_timestamp, file_name, file_size                            │
│  • Write as Delta tables (append mode, CDF enabled)                                 │
│  • Move source files from landing/ → processed/                                     │
└──────────────────────────┬──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  SILVER (Cleaned & Standardized)                                                    │
│  • Deduplication                                                                     │
│  • Data type corrections & standardization                                          │
│  • Typo fixes, title-case normalization                                              │
│  • Business rule enforcement                                                        │
│  • Enrichment via joins (e.g., product_code lookup)                                 │
│  • Write as Delta tables (overwrite or MERGE)                                       │
└──────────────────────────┬──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  GOLD (Curated & Business-Ready)                                                    │
│  • Select business-relevant columns only                                            │
│  • Child company gold tables: sb_dim_* / sb_fact_*                                  │
│  • Parent company gold tables: dim_* / fact_* (via Delta MERGE)                     │
│  • Monthly aggregation for fact data                                               │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Parent-Child Merge Pattern

A distinguishing feature of this pipeline is that the child company ("Sports Bar") maintains its own gold tables (`sb_dim_*`, `sb_fact_*`) which are then merged into the parent company's master gold tables (`dim_*`, `fact_*`) using Delta `MERGE` operations. This allows:

* **Idempotent updates** — re-running a notebook won't create duplicates
* **Schema evolution** — `mergeSchema` enabled on writes
* **Change Data Feed** — `delta.enableChangeDataFeed` enabled on all tables for incremental tracking
* **Audit trail** — each record carries source file metadata (file_name, file_size, read_timestamp)

---

## Data Model & Schema Layout

### Unity Catalog Structure

```
fmcg (Catalog)
├── bronze (Schema) — Raw landing data with metadata
│   ├── customers
│   ├── products
│   ├── gross_price
│   ├── orders
│   └── staging_orders (temp, incremental only)
│
├── silver (Schema) — Cleaned & standardized data
│   ├── customers
│   ├── products
│   ├── gross_price
│   ├── orders
│   └── staging_orders (temp, incremental only)
│
└── gold (Schema) — Curated, business-ready tables
    ├── dim_date              — Date dimension (monthly, 2024-2025)
    ├── dim_customers         — Parent customer dimension (merged)
    ├── dim_products          — Parent product dimension (merged)
    ├── dim_gross_price       — Parent pricing dimension (merged)
    ├── fact_orders           — Parent fact table (monthly, merged)
    ├── sb_dim_customers      — Child customer dimension
    ├── sb_dim_products       — Child product dimension
    ├── sb_dim_gross_price    — Child pricing dimension
    └── sb_fact_orders        — Child fact table (daily grain)
```

### Key Tables

| Table | Layer | Grain | Purpose |
| --- | --- | --- | --- |
| `fmcg.gold.dim_date` | Gold | Monthly | Date dimension with year, quarter, month metadata |
| `fmcg.gold.dim_customers` | Gold | Customer | Parent company master customer dimension |
| `fmcg.gold.dim_products` | Gold | Product | Parent company master product dimension |
| `fmcg.gold.dim_gross_price` | Gold | Product × Year | Parent company pricing dimension |
| `fmcg.gold.fact_orders` | Gold | Monthly × Product × Customer | Parent company fact table |
| `fmcg.gold.sb_dim_customers` | Gold | Customer | Child company curated customers |
| `fmcg.gold.sb_dim_products` | Gold | Product | Child company curated products |
| `fmcg.gold.sb_dim_gross_price` | Gold | Product × Month | Child company curated pricing |
| `fmcg.gold.sb_fact_orders` | Gold | Daily × Product × Customer | Child company fact table |

---

## Pipeline Stages

### Stage 1: Setup

The setup stage creates the foundational infrastructure: the Unity Catalog, schemas, date dimension, and shared utility constants.

#### 1.1 `1_setup_catalog` (SQL Notebook)

| Step | Description |
| --- | --- |
| 1 | Creates the `fmcg` catalog |
| 2 | Creates three schemas: `bronze`, `silver`, and `gold` |

#### 1.2 `dim_date_table_creation` (Python Notebook)

Generates a monthly date dimension table covering January 2024 through December 2025.

| Column | Type | Description |
| --- | --- | --- |
| `date_key` | int | Year-month as integer (e.g., 202401 for Jan 2024) |
| `year` | int | Calendar year |
| `month_name` | string | Full month name (e.g., "January") |
| `month_short_name` | string | Abbreviated month name (e.g., "Jan") |
| `quarter` | string | Quarter identifier (Q1–Q4) |
| `year_quarter` | string | Combined year-quarter (e.g., "2024-Q1") |

**Output table:** `fmcg.gold.dim_date`

#### 1.3 `utilities` (Python Notebook)

A shared utility notebook imported via `%run` by all downstream notebooks. Defines schema name constants used across the pipeline:

* `bronze_schema = "bronze"`
* `silver_schema = "silver"`
* `gold_schema = "gold"`

---

### Stage 2: Dimension Data Processing

All three dimension notebooks follow a consistent pattern:

1. Import libraries (`pyspark.sql.functions`, `DeltaTable`)
2. Load shared utilities via `%run /Workspace/consolidated_pipeline/1_setup/utilities`
3. Define widgets: `catalog` (default: `fmcg`) and `data_source` (e.g., `customers`)
4. **Bronze:** Read raw CSV from S3 → add metadata columns → write Delta table (append, CDF enabled)
5. **Silver:** Read bronze → apply transformations → write Delta table (overwrite, CDF enabled)
6. **Gold:** Select business columns → write to child gold table (`sb_dim_*`)
7. **Merge:** Delta MERGE child gold into parent gold table (`dim_*`)

#### 2.1 `1_customers_data_processing` (Python, 48 cells)

**Source:** `s3://usman-sportsbar-dp/customers/landing/*.csv`

**Bronze → Silver Transformations:**

| Step | Transformation | Details |
| --- | --- | --- |
| 1 | Deduplication | Drop duplicates on `customer_id` |
| 2 | Trim whitespace | Remove leading/trailing spaces from `customer_name` |
| 3 | City typo correction | Map misspelled cities to canonical names (e.g., `Bengaluruu` → `Bengaluru`, `Hyderabadd` → `Hyderabad`, `NewDheli` → `New Delhi`). Only allows known valid cities; all others set to `null` |
| 4 | Title case fix | Apply `initcap()` to `customer_name` for consistent casing |
| 5 | Missing city handling | Business-confirmed city assignments for 4 customers with null cities (Sprintx Nutrition → New Delhi, Zenathlete Foods → Bengaluru, Primefuel Nutrition → Hyderabad, Recovery Lane → Hyderabad) |
| 6 | Type casting | Convert `customer_id` to string |
| 7 | Business alignment | Add `customer` column as `"Name-City"` format; set static attributes: `market=India`, `platform=Sports Bar`, `channel=Acquisition` |

**Gold table:** `fmcg.gold.sb_dim_customers` — columns: `customer_id`, `customer_name`, `city`, `customer`, `market`, `platform`, `channel`

**Parent merge:** MERGE into `fmcg.gold.dim_customers` on `customer_code`

#### 2.2 `2_products_data_processing` (Python, 34 cells)

**Source:** `s3://usman-sportsbar-dp/products/landing/*.csv`

**Bronze → Silver Transformations:**

| Step | Transformation | Details |
| --- | --- | --- |
| 1 | Deduplication | Drop duplicates on `product_id` |
| 2 | Title case fix | Normalize `category` to title case (e.g., `energy bars` → `Energy Bars`) |
| 3 | Spelling correction | Fix `"Protien"` → `"Protein"` in both `product_name` and `category` |
| 4 | Division mapping | Assign `division` based on category (e.g., Energy Bars → Nutrition Bars, Protein Bars → Nutrition Bars) |
| 5 | Variant extraction | Extract text within parentheses from `product_name` as `variant` |
| 6 | Product code generation | Generate `product_code` as SHA-256 hash of `product_name` for stable unique identifiers |
| 7 | ID cleaning | Keep numeric `product_id` values; non-numeric set to `999999`; cast to string |
| 8 | Column rename | Rename `product_name` → `product` |

**Gold table:** `fmcg.gold.sb_dim_products` — columns: `product_code`, `product_id`, `division`, `category`, `product`, `variant`

**Parent merge:** MERGE into `fmcg.gold.dim_products` on `product_code`

#### 2.3 `3_pricing_data_processing` (Python, 36 cells)

**Source:** `s3://usman-sportsbar-dp/gross_price/landing/*.csv`

**Bronze → Silver Transformations:**

| Step | Transformation | Details |
| --- | --- | --- |
| 1 | Date normalization | Parse `month` field from multiple date formats (`yyyy-MM-dd`, `dd-MM-yyyy`) using `try_to_date` with `coalesce` |
| 2 | Price validation | Validate `gross_price`: convert to double, fix negative values to positive, non-numeric values to `0` using regex check `^-?\d+(\.\d+)?$` |
| 3 | Product enrichment | Join with `fmcg.silver.products` on `product_id` to obtain `product_code` (inner join) |

**Gold table:** `fmcg.gold.sb_dim_gross_price` — columns: `product_code`, `month`, `gross_price`

**Parent merge:** Extract latest non-zero price per product per year using `row_number()` window function → rename to `price_inr` and `year` (string) → MERGE into `fmcg.gold.dim_gross_price` on `product_code`

---

### Stage 3: Fact Data Processing

The fact processing stage handles order data with two notebooks: one for the initial full load and one for subsequent incremental loads. Both follow a similar transformation pipeline but differ in their merge strategy and staging approach.

#### Shared Transformations (Both Notebooks)

| Step | Transformation | Details |
| --- | --- | --- |
| 1 | Null filter | Remove rows where `order_qty` is null |
| 2 | Customer ID cleaning | Keep numeric `customer_id`; non-numeric set to `999999`; cast to string |
| 3 | Date cleaning | Remove weekday prefix from `order_placement_date` using regex `^[A-Za-z]+,\s*` |
| 4 | Date parsing | Parse from multiple formats (`yyyy/MM/dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, `MMMM dd, yyyy`) using `coalesce` with `try_to_date` |
| 5 | Deduplication | Drop duplicates on `order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty` |
| 6 | Type casting | Cast `product_id` to string |
| 7 | Product enrichment | Inner join with `fmcg.silver.products` on `product_id` to obtain `product_code` |

#### 3.1 `1_full_load_fact` (Python, 30 cells) — Initial Full Load

**Source:** `s3://usman-sportsbar-dp/orders/landing/*.csv`

**Pipeline Flow:**

| Phase | Action |
| --- | --- |
| Bronze | Read all CSV files from landing → append to `fmcg.bronze.orders` (append mode, CDF enabled) → move files from `landing/` to `processed/` |
| Silver | Read from `fmcg.bronze.orders` → apply shared transformations → join with products for `product_code` → **if silver table doesn't exist:** overwrite-create; **if exists:** Delta MERGE (upsert) on `order_placement_date`, `order_id`, `product_code`, `customer_id` |
| Gold (child) | Select `order_id`, `date`, `customer_code`, `product_code`, `product_id`, `sold_quantity` from silver → **if gold table doesn't exist:** overwrite-create; **if exists:** Delta MERGE on `date`, `order_id`, `product_code`, `customer_code` |
| Gold (parent merge) | Read ALL child gold data → truncate to monthly grain (`F.trunc("date", "MM")`) → group by `month_start`, `product_code`, `customer_code` → sum `sold_quantity` → MERGE into `fmcg.gold.fact_orders` on `date`, `product_code`, `customer_code` |

#### 3.2 `2_incremental_load_fact` (Python, 39 cells) — Incremental Load

**Source:** `s3://usman-sportsbar-dp/orders/landing/*.csv` (new files only)

**Pipeline Flow:**

| Phase | Action |
| --- | --- |
| Bronze | Read NEW CSV files from landing → append to `fmcg.bronze.orders` → **also** write to staging table `fmcg.bronze.staging_orders` (overwrite mode) → move files from `landing/` to `processed/` |
| Silver | Read from `staging_orders` (not bronze main table) → apply shared transformations → join with products → write to `fmcg.silver.staging_orders` (overwrite) |
| Gold (child) | Select business columns from staging silver → **if gold table doesn't exist:** overwrite-create; **if exists:** Delta MERGE into `fmcg.gold.sb_fact_orders` |
| Gold (parent merge — incremental) | 1. Extract distinct affected months from incremental data (`F.trunc("date", "MM")`) → 2. Pull existing child gold data for those months only → 3. Re-aggregate at monthly level (sum `sold_quantity` by month, product, customer) → 4. MERGE recalculated monthly data into `fmcg.gold.fact_orders` |
| Cleanup | Drop staging tables: `fmcg.bronze.staging_orders` and `fmcg.silver.staging_orders` |

**Key difference from full load:** The incremental notebook only re-aggregates the affected months rather than the entire dataset, making it significantly more efficient for ongoing operations.

---

### Stage 4: Dashboard Insights

The `4_Dashboard_Insights/` folder contains five dashboard screenshots showcasing analytics visualizations built from the Gold-layer tables. These screenshots represent the BI outputs of the pipeline, highlighting insights derived from the consolidated parent-child data model.

### Dashboard 1

![Dashboard 1](<4_Dashboard_Insights/Screenshot 2026-10-28 135604.png>)

**Type:** Data table dashboard with summary chart

**Description:** This dashboard displays detailed records from the Gold layer alongside a summary chart.

**Likely data source:** `fmcg.gold.fact_orders` or `fmcg.gold.sb_fact_orders`.

---

### Dashboard 2

![Dashboard 2](<4_Dashboard_Insights/Screenshot 2026-10-28 135636.png>)

**Type:** Multi-widget dashboard with bar charts and KPI indicators

**Description:** This dashboard presents multiple visualizations for analyzing sales performance, products, customers, and business metrics.

**Likely data source:** `fmcg.gold.fact_orders` aggregated by product or customer dimensions.

---

### Dashboard 3

![Dashboard 3](<4_Dashboard_Insights/Screenshot 2026-10-28 135654.png>)

**Type:** Dual-series bar chart

**Description:** This dashboard compares two data series to help analyze sales quantities, revenue, or performance across categories or time periods.

**Likely data source:** Aggregated data from `fmcg.gold.fact_orders`.

---

### Dashboard 4

![Dashboard 4](<4_Dashboard_Insights/Screenshot 2026-10-28 135711.png>)

**Type:** Multi-widget dashboard with tables and charts

**Description:** This dashboard combines tabular information and chart visualizations to support analysis of products, customers, and orders.

**Likely data source:** `fmcg.gold.dim_products`, `fmcg.gold.dim_customers`, and `fmcg.gold.fact_orders`.

---

### Dashboard 5

![Dashboard 5](<4_Dashboard_Insights/Screenshot 2026-10-28 135737.png>)

**Type:** Multi-bar-chart dashboard

**Description:** This dashboard presents multiple chart visualizations for exploring order volumes, sales quantities, and category-level distributions.

**Likely data source:** Aggregated data from `fmcg.gold.fact_orders`, potentially grouped by product or customer.

---

### Dashboard Summary

These dashboards represent the Business Intelligence (BI) layer of the project, transforming Gold-layer data into visual reports that help users explore transactional data, compare performance, and identify business trends.

### Dashboard Summary

| Screenshot | Type | Key Visual Elements | Likely Insight |
| --- | --- | --- | --- |
| 135604 | Table + summary chart | Data grid with colored bar summary | Detailed transactional records with aggregated summary |
| 135636 | Multi-widget bar charts | Red and blue bars across widgets | Comparative metrics with highlighted alerts |
| 135654 | Dual-series bar chart | Blue + orange bars, left-aligned | Year-over-year or category comparison |
| 135711 | Table + small charts | Data grid with ranking widgets | Multi-dimensional summary with top-N rankings |
| 135737 | Multi-bar-chart dashboard | Dense blue bars + table | Sales volume distribution across dimensions |

---

## Data Quality & Transformations

### Summary of All Data Quality Measures

| Category | Measures Applied |
| --- | --- |
| **Deduplication** | Customer-level (customer_id), product-level (product_id), order-level (composite key of order_id, date, customer_id, product_id, order_qty) |
| **Text Standardization** | Whitespace trimming, `initcap()` title casing, spelling corrections (`Protien` → `Protein`) |
| **Categorical Corrections** | City typo mapping (7 misspellings → 3 canonical cities), category normalization |
| **Missing Value Handling** | Business-confirmed city assignments for 4 customers with null cities |
| **Data Type Enforcement** | String casting for IDs, date parsing from 4+ formats, numeric validation for prices |
| **Value Validation** | Negative price correction, non-numeric price → 0, non-numeric customer/product ID → 999999 |
| **Business Rule Alignment** | Customer naming convention ("Name-City"), static market/platform/channel attributes, division mapping from categories |
| **Surrogate Keys** | SHA-256 hash-based `product_code` for stable cross-system joins |
| **Change Data Feed** | Enabled on all Delta tables for CDC tracking |
| **Idempotent Writes** | Delta MERGE (upsert) pattern on silver and gold layers |
| **File Lifecycle** | Source files moved from `landing/` → `processed/` after ingestion to prevent reprocessing |

### Transformation Flow by Layer

```
BRONZE (Raw + Metadata)          SILVER (Cleaned)                    GOLD (Curated)
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│ • CSV from S3         │    │ • Deduplication      │    │ • Business columns   │
│ • read_timestamp      │──▶│ • Text fixes         │──▶│   only               │
│ • file_name           │    │ • Typo correction    │    │ • Parent merge via   │
│ • file_size           │    │ • Type casting       │    │   Delta MERGE        │
│ • CDF enabled         │    │ • Date parsing       │    │ • Monthly aggregation│
│ • Append mode         │    │ • Joins              │    │   (fact data)         │
└──────────────────────┘    └──────────────────────┘    └──────────────────────┘
```

---

## Key Design Patterns

### 1. Widget Parameterization
All notebooks use `dbutils.widgets` for runtime parameters:
* `catalog` — Unity Catalog name (default: `fmcg`)
* `data_source` — Data source identifier (e.g., `customers`, `products`, `orders`)

This allows notebooks to be reused across different data sources or catalogs without code changes.

### 2. Shared Utilities via `%run`
The `utilities` notebook is imported in every processing notebook via:
```
%run /Workspace/consolidated_pipeline/1_setup/utilities
```
This ensures consistent schema name references across the pipeline.

### 3. Delta MERGE for Idempotent Upserts
Both silver and gold layers use Delta `MERGE` operations:
* **Silver:** MERGE on composite business keys to handle re-runs without duplicates
* **Gold (child):** MERGE on order/date/product/customer keys
* **Gold (parent):** MERGE child gold data into parent master tables

### 4. Conditional Table Creation
Silver and gold layers check if the target table exists before writing:
* **If table doesn't exist:** `mode("overwrite")` to create fresh
* **If table exists:** Delta MERGE to upsert

### 5. Incremental Processing with Staging
The incremental fact notebook uses a staging pattern:
* Staging tables isolate new data for processing
* Only affected months are re-aggregated for the parent merge
* Staging tables are dropped after processing (cleanup)

### 6. File Lifecycle Management
Source CSV files are moved from `landing/` to `processed/` after successful bronze ingestion, preventing duplicate processing on subsequent runs.

---

## Usage Instructions

### Prerequisites

* Databricks workspace with Unity Catalog enabled
* S3 bucket `s3://usman-sportsbar-dp/` accessible with the following subdirectories:
  * `customers/landing/` — Customer CSV files
  * `products/landing/` — Product CSV files
  * `gross_price/landing/` — Pricing CSV files
  * `orders/landing/` — Order CSV files
* Each data source folder has `landing/` and `processed/` subdirectories

### Execution Order

The notebooks must be run in the following sequence:

| Order | Notebook | Purpose |
| --- | --- | --- |
| 1 | `1_setup/1_setup_catalog` | Create catalog and schemas (one-time) |
| 2 | `1_setup/dim_date_table_creation` | Generate date dimension (one-time) |
| 3 | `1_setup/utilities` | Reference only — imported by other notebooks via `%run` |
| 4 | `2_dimension_data_processing/1_customers_data_processing` | Process customer dimension |
| 5 | `2_dimension_data_processing/2_products_data_processing` | Process product dimension |
| 6 | `2_dimension_data_processing/3_pricing_data_processing` | Process pricing dimension |
| 7 | `3_fact_data_processing/1_full_load_fact` | Initial full load of order facts |
| 8 | `3_fact_data_processing/2_incremental_load_fact` | Subsequent incremental loads (repeat as needed) |

### Running Incremental Loads

After the initial full load, use `2_incremental_load_fact` for ongoing data ingestion:
1. Place new CSV files in `s3://usman-sportsbar-dp/orders/landing/`
2. Run the notebook with default widget parameters (`catalog=fmcg`, `data_source=orders`)
3. The notebook will process only new files, upsert silver/gold tables, re-aggregate affected months, and clean up staging tables

### Widget Parameters

| Widget | Default | Description |
| --- | --- | --- |
| `catalog` | `fmcg` | Unity Catalog name |
| `data_source` | `customers` / `products` / `gross_price` / `orders` | S3 subfolder and table name suffix |

---

## Repository Structure

```
parent-child-consolidated-pipelines/
├── README.md                              ← This file
│
├── 1_setup/
│   ├── 1_setup_catalog                     ← SQL: Create fmcg catalog & schemas
│   ├── dim_date_table_creation              ← Python: Generate dim_date (2024-2025)
│   └── utilities                           ← Python: Schema name constants
│
├── 2_dimension_data_processing/
│   ├── 1_customers_data_processing          ← Python: Customer dimension ETL
│   ├── 2_products_data_processing           ← Python: Product dimension ETL
│   └── 3_pricing_data_processing            ← Python: Pricing dimension ETL
│
├── 3_fact_data_processing/
│   ├── 1_full_load_fact                     ← Python: Initial full load of orders
│   └── 2_incremental_load_fact              ← Python: Incremental order loading
│
└── 4_Dashboard_Insights/
    ├── Screenshot 2026-10-28 135604.png     ← Dashboard screenshot 1
    ├── Screenshot 2026-10-28 135636.png     ← Dashboard screenshot 2
    ├── Screenshot 2026-10-28 135654.png     ← Dashboard screenshot 3
    ├── Screenshot 2026-10-28 135711.png     ← Dashboard screenshot 4
    └── Screenshot 2026-10-28 135737.png     ← Dashboard screenshot 5
```

---

## Technologies Used

| Technology | Purpose |
| --- | --- |
| Databricks | Compute platform and notebook environment |
| Unity Catalog | Governance, catalog (`fmcg`), and schema management |
| Delta Lake | Storage format with CDF, MERGE, and schema evolution |
| PySpark | Data processing and transformations |
| Amazon S3 | External data source (CSV files) |
| dbutils.widgets | Runtime parameterization |
