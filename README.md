# 🛒 Sales Data Pipeline — GCP Medallion Architecture

An end-to-end automated sales data pipeline built on Google Cloud Platform, implementing the **Bronze → Silver → Gold** medallion architecture. Raw sales data is ingested, progressively refined through PySpark transformations on Dataproc, and finally loaded into BigQuery for analytics.

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Cloud Composer (Airflow)                      │
│                    DAG: sales_pipeline_bronze_to_bq                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ orchestrates
          ┌────────────────────▼─────────────────────┐
          │                                           │
   ┌──────▼──────┐     ┌──────────────┐     ┌────────▼───────┐
   │   BRONZE    │────▶│    SILVER    │────▶│      GOLD      │
   │  GCS Bucket │     │  GCS Bucket  │     │   GCS Bucket   │
   │  (raw CSV)  │     │  (cleaned)   │     │ (star schema)  │
   └─────────────┘     └──────────────┘     └────────┬───────┘
         │                    │                       │
    Dataproc              Dataproc               GCS Sensors
   PySpark Job           PySpark Job                  │
  bronze_to_silver      silver_to_gold                │
                                               ┌──────▼──────┐
                                               │  BigQuery   │
                                               │ sales_gold  │
                                               │  dataset    │
                                               └─────────────┘
```

### Data Flow

| Step | Tool | Description |
|------|------|-------------|
| 1. Ingest | GCS (Bronze) | Raw sales CSV files land in the bronze bucket |
| 2. Clean | Dataproc PySpark | `bronze_to_silver.py` — deduplication, null handling, type casting |
| 3. Transform | Dataproc PySpark | `silver_to_gold.py` — build star schema, enrich with metrics |
| 4. Sense | GCS Sensor | Airflow waits for gold `part-*.csv` files to appear |
| 5. Load | GCSToBigQuery | Loads all dimension and fact tables into BigQuery |

---

## 🗂️ Project Structure

```
├── dags/
│   └── sales_pipeline_dag.py        # Main Airflow DAG
├── scripts/
│   ├── bronze_to_silver.py          # PySpark: raw → cleaned
│   └── silver_to_gold.py            # PySpark: cleaned → star schema
├── schemas/
│   └── fact_sales_schema.json       # BigQuery schema reference
└── README.md
```

---

## 🏗️ Star Schema (BigQuery — `sales_gold` dataset)

### Fact Table

**`fact_sales`** — 26 columns

| Column | Type | Description |
|--------|------|-------------|
| `customer_sk` | INTEGER | Foreign key → dim_customer |
| `product_sk` | INTEGER | Foreign key → dim_product |
| `date_sk` | INTEGER | Foreign key → dim_date |
| `region_sk` | INTEGER | Foreign key → dim_region |
| `order_id` | STRING | Natural order identifier |
| `customer_id` | STRING | Source customer identifier |
| `product_id` | STRING | Source product identifier |
| `order_date` | DATE | Order transaction date |
| `order_year` | INTEGER | Extracted year |
| `order_month` | INTEGER | Extracted month |
| `status` | STRING | Order status (e.g. completed) |
| `quantity` | INTEGER | Units ordered |
| `unit_price` | FLOAT | Price per unit |
| `total_amount` | FLOAT | Source total |
| `calculated_total` | FLOAT | quantity × unit_price |
| `revenue` | FLOAT | Recognized revenue |
| `discount_amount` | FLOAT | Discount applied |
| `discount_rate` | FLOAT | Discount as a fraction |
| `avg_unit_price` | FLOAT | Average unit price |
| `is_high_value` | BOOLEAN | High-value order flag |
| `order_size_bucket` | STRING | small / medium / large |
| `revenue_band` | STRING | low / medium / high |
| `is_date_missing` | BOOLEAN | Data quality flag |
| `total_amount_mismatch` | BOOLEAN | Data quality flag |
| `processed_at` | TIMESTAMP | Silver layer processing time |
| `processed_at_gold` | TIMESTAMP | Gold layer processing time |

### Dimension Tables

**`dim_customer`**

| Column | Type |
|--------|------|
| `customer_sk` | INTEGER (PK) |
| `customer_id` | STRING |
| `customer_name` | STRING |
| `region` | STRING |

**`dim_product`**

| Column | Type |
|--------|------|
| `product_sk` | INTEGER (PK) |
| `product_id` | STRING |
| `product_name` | STRING |
| `category` | STRING |

**`dim_date`**

| Column | Type |
|--------|------|
| `date_sk` | INTEGER (PK) |
| `order_date` | DATE |
| `year` | INTEGER |
| `quarter` | INTEGER |
| `month` | INTEGER |
| `month_name` | STRING |
| `week_of_year` | INTEGER |
| `day_of_month` | INTEGER |
| `day_of_week` | INTEGER |
| `day_name` | STRING |
| `is_weekend` | BOOLEAN |

**`dim_region`**

| Column | Type |
|--------|------|
| `region_sk` | INTEGER (PK) |
| `region_name` | STRING |
| `region_code` | STRING |
| `geo_zone` | STRING |

---

## ⚙️ GCP Infrastructure

| Component | Resource |
|-----------|----------|
| Orchestration | Cloud Composer (Apache Airflow) |
| Processing | Dataproc (PySpark) |
| Storage | Google Cloud Storage (3 buckets) |
| Warehouse | BigQuery |
| Cluster | `my-cluster` (configurable via Airflow Variable) |

### GCS Bucket Layout

```
data-platform-bronze-layer/
├── scripts/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
└── <raw source files>

data-platform-silver-layer/
└── sales/
    └── <cleaned parquet/csv files>

data-platform-gold-layer/
└── sales/
    ├── dim_customer/part-*.csv
    ├── dim_product/part-*.csv
    ├── dim_date/part-*.csv
    ├── dim_region/part-*.csv
    └── fact_sales/part-*.csv
```

---

## 🔧 Airflow Variables

Set these in your Cloud Composer environment before triggering the DAG:

| Variable | Default | Description |
|----------|---------|-------------|
| `GCP_PROJECT_ID` | `data-platform-dev-493507` | GCP project ID |
| `GCP_REGION` | `us-central1` | Region for Dataproc and BQ |
| `DATAPROC_CLUSTER` | `my-cluster` | Dataproc cluster name |
| `BQ_DATASET` | `sales_gold` | Target BigQuery dataset |
| `BQ_LOCATION` | `us-central1` | BigQuery dataset location |

---

## 🚀 DAG Details

**DAG ID:** `sales_pipeline_bronze_to_bq`  
**Schedule:** `30 4 * * *` (daily at 04:30 UTC)  
**Retries:** 1 retry with 10-minute delay

### Task Dependency Graph

```
start
  └── bronze_to_silver_job (Dataproc)
        └── silver_to_gold_job (Dataproc)
              ├── sensor_dim_customer ──► load_dim_customer ──┐
              ├── sensor_dim_product  ──► load_dim_product  ──┤
              ├── sensor_dim_date     ──► load_dim_date     ──┼──► load_fact_sales ──► end
              ├── sensor_dim_region   ──► load_dim_region   ──┤
              └── sensor_fact_sales ─────────────────────────┘
```

`load_fact_sales` waits for **all four dimension tables to load** plus the fact sensor before executing, ensuring referential integrity.

---

## 🐛 Known Issues & Fixes

### `load_fact_sales` — BigQuery 400 Error (Fixed)

**Symptom:** `CSV processing encountered too many errors, giving up. Rows: 95; errors: 95`

**Root Cause:** `SCHEMA_FACT_SALES` in the DAG defined only 8 columns. The actual gold CSV produced by the Spark job contains 26 columns. BigQuery reads positionally, so column index 5 (`customer_id`, a string like `"CUST01"`) was being parsed as `quantity` (INT64) — failing on every row.

**Fix:** Updated `SCHEMA_FACT_SALES` to declare all 26 columns in the correct order, matching the gold layer CSV output exactly.

> ⚠️ **Tip:** Whenever the silver-to-gold Spark transformation adds or renames columns, the `SCHEMA_FACT_SALES` definition in the DAG must be updated to stay in sync.

---

## 📋 Prerequisites

- GCP project with billing enabled
- Cloud Composer environment (Airflow 2.x)
- Dataproc cluster running and accessible
- GCS buckets created (`bronze`, `silver`, `gold`)
- BigQuery dataset `sales_gold` created in the target project
- Airflow Variables configured (see table above)
- Service account with roles: `BigQuery Data Editor`, `Dataproc Editor`, `Storage Object Admin`

---

## 📄 License

This project is intended for internal data platform use.
