"""
Cloud Composer DAG — Sales Data Pipeline
=========================================
Flow:
  bronze_to_silver_job
        ↓
  silver_to_gold_job
        ↓  (all four Gold CSVs must exist before any BQ load)
  ┌──────────────────────────────────────────┐
  │  gcs_sensor_dim_customer                 │
  │  gcs_sensor_dim_product                  │
  │  gcs_sensor_dim_date                     │
  │  gcs_sensor_dim_region                   │
  │  gcs_sensor_fact_sales                   │
  └──────────────────────────────────────────┘
        ↓ (all sensors pass)
  ┌──────────────────────────────────────────┐
  │  load_dim_customer_to_bq                 │
  │  load_dim_product_to_bq                  │
  │  load_dim_date_to_bq                     │
  │  load_dim_region_to_bq                   │
  └──────────────────────────────────────────┘
        ↓ (all dims loaded)
  load_fact_sales_to_bq
        ↓
  pipeline_complete  (dummy finish gate)

SLA  : 10:00 – 11:00 IST  (04:30 – 05:30 UTC)
Schedule: daily @ 04:30 UTC  →  triggers at 10:00 AM IST
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  — edit these or override via Airflow Variables
# ─────────────────────────────────────────────────────────────────────────────

# GCP project & region
GCP_PROJECT_ID   = Variable.get("GCP_PROJECT_ID",   default_var="your-gcp-project-id")
GCP_REGION       = Variable.get("GCP_REGION",        default_var="us-central1")

# Dataproc cluster name (must already exist)
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER",  default_var="sales-pipeline-cluster")

# GCS paths — must match the paths hard-coded in your PySpark scripts
BRONZE_BUCKET    = "data-platform-bronze-layer"
SILVER_BUCKET    = "data-platform-silver-layer"
GOLD_BUCKET      = "data-platform-gold-layer"

GOLD_BASE_PATH   = "sales"                           # gs://data-platform-gold-layer/sales/

# GCS paths to the PySpark scripts (upload these before running the DAG)
BRONZE_TO_SILVER_SCRIPT = f"gs://{BRONZE_BUCKET}/scripts/bronze_to_silver.py"
SILVER_TO_GOLD_SCRIPT   = f"gs://{BRONZE_BUCKET}/scripts/silver_to_gold.py"

# BigQuery — must match the dataset + tables you create in the pre-setup steps
BQ_DATASET       = Variable.get("BQ_DATASET",        default_var="sales_gold")
BQ_LOCATION      = Variable.get("BQ_LOCATION",       default_var="US")

# GCS sensors: prefix inside the Gold bucket to confirm CSVs are present.
# Dataproc writes a folder like  sales/fact_sales/part-00000-*.csv
# The sensor looks for any object under that prefix.
GOLD_PREFIXES = {
    "dim_customer": f"{GOLD_BASE_PATH}/dim_customer/",
    "dim_product":  f"{GOLD_BASE_PATH}/dim_product/",
    "dim_date":     f"{GOLD_BASE_PATH}/dim_date/",
    "dim_region":   f"{GOLD_BASE_PATH}/dim_region/",
    "fact_sales":   f"{GOLD_BASE_PATH}/fact_sales/",
}

# GCS source URI pattern for GCSToBigQueryOperator
# BigQuery can read the whole folder; wildcard picks up the part file(s).
GOLD_GCS_SOURCES = {
    "dim_customer": f"{GOLD_BASE_PATH}/dim_customer/*.csv",
    "dim_product":  f"{GOLD_BASE_PATH}/dim_product/*.csv",
    "dim_date":     f"{GOLD_BASE_PATH}/dim_date/*.csv",
    "dim_region":   f"{GOLD_BASE_PATH}/dim_region/*.csv",
    "fact_sales":   f"{GOLD_BASE_PATH}/fact_sales/*.csv",
}

# ─────────────────────────────────────────────────────────────────────────────
# BIGQUERY SCHEMAS
# Mirror the columns produced by your Silver → Gold PySpark job exactly.
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_DIM_CUSTOMER = [
    {"name": "customer_sk",   "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_id",   "type": "STRING",  "mode": "NULLABLE"},
    {"name": "customer_name", "type": "STRING",  "mode": "NULLABLE"},
    {"name": "region",        "type": "STRING",  "mode": "NULLABLE"},
]

SCHEMA_DIM_PRODUCT = [
    {"name": "product_sk",   "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "product_id",   "type": "STRING",  "mode": "NULLABLE"},
    {"name": "product_name", "type": "STRING",  "mode": "NULLABLE"},
    {"name": "category",     "type": "STRING",  "mode": "NULLABLE"},
]

SCHEMA_DIM_DATE = [
    {"name": "date_sk",        "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "order_date",     "type": "DATE",    "mode": "NULLABLE"},
    {"name": "year",           "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "quarter",        "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "month",          "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "month_name",     "type": "STRING",  "mode": "NULLABLE"},
    {"name": "week_of_year",   "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "day_of_month",   "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "day_of_week",    "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "day_name",       "type": "STRING",  "mode": "NULLABLE"},
    {"name": "is_weekend",     "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "is_month_start", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "is_month_end",   "type": "BOOLEAN", "mode": "NULLABLE"},
]

SCHEMA_DIM_REGION = [
    {"name": "region_sk",   "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "region_name", "type": "STRING",  "mode": "NULLABLE"},
    {"name": "region_code", "type": "STRING",  "mode": "NULLABLE"},
    {"name": "geo_zone",    "type": "STRING",  "mode": "NULLABLE"},
]

SCHEMA_FACT_SALES = [
    # Surrogate keys (FKs to dims)
    {"name": "customer_sk",           "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "product_sk",            "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "date_sk",               "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "region_sk",             "type": "INTEGER",   "mode": "NULLABLE"},
    # Natural / degenerate keys
    {"name": "order_id",              "type": "STRING",    "mode": "NULLABLE"},
    {"name": "customer_id",           "type": "STRING",    "mode": "NULLABLE"},
    {"name": "product_id",            "type": "STRING",    "mode": "NULLABLE"},
    # Date convenience
    {"name": "order_date",            "type": "DATE",      "mode": "NULLABLE"},
    {"name": "order_year",            "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "order_month",           "type": "INTEGER",   "mode": "NULLABLE"},
    # Status
    {"name": "status",                "type": "STRING",    "mode": "NULLABLE"},
    # Raw measures
    {"name": "quantity",              "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "unit_price",            "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "total_amount",          "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "calculated_total",      "type": "FLOAT64",   "mode": "NULLABLE"},
    # Business metrics
    {"name": "revenue",               "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "discount_amount",       "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "discount_rate",         "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "avg_unit_price",        "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "is_high_value",         "type": "BOOLEAN",   "mode": "NULLABLE"},
    {"name": "order_size_bucket",     "type": "STRING",    "mode": "NULLABLE"},
    {"name": "revenue_band",          "type": "STRING",    "mode": "NULLABLE"},
    # Audit flags
    {"name": "is_date_missing",       "type": "BOOLEAN",   "mode": "NULLABLE"},
    {"name": "total_amount_mismatch", "type": "BOOLEAN",   "mode": "NULLABLE"},
    # Audit timestamps
    {"name": "processed_at",          "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "processed_at_gold",     "type": "TIMESTAMP", "mode": "NULLABLE"},
]

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    # SLA: entire pipeline must finish within 60 min of start (10 AM IST = 04:30 UTC)
    "sla":              timedelta(minutes=60),
    "email_on_failure": True,
    "email_on_retry":   False,
    # Replace with actual alert email
    "email":            ["data-engineering-alerts@yourcompany.com"],
}

# ─────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="sales_pipeline_bronze_to_bq",
    default_args=default_args,
    description="Daily sales pipeline: Bronze CSV → Silver → Gold (Star Schema) → BigQuery",
    # 04:30 UTC = 10:00 AM IST (UTC+5:30)
    schedule_interval="30 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "dataproc", "bigquery", "star-schema"],
) as dag:

    # ── Start gate ────────────────────────────────────────────────────────────
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # ── STEP 1: Bronze → Silver ───────────────────────────────────────────────
    bronze_to_silver = DataprocSubmitJobOperator(
        task_id="bronze_to_silver_job",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": BRONZE_TO_SILVER_SCRIPT,
                # Pass any extra packages or jars if needed
                # "jar_file_uris": [],
                # "python_file_uris": [],
                "properties": {
                    "spark.sql.legacy.timeParserPolicy": "LEGACY",
                },
            },
            "labels": {
                "pipeline": "sales",
                "layer":    "bronze-to-silver",
            },
        },
        execution_timeout=timedelta(minutes=45),
    )

    # ── STEP 2: Silver → Gold ─────────────────────────────────────────────────
    silver_to_gold = DataprocSubmitJobOperator(
        task_id="silver_to_gold_job",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": SILVER_TO_GOLD_SCRIPT,
                "properties": {
                    "spark.sql.legacy.timeParserPolicy": "LEGACY",
                },
            },
            "labels": {
                "pipeline": "sales",
                "layer":    "silver-to-gold",
            },
        },
        execution_timeout=timedelta(minutes=45),
    )

    # ── STEP 3: GCS Sensors — verify Gold files exist before loading to BQ ────
    # Each sensor polls for at least one object under the given prefix.
    # poke_interval=60 → checks every 60 s; timeout=900 → give up after 15 min.

    sensor_dim_customer = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs_sensor_dim_customer",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_customer"],
        google_cloud_conn_id="google_cloud_default",
        poke_interval=60,
        timeout=900,
        mode="reschedule",         # frees the worker slot while waiting
    )

    sensor_dim_product = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs_sensor_dim_product",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_product"],
        google_cloud_conn_id="google_cloud_default",
        poke_interval=60,
        timeout=900,
        mode="reschedule",
    )

    sensor_dim_date = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs_sensor_dim_date",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_date"],
        google_cloud_conn_id="google_cloud_default",
        poke_interval=60,
        timeout=900,
        mode="reschedule",
    )

    sensor_dim_region = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs_sensor_dim_region",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_region"],
        google_cloud_conn_id="google_cloud_default",
        poke_interval=60,
        timeout=900,
        mode="reschedule",
    )

    sensor_fact_sales = GCSObjectsWithPrefixExistenceSensor(
        task_id="gcs_sensor_fact_sales",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["fact_sales"],
        google_cloud_conn_id="google_cloud_default",
        poke_interval=60,
        timeout=900,
        mode="reschedule",
    )

    # ── STEP 4: Load Dimension tables to BigQuery (parallel) ──────────────────
    # write_disposition=WRITE_TRUNCATE → full refresh every day (safe for dims).
    # Switch to WRITE_APPEND + dedup if you need incremental loads.

    load_dim_customer = GCSToBigQueryOperator(
        task_id="load_dim_customer_to_bq",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_customer"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_customer",
        schema_fields=SCHEMA_DIM_CUSTOMER,
        source_format="CSV",
        skip_leading_rows=1,          # header row
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    load_dim_product = GCSToBigQueryOperator(
        task_id="load_dim_product_to_bq",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_product"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product",
        schema_fields=SCHEMA_DIM_PRODUCT,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    load_dim_date = GCSToBigQueryOperator(
        task_id="load_dim_date_to_bq",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_date"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_date",
        schema_fields=SCHEMA_DIM_DATE,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    load_dim_region = GCSToBigQueryOperator(
        task_id="load_dim_region_to_bq",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_region"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_region",
        schema_fields=SCHEMA_DIM_REGION,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    # ── STEP 5: Load Fact table to BigQuery ───────────────────────────────────
    # Runs ONLY after all four dimension tables are loaded successfully.

    load_fact_sales = GCSToBigQueryOperator(
        task_id="load_fact_sales_to_bq",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["fact_sales"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.fact_sales",
        schema_fields=SCHEMA_FACT_SALES,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    # ── End gate ──────────────────────────────────────────────────────────────
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # DEPENDENCY GRAPH
    # ─────────────────────────────────────────────────────────────────────────

    # Start
    pipeline_start >> bronze_to_silver

    # Spark jobs are sequential (Silver must finish before Gold starts)
    bronze_to_silver >> silver_to_gold

    # After Gold is written, run all 5 sensors in parallel
    silver_to_gold >> [
        sensor_dim_customer,
        sensor_dim_product,
        sensor_dim_date,
        sensor_dim_region,
        sensor_fact_sales,
    ]

    # Dimension loads: each sensor gates its own load (parallel loads)
    sensor_dim_customer >> load_dim_customer
    sensor_dim_product  >> load_dim_product
    sensor_dim_date     >> load_dim_date
    sensor_dim_region   >> load_dim_region

    # Fact load waits for ALL sensors (gold files confirmed) AND all dim loads
    [
        sensor_fact_sales,
        load_dim_customer,
        load_dim_product,
        load_dim_date,
        load_dim_region,
    ] >> load_fact_sales

    # Pipeline complete
    load_fact_sales >> pipeline_complete