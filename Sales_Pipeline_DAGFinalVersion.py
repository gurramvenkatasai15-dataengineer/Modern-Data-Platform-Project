"""
Cloud Composer DAG — Sales Data Pipeline
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

GCP_PROJECT_ID   = Variable.get("GCP_PROJECT_ID", default_var="data-platform-dev-493507")
GCP_REGION       = Variable.get("GCP_REGION", default_var="us-central1")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER", default_var="my-cluster")

BRONZE_BUCKET = "data-platform-bronze-layer"
SILVER_BUCKET = "data-platform-silver-layer"
GOLD_BUCKET   = "data-platform-gold-layer"

GOLD_BASE_PATH = "sales"

BRONZE_TO_SILVER_SCRIPT = f"gs://{BRONZE_BUCKET}/scripts/bronze_to_silver.py"
SILVER_TO_GOLD_SCRIPT   = f"gs://{BRONZE_BUCKET}/scripts/silver_to_gold.py"

BQ_DATASET  = Variable.get("BQ_DATASET", default_var="sales_gold")
BQ_LOCATION = Variable.get("BQ_LOCATION", default_var="us-central1")


GOLD_PREFIXES = {
    "dim_customer": f"{GOLD_BASE_PATH}/dim_customer/part-",
    "dim_product":  f"{GOLD_BASE_PATH}/dim_product/part-",
    "dim_date":     f"{GOLD_BASE_PATH}/dim_date/part-",
    "dim_region":   f"{GOLD_BASE_PATH}/dim_region/part-",
    "fact_sales":   f"{GOLD_BASE_PATH}/fact_sales/part-",
}

GOLD_GCS_SOURCES = {
    "dim_customer": f"{GOLD_BASE_PATH}/dim_customer/*.csv",
    "dim_product":  f"{GOLD_BASE_PATH}/dim_product/*.csv",
    "dim_date":     f"{GOLD_BASE_PATH}/dim_date/*.csv",
    "dim_region":   f"{GOLD_BASE_PATH}/dim_region/*.csv",
    "fact_sales":   f"{GOLD_BASE_PATH}/fact_sales/*.csv",
}


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_DIM_CUSTOMER = [
    {"name": "customer_sk", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_id", "type": "STRING"},
    {"name": "customer_name", "type": "STRING"},
    {"name": "region", "type": "STRING"},
]

SCHEMA_DIM_PRODUCT = [
    {"name": "product_sk", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "product_id", "type": "STRING"},
    {"name": "product_name", "type": "STRING"},
    {"name": "category", "type": "STRING"},
]

SCHEMA_DIM_DATE = [
    {"name": "date_sk", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "order_date", "type": "DATE"},
    {"name": "year", "type": "INTEGER"},
    {"name": "quarter", "type": "INTEGER"},
    {"name": "month", "type": "INTEGER"},
    {"name": "month_name", "type": "STRING"},
    {"name": "week_of_year", "type": "INTEGER"},
    {"name": "day_of_month", "type": "INTEGER"},
    {"name": "day_of_week", "type": "INTEGER"},
    {"name": "day_name", "type": "STRING"},
    {"name": "is_weekend", "type": "BOOLEAN"},
]

SCHEMA_DIM_REGION = [
    {"name": "region_sk", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "region_name", "type": "STRING"},
    {"name": "region_code", "type": "STRING"},
    {"name": "geo_zone", "type": "STRING"},
]

# 🔥 FIX: Schema updated to match all 26 columns in the actual gold CSV.
# The original schema had only 8 columns, causing a column offset where
# BigQuery read customer_id (string) into the quantity (INT64) field → 400 error.
SCHEMA_FACT_SALES = [
    {"name": "customer_sk",         "type": "INTEGER"},
    {"name": "product_sk",          "type": "INTEGER"},
    {"name": "date_sk",             "type": "INTEGER"},
    {"name": "region_sk",           "type": "INTEGER"},
    {"name": "order_id",            "type": "STRING"},
    {"name": "customer_id",         "type": "STRING"},   # was missing — caused the offset
    {"name": "product_id",          "type": "STRING"},   # was missing
    {"name": "order_date",          "type": "DATE"},     # was missing
    {"name": "order_year",          "type": "INTEGER"},  # was missing
    {"name": "order_month",         "type": "INTEGER"},  # was missing
    {"name": "status",              "type": "STRING"},   # was missing
    {"name": "quantity",            "type": "INTEGER"},  # now at correct column index 11
    {"name": "unit_price",          "type": "FLOAT"},
    {"name": "total_amount",        "type": "FLOAT"},
    {"name": "calculated_total",    "type": "FLOAT"},    # was missing
    {"name": "revenue",             "type": "FLOAT"},    # was missing
    {"name": "discount_amount",     "type": "FLOAT"},    # was missing
    {"name": "discount_rate",       "type": "FLOAT"},    # was missing
    {"name": "avg_unit_price",      "type": "FLOAT"},    # was missing
    {"name": "is_high_value",       "type": "BOOLEAN"},  # was missing
    {"name": "order_size_bucket",   "type": "STRING"},   # was missing
    {"name": "revenue_band",        "type": "STRING"},   # was missing
    {"name": "is_date_missing",     "type": "BOOLEAN"},  # was missing
    {"name": "total_amount_mismatch","type": "BOOLEAN"}, # was missing
    {"name": "processed_at",        "type": "TIMESTAMP"},# was missing
    {"name": "processed_at_gold",   "type": "TIMESTAMP"},# was missing
]


# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="sales_pipeline_bronze_to_bq",
    default_args=default_args,
    schedule_interval="30 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_to_silver = DataprocSubmitJobOperator(
        task_id="bronze_to_silver_job",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {"main_python_file_uri": BRONZE_TO_SILVER_SCRIPT},
        },
    )

    silver_to_gold = DataprocSubmitJobOperator(
        task_id="silver_to_gold_job",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {"main_python_file_uri": SILVER_TO_GOLD_SCRIPT},
        },
    )

    sensor_dim_customer = GCSObjectsWithPrefixExistenceSensor(
        task_id="sensor_dim_customer",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_customer"],
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    sensor_dim_product = GCSObjectsWithPrefixExistenceSensor(
        task_id="sensor_dim_product",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_product"],
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    sensor_dim_date = GCSObjectsWithPrefixExistenceSensor(
        task_id="sensor_dim_date",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_date"],
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    sensor_dim_region = GCSObjectsWithPrefixExistenceSensor(
        task_id="sensor_dim_region",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["dim_region"],
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    sensor_fact_sales = GCSObjectsWithPrefixExistenceSensor(
        task_id="sensor_fact_sales",
        bucket=GOLD_BUCKET,
        prefix=GOLD_PREFIXES["fact_sales"],
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    load_dim_customer = GCSToBigQueryOperator(
        task_id="load_dim_customer",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_customer"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_customer",
        schema_fields=SCHEMA_DIM_CUSTOMER,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_dim_product = GCSToBigQueryOperator(
        task_id="load_dim_product",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_product"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product",
        schema_fields=SCHEMA_DIM_PRODUCT,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_dim_date = GCSToBigQueryOperator(
        task_id="load_dim_date",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_date"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_date",
        schema_fields=SCHEMA_DIM_DATE,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_dim_region = GCSToBigQueryOperator(
        task_id="load_dim_region",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["dim_region"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_region",
        schema_fields=SCHEMA_DIM_REGION,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_fact_sales = GCSToBigQueryOperator(
        task_id="load_fact_sales",
        bucket=GOLD_BUCKET,
        source_objects=[GOLD_GCS_SOURCES["fact_sales"]],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.fact_sales",
        schema_fields=SCHEMA_FACT_SALES,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    # DAG FLOW
    start >> bronze_to_silver >> silver_to_gold

    silver_to_gold >> [
        sensor_dim_customer,
        sensor_dim_product,
        sensor_dim_date,
        sensor_dim_region,
        sensor_fact_sales,
    ]

    sensor_dim_customer >> load_dim_customer
    sensor_dim_product >> load_dim_product
    sensor_dim_date >> load_dim_date
    sensor_dim_region >> load_dim_region

    [
        sensor_fact_sales,
        load_dim_customer,
        load_dim_product,
        load_dim_date,
        load_dim_region,
    ] >> load_fact_sales

    load_fact_sales >> end