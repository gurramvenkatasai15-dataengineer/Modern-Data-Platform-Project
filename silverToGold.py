"""
Silver → Gold Layer Transformation
Sales Data Pipeline — Star Schema Design
PySpark on Dataproc

Star Schema:
  fact_sales
    ├── dim_customer
    ├── dim_product
    ├── dim_date
    └── dim_region
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, LongType
import logging

# ─────────────────────────────────────────────
# CONFIG — update these paths
# ─────────────────────────────────────────────
SILVER_PATH = "gs://data-platform-silver-layer/sales"
GOLD_PATH   = "gs://data-platform-gold-layer/sales"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Silver_to_Gold_Sales") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# STEP 1 — READ CSV FROM SILVER
# ─────────────────────────────────────────────
logger.info("Reading CSV from Silver: %s", SILVER_PATH)

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(SILVER_PATH)

logger.info("Silver record count: %d", df.count())
df.printSchema()

# ─────────────────────────────────────────────
# STEP 2 — BUILD dim_customer
# ─────────────────────────────────────────────
logger.info("Building dim_customer...")

dim_customer = df.select(
    "customer_id",
    "customer_name",
    "region"
).distinct()

# Surrogate key (monotonically increasing, unique per customer_id)
dim_customer = dim_customer.withColumn(
    "customer_sk",
    F.dense_rank().over(Window.orderBy("customer_id")).cast(LongType())
)

dim_customer = dim_customer.select(
    "customer_sk",
    "customer_id",
    "customer_name",
    "region"
)

logger.info("dim_customer: %d rows", dim_customer.count())

# ─────────────────────────────────────────────
# STEP 3 — BUILD dim_product
# ─────────────────────────────────────────────
logger.info("Building dim_product...")

dim_product = df.select(
    "product_id",
    "product_name",
    "category"
).distinct()

dim_product = dim_product.withColumn(
    "product_sk",
    F.dense_rank().over(Window.orderBy("product_id")).cast(LongType())
)

dim_product = dim_product.select(
    "product_sk",
    "product_id",
    "product_name",
    "category"
)

logger.info("dim_product: %d rows", dim_product.count())

# ─────────────────────────────────────────────
# STEP 4 — BUILD dim_date
# Generated from all unique order_date values in the Silver data
# ─────────────────────────────────────────────
logger.info("Building dim_date...")

dim_date = df.select("order_date").distinct() \
    .filter(F.col("order_date").isNotNull())

dim_date = dim_date \
    .withColumn("date_sk",        F.date_format("order_date", "yyyyMMdd").cast(IntegerType())) \
    .withColumn("year",           F.year("order_date")) \
    .withColumn("quarter",        F.quarter("order_date")) \
    .withColumn("month",          F.month("order_date")) \
    .withColumn("month_name",     F.date_format("order_date", "MMMM")) \
    .withColumn("week_of_year",   F.weekofyear("order_date")) \
    .withColumn("day_of_month",   F.dayofmonth("order_date")) \
    .withColumn("day_of_week",    F.dayofweek("order_date")) \
    .withColumn("day_name",       F.date_format("order_date", "EEEE")) \
    .withColumn("is_weekend",     F.dayofweek("order_date").isin([1, 7]).cast("boolean")) \
    .withColumn("is_month_start", F.dayofmonth("order_date") == 1) \
    .withColumn("is_month_end",
        F.dayofmonth("order_date") == F.dayofmonth(F.last_day("order_date")))

dim_date = dim_date.select(
    "date_sk",
    "order_date",
    "year",
    "quarter",
    "month",
    "month_name",
    "week_of_year",
    "day_of_month",
    "day_of_week",
    "day_name",
    "is_weekend",
    "is_month_start",
    "is_month_end"
)

logger.info("dim_date: %d rows", dim_date.count())

# ─────────────────────────────────────────────
# STEP 5 — BUILD dim_region
# ─────────────────────────────────────────────
logger.info("Building dim_region...")

# GEO-ZONE MAPPING — extend as needed
geo_zone_map = {
    "north":     "North Zone",
    "south":     "South Zone",
    "east":      "East Zone",
    "west":      "West Zone",
    "central":   "Central Zone",
    "northeast": "North Zone",
    "northwest": "North Zone",
    "southeast": "South Zone",
    "southwest": "South Zone",
}

# Build mapping expression
geo_zone_expr = F.lit("Unknown Zone")
for region_val, zone in geo_zone_map.items():
    geo_zone_expr = F.when(F.col("region_name") == region_val, zone).otherwise(geo_zone_expr)

dim_region = df.select(
    F.col("region").alias("region_name")
).distinct()

dim_region = dim_region \
    .withColumn("region_sk",   F.dense_rank().over(Window.orderBy("region_name")).cast(LongType())) \
    .withColumn("region_code", F.upper(F.col("region_name"))) \
    .withColumn("geo_zone",    geo_zone_expr)

dim_region = dim_region.select(
    "region_sk",
    "region_name",
    "region_code",
    "geo_zone"
)

logger.info("dim_region: %d rows", dim_region.count())

# ─────────────────────────────────────────────
# STEP 6 — BUILD fact_sales
# Join back to dimension tables to get surrogate keys
# ─────────────────────────────────────────────
logger.info("Building fact_sales...")

# Align column names used in joins
df_with_date_sk = df.withColumn(
    "date_sk",
    F.date_format("order_date", "yyyyMMdd").cast(IntegerType())
)

fact = df_with_date_sk \
    .join(dim_customer.select("customer_sk", "customer_id"),
          on="customer_id", how="left") \
    .join(dim_product.select("product_sk", "product_id"),
          on="product_id", how="left") \
    .join(
        # Only bring in the join key (_date) from dim_date — NOT date_sk again,
        # because df_with_date_sk already has date_sk computed above.
        # Joining on date_sk = date_sk would create two date_sk columns → AMBIGUOUS_REFERENCE.
        dim_date.select(F.col("order_date").alias("_date")),
        df_with_date_sk["order_date"] == F.col("_date"),
        how="left"
    ) \
    .drop("_date") \
    .join(dim_region.select("region_sk", F.col("region_name").alias("region")),
          on="region", how="left")

# ─────────────────────────────────────────────
# STEP 7 — ADD BUSINESS METRICS
# ─────────────────────────────────────────────
logger.info("Adding business metrics...")

fact = fact \
    .withColumn("revenue",
        F.round(F.col("quantity") * F.col("unit_price"), 2)) \
    .withColumn("discount_amount",
        F.round(F.col("revenue") - F.col("total_amount"), 2)) \
    .withColumn("discount_rate",
        F.when(F.col("revenue") > 0,
               F.round((F.col("revenue") - F.col("total_amount")) / F.col("revenue"), 4)
              ).otherwise(F.lit(0.0))) \
    .withColumn("avg_unit_price",
        F.round(F.col("total_amount") / F.when(F.col("quantity") > 0, F.col("quantity")).otherwise(1), 2)) \
    .withColumn("is_high_value",
        (F.col("total_amount") > 500).cast("boolean")) \
    .withColumn("order_size_bucket",
        F.when(F.col("quantity") <= 1,  "single")
         .when(F.col("quantity") <= 5,  "small")
         .when(F.col("quantity") <= 20, "medium")
         .otherwise("large")) \
    .withColumn("revenue_band",
        F.when(F.col("total_amount") < 50,    "low")
         .when(F.col("total_amount") < 500,   "medium")
         .when(F.col("total_amount") < 5000,  "high")
         .otherwise("enterprise")) \
    .withColumn("processed_at_gold", F.current_timestamp())

# ─────────────────────────────────────────────
# STEP 8 — SELECT FINAL FACT COLUMNS
# ─────────────────────────────────────────────
fact_sales = fact.select(
    # Surrogate keys (FKs to dims)
    "customer_sk",
    "product_sk",
    "date_sk",
    "region_sk",

    # Natural / degenerate keys
    "order_id",
    "customer_id",
    "product_id",

    # Date fields (for partition + convenience)
    "order_date",
    "order_year",
    "order_month",

    # Status
    "status",

    # Raw measures
    "quantity",
    "unit_price",
    "total_amount",
    "calculated_total",

    # Business metrics
    "revenue",
    "discount_amount",
    "discount_rate",
    "avg_unit_price",
    "is_high_value",
    "order_size_bucket",
    "revenue_band",

    # Audit flags (carried over from Silver)
    "is_date_missing",
    "total_amount_mismatch",

    # Audit timestamps
    "processed_at",
    "processed_at_gold"
)

logger.info("fact_sales: %d rows", fact_sales.count())
fact_sales.printSchema()

# ─────────────────────────────────────────────
# STEP 9 — WRITE DIMENSION TABLES TO GOLD (no partition needed)
# ─────────────────────────────────────────────
logger.info("Writing dimension tables to Gold...")

dim_tables = {
    "dim_customer": dim_customer,
    "dim_product":  dim_product,
    "dim_date":     dim_date,
    "dim_region":   dim_region,
}

for table_name, dim_df in dim_tables.items():
    path = f"{GOLD_PATH}/{table_name}"
    logger.info("Writing %s -> %s", table_name, path)
    dim_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)
    logger.info("%s written successfully.", table_name)

# ─────────────────────────────────────────────
# STEP 10 — WRITE FACT TABLE TO GOLD
# Single CSV — suitable for small datasets (<= ~10k rows)
# For larger data switch back to .partitionBy(...).parquet()
# ─────────────────────────────────────────────
fact_path = f"{GOLD_PATH}/fact_sales"
logger.info("Writing fact_sales -> %s", fact_path)

fact_sales.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(fact_path)

logger.info("fact_sales written successfully.")

# ─────────────────────────────────────────────
# STEP 11 — QUICK SANITY CHECKS
# ─────────────────────────────────────────────
logger.info("Running sanity checks...")

logger.info("── Null surrogate key check ──")
null_sk_count = fact_sales.filter(
    F.col("customer_sk").isNull() |
    F.col("product_sk").isNull()  |
    F.col("date_sk").isNull()     |
    F.col("region_sk").isNull()
).count()
logger.info("Rows with null SKs: %d  (expected: 0)", null_sk_count)

logger.info("── Revenue vs total_amount mismatch ──")
mismatch_count = fact_sales.filter(F.col("total_amount_mismatch") == True).count()
logger.info("Amount mismatches carried from Silver: %d", mismatch_count)

logger.info("── Revenue band distribution ──")
fact_sales.groupBy("revenue_band") \
    .agg(F.count("*").alias("orders"),
         F.round(F.sum("revenue"), 2).alias("total_revenue")) \
    .orderBy("revenue_band") \
    .show()

logger.info("── Row counts per partition (sample) ──")
fact_sales.groupBy("order_year", "order_month") \
    .count() \
    .orderBy("order_year", "order_month") \
    .show(24)

# ─────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────
logger.info("Gold layer pipeline complete.")
logger.info("Gold output: %s", GOLD_PATH)
spark.stop()