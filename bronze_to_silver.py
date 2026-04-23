"""
Bronze → Silver Layer Transformation
Sales Data Pipeline using PySpark on Dataproc
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging

# ─────────────────────────────────────────────
# CONFIG — update these paths
# ─────────────────────────────────────────────
BRONZE_PATH = "gs://data-platform-bronze-layer/sales/sales.csv"
SILVER_PATH  = "gs://data-platform-silver-layer/sales"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Bronze_to_Silver_Sales") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# SCHEMA — all raw fields read as String, cast later
# ─────────────────────────────────────────────
schema = StructType([
    StructField("order_id",      StringType(), True),
    StructField("customer_id",   StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product_id",    StringType(), True),
    StructField("product_name",  StringType(), True),
    StructField("category",      StringType(), True),
    StructField("quantity",      StringType(), True),   # cast → Int
    StructField("unit_price",    StringType(), True),   # cast → Double
    StructField("total_amount",  StringType(), True),   # cast → Double
    StructField("order_date",    StringType(), True),   # cast → Date
    StructField("region",        StringType(), True),
    StructField("status",        StringType(), True),
])

# ─────────────────────────────────────────────
# STEP 1 — READ CSV FROM BRONZE
# ─────────────────────────────────────────────
logger.info("Reading CSV from Bronze: %s", BRONZE_PATH)

df = spark.read \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .schema(schema) \
    .csv(BRONZE_PATH)

logger.info("Raw record count: %d", df.count())

# ─────────────────────────────────────────────
# STEP 2 — REMOVE DUPLICATES
# ─────────────────────────────────────────────
logger.info("Removing duplicates...")

df = df.dropDuplicates()                              # full row dups
df = df.dropDuplicates(["order_id", "product_id"])    # business-key dups

logger.info("After dedup: %d rows", df.count())

# ─────────────────────────────────────────────
# STEP 3 — STANDARDIZE TEXT
# ─────────────────────────────────────────────
logger.info("Standardizing text...")

lower_cols = ["category", "status", "region"]
trim_cols  = ["order_id", "customer_id", "product_id", "customer_name", "product_name"]

for col in lower_cols:
    df = df.withColumn(col, F.lower(F.trim(F.col(col))))

for col in trim_cols:
    df = df.withColumn(col, F.trim(F.col(col)))

# Fix common UTF-8 encoding artifacts
encoding_fixes = {
    "â€™": "'",
    "â€œ": '"',
    "â€":  '"',
    "Ã©":  "é",
    "\x00": "",    # null bytes
}
for col in lower_cols + trim_cols:
    for bad, good in encoding_fixes.items():
        df = df.withColumn(col, F.regexp_replace(F.col(col), bad, good))

# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# STEP 4 — CAST DATA TYPES + HANDLE MULTIPLE DATE FORMATS
# ─────────────────────────────────────────────
logger.info("Casting types...")

# Preserve raw date
df = df.withColumnRenamed("order_date", "order_date_raw")

# Normalize date (trim + replace / with -)
df = df.withColumn(
    "order_date_clean",
    F.regexp_replace(F.trim(F.col("order_date_raw")), "/", "-")
)

# Parse multiple formats carefully
df = df.withColumn(
    "order_date",
    F.coalesce(
        # 1. ISO format (safest)
        F.to_date("order_date_clean", "yyyy-MM-dd"),

        # 2. dd-MM-yyyy (when day > 12 → unambiguous)
        F.when(
            F.col("order_date_clean").substr(1,2).cast("int") > 12,
            F.to_date("order_date_clean", "dd-MM-yyyy")
        ),

        # 3. MM-dd-yyyy (fallback)
        F.to_date("order_date_clean", "MM-dd-yyyy"),

        # 4. dd-MM-yyyy fallback (for remaining)
        F.to_date("order_date_clean", "dd-MM-yyyy")
    )
)

# Cast numeric fields
df = df \
    .withColumn("quantity",     F.col("quantity").cast(IntegerType())) \
    .withColumn("unit_price",   F.col("unit_price").cast("double")) \
    .withColumn("total_amount", F.col("total_amount").cast("double"))

# ─────────────────────────────────────────────
# STEP 5 — HANDLE NULLS
# ─────────────────────────────────────────────
logger.info("Handling nulls...")

# Drop rows where critical keys are null
df = df.filter(
    F.col("order_id").isNotNull() &
    F.col("customer_id").isNotNull() &
    F.col("product_id").isNotNull()
)

# Fill remaining nulls with safe defaults
df = df.fillna({
    "customer_name": "unknown",
    "product_name":  "unknown",
    "category":      "uncategorized",
    "region":        "unknown",
    "status":        "unknown",
    "quantity":      0,
    "unit_price":    0.0,
    "total_amount":  0.0,
})

# Flag missing dates (don't fill — avoids wrong defaults)
df = df.withColumn("is_date_missing",
    F.col("order_date").isNull().cast("boolean"))

# ─────────────────────────────────────────────
# STEP 6 — DERIVED & AUDIT COLUMNS
# ─────────────────────────────────────────────
logger.info("Adding derived columns...")

df = df.withColumn("calculated_total",
        F.round(F.col("quantity") * F.col("unit_price"), 2)) \
       .withColumn("total_amount_mismatch",
        (F.abs(F.col("total_amount") - F.col("calculated_total")) > 0.01).cast("boolean")) \
       .withColumn("order_year",  F.year(F.col("order_date"))) \
       .withColumn("order_month", F.month(F.col("order_date"))) \
       .withColumn("processed_at", F.current_timestamp())

# ─────────────────────────────────────────────
# STEP 7 — LOG FINAL STATE
# ─────────────────────────────────────────────
df.printSchema()
logger.info("Final record count: %d", df.count())

# ─────────────────────────────────────────────
# STEP 8 — WRITE PARQUET TO SILVER
# Partitioned by year/month for fast downstream queries
# ─────────────────────────────────────────────
logger.info("Writing Parquet to Silver: %s", SILVER_PATH)

# df.write \
#     .mode("overwrite") \
#     .partitionBy("order_year", "order_month") \
#     .parquet(SILVER_PATH)

df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(SILVER_PATH)

logger.info("Silver layer write complete.")
spark.stop()