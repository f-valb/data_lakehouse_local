"""
Medallion batch job: Bronze → Silver → Gold.
Runs in an infinite loop every 60 seconds.
"""
import json, os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, to_timestamp, when, count, sum as _sum,
    avg, max as _max, min as _min, countDistinct, round as _round,
    current_timestamp, date_trunc, expr, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    BooleanType, TimestampType
)
from delta.tables import DeltaTable

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_KEY        = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET     = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
INTERVAL_SECS  = int(os.getenv("MEDALLION_INTERVAL", "60"))

spark = (
    SparkSession.builder
    .appName("MedallionBatch")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Schemas ────────────────────────────────────────────────────────────────────

ORDER_SCHEMA = StructType([
    StructField("event_type",      StringType()),
    StructField("order_id",        StringType()),
    StructField("user_id",         StringType()),
    StructField("session_id",      StringType()),
    StructField("product_id",      StringType()),
    StructField("product_name",    StringType()),
    StructField("category",        StringType()),
    StructField("quantity",        IntegerType()),
    StructField("unit_price",      DoubleType()),
    StructField("total_amount",    DoubleType()),
    StructField("currency",        StringType()),
    StructField("status",          StringType()),
    StructField("payment_method",  StringType()),
    StructField("country",         StringType()),
    StructField("timestamp",       StringType()),
    StructField("kafka_key",       StringType()),
])

CLICK_SCHEMA = StructType([
    StructField("event_type",   StringType()),
    StructField("click_id",     StringType()),
    StructField("user_id",      StringType()),
    StructField("session_id",   StringType()),
    StructField("product_id",   StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("page",         StringType()),
    StructField("referrer",     StringType()),
    StructField("device",       StringType()),
    StructField("duration_ms",  IntegerType()),
    StructField("converted",    BooleanType()),
    StructField("timestamp",    StringType()),
])

# ── Bronze → Silver ────────────────────────────────────────────────────────────

def bronze_to_silver_orders():
    try:
        raw = spark.read.format("delta").load("s3a://bronze/orders")
    except Exception as e:
        print(f"⚠️  Bronze orders not ready yet: {e}", flush=True)
        return

    parsed = (
        raw.withColumn("data", from_json(col("raw_value"), ORDER_SCHEMA))
        .select("data.*", "ingested_at")
        .filter(col("order_id").isNotNull())
        .filter(col("total_amount") > 0)
        .filter(col("status").isin("completed","pending","cancelled","refunded"))
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("total_amount", _round(col("total_amount"), 2))
        .dropDuplicates(["order_id"])
        .withColumn("processed_at", current_timestamp())
    )

    (
        parsed.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://silver/orders")
    )
    print(f"✅ Silver orders written: {parsed.count()} rows", flush=True)


def bronze_to_silver_clicks():
    try:
        raw = spark.read.format("delta").load("s3a://bronze/clicks")
    except Exception as e:
        print(f"⚠️  Bronze clicks not ready yet: {e}", flush=True)
        return

    parsed = (
        raw.withColumn("data", from_json(col("raw_value"), CLICK_SCHEMA))
        .select("data.*", "ingested_at")
        .filter(col("click_id").isNotNull())
        .filter(col("duration_ms") > 0)
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("referrer", when(col("referrer").isNull(), lit("direct")).otherwise(col("referrer")))
        .dropDuplicates(["click_id"])
        .withColumn("processed_at", current_timestamp())
    )

    (
        parsed.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://silver/clicks")
    )
    print(f"✅ Silver clicks written: {parsed.count()} rows", flush=True)


# ── Silver → Gold ──────────────────────────────────────────────────────────────

def silver_to_gold():
    # ── daily_revenue ──────────────────────────────────
    try:
        orders = spark.read.format("delta").load("s3a://silver/orders")
    except Exception as e:
        print(f"⚠️  Silver orders not ready: {e}", flush=True)
        return

    completed = orders.filter(col("status") == "completed")

    daily_revenue = (
        completed
        .withColumn("date", date_trunc("day", col("event_ts")))
        .groupBy("date")
        .agg(
            _round(_sum("total_amount"), 2).alias("revenue"),
            count("order_id").alias("order_count"),
            countDistinct("user_id").alias("unique_customers"),
            _round(avg("total_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    (
        daily_revenue.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://gold/daily_revenue")
    )
    print(f"✅ Gold daily_revenue: {daily_revenue.count()} rows", flush=True)

    # ── product_metrics ────────────────────────────────
    product_metrics = (
        completed
        .groupBy("product_id", "product_name", "category")
        .agg(
            count("order_id").alias("total_orders"),
            _round(_sum("total_amount"), 2).alias("total_revenue"),
            _round(avg("unit_price"), 2).alias("avg_price"),
            _sum("quantity").alias("units_sold"),
            countDistinct("user_id").alias("unique_buyers"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    (
        product_metrics.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://gold/product_metrics")
    )
    print(f"✅ Gold product_metrics: {product_metrics.count()} rows", flush=True)

    # ── user_metrics ───────────────────────────────────
    user_orders = (
        orders
        .groupBy("user_id")
        .agg(
            count("order_id").alias("total_orders"),
            count(when(col("status") == "completed", 1)).alias("completed_orders"),
            count(when(col("status") == "cancelled", 1)).alias("cancelled_orders"),
            _round(_sum(when(col("status") == "completed", col("total_amount"))), 2).alias("total_spend"),
            _round(avg(when(col("status") == "completed", col("total_amount"))), 2).alias("avg_order_value"),
            _max("event_ts").alias("last_order_ts"),
            _min("event_ts").alias("first_order_ts"),
            countDistinct("product_id").alias("unique_products_bought"),
        )
    )

    try:
        clicks = spark.read.format("delta").load("s3a://silver/clicks")
        user_clicks = (
            clicks.groupBy("user_id")
            .agg(
                count("click_id").alias("total_clicks"),
                count(when(col("converted"), 1)).alias("converted_clicks"),
                countDistinct("session_id").alias("total_sessions"),
            )
        )
        user_metrics = (
            user_orders.join(user_clicks, "user_id", "left")
            .withColumn("conversion_rate",
                _round(col("completed_orders") / (col("total_clicks") + lit(1)), 4))
            .withColumn("updated_at", current_timestamp())
        )
    except Exception:
        user_metrics = user_orders.withColumn("updated_at", current_timestamp())

    (
        user_metrics.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://gold/user_metrics")
    )
    print(f"✅ Gold user_metrics: {user_metrics.count()} rows", flush=True)

    # ── funnel ────────────────────────────────────────
    try:
        clicks = spark.read.format("delta").load("s3a://silver/clicks")
        funnel = (
            clicks
            .groupBy("page")
            .agg(
                count("click_id").alias("visits"),
                countDistinct("session_id").alias("sessions"),
                countDistinct("user_id").alias("unique_users"),
                count(when(col("converted"), 1)).alias("conversions"),
            )
            .withColumn("conversion_rate", _round(col("conversions") / (col("visits") + lit(1)), 4))
            .withColumn("updated_at", current_timestamp())
        )

        (
            funnel.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save("s3a://gold/funnel")
        )
        print(f"✅ Gold funnel: {funnel.count()} rows", flush=True)
    except Exception as e:
        print(f"⚠️  Funnel skipped: {e}", flush=True)


# ── Main loop ──────────────────────────────────────────────────────────────────

def run_once():
    print("🔄 Running medallion batch...", flush=True)
    bronze_to_silver_orders()
    bronze_to_silver_clicks()
    silver_to_gold()
    print(f"✅ Medallion batch complete. Next run in {INTERVAL_SECS}s\n", flush=True)

if __name__ == "__main__":
    # Wait for bronze to have some data
    print("⏳ Waiting 30s for Bronze to accumulate data...", flush=True)
    time.sleep(30)
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"❌ Batch error: {e}", flush=True)
        time.sleep(INTERVAL_SECS)
