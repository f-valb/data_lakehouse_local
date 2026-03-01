"""
Spark Structured Streaming: Kafka → Bronze Delta tables.
Writes raw events exactly as received, no transformations.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, schema_of_json
from pyspark.sql.types import StringType, StructType, StructField

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_KEY         = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET      = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

spark = (
    SparkSession.builder
    .appName("BronzeIngestion")
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

def read_kafka(topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

def write_bronze(topic):
    df = read_kafka(topic)
    # Keep the raw kafka fields plus ingestion metadata
    bronze = df.select(
        col("key").cast(StringType()).alias("kafka_key"),
        col("value").cast(StringType()).alias("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    )
    return (
        bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"s3a://bronze/_checkpoints/{topic}")
        .option("path", f"s3a://bronze/{topic}")
        .trigger(processingTime="5 seconds")
        .start()
    )

orders_query = write_bronze("orders")
clicks_query  = write_bronze("clicks")

print("✅ Bronze streaming started for orders + clicks", flush=True)
orders_query.awaitTermination()
