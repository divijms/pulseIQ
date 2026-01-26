from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("PulseIQ-Silver-Streaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("product", StructType([
        StructField("feature", StringType()),
        StructField("action", StringType())
    ])),
    StructField("device", StructType([
        StructField("os", StringType()),
        StructField("app_version", StringType())
    ])),
    StructField("geo", StructType([
        StructField("country", StringType()),
        StructField("region", StringType())
    ])),
    StructField("metrics", StructType([
        StructField("latency_ms", IntegerType()),
        StructField("value", DoubleType())
    ])),
    StructField("event_time", TimestampType())
])

# Read Bronze Delta
bronze_df = (
    spark.readStream
    .format("delta")
    .load("/tmp/pulseiq/bronze/events")
)

# Data Quality + Normalization
silver_df = (
    bronze_df
    .filter(col("event_id").isNotNull())
    .filter(col("event_time").isNotNull())
    .filter(col("metrics.latency_ms") >= 0)
    .withColumn("event_time", to_timestamp(col("event_time")))
)

# Deduplication
silver_deduped = (
    silver_df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

# Write to Silver
query = (
    silver_deduped
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/pulseiq/silver/checkpoints")
    .start("/tmp/pulseiq/silver/events")
)

query.awaitTermination()

