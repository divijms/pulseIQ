from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, expr
)

spark = (
    SparkSession.builder
    .appName("PulseIQ-Gold-Streaming")
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

# -----------------------------
# Read Silver Delta (Streaming)
# -----------------------------
silver_df = (
    spark.readStream
    .format("delta")
    .load("/tmp/pulseiq/silver/events")
)

# -----------------------------
# Windowed Aggregations
# -----------------------------
gold_agg = (
    silver_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("product.feature").alias("feature"),
        col("geo.region").alias("region")
    )
    .agg(
        count("*").alias("events_count"),
        avg(col("metrics.latency_ms")).alias("avg_latency_ms"),
        expr("percentile_approx(metrics.latency_ms, 0.95)").alias("p95_latency_ms")
    )
)

# -----------------------------
# Write to Gold Delta
# -----------------------------
query = (
    gold_agg
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/pulseiq/gold/checkpoints")
    .start("/tmp/pulseiq/gold/metrics")
)

query.awaitTermination()

