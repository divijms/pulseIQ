from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev, abs, col

spark = (
    SparkSession.builder
    .appName("PulseIQ-Anomaly-Detection")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Read Gold Metrics (Streaming)
# -----------------------------
gold_df = (
    spark.readStream
    .format("delta")
    .load("/tmp/pulseiq/gold/metrics")
)

# -----------------------------
# Per-batch anomaly detection
# -----------------------------
def detect_anomalies(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    stats_df = (
        batch_df
        .groupBy("feature", "region")
        .agg(
            avg("events_count").alias("mean_events"),
            stddev("events_count").alias("std_events"),
            avg("p95_latency_ms").alias("mean_latency"),
            stddev("p95_latency_ms").alias("std_latency")
        )
    )

    anomaly_df = (
        batch_df
        .join(stats_df, ["feature", "region"])
        .withColumn(
            "event_count_zscore",
            abs(col("events_count") - col("mean_events")) / col("std_events")
        )
        .withColumn(
            "latency_zscore",
            abs(col("p95_latency_ms") - col("mean_latency")) / col("std_latency")
        )
        .withColumn(
            "is_anomaly",
            (col("event_count_zscore") > 3) | (col("latency_zscore") > 3)
        )
    )

    (
        anomaly_df
        .write
        .format("delta")
        .mode("append")
        .save("/tmp/pulseiq/anomaly/events")
    )

# -----------------------------
# Start streaming query
# -----------------------------
query = (
    gold_df
    .writeStream
    .foreachBatch(detect_anomalies)
    .option("checkpointLocation", "/tmp/pulseiq/anomaly/checkpoints")
    .start()
)

query.awaitTermination()

