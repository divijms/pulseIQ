import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, current_timestamp

# -----------------------------
# Args
# -----------------------------
start_ts = sys.argv[1]  # e.g. 2025-01-20T00:00:00
end_ts   = sys.argv[2]  # e.g. 2025-01-20T23:59:59

spark = (
    SparkSession.builder
    .appName("PulseIQ-Backfill")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -----------------------------
# Paths
# -----------------------------
GOLD_PATH = "/tmp/pulseiq/gold/metrics"
ANOMALY_PATH = "/tmp/pulseiq/anomaly/events"

ANALYTICS_OUT = "/tmp/pulseiq/analytics/hourly_feature_metrics"
ALERTS_OUT = "/tmp/pulseiq/alerts/escalated_anomalies"

# -----------------------------
# Load Gold (bounded)
# -----------------------------
gold_df = (
    spark.read.format("delta").load(GOLD_PATH)
    .filter(
        (col("event_time") >= start_ts) &
        (col("event_time") <= end_ts)
    )
)

# -----------------------------
# Recompute Hourly Analytics
# -----------------------------
hourly = (
    gold_df
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("feature"),
        col("region")
    )
    .agg(
        count("*").alias("event_count"),
        avg("avg_latency_ms").alias("avg_latency_ms")
    )
)

(
    hourly
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(ANALYTICS_OUT)
)

# -----------------------------
# Replay Escalated Anomalies
# -----------------------------
anomaly_df = (
    spark.read.format("delta").load(ANOMALY_PATH)
    .filter(
        (col("event_time") >= start_ts) &
        (col("event_time") <= end_ts)
    )
    .filter(col("is_anomaly") == True)
)

alerts = anomaly_df.withColumn("replayed_at", current_timestamp())

(
    alerts
    .write
    .format("delta")
    .mode("append")
    .save(ALERTS_OUT)
)

spark.stop()

