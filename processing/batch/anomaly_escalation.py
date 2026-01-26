from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = (
    SparkSession.builder
    .appName("PulseIQ-Anomaly-Escalation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

ANOMALY_PATH = "/tmp/pulseiq/anomaly/events"
OUTPUT_PATH = "/tmp/pulseiq/alerts/escalated_anomalies"

df = spark.read.format("delta").load(ANOMALY_PATH)

# -----------------------------
# Escalation Rules (Business)
# -----------------------------
escalated = (
    df
    .filter(col("is_anomaly") == True)
    .filter(
        (col("event_count_zscore") >= 3.5) |
        (col("latency_zscore") >= 3.5)
    )
    .withColumn("escalated_at", current_timestamp())
)

(
    escalated
    .write
    .format("delta")
    .mode("append")
    .save(OUTPUT_PATH)
)

spark.stop()

