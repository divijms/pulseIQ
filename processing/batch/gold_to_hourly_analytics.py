from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, avg, count

spark = (
    SparkSession.builder
    .appName("PulseIQ-Hourly-Batch-Aggregation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

GOLD_PATH = "/tmp/pulseiq/gold/metrics"
OUTPUT_PATH = "/tmp/pulseiq/analytics/hourly_feature_metrics"

df = spark.read.format("delta").load(GOLD_PATH)

hourly_agg = (
    df
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("product.feature"),
        col("geo.region")
    )
    .agg(
        count("*").alias("event_count"),
        avg("metrics.latency_ms").alias("avg_latency_ms")
    )
)

(
    hourly_agg
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(OUTPUT_PATH)
)

spark.stop()

