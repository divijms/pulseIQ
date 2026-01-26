from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max
from datetime import datetime
import sys

delta_path = sys.argv[1]
layer_name = sys.argv[2]
freshness_threshold_minutes = int(sys.argv[3])

spark = (
    SparkSession.builder
    .appName(f"PulseIQ-{layer_name}-Healthcheck")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df = spark.read.format("delta").load(delta_path)

max_event_time = (
    df.select(spark_max("event_time").alias("max_event_time"))
      .collect()[0]["max_event_time"]
)

if max_event_time is None:
    raise ValueError(f"{layer_name} table is empty")

age_minutes = (datetime.utcnow() - max_event_time).total_seconds() / 60

if age_minutes > freshness_threshold_minutes:
    raise ValueError(
        f"{layer_name} data is stale! "
        f"Last event was {age_minutes:.1f} minutes ago"
    )

print(f"✅ {layer_name} healthy — last event {age_minutes:.1f} minutes ago")

spark.stop()

