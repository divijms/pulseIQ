from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("PulseIQ-Bronze-Streaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "product.events.v1")
    .option("startingOffsets", "latest")
    .load()
)

event_schema = StructType([
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
    ]))
])

# Parse Kafka Value
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", expr("timestamp(event_ts)"))
)

# Watermark (Late Data Handling)
watermarked_df = (
    parsed_df
    .withWatermark("event_time", "10 minutes")
)

# Write to Bronze (Delta)
query = (
    watermarked_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/pulseiq/bronze/checkpoints")
    .start("/tmp/pulseiq/bronze/events")
)

query.awaitTermination()

