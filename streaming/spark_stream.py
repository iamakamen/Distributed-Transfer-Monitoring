import os
from pathlib import Path  # NEW
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from prometheus_client import Gauge, start_http_server


# Prometheus metrics (now per src_site)
BYTES_GAUGE = Gauge(
    "dtms_stream_bytes",
    "Live streaming transfer size (bytes) averaged per src_site",
    ["src_site"],
)
LATENCY_GAUGE = Gauge(
    "dtms_stream_latency_ms",
    "Live streaming transfer latency (ms) averaged per src_site",
    ["src_site"],
)

# Expose metrics on 0.0.0.0:8002/metrics
start_http_server(8002)
print("[spark_exporter] Prometheus metrics server on :8002/metrics")

# Kafka config
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
print(f"[spark_exporter] Using bootstrap servers: {bootstrap_servers}")

# Base directory (repo root inside container is /app)
BASE_DIR = Path(__file__).resolve().parent.parent
PARQUET_DIR = BASE_DIR / "sample_data" / "parquet" / "site_aggregates"

# Ensure parent folder exists (Spark will create leaf dir)
PARQUET_DIR.parent.mkdir(parents=True, exist_ok=True)
print(f"[spark_exporter] Writing Parquet aggregates to: {PARQUET_DIR}")


# Spark session with Kafka connector
spark = SparkSession.builder \
    .appName("DTMS-Spark-Streaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema of incoming records (matches producer events)
schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("src_site", StringType(), True),
    StructField("dst_site", StringType(), True),
    StructField("reporting_site", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("bytes", LongType(), True),
    StructField("duration", DoubleType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", DoubleType(), True),
])

# Read from Kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "dtms_transfers") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")


def update_metrics(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Group by src_site and compute averages per site
    agg_df = batch_df.groupBy("src_site").agg(
        avg("bytes").alias("avg_bytes"),
        avg("latency_ms").alias("avg_latency"),
    )

    rows = agg_df.collect()

    for row in rows:
        src_site = row["src_site"]
        avg_bytes = row["avg_bytes"]
        avg_latency = row["avg_latency"]

        # Update Prometheus gauges with a src_site label
        BYTES_GAUGE.labels(src_site=src_site).set(avg_bytes)
        LATENCY_GAUGE.labels(src_site=src_site).set(avg_latency)

        print(
            f"[spark_exporter] Batch {batch_id} src_site={src_site}: "
            f"avg_bytes={avg_bytes:.2f}, avg_latency={avg_latency:.2f} ms"
        )
    
    
    # NEW: write per-site aggregates to Parquet
    agg_df.write.mode("append").parquet(str(PARQUET_DIR))
    print(f"[spark_exporter] Batch {batch_id}: wrote aggregates to {PARQUET_DIR}")


query = parsed.writeStream \
    .outputMode("update") \
    .foreachBatch(update_metrics) \
    .start()

print("[spark_exporter] Spark streaming started.")
query.awaitTermination()
