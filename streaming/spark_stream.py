import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from prometheus_client import Gauge, start_http_server

# Prometheus metrics
BYTES_GAUGE = Gauge("dtms_stream_bytes", "Live streaming transfer size (bytes)")
LATENCY_GAUGE = Gauge("dtms_stream_latency_ms", "Live streaming transfer latency (ms)")

# Expose metrics on 0.0.0.0:8002/metrics
start_http_server(8002)
print("[spark_exporter] Prometheus metrics server on :8002/metrics")

# Kafka config
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
print(f"[spark_exporter] Using bootstrap servers: {bootstrap_servers}")

# Spark session with Kafka connector
spark = SparkSession.builder \
    .appName("DTMS-Spark-Streaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema of incoming records
schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("bytes", LongType(), True),
    StructField("latency_ms", DoubleType(), True),
])

# Read from Kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "dtms_transfers") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


def update_metrics(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    agg = batch_df.agg(
        avg("bytes").alias("avg_bytes"),
        avg("latency_ms").alias("avg_latency"),
    ).collect()[0]

    BYTES_GAUGE.set(agg["avg_bytes"])
    LATENCY_GAUGE.set(agg["avg_latency"])
    print(f"[spark_exporter] Batch {batch_id}: avg_bytes={agg['avg_bytes']:.2f}, "
          f"avg_latency={agg['avg_latency']:.2f} ms")


query = parsed.writeStream \
    .outputMode("update") \
    .foreachBatch(update_metrics) \
    .start()

print("[spark_exporter] Spark streaming started.")
query.awaitTermination()