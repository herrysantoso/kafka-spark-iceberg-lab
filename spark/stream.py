"""Spark Structured Streaming: Kafka -> Iceberg (Hadoop catalog)."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")

spark = (
    SparkSession.builder.appName("kafka-to-iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.orders (
        id BIGINT, city STRING, amount INT, ts DOUBLE
    ) USING iceberg
""")

schema = "id BIGINT, city STRING, amount INT, ts DOUBLE"
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)
df = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

query = (
    df.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="10 seconds")  # tiap 10 detik = 1 snapshot baru
    .option("checkpointLocation", "/checkpoints/orders")
    .toTable("local.db.orders")
)
query.awaitTermination()
