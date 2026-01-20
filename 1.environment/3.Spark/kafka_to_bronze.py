import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC", "leads_raw")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/datalake/bronze/leads_raw")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/datalake/checkpoints/leads_raw")

spark = (
    SparkSession.builder
    .appName("kafka-to-bronze")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Bronze: guardamos key/value como string + timestamp Kafka
bronze = (
    df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("timestamp").alias("kafka_timestamp")
    )
    .withColumn("ingestion_date", to_date(col("kafka_timestamp")))
)

query = (
    bronze.writeStream
    .format("parquet")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingestion_date")
    .outputMode("append")
    .trigger(availableNow=True)   # procesa lo disponible y termina
    .start()
)

query.awaitTermination()
