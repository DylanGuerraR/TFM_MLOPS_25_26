"""
Script: kafka_to_bronze.py
Descripción: Job de Spark para la ingesta desde Kafka a Bronze.
Refactorización Senior: Se separa la lógica de transformación de la infraestructura para permitir unit testing.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit

def transform_to_bronze(df, ingestion_date_override=None):
    """
    Lógica de transformación pura: convierte el esquema de Kafka al esquema de la capa Bronze.
    
    Args:
        df (DataFrame): DataFrame de entrada con esquema de Kafka (key, value, timestamp, etc.)
        ingestion_date_override (str, optional): Fecha ISO para forzar el particionado.
        
    Returns:
        DataFrame: DataFrame transformado listo para ser escrito en Bronze.
    """
    # Proyectamos las columnas necesarias y convertimos binarios a string
    bronze = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("timestamp").alias("kafka_timestamp")
    )

    # Aplicamos la lógica de particionado (Override para Backfills vs Fecha Real)
    if ingestion_date_override:
        return bronze.withColumn("ingestion_date", to_date(lit(ingestion_date_override)))
    else:
        return bronze.withColumn("ingestion_date", to_date(col("kafka_timestamp")))

def run_ingestion():
    """
    Orquestador de la ingesta: Maneja la conexión con Kafka y la escritura en disco.
    """
    # 1. Configuración desde entorno
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker-1:29092")
    TOPIC = os.getenv("KAFKA_TOPIC", "leads_raw")
    BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/datalake/bronze/leads_raw")
    CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/datalake/checkpoints/leads_raw")
    INGESTION_DATE_OVERRIDE = os.getenv("INGESTION_DATE_OVERRIDE")

    # 2. Inicialización de Spark
    spark = SparkSession.builder.appName("kafka-to-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 3. Lectura de Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 4. Transformación (Uso de la función testeable)
    bronze_df = transform_to_bronze(raw_stream, INGESTION_DATE_OVERRIDE)

    # 5. Escritura en Datalake
    query = (
        bronze_df.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingestion_date")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print(f"[Spark] Ingesta finalizada en {BRONZE_PATH}")

if __name__ == "__main__":
    run_ingestion()
