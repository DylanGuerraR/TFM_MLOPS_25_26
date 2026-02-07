import os
import time
from pyspark.sql import SparkSession

def check_parity():
    # 1. Configuración desde entorno
    KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker-1:29092")
    TOPIC = os.getenv("KAFKA_TOPIC", "leads_raw")
    PART_PATH = os.getenv("PART_PATH")

    if not PART_PATH:
        print("[Error] PART_PATH no definida.")
        exit(1)

    print(f"--- Iniciando Chequeo de Paridad Senior (Native Spark) ---")
    print(f"Topic: {TOPIC}, Partición: {PART_PATH}")

    # 2. Inicialización de Spark
    # Importante: Necesita el paquete spark-sql-kafka-0-10
    spark = (
        SparkSession.builder
        .appName("parity-check")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 3. Obtener cuenta de Kafka usando el conector nativo
        # Hacemos una lectura batch desde el inicio hasta el final actual
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        kafka_total = kafka_df.count()
        
        # 4. Obtener cuenta de Bronze con reintentos para cargas pesadas
        bronze_total = 0
        max_retries = 5
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"Reintento {attempt}/{max_retries} esperando persistencia física...")
                    time.sleep(5)
                
                bronze_df = spark.read.parquet(PART_PATH)
                bronze_total = bronze_df.count()
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"[Error] No se pudo leer la partición Bronze tras {max_retries} intentos: {e}")
                    exit(1)
                continue

        # 5. Comparación y Veredicto
        print(f"\nResultados del Chequeo:")
        print(f"  - Mensajes en Kafka: {kafka_total}")
        print(f"  - Registros en Bronze: {bronze_total}")

        if kafka_total == bronze_total:
            print("ÉXITO: Paridad total. No hay pérdida de datos.")
        elif bronze_total > kafka_total:
            print("AVISO: Más datos en Bronze que en Kafka (posible solapamiento o re-ejecución).")
        else:
            diff = kafka_total - bronze_total
            print(f" FALLO: Pérdida de datos detectada. Faltan {diff} registros en Bronze.")
            exit(2)

    except Exception as e:
        print(f"ERROR CRÍTICO durante el chequeo: {e}")
        exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    check_parity()
