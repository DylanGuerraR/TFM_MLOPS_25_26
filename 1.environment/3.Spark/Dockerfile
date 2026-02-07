# Fichero: 3.Spark/Dockerfile
# Propósito: Imagen personalizada para desplegar el procesamiento de Spark 
# en el Data Lake. Usada por el servicio 'spark-kafka-to-bronze'.

# Base Bitnami Spark 3.5.0
FROM bitnamilegacy/spark:3.5.0

# Acceso root para preparar el entorno de la aplicación
USER 0

# Carpeta de destino del código fuente en el contenedor
RUN mkdir -p /opt/spark-app

# Instalamos pytest para los tests unitarios de ingeniería
RUN pip install pytest

# Incorporamos la lógica de ingesta y validación al contenedor
COPY kafka_to_bronze.py /opt/spark-app/kafka_to_bronze.py
COPY check_parity.py /opt/spark-app/check_parity.py
COPY test_ingestion.py /opt/spark-app/test_ingestion.py
