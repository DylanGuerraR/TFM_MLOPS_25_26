# Base oficial de Bitnami Spark para consistencia con el cluster
FROM bitnamilegacy/spark:3.5.0

# Cambiamos a root para configurar la estructura de la aplicación
USER 0

# Directorio de trabajo para los scripts de Spark
RUN mkdir -p /opt/spark-app

# Instalamos pytest para los tests unitarios
RUN pip install pytest

# Copiamos los scripts de ingesta y validación
COPY kafka_to_bronze.py /opt/spark-app/kafka_to_bronze.py
COPY check_parity.py /opt/spark-app/check_parity.py
COPY test_ingestion.py /opt/spark-app/test_ingestion.py
