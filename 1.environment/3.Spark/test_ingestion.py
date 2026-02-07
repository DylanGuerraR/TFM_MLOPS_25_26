"""
Tests unitarios para la lógica de transformación de Spark (Kafka -> Bronze).
Usa una SparkSession local para validar la lógica pura sin infraestructura externa.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, TimestampType
from kafka_to_bronze import transform_to_bronze
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    """Fixture que crea una sesión de Spark local para los tests."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("spark-unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def test_transform_to_bronze_basic_mapping(spark):
    """Verifica que el mapeo de campos de Kafka a Bronze sea correcto."""
    
    # Datos de prueba simulando el esquema de Kafka
    data = [
        (b"key1", b'{"id": 1}', datetime(2026, 2, 2, 10, 0, 0)),
    ]
    schema = StructType([
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("timestamp", TimestampType()),
    ])
    
    df_input = spark.createDataFrame(data, schema)
    
    # Ejecutamos transformación
    df_output = transform_to_bronze(df_input)
    
    # Validación
    row = df_output.collect()[0]
    assert row["kafka_key"] == "key1"
    assert row["raw_json"] == '{"id": 1}'
    assert str(row["ingestion_date"]) == "2026-02-02"

def test_transform_to_bronze_with_date_override(spark):
    """Verifica que el override de fecha funcione correctamente para backfills."""
    
    data = [
        (b"key1", b"{}", datetime(2026, 2, 2, 10, 0, 0)),
    ]
    schema = StructType([
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("timestamp", TimestampType()),
    ])
    
    df_input = spark.createDataFrame(data, schema)
    
    # Forzamos una fecha distinta a la del timestamp de Kafka
    OVERRIDE_DATE = "2025-01-01"
    df_output = transform_to_bronze(df_input, ingestion_date_override=OVERRIDE_DATE)
    
    # Validación
    row = df_output.collect()[0]
    assert str(row["ingestion_date"]) == OVERRIDE_DATE
