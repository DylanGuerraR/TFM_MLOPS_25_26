"""
Script: producer.py
Descripción: Generador de datos sintéticos de alta fidelidad para el dataset de Leads.
Simula el comportamiento de un CRM enviando mensajes JSON a un topic de Kafka.
"""

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from faker import Faker
from kafka import KafkaProducer


def weighted_choice(weight_map: dict):
    """
    Selecciona una clave de un diccionario basándose en sus pesos (valores).
    
    Args:
        weight_map (dict): Diccionario donde la clave es el valor a elegir y el valor es el peso.
        
    Returns:
        any: La clave seleccionada aleatoriamente según la distribución de pesos.
    """
    items = list(weight_map.items())
    values = [v for v, _ in items]
    weights = [w for _, w in items]
    return random.choices(values, weights=weights, k=1)[0]


def env_int(name: str, default: int) -> int:
    """
    Obtiene una variable de entorno y la convierte a entero.
    
    Args:
        name (str): Nombre de la variable de entorno.
        default (int): Valor por defecto si la variable no existe o está vacía.
        
    Returns:
        int: El valor de la variable de entorno o el default.
    """
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def env_float(name: str, default: float) -> float:
    """
    Obtiene una variable de entorno y la convierte a flotante.
    
    Args:
        name (str): Nombre de la variable de entorno.
        default (float): Valor por defecto si la variable no existe o está vacía.
        
    Returns:
        float: El valor de la variable de entorno o el default.
    """
    v = os.getenv(name, "").strip()
    return float(v) if v else default


def env_str(name: str, default: str) -> str:
    """
    Obtiene una variable de entorno como cadena de texto.
    
    Args:
        name (str): Nombre de la variable de entorno.
        default (str): Valor por defecto si la variable no existe o está vacía.
        
    Returns:
        str: El valor de la variable de entorno o el default.
    """
    v = os.getenv(name, "").strip()
    return v if v else default


def clamp(x, lo, hi):
    """
    Restringe un valor numérico entre un límite inferior y uno superior.
    
    Args:
        x (num): Valor a restringir.
        lo (num): Límite inferior.
        hi (num): Límite superior.
        
    Returns:
        num: El valor x si está en rango, de lo contrario el límite correspondiente.
    """
    return max(lo, min(hi, x))


def iso_utc(dt: datetime) -> str:
    """
    Convierte un objeto datetime a cadena ISO 8601 en formato UTC.
    
    Args:
        dt (datetime): El objeto fecha/hora a convertir.
        
    Returns:
        str: Representación ISO de la fecha.
    """
    return dt.astimezone(timezone.utc).isoformat()


def main():
    """
    Función principal que configura el productor de Kafka y genera el flujo de datos sintéticos.
    """
    # ---- 1. Configuración vía Variables de Entorno ----
    bootstrap = env_str(
        "KAFKA_BOOTSTRAP_SERVERS",
        "broker-1:29092,broker-2:29093,broker-3:29094",
    )
    topic = env_str("KAFKA_TOPIC", "leads_raw")
    rows = env_int("ROWS", 1000)
    days = env_int("DAYS", 30)
    
    # Manejo de fechas para Backfills y sincronización con Airflow
    start_date_str = os.getenv("START_DATE", "").strip()
    if start_date_str:
        start_dt = datetime.fromisoformat(start_date_str).replace(tzinfo=timezone.utc)
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=days)

    seed = env_int("SEED", 42)
    convert_rate = env_float("CONVERT_RATE", 0.35)
    sleep_ms = env_int("SLEEP_MS", 0)
    schema_version = env_str("SCHEMA_VERSION", "v1")
    source_system = env_str("SOURCE_SYSTEM", "crm_leads_v1")

    # Inicialización de generadores aleatorios
    random.seed(seed)
    fake = Faker("en_IN")

    # ---- 2. Distribuciones Categóricas (Modelado del mundo real) ----
    # Estas tablas de pesos se basan en el análisis de datos original del TFM (Kaggle)
    CITY_W = {
        "Mumbai": 3016, "Thane & Outskirts": 670, "Other Cities": 625,
        "Unknown": 607, "Other Cities of Maharashtra": 416,
        "Other Metro Cities": 369, "Select": 228, "Tier II Cities": 73,
    }

    LEAD_ORIGIN_W = {
        "Landing Page Submission": 4808, "API": 3580, "Lead Add Form": 718, "Lead Import": 55,
    }

    LEAD_SOURCE_W = {
        "Direct Traffic": 2543, "Google": 2868, "Organic Search": 1154, "Olark Chat": 1755,
        "Reference": 534, "Welingak Website": 142, "Referral Sites": 125, "Facebook": 55,
    }

    LAST_ACTIVITY_W = {
        "Email Opened": 3437, "SMS Sent": 2745, "Olark Chat Conversation": 973,
        "Page Visited on Website": 640, "Converted to Lead": 428, "Email Bounced": 326,
    }

    OCCUPATION_W = {
        "Unemployed": 5600, "Working Professional": 706, "Student": 210, "Unknown": 2600,
    }

    ASYM_INDEX_W = {"01.Low": 45, "02.Medium": 35, "03.High": 20}

    COUNTRY_W = {"India": 6492, "Unknown": 2461, "Others": 287}

    # ---- 3. Inicialización del Productor de Kafka ----
    producer = KafkaProducer(
        bootstrap_servers=[b.strip() for b in bootstrap.split(",")],
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=8,
        linger_ms=10,
    )

    # Configuración de rendimiento
    flush_every = env_int("FLUSH_EVERY", 2000)

    # Control de fecha de ingesta para sincronización Airflow
    ingestion_date_str = os.getenv("INGESTION_DATE", "").strip()
    if ingestion_date_str:
        print(f"[producer] Usando fecha override para ingestion_time: {ingestion_date_str}")
        fixed_ing_dt = datetime.fromisoformat(ingestion_date_str).replace(tzinfo=timezone.utc)
    else:
        fixed_ing_dt = None

    # ---- 4. Bucle de Generación de Mensajes ----
    for i in range(rows):
        # Generación de tiempos (evento e ingesta)
        event_time = start_dt + timedelta(seconds=random.randint(0, days * 24 * 3600))
        ingestion_time = fixed_ing_dt or datetime.now(timezone.utc)

        # Identificadores de negocio
        prospect_id = fake.uuid4()
        lead_number = random.randint(1000000, 9999999)

        # Variables numéricas con distribución normal (Gaussiana)
        total_visits = clamp(int(random.gauss(5, 4)), 0, 60)
        total_time = clamp(int(random.gauss(450, 350)), 1, 5000)
        page_views = round(clamp(random.gauss(2.5, 1.2), 0.1, 20.0), 2)

        # Variable Target: Conversión basada en probabilidad configurada
        converted = 1 if random.random() < convert_rate else 0

        # Construcción del mensaje JSON
        msg = {
            # Metadatos del sistema para monitorización y linaje
            "event_time": iso_utc(event_time),
            "ingestion_time": iso_utc(ingestion_time),
            "source_system": source_system,
            "schema_version": schema_version,

            # Campos exactos del dataset original (CRM Raw)
            "Prospect ID": prospect_id,
            "Lead Number": lead_number,
            "Lead Origin": weighted_choice(LEAD_ORIGIN_W),
            "Lead Source": weighted_choice(LEAD_SOURCE_W),
            "Converted": converted,
            "TotalVisits": total_visits,
            "Total Time Spent on Website": total_time,
            "Page Views Per Visit": page_views,
            "Last Activity": weighted_choice(LAST_ACTIVITY_W),
            "Country": weighted_choice(COUNTRY_W),
            "What is your current occupation": weighted_choice(OCCUPATION_W),
            "City": weighted_choice(CITY_W),
            "Tags": random.choice(["Will revert after reading the email", "Ringing", "Already a student", "Unknown"]),
            "A free copy of Mastering The Interview": random.choice(["True", "False"]),
            "Last Notable Activity": weighted_choice(LAST_ACTIVITY_W),
            "Asymmetrique Activity Index": weighted_choice(ASYM_INDEX_W),
            "Asymmetrique Profile Index": weighted_choice(ASYM_INDEX_W),
            "Asymmetrique Activity Score": random.randint(10, 18),
            "Asymmetrique Profile Score": random.randint(12, 20),
        }

    # ---- 5. Envío y Cierre ----
        producer.send(topic, key=lead_number, value=msg)

        # Control de velocidad y frecuencia de volcado a Kafka
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

        if (i + 1) % flush_every == 0:
            producer.flush()
            print(f"[producer] sent {i+1}/{rows}")

    producer.flush()
    producer.close()
    print(f"[producer] Proceso finalizado. Topic: '{topic}', Filas: {rows}")


if __name__ == "__main__":
    main()
