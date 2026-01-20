# TFM MLOps - Fase 1: Ingesta Kafka & Spark

Este entorno despliega una infraestructura completa de MLOps local basada en Docker, incluyendo:
- **Airflow**: Orquestador de tareas.
- **Kafka + Zookeeper**: Bus de mensajería (3 brokers, 3 controllers).
- **Spark**: Motor de procesamiento distribuido (Master + Worker).
- **Data Lake (Simulado)**: Directorios locales mapeados.

## 🚀 Despliegue Rápido

### Requisitos
- Docker Desktop instalado.
- Terminal (PowerShell recomendado en Windows).

### 1. Inicialización (Primera vez)
Este comando inicializa la base de datos de Airflow y crea el usuario administrador.
*(Ejecutar desde la carpeta `1.environment`)*

```powershell
docker compose -f compose.base.yml -f compose.airflow.yml up airflow-init
```
> **Esperar** hasta ver el mensaje: `User admin created`.

### 2. Arrancar Servicios
Una vez inicializado, levanta todos los servicios (Airflow, Kafka, Spark):

```powershell
docker compose -f compose.base.yml -f compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

### 3. Ejecutar Pipeline
1. Abrir navegador en **[http://localhost:8080](http://localhost:8080)**.
2. Login: `admin` / `admin`.
3. Buscar el DAG `phase1_kafka_to_bronze`.
4. Activar el toggle (ON) y ejecutar (Trigger DAG).

---

## 🛠️ Comandos de Gestión

### Ver estado
```powershell
docker compose -f compose.base.yml -f compose.airflow.yml ps
```

### Detener entorno
```powershell
docker compose -f compose.base.yml -f compose.airflow.yml down
```

### 🧹 Limpieza Total (Reset de Fábrica)
Si necesitas reiniciar desde cero (borra datos, topics y usuarios):

```powershell
# Borrar contenedores
docker rm -f airflow-postgres airflow-init airflow-webserver airflow-scheduler topic-init producer spark-kafka-to-bronze

# Borrar volumen de datos
docker volume rm airflow_pgdata
```

## 📂 Estructura del Proyecto
- `compose.airflow.yml`: Definición de servicios de Airflow.
- `compose.kafka.yml`: Cluster de Kafka.
- `compose.spark.yml`: Cluster de Spark.
- `compose.jobs.yml`: Definición de tareas (Producer, Topic Init, Spark Job).
- `2.producer/`: Código Python del productor de datos.
- `3.Spark/`: Código PySpark y Dockerfile para el trabajo de ingesta.