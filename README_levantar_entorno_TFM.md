# TFM_MLOPS_25_26 – Guía de levantamiento controlado (Local)

## 0) Descripción del proyecto
Este repositorio implementa una arquitectura MLOps reproducible en local orientada a reentrenamiento. El flujo simula un escenario “production-like” con un Data Lake por capas (Bronze/Silver/Gold en Parquet), orquestado con Airflow, trazabilidad de entrenamiento con MLflow y explotación de resultados con Superset sobre un warehouse local (DuckDB).

**Fases (DAGs):**
1. Phase 1: Producer → Kafka → Spark → Bronze  
2. Phase 2: dbt + DuckDB → Silver/Gold + export Parquet  
3. Phase 3: Train XGBoost + MLflow → export métricas a DuckDB → dashboards en Superset

---

## 1) Requisitos previos
- Docker Desktop instalado y **en ejecución**
- Si usas Windows + WSL2: WSL integration activada para tu distro
- Verifica:

```bash
docker version
docker compose version
```

---

## 2) Configuración de entorno (.env) – IMPORTANTE
Docker Compose carga automáticamente el fichero `.env` si está en el mismo directorio que los `compose.*.yml`.

### 2.1 Crear `.env` (recomendado)
El repositorio debe incluir un `.env.example` (plantilla). Cada usuario debe crear su propio `.env` local (no se versiona).

```bash
cp .env.example .env
```

### 2.2 Configurar DOCKER_GID automáticamente (obligatorio para Airflow)
Airflow ejecuta comandos Docker desde dentro del contenedor (Docker-out-of-Docker).  
Para que pueda comunicarse con el Docker daemon del host mediante `/var/run/docker.sock`, el contenedor debe pertenecer al **grupo propietario** del socket.

Ejecuta desde `1.environment/`:

```bash
DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
echo "Detected DOCKER_GID=${DOCKER_GID}"
grep -q '^DOCKER_GID=' .env && sed -i "s/^DOCKER_GID=.*/DOCKER_GID=${DOCKER_GID}/" .env || echo "DOCKER_GID=${DOCKER_GID}" >> .env
```

Verificación:

```bash
grep '^DOCKER_GID=' .env
stat -c '%g %a %n' /var/run/docker.sock
```

> Nota técnica: en `compose.airflow.yml` el campo `group_add` debe ser una **lista**:
> ```yaml
> group_add:
>   - "${DOCKER_GID}"
> ```

---

## 3) Preparar Spark (pull + tag)
El proyecto usa `bitnamilegacy/spark:3.5.0`. Para garantizar compatibilidad se descarga desde ECR público y se taggea:

```bash
docker pull public.ecr.aws/bitnami/spark:3.5.0
docker tag public.ecr.aws/bitnami/spark:3.5.0 bitnamilegacy/spark:3.5.0
```

Verificación:

```bash
docker images | grep -E "bitnami|bitnamilegacy" | grep spark | head
```

---

## 4) Descargar imágenes de terceros (infra)
> `TAG` se lee desde `.env`.

```bash
docker pull confluentinc/cp-kafka:${TAG}
docker pull provectuslabs/kafka-ui:latest
docker pull postgres:15
docker pull redis:7
docker pull alpine:3.20
docker pull duckdb/duckdb:latest
docker pull ghcr.io/mlflow/mlflow:v2.10.2
```

---

## 5) Construir TODAS las imágenes del proyecto (una sola vez, sin cache)
Este paso deja todo preparado **antes** de ejecutar los DAGs (evita builds durante runtime y reduce fallos por dependencias/healthchecks).

```bash
docker compose -p tfm \
  -f compose.base.yml \
  -f compose.kafka.yml \
  -f compose.spark.yml \
  -f compose.jobs.yml \
  -f compose.dbt.yml \
  -f compose.duckdb.yml \
  -f compose.mlflow.yml \
  -f compose.superset.yml \
  -f compose.airflow.yml \
  build --no-cache
```

Comprobación (opcional):

```bash
docker images | grep -i tfm | head -n 30
```

---

## 6) Levantar TODO el entorno (sin pulls inesperados)
Con todo ya descargado y construido:

```bash
docker compose -p tfm \
  -f compose.base.yml \
  -f compose.kafka.yml \
  -f compose.spark.yml \
  -f compose.jobs.yml \
  -f compose.dbt.yml \
  -f compose.duckdb.yml \
  -f compose.mlflow.yml \
  -f compose.superset.yml \
  -f compose.airflow.yml \
  up -d --pull never
```

---

## 7) Comprobación rápida
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | sed -n '1,50p'
```

Puertos esperados (pueden variar si se han modificado en compose):
- Airflow UI: http://localhost:8080  
- Kafka UI: http://localhost:8085  
- Spark Master UI: http://localhost:8086  
- Spark Worker UI: http://localhost:8087  
- MLflow UI: http://localhost:5000  
- Superset UI: http://localhost:8088  

---

## 8) Validación de permisos Docker desde Airflow (recomendado)
Si Airflow no puede ejecutar Docker, los DAGs fallarán con `permission denied` sobre `/var/run/docker.sock`.

```bash
docker exec -it airflow-scheduler bash -lc 'id && docker ps'
```

Si aparece `permission denied`, revisa:
- que `.env` tenga `DOCKER_GID` correcto
- que `compose.airflow.yml` use `group_add: ["${DOCKER_GID}"]`
- y reinicia Airflow

---

## 9) Ejecutar los DAGs (orden recomendado)
En Airflow (UI):
1) `phase1_kafka_to_bronze`  
2) `phase2_dbt_silver_gold_export`  
3) `phase3_mlflow_training`

---

## 10) Parar el entorno (sin borrar datos)
```bash
docker compose -p tfm \
  -f compose.base.yml \
  -f compose.kafka.yml \
  -f compose.spark.yml \
  -f compose.jobs.yml \
  -f compose.dbt.yml \
  -f compose.duckdb.yml \
  -f compose.mlflow.yml \
  -f compose.superset.yml \
  -f compose.airflow.yml \
  down --remove-orphans
```

---



## 12) Smoke test (confirmar que todo está OK)
Comprueba rápidamente que las UIs principales responden:

```bash
curl -sf http://localhost:8080/health >/dev/null && echo "OK Airflow" || echo "FAIL Airflow"
curl -sf http://localhost:8088/health >/dev/null && echo "OK Superset" || echo "FAIL Superset"
curl -sf http://localhost:5000 >/dev/null && echo "OK MLflow" || echo "FAIL MLflow"

docker ps --format "table {{.Names}}\t{{.Status}}"
```

---

## 13) Lanzar entrenamientos y ver cómo se acumulan

### Opción 1 (recomendada): disparar el DAG de fase 3 desde Airflow
```bash
docker exec -it airflow-webserver airflow dags trigger phase3_mlflow_training
```

Ver runs:
```bash
docker exec -it airflow-webserver airflow dags list-runs -d phase3_mlflow_training
```

Ver tasks del DAG:
```bash
docker exec -it airflow-webserver airflow tasks list phase3_mlflow_training
```

### Opción 2: ejecutar entrenamiento directo (sin Airflow)
Entrenamiento:
```bash
docker compose -p tfm \
  -f compose.base.yml -f compose.duckdb.yml -f compose.mlflow.yml \
  run --rm train-xgboost python train_xgboost.py
```

Export de métricas a DuckDB:
```bash
docker compose -p tfm \
  -f compose.base.yml -f compose.duckdb.yml -f compose.mlflow.yml \
  run --rm train-xgboost python export_metrics.py
```

## 11) WIPE controlado del proyecto (borra solo TFM, no todo Docker)
Este wipe elimina contenedores/red/volúmenes **solo** del proyecto `tfm`.

### 11.1 Down + borrar volúmenes declarados (-v)
```bash
docker compose -p tfm \
  -f compose.base.yml \
  -f compose.kafka.yml \
  -f compose.spark.yml \
  -f compose.jobs.yml \
  -f compose.dbt.yml \
  -f compose.duckdb.yml \
  -f compose.mlflow.yml \
  -f compose.superset.yml \
  -f compose.airflow.yml \
  down --remove-orphans -v
```

### 11.2 Borrar volumen/red “por si queda algo”
```bash
docker volume rm tfm_datalake 2>/dev/null || true
docker network rm tfm_net 2>/dev/null || true
```

### 11.3 (Opcional) borrar imágenes del proyecto si quieres rebuild total
```bash
docker images | awk 'tolower($1) ~ /^tfm/ {print $3}' | xargs -r docker rmi -f
```
