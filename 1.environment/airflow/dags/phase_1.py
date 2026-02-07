"""
DAG: phase1_kafka_to_bronze
Descripción: Primera fase del pipeline MLOps. Encargada de la ingesta de datos desde Kafka hacia la capa Bronze.
Proceso: 
  1. Inicializa infraestructura (Kafka, Spark).
  2. Prepara el sistema de archivos (Datalake).
  3. Ejecuta el generador de leads (Producer).
  4. Ejecuta el job de Spark para convertir JSON a Parquet particionado.
  5. Valida la integridad de la partición creada.
"""

from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Ruta base del repositorio dentro del contenedor de Airflow
REPO_DIR = "/opt/airflow/repo"

# Comando base de Docker Compose unificando los ficheros de infraestructura necesarios
COMPOSE = (
    f"docker compose -p tfm "
    f"-f {REPO_DIR}/compose.base.yml "
    f"-f {REPO_DIR}/compose.kafka.yml "
    f"-f {REPO_DIR}/compose.spark.yml "
    f"-f {REPO_DIR}/compose.jobs.yml "
)

with DAG(
    dag_id="phase1_kafka_to_bronze",
    description="TFM Fase 1: Kafka -> Bronze (Parquet particionado)",
    start_date=datetime(2026, 2, 2), # Fecha de inicio ajustada para el reset de hoy
    schedule="0 0 1 * *",            # Frecuencia mensual
    catchup=False,                   # Evita ejecuciones retroactivas automáticas
    max_active_runs=1,
    default_args={
        "retries": 1,
    },
    # Parámetros por defecto para la ejecución del generador de datos
    params={
        "rows": "1000",
        "days": "7",
        "convert_rate": "0.35",
        "seed": "42",
        "topic": "leads_raw",
    },
) as dag:

    # --- Tarea 0: infra_up ---
    # Asegura que brokers de Kafka y cluster Spark estén levantados antes de proceder.
    infra_up = BashOperator(
        task_id="infra_up",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} up -d controller-1 controller-2 controller-3 broker-1 broker-2 broker-3 kafka-ui spark-master spark-worker
        """,
    )

    # --- Tarea 1: datalake_init ---
    # Crea la estructura de directorios en el volumen 'tfm_datalake' (/opt/datalake/bronze, etc.)
    datalake_init = BashOperator(
        task_id="datalake_init",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} run --rm --no-deps datalake-init
        """,
    )

    # --- Tarea 2: topic_init ---
    # Crea el topic de Kafka 'leads_raw' si no existe, garantizando que el Producer pueda enviar datos.
    topic_init = BashOperator(
        task_id="topic_init",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} run --rm --no-deps topic-init
        """,
    )

    # --- Tarea 3: run_producer ---
    # Lanza el script Python (producer.py) que genera leads aleatorios y los envía a Kafka.
    # Pasamos {{ ds }} (fecha lógica de Airflow) para que los datos coincidan con el periodo de ejecución.
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} run --rm --no-deps \
          -e KAFKA_TOPIC="{{{{ params.topic }}}}" \
          -e ROWS="{{{{ params.rows }}}}" \
          -e DAYS="{{{{ params.days }}}}" \
          -e CONVERT_RATE="{{{{ params.convert_rate }}}}" \
          -e SEED="{{{{ params.seed }}}}" \
          -e START_DATE="{{{{ ds }}}}" \
          -e INGESTION_DATE="{{{{ ds }}}}" \
          producer
        """,
    )

    # --- Tarea 4: spark_to_bronze ---
    # Lanza el job Spark (kafka_to_bronze.py) que lee de Kafka y escribe en Parquet en el Datalake.
    # Usamos INGESTION_DATE_OVERRIDE para que la partición sea exactamente la fecha lógica del DAG.
    spark_to_bronze = BashOperator(
        task_id="spark_to_bronze",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} run --rm --no-deps -e INGESTION_DATE_OVERRIDE="{{{{ ds }}}}" spark-kafka-to-bronze
        """,
    )

    # --- Tarea 5: validate_bronze_partition ---
    # Verifica que la carpeta de la partición existe y contiene ficheros Parquet con datos legibles.
    # Ejecuta comandos dentro del contenedor spark-master para acceder directamente al sistema de archivos.
    validate_bronze = BashOperator(
        task_id="validate_bronze_partition",
        bash_command=r"""
        set -e
        PART="/opt/datalake/bronze/leads_raw/ingestion_date={{ ds }}"
        echo "Checking partition: ${PART}"

        # 1) Comprobar existencia física del directorio de partición
        docker exec spark-master bash -lc "test -d '${PART}'"

        # 2) Mostrar muestra de ficheros y conteo
        echo "Parquet file count:"
        docker exec spark-master bash -lc "find '${PART}' -name '*.parquet' -type f | wc -l"

        # 3) Prueba de lectura: Spark Shell lee la partición y muestra el esquema
        echo "Spark read check:"
        docker exec -e PART_PATH="${PART}" spark-master bash -lc '
        /opt/bitnami/spark/bin/spark-shell << "SCALA"
        val path = sys.env("PART_PATH")
        val df = spark.read.parquet(path)
        println(s"RESULTADO: Se han encontrado ${df.count()} filas en la particion.")
        df.printSchema()
        System.exit(0)
    SCALA
        '
        """,
    )

    # --- Tarea 6: check_ingestion_parity (Senior Level Check) ---
    # Compara el número de mensajes en Kafka con el conteo de filas en Bronze.
    # Si hay una discrepancia (Data Loss), la tarea falla y detiene el flujo.
    # Usamos spark-submit para asegurar que las dependencias de PySpark y Kafka estén cargadas.
    check_parity = BashOperator(
        task_id="check_ingestion_parity",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        PART="/opt/datalake/bronze/leads_raw/ingestion_date={{{{ ds }}}}"
        docker run --rm \
          --network tfm_net \
          -v tfm_datalake:/opt/datalake \
          -e PART_PATH="${{PART}}" \
          -e KAFKA_TOPIC="{{{{ params.topic }}}}" \
          -e KAFKA_BOOTSTRAP_SERVERS="broker-1:29092,broker-2:29093,broker-3:29094" \
          tfm-spark-kafka-to-bronze:3.5.0 \
          /opt/bitnami/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /opt/spark-app/check_parity.py
        """,
    )

    # Definición de dependencias secuenciales
    infra_up >> datalake_init >> topic_init >> run_producer >> spark_to_bronze >> validate_bronze >> check_parity
