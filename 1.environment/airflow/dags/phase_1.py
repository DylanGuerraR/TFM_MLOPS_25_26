from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_DIR = "/opt/airflow/repo"


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
    start_date=datetime(2026, 1, 1),
    schedule="0 0 1 * *",  # dia 1 de cada mes
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
    },
    params={
        "rows": "1000",
        "days": "7",
        "convert_rate": "0.35",
        "seed": "42",
        "topic": "leads_raw",
    },
) as dag:

    # 0) opcional: levantar infraestructura si no la dejas always-on
    infra_up = BashOperator(
        task_id="infra_up",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} up -d controller-1 controller-2 controller-3 broker-1 broker-2 broker-3 kafka-ui spark-master spark-worker
        """,
    )

    # 1) preparar carpetas /opt/datalake/bronze y checkpoints (service datalake-init)
    datalake_init = BashOperator(
        task_id="datalake_init",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} run --rm --no-deps datalake-init
        """,
    )

    # 2) crear topic leads_raw si no existe (service topic-init ya es idempotente)
    topic_init = BashOperator(
        task_id="topic_init",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} run --rm --no-deps topic-init
        """,
    )

    # 3) producer (genera mensajes y termina: restart "no")
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
          producer
        """,
    )

    # 4) spark job kafka -> bronze
    # requiere que kafka_to_bronze.py termine (trigger availableNow)
    spark_to_bronze = BashOperator(
        task_id="spark_to_bronze",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        {COMPOSE} run --rm --no-deps spark-kafka-to-bronze
        """,
    )

    # 5) validación: existen ficheros parquet en ingestion_date={{ ds }}
    # (usa spark-master porque tiene montado el volumen datalake) :contentReference[oaicite:5]{index=5}
    validate_bronze = BashOperator(
        task_id="validate_bronze_partition",
        bash_command=r"""
        set -e
        PART="/opt/datalake/bronze/leads_raw/ingestion_date={{ ds }}"
        echo "Checking partition: ${PART}"
        docker exec spark-master bash -lc "test -d '${PART}'"
        docker exec spark-master bash -lc "find '${PART}' -type f | wc -l"
        """,
    )

    infra_up >> datalake_init >> topic_init >> run_producer >> spark_to_bronze >> validate_bronze
