from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Repositorio dentro del contenedor de Airflow
REPO_DIR = "/opt/airflow/repo"

default_args = {"owner": "tfm", "retries": 0}

with DAG(
    dag_id="phase3_mlflow_training",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tfm", "phase3", "mlflow", "xgboost"],
) as dag:

    # 1. Verificar existencia de archivos
    check_repo = BashOperator(
        task_id="check_repo",
        bash_command=f"ls -l {REPO_DIR}/4.MLflow/train_xgboost.py",
    )

    # 2. Construir imagen de entrenamiento
    # Se añade --build para asegurar que toma el nuevo código y requerimientos (matplotlib, etc)
    build_training_image = BashOperator(
        task_id="build_training_image",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        docker compose -f compose.base.yml -f compose.mlflow.yml build train-xgboost
        """,
    )

    # 3. Ejecutar entrenamiento y registro en MLflow
    # Nota: se lanza incluyendo compose.duckdb.yml para que train-xgboost encuentre el volumen del datalake
    train_model = BashOperator(
        task_id="train_model",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        # Asegurar que el servidor de tracking esté arriba
        docker compose -f compose.base.yml -f compose.mlflow.yml up -d mlflow-server
        # Ejecutar el job de entrenamiento con acceso a DuckDB
        docker compose -f compose.base.yml -f compose.duckdb.yml -f compose.mlflow.yml up train-xgboost
        """,
    )

    check_repo >> build_training_image >> train_model
