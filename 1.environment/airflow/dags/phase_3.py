"""
DAG: phase3_mlflow_training
Descripción: Tercera fase del pipeline. Automatiza el entrenamiento del modelo, registro en MLflow y visualización en Superset.

Proceso:
  1. Valida que el código existe.
  2. Asegura rutas necesarias en el datalake para MLflow (SQLite + artifacts).
  3. Construye la imagen de entrenamiento.
  4. Levanta MLflow server y ejecuta entrenamiento.
  5. Exporta métricas desde MLflow hacia DuckDB.
  6. Verifica que existe main.v_dashboard_model_metrics en DuckDB (si no, falla).
  7. Levanta Superset, hace superset-init y aplica assets (apply_assets.py).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_DIR = "/opt/airflow/repo"
PROJECT_NAME = "tfm"

default_args = {"owner": "tfm", "retries": 0}

with DAG(
    dag_id="phase3_mlflow_training",
    default_args=default_args,
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    tags=["tfm", "phase3", "mlflow", "xgboost"],
) as dag:

    check_repo = BashOperator(
        task_id="check_repo",
        bash_command=f"""
        set -euo pipefail
        ls -l {REPO_DIR}/4.MLflow/train_xgboost.py
        ls -l {REPO_DIR}/4.MLflow/export_metrics.py
        ls -l {REPO_DIR}/5.Vizz/superset_as_code/apply_assets.py
        ls -l {REPO_DIR}/5.Vizz/superset_as_code/verify_duckdb_view.py
        """,
    )

    ensure_mlflow_paths = BashOperator(
        task_id="ensure_mlflow_paths",
        bash_command="""
        set -euo pipefail
        docker run --rm -v tfm_datalake:/opt/datalake alpine:3.20 sh -lc '
          mkdir -p /opt/datalake/4.MLflow/mlflow_artifacts &&
          chmod -R 777 /opt/datalake/4.MLflow
        '
        """,
    )

    build_training_image = BashOperator(
        task_id="build_training_image",
        bash_command=f"""
        set -euo pipefail
        cd {REPO_DIR}
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.mlflow.yml build train-xgboost
        """,
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command=f"""
        set -euo pipefail
        cd {REPO_DIR}

        echo "[train_model] up mlflow-server"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.mlflow.yml up -d mlflow-server

        echo "[train_model] run training"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.duckdb.yml -f compose.mlflow.yml \
          run --rm train-xgboost python train_xgboost.py
        """,
    )

    export_metrics_to_duckdb = BashOperator(
        task_id="export_metrics_to_duckdb",
        bash_command=f"""
        set -euo pipefail
        cd {REPO_DIR}

        echo "[export_metrics_to_duckdb] export metrics -> DuckDB"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.duckdb.yml -f compose.mlflow.yml \
          run --rm train-xgboost python export_metrics.py
        """,
    )

    verify_duckdb_view = BashOperator(
        task_id="verify_duckdb_view",
        bash_command=f"""
        set -euo pipefail
        cd {REPO_DIR}

        echo "[verify_duckdb_view] verify DuckDB file exists (volume)"
        docker run --rm -v tfm_datalake:/opt/datalake alpine:3.20 sh -lc '
          test -f /opt/datalake/warehouse/tfm.duckdb || (echo "ERROR: /opt/datalake/warehouse/tfm.duckdb missing" && exit 1)
          ls -lah /opt/datalake/warehouse/tfm.duckdb
        '

        echo "[verify_duckdb_view] python check using superset image (no heredocs)"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.superset.yml \
          run --rm --no-deps --entrypoint python3 superset /app/superset_as_code/verify_duckdb_view.py
        """,
    )

    apply_superset_assets = BashOperator(
        task_id="apply_superset_assets",
        bash_command=f"""
        set -euo pipefail
        cd {REPO_DIR}

        echo "[apply_superset_assets] up superset deps"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.superset.yml up -d \
          superset-db superset-redis superset-datalake-perms

        echo "[apply_superset_assets] run superset-init (db upgrade + admin + init)"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.superset.yml up \
          --abort-on-container-exit --exit-code-from superset-init superset-init

        echo "[apply_superset_assets] apply assets (db/datasets + import dashboards zip)"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.superset.yml \
          run --rm --no-deps --entrypoint python3 superset /app/superset_as_code/apply_assets.py

        echo "[apply_superset_assets] ensure superset web up"
        docker compose -p {PROJECT_NAME} -f compose.base.yml -f compose.superset.yml up -d superset
        """,
    )

    (
        check_repo
        >> ensure_mlflow_paths
        >> build_training_image
        >> train_model
        >> export_metrics_to_duckdb
        >> verify_duckdb_view
        >> apply_superset_assets
    )
