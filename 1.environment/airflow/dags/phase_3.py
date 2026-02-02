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
    train_model = BashOperator(
        task_id="train_model",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        # Aseguramos que el servidor de MLflow esté arriba y saludable (vía healthcheck en compose)
        docker compose -f compose.base.yml -f compose.mlflow.yml up -d mlflow-server
        
        # Ejecutamos el entrenamiento. 
        # Al usar 'run --rm' y tener 'depends_on' con 'service_healthy', 
        # docker esperará automáticamente a que mlflow esté listo.
        docker compose -f compose.base.yml -f compose.duckdb.yml -f compose.mlflow.yml run --rm train-xgboost python train_xgboost.py
        """,
    )

    # 4. Aterrizar métricas en DuckDB para Superset
    landing_to_superset = BashOperator(
        task_id="landing_to_superset",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        # Sincronizamos métricas
        docker compose -f compose.base.yml -f compose.mlflow.yml run --rm train-xgboost python export_metrics.py
        """,
    )

    # 5. Inicializar/Actualizar activos en Superset (Dashboards as Code)
    init_superset_assets = BashOperator(
        task_id="init_superset_assets",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        # 1. Levantamos en background para asegurar que el contenedor existe (dependencies, network, etc)
        docker compose -f compose.base.yml -f compose.superset.yml up -d superset-init
        
        # 2. Copiamos manualmmnete los dashboards desde el repo local (Airflow) al contenedor
        # Esto soluciona si hay un problema de "Bind Mounts" vacíos cuando se corre Docker-in-Docker
        docker cp ./5.Vizz/dashboards/. superset-init:/app/dashboards/
        
        # 3. Exportamos variables y reiniciamos attached para ver logs y resultado
        export $(grep -v '^#' .env | xargs) && docker compose -f compose.base.yml -f compose.superset.yml restart superset-init && docker logs -f superset-init
        """,
    )

    check_repo >> build_training_image >> train_model >> landing_to_superset >> init_superset_assets
