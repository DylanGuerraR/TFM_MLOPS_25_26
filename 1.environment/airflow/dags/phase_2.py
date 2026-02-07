"""
DAG: phase2_dbt_silver_gold_export
Descripción: Segunda fase del pipeline. Transforma los datos de Bronze a Silver y Gold utilizando dbt.
Tecnologías: dbt, DuckDB, Docker.
Proceso:
  1. Valida que el código de dbt esté disponible.
  2. Inicializa el almacenamiento de la base de datos DuckDB (Warehouse).
  3. Construye la imagen de dbt con los drivers necesarios.
  4. Ejecuta los modelos leads_silver (limpieza) y leads_gold_features (ingeniería de variables).
  5. Exporta los resultados finales a Parquet para consumo del modelo de ML.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Directorio de trabajo del repositorio en el contenedor Airflow
REPO_DIR = "/opt/airflow/repo"

# Definición de recursos de infraestructura
DUCKDB_IMAGE = "duckdb/duckdb:latest"
ALPINE_IMAGE = "alpine:3.20"
DATALAKE_VOL = "tfm_datalake"
WAREHOUSE_DB = "/opt/datalake/warehouse/tfm.duckdb"

# Argumentos por defecto para las tareas
default_args = {"owner": "tfm", "retries": 0}

with DAG(
    dag_id="phase2_dbt_silver_gold_export",
    default_args=default_args,
    start_date=datetime(2026, 2, 2),
    schedule=None,                   # Ejecución manual o disparada por Phase 1
    catchup=False,
    tags=["tfm", "phase2", "dbt", "duckdb"],
) as dag:

    # --- Tarea 1: repo_sanity_check ---
    # Verifica que los archivos de configuración y la carpeta de dbt existen antes de empezar.
    repo_sanity_check = BashOperator(
        task_id="repo_sanity_check",
        bash_command=f"""
        set -e
        test -f {REPO_DIR}/compose.base.yml
        test -d {REPO_DIR}/dbt
        echo "OK: Código de dbt detectado."
        """,
    )

    # --- Tarea 2: warehouse_init ---
    # Crea el directorio físico donde residirá el archivo .duckdb en el volumen compartido.
    warehouse_init = BashOperator(
        task_id="warehouse_init",
        bash_command=r"""
        set -e
        docker run --rm -v {{ params.datalake_vol }}:/opt/datalake {{ params.alpine }} \
          mkdir -p /opt/datalake/warehouse
        """,
        params={"datalake_vol": DATALAKE_VOL, "alpine": ALPINE_IMAGE},
    )

    # --- Tarea 3: dbt_image_build ---
    # Construye la imagen Docker personalizada para dbt-duckdb.
    DBT_IMAGE = "tfm-dbt:latest"
    dbt_image_build = BashOperator(
        task_id="dbt_image_build",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        docker build -t {DBT_IMAGE} -f {REPO_DIR}/dbt-image/Dockerfile {REPO_DIR}
        """,
    )

    # --- Tarea 4: dbt_run_silver ---
    # Ejecuta la transformación Silver: limpia el JSON de Bronze y genera una tabla relacional.
    # Se filtran los datos por ventana de tiempo (último año por defecto).
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"""
        set -e
        docker run --rm \
          -v {DATALAKE_VOL}:/opt/datalake \
          {DBT_IMAGE} \
          dbt run -s leads_silver --full-refresh --vars '{{start_date: "{{{{ macros.ds_add(ds, -365) }}}}", end_date: "{{{{ ds }}}}"}}'
        """,
    )

    # --- Tarea 5: dbt_run_gold_features ---
    # Genera el tablón de analítica (Gold) con las features calculadas para el modelo XGBoost.
    dbt_run_gold_features = BashOperator(
        task_id="dbt_run_gold_features",
        bash_command=f"""
        set -e
        docker run --rm \
          -v {DATALAKE_VOL}:/opt/datalake \
          {DBT_IMAGE} \
          dbt run -s leads_gold_features+ --full-refresh --vars '{{start_date: "{{{{ macros.ds_add(ds, -365) }}}}", end_date: "{{{{ ds }}}}"}}'
        """,
    )

    # --- Tarea 5.1: dbt_test ---
    # Realiza la validación de calidad de datos definida en schema.yml.
    # Comprueba nulidad, unicidad y tipos de datos en Silver y Gold.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        set -e
        docker run --rm \
          -v {DATALAKE_VOL}:/opt/datalake \
          {DBT_IMAGE} \
          dbt test --vars '{{start_date: "{{{{ macros.ds_add(ds, -365) }}}}", end_date: "{{{{ ds }}}}"}}'
        """,
    )

    # --- Tarea 6: duckdb_counts ---
    # Tarea de monitorización para mostrar en logs el número de filas procesadas.
    duckdb_counts = BashOperator(
        task_id="duckdb_counts",
        bash_command=f"""
        set -e
        sleep 2
        docker run --rm -v {DATALAKE_VOL}:/opt/datalake {DUCKDB_IMAGE} \
          duckdb -readonly {WAREHOUSE_DB} -c "
            SELECT 'leads_silver' AS table, count(*) AS n FROM leads_silver
            UNION ALL
            SELECT 'leads_gold_features', count(*) FROM leads_gold_features;
          "
        """,
    )

    # --- Tarea 7: gold_export_parquet ---
    # Copia los datos de la tabla Gold de DuckDB a un fichero Parquet físico en el Datalake.
    # Esto desacopla el entrenamiento del modelo de la base de datos activa.
    gold_export_parquet = BashOperator(
        task_id="gold_export_parquet",
        bash_command=f"""
        set -e
        OUTDIR="/opt/datalake/gold/leads_gold_features/ingestion_date={{{{ ds }}}}"
        sleep 2
        docker run --rm -v {DATALAKE_VOL}:/opt/datalake {ALPINE_IMAGE} \
          mkdir -p "${{OUTDIR}}"

        docker run --rm -v {DATALAKE_VOL}:/opt/datalake {DUCKDB_IMAGE} \
          duckdb -readonly {WAREHOUSE_DB} -c "
            COPY (SELECT *, DATE '{{{{ ds }}}}' AS ingestion_date FROM leads_gold_features) 
            TO '${{OUTDIR}}/part-00000.parquet' (FORMAT PARQUET);
          "
        """,
    )

    # --- Tarea 8: gold_parquet_validate ---
    # Valida que el fichero Parquet exportado sea legible y tenga contenido.
    gold_parquet_validate = BashOperator(
        task_id="gold_parquet_validate",
        bash_command=f"""
        set -e
        docker run --rm -v {DATALAKE_VOL}:/opt/datalake {DUCKDB_IMAGE} \
          duckdb -c "SELECT count(*) FROM read_parquet('/opt/datalake/gold/leads_gold_features/ingestion_date={{{{ ds }}}}/part-*.parquet');"
        """,
    )

    # Flujo de ejecución del dbt
    (
        repo_sanity_check
        >> warehouse_init
        >> dbt_image_build
        >> dbt_run_silver
        >> dbt_run_gold_features
        >> dbt_test
        >> duckdb_counts
        >> gold_export_parquet
        >> gold_parquet_validate
    )
