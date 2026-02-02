from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Dentro del contenedor Airflow, tu repo está aquí (según compose.airflow.yml)
REPO_DIR = "/opt/airflow/repo"

# Usa rutas ABSOLUTAS para que no dependa del working dir (/tmp/...)
DBT_COMPOSE = (
    f"docker compose -p tfm "
    f"-f {REPO_DIR}/compose.base.yml "
    f"-f {REPO_DIR}/compose.dbt.yml"
)

DUCKDB_IMAGE = "duckdb/duckdb:latest"
ALPINE_IMAGE = "alpine:3.20"
DATALAKE_VOL = "tfm_datalake"
WAREHOUSE_DB = "/opt/datalake/warehouse/tfm.duckdb"

default_args = {"owner": "tfm", "retries": 0}

with DAG(
    dag_id="phase2_dbt_silver_gold_export",
    default_args=default_args,
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    tags=["tfm", "phase2", "dbt", "duckdb"],
) as dag:

    repo_sanity_check = BashOperator(
        task_id="repo_sanity_check",
        bash_command=f"""
        set -e
        test -f {REPO_DIR}/compose.base.yml
        test -f {REPO_DIR}/compose.dbt.yml
        test -d {REPO_DIR}/dbt
        echo "OK: repo + compose files found in {REPO_DIR}"
        """,
    )

    warehouse_init = BashOperator(
        task_id="warehouse_init",
        bash_command=r"""
        set -e
        docker run --rm -v {{ params.datalake_vol }}:/opt/datalake {{ params.alpine }} \
          mkdir -p /opt/datalake/warehouse
        """,
        params={"datalake_vol": DATALAKE_VOL, "alpine": ALPINE_IMAGE},
    )

    DBT_IMAGE = "tfm-dbt:latest"

    dbt_image_build = BashOperator(
        task_id="dbt_image_build",
        bash_command=f"""
        set -e
        cd {REPO_DIR}
        docker build -t {DBT_IMAGE} -f {REPO_DIR}/dbt-image/Dockerfile {REPO_DIR}
        """,
    )

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

    dbt_run_gold_features = BashOperator(
        task_id="dbt_run_gold_features",
        bash_command=f"""
        set -e
        docker run --rm \
          -v {DATALAKE_VOL}:/opt/datalake \
          {DBT_IMAGE} \
          dbt run -s leads_gold_features --full-refresh --vars '{{start_date: "{{{{ macros.ds_add(ds, -365) }}}}", end_date: "{{{{ ds }}}}"}}'
        """,
    )

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
            COPY (
              SELECT *, DATE '{{{{ ds }}}}' AS ingestion_date
              FROM leads_gold_features
            ) TO '${{OUTDIR}}/part-00000.parquet'
            (FORMAT PARQUET);
          "
        """,
    )

    gold_parquet_validate = BashOperator(
        task_id="gold_parquet_validate",
        bash_command=f"""
        set -e
        sleep 2
        docker run --rm -v {DATALAKE_VOL}:/opt/datalake {DUCKDB_IMAGE} \
          duckdb -c "
            SELECT count(*) AS n
            FROM read_parquet('/opt/datalake/gold/leads_gold_features/ingestion_date={{{{ ds }}}}/part-*.parquet');
          "
        """,
    )

    (
        repo_sanity_check
        >> warehouse_init
        >> dbt_image_build
        >> dbt_run_silver
        >> dbt_run_gold_features
        >> duckdb_counts
        >> gold_export_parquet
        >> gold_parquet_validate
    )
