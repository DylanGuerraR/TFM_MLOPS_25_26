import sqlite3
import duckdb
import os
import datetime

# Rutas (dentro del contenedor)
MLFLOW_DB_PATH = "/opt/datalake/4.MLflow/mlflow.db"
DUCKDB_PATH = "/opt/datalake/warehouse/tfm.duckdb"

def export_metrics():
    print(f"Connecting to MLflow DB: {MLFLOW_DB_PATH}")
    if not os.path.exists(MLFLOW_DB_PATH):
        print("Error: mlflow.db not found!")
        return

    # 1. Extraer métricas de MLflow (SQLite)
    conn_mlflow = sqlite3.connect(MLFLOW_DB_PATH)
    cursor = conn_mlflow.cursor()

    query = """
    SELECT 
        r.run_uuid,
        datetime(r.start_time / 1000, 'unixepoch') as start_time,
        e.name as experiment_name,
        MAX(CASE WHEN m.key = 'eval-auc' OR m.key = 'auc' THEN m.value END) as auc,
        MAX(CASE WHEN m.key = 'precision' THEN m.value END) as precision,
        MAX(CASE WHEN m.key = 'recall' THEN m.value END) as recall,
        MAX(CASE WHEN m.key = 'true_positives' THEN m.value END) as tp,
        MAX(CASE WHEN m.key = 'true_negatives' THEN m.value END) as tn,
        MAX(CASE WHEN m.key = 'false_positives' THEN m.value END) as fp,
        MAX(CASE WHEN m.key = 'false_negatives' THEN m.value END) as fn
    FROM runs r
    JOIN experiments e ON r.experiment_id = e.experiment_id
    LEFT JOIN latest_metrics m ON r.run_uuid = m.run_uuid
    WHERE r.lifecycle_stage = 'active' AND r.status = 'FINISHED'
    GROUP BY r.run_uuid, r.start_time, e.name
    ORDER BY r.start_time DESC
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn_mlflow.close()

    if not rows:
        print("No finished runs found in MLflow.")
        return

    print(f"Found {len(rows)} runs. Syncing to DuckDB...")

    # 2. Cargar en DuckDB para Superset
    con_duck = duckdb.connect(DUCKDB_PATH)
    
    # Crear tabla de reporte si no existe
    con_duck.execute("""
    CREATE TABLE IF NOT EXISTS reporting_model_metrics (
        run_id VARCHAR PRIMARY KEY,
        run_timestamp TIMESTAMP,
        experiment_name VARCHAR,
        auc DOUBLE,
        precision DOUBLE,
        recall DOUBLE,
        tp DOUBLE,
        tn DOUBLE,
        fp DOUBLE,
        fn DOUBLE,
        sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Insertar o actualizar (upsert manual en versiones antiguas de duckdb o simple insert)
    # Para simplificar en TFM, borramos y reinsertamos lo que ha cambiado o todo si es pequeño
    # O mejor, usamos la cláusula ON CONFLICT si la versión lo soporta, o un simple delete/insert.
    
    for row in rows:
        run_id = row[0]
        # Borrar si ya existe para evitar duplicados en el reporte
        con_duck.execute("DELETE FROM reporting_model_metrics WHERE run_id = ?", [run_id])
        # Insertar
        con_duck.execute("""
            INSERT INTO reporting_model_metrics (run_id, run_timestamp, experiment_name, auc, precision, recall, tp, tn, fp, fn)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

    con_duck.close()
    
    # FIX: Ajustar permisos para que Superset (usuario no roor) pueda leer/escribir
    try:
        os.chmod(DUCKDB_PATH, 0o666)
        print(f"Permissions for {DUCKDB_PATH} set to 666.")
    except Exception as e:
        print(f"Warning: Could not set permissions on {DUCKDB_PATH}: {e}")

    print("Metrics successfully landed in DuckDB table 'reporting_model_metrics'.")

if __name__ == "__main__":
    export_metrics()
