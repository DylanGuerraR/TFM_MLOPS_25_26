"""
Script: export_metrics.py
Descripción: Sincronizador de métricas entre MLflow (Tracking) y DuckDB (Reporting).
Finalidad: Permite que Apache Superset acceda a los resultados de los experimentos de ML sin conectarse directamente a SQLite.
"""

import sqlite3
import duckdb
import os
import datetime

# --- Rutas de Archivos (Mapeadas vía Volúmenes en Docker) ---
MLFLOW_DB_PATH = "/opt/datalake/4.MLflow/mlflow.db"
DUCKDB_PATH = "/opt/datalake/warehouse/tfm.duckdb"

def export_metrics():
    """
    Extrae las métricas de los 'Runs' finalizados en MLflow y las inserta en DuckDB.
    Incluye el ID del experimento, AUC, Precision, Recall y la Matriz de Confusión.
    """
    print(f"[Sync] Conectando a la base de datos de MLflow: {MLFLOW_DB_PATH}")
    
    # Verificación de pre-requisitos
    if not os.path.exists(MLFLOW_DB_PATH):
        print("[!] Error: No se encuentra mlflow.db. ¿Ha comenzado algún entrenamiento?")
        return

    # ---- 1. Extracción desde MLflow (SQLite) ----
    # Conectamos a SQLite del tracking server para leer el estado de los experimentos
    conn_mlflow = sqlite3.connect(MLFLOW_DB_PATH)
    cursor = conn_mlflow.cursor()

    # Query para pivotar las métricas de MLflow a un formato de tabla plana (Reporting ready)
    query = """
    SELECT 
        r.run_uuid,
        datetime(r.start_time / 1000, 'unixepoch') as start_time,
        e.name as experiment_name,
        MAX(CASE WHEN m.key IN ('eval-auc', 'auc') THEN m.value END) as auc,
        MAX(CASE WHEN m.key = 'precision' THEN m.value END) as precision,
        MAX(CASE WHEN m.key = 'recall' THEN m.value END) as recall,
        MAX(CASE WHEN m.key = 'true_positives' THEN m.value END) as tp,
        MAX(CASE WHEN m.key = 'true_negatives' THEN m.value END) as tn,
        MAX(CASE WHEN m.key = 'false_positives' THEN m.value END) as fp,
        MAX(CASE WHEN m.key = 'false_negatives' THEN m.value END) as fn,
        MAX(CASE WHEN m.key = 'best_iteration' THEN m.value END) as best_iter,
        MAX(CASE WHEN m.key = 'stopped_iteration' THEN m.value END) as stopped_iter
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
        print("[Sync] No se han encontrado ejecuciones finalizadas para exportar.")
        return

    print(f"[Sync] Sincronizando {len(rows)} ejecuciones hacia DuckDB...")

    # ---- 2. Carga en DuckDB (Reporting Layer) ----
    con_duck = duckdb.connect(DUCKDB_PATH)
    
    # Aseguramos la existencia de la tabla en la capa de reporte del Datalake
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
        best_iter DOUBLE,
        stopped_iter DOUBLE,
        sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    con_duck.execute("""
    CREATE OR REPLACE VIEW v_dashboard_model_metrics AS
    SELECT
    run_id,
    run_timestamp,
    experiment_name,
    auc,
    precision,
    recall,
    -- F1 derivado de precision/recall
    CASE
        WHEN precision IS NULL OR recall IS NULL OR (precision + recall) = 0 THEN NULL
        ELSE 2 * precision * recall / (precision + recall)
    END AS f1,
    -- Accuracy derivado de matriz de confusión
    CASE
        WHEN tp IS NULL OR tn IS NULL OR fp IS NULL OR fn IS NULL OR (tp + tn + fp + fn) = 0 THEN NULL
        ELSE (tp + tn) / (tp + tn + fp + fn)
    END AS accuracy,
    tp, tn, fp, fn,
    best_iter,
    stopped_iter,
    sync_timestamp
    FROM reporting_model_metrics;
    """)


    # Operación de Upsert manual: borramos el ID si ya existe y reinsertamos
    # Esto garantiza que los datos en Superset estén siempre actualizados
    for row in rows:
        run_id = row[0]
        con_duck.execute("DELETE FROM reporting_model_metrics WHERE run_id = ?", [run_id])
        con_duck.execute("""
            INSERT INTO reporting_model_metrics (run_id, run_timestamp, experiment_name, auc, precision, recall, tp, tn, fp, fn, best_iter, stopped_iter)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

    con_duck.close()
    
    # ---- 3. Ajuste de Permisos ----
    # Importante: Superset corre con un usuario no-root dentro del contenedor.
    # Debemos asegurar que el archivo de DuckDB sea legible/escribible para todos.
    try:
        os.chmod(DUCKDB_PATH, 0o666)
        print(f"[Sync] Permisos de {DUCKDB_PATH} configurados para acceso multi-usuario.")
    except Exception as e:
        print(f"[!] Warning: No se pudieron ajustar los permisos: {e}")

    print("[Sync] Proceso de sincronización completado. Datos listos en Superset.")

if __name__ == "__main__":
    export_metrics()
