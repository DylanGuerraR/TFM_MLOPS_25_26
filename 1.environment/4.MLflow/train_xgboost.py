"""
Script: train_xgboost.py
Descripción: Orquestación del entrenamiento de un modelo XGBoost para la predicción de conversiones de Leads.
Integración: Utiliza DuckDB como fuente de datos (Gold Layer) y MLflow para el seguimiento de experimentos y registro de modelos.
"""

import mlflow
import mlflow.xgboost
import xgboost as xgb
import duckdb
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

def train():
    """
    Realiza el ciclo completo de entrenamiento:
    1. Configura el tracking de MLflow.
    2. Carga datos procesados desde DuckDB.
    3. Prepara matrices X e y para XGBoost.
    4. Entrena el modelo con validación temprana y autologging.
    5. Genera visualizaciones (Matriz de Confusión e Impotancia de Variables) como artefactos.
    6. Registra el modelo en el Model Registry de MLflow.
    """
    
    # ---- 1. Configuración de Entorno y MLflow ----
    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/datalake/warehouse/tfm.duckdb")
    TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
    EXPERIMENT_NAME = "XGBoost_Lead_Conversion"

    # Conectar con el servidor de MLflow
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    # MLflow XGBoost Autologging: captura automáticamente parámetros, métricas (AUC) y el árbol del modelo.
    mlflow.xgboost.autolog()

    # ---- 2. Extracción de Datos (Capa Gold) ----
    print(f"[MLflow] Conectando a DuckDB: {DUCKDB_PATH}")
    con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
    
    # Extraemos el dataset de features generado por dbt
    data_dict = con.execute("SELECT * FROM leads_gold_features").fetchnumpy()
    con.close()

    # Separación de variable objetivo (Converted) y características
    y = data_dict["Converted"]
    # Manejo de MaskedArrays para evitar fallos si hay nulos inesperados
    if hasattr(y, 'filled'): y = y.filled(0)
    
    feature_cols = [col for col in data_dict.keys() if col not in ["Lead Number", "Converted"]]
    print(f"[MLflow] Features detectadas: {feature_cols}")

    # ---- 3. Preprocesamiento y Diagnóstico ----
    X_list = []
    print("\n--- Diagnóstico de Características ---")
    for col in feature_cols:
        arr = data_dict[col]
        # Limpieza de datos antes de entrar a la matriz: rellenar NaNs
        if hasattr(arr, 'filled'):
            arr = arr.filled(np.nan if arr.dtype.kind in 'fc' else 0)
        
        # Logs de inspección para asegurar que las features tienen varianza
        f_min, f_max = np.nanmin(arr), np.nanmax(arr)
        unique_vals = len(np.unique(arr[~np.isnan(arr)]))
        print(f" - {col}: Rango [{f_min}, {f_max}], Valores Únicos: {unique_vals}")
        
        if unique_vals <= 1:
            print(f"   [!] WARNING: La variable '{col}' es constante (sin información).")
            
        X_list.append(arr)
    
    # Unión horizontal de las columnas para formar la matriz X
    X = np.column_stack(X_list).astype(np.float32)
    y = y.astype(np.float32)

    # ---- 4. Ejecución del Experimento (MLflow Run) ----
    with mlflow.start_run(run_name="XGBoost_Lead_Scoring") as run:
        # Configurar Matplotlib para modo "headless" (sin interfaz gráfica)
        matplotlib.use('Agg')
        
        # Partición manual de datos (80% Train, 20% Test)
        indices = np.arange(len(y))
        np.random.seed(42)
        np.random.shuffle(indices)
        train_size = int(0.8 * len(y))
        
        X_train, X_test = X[indices[:train_size]], X[indices[train_size:]]
        y_train, y_test = y[indices[:train_size]], y[indices[train_size:]]

        # Hiperparámetros base de XGBoost
        params = {
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "learning_rate": 0.05,
            "max_depth": 6,
            "subsample": 0.8,
            "seed": 42
        }

        # Estructuras de datos optimizadas de XGBoost (DMatrix)
        dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=feature_cols)
        dtest = xgb.DMatrix(X_test, label=y_test, feature_names=feature_cols)

        # Entrenamiento con Early Stopping (parada temprana si el AUC no mejora)
        model = xgb.train(
            params,
            dtrain,
            num_boost_round=100,
            evals=[(dtest, "eval")],
            early_stopping_rounds=10
        )

        # ---- 5. Evaluación Detallada ----
        preds_prob = model.predict(dtest)
        preds_bin = (preds_prob > 0.5).astype(int)

        # Cálculo manual de métricas para mayor control y registro especializado
        tp = np.sum((preds_bin == 1) & (y_test == 1))
        tn = np.sum((preds_bin == 0) & (y_test == 0))
        fp = np.sum((preds_bin == 1) & (y_test == 0))
        fn = np.sum((preds_bin == 0) & (y_test == 1))

        mlflow.log_metrics({
            "precision": float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0,
            "recall": float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        })

        # --- Artefacto: Importancia de Variables ---
        importance_fig, ax = plt.subplots(figsize=(10, 8))
        xgb.plot_importance(model, ax=ax, max_num_features=20, height=0.5)
        plt.title("Importancia de Variables (XGBoost)")
        plt.tight_layout()
        importance_fig.savefig("feature_importance.png")
        mlflow.log_artifact("feature_importance.png")
        plt.close(importance_fig)

        # --- Artefacto: Matriz de Confusión ---
        cm_fig, ax = plt.subplots(figsize=(6, 5))
        cm = np.array([[tn, fp], [fn, tp]])
        im = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
        ax.set(title="Matriz de Confusión", xticklabels=["Neg", "Pos"], yticklabels=["Neg", "Pos"])
        plt.tight_layout()
        cm_fig.savefig("confusion_matrix.png")
        mlflow.log_artifact("confusion_matrix.png")
        plt.close(cm_fig)

        # Registro del modelo en el catálogo centralizado
        try:
            model_uri = f"runs:/{run.info.run_id}/model"
            mlflow.register_model(model_uri, "Lead_Scoring_Model_v1")
            print("[MLflow] Modelo registrado correctamente en el Model Registry.")
        except Exception as e:
            print(f"[MLflow] Warning: Error al registrar modelo (el tracking funcionó): {e}")
        
    print(f"[MLflow] Ejecución finalizada con éxito. Run ID: {run.info.run_id}")

if __name__ == "__main__":
    train()
