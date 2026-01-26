import mlflow
import mlflow.xgboost
import xgboost as xgb
import duckdb
import os
import numpy as np

def train():
    # ---- 1. Configuración de Rutas y MLflow ----
    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/datalake/warehouse/tfm.duckdb")
    TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
    EXPERIMENT_NAME = "XGBoost_Lead_Conversion"

    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    # Habilitamos el autologging para capturar parámetros y métricas automáticamente
    mlflow.xgboost.autolog()

    # ---- 2. Carga de Datos desde DuckDB (Capa Gold) ----
    print(f"Connecting to DuckDB at: {DUCKDB_PATH}")
    con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
    
    # Obtenemos los datos de la tabla gold_features (ya preparada por dbt)
    data_dict = con.execute("SELECT * FROM leads_gold_features").fetchnumpy()
    con.close()

    print(f"Columns found in DuckDB: {list(data_dict.keys())}")

    # Separar features y target
    y = data_dict["Converted"]
    # Si y es un MaskedArray, lo convertimos a array normal (asumimos no hay nulos en target)
    if hasattr(y, 'filled'):
        y = y.filled(0)
    feature_cols = [col for col in data_dict.keys() if col not in ["Lead Number", "Converted"]]
    print(f"Features used for training: {feature_cols}")

    # Construimos la matriz X manejando posibles MaskedArrays de fetchnumpy()
    X_list = []
    print("\n--- Feature Diagnostics ---")
    for col in feature_cols:
        arr = data_dict[col]
        if hasattr(arr, 'filled'):
            arr = arr.filled(np.nan if arr.dtype.kind in 'fc' else 0)
        
        # Estadísticas básicas
        f_min, f_max = np.nanmin(arr), np.nanmax(arr)
        f_mean = np.nanmean(arr)
        f_std = np.nanstd(arr)
        unique_vals = len(np.unique(arr[~np.isnan(arr)]))
        
        print(f"Feature: {col}")
        print(f"  Range: [{f_min}, {f_max}], Mean: {f_mean:.4f}, Std: {f_std:.4f}, Unique: {unique_vals}")
        
        if unique_vals <= 1:
            print(f"  WARNING: Feature '{col}' has NO variance!")
            
        X_list.append(arr)
    
    X = np.column_stack(X_list).astype(np.float32)
    y = y.astype(np.float32)

    print(f"\nFinal X shape: {X.shape}, y shape: {y.shape}")
    print(f"Target distribution (y): {np.unique(y, return_counts=True)}")

    # ---- 3. Entrenamiento y Evaluación con MLflow ----
    with mlflow.start_run(run_name="XGBoost_Main_Run") as run:
        import matplotlib.pyplot as plt
        import matplotlib
        matplotlib.use('Agg') # Evitar que intente abrir ventanas en Docker
        
        # Mezclamos y dividimos los datos manualmente (80% train, 20% test)
        indices = np.arange(len(y))
        np.random.seed(42)
        np.random.shuffle(indices)
        
        train_size = int(0.8 * len(y))
        train_idx, test_idx = indices[:train_size], indices[train_size:]
        
        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        params = {
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "learning_rate": 0.05,
            "max_depth": 6,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "seed": 42
        }

        # DMatrix para entrenamiento y prueba
        dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=feature_cols)
        dtest = xgb.DMatrix(X_test, label=y_test, feature_names=feature_cols)

        # Entrenamiento
        model = xgb.train(
            params,
            dtrain,
            num_boost_round=100,
            evals=[(dtest, "eval")],
            early_stopping_rounds=10
        )

        # ---- 4. Cálculo de métricas y Visualizaciones ----
        preds_prob = model.predict(dtest)
        preds_bin = (preds_prob > 0.5).astype(int)

        # Matriz de confusión manual
        tp = np.sum((preds_bin == 1) & (y_test == 1))
        tn = np.sum((preds_bin == 0) & (y_test == 0))
        fp = np.sum((preds_bin == 1) & (y_test == 0))
        fn = np.sum((preds_bin == 0) & (y_test == 1))

        mlflow.log_metrics({
            "true_positives": float(tp),
            "true_negatives": float(tn),
            "false_positives": float(fp),
            "false_negatives": float(fn),
            "precision": float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0,
            "recall": float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        })

        # --- ARTEFACTO 1: Feature Importance ---
        importance_fig, ax = plt.subplots(figsize=(10, 8))
        xgb.plot_importance(model, ax=ax, max_num_features=20, height=0.5)
        plt.title("Feature Importance (Gains)")
        plt.tight_layout()
        importance_fig.savefig("feature_importance.png")
        mlflow.log_artifact("feature_importance.png")
        plt.close(importance_fig)

        # --- ARTEFACTO 2: Confusion Matrix Image ---
        cm_fig, ax = plt.subplots(figsize=(6, 5))
        cm = np.array([[tn, fp], [fn, tp]])
        im = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
        ax.figure.colorbar(im, ax=ax)
        ax.set(xticks=np.arange(cm.shape[1]), yticks=np.arange(cm.shape[0]),
               xticklabels=["Neg", "Pos"], yticklabels=["Neg", "Pos"],
               title="Confusion Matrix", ylabel="True label", xlabel="Predicted label")
        
        # Anotar los números en el gráfico
        thresh = cm.max() / 2.
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, format(cm[i, j], 'd'),
                        ha="center", va="center",
                        color="white" if cm[i, j] > thresh else "black")
        
        plt.tight_layout()
        cm_fig.savefig("confusion_matrix.png")
        mlflow.log_artifact("confusion_matrix.png")
        plt.close(cm_fig)

        # Registro del modelo
        model_uri = f"runs:/{run.info.run_id}/model"
        mlflow.register_model(model_uri, "XGBoost_Conversion_Model")
        
        print(f"Run ID: {run.info.run_id}")
        print(f"Final Metrics logged to MLflow UI.")
        print("Model trained, visualizations generated and registered successfully.")

if __name__ == "__main__":
    train()
