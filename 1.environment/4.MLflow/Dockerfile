# Fichero: 4.MLflow/Dockerfile
# Propósito: Entorno de ejecución para el entrenamiento de modelos de ML (XGBoost) 
# y la sincronización de métricas a DuckDB.

FROM python:3.10-slim

WORKDIR /app

# Instalamos primero los requerimientos para aprovechar la caché de capas de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código (train_xgboost.py, export_metrics.py, etc.)
COPY . .

# Comando por defecto al arrancar el contenedor: inicia el entrenamiento del modelo.
CMD ["python", "train_xgboost.py"]
