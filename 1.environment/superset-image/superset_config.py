import os

# Fichero: superset_config.py
# Propósito: Configuración mínima para el entorno de TFM.

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "admin1234.")
SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")

# Permitir campos SQL personalizados en dashboards (necesario para dashboards importados)
PREVENT_UNSAFE_DB_CONNECTIONS = False
ALLOW_ADHOC_SUBQUERY = True

# Para no cargar ejemplos en el TFM (evitar ruido)
SUPERSET_LOAD_EXAMPLES = False
