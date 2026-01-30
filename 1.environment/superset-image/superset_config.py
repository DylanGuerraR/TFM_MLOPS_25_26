import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY","admin1234.")

# No cargues ejemplos en un TFM (ruido)
SUPERSET_LOAD_EXAMPLES = False
