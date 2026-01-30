FROM apachesuperset.docker.scarf.sh/apache/superset:4.1.2

USER root
# Instalamos drivers para DuckDB
RUN pip install --no-cache-dir duckdb duckdb-engine

# Copiamos la configuración personalizada
COPY superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH /app/pythonpath/superset_config.py

USER superset
