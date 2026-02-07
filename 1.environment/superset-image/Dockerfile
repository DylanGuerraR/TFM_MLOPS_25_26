# Propósito: Imagen personalizada de Apache Superset con soporte para DuckDB.

FROM apachesuperset.docker.scarf.sh/apache/superset:4.1.2

USER root

# 1) Paquetes del sistema para automatizar prompts (expect)
RUN apt-get update && apt-get install -y --no-install-recommends \
    expect \
  && rm -rf /var/lib/apt/lists/*

# 2) Drivers Python para DuckDB + Postgres
RUN pip install --no-cache-dir duckdb duckdb-engine psycopg2-binary pyyaml

# 3) Config custom
COPY superset-image/superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
COPY 4.MLflow/verify_duckdb_view.py /app/scripts/verify_duckdb_view.py
RUN chmod +x /app/scripts/verify_duckdb_view.py

# 4) Assets dentro de la imagen 
COPY 5.Vizz/superset_as_code /app/superset_as_code
COPY 5.Vizz/dashboards /app/dashboards

USER superset
