FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# dbt + adapter duckdb
RUN pip install --no-cache-dir "dbt-duckdb>=1.7,<2.0"

WORKDIR /usr/app

# Copia el proyecto dbt (carpeta dbt/) dentro de la imagen
COPY dbt/ /usr/app/

# dbt necesita saber dónde están profiles.yml
ENV DBT_PROFILES_DIR=/usr/app

# si usas packages.yml (si no, no pasa nada)
RUN dbt deps || true
