# Fichero: dbt-image/Dockerfile
# Propósito: Imagen personalizada para ejecutar dbt con soporte nativo para DuckDB.

# Base ligera de Python 3.11
FROM python:3.11-slim

# Optimización de Python para contenedores
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Instalación de dependencias del sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalación de dbt y el adaptador específico para DuckDB
# Se fija la versión para evitar incompatibilidades con DuckDB 1.x
RUN pip install --no-cache-dir "dbt-duckdb>=1.7,<2.0"

WORKDIR /usr/app

# Incorporamos el código de dbt y el perfil directamente en la imagen.
# Esto evita fallos al montar volúmenes en entornos Docker-in-Docker.
COPY dbt /usr/app

# Se define la ruta de perfiles para que dbt encuentre el profiles.yml automáticamente
ENV DBT_PROFILES_DIR=/usr/app

# Instalamos las dependencias internas de dbt (si las hay)
RUN dbt deps || true
