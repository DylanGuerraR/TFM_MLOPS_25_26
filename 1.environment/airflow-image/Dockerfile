# Fichero: airflow-image/Dockerfile
# Propósito: Imagen personalizada de Airflow con soporte para 'Docker in Docker' (DinD) 
# y Docker Compose, permitiendo que Airflow orquestre el resto de contenedores del TFM.

# Base oficial de Apache Airflow
FROM apache/airflow:2.8.4

# Cambiamos a root para instalar paquetes de sistema y dependencias de Docker
USER root

# Instalación de Docker CLI y Docker Compose Plugin
# Esto permite que los BashOperators de Airflow ejecuten comandos 'docker' y 'docker compose'
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg lsb-release \
    && install -m 0755 -d /etc/apt/keyrings \
    # Añadir llave GPG oficial de Docker
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    # Configurar el repositorio oficial de Docker para Debian
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
    > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    # Instalar exclusivamente el cliente y el plugin de compose (sin el motor/daemon)
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Devolvemos el control al usuario 'airflow' por razones de seguridad
USER airflow
