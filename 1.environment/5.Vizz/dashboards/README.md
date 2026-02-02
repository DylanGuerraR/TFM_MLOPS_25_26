# Carpeta de Dashboards (Dashboard as Code)

En esta carpeta puedes guardar los archivos exportados de Apache Superset para que se carguen automáticamente al iniciar el entorno.

## Cómo usar esta carpeta:
1. Diseña tu dashboard en la interfaz de Superset (http://localhost:8088).
2. Ve a la lista de **Dashboards**.
3. Haz clic en el icono de **Exportar** (flecha hacia afuera) del dashboard que desees. Esto descargará un archivo `.zip`.
4. Copia ese archivo `.zip` en esta carpeta (`1.environment/5.Vizz/dashboards/`).
5. Reinicia o arranca el contenedor de `superset-init` desde Airflow o terminal.

El sistema detectará cualquier archivo `.zip` en esta carpeta e intentará importarlo automáticamente, incluyendo sus Charts, Datasets y la conexión a la base de datos necesaria.

> [!IMPORTANT]
> Cuando exportes el dashboard, Superset incluirá la definición de la base de datos. Asegúrate de que la conexión en Superset use la ruta de red del contenedor: `duckdb:////opt/datalake/warehouse/tfm.duckdb`.
