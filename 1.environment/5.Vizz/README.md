# Capa de Visualización (BI) - VIZZ Dashboard

Esta carpeta contiene los activos necesarios para la capa de **Business Intelligence (BI)** del proyecto TFM. Su función principal es transformar los datos procesados en el Datalake y las métricas de MLflow en **Dashboards interactivos** para la toma de decisiones.

## ¿Qué papel cumple en este proyecto?
La carpeta `5.Vizz` representa el punto final de consumo de datos del pipeline MLOps:
1.  **Monitorización de Leads**: Visualiza la calidad de los leads entrantes (Capa Gold de DuckDB).
2.  **Performance del Modelo**: Permite comparar diferentes ejecuciones de entrenamiento (vía DuckDB Landing Table).
3.  **Dashboards as Code**: Permite que los cuadros de mando sean versionables y se desplieguen automáticamente en Apache Superset.

## Estructura de la carpeta
- `dashboards/`: Contiene ficheros `.zip` que son exportaciones oficiales de Superset. Estos archivos incluyen los Datasets, Charts y el Dashboard completo.

## Cómo se utiliza
El servicio `superset-init` (configurado en `compose.superset.yml`) escanea automáticamente esta carpeta durante el arranque e importa los dashboards directamente a la instancia de Superset sin intervención manual.

Para acceder a la visualización:
- **URL**: `http://localhost:8088`
- **Fuente de Datos**: DuckDB (`/opt/datalake/warehouse/tfm.duckdb`)
- **Tablas Principales**: `leads_gold_features` y `reporting_model_metrics`.
