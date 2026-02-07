-- models/silver/leads_silver.sql
-- Capa Silver: Refinado de datos.
-- Objetivo: Parsear el JSON de Bronze, tipar correctamente y eliminar duplicados.

WITH bronze AS (
  -- 1. Selección de datos desde la capa Bronze (Datalake) basada en ventana de tiempo
  SELECT
    kafka_key,
    raw_json,
    kafka_timestamp,
    ingestion_date
  FROM read_parquet('/opt/datalake/bronze/leads_raw/ingestion_date=*/part-*.parquet')
  WHERE ingestion_date BETWEEN
    CAST('{{ var("start_date") }}' AS DATE)
    AND CAST('{{ var("end_date") }}' AS DATE)
),

parsed AS (
  -- 2. Extracción de campos desde el JSON crudo usando DuckDB JSON Path
  SELECT
    kafka_key,
    kafka_timestamp,
    ingestion_date,

    -- Conversión a tipos de datos nativos para optimizar consultas futuras
    CAST(json_extract_string(raw_json, '$.event_time') AS TIMESTAMPTZ)     AS event_time,
    CAST(json_extract_string(raw_json, '$.ingestion_time') AS TIMESTAMPTZ) AS ingestion_time,
    json_extract_string(raw_json, '$.source_system')                       AS source_system,

    -- Mapeo de columnas del CRM (Case-Sensitive del JSON original)
    CAST(json_extract(raw_json, '$."Lead Number"') AS BIGINT)              AS lead_number,
    json_extract_string(raw_json, '$."Lead Origin"')                       AS lead_origin,
    json_extract_string(raw_json, '$."Lead Source"')                       AS lead_source,
    CAST(json_extract(raw_json, '$."Converted"') AS INTEGER)               AS converted,

    CAST(json_extract(raw_json, '$."TotalVisits"') AS INTEGER)             AS total_visits,
    CAST(json_extract(raw_json, '$."Total Time Spent on Website"') AS INTEGER) AS total_time_spent_on_website,
    CAST(json_extract(raw_json, '$."Page Views Per Visit"') AS DOUBLE)     AS page_views_per_visit,

    json_extract_string(raw_json, '$."Last Activity"')                     AS last_activity,
    json_extract_string(raw_json, '$."Country"')                           AS country,
    json_extract_string(raw_json, '$."Specialization"')                    AS specialization,
    json_extract_string(raw_json, '$."What is your current occupation"')   AS current_occupation,
    json_extract_string(raw_json, '$."City"')                             AS city,
    json_extract_string(raw_json, '$."Tags"')                             AS tags,
    json_extract_string(raw_json, '$."Last Notable Activity"')            AS last_notable_activity
  FROM bronze
),

cleaned AS (
  -- 3. Limpieza de nulos y Feature Engineering básico
  SELECT
    *,
    COALESCE(total_visits, 0) AS total_visits_filled,
    COALESCE(country, 'Unknown') AS country_filled,
    -- Cálculo de métrica clave: tiempo medio por visita
    (total_time_spent_on_website / NULLIF(COALESCE(total_visits, 0), 0))::DOUBLE AS average_time_per_visit
  FROM parsed
  -- Descartamos leads que no han pasado tiempo en la web (ruido)
  WHERE total_time_spent_on_website > 0
),

dedup AS (
  -- 4. Deduplicación por Lead Number: Nos quedamos con la versión más reciente del prospecto
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY lead_number
      ORDER BY ingestion_time DESC, kafka_timestamp DESC
    ) AS rn
  FROM cleaned
)

SELECT
  * EXCLUDE (rn)
FROM dedup
WHERE rn = 1
