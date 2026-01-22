-- models/silver/leads_silver.sql
-- Silver = parse + limpieza + deduplicación (lead_number) + feature avg_time_per_visit

WITH bronze AS (
  SELECT
    kafka_key,
    raw_json,
    kafka_timestamp,
    ingestion_date
  FROM read_parquet('/opt/datalake/bronze/leads_raw/ingestion_date=*/part-*.parquet')
),

parsed AS (
  SELECT
    -- metadatos del pipeline
    kafka_key,
    kafka_timestamp,
    ingestion_date,

    -- metadatos dentro del JSON
    CAST(json_extract_string(raw_json, '$.event_time') AS TIMESTAMPTZ)     AS event_time,
    CAST(json_extract_string(raw_json, '$.ingestion_time') AS TIMESTAMPTZ) AS ingestion_time,
    json_extract_string(raw_json, '$.source_system')                       AS source_system,
    json_extract_string(raw_json, '$.schema_version')                      AS schema_version,

    -- columnas del dataset (las que usabas en cleaned_leads)
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
    json_extract_string(raw_json, '$."A free copy of Mastering The Interview"') AS free_copy_mastering_interview,
    json_extract_string(raw_json, '$."Tags"')                              AS tags,
    json_extract_string(raw_json, '$."City"')                              AS city,

    -- ✅ renombradas para que coincidan con tu lista del TFG
    json_extract_string(raw_json, '$."Asymmetrique Activity Index"')       AS asymmetrique_activity_index,
    json_extract_string(raw_json, '$."Asymmetrique Profile Index"')        AS asymmetrique_profile_index,
    CAST(json_extract(raw_json, '$."Asymmetrique Activity Score"') AS INTEGER) AS asymmetrique_activity_score,
    CAST(json_extract(raw_json, '$."Asymmetrique Profile Score"') AS INTEGER)  AS asymmetrique_profile_score,

    json_extract_string(raw_json, '$."Last Notable Activity"')             AS last_notable_activity
  FROM bronze
),

cleaned AS (
  SELECT
    kafka_key,
    kafka_timestamp,
    ingestion_date,
    event_time,
    ingestion_time,
    source_system,
    schema_version,

    lead_number,
    lead_origin,
    lead_source,
    converted,

    COALESCE(total_visits, 0)                 AS total_visits,
    total_time_spent_on_website,
    page_views_per_visit,

    last_activity,
    COALESCE(country, 'Unknown')              AS country,
    COALESCE(specialization, 'Unknown')       AS specialization,
    current_occupation,
    free_copy_mastering_interview,
    tags,
    city,

    asymmetrique_activity_index,
    asymmetrique_profile_index,
    asymmetrique_activity_score,
    asymmetrique_profile_score,

    last_notable_activity,

    -- feature engineering (igual que en tu BigQuery)
    (total_time_spent_on_website / NULLIF(COALESCE(total_visits, 0), 0))::DOUBLE AS average_time_per_visit
  FROM parsed
  WHERE total_time_spent_on_website > 0
),

dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY lead_number
      ORDER BY
        ingestion_time DESC NULLS LAST,
        kafka_timestamp DESC NULLS LAST
    ) AS rn
  FROM cleaned
)

SELECT
  * EXCLUDE (rn)
FROM dedup
WHERE rn = 1
