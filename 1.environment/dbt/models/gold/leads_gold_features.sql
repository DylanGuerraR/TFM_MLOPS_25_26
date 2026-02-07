-- models/gold/leads_gold_features.sql
-- Capa Gold: Ingeniería de Atributos (Features).
-- Objetivo: Generar el tablón final con One-Hot Encoding listo para el modelo XGBoost.

{{ config(materialized='table') }}

with base as (
  -- 1. Selección de columnas necesarias de la capa Silver
  select
    lead_number,
    converted,
    total_visits,
    total_time_spent_on_website,
    page_views_per_visit,
    average_time_per_visit,
    current_occupation,
    tags,
    city,
    last_notable_activity
  from {{ ref('leads_silver') }}
),

clean as (
  -- 2. Imputación de nulos y limpieza de strings antes de categorizar
  select
    lead_number,
    converted,
    -- Imputación por la media móvil (over()) para variables numéricas detectadas como vacías
    coalesce(total_time_spent_on_website, avg(total_time_spent_on_website) over ()) as total_time_spent_on_website,
    coalesce(page_views_per_visit, avg(page_views_per_visit) over ())               as page_views_per_visit,
    coalesce(total_visits, avg(total_visits) over ())                               as total_visits,
    coalesce(average_time_per_visit, avg(average_time_per_visit) over ())           as average_time_per_visit,

    nullif(trim(cast(current_occupation as varchar)), '')            as current_occupation,
    nullif(trim(cast(tags as varchar)), '')                          as tags,
    nullif(trim(cast(city as varchar)), '')                          as city,
    nullif(trim(cast(last_notable_activity as varchar)), '')         as last_notable_activity
  from base
),

bucketed as (
  -- 3. Agrupación de categorías raras (Bucketing) para reducir dimensionalidad
  select
    *,
    case
      when tags is null then 'Unknown'
      when tags in ('Already a student', 'Closed by Horizzon', 'Interested in other courses', 'Ringing', 'Will revert after reading the email') then tags
      else 'Other Tags'
    end as tags_bucket
  from clean
)

select
  -- 4. Construcción del tablón final con Dummy Variables (One-Hot Encoding)
  -- NOTA: Se usan comillas para mantener los nombres exactos requeridos por el modelo de ML (Kaggle metadata).
  lead_number as "Lead Number",
  total_time_spent_on_website as "Total Time Spent on Website",
  page_views_per_visit as "Page Views Per Visit",
  total_visits as "TotalVisits",
  average_time_per_visit as "Average Time Per Visit",

  -- Binarización de variables categóricas
  case when current_occupation = 'Unemployed' then 1 else 0 end as "What is your current occupation_Unemployed",
  case when current_occupation = 'Working Professional' then 1 else 0 end as "What is your current occupation_Working Professional",

  case when tags_bucket = 'Closed by Horizzon' then 1 else 0 end as "Tags_Closed by Horizzon",
  case when tags_bucket = 'Ringing' then 1 else 0 end as "Tags_Ringing",
  case when tags_bucket = 'Will revert after reading the email' then 1 else 0 end as "Tags_Will revert after reading the email",

  case when city = 'Mumbai' then 1 else 0 end as "City_Mumbai",
  case when last_notable_activity = 'SMS Sent' then 1 else 0 end as "Last Notable Activity_SMS Sent",

  -- Columna objetivo (Target)
  converted as "Converted"
from bucketed
