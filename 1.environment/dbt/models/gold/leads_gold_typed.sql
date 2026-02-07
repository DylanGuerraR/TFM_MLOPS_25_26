-- models/gold/leads_gold_typed.sql
-- Capa Gold: Vista de datos tipados y categorizados.
-- Objetivo: Proporcionar una versión legible (no binarizada) pero ya imputada y agrupada.

{{ config(materialized='view') }}

with base as (
  -- 1. Selección de columnas de la capa Silver (limpieza inicial y deduplicación)
  select
    lead_number,
    converted,
    total_time_spent_on_website,
    page_views_per_visit,
    total_visits,
    average_time_per_visit,
    current_occupation,
    tags,
    city,
    last_notable_activity
  from {{ ref('leads_silver') }}
),

clean as (
  -- 2. Imputación de nulos numéricos usando la media aritmética global del conjunto actual
  select
    lead_number,
    converted,
    coalesce(total_time_spent_on_website, avg(total_time_spent_on_website) over ()) as total_time_spent_on_website,
    coalesce(page_views_per_visit, avg(page_views_per_visit) over ())               as page_views_per_visit,
    coalesce(total_visits, avg(total_visits) over ())                               as total_visits,
    coalesce(average_time_per_visit, avg(average_time_per_visit) over ())           as average_time_per_visit,

    -- Normalización de texto: eliminación de espacios y manejo de cadenas vacías
    nullif(trim(cast(current_occupation as varchar)), '')            as current_occupation,
    nullif(trim(cast(tags as varchar)), '')                          as tags,
    nullif(trim(cast(city as varchar)), '')                          as city,
    nullif(trim(cast(last_notable_activity as varchar)), '')         as last_notable_activity
  from base
),

bucketed as (
  -- 3. Lógica de Agrupación (Bucketing). 
  -- Agrupamos valores de baja frecuencia en categorías tipo 'Other' o 'Unknown' para ganar robustez.
  select
    lead_number,
    converted,
    total_time_spent_on_website,
    page_views_per_visit,
    total_visits,
    average_time_per_visit,

    case
      when current_occupation is null then 'Unknown'
      when current_occupation in ('Unemployed', 'Working Professional') then current_occupation
      else 'Other'
    end as current_occupation_bucket,

    case
      when tags is null then 'Unknown'
      when tags in ('Already a student', 'Closed by Horizzon', 'Interested in other courses', 'Ringing', 'Will revert after reading the email') then tags
      else 'Other Tags'
    end as tags_bucket,

    case
      when city = 'Mumbai' then 'Mumbai'
      else 'Other Cities'
    end as city_bucket,

    case
      when last_notable_activity in ('Modified', 'SMS Sent') then last_notable_activity
      else 'Other Activity'
    end as last_notable_activity_bucket
  from clean
)

select * from bucketed
