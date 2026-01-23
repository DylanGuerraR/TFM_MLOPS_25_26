{{ config(materialized='view') }}

with base as (
  select
    lead_number,
    converted,

    -- numéricas
    total_time_spent_on_website,
    page_views_per_visit,
    total_visits,
    average_time_per_visit,

    -- categóricas crudas (silver)
    free_copy_mastering_interview,
    last_activity,
    current_occupation,
    tags,
    city,
    last_notable_activity

  from {{ ref('leads_silver') }}
),

clean as (
  select
    lead_number,
    converted,

    -- imputación numérica simple (media global). Ajusta si quieres mediana.
    coalesce(total_time_spent_on_website, avg(total_time_spent_on_website) over ()) as total_time_spent_on_website,
    coalesce(page_views_per_visit, avg(page_views_per_visit) over ())               as page_views_per_visit,
    coalesce(total_visits, avg(total_visits) over ())                               as total_visits,
    coalesce(average_time_per_visit, avg(average_time_per_visit) over ())           as average_time_per_visit,

    -- normalización texto
    nullif(trim(cast(free_copy_mastering_interview as varchar)), '') as free_copy_mastering_interview,
    nullif(trim(cast(last_activity as varchar)), '')                 as last_activity,
    nullif(trim(cast(current_occupation as varchar)), '')            as current_occupation,
    nullif(trim(cast(tags as varchar)), '')                          as tags,
    nullif(trim(cast(city as varchar)), '')                          as city,
    nullif(trim(cast(last_notable_activity as varchar)), '')         as last_notable_activity

  from base
),

bucketed as (
  select
    lead_number,
    converted,
    total_time_spent_on_website,
    page_views_per_visit,
    total_visits,
    average_time_per_visit,

    -- binarización estable
    case
      when lower(coalesce(free_copy_mastering_interview, '')) = 'true'  then 1
      when lower(coalesce(free_copy_mastering_interview, '')) = 'false' then 0
      else null
    end as free_copy_mastering_interview_bin,

    -- BUCKETS: fija exactamente lo que usaste en training
    case
      when last_activity is null then 'Other Last Activity'
      when last_activity = 'SMS Sent' then 'SMS Sent'
      else 'Other Last Activity'
    end as last_activity_bucket,

    case
      when current_occupation is null then 'Unknown'
      when current_occupation in ('Unemployed', 'Working Professional') then current_occupation
      when current_occupation = 'Unknown' then 'Unknown'
      else 'Other'
    end as current_occupation_bucket,

    case
      when tags is null then 'Unknown'
      when tags in (
        'Already a student',
        'Closed by Horizzon',
        'Interested in other courses',
        'Ringing',
        'Will revert after reading the email'
      ) then tags
      else 'Other Tags'
    end as tags_bucket,

    case
      when city is null then 'Unknown'
      when city = 'Mumbai' then 'Mumbai'
      else 'Other Cities'
    end as city_bucket,

    case
      when last_notable_activity is null then 'Other Activity'
      when last_notable_activity in ('Modified', 'SMS Sent') then last_notable_activity
      else 'Other Activity'
    end as last_notable_activity_bucket

  from clean
)

select *
from bucketed

