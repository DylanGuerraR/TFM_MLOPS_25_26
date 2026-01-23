{{ config(materialized='table') }}

with base as (
  select
    lead_number,
    converted,
    total_visits,
    total_time_spent_on_website,
    page_views_per_visit,
    average_time_per_visit,
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

    coalesce(total_time_spent_on_website, avg(total_time_spent_on_website) over ()) as total_time_spent_on_website,
    coalesce(page_views_per_visit, avg(page_views_per_visit) over ())               as page_views_per_visit,
    coalesce(total_visits, avg(total_visits) over ())                               as total_visits,
    coalesce(average_time_per_visit, avg(average_time_per_visit) over ())           as average_time_per_visit,

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
    *,
    case
      when lower(coalesce(free_copy_mastering_interview, '')) = 'true'  then 1
      when lower(coalesce(free_copy_mastering_interview, '')) = 'false' then 0
      else null
    end as free_copy_bin,

    -- tags bucket para que exista "Other Tags" siempre igual que en training
    case
      when tags is null then 'Unknown'
      when tags in (
        'Already a student',
        'Closed by Horizzon',
        'Interested in other courses',
        'Ringing',
        'Will revert after reading the email',
        'Unknown'
      ) then tags
      else 'Other Tags'
    end as tags_bucket
  from clean
)

select
  -- OJO: nombres EXACTOS como en tu notebook (con espacios). Por eso van entre comillas.
  lead_number as "Lead Number",
  total_time_spent_on_website as "Total Time Spent on Website",
  page_views_per_visit as "Page Views Per Visit",
  free_copy_bin as "A free copy of Mastering The Interview",
  total_visits as "TotalVisits",
  average_time_per_visit as "Average Time Per Visit",

  -- dummies EXACTOS del entrenamiento
  case when last_activity = 'SMS Sent' then 1 else 0 end as "Last Activity_SMS Sent",

  case when current_occupation = 'Unemployed' then 1 else 0 end as "What is your current occupation_Unemployed",
  case when current_occupation = 'Unknown' then 1 else 0 end as "What is your current occupation_Unknown",
  case when current_occupation = 'Working Professional' then 1 else 0 end as "What is your current occupation_Working Professional",

  case when tags_bucket = 'Already a student' then 1 else 0 end as "Tags_Already a student",
  case when tags_bucket = 'Closed by Horizzon' then 1 else 0 end as "Tags_Closed by Horizzon",
  case when tags_bucket = 'Interested in other courses' then 1 else 0 end as "Tags_Interested in other courses",
  case when tags_bucket = 'Other Tags' then 1 else 0 end as "Tags_Other Tags",
  case when tags_bucket = 'Ringing' then 1 else 0 end as "Tags_Ringing",
  case when tags_bucket = 'Unknown' then 1 else 0 end as "Tags_Unknown",
  case when tags_bucket = 'Will revert after reading the email' then 1 else 0 end as "Tags_Will revert after reading the email",

  case when city = 'Mumbai' then 1 else 0 end as "City_Mumbai",

  case when last_notable_activity = 'Modified' then 1 else 0 end as "Last Notable Activity_Modified",
  case when last_notable_activity = 'SMS Sent' then 1 else 0 end as "Last Notable Activity_SMS Sent",

  -- target (si quieres entrenar desde aquí)
  converted as "Converted"

from bucketed

-- notebook: drop rows con Lead Source null (aquí no lo tenemos en gold_features, pero puedes filtrar si quieres)
-- where ...
