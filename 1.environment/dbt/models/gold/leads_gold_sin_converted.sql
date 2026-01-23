{{ config(materialized='view') }}

select
  "Lead Number",
  "Total Time Spent on Website",
  "Page Views Per Visit",
  "A free copy of Mastering The Interview",
  "TotalVisits",
  "Average Time Per Visit",
  "Last Activity_SMS Sent",
  "What is your current occupation_Unemployed",
  "What is your current occupation_Unknown",
  "What is your current occupation_Working Professional",
  "Tags_Already a student",
  "Tags_Closed by Horizzon",
  "Tags_Interested in other courses",
  "Tags_Other Tags",
  "Tags_Ringing",
  "Tags_Unknown",
  "Tags_Will revert after reading the email",
  "City_Mumbai",
  "Last Notable Activity_Modified",
  "Last Notable Activity_SMS Sent"
from {{ ref('leads_gold_features') }}
