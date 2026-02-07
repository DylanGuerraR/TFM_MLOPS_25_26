-- models/gold/leads_gold_sin_converted.sql
-- Capa Gold: Vista para Inferencia (Puntuación de Leads).
-- Objetivo: Proporcionar exactamente la misma estructura de features que el entrenamiento,
-- pero EXCLUYENDO la columna objetivo 'Converted'.

{{ config(materialized='view') }}

select
  "Lead Number",
  "Total Time Spent on Website",
  "Page Views Per Visit",
  "TotalVisits",
  "Average Time Per Visit",

  -- Columnas binarizadas (One-Hot) heredadas de leads_gold_features
  "What is your current occupation_Unemployed",
  "What is your current occupation_Working Professional",
  "Tags_Closed by Horizzon",
  "Tags_Ringing",
  "Tags_Will revert after reading the email",
  "City_Mumbai",
  "Last Notable Activity_SMS Sent"
from {{ ref('leads_gold_features') }}
-- Excluimos 'Converted' porque en inferencia es el valor que queremos predecir.
