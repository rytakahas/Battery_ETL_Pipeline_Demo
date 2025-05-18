{{ config(materialized='table') }}

SELECT
  *,
  CAST("voltage(v)" AS DOUBLE) * CAST("current(a)" AS DOUBLE) AS power_watt,
  CAST("charge_capacity(ah)" AS DOUBLE) + CAST("discharge_capacity(ah)" AS DOUBLE) AS total_capacity
FROM {{ ref('battery_cleaned') }}

