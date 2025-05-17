{{ config(materialized='table') }}

SELECT
  *,
  "voltage(v)" * "current(a)" AS power_watt,
  "charge_capacity(ah)" + "discharge_capacity(ah)" AS total_capacity
FROM {{ ref('battery_cleaned') }}

