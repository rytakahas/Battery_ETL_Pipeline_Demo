{{ config(materialized='table') }}

SELECT
  *
FROM battery_ts  -- <-- referencing physical table, NOT ref('battery_ts')
WHERE 
  "current(a)" IS NOT NULL
  AND "voltage(v)" IS NOT NULL

