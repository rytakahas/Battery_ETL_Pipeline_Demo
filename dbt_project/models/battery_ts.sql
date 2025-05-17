-- battery_ts.sql (raw base, materialized as a view)
{{ config(materialized='view') }}

SELECT * FROM battery_ts_cleaned

