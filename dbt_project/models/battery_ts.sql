i-- No transformation, raw source pass-through
SELECT * FROM {{ source('motherduck', 'battery_ts') }}

