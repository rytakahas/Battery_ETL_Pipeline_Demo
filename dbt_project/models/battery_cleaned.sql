SELECT
    id,
    TRY_CAST(capacity AS DOUBLE) AS capacity,
    TRY_CAST(voltage AS DOUBLE) AS voltage,
    TRY_CAST(current AS DOUBLE) AS current,
    source_file,
    streamed_at
FROM {{ ref('battery_ts') }}
WHERE
    capacity IS NOT NULL AND voltage IS NOT NULL
