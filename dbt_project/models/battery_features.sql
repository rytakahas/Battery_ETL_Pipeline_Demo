iSELECT
    *,
    capacity / voltage AS cap_to_volt_ratio,
    current * voltage AS power
FROM {{ ref('battery_cleaned') }}
