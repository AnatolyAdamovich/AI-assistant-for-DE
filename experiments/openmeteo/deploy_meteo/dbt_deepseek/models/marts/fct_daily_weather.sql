{{ config(
    materialized='table',
    order_by='date',
    engine='MergeTree()',
    partition_by='toYYYYMM(date)'
) }}

SELECT
    dw.date,
    dw.avg_temperature,
    dw.avg_humidity,
    dw.avg_wind_speed,
    dw.total_precipitation,
    ta.temperature_anomaly,
    ci.comfort_index
FROM {{ ref('int_daily_weather_metrics') }} dw
LEFT JOIN {{ ref('int_temperature_anomalies') }} ta
    ON dw.date = ta.date
LEFT JOIN {{ ref('int_comfort_index') }} ci
    ON dw.date = ci.date