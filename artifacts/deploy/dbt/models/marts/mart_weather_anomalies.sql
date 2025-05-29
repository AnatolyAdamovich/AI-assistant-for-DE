{{ config(
    materialized='table',
    order_by=['measurement_date'],
    engine='MergeTree()'
) }}

SELECT
    measurement_date,
    daily_avg_temperature AS temperature_celsius,
    baseline_temperature AS climate_norm,
    temperature_anomaly,
    anomaly_severity,
    CASE
        WHEN abs(temperature_anomaly) > 5 THEN 1
        ELSE 0
    END AS significant_anomaly_alert,
    now() AS _processed_at
FROM {{ ref('int_weather_anomalies') }}