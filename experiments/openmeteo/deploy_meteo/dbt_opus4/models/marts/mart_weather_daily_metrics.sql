{{ config(
    materialized='table',
    order_by=['measurement_date'],
    engine='MergeTree()'
) }}

SELECT
    measurement_date,
    avg_temperature_celsius,
    avg_humidity_percent,
    avg_wind_speed_ms,
    total_precipitation_mm,
    CASE
        WHEN avg_temperature_celsius > 25 THEN 1
        ELSE 0
    END AS high_temperature_alert,
    CASE
        WHEN avg_humidity_percent > 80 THEN 1
        ELSE 0
    END AS high_humidity_alert,
    CASE
        WHEN avg_wind_speed_ms > 10 THEN 1
        ELSE 0
    END AS high_wind_alert,
    CASE
        WHEN total_precipitation_mm > 5 THEN 1
        ELSE 0
    END AS high_precipitation_alert,
    now() AS _processed_at
FROM {{ ref('int_weather_daily_aggregates') }}