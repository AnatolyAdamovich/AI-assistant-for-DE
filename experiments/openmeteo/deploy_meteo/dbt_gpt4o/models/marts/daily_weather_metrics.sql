-- dbt model: daily_weather_metrics

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['date'],
    partition_by=['toYYYYMM(date)']
) }}

WITH metrics AS (
    SELECT
        date,
        AVG(avg_temperature_2m) AS avg_temperature,
        AVG(avg_relative_humidity_2m) AS avg_humidity,
        AVG(avg_wind_speed_10m) AS avg_wind_speed,
        SUM(total_precipitation) AS total_precipitation
    FROM {{ ref('int_daily_weather_metrics') }}
    GROUP BY date
)

SELECT *
FROM metrics;