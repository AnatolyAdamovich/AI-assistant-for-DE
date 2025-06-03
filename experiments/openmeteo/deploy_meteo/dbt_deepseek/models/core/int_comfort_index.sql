{{ config(
    materialized='incremental',
    order_by='date',
    engine='MergeTree()',
    partition_by='toYYYYMM(date)',
    incremental_strategy='append'
) }}

SELECT
    date,
    avg_temperature,
    avg_humidity,
    avg_wind_speed,
    /* Simplified comfort index formula */
    (avg_temperature * 0.5) + (avg_humidity * 0.3) - (avg_wind_speed * 0.2) AS comfort_index
FROM {{ ref('int_daily_weather_metrics') }}