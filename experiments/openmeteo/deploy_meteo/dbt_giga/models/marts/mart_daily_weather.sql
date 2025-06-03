-- dbt model for mart_daily_weather

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='date',
    partition_by='toYYYYMM(date)'
) }}

SELECT
    date,
    avg_temperature_2m AS avg_temperature,
    avg_relative_humidity_2m AS avg_humidity,
    avg_wind_speed_10m AS avg_wind_speed,
    total_precipitation AS precipitation
FROM
    {{ ref('int_daily_weather') }}