-- dbt model: comfort_index

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['date'],
    partition_by=['toYYYYMM(date)']
) }}

SELECT
    date,
    AVG(avg_temperature_2m) AS avg_temperature,
    AVG(avg_relative_humidity_2m) AS avg_humidity,
    AVG(avg_wind_speed_10m) AS avg_wind_speed,
    AVG(comfort_index) AS comfort_index
FROM {{ ref('int_comfort_index') }}
GROUP BY date;