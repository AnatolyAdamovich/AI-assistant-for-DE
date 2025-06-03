-- dbt model for intermediate layer

{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by='date',
    partition_by='toYYYYMM(date)',
    unique_key='date'
) }}

SELECT
    toDate(load_datetime) AS date,
    AVG(temperature_2m) AS avg_temperature_2m,
    AVG(relative_humidity_2m) AS avg_relative_humidity_2m,
    AVG(wind_speed_10m) AS avg_wind_speed_10m,
    SUM(precipitation) AS total_precipitation
FROM {{ ref('stg_openmeteo_api') }}
GROUP BY date
{% if is_incremental() %}
WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{ this }})
{% endif %}