{{ config(
    materialized='incremental',
    order_by='date',
    engine='MergeTree()',
    partition_by='toYYYYMM(date)',
    incremental_strategy='append'
) }}

SELECT
    toDate(_loaded_at) AS date,
    avg(temperature_2m) AS avg_temperature,
    avg(relative_humidity_2m) AS avg_humidity,
    avg(wind_speed_10m) AS avg_wind_speed,
    sum(precipitation) AS total_precipitation
FROM {{ ref('stg_open_meteo') }}
{% if is_incremental() %}
WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
GROUP BY date