-- dbt model for intermediate layer

{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by='date',
    partition_by='toYYYYMM(date)',
    unique_key='date'
) }}

SELECT
    date,
    f(avg_temperature_2m, avg_relative_humidity_2m, avg_wind_speed_10m) AS comfort_index
FROM {{ ref('int_daily_weather') }}
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}