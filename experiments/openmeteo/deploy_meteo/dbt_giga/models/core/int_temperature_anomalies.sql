-- dbt model for intermediate layer

{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by='date',
    partition_by='toYYYYMM(date)',
    unique_key='date'
) }}

SELECT
    dw.date,
    dw.avg_temperature_2m - cl.climate_norm AS temperature_anomaly
FROM {{ ref('int_daily_weather') }} dw
JOIN climate cl ON dw.date = cl.date
{% if is_incremental() %}
WHERE dw.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}