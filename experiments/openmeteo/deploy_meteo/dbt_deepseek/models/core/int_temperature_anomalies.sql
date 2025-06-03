{{ config(
    materialized='incremental',
    order_by='date',
    engine='MergeTree()',
    partition_by='toYYYYMM(date)',
    incremental_strategy='append'
) }}

SELECT
    dw.date,
    dw.avg_temperature,
    cn.climate_norm,
    dw.avg_temperature - cn.climate_norm AS temperature_anomaly
FROM {{ ref('int_daily_weather_metrics') }} dw
LEFT JOIN {{ ref('stg_climate_norms') }} cn
ON dw.date = cn.date