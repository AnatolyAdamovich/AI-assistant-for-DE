-- dbt model: temperature_anomalies

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['date'],
    partition_by=['toYYYYMM(date)']
) }}

SELECT
    date,
    AVG(avg_temperature_2m) AS avg_temperature,
    AVG(climate_norm_temperature) AS climate_norm,
    AVG(avg_temperature_2m) - AVG(climate_norm_temperature) AS temperature_anomaly
FROM {{ ref('int_temperature_anomalies') }}
GROUP BY date;