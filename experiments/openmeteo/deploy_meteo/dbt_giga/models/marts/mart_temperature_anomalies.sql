-- dbt model for mart_temperature_anomalies

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='date',
    partition_by='toYYYYMM(date)'
) }}

SELECT
    date,
    temperature_anomaly
FROM
    {{ ref('int_temperature_anomalies') }}