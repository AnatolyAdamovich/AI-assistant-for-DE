WITH climate_norm AS (
    SELECT
        date,
        AVG(temperature_2m) AS climate_norm_temperature
    FROM {{ ref('stg_openmeteo_api') }}
    GROUP BY date
),
weather_data AS (
    SELECT
        toDate(load_timestamp) AS date,
        AVG(temperature_2m) AS avg_temperature_2m
    FROM {{ ref('stg_openmeteo_api') }}
    GROUP BY date
)
SELECT
    wd.date,
    wd.avg_temperature_2m,
    cn.climate_norm_temperature,
    wd.avg_temperature_2m - cn.climate_norm_temperature AS temperature_anomaly
FROM weather_data wd
LEFT JOIN climate_norm cn
ON wd.date = cn.date

{% set config_dict = {
    'materialized': 'incremental',
    'incremental_strategy': 'merge',
    'unique_key': 'date',
    'order_by': 'date',
    'engine': 'MergeTree()',
    'partition_by': 'toYYYYMM(date)'
} %}

{{ config(config_dict) }}