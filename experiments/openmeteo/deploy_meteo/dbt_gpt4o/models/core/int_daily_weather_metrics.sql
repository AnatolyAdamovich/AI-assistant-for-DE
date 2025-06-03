WITH daily_aggregates AS (
    SELECT
        toDate(load_timestamp) AS date,
        AVG(temperature_2m) AS avg_temperature_2m,
        AVG(relative_humidity_2m) AS avg_relative_humidity_2m,
        AVG(wind_speed_10m) AS avg_wind_speed_10m,
        SUM(precipitation) AS total_precipitation
    FROM {{ ref('stg_openmeteo_api') }}
    GROUP BY date
)
SELECT
    date,
    avg_temperature_2m,
    avg_relative_humidity_2m,
    avg_wind_speed_10m,
    total_precipitation
FROM daily_aggregates

{% set config_dict = {
    'materialized': 'incremental',
    'incremental_strategy': 'merge',
    'unique_key': 'date',
    'order_by': 'date',
    'engine': 'MergeTree()',
    'partition_by': 'toYYYYMM(date)'
} %}

{{ config(config_dict) }}