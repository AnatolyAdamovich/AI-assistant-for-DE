{{ config(
    materialized='incremental',
    order_by=['measurement_date'],
    engine='MergeTree()',
    partition_by='toYYYYMM(measurement_date)',
    unique_key='measurement_date',
    on_schema_change='fail'
) }}

WITH daily_metrics AS (
    SELECT
        toDate(_loaded_at) AS measurement_date,
        AVG(temperature_celsius) AS avg_temperature_celsius,
        MIN(temperature_celsius) AS min_temperature_celsius,
        MAX(temperature_celsius) AS max_temperature_celsius,
        AVG(humidity_percent) AS avg_humidity_percent,
        AVG(wind_speed_ms) AS avg_wind_speed_ms,
        MAX(wind_speed_ms) AS max_wind_speed_ms,
        SUM(precipitation_mm) AS total_precipitation_mm,
        COUNT(*) AS measurement_count
    FROM {{ ref('stg_weather_metrics') }}
    {% if is_incremental() %}
    WHERE _loaded_at >= (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
    GROUP BY measurement_date
)

SELECT
    measurement_date,
    avg_temperature_celsius,
    min_temperature_celsius,
    max_temperature_celsius,
    avg_humidity_percent,
    avg_wind_speed_ms,
    max_wind_speed_ms,
    total_precipitation_mm,
    measurement_count,
    now() AS _processed_at
FROM daily_metrics