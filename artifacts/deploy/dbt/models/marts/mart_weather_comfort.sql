{{ config(
    materialized='incremental',
    order_by=['measurement_date', 'hour'],
    engine='MergeTree()',
    unique_key=['measurement_date', 'hour']
) }}

WITH daily_comfort AS (
    SELECT
        toDate(measurement_datetime) AS measurement_date,
        toHour(measurement_datetime) AS hour,
        avg(comfort_index) AS avg_comfort_index,
        min(comfort_index) AS min_comfort_index,
        max(comfort_index) AS max_comfort_index,
        argMax(comfort_category, comfort_index) AS worst_comfort_category
    FROM {{ ref('int_weather_comfort_index') }}
    {% if is_incremental() %}
    WHERE measurement_datetime >= (SELECT max(measurement_date) FROM {{ this }})
    {% endif %}
    GROUP BY measurement_date, hour
)
SELECT
    measurement_date,
    hour,
    avg_comfort_index,
    min_comfort_index,
    max_comfort_index,
    worst_comfort_category,
    CASE
        WHEN min_comfort_index < 30 THEN 1
        ELSE 0
    END AS low_comfort_alert,
    now() AS _processed_at
FROM daily_comfort