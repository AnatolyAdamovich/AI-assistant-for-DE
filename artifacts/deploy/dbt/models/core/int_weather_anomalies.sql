{{ config(
    materialized='incremental',
    order_by=['measurement_date'],
    engine='MergeTree()',
    partition_by='toYYYYMM(measurement_date)',
    unique_key='measurement_date',
    on_schema_change='fail'
) }}

WITH daily_data AS (
    SELECT
        toDate(_loaded_at) AS measurement_date,
        AVG(temperature_celsius) AS daily_avg_temperature
    FROM {{ ref('stg_weather_metrics') }}
    {% if is_incremental() %}
    WHERE toDate(_loaded_at) >= (SELECT MAX(measurement_date) FROM {{ this }})
    {% endif %}
    GROUP BY measurement_date
),
historical_baseline AS (
    SELECT
        toDayOfYear(measurement_date) AS day_of_year,
        AVG(daily_avg_temperature) AS baseline_temperature,
        stddevPop(daily_avg_temperature) AS temperature_stddev
    FROM (
        SELECT
            toDate(_loaded_at) AS measurement_date,
            AVG(temperature_celsius) AS daily_avg_temperature
        FROM {{ ref('stg_weather_metrics') }}
        WHERE _loaded_at >= today() - INTERVAL 365 DAY
        GROUP BY measurement_date
    )
    GROUP BY day_of_year
)

SELECT
    d.measurement_date,
    d.daily_avg_temperature,
    h.baseline_temperature,
    h.temperature_stddev,
    d.daily_avg_temperature - h.baseline_temperature AS temperature_anomaly,
    (d.daily_avg_temperature - h.baseline_temperature) / NULLIF(h.temperature_stddev, 0) AS anomaly_z_score,
    CASE
        WHEN abs((d.daily_avg_temperature - h.baseline_temperature) / NULLIF(h.temperature_stddev, 0)) > 3 THEN 'Extreme'
        WHEN abs((d.daily_avg_temperature - h.baseline_temperature) / NULLIF(h.temperature_stddev, 0)) > 2 THEN 'Significant'
        WHEN abs((d.daily_avg_temperature - h.baseline_temperature) / NULLIF(h.temperature_stddev, 0)) > 1 THEN 'Moderate'
        ELSE 'Normal'
    END AS anomaly_severity,
    now() AS _processed_at
FROM daily_data d
LEFT JOIN historical_baseline h ON toDayOfYear(d.measurement_date) = h.day_of_year