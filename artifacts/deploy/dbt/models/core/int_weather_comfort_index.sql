{{ config(
    materialized='incremental',
    order_by=['measurement_datetime'],
    engine='MergeTree()',
    partition_by='toYYYYMM(measurement_datetime)',
    unique_key='measurement_datetime',
    on_schema_change='fail'
) }}

WITH comfort_calculations AS (
    SELECT
        _loaded_at AS measurement_datetime,
        temperature_celsius,
        humidity_percent,
        wind_speed_ms,
        -- Heat Index calculation (simplified formula)
        CASE
            WHEN temperature_celsius >= 27 AND humidity_percent >= 40
            THEN temperature_celsius + 0.5 * (temperature_celsius * humidity_percent / 100)
            ELSE temperature_celsius
        END AS heat_index,
        -- Wind Chill calculation (simplified formula)
        CASE
            WHEN temperature_celsius <= 10 AND wind_speed_ms > 1.3
            THEN 13.12 + 0.6215 * temperature_celsius - 11.37 * pow(wind_speed_ms * 3.6, 0.16) + 0.3965 * temperature_celsius * pow(wind_speed_ms * 3.6, 0.16)
            ELSE temperature_celsius
        END AS wind_chill,
        -- Comfort Index (0-100 scale)
        CASE
            WHEN temperature_celsius BETWEEN 18 AND 24 AND humidity_percent BETWEEN 30 AND 60 AND wind_speed_ms < 5
            THEN 100
            WHEN temperature_celsius BETWEEN 15 AND 27 AND humidity_percent BETWEEN 20 AND 70 AND wind_speed_ms < 10
            THEN 80 - abs(temperature_celsius - 21) * 2 - abs(humidity_percent - 45) * 0.5 - wind_speed_ms * 2
            ELSE greatest(0, 50 - abs(temperature_celsius - 21) * 3 - abs(humidity_percent - 45) * 0.5 - wind_speed_ms * 3)
        END AS comfort_index
    FROM {{ ref('stg_weather_metrics') }}
    {% if is_incremental() %}
    WHERE _loaded_at >= (SELECT MAX(measurement_datetime) FROM {{ this }})
    {% endif %}
)

SELECT
    measurement_datetime,
    temperature_celsius,
    humidity_percent,
    wind_speed_ms,
    heat_index,
    wind_chill,
    comfort_index,
    CASE
        WHEN comfort_index >= 80 THEN 'Excellent'
        WHEN comfort_index >= 60 THEN 'Good'
        WHEN comfort_index >= 40 THEN 'Fair'
        WHEN comfort_index >= 20 THEN 'Poor'
        ELSE 'Very Poor'
    END AS comfort_category,
    now() AS _processed_at
FROM comfort_calculations