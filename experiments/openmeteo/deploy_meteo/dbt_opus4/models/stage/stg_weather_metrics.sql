{{ config(
    materialized='view',
    schema='stage'
) }}

WITH source_data AS (
    SELECT
        temperature_2m,
        relative_humidity_2m,
        wind_speed_10m,
        precipitation,
        now() AS _loaded_at
    FROM {{ source('exported_data', 'OpenMeteo API') }}
)

SELECT
    -- Temperature metrics
    temperature_2m AS temperature_celsius,
    round(temperature_2m * 9/5 + 32, 2) AS temperature_fahrenheit,
    
    -- Humidity metrics
    relative_humidity_2m AS humidity_percent,
    
    -- Wind metrics
    wind_speed_10m AS wind_speed_ms,
    round(wind_speed_10m * 3.6, 2) AS wind_speed_kmh,
    
    -- Precipitation metrics
    precipitation AS precipitation_mm,
    
    -- Metadata
    _loaded_at,
    toDate(_loaded_at) AS _loaded_date
FROM source_data
WHERE temperature_2m IS NOT NULL
    OR relative_humidity_2m IS NOT NULL
    OR wind_speed_10m IS NOT NULL
    OR precipitation IS NOT NULL