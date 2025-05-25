-- dbt model: stg_openmeteo_api

{{ config(
    materialized='view',
    tags=['stage', 'weather_data']
) }}

WITH source_data AS (
    SELECT
        temperature,
        humidity,
        wind_speed,
        precipitation,
        now() AS extracted_at -- Adding timestamp for when the data was staged
    FROM {{ source('exported_data', 'OpenMeteo API') }}
)

SELECT *
FROM source_data;