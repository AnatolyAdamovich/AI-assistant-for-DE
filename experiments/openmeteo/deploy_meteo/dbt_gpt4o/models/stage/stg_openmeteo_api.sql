-- dbt model for staging OpenMeteo API data

{{ config(
    materialized='view',
    alias='stg_openmeteo_api'
) }}

WITH source_data AS (
    SELECT
        temperature_2m,
        relative_humidity_2m,
        wind_speed_10m,
        precipitation,
        now() AS load_timestamp -- Adding metadata for load time
    FROM {{ source('exported_data', 'OpenMeteo API') }}
)

SELECT *
FROM source_data;