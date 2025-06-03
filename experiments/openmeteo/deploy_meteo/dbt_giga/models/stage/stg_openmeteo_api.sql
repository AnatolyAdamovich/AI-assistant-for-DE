-- dbt model for stage layer

{{ config(
    materialized='view',
    schema='stg'
) }}

SELECT
    temperature_2m,
    relative_humidity_2m,
    wind_speed_10m,
    precipitation,
    now() as load_datetime
FROM {{ source('exported_data', 'OpenMeteo API_last_data') }}