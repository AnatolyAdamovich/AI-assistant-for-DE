{{ config(
    materialized='view',
    alias='stg_open_meteo',
    tags=['stage']
) }}

select
    toDateTime(now()) as _loaded_at,
    temperature_2m,
    relative_humidity_2m,
    wind_speed_10m,
    precipitation
from {{ source('exported_data', 'OpenMeteo API_last_data') }}