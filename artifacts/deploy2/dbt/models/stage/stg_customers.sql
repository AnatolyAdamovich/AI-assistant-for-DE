with source as (
    select * from {{ source('exported_data', 'customers_last_data') }}
)

select
    region_id,
    registration_date,
    customer_id,
    now() as etl_loaded_at
from source

{{ config(materialized='view') }}