with source as (
    select
        region_id,
        registration_date,
        customer_id,
        toDate(registration_date) as registration_date_trunc,
        now() as etl_loaded_at
    from {{ source('exported_data', 'customers_last_data') }}
)

select
    region_id,
    registration_date,
    customer_id,
    registration_date_trunc,
    etl_loaded_at
from source