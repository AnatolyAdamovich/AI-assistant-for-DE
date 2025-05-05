with source as (
    select
        order_id,
        product_id,
        timestamp,
        customer_id,
        money,
        toDate(timestamp) as event_date,
        now() as etl_loaded_at
    from {{ source('exported_data', 'orders_last_data') }}
)

select
    order_id,
    product_id,
    timestamp,
    customer_id,
    money,
    event_date,
    etl_loaded_at
from source