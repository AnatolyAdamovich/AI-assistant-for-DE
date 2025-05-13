with source as (
    select * from {{ source('exported_data', 'orders_last_data') }}
)

select
    order_id,
    product_id,
    timestamp,
    customer_id,
    money,
    now() as etl_loaded_at
from source

{{ config(materialized='view') }}