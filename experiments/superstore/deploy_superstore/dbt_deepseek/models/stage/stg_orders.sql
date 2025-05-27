{{ config(
    materialized='view',
    alias='stg_orders',
    tags=['stage']
) }}

select
    order_id,
    order_date,
    ship_date,
    customer_id,
    customer_name,
    segment,
    region,
    city,
    state,
    postal_code,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit,
    now() as etl_loaded_at
from {{ source('exported_data', 'orders_last_data') }}