{% set source_table = source('exported_data', 'orders') %}

select
    order_id,
    product_id,
    timestamp,
    customer_id,
    money,
    now() as loaded_at
from {{ source_table }}