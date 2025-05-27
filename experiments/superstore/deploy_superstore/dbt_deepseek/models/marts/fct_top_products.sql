{{ config(
    materialized='table',
    order_by='(total_sales)',
    engine='MergeTree()'
) }}

select
    product_id,
    sum(sales_amount) as total_sales
from {{ ref('int_orders_cleaned') }}
group by product_id
order by total_sales desc
limit 10