{{ config(
    materialized='table',
    order_by='customer_segment',
    engine='MergeTree()'
) }}

select
    seg.customer_segment,
    sum(ord.sales_amount) as total_sales,
    count(distinct ord.order_id) as total_orders,
    total_sales / total_orders as average_ticket
from {{ ref('int_orders_cleaned') }} ord
join {{ ref('int_customer_segments') }} seg
on ord.customer_id = seg.customer_id
group by seg.customer_segment