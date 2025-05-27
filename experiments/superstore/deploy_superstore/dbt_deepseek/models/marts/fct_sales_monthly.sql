{{ config(
    materialized='table',
    order_by='order_month',
    engine='MergeTree()'
) }}

select
    toStartOfMonth(order_date) as order_month,
    toQuarter(order_month) as order_quarter,
    toYear(order_month) as order_year,
    sum(sales_amount) as total_sales,
    count(distinct order_id) as total_orders
from {{ ref('int_orders_cleaned') }}
group by order_month
order by order_month