{{ config(
    materialized='table',
    order_by='order_month',
    engine='MergeTree()'
) }}

with monthly_customers as (
    select
        toStartOfMonth(order_date) as order_month,
        customer_id
    from {{ ref('int_orders_cleaned') }}
    group by order_month, customer_id
),

retention_data as (
    select
        order_month,
        count(customer_id) as current_customers,
        countIf(lagInFrame(order_month, 1) over (partition by customer_id order by order_month) = 
                order_month - interval 1 month) as retained_customers
    from monthly_customers
    group by order_month
)

select
    order_month,
    retained_customers,
    lagInFrame(current_customers, 1) over (order by order_month) as previous_customers,
    if(previous_customers > 0, retained_customers / previous_customers, 0) as retention_rate
from retention_data