{{ config(
    materialized='table',
    order_by='customer_id',
    engine='MergeTree()'
) }}

with customer_stats as (
    select
        customer_id,
        count(distinct order_id) as purchase_count,
        sum(sales_amount) as total_spent,
        min(order_date) as first_order,
        max(order_date) as last_order
    from {{ ref('int_orders_cleaned') }}
    group by customer_id
)

select
    customer_id,
    total_spent / purchase_count as avg_order_value,
    purchase_count / greatest(dateDiff('month', first_order, last_order), 1) as purchase_frequency,
    dateDiff('month', first_order, last_order) as customer_lifespan,
    avg_order_value * purchase_frequency * customer_lifespan as clv
from customer_stats