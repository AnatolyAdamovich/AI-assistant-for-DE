{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='customer_tier',
    partition_by='customer_tier'
) }}

with customer_metrics as (
    select
        cm.customer_id,
        cm.total_orders,
        cm.total_spent,
        cm.customer_tier,
        cm.customer_lifetime_days,
        cm.total_spent / nullif(cm.total_orders, 0) as avg_order_value,
        case
            when cm.customer_lifetime_days > 0 then cm.total_orders / (cm.customer_lifetime_days / 365.0)
            else 0
        end as purchase_frequency,
        case
            when cm.customer_lifetime_days > 0 then (cm.total_spent / nullif(cm.total_orders, 0)) * (cm.total_orders / (cm.customer_lifetime_days / 365.0)) * (cm.customer_lifetime_days / 365.0)
            else cm.total_spent
        end as customer_lifetime_value
    from {{ ref('int_customer_metrics') }} cm
),

segment_stats as (
    select
        customer_tier,
        count(*) as customers_count,
        avg(total_spent) as avg_total_spent,
        avg(total_orders) as avg_total_orders,
        avg(avg_order_value) as avg_order_value_segment,
        avg(customer_lifetime_value) as avg_clv,
        sum(total_spent) as segment_revenue,
        sum(total_orders) as segment_orders
    from customer_metrics
    group by customer_tier
)

select
    cm.*,
    ss.customers_count,
    ss.avg_total_spent,
    ss.avg_total_orders,
    ss.avg_order_value_segment,
    ss.avg_clv,
    ss.segment_revenue,
    ss.segment_orders,
    cm.total_spent / nullif(ss.segment_revenue, 0) as revenue_contribution_pct
from customer_metrics cm
left join segment_stats ss on cm.customer_tier = ss.customer_tier