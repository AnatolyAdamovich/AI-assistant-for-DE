{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(year, quarter, month)',
    partition_by='year'
) }}

with monthly_sales as (
    select
        toYear(order_date) as year,
        toQuarter(order_date) as quarter,
        toMonth(order_date) as month,
        toStartOfMonth(order_date) as month_start,
        sum(net_sales) as total_sales,
        sum(profit) as total_profit,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        total_profit / nullif(total_sales, 0) as profit_margin
    from {{ ref('int_orders_enriched') }}
    group by year, quarter, month, month_start
),

quarterly_sales as (
    select
        year,
        quarter,
        sum(total_sales) as quarterly_sales,
        sum(total_profit) as quarterly_profit,
        sum(total_orders) as quarterly_orders
    from monthly_sales
    group by year, quarter
),

yearly_sales as (
    select
        year,
        sum(total_sales) as yearly_sales,
        sum(total_profit) as yearly_profit,
        sum(total_orders) as yearly_orders
    from monthly_sales
    group by year
)

select
    m.year,
    m.quarter,
    m.month,
    m.month_start,
    m.total_sales,
    m.total_profit,
    m.total_orders,
    m.unique_customers,
    m.profit_margin,
    q.quarterly_sales,
    q.quarterly_profit,
    q.quarterly_orders,
    y.yearly_sales,
    y.yearly_profit,
    y.yearly_orders,
    m.total_sales / nullif(m.total_orders, 0) as avg_order_value
from monthly_sales m
left join quarterly_sales q on m.year = q.year and m.quarter = q.quarter
left join yearly_sales y on m.year = y.year
order by m.year, m.month