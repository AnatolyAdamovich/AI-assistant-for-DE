{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(abc_category, total_sales_desc)',
    partition_by='abc_category'
) }}

with product_sales as (
    select
        oe.product_id,
        sum(oe.net_sales) as total_sales,
        sum(oe.profit) as total_profit,
        count(distinct oe.order_id) as total_orders,
        sum(oe.profit) / nullif(sum(oe.net_sales), 0) as profit_margin
    from {{ ref('int_orders_enriched') }} oe
    group by oe.product_id
),

ranked_products as (
    select
        *,
        row_number() over (order by total_sales desc) as sales_rank,
        sum(total_sales) over () as total_company_sales,
        sum(total_sales) over (order by total_sales desc rows unbounded preceding) as cumulative_sales
    from product_sales
),

abc_categorized as (
    select
        *,
        cumulative_sales / total_company_sales as cumulative_sales_pct,
        case
            when cumulative_sales / total_company_sales <= 0.8 then 'A'
            when cumulative_sales / total_company_sales <= 0.95 then 'B'
            else 'C'
        end as abc_category
    from ranked_products
)

select
    product_id,
    total_sales,
    total_profit,
    total_orders,
    profit_margin,
    sales_rank,
    cumulative_sales_pct,
    abc_category,
    total_sales as total_sales_desc
from abc_categorized
order by total_sales desc