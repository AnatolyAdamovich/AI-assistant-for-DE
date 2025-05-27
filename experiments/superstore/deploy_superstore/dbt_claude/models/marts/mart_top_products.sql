{{ config(
    materialized='incremental',
    unique_key='analysis_date',
    engine='ReplacingMergeTree()',
    order_by='analysis_date',
    partition_by='toYYYYMM(analysis_date)'
) }}

with top_selling_products as (
    select
        product_id,
        sum(net_sales) as total_sales,
        count(distinct order_id) as total_orders,
        'sales' as metric_type,
        row_number() over (order by sum(net_sales) desc) as rank
    from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
        where order_date >= (select max(analysis_date) from {{ this }})
    {% endif %}
    group by product_id
    qualify rank <= 10
),

top_profitable_products as (
    select
        product_id,
        sum(profit) as total_profit,
        count(distinct order_id) as total_orders,
        'profit' as metric_type,
        row_number() over (order by sum(profit) desc) as rank
    from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
        where order_date >= (select max(analysis_date) from {{ this }})
    {% endif %}
    group by product_id
    qualify rank <= 10
)

select
    today() as analysis_date,
    'sales' as analysis_type,
    product_id,
    total_sales as metric_value,
    total_orders,
    rank
from top_selling_products

union all

select
    today() as analysis_date,
    'profit' as analysis_type,
    product_id,
    total_profit as metric_value,
    total_orders,
    rank
from top_profitable_products