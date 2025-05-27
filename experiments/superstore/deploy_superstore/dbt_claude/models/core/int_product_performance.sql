{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by=['category', 'sub_category', 'product_id'],
    partition_by='category',
    unique_key='product_id'
) }}

SELECT
    product_id,
    product_name,
    category,
    sub_category,
    
    -- Агрегированные метрики по продукту
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_quantity_sold,
    SUM(net_sales) AS total_revenue,
    SUM(profit) AS total_profit,
    AVG(net_sales) AS avg_order_value,
    AVG(profit_margin_pct) AS avg_profit_margin,
    
    -- Временные метрики
    MIN(order_date) AS first_sale_date,
    MAX(order_date) AS last_sale_date,
    
    -- Географическое распределение
    COUNT(DISTINCT region) AS regions_sold,
    COUNT(DISTINCT city) AS cities_sold,
    argMax(region, net_sales) AS top_region_by_sales,
    
    -- Сегментация продуктов по производительности
    CASE 
        WHEN SUM(net_sales) >= 10000 AND COUNT(DISTINCT order_id) >= 50 THEN 'Star'
        WHEN SUM(net_sales) >= 5000 THEN 'High Performer'
        WHEN COUNT(DISTINCT order_id) >= 20 THEN 'Popular'
        WHEN AVG(profit_margin_pct) >= 0.3 THEN 'High Margin'
        ELSE 'Standard'
    END AS product_tier,
    
    -- Категоризация по объему продаж
    CASE 
        WHEN SUM(quantity) >= 100 THEN 'High Volume'
        WHEN SUM(quantity) >= 50 THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS volume_category,
    
    -- Метаданные
    MAX(_loaded_at) AS _loaded_at,
    MAX(_dbt_run_started_at) AS _dbt_run_started_at,
    MAX(_dbt_invocation_id) AS _dbt_invocation_id
    
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}

GROUP BY 
    product_id,
    product_name,
    category,
    sub_category