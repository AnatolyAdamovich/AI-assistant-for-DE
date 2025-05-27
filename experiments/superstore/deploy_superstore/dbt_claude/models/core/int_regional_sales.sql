{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by=['region', 'order_year', 'order_month'],
    partition_by='order_year',
    unique_key=['region', 'order_year', 'order_month']
) }}

SELECT
    region,
    toYear(order_date) AS order_year,
    toMonth(order_date) AS order_month,
    
    -- Агрегированные метрики по региону и месяцу
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products,
    SUM(net_sales) AS total_sales,
    SUM(profit) AS total_profit,
    SUM(quantity) AS total_quantity,
    AVG(net_sales) AS avg_order_value,
    AVG(profit_margin_pct) AS avg_profit_margin,
    
    -- Распределение по сегментам клиентов
    countIf(segment = 'Consumer') AS consumer_orders,
    countIf(segment = 'Corporate') AS corporate_orders,
    countIf(segment = 'Home Office') AS home_office_orders,
    
    -- Топ категории по продажам
    argMax(category, net_sales) AS top_category_by_sales,
    
    -- Количество уникальных городов
    COUNT(DISTINCT city) AS unique_cities,
    
    -- Метрики доставки
    AVG(CASE WHEN ship_date IS NOT NULL THEN dateDiff('day', order_date, ship_date) END) AS avg_delivery_days,
    countIf(ship_date IS NULL) AS unshipped_orders,
    
    -- Метаданные
    MAX(_loaded_at) AS _loaded_at,
    MAX(_dbt_run_started_at) AS _dbt_run_started_at,
    MAX(_dbt_invocation_id) AS _dbt_invocation_id
    
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}

GROUP BY 
    region,
    toYear(order_date),
    toMonth(order_date)