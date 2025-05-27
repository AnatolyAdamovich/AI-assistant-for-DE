{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by=['customer_id'],
    unique_key='customer_id'
) }}

SELECT
    customer_id,
    customer_name,
    segment,
    
    -- Агрегированные метрики по клиенту
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(net_sales) AS total_spent,
    AVG(net_sales) AS avg_order_value,
    SUM(profit) AS total_profit,
    SUM(quantity) AS total_quantity,
    
    -- Временные метрики
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    dateDiff('day', MIN(order_date), MAX(order_date)) AS customer_lifetime_days,
    
    -- Сегментация клиентов по активности
    CASE 
        WHEN SUM(net_sales) >= 5000 AND COUNT(DISTINCT order_id) >= 10 THEN 'VIP'
        WHEN COUNT(DISTINCT order_id) >= 5 THEN 'Frequent'
        WHEN SUM(net_sales) >= 1000 THEN 'High Value'
        ELSE 'Regular'
    END AS customer_tier,
    
    -- Географические данные (последний известный адрес)
    argMax(region, order_date) AS latest_region,
    argMax(city, order_date) AS latest_city,
    argMax(state, order_date) AS latest_state,
    argMax(postal_code, order_date) AS latest_postal_code,
    
    -- Предпочтения по категориям
    argMax(category, net_sales) AS preferred_category,
    argMax(sub_category, net_sales) AS preferred_sub_category,
    
    -- Метрики прибыльности
    AVG(profit_margin_pct) AS avg_profit_margin,
    
    -- Метаданные
    MAX(_loaded_at) AS _loaded_at,
    MAX(_dbt_run_started_at) AS _dbt_run_started_at,
    MAX(_dbt_invocation_id) AS _dbt_invocation_id
    
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}

GROUP BY 
    customer_id,
    customer_name,
    segment