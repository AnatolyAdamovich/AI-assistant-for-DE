{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by=['order_date', 'customer_id'],
    partition_by='toYYYYMM(order_date)',
    unique_key=['order_id', 'product_id']
) }}

SELECT
    order_id,
    product_id,
    customer_id,
    order_date,
    ship_date,
    customer_name,
    segment,
    region,
    city,
    state,
    postal_code,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit,
    net_sales,
    profit_margin_pct,
    
    -- Время доставки
    CASE 
        WHEN ship_date IS NOT NULL 
        THEN dateDiff('day', order_date, ship_date)
        ELSE NULL
    END AS delivery_days,
    
    -- Статус доставки
    CASE 
        WHEN ship_date IS NULL THEN 'Not Shipped'
        WHEN dateDiff('day', order_date, ship_date) <= 3 THEN 'Fast'
        WHEN dateDiff('day', order_date, ship_date) <= 7 THEN 'Standard'
        ELSE 'Slow'
    END AS delivery_status,
    
    -- Временные метрики
    toYear(order_date) AS order_year,
    toMonth(order_date) AS order_month,
    toQuarter(order_date) AS order_quarter,
    toDayOfWeek(order_date) AS order_day_of_week,
    
    -- Категоризация по размеру заказа
    CASE 
        WHEN net_sales >= 1000 THEN 'Large'
        WHEN net_sales >= 500 THEN 'Medium'
        WHEN net_sales >= 100 THEN 'Small'
        ELSE 'Micro'
    END AS order_size_category,
    
    -- Категоризация по прибыльности
    CASE 
        WHEN profit_margin_pct >= 0.3 THEN 'High Margin'
        WHEN profit_margin_pct >= 0.1 THEN 'Medium Margin'
        WHEN profit_margin_pct >= 0 THEN 'Low Margin'
        ELSE 'Loss'
    END AS profitability_category,
    
    _loaded_at,
    _dbt_run_started_at,
    _dbt_invocation_id
    
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}