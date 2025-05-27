{{ config(
    materialized='incremental',
    order_by='(category, region, order_month)',
    engine='MergeTree()',
    partition_by='toYYYYMM(order_month)'
) }}

SELECT
    category,
    region,
    toStartOfMonth(order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    SUM(profit) AS total_profit,
    COUNT(DISTINCT order_id) AS order_count,
    MAX(etl_loaded_at) AS etl_loaded_at
FROM {{ ref('int_orders_cleaned') }}
{% if is_incremental() %}
WHERE etl_loaded_at > (SELECT MAX(etl_loaded_at) FROM {{ this }})
{% endif %}
GROUP BY
    category,
    region,
    order_month