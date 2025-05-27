-- dbt model for intermediate layer

{{ config(
    materialized='incremental',
    partition_by='order_date',
    engine='MergeTree()',
    order_by='order_date'
) }}

SELECT
    category,
    region,
    toMonth(order_date) AS order_month,
    SUM(sales) AS total_sales,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT customer_id) AS customer_count
FROM {{ ref('stg_orders') }}
GROUP BY category, region, order_month
ORDER BY category, region, order_month

{% if is_incremental() %}
WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})
{% endif %}