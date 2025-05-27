-- dbt model: top_10_products

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['sales DESC'],
    partition_by=['category']
) }}

WITH top_products AS (
    SELECT
        category,
        product_name,
        SUM(sales) AS total_sales,
        SUM(profit) AS total_profit
    FROM {{ ref('int_orders_summary') }}
    GROUP BY category, product_name
    ORDER BY total_sales DESC
    LIMIT 10
)

SELECT *
FROM top_products;