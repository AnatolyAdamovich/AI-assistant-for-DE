WITH daily_sales AS (
    SELECT
        toDate(first_order_date) AS order_date,
        SUM(total_amount) AS daily_total_sales
    FROM {{ ref('int_orders_summary') }}
    GROUP BY order_date
)

SELECT
    order_date,
    daily_total_sales
FROM daily_sales
ORDER BY order_date

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='order_date',
    partition_by='order_date'
) }}