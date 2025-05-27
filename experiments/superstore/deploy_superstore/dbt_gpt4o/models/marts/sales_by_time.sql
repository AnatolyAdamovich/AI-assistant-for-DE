-- dbt model: sales_by_time

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['year', 'quarter', 'month'],
    partition_by=['year']
) }}

WITH sales_data AS (
    SELECT
        toYear(order_date) AS year,
        toQuarter(order_date) AS quarter,
        toMonth(order_date) AS month,
        SUM(sales) AS total_sales
    FROM {{ ref('int_orders_summary') }}
    GROUP BY year, quarter, month
)

SELECT *
FROM sales_data;