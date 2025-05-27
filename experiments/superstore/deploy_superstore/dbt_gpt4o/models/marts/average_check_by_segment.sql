-- dbt model: average_check_by_segment

{{ config(
    materialized='view'
) }}

WITH avg_check_data AS (
    SELECT
        customer_segment,
        SUM(sales) / COUNT(DISTINCT order_id) AS average_check
    FROM {{ ref('int_orders_summary') }}
    GROUP BY customer_segment
)

SELECT *
FROM avg_check_data;