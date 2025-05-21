WITH average_order AS (
    SELECT
        customer_id,
        AVG(total_amount) AS avg_order_amount
    FROM {{ ref('int_orders_summary') }}
    GROUP BY customer_id
)

SELECT
    customer_id,
    avg_order_amount
FROM average_order
ORDER BY customer_id

{{ config(
    materialized='view',
    order_by='customer_id'
) }}