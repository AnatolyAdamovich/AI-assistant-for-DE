-- Customer Order Summary Model

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    engine='ReplacingMergeTree()',
    order_by=['customer_id']
) }}

SELECT
    customer_id,
    COUNT(order_id) AS total_orders,
    SUM(amount) AS total_spent,
    AVG(amount) AS average_order_value
FROM {{ ref('int_orders_enriched') }}
GROUP BY customer_id;