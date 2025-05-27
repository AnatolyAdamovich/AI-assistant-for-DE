-- dbt model for intermediate layer

{{ config(
    materialized='incremental',
    partition_by='customer_id',
    engine='MergeTree()',
    order_by='customer_id'
) }}

WITH customer_aggregates AS (
    SELECT
        customer_id,
        SUM(sales) AS total_spent,
        COUNT(DISTINCT order_id) AS order_count
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

SELECT
    customer_id,
    CASE
        WHEN total_spent > 10000 THEN 'VIP'
        WHEN order_count > 10 THEN 'Frequent'
        ELSE 'Regular'
    END AS customer_segment
FROM customer_aggregates

{% if is_incremental() %}
WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}