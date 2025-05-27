{{ config(
    materialized='incremental',
    order_by='customer_id',
    engine='ReplacingMergeTree()',
    partition_by='customer_id'
) }}

WITH customer_aggregates AS (
    SELECT
        customer_id,
        any(customer_name) AS customer_name,
        any(segment) AS segment,
        any(region) AS region,
        SUM(sales_amount) AS total_spent,
        COUNT(DISTINCT order_id) AS order_count,
        MAX(etl_loaded_at) AS etl_loaded_at
    FROM {{ ref('int_orders_cleaned') }}
    {% if is_incremental() %}
    WHERE etl_loaded_at > (SELECT MAX(etl_loaded_at) FROM {{ this }})
    {% endif %}
    GROUP BY customer_id
)

SELECT
    customer_id,
    customer_name,
    segment,
    region,
    total_spent,
    order_count,
    CASE
        WHEN total_spent > 1000 THEN 'VIP'
        WHEN order_count > 5 THEN 'Frequent'
        ELSE 'Regular'
    END AS customer_segment,
    etl_loaded_at
FROM customer_aggregates