-- This model provides a summary of customer data

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    order_by='customer_id',
    engine='MergeTree()',
    partition_by='region_id'
) }}

SELECT
    customer_id,
    name,
    region_id,
    age,
    COUNT(order_id) AS total_orders,
    SUM(amount) AS total_spent
FROM (
    SELECT
        c.customer_id,
        c.name,
        c.region_id,
        c.age,
        o.order_id,
        o.amount
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o
    ON c.customer_id = o.customer_id
)
GROUP BY customer_id, name, region_id, age