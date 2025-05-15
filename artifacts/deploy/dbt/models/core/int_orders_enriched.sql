-- This model enriches order data with customer information

{{ config(
    materialized='incremental',
    unique_key='order_id',
    order_by='order_id',
    engine='MergeTree()',
    partition_by='toYYYYMM(timestamp)'
) }}

WITH cleaned_orders AS (
    SELECT
        order_id,
        product_id,
        timestamp,
        customer_id,
        amount
    FROM {{ ref('stg_orders') }}
    WHERE amount > 0 -- Filter out orders with non-positive amounts
),

joined_data AS (
    SELECT
        o.order_id,
        o.product_id,
        o.timestamp,
        o.customer_id,
        o.amount,
        c.name AS customer_name,
        c.region_id,
        c.age
    FROM cleaned_orders o
    LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id
)

SELECT * FROM joined_data