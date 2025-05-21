-- dbt model for intermediate layer

{{ config(
    materialized='incremental',
    unique_key='order_id',
    order_by='timestamp',
    engine='MergeTree()',
    partition_by='toYYYYMM(timestamp)'
) }}

WITH cleaned_data AS (
    SELECT
        order_id,
        product_id,
        timestamp,
        customer_id,
        amount,
        processed_at
    FROM {{ ref('stg_orders') }}
    WHERE amount > 0 -- Filter out invalid order amounts
)

SELECT
    order_id,
    product_id,
    timestamp,
    customer_id,
    amount,
    processed_at
FROM cleaned_data