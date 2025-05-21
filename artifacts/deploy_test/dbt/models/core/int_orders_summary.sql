-- dbt model: int_orders_summary

{{ config(
    materialized='incremental',
    unique_key='order_id',
    engine='ReplacingMergeTree(processed_at)',
    order_by=['order_id'],
    partition_by=['toYYYYMM(timestamp)']
) }}

WITH cleaned_data AS (
    SELECT
        order_id,
        product_id,
        customer_id,
        amount,
        timestamp,
        processed_at
    FROM {{ ref('stg_orders') }}
    WHERE amount > 0  -- Удаляем записи с некорректной суммой заказа
),
aggregated_data AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        SUM(amount) AS total_amount,
        MIN(timestamp) AS first_order_date,
        MAX(timestamp) AS last_order_date
    FROM cleaned_data
    GROUP BY customer_id
)
SELECT *
FROM aggregated_data
