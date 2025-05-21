-- models/stage/stg_orders.sql
{{ config(
    materialized='view',
    tags=['stage']
) }}

WITH enriched_orders AS (
    SELECT
        order_id,
        product_id,
        customer_id,
        amount,
        timestamp,
        now() AS processed_at -- добавляем метку времени обработки
    FROM
        {{ source('exported_data', 'orders') }}
)

SELECT *
FROM enriched_orders;