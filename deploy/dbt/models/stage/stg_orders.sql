-- models/stg_orders.sql

{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        order_id,
        product_id,
        timestamp AS order_timestamp,
        customer_id,
        money,
        now() AS load_timestamp
    FROM
        {{ source('exported_data', 'orders') }}
)

SELECT *
FROM source_data;