-- dbt model: stg_orders

{{ config(
    materialized='view',
    alias='stg_orders'
) }}

WITH source_data AS (
    SELECT
        order_id,
        order_date,
        ship_date,
        customer_id,
        customer_name,
        segment,
        region,
        city,
        state,
        postal_code,
        product_id,
        category,
        sub_category,
        product_name,
        sales,
        quantity,
        discount,
        profit,
        now() AS load_timestamp -- Adding metadata for stage layer
    FROM {{ source('exported_data', 'orders') }}
)

SELECT *
FROM source_data;