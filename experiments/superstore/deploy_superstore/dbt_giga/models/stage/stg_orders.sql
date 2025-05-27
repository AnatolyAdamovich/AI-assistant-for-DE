---
{{ config(materialized='view') }}

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
    now() as load_date
FROM
    {{ source('exported_data', 'orders') }}