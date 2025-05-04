{% set schema = 'raw' %}

{{ config(materialized='view', schema=schema) }}

SELECT
    order_id,
    product_id,
    timestamp,
    customer_id,
    money
FROM
    {{ source('raw', 'orders') }}