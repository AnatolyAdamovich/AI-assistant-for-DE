-- Average Order Value Model

{{ config(
    materialized='view'
) }}

SELECT
    AVG(amount) AS average_order_value
FROM {{ ref('int_orders_enriched') }};