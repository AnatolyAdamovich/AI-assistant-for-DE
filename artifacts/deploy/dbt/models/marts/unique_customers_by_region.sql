-- Unique Customers by Region Model

{{ config(
    materialized='view'
) }}

SELECT
    region_id,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM {{ ref('int_orders_enriched') }}
GROUP BY region_id
ORDER BY region_id;