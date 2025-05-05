-- Customer Lifetime Value Model

{{ config(
    materialized='view'
) }}

SELECT
    customer_id,
    SUM(money) AS lifetime_value
FROM {{ ref('int_enriched_orders') }}
GROUP BY customer_id;