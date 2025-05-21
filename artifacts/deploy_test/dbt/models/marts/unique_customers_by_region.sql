WITH unique_customers AS (
    SELECT
        region_id,
        COUNT(DISTINCT customer_id) AS unique_customers_count
    FROM {{ ref('int_customers_enriched') }}
    GROUP BY region_id
)

SELECT
    region_id,
    unique_customers_count
FROM unique_customers
ORDER BY region_id

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='region_id'
) }}