-- Sales by Day Model

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['date'],
    partition_by=['date']
) }}

WITH daily_sales AS (
    SELECT
        toDate(timestamp) AS date,
        SUM(amount) AS total_sales
    FROM {{ ref('int_orders_enriched') }}
    GROUP BY date
)

SELECT
    date,
    total_sales
FROM daily_sales
ORDER BY date;