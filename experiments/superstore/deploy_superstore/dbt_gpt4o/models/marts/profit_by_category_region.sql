-- dbt model: profit_by_category_region

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['category', 'region'],
    partition_by=['region']
) }}

WITH profit_data AS (
    SELECT
        category,
        region,
        SUM(profit) AS total_profit,
        SUM(profit) / SUM(sales) AS profitability
    FROM {{ ref('int_orders_summary') }}
    GROUP BY category, region
)

SELECT *
FROM profit_data;