-- Sales Summary Model

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['order_id'],
    partition_by=['toYYYYMM(timestamp)']
) }}

WITH total_sales AS (
    SELECT
        toDate(timestamp) AS order_date,
        SUM(money) AS total_sales
    FROM {{ ref('int_enriched_orders') }}
    GROUP BY order_date
),

total_sales_per_region AS (
    SELECT
        region_id,
        SUM(money) AS total_sales_per_region
    FROM {{ ref('int_enriched_orders') }}
    GROUP BY region_id
)

SELECT
    eo.order_id,
    eo.product_id,
    eo.timestamp,
    eo.customer_id,
    eo.money,
    eo.region_id,
    eo.registration_date,
    ts.total_sales,
    tspr.total_sales_per_region
FROM {{ ref('int_enriched_orders') }} eo
LEFT JOIN total_sales ts ON toDate(eo.timestamp) = ts.order_date
LEFT JOIN total_sales_per_region tspr ON eo.region_id = tspr.region_id;