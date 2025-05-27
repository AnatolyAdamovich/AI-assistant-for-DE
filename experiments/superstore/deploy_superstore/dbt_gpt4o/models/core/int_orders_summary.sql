-- dbt model: int_orders_summary

{{ config(
    materialized='incremental',
    unique_key='order_id',
    order_by=['order_date'],
    engine='MergeTree()',
    partition_by=['toYYYYMM(order_date)']
) }}

WITH customer_segments AS (
    SELECT
        customer_id,
        CASE
            WHEN SUM(sales) > 10000 THEN 'VIP'
            WHEN COUNT(order_id) > 50 THEN 'Frequent'
            ELSE 'Regular'
        END AS customer_segment
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),

aggregated_orders AS (
    SELECT
        category,
        region,
        toMonth(order_date) AS order_month,
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        AVG(discount) AS avg_discount,
        SUM(profit) AS total_profit
    FROM {{ ref('stg_orders') }}
    GROUP BY category, region, order_month
)

SELECT
    o.order_id,
    CAST(o.order_date AS DATE) AS order_date,
    o.customer_id,
    c.customer_segment,
    o.category,
    o.region,
    o.sales,
    o.quantity,
    o.discount,
    o.profit,
    a.total_sales,
    a.total_quantity,
    a.avg_discount,
    a.total_profit
FROM {{ ref('stg_orders') }} o
LEFT JOIN customer_segments c
    ON o.customer_id = c.customer_id
LEFT JOIN aggregated_orders a
    ON o.category = a.category AND o.region = a.region AND toMonth(o.order_date) = a.order_month
WHERE o.load_timestamp >= (SELECT max(load_timestamp) FROM {{ this }})
