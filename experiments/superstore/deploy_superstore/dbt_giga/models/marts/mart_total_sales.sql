SELECT
    category,
    region,
    order_month,
    SUM(total_sales) AS total_sales
FROM {{ ref('int_orders_aggregated') }}
GROUP BY category, region, order_month
ORDER BY order_month