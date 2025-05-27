SELECT
    category,
    region,
    SUM(total_sales - total_sales * 0.2) AS profit,
    (SUM(total_sales - total_sales * 0.2) / SUM(total_sales)) AS margin
FROM {{ ref('int_orders_aggregated') }}
GROUP BY category, region