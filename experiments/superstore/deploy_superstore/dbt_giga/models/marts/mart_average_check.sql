SELECT
    customer_segment,
    SUM(total_sales) / COUNT(DISTINCT order_id) AS average_check
FROM {{ ref('int_orders_aggregated') }}
JOIN {{ ref('int_customer_segments') }} USING (customer_id)
GROUP BY customer_segment