{{ config(
    materialized='incremental',
    order_by='order_id',
    engine='MergeTree()'
) }}

SELECT
    order_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(ship_date AS DATE) AS ship_date,
    customer_id,
    customer_name,
    segment,
    region,
    city,
    state,
    postal_code,
    product_id,
    category,
    sub_category,
    product_name,
    CAST(sales AS Decimal32(2)) AS sales_amount,
    quantity,
    discount,
    profit,
    etl_loaded_at
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE etl_loaded_at > (SELECT MAX(etl_loaded_at) FROM {{ this }})
{% endif %}