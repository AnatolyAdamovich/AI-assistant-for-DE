-- dbt model: int_customers_enriched

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    engine='ReplacingMergeTree(processed_at)',
    order_by=['customer_id'],
    partition_by=['region_id']
) }}

WITH cleaned_data AS (
    SELECT
        customer_id,
        name,
        region_id,
        age,
        processed_at
    FROM {{ ref('stg_customers') }}
    WHERE age >= 18  -- Удаляем записи с некорректным возрастом клиента
),
joined_data AS (
    SELECT
        c.customer_id,
        c.name,
        c.region_id,
        c.age,
        c.processed_at,
        os.total_orders,
        os.total_amount,
        os.first_order_date,
        os.last_order_date
    FROM cleaned_data AS c
    LEFT JOIN {{ ref('int_orders_summary') }} AS os
    ON c.customer_id = os.customer_id
)
SELECT *
FROM joined_data
