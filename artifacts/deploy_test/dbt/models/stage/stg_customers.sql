-- models/stage/stg_customers.sql
{{ config(
    materialized='view',
    tags=['stage']
) }}

WITH enriched_customers AS (
    SELECT
        customer_id,
        name,
        region_id,
        age,
        now() AS processed_at -- добавляем метку времени обработки
    FROM
        {{ source('exported_data', 'customers') }}
)

SELECT *
FROM enriched_customers;