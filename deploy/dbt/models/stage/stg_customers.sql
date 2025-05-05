-- models/stg_customers.sql

{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        customer_id,
        region_id,
        registration_date,
        now() AS load_timestamp
    FROM
        {{ source('exported_data', 'customers') }}
)

SELECT *
FROM source_data;