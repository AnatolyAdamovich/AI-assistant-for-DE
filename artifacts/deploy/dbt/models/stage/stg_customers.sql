-- models/stg_customers.sql

{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        customer_id,
        name,
        region_id,
        age,
        now() AS load_timestamp
    FROM
        {{ source('exported_data', 'customers') }}
)

SELECT * FROM source_data;
