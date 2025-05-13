{{ config(materialized='view') }}

SELECT *
FROM {{ source('exported_data', 'orders_last_data') }}