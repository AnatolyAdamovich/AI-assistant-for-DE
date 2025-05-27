-- dbt model: int_geo_enriched_customers

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    order_by=['region', 'city'],
    engine='MergeTree()',
    partition_by=['region']
) }}

SELECT
    customer_id,
    customer_name,
    region,
    city,
    state,
    postal_code,
    latitude,
    longitude
FROM (
    SELECT
        customer_id,
        customer_name,
        region,
        city,
        state,
        postal_code,
        -- Mock geo-enrichment logic
        CASE
            WHEN region = 'North' THEN 45.0
            WHEN region = 'South' THEN 30.0
            ELSE 0.0
        END AS latitude,
        CASE
            WHEN region = 'North' THEN -93.0
            WHEN region = 'South' THEN -80.0
            ELSE 0.0
        END AS longitude
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id, customer_name, region, city, state, postal_code
)
WHERE load_timestamp >= (SELECT max(load_timestamp) FROM {{ this }})
