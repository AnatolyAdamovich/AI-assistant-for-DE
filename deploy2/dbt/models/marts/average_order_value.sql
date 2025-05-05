-- Average Order Value Model

{{ config(
    materialized='incremental',
    unique_key='order_id',
    engine='ReplacingMergeTree()',
    order_by=['order_id'],
    partition_by=['toYYYYMM(timestamp)']
) }}

SELECT
    order_id,
    AVG(money) AS average_order_value
FROM {{ ref('int_enriched_orders') }}
GROUP BY order_id;