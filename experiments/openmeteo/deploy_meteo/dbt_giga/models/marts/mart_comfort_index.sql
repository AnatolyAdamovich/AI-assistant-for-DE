-- dbt model for mart_comfort_index

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='date',
    partition_by='toYYYYMM(date)'
) }}

SELECT
    date,
    comfort_index
FROM
    {{ ref('int_comfort_index') }}