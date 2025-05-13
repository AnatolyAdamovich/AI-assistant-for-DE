{% set source_table = source('exported_data', 'customers') %}

select
    region_id,
    registration_date,
    customer_id,
    now() as loaded_at
from {{ source_table }}