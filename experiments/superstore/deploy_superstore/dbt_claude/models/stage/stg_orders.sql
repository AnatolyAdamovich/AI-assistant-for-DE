{{ config(
    materialized='view',
    tags=['stage', 'orders']
) }}

select
    -- Primary keys
    order_id,
    product_id,
    customer_id,
    
    -- Dates
    order_date,
    ship_date,
    
    -- Customer information
    customer_name,
    segment,
    
    -- Geography
    region,
    city,
    state,
    postal_code,
    
    -- Product information
    category,
    sub_category,
    product_name,
    
    -- Metrics
    sales,
    quantity,
    discount,
    profit,
    
    -- Calculated fields
    sales - discount as net_sales,
    case 
        when sales > 0 then (profit / sales) * 100
        else 0
    end as profit_margin_pct,
    
    -- Metadata
    now() as _loaded_at,
    '{{ run_started_at }}' as _dbt_run_started_at,
    '{{ invocation_id }}' as _dbt_invocation_id
    
from {{ source('exported_data', 'orders') }}