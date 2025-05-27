{{ config(
    materialized='view'
) }}

with delivery_metrics as (
    select
        delivery_status,
        count(*) as orders_count,
        avg(delivery_days) as avg_delivery_days,
        min(delivery_days) as min_delivery_days,
        max(delivery_days) as max_delivery_days,
        quantile(0.5)(delivery_days) as median_delivery_days,
        quantile(0.95)(delivery_days) as p95_delivery_days
    from {{ ref('int_orders_enriched') }}
    where delivery_status != 'Not Shipped'
    group by delivery_status
),

overall_metrics as (
    select
        'Overall' as delivery_status,
        count(*) as orders_count,
        avg(delivery_days) as avg_delivery_days,
        min(delivery_days) as min_delivery_days,
        max(delivery_days) as max_delivery_days,
        quantile(0.5)(delivery_days) as median_delivery_days,
        quantile(0.95)(delivery_days) as p95_delivery_days
    from {{ ref('int_orders_enriched') }}
    where delivery_status != 'Not Shipped'
)

select * from delivery_metrics
union all
select * from overall_metrics
order by 
    case delivery_status
        when 'Overall' then 1
        when 'Fast' then 2
        when 'Standard' then 3
        when 'Slow' then 4
    end