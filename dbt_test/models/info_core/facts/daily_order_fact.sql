{{ config(materialized = 'table') }} 

with daily_order_sat as (
    select *
    from {{ ref('daily_order_sat') }} 
),
final as (
    select distinct
         title_info_key as title_info_key
        ,isbn_13
        ,record_source
        ,dw_load_date_time
        ,order_date
        ,ordered_units
        ,ordered_revenue        
        ,average_sales_price
        ,glance_views
        ,conversion_rate        
    from daily_order_sat 
    --where expired_at = '9999-12-31'
)
select *
from final

