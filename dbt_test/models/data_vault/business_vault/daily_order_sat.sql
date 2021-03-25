{{ config(materialized='view') }}

with daily_order_amazon_sat as 
(
    select * 
        ,  coalesce ( lead (dw_load_date_time ) over(partition by business_key,order_date order by dw_load_date_time), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ ref('daily_order_amazon_sat') }}
),
final as
( 
select distinct
     business_key    
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    ,order_date
    ,ordered_revenue
    ,ordered_units
    ,average_sales_price
    ,glance_views
    -- ,conversion_rate
    -- ,rep_oos
    -- ,lbb_price
from daily_order_amazon_sat
union
select distinct
      business_key    
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    ,order_date
    ,-ordered_revenue
    ,-ordered_units
    ,-average_sales_price
    ,-glance_views
    -- ,-conversion_rate
    -- ,-rep_oos
    -- ,-lbb_price
from daily_order_amazon_sat
where expirydatetime <> '9999-12-31'
) 
select * 
from final
