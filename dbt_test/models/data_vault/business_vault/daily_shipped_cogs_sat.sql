{{ config(materialized='view') }}

with daily_shipped_cogs_amazon_sat as 
(
    select * 
    ,  coalesce ( lead (dw_load_date_time ) over(partition by business_key,ship_date order by dw_load_date_time), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ ref('daily_shipped_cogs_amazon_sat') }} 
),
final as
( 
select distinct
     business_key
    ,ship_date
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    ,ordered_units
    ,shipped_cogs        
    ,shipped_units    
    ,customer_returns
    ,free_replacements
    -- ,average_sales_price 
    -- ,conversion_rate
    -- ,glance_views
    -- ,lbb_price
    -- ,rep_oos
from daily_shipped_cogs_amazon_sat
union
select distinct
     business_key
    ,ship_date
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    ,-ordered_units
    ,-shipped_cogs        
    ,-shipped_units     
    ,-customer_returns
    ,-free_replacements
    -- ,-average_sales_price   
    -- ,-conversion_rate
    -- ,-glance_views
    -- ,-lbb_price
    -- ,-rep_oos
from daily_shipped_cogs_amazon_sat
where expirydatetime <> '9999-12-31'
) 
select * 
from final 
