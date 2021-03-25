{{ config(materialized = 'view') }} 

with daily_shipped_cogs_sat as (
    select *
    from {{ ref('daily_shipped_cogs_sat') }} 
),
title_info_daily_shipped_cogs_link as (
    select *
    from {{ ref('title_info_daily_shipped_cogs_link') }} 
),
final as (
    select distinct 
         polnk.title_info_key as title_info_business_key
        ,polnk.daily_shipped_key as daily_shipped_key
        ,posat.isbn_13
        ,posat.record_source
        ,posat.dw_load_date_time
        ,posat.ship_date
        -- ,posat.ordered_units
        ,posat.shipped_cogs
        ,posat.shipped_units
        -- ,posat.average_sales_price
        -- ,posat.conversion_rate
        ,posat.customer_returns
        ,posat.free_replacements
        -- ,posat.glance_views
        -- ,posat.lbb_price
        -- ,posat.rep_oos
    from daily_shipped_cogs_sat as posat
    join title_info_daily_shipped_cogs_link as polnk 
        on polnk.daily_shipped_key = posat.business_key
)
select *
from final
