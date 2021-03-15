{{ config(materialized = 'view') }} 

with daily_order_sat as (
    select *
    from {{ ref('daily_order_sat') }} 
),
title_info_daily_order_link as (
    select *
    from {{ ref('title_info_daily_order_link') }} 
),
final as (
    select distinct 
        polnk.title_info_key as title_info_business_key,
        polnk.daily_order_key as daily_order_key,
        posat.isbn_13,
        posat.record_source,
        posat.dw_load_date_time,
        posat.order_date,
        posat.ordered_revenue,
        posat.ordered_units
    from daily_order_sat as posat
    join title_info_daily_order_link as polnk 
        on polnk.daily_order_key = posat.business_key
)
select *
from final
