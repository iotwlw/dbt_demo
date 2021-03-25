{{ config(materialized = 'view') }} 

with daily_traffic_diagnos_sat as (
    select *
    from {{ ref('daily_traffic_diagnos_sat') }} 
),
title_info_daily_traffic_diagnos_link as (
    select *
    from {{ ref('title_info_daily_traffic_diagnos_link') }} 
),
final as (
    select distinct 
         polnk.title_info_key as title_info_business_key
        ,polnk.daily_diagnos_key as daily_diagnos_key
        ,posat.isbn_13
        ,posat.record_source
        ,posat.dw_load_date_time
        ,posat.trading_date
        -- ,posat.conversion_percentile
        ,posat.conversion_rate
        -- ,posat.fast_track_glance_view
        ,posat.glance_views
    from daily_traffic_diagnos_sat as posat
    join title_info_daily_traffic_diagnos_link as polnk 
        on polnk.daily_diagnos_key = posat.business_key
)
select *
from final
