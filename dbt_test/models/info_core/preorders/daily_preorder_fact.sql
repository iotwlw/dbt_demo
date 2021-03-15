{{ config(materialized = 'view') }} 

with daily_preorder_sat as (
    select *
    from {{ ref('daily_preorder_sat') }} 
),
title_info_daily_preorder_link as (
    select *
    from {{ ref('title_info_daily_preorder_link') }} 
),
final as (
    select distinct 
        polnk.title_info_key as title_info_business_key,
        polnk.daily_preorder_key as daily_preorder_key,
        posat.isbn_13,
        posat.record_source,
        posat.dw_load_date_time,
        posat.preorder_date,
        posat.pre_ordered_revenue,
        posat.pre_ordered_units
    from daily_preorder_sat as posat
    join title_info_daily_preorder_link as polnk 
        on polnk.daily_preorder_key = posat.business_key
)
select *
from final
