{{ config(materialized = 'view') }} 

with weekly_traffic_diagnos_sat as (
    select *
    from {{ ref('weekly_traffic_diagnos_sat') }} 
),
final as (
    select distinct 
         title_info_key as title_info_business_key
        ,isbn_13
        ,record_source
        ,dw_load_date_time
        ,trading_date
        ,conversion_rate
        ,glance_views
    from weekly_traffic_diagnos_sat
    where expired_at = '9999-12-31'
)
select *
from final
