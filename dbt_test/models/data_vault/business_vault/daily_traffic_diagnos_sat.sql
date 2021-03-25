{{ config(materialized='view') }}

with daily_traffic_diagnos_amazon_sat as 
(
    select * 
    ,  coalesce ( lead (dw_load_date_time ) over(partition by business_key,trading_date order by dw_load_date_time), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ ref('daily_traffic_diagnos_amazon_sat') }}
),
final as
( 
select distinct
     business_key
    ,trading_date
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    -- ,conversion_percentile
    ,conversion_rate
    -- ,fast_track_glance_view
    ,glance_views
from daily_traffic_diagnos_amazon_sat
union
select distinct
     business_key
    ,trading_date
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    --,-conversion_percentile
    ,-conversion_rate
    --,-fast_track_glance_view
    ,-glance_views
from daily_traffic_diagnos_amazon_sat
where expirydatetime <> '9999-12-31'
) 
select * 
from final 
