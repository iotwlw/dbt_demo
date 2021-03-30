{{ config(materialized='view') }}

with amazon_traffic_diagnos as 
(
    select * 
    ,  coalesce ( lead (_fivetran_synced ) over(partition by isbn_13,cast(left(right(_file,10),6) as date) order by _fivetran_synced), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ source('ha_amazon','amazon_traffic_diagnos') }}
    where reporting_range = 'Daily'
),
final as
( 
select distinct
     concat(isbn_13,'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"') as business_key
    ,cast(isbn_13 as varchar(256)) as title_info_key
    ,cast(left(right(_file,10),6) as date) as trading_date
    ,isbn_13
    ,_file as record_source
    ,_fivetran_synced as dw_load_date_time
    ,conversion_rate 
    ,change_in_conversion_prior_period 
    ,change_in_conversion_last_year 
    ,unique_visitors_prior_period 
    ,unique_visitors_last_year 
    ,conversion_percentile 
    ,glance_views
    ,change_in_gv_prior_period 
    ,change_in_gv_last_year 
    ,change_in_glance_view_prior_period 
    ,_of_total_gvs 
    ,fast_track_glance_view 
    ,fast_track_glance_view_prior_period 
    ,fast_track_glance_view_last_year
    ,_fivetran_synced as effective_at
    ,expirydatetime as expired_at
    ,iff(expirydatetime = '9999-12-31',1,0) as is_latest
from amazon_traffic_diagnos
where 
--Data validations to exclude invalid ISBN
isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
) 
select * 
from final 
