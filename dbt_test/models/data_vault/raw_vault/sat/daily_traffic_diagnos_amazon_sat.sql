{{ config(materialized='view') }}

with amazon_traffic_diagnos as 
(
    select * from {{ source('ha_amazon','amazon_traffic_diagnos') }}
),
final as
( 
select distinct
     concat(isbn_13,'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"') as business_key
    ,cast(left(right(_file,10),6) as date) as trading_date
    ,isbn_13
    ,_file as record_source
    ,_fivetran_synced as dw_load_date_time
    -- ,conversion_percentile
    ,conversion_rate
    -- ,fast_track_glance_view
    ,glance_views
from amazon_traffic_diagnos
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and reporting_range = 'Daily'
) 
select * 
from final




