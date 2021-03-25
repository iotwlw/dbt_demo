{{ config(materialized='view') }}

with amazon_traffic_diagnos as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_traffic_diagnos') }}
),
final as
( 
select distinct
     cast(isbn_13 as varchar(256)) as title_info_key
    ,concat(cast(isbn_13 as varchar(256)),'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"') as daily_diagnos_key
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_traffic_diagnos
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and reporting_range = 'Daily'
and sequencenumber = 1
) 
select * 
from final

