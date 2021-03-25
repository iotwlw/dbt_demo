{{ config(materialized='view') }}

with amazon_orders as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_ordered_revenue') }}
),
final as
( 
select distinct
     cast(isbn_13 as varchar(256)) as title_info_key
    ,concat(cast(isbn_13 as varchar(256)),'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"') as daily_order_key
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_orders
where cast(isbn_13 as varchar(256)) <> '0'
and cast(isbn_13 as varchar(256)) <> 'UNKNOWN'
and cast(isbn_13 as varchar(256)) <> 'EISBN'
and cast(isbn_13 as varchar(256)) <> 'ISBN-13'
and reporting_range = 'Daily'
and sequencenumber = 1
) 
select * 
from final

