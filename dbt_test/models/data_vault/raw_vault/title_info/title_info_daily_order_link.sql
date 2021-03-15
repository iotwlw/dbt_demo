{{ config(materialized='view') }}

with amazon_orders as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_ordered_revenue_all') }}
),
final as
( 
select distinct
     isbn_13 as title_info_key
    ,concat(isbn_13,'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"') as daily_order_key
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_orders
where isbn_13 <> '0'
and isbn_13 <> 'unknown'
and isbn_13 <> 'eisbn'
and isbn_13 <> 'isbn-13'
and sequencenumber = 1
) 
select * 
from final

