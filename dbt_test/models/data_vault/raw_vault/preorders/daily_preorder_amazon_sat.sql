{{ config(materialized='view') }}

with amazon_preorders as 
(
    select * from {{ source('ha_amazon','amazon_preorders') }}
),
final as
( 
select distinct
     concat(isbn_13,'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"') as business_key
    ,cast(left(right(_file,10),6) as date) as preorder_date
    ,isbn_13
    ,_file as record_source
    ,_fivetran_synced as dw_load_date_time
    ,pre_ordered_revenue
    ,pre_ordered_units
from amazon_preorders
where isbn_13 <> '0'
and isbn_13 <> 'unknown'
and isbn_13 <> 'eisbn'
and isbn_13 <> 'isbn-13'
) 
select * 
from final


