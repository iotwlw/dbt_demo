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
    ,average_pre_order_sales_price
from amazon_preorders
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and reporting_range = 'Daily'
-- and 
-- (
--    pre_ordered_revenue > 0
-- or pre_ordered_units > 0
-- or average_pre_order_sales_price > 0
-- )
) 
select * 
from final


