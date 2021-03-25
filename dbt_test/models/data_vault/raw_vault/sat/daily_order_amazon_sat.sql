{{ config(materialized='view') }}

with amazon_orders as 
(
    select * 
    from {{ source('ha_amazon','amazon_sales_diagnostic_ordered_revenue') }}
),
final as
( 
select distinct
     concat(cast(isbn_13 as varchar(256)),'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"')  as business_key
    ,cast(isbn_13 as varchar(256))        as isbn_13
    ,_file                                as record_source
    ,_modified                            as dw_load_date_time
    ,cast(left(right(_file,10),6) as date) as order_date
    ,ordered_revenue                      as ordered_revenue
    ,ordered_units                        as ordered_units
    ,average_sales_price                  as average_sales_price
    ,glance_views                         as glance_views
    --,conversion_rate                      as conversion_rate
    --,rep_oos                              as rep_oos
    --,lbb_price_                           as lbb_price
from amazon_orders
where cast(isbn_13 as varchar(256)) <> '0'
and cast(isbn_13 as varchar(256)) <> 'UNKNOWN'
and cast(isbn_13 as varchar(256)) <> 'EISBN'
and cast(isbn_13 as varchar(256)) <> 'ISBN-13'
and reporting_range = 'Daily'
) 
select * 
from final



