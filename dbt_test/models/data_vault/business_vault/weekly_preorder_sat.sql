{{ config(materialized='view') }}

with amazon_preorders as 
(
    select * 
    ,  coalesce ( lead (_fivetran_synced ) over(partition by isbn_13,to_date(left(right(_file,10),6),'ddmmyy') order by _fivetran_synced), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ source('ha_amazon','amazon_preorders') }}
    where reporting_range = 'Weekly'
),
final as
( 
select distinct
     concat(isbn_13,'|"|~|"|',to_date(left(right(_file,10),6),'ddmmyy'),'|"|~|"|"amazon"') as business_key
    ,cast(isbn_13 as varchar(256)) as title_info_key
    ,to_date(left(right(_file,10),6),'ddmmyy') as preorder_date
    ,isbn_13
    ,_file as record_source
    ,_fivetran_synced as dw_load_date_time    
    ,pre_ordered_units
    ,pre_ordered_units_prior_period
    ,pre_ordered_revenue
    ,pre_ordered_revenue_prior_period
    ,average_pre_order_sales_price
    ,average_pre_order_sales_price_previous_period
    ,_fivetran_synced as effective_at
    ,expirydatetime as expired_at
    ,iff(expirydatetime = '9999-12-31',1,0) as is_latest
from amazon_preorders
where 
--Data validations to exclude invalid ISBN
isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
) 
select * 
from final 
