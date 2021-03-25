{{ config(materialized='view') }}

with daily_preorder_amazon_sat as 
(
    select * 
    ,  coalesce ( lead (dw_load_date_time ) over(partition by business_key,preorder_date order by dw_load_date_time), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ ref('daily_preorder_amazon_sat') }}
),
final as
( 
select distinct
     business_key
    ,preorder_date
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    ,pre_ordered_revenue
    ,pre_ordered_units
    ,average_pre_order_sales_price
from daily_preorder_amazon_sat
union
select distinct
     business_key
    ,preorder_date
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    ,-pre_ordered_revenue
    ,-pre_ordered_units
    ,-average_pre_order_sales_price
from daily_preorder_amazon_sat
where expirydatetime <> '9999-12-31'
) 
select * 
from final 
