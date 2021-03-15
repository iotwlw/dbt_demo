{{ config(materialized='view') }}

with daily_preorder_amazon_sat as 
(
    select * 
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
from daily_preorder_amazon_sat
) 
select * 
from final
