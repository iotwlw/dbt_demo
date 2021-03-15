{{ config(materialized='view') }}

with daily_order_amazon_sat as 
(
    select * 
    from {{ ref('daily_order_amazon_sat') }}
),
final as
( 
select distinct
     business_key    
    ,isbn_13
    ,record_source
    ,dw_load_date_time
    ,order_date
    ,ordered_revenue
    ,ordered_units
from daily_order_amazon_sat
) 
select * 
from final
