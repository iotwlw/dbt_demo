{{ config(materialized = 'view') }} 

with weekly_preorder_sat as (
    select *
    from {{ ref('weekly_preorder_sat') }} 
),
final as (
    select distinct 
        title_info_key as title_info_business_key,
        isbn_13,
        record_source,
        dw_load_date_time,
        preorder_date,
        pre_ordered_units,
        pre_ordered_revenue,        
        average_pre_order_sales_price
    from weekly_preorder_sat
    where expired_at = '9999-12-31'    
)
select *
from final