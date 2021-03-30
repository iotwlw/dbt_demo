{{ config(materialized = 'view') }} 

with daily_amazon_all_sat as (
    select *
    from {{ ref('daily_amazon_all_sat') }} 
),
final as (
    select 
         date         
        ,frequency        
        ,pre_ordered_units
        ,pre_ordered_revenue
        ,average_pre_order_sales_price
        ,ordered_revenue
        ,ordered_units
        ,average_sales_price
        ,glance_views
        ,conversion_rate
        ,shipped_cogs
        ,shipped_units
        ,shipped_units_of_total
        ,shipped_revenue
        ,free_replacements
        ,customer_returns
    from daily_amazon_all_sat     
)
select *
from final
