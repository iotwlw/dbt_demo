{{ config(materialized = 'incremental') }} 

with daily_order_sat as (
    select *
    from {{ ref('daily_order_sat') }}
    where expired_at = '9999-12-31' 
),
daily_preorder_sat as (
    select *
    from {{ ref('daily_preorder_sat') }} 
    where expired_at = '9999-12-31'
),
daily_shipped_cogs_sat as (
    select *
    from {{ ref('daily_shipped_cogs_sat') }} 
    where expired_at = '9999-12-31'
),
daily_shipped_revenue_sat as (
    select *
    from {{ ref('daily_shipped_revenue_sat') }} 
    where expired_at = '9999-12-31'
),
isbn_info as
(
    select isbn_13
          ,preorder_date as date
    from daily_preorder_sat
    union
    select isbn_13
          ,order_date as date
    from daily_order_sat
    union
    select isbn_13
          ,ship_date as date
    from daily_shipped_cogs_sat
    union
    select isbn_13
          ,ship_date as date
    from daily_shipped_revenue_sat
),
final as (
    select 
         ii.date         
        ,'daily' as frequency        
        ,po.pre_ordered_units
        ,po.pre_ordered_revenue
        ,po.average_pre_order_sales_price
        ,dof.ordered_revenue
        ,dof.ordered_units
        ,dof.average_sales_price
        ,dof.glance_views
        ,dof.conversion_rate
        ,sc.shipped_cogs
        ,sr.shipped_units
        ,sr.shipped_units_of_total
        ,sr.shipped_revenue
        ,sr.free_replacements
        ,sr.customer_returns
    from isbn_info ii
    left join daily_preorder_sat po on ii.isbn_13 = po.isbn_13 and ii.date = po.preorder_date 
    left join daily_order_sat dof on ii.isbn_13 = dof.isbn_13 and ii.date = dof.order_date
    left join daily_shipped_cogs_sat as sc on ii.isbn_13 = sc.isbn_13 and ii.date = sc.ship_date
    left join daily_shipped_revenue_sat as sr on ii.isbn_13 = sr.isbn_13 and ii.date = sr.ship_date  
)
select *
from final
