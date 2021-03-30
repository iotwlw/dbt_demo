{{ config(materialized = 'table') }} 

with daily_order_fact as (
    select *
    from {{ ref('daily_order_fact') }} 
),
daily_preorder_fact as (
    select *
    from {{ ref('daily_preorder_fact') }} 
),
daily_shipped_cogs_fact as (
    select *
    from {{ ref('daily_shipped_cogs_fact') }} 
),
daily_shipped_revenue_fact as (
    select *
    from {{ ref('daily_shipped_revenue_fact') }} 
),
isbn_info as
(
    select isbn_13
          ,preorder_date as date
    from daily_preorder_fact
    union
    select isbn_13
          ,order_date as date
    from daily_order_fact
    union
    select isbn_13
          ,ship_date as date
    from daily_shipped_cogs_fact
    union
    select isbn_13
          ,ship_date as date
    from daily_shipped_revenue_fact
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
    left join daily_preorder_fact po on ii.isbn_13 = po.isbn_13 and ii.date = po.preorder_date 
    left join daily_order_fact dof on ii.isbn_13 = dof.isbn_13 and ii.date = dof.order_date
    left join daily_shipped_cogs_fact as sc on ii.isbn_13 = sc.isbn_13 and ii.date = sc.ship_date
    left join daily_shipped_revenue_fact as sr on ii.isbn_13 = sr.isbn_13 and ii.date = sr.ship_date  
)
select *
from final
