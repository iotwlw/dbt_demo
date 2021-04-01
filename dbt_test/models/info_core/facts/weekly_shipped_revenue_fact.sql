{{ config(materialized = 'view') }} 

with weekly_shipped_revenue_sat as (
    select *
    from {{ ref('weekly_shipped_revenue_sat') }} 
),
final as (
    select distinct 
         title_info_key as title_info_business_key
        ,isbn_13
        ,record_source
        ,dw_load_date_time
        ,ship_date
        ,shipped_revenue        
        ,shipped_units
        ,shipped_units_of_total
        ,customer_returns
        ,free_replacements       
    from weekly_shipped_revenue_sat
    where expired_at = '9999-12-31'
)
select *
from final
