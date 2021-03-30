{{ config(materialized = 'view') }} 

with daily_shipped_cogs_sat as (
    select *
    from {{ ref('daily_shipped_cogs_sat') }} 
),
final as (
    select distinct 
         title_info_key as title_info_business_key
        ,isbn_13
        ,record_source
        ,dw_load_date_time
        ,ship_date
        ,shipped_units
        ,shipped_cogs        
        ,customer_returns
        ,free_replacements
    from daily_shipped_cogs_sat
    where expired_at = '9999-12-31'
)
select *
from final
