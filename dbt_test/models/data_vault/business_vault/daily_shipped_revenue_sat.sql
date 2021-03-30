{{ config(materialized='view') }}

with amazon_shipped_revenue as 
(
    select * 
    ,  coalesce ( lead (_fivetran_synced ) over(partition by isbn_13,cast(left(right(_file,10),6) as date) order by _fivetran_synced), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ source('ha_amazon','amazon_sales_diagnostic_shipped_revenue') }}
    where reporting_range = 'Daily'
),
final as
( 
select distinct
     concat(isbn_13,'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"') as business_key
    ,cast(isbn_13 as varchar(256)) as title_info_key
    ,cast(left(right(_file,10),6) as date) as ship_date
    ,isbn_13
    ,_file as record_source
    ,_fivetran_synced as dw_load_date_time     
    ,shipped_revenue
    ,shipped_revenue_of_total
    ,shipped_revenue_prior_period
    ,shipped_revenue_last_year
    ,shipped_units
    ,shipped_units_of_total
    ,shipped_units_prior_period
    ,shipped_units_last_year
    ,ordered_units
    ,ordered_units_of_total
    ,ordered_units_prior_period
    ,ordered_units_last_year
    ,customer_returns
    ,free_replacements
    ,sub_category_sales_rank_ as sub_category_sales_rank
    ,sub_category_better_worse_ as sub_category_better_worse
    ,average_sales_price
    ,average_sales_price_prior_period
    ,glance_views
    ,change_in_gv_prior_period
    ,change_in_glance_view_prior_period
    ,change_in_gv_last_year
    ,conversion_rate
    ,rep_oos
    ,rep_oos_of_total
    ,rep_oos_prior_period
    ,lbb_price_ as lbb_price
    ,_fivetran_synced as effective_at
    ,expirydatetime as expired_at
    ,iff(expirydatetime = '9999-12-31',1,0) as is_latest
from amazon_shipped_revenue
where 
--Data validations to exclude invalid ISBN
isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
) 
select * 
from final 
