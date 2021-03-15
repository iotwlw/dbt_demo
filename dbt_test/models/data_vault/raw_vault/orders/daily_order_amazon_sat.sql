{{ config(materialized='view') }}

with amazon_orders as 
(
    select * 
    from {{ source('ha_amazon','amazon_sales_diagnostic_ordered_revenue_all') }}
),
final as
( 
select distinct
     concat(isbn_13,'|"|~|"|',left(right(_file,10),6),'|"|~|"|"amazon"')  as business_key
    ,isbn_13                              as isbn_13
    ,_file                                as record_source
    ,_modified                            as dw_load_date_time
    ,cast(left(right(_file,10),6) as date) as order_date
    ,ordered_revenue                      as ordered_revenue
    ,ordered_revenue_of_total             as ordered_revenue_of_total
    ,ordered_revenue_prior_period         as ordered_revenue_prior_period
    ,ordered_revenue_last_year            as ordered_revenue_last_year
    ,ordered_units                        as ordered_units
    ,ordered_units_of_total               as ordered_units_of_total
    ,ordered_units_prior_period           as ordered_units_prior_period
    ,ordered_units_last_year              as ordered_units_last_year
    ,sub_category_sales_rank_             as sub_category_sales_rank
    ,average_sales_price                  as average_sales_price
    ,average_sales_price_prior_period     as average_sales_price_prior_period
    ,glance_views                         as glance_views
    ,change_in_glance_view_prior_period   as change_in_glance_view_prior_period
    ,change_in_gv_last_year               as change_in_gv_last_year
    ,conversion_rate                      as conversion_rate
    ,rep_oos                              as rep_oos
    ,rep_oos_of_total                     as rep_oos_of_total
    ,rep_oos_prior_period                 as rep_oos_prior_period
    --,lbb_price                            as lbb_price
from amazon_orders
where isbn_13 <> '0'
and isbn_13 <> 'unknown'
and isbn_13 <> 'eisbn'
and isbn_13 <> 'isbn-13'
) 
select * 
from final


