{{ config(materialized='view') }}

with amazon_preorders as 
(
    select *  
          ,rank() over (partition by asin,parent_asin,ean,isbn_13,product_title    
                                ,brand,subcategory,category,author_artist,binding,release_date,sitb_enabled_ 
                    order by _fivetran_synced desc) as sequencenumber 
    from {{ source('ha_amazon','amazon_preorders') }}
),
amazon_orders as 
(
    select *          
    ,rank() over (partition by asin,parent_asin,ean,isbn_13,product_title    
                              ,brand,subcategory,category,author_artist,binding,release_date 
                  order by _fivetran_synced desc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_ordered_revenue_all') }}
),
final as
( 
select distinct
     _file as record_source
    ,_fivetran_synced  as dw_load_date_time
    ,isbn_13		   as business_key
    ,asin		       as asin
    ,parent_asin	   as parent_asin
    ,ean			   as ean
    ,isbn_13		   as isbn_13
    ,product_title     as product_title
    ,brand		       as brand
    ,subcategory	   as subcategory
    ,category	       as category
    ,author_artist     as author_artist
    ,binding		   as binding
    ,release_date      as release_date
    ,sitb_enabled_     as sitb_enabled
from amazon_preorders
where isbn_13 <> '0'
and isbn_13 <> 'unknown'
and isbn_13 <> 'eisbn'
and isbn_13 <> 'isbn-13'
and sequencenumber =1

union
select distinct
     _file as record_source
    ,_fivetran_synced  as dw_load_date_time
    ,isbn_13		   as business_key
    ,asin		       as asin
    ,parent_asin	   as parent_asin
    ,ean			   as ean
    ,isbn_13		   as isbn_13
    ,product_title     as product_title
    ,brand		       as brand
    ,subcategory	   as subcategory
    ,category	       as category
    ,author_artist     as author_artist
    ,binding		   as binding
    ,release_date      as release_date
    ,'unknown'         as sitb_enabled    
from amazon_orders
where isbn_13 <> '0'
and sequencenumber =1
-- and isbn_13 <> 'unknown'
-- and isbn_13 <> 'eisbn'
-- and isbn_13 <> 'isbn-13'
) 
select * 
from final

