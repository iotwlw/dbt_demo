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
    from {{ source('ha_amazon','amazon_sales_diagnostic_ordered_revenue') }}
),
amazon_sales_diagnostic_shipped_cogs as 
(
    select *          
    ,rank() over (partition by asin,parent_asin,ean,isbn_13,product_title    
                              ,brand,subcategory,category,author_artist,binding,release_date 
                  order by _fivetran_synced desc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_shipped_cogs') }}
),
amazon_sales_diagnostic_shipped_revenue as 
(
    select *          
    ,rank() over (partition by asin,parent_asin,ean,isbn_13,product_title    
                              ,brand,subcategory,category,author_artist,binding,release_date 
                  order by _fivetran_synced desc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_shipped_revenue') }}
),
amazon_traffic_diagnos as 
(
    select *          
    ,rank() over (partition by asin,parent_asin,ean,isbn_13,product_title    
                              ,brand,subcategory,category,author_artist,binding
                  order by _fivetran_synced desc) as sequencenumber
    from {{ source('ha_amazon','amazon_traffic_diagnos') }}
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
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and sequencenumber =1

union
select distinct
     _file as record_source
    ,_fivetran_synced  as dw_load_date_time
    ,cast(isbn_13 as varchar(256))   as business_key
    ,asin		       as asin
    ,parent_asin	   as parent_asin
    ,ean			   as ean
    ,cast(isbn_13 as varchar(256))   as isbn_13
    ,product_title     as product_title
    ,brand		       as brand
    ,subcategory	   as subcategory
    ,category	       as category
    ,author_artist     as author_artist
    ,binding		   as binding
    ,release_date      as release_date
    ,'UNKNOWN'         as sitb_enabled    
from amazon_orders
where cast(isbn_13 as varchar(256)) <> '0'
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
    ,'UNKNOWN'         as sitb_enabled
from amazon_sales_diagnostic_shipped_cogs
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
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
    ,'UNKNOWN'         as sitb_enabled
from amazon_sales_diagnostic_shipped_revenue
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
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
    ,cast( '9999-12-31' as date)  as release_date
    ,'UNKNOWN'         as sitb_enabled
from amazon_traffic_diagnos
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and sequencenumber =1
) 
select * 
from final

