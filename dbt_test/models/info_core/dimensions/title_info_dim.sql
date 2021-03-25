{{ config(materialized='view') }}

with title_info_sat as 
(
    select * 
    from {{ ref('title_info_sat') }}
),
final as
( 
select distinct
     record_source  	   as record_source
    ,dw_load_date_time     as dw_load_date_time
    ,business_key		   as business_key
    ,asin		           as asin   
    ,amazon_isbn_13	       as amazon_isbn_13
    ,amazon_product_title  as amazon_product_title
    ,amazon_brand		   as amazon_brand
    ,amazon_subcategory  as amazon_subcategory
    ,amazon_category     as amazon_category
    ,amazon_author       as amazon_author
    ,amazon_binding      as amazon_binding
    ,amazon_release_date as amazon_release_date
    ,amazon_sitb_enabled as amazon_sitb_enabled 
    --biblio
    --,business_key        as business_key
    --,record_source       as record_source
    --,dw_load_date_time   as dw_load_date_time
    ,work_ref            as work_ref
    ,title     as title
    ,cover_author    as cover_author
    ,is_fiction      as is_fiction
    --,business_key        as business_key
    --,dw_load_date_time   as dw_load_date_time
    ,rrp    as rrp
    ,original_publication_date as original_publication_date
    ,isbn_13         as isbn_13
    ,binding    as binding
from title_info_sat
) 
select * 
from final

