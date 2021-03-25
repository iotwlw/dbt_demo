{{ config(materialized='view') }}

with title_info_amazon_sat as 
(
    select * 
    from {{ ref('title_info_amazon_sat') }}
),
title_info_biblio_sat as 
(
    select * 
    from {{ ref('title_info_biblio_sat') }}
),
final as
( 
select distinct
     concat(a.record_source,b.record_source)	   as record_source
    --,a.dw_load_date_time   as dw_load_date_time
    ,a.isbn_13		       as business_key
    ,a.asin		           as asin
    ,a.isbn_13		       as amazon_isbn_13
    ,a.product_title       as amazon_product_title
    ,a.brand		       as amazon_brand
    ,a.subcategory	       as amazon_subcategory
    ,a.category	           as amazon_category
    ,a.author_artist       as amazon_author
    ,a.binding		       as amazon_binding
    ,a.release_date        as amazon_release_date
    ,a.sitb_enabled        as amazon_sitb_enabled
    --biblio
    --,b.business_key        as business_key
    --,b.record_source       as record_source
    ,b.dw_load_date_time   as dw_load_date_time
    ,b.work_ref            as work_ref
    ,b.work_covertitle     as title
    ,b.work_coverauthor    as cover_author
    ,b.work_isfiction      as is_fiction
    --,b.business_key        as business_key
    --,b.dw_load_date_time   as dw_load_date_time
    ,b.edition_pubprice    as rrp
    ,b.edition_origpubdate as original_publication_date
    ,b.edition_ean         as isbn_13
    ,b.binding_longname    as binding
from title_info_amazon_sat a
left join title_info_biblio_sat b 
    on a.business_key = b.edition_ean
) 
select * 
from final

