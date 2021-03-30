{{ config(materialized='view') }}

with title_info_sat as 
(
    select * 
    from {{ ref('title_info_sat') }}
),
final as
( 
select distinct
     record_source  	   
    ,dw_load_date_time     
    ,business_key		  
    ,asin		           
    ,amazon_isbn_13	       
    ,amazon_product_title  
    ,amazon_brand		   
    ,amazon_subcategory    
    ,amazon_category      
    ,amazon_author         
    ,amazon_binding        
    ,amazon_release_date   
    ,amazon_sitb_enabled   
    --biblio
    --,business_key        as business_key
    --,record_source       as record_source
    --,dw_load_date_time   as dw_load_date_time
    ,work_ref            
    ,title    
    ,cover_author    
    ,is_fiction      
    --,business_key        as business_key
    --,dw_load_date_time   as dw_load_date_time
    ,rrp    
    ,original_publication_date 
    ,isbn_13        
    ,Months_until_publication 
    ,binding    
from title_info_sat
) 
select * 
from final

