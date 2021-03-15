{{ config(materialized='view') }}

with amazon_preorders as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_preorders') }}
),
edition as 
(
    select * 
    from {{ source('ha_biblio','edition') }}
),
final as
( 
select distinct
     isbn_13          as business_key
    ,isbn_13          as isbn_13
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_preorders
--these are validations
where isbn_13 <> '0'
and isbn_13 <> 'unknown'
and isbn_13 <> 'eisbn'
and isbn_13 <> 'isbn-13'
and sequencenumber = 1
union 
select distinct
     edition_ean      as business_key
    ,edition_ean      as isbn_13
    ,'biblio'         as record_source
    ,_modified        as dw_load_date_time
from edition
) 
select * 
from final

