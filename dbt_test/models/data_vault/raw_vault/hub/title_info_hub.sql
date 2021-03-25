{{ config(materialized='view') }}

with amazon_preorders as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_preorders') }}
),
amazon_orders as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_ordered_revenue') }}
),
amazon_sales_diagnostic_shipped_cogs as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_shipped_cogs') }}
),
amazon_sales_diagnostic_shipped_revenue as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_sales_diagnostic_shipped_revenue') }}
),
amazon_traffic_diagnos as 
(
    select * 
          ,rank() over (partition by isbn_13 order by _fivetran_synced asc) as sequencenumber
    from {{ source('ha_amazon','amazon_traffic_diagnos') }}
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
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and sequencenumber = 1

union 
select distinct
     isbn_13          as business_key
    ,isbn_13          as isbn_13
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_orders
--these are validations
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and sequencenumber = 1

union 
select distinct
     isbn_13          as business_key
    ,isbn_13          as isbn_13
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_sales_diagnostic_shipped_cogs
--these are validations
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and sequencenumber = 1

union 
select distinct
     isbn_13          as business_key
    ,isbn_13          as isbn_13
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_sales_diagnostic_shipped_revenue
--these are validations
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
and sequencenumber = 1

union 
select distinct
     isbn_13          as business_key
    ,isbn_13          as isbn_13
    ,_file            as record_source
    ,_fivetran_synced as dw_load_date_time
from amazon_traffic_diagnos
--these are validations
where isbn_13 <> '0'
and isbn_13 <> 'UNKNOWN'
and isbn_13 <> 'EISBN'
and isbn_13 <> 'ISBN-13'
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

