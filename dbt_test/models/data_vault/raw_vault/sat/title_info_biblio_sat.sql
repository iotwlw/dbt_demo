{{ config(materialized='view') }}

with work as 
(
    select * 
          , coalesce ( lead (_modified ) over(partition by work_id order by _modified), cast( '9999-12-31' as datetime)  ) as expirydatetime
    from {{ source('ha_biblio','work') }}
),
edition as 
(
   select * 
        ,  coalesce ( lead (_modified ) over(partition by work_id order by _modified), cast( '9999-12-31' as datetime)  ) as expirydatetime
   from {{ source('ha_biblio','edition') }}
),
binding as 
(
   select * 
        ,  coalesce ( lead (_modified ) over(partition by binding_id order by _modified), cast( '9999-12-31' as datetime)  ) as expirydatetime
   from {{ source('ha_biblio','binding') }}
),
final as
( 
select distinct
     w.work_ref as business_key
    ,'biblio' as record_source
    ,w._modified as dw_load_date_time
    ,w.work_ref as work_ref
    ,w.work_covertitle as work_covertitle
    ,w.work_coverauthor as work_coverauthor
    ,w.work_isfiction as work_isfiction
    ,e.edition_ean as business_key_e
    ,e._modified as dw_load_date_time_e
    ,e.edition_pubprice
    ,e.edition_origpubdate
    ,e.edition_ean
    ,b.binding_longname
from work w
left join edition e 
    on w.work_id = e.work_id 
    and e._modified >= w._modified
    and e.expirydatetime <= w.expirydatetime
left join binding b
    on  e.binding_id = b.binding_id  
    and b._modified >= e._modified
    and b.expirydatetime <= e.expirydatetime 
where  w._modified >= dateadd(day,{{ var("no_of_days") }},getdate()) 
) 
select * 
from final




