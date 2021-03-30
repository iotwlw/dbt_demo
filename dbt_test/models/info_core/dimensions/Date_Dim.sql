{{ config(materialized = 'view') }} 

with date_dim as (
    select *
    from {{ source('ha_sap','date_dim') }}
),
final as 
(
select a.*
    ,case when a.year = date_part('year',(current_date)) then 'y' else 'n' end as is_current_year 
    ,case when a.year = date_part('year',(current_date)) - 1 then 'y' else 'n' end as is_previous_year 
    ,case when a.year = date_part('year',(current_date)) - 1 then 'y' else 'n' end as is_last_completed_year
    ,case when a.year = date_part('year',(current_date)) - 2 then 'y' else 'n' end as is_2_completed_years_back
    ,case when a.year = date_part('year',(current_date)) - 3 then 'y' else 'n' end as is_3_completed_years_back
    ,case when a.year = date_part('year',(current_date)) and date_part('month',(a.date)) = date_part('month',(current_date)) then 'y' else 'n' end as is_current_month
    ,case when date_part('month',(current_date)) - 1 = 0 
            then case when a.year = date_part('year',(current_date)) -1  and date_part('month',(a.date)) = 12 
                        then 'y' else 'n' 
                end
        else case when a.year = date_part('year',(current_date)) and date_part('month',(a.date)) = date_part('month',(current_date)) - 1 
                        then 'y' 
                    else 'n' 
            end 
    end as is_previous_month
    ,case when a.year_month between  date_part('year',(current_date)) - 1 || lpad(date_part('month',(current_date)) + 1, 2, '0')
                            and date_part('year',(current_date)) || lpad(date_part('month',(current_date)), 2,'0') 
                then 'y' else 'n' 
    end as is_in_rolling_12_months
    ,case when a.year = date_part('year',(current_date)) and date_part('month',(a.date)) = date_part('month',(current_date)) - 2 
            then 'y' else 'n' 
    end as is_2_months_back
    ,case when a.year = date_part('year',(current_date)) - 2 then 'y' else 'n' end  as is_2_years_back
    ,case when a.year = date_part('year',(current_date)) - 3 then 'y' else 'n' end as is_3_years_back
    ,case when a.year = date_part('year',(current_date)) - 4 then 'y' else 'n' end as is_4_years_back
    ,case when a.year = date_part('year',(current_date)) and a.week_in_year = date_part('week',(current_date)) 
            then 'y' else 'n' 
    end as is_current_week
    ,case when a.year = date_part('year',(current_date))
                and a.week_in_year = date_part('week',(current_date))
                and a.day_of_week between 1 and date_part('dow',(current_date))
            then 'y' else 'n' 
    end as is_current_wtd
    ,case when a.date = current_date - 1 then 'y' else 'n' end as is_current_day
    ,case when a.date between add_months(current_date,-12)+1 and current_date - 1 
            then 'y' else 'n' 
    end as is_in_rolling_12_months_dates
    ,a.year - date_part ('year', (current_date)) relative_year
    ,cast(months_between(last_day(a.date), last_day(current_date)) as int)  relative_month
    ,a.date - current_date	as relative_day
from date_dim a
)
select *
from final