{{ config(materialized='view',schema='info_core') }}

with cte_my_date as 
(
	select dateadd(day, seq4(), '2000-01-01') as my_date
	from table(generator(rowcount=>10000)) -- number of days after reference date in previous line
)
select my_date 
	,year(my_date) as year
    ,month(my_date) as month
	,monthname(my_date) as monthname
    ,day(my_date)   as day
	,dayofweek(my_date) as dayofweek
    ,weekofyear(my_date) as weekofyear
    ,dayofyear(my_date)  as dayofyear
from cte_my_date

