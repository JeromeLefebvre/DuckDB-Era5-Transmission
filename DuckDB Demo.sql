/*
ls -hl Era5

duckdb
*/

-- Take a look at the era5 files
from 'Era5/era5_australia_2000.parquet';

-- That took a few seconds, limit to only reading the a few lines
from 'Era5/era5_australia_2000.parquet' limit 10;

-- How to look at the file as a table
describe from 'Era5/era5_australia_2000.parquet';

-- Let's take a look at the hotest hour in WA
from 'Era5/era5_australia_2000.parquet' order by t2m desc limit 2;

-- Some simple math
from 'Era5/era5_australia_2000.parquet'
select
    round(t2m - 273.15,1) as "Temperature",
    ---Date'2024-06-30 12:00:00' AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' as local_time;
    * exclude(t2m)
order by t2m desc
limit 2;

-- Let's not re-read the file each time, but create a table in memory
create table weather as from 'Era5/era5_australia_2000.parquet'; 

-- Big speed increase after loading everything in memory
from weather
select
    round(t2m - 273.15,2) as "Temperature",
    * exclude(t2m)
order by t2m desc
limit 2;

-- With Statement to simplify SQL by refactoring subqueries
with hotdays as (
    from weather
    select
        round(t2m - 273.15,2) as "Temperature",
        * exclude(t2m)
    order by t2m desc
    limit 5
)
from hotdays
select
    Temperature,
    concat('https://www.google.com/maps/@', latitude, ',', longitude, ',8000m/data=!3m1!1e3!5m1!1e4?entry=ttu') as map,
    time at time zone 'UTC' at time zone 'Australia/Perth' as "local time",;




.quit

