/*
Verify the size of the data set
ls -hl Era5
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
    * exclude(t2m)
order by t2m desc
limit 2;

-- Let's not re-read the file each time, but create a table in memory
create table era5 as from 'Era5/era5_australia_2000.parquet';

-- Big speed increase after loading everything in memory
from era5
select
    round(t2m - 273.15,2) as "Temperature",
    * exclude(t2m)
order by t2m desc
limit 2;

-- With Statement to simplify SQL by refactoring subqueries
with hotdays as (
    from era5
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

load spatial;

.mode line
from st_read('Transmission_Overhead_Powerlines_WP_032_WA_GDA2020_Public_Secure_Shapefile/Transmission_Overhead_Powerlines_WP_032.shp') limit 1;

.quit
