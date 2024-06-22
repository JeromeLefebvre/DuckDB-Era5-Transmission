--- Using st_contains for this data is not good
create or replace table square as (select ST_Envelope_Agg(tile) from minimalTiles);

--- Using st_contains for this data is not good
create table squareWeather as (from 'Era5Parquet/era5_australia_2000.parquet' where st_contains((from square), st_point(longitude, latitude)));

--- Using st_contains for this data is not good
copy (
from squareWeather where st_contains((select st_collect(list(tile)) from minimalTiles), st_point(longitude, latitude))
) to 'squareWeather_2000.parquet';

--- Using st_contains for this data is not good
from (select st_point(longitude, latitude) as geom, * from 'Era5Parquet/era5_australia_2000.parquet' where st_contains((from square), st_point(longitude, latitude))) where st_contains((select st_collect(list(tile)) from minimalTiles), geom);

--- Using st_contains for this data is not good
select st_point(longitude, latitude) as geom, * from 'Era5Parquet/era5_australia_2000.parquet' where st_contains((select st_collect(list(tile)) from minimalTiles), st_point(longitude, latitude));

-- Tuples help
from 'Era5Parquet/era5_australia_20*.parquet' where (longitude, latitude) in (select (lon, lat) from minimalTiles)