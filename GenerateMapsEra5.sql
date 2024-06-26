load spatial;

-- Returns the direction of a line using the start and end point in degrees
create macro directionOfLine(line) AS 
(degrees(atan2(ST_Y(st_EndPoint(line)) - ST_Y(st_StartPoint(line)), ST_X(st_EndPoint(line)) - ST_X(st_StartPoint(line)))) + 360) % 360;

-- Query #1 Take all transmissions lines and split them into individual components
create or replace view lines as
with allTransmissions as
(
       -- Turn everything into linestrings
       from st_read('Transmission_Overhead_Powerlines_WP_032_WA_GDA2020_Public_Secure_Shapefile/Transmission_Overhead_Powerlines_WP_032.shp')
       select
              case when ST_GeometryType(geom) = 'MULTILINESTRING' then unnest(ST_DUMP(geom)).geom
                     else geom end as geom,
              * exclude (geom)
)
from allTransmissions, range(1,800)
select
       -- The geometry are the individual line segments
       st_makeline(st_pointN(geom, range::int), st_pointN(geom, range::int + 1)) as segment,
       -- Features
       directionOfLine(segment) as "direction",
       line_name as "line name",
       range as "index",
       kv "capacity", 
       -- Foreign key to the parquet file
       round(st_x(ST_StartPoint(segment))*4)/4 as "weather longitude",
       round(st_y(ST_StartPoint(segment))*4)/4 as "weather latitude",       
where segment is not null;


-- Sanity check
select concat('https://www.google.com/maps/@', st_y(st_StartPoint(segment)), ',', st_x(st_StartPoint(segment)), ',1004m/data=!3m1!1e3!5m1!1e4?entry=ttu') from lines where "line name" = 'MOR-TS 81' and index=99;

-- The weather
describe from 'Era5Parquet/era5_australia_200*.parquet';

-- Query #2 weather KPi
create or replace view weather as
from 'Era5Parquet/era5_australia_200*.parquet'
select date_part('year', time) as year,
       date_part('Month', time) as month,
       date_part('hour', time)::int as h,
       latitude,
       longitude,
       avg(t2m) - 273.15 as "avg temperature", -- switch from Ke
       avg(u10) as "avg u10",
       avg(v10) as "avg v10", 
       avg(ssr) as "avg solar irradiance",
       (180 + 180/pi()*atan2("avg u10", "avg v10"))%360 as "avg wind direction",
       SQRT(POWER("avg u10", 2) + POWER("avg v10", 2)) as "avg wind speed",
       avg(100 * (exp((17.27 * d2m) / (237.3 + d2m)) / exp((17.27 * t2m) / (237.3 + t2m)))) as "avg humidity"
group by all;

-- Query #3 power
create or replace view linesWithWeather as
select round(least(abs(("avg wind direction" % 180) - ("avg wind direction" % 180)), 180 - abs(("avg wind direction" % 180) - ("avg wind direction" % 180))), 1) AS lineOfAttack,
       st_makeline("Segment Start","Segment End") as segment,
       * exclude("avg wind direction", "segment start", "segment end", "direction", "avg u10", "avg v10", latitude, longitude, "weather longitude", "weather latitude")
from lines, weather
where weather.latitude = lines."weather latitude" and weather.longitude = lines."weather longitude";

describe linesWithWeather;

-- one day
copy (
       select ST_AsWKB()
) to 'Maps/linesWithWeather.parquet';

from lines, from 'Era5Parquet/era5_australia_20*.parquet'


where longitude, latitude)

-- add weather to mininimal tiles
create view coverWithWeather as
from minimalTiles, (from 'Era5Parquet/era5_australia_200*.parquet') as p where (minimalTiles.lon, minimalTiles.lat) = (p.longitude, p.latitude);

-- 
DESCRIBE coverWithWeather;

-- Take vertical averages
-- TODO:Verify if this is all mathematically correct, in particular avg wind direction
create or replace view coverWithWeatherKPI as 
select date_part('year', time) as year,
       date_part('Month', time) as month,
       date_part('hour', time)::int as h,
       lat,
       lon,
       tile,
       avg(t2m) - 273.15 as "avg temperature", -- switch from Ke
       avg(u10) as "avg u10",
       avg(v10) as "avg v10", 
       (180 + 180/pi()*atan2("avg u10", "avg v10"))%360 as "avg wind direction",
       SQRT(POWER("avg u10", 2) + POWER("avg v10", 2)) as "avg wind speed",
       avg(100 * (exp((17.27 * d2m) / (237.3 + d2m)) / exp((17.27 * t2m) / (237.3 + t2m)))) as "avg humidity",
       avg(ssr) as "avg solar irradiance"
from coverWithWeather group by all order by all;

create or replace view linesWithWeather as
select round(least(abs(("avg wind direction" % 180) - (dir % 180)), 180 - abs(("avg wind direction" % 180) - (dir % 180))), 1) AS lineOfAttack, st_makeline(a,b), * exclude(a,b, dir, "avg wind direction", tile, lat, lon)
from coverWithWeatherKPI, lines where st_contains(coverWithWeatherKPI.tile, a);


DESCRIBE linesWithWeather;
.mode csv
copy(
select date'2000-01-01 00:00:00' + interval (h) hours as time, * from linesWithWeather where month=1 and year=2000 and line_name = 'CTB-ENB 81' and idx = 10 order by time)
to 'bug.csv';

copy (select date'2000-01-01 00:00:00' + interval (h) hours as time, * from linesWithWeather where month=1 and line_name = 'CTB-ENB 81' and year=2000) to 'Maps/linesWithWeatherCTB1.gdb' with (format GDAL, driver 'OpenFileGDB', SRS 'EPSG:4326', GEOMETRY_TYPE 'linestring');

copy weatherMap to 'Maps/BOM_Weather_Station_Details.gdb' with (format GDAL, Driver 'OpenFileGDB', SRS 'EPSG:4326', GEOMETRY_TYPE 'Point');

select interval (range) hours from range(1,10);

------------------------------------------------------------------------------------
-- visualize
copy weatherForCell to 'maps/6. Weather for minimal cover.gdb' with (format gdal, driver 'g');


-- Add the weather to the line base on where the start point is
CREATE or replace view linesWithWeather AS
SELECT  line_name
       ,kv
       ,idx
       ,st_makeline(a,b)                   AS lineSeg
       --,round(case when abs((winddir % 180) - (dir % 180)) < 90 then abs((winddir % 180) - (dir % 180))
       --else 180 - abs((winddir % 180) - (dir % 180)) end,1) as 
       ,round(least(abs((winddir % 180) - (dir % 180)), 180 - abs((winddir % 180) - (dir % 180))), 1) AS lineOfAttack

       ,windSpeed
       ,temp
       ,humidity
       ,DN
FROM lineDirection, weatherForCell
WHERE st_contains(cell, a);



-- Todo: use a time range for the weather and apply a circular mean
https://en.wikipedia.org/wiki/Circular_mean

-- Todo: use some UDFs in python to actually compute this impact
https://www.electrical4u.com/sag-in-overhead-conductor/
https://github.com/tommz9/pylinerating


-- visualize wind directions
create or replace view windArrows as 
SELECT  st_centroid(cell)                                                                                    AS a
       ,st_x(st_centroid(cell))                                                                              AS xc
       ,st_y(st_centroid(cell))                                                                              AS yc
       ,st_x(st_centroid(cell)) + 0.2                                                                        AS x0
       ,st_y(st_centroid(cell))                                                                              AS y0
       ,radians(winddir)                                                                                     AS theta
       ,st_point( (x0-xc)*cos(theta) - (y0-yc)*sin(theta) + xc,(x0-xc)*sin(theta) + (y0-yc)*cos(theta) + yc) AS b
       ,st_makeline(a,b)                                                                                     AS arrow
       ,degreesVect(a,b)
       ,winddir
FROM weatherForCell;

copy (
select windDir, arrow from windArrows
) to 'Maps/5. Wind direction.geojson' with (format gdal, driver 'geoJSON');