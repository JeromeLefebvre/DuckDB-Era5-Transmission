-- Look at what is inside td
from td;

-- Query #2 Create a set of tiles that cover the grid
-- This is a Voronoi map on the weather data
create view tiles as
with allTiles as
(
from range(-36*4, -13*4) AS latRange, range(110*4, 130*4) AS lonRange
select latRange.range/4 as lat, 
       lonRange.range/4 as lon,
       st_ConvexHull(st_collect([
              st_point(lon-0.125,lat-0.125),
              st_point(lon-0.125,lat+0.125),
              st_point(lon+0.125,lat-0.125),
              st_point(lon+0.125,lat+0.125),
              st_point(lon-0.125,lat-0.125),
              ])) as tile
)
from allTiles,td
select distinct 
       tile,
       allTiles.lat as lat,
       allTiles.lon as lon
where ST_Intersects(allTiles.tile, td.geom);


-- Visualize 
copy tiles
to 'maps/1. All tiles.geojson' with (format gdal, driver 'geojson');
