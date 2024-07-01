import duckdb
from duckdb.typing import *

from math import sqrt

def line_rating(ambient_temp, wind_speed, angle_of_attack, solar_irradiation, conductor_temp):
    # raise NotImplementedError("Yes yes I know")
    return 0

db = duckdb.connect('transmissions.db')
db.load_extension('spatial')

db.sql('from weather limit 10').to_df()

#db.remove_function("line_rating")
db.create_function("line_rating", line_rating, [DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE], DOUBLE)

db.sql('select "line name", segment, line_rating(temperature, "wind speed", "line Of Attack", "solar irradiance", 75) as "line rating" from linesWithWeather limit 1')
