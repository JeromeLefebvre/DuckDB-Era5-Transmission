import duckdb
import numpy as np
from duckdb.typing import *

def line_rating(ambient_temp, wind_speed, angle_of_attack, solar_irradiation, conductor_temp):
    """
    Calculate the line rating of a power line using the IEEE 738-2006 standard.
    
    Parameters:
    - ambient_temp: Ambient temperature in Celsius.
    - wind_speed: Wind speed in m/s.
    - angle_of_attack: Angle of attack of the wind in degrees.
    - solar_irradiation: Solar irradiation in W/m^2.
    - conductor_temp: Conductor temperature in Celsius.
    - conductor_details: Dictionary containing conductor details such as diameter, emissivity, absorptivity, resistance at 20°C, and maximum temperature.
    
    Returns:
    - line_rating: Line rating in A (Amperes).
    """
    # Constants
    epsilon = 0.8  # Emissivity (typical value, replace with actual if available)
    alpha = 0.5  # Absorptivity (typical value, replace with actual if available)
    sigma = 5.67e-8  # Stefan-Boltzmann constant (W/m^2K^4)
    # Example usage
    conductor_details = {
        'diameter': 0.01,  # in meters
        'resistance': 0.0002,  # in ohms per meter at 20°C
        'emissivity': 0.8,
        'absorptivity': 0.5,
        'max_temperature': 75  # in Celsius
    }
    # Extract conductor details
    diameter = conductor_details['diameter']
    resistance = conductor_details['resistance']
    # Convert temperatures to Kelvin
    T_a = ambient_temp + 273.15
    T_c = conductor_temp + 273.15
    # Convert angle of attack to radians
    angle_rad = np.deg2rad(angle_of_attack)
    # Convective heat loss
    convective_heat_loss = 0.0205 * wind_speed**0.6 * (conductor_temp - ambient_temp)**1.25
    # Radiative heat loss
    radiative_heat_loss = epsilon * sigma * (T_c**4 - T_a**4)
    # Solar heat gain
    solar_heat_gain = alpha * solar_irradiation
    # Total heat gain
    total_heat_gain = solar_heat_gain - convective_heat_loss - radiative_heat_loss
    # Current calculation
    line_rating = np.sqrt(abs(total_heat_gain / resistance))
    return line_rating



db = duckdb.connect()
db.load_extension('spatial')

db.sql("create view TDwithWeather as from st_read('maps/7. final weather map.geojson')")

db.sql("from TDwithWeather limit 1")

#db.remove_function("line_rating")
db.create_function("line_rating", line_rating, [DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE], DOUBLE)

# def line_rating(ambient_temp, wind_speed, angle_of_attack, solar_irradiation, conductor_temp):
db.sql("create view TDwithLineRating as (select line_name, geom, line_rating(temp, windSpeed, lineOfAttack, DN, 75) as lineRating from TDwithWeather)")

db.sql("copy TDwithLineRating to 'Maps/8. TD with line rating.geojson' with (format GDAL, Driver 'Geojson')")