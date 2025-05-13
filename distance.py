from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import time

def calculate_distance(departure_city, destination_city):
    """
    Calculates the distance between the departure and destination cities of a mission.

    Args:
        mission (dict): A dictionary representing a mission with "VILLE_DEPART" and "VILLE_DESTINATION" keys.

    Returns:
        float: The distance between the cities in kilometers, or None if the cities cannot be geocoded.
    """
    try:
        geolocator = Nominatim(user_agent="mission_distance_calculator")

        departure_location = geolocator.geocode(departure_city)
        time.sleep(1)  # To avoid hitting the API too quickly
        destination_location = geolocator.geocode(destination_city)

        if departure_location and destination_location:
            departure_coordinates = (departure_location.latitude, departure_location.longitude)
            destination_coordinates = (destination_location.latitude, destination_location.longitude)
            distance = geodesic(departure_coordinates, destination_coordinates).km
            return distance
        else:
            return None

    except Exception as e:
        print(f"Error calculating distance: {e}")
        return None