from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import time

def calculate_distance(departure_city, destination_city, max_retries=3):
    """
    Calculates the distance between the departure and destination cities.

    Args:
        departure_city (str): Name of the departure city.
        destination_city (str): Name of the destination city.
        max_retries (int): Maximum number of retry attempts for geocoding.

    Returns:
        float: Distance in kilometers, or None if unable to calculate.
    """
    def geocode_with_retry(query):
        for attempt in range(max_retries):
            try:
                return geolocator.geocode(query)
            except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as e:
                print(f"Geocoder error (attempt {attempt + 1}) for '{query}': {e}")
                time.sleep(2)
            except Exception as e_gen:
                print(f"Unexpected geocoder error for '{query}': {e_gen}")
                break
        return None

    try:
        geolocator = Nominatim(user_agent="mission_distance_calculator")

        departure_location = geocode_with_retry(departure_city)
        time.sleep(1)
        destination_location = geocode_with_retry(destination_city)

        if departure_location and destination_location:
            departure_coords = (departure_location.latitude, departure_location.longitude)
            destination_coords = (destination_location.latitude, destination_location.longitude)
            return geodesic(departure_coords, destination_coords).km
        else:
            return None
    except Exception as e:
        print(f"Error calculating distance: {e}")
        return None