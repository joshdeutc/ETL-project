import os
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import time

DISTANCE_CACHE_FILE = "distances_cache.csv"
COORDINATES_CACHE_FILE = "coordinates_cache.csv"

def calculate_distance(departure_city, destination_city, geolocator, max_retries=20):

    # Geocode with retry
    def geocode_with_retry(query):
        for _ in range(max_retries):
            try:
                return geolocator.geocode(query)
            except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError):
                print("error numéro :", _, "avec la query", query)
                time.sleep(0.5)
            except Exception:
                break
        return None

    # Load caches
    if os.path.exists(DISTANCE_CACHE_FILE):
        distance_cache_df = pd.read_csv(DISTANCE_CACHE_FILE)
    else:
        distance_cache_df = pd.DataFrame(columns=["departure", "destination", "distance_km"])
    
    if os.path.exists(COORDINATES_CACHE_FILE):
        coordinates_cache_df = pd.read_csv(COORDINATES_CACHE_FILE)
    else:
        coordinates_cache_df = pd.DataFrame(columns=["city", "latitude", "longitude"])

    # Check if distance already cached
    cached_row = distance_cache_df[
        ((distance_cache_df["departure"] == departure_city) & (distance_cache_df["destination"] == destination_city)) |
        ((distance_cache_df["departure"] == destination_city) & (distance_cache_df["destination"] == departure_city))
    ]
    if not cached_row.empty:
        return float(cached_row.iloc[0]["distance_km"])

    # Get or fetch coordinates for departure
    cached_departure_city = coordinates_cache_df[coordinates_cache_df["city"] == departure_city]
    if not cached_departure_city.empty:
        departure_coords = (
            float(cached_departure_city.iloc[0]["latitude"]),
            float(cached_departure_city.iloc[0]["longitude"])
        )
    else:
        departure_location = geocode_with_retry(departure_city)
        if departure_location is None:
            return None
        departure_coords = (departure_location.latitude, departure_location.longitude)
        new_row = pd.DataFrame([{
            "city": departure_city,
            "latitude": departure_coords[0],
            "longitude": departure_coords[1]
        }])
        coordinates_cache_df = pd.concat([coordinates_cache_df, new_row], ignore_index=True)
        coordinates_cache_df.to_csv(COORDINATES_CACHE_FILE, index=False)

    # Get or fetch coordinates for destination
    cached_destination_city = coordinates_cache_df[coordinates_cache_df["city"] == destination_city]
    if not cached_destination_city.empty:
        destination_coords = (
            float(cached_destination_city.iloc[0]["latitude"]),
            float(cached_destination_city.iloc[0]["longitude"])
        )
    else:
        destination_location = geocode_with_retry(destination_city)
        if destination_location is None:
            return None
        destination_coords = (destination_location.latitude, destination_location.longitude)
        new_row = pd.DataFrame([{
            "city": destination_city,
            "latitude": destination_coords[0],
            "longitude": destination_coords[1]
        }])
        coordinates_cache_df = pd.concat([coordinates_cache_df, new_row], ignore_index=True)
        coordinates_cache_df.to_csv(COORDINATES_CACHE_FILE, index=False)

    # Calculate and save distance
    distance_km = geodesic(departure_coords, destination_coords).km
    new_row = pd.DataFrame([{
        "departure": departure_city,
        "destination": destination_city,
        "distance_km": distance_km
    }])
    distance_cache_df = pd.concat([distance_cache_df, new_row], ignore_index=True)
    distance_cache_df.to_csv(DISTANCE_CACHE_FILE, index=False)

    return distance_km
