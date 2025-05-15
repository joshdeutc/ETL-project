import os
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import time

CACHE_FILE = "distances_cache.csv"

def calculate_distance(departure_city, destination_city, geolocator, max_retries=20):

    # Load cache if exists
    if os.path.exists(CACHE_FILE):
        cache_df = pd.read_csv(CACHE_FILE)
    else:
        cache_df = pd.DataFrame(columns=["departure", "destination", "distance_km"])

    # Check if the distance is already in the cache
    cached_row = cache_df[
        ((cache_df["departure"] == departure_city) & (cache_df["destination"] == destination_city)) |
        ((cache_df["departure"] == destination_city) & (cache_df["destination"] == departure_city))
    ]

    if not cached_row.empty:
        return float(cached_row.iloc[0]["distance_km"])

    # Geocode with retry
    def geocode_with_retry(query):
        for _ in range(max_retries):
            try:
                return geolocator.geocode(query)
            except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError):
                print("error numero :", _, "avec la query", query)
                time.sleep(0.5)
            except Exception:
                break
        return None

    try:
        departure_location = geocode_with_retry(departure_city)
        destination_location = geocode_with_retry(destination_city)

        if departure_location and destination_location:
            departure_coords = (departure_location.latitude, departure_location.longitude)
            destination_coords = (destination_location.latitude, destination_location.longitude)
            distance_km = geodesic(departure_coords, destination_coords).km

            # Save to cache
            new_row = pd.DataFrame([{
                "departure": departure_city,
                "destination": destination_city,
                "distance_km": distance_km
            }])
            cache_df = pd.concat([cache_df, new_row], ignore_index=True)
            cache_df.to_csv(CACHE_FILE, index=False)

            return distance_km
        return None
    except Exception:
        return None

