from distance import *
from etl_warehouse import *

list_with_no_avion = ["Compiègne"]
geolocator = Nominatim(user_agent="mission_distance_calculator")

def get_closest_city_with_avion(ville_destination):
    closest_city_with_avion = {"Compiègne": "Paris"}
    return closest_city_with_avion.get(ville_destination, ville_destination)

def avion_GES(ville_depart, ville_destination):
    distance = calculate_distance(ville_depart, ville_destination, geolocator)
    if distance is None:
        return 0
    distance += 95
    if distance < 1000:
        facteur_emissions = 0.2586
    elif distance < 3500:
        facteur_emissions = 0.1875
    else:
        facteur_emissions = 0.152
    return distance * facteur_emissions

def taxi_GES(ville_depart, ville_destination):
    distance = calculate_distance(ville_depart, ville_destination, geolocator)
    facteur_emissions = 0.2156
    return distance * 1.2 * 2 * facteur_emissions

def commun_GES(ville_depart, ville_destination):
    distance = calculate_distance(ville_depart, ville_destination, geolocator)
    facteur_emissions = 0.129
    return distance * 1.5 * facteur_emissions

def train_GES(ville_depart, ville_destination):
    distance = calculate_distance(ville_depart, ville_destination, geolocator)
    if distance:
        if distance < 200:
            facteur_emissions = 0.018
        else:
            facteur_emissions = 0.0033
        return distance * 1.2 * facteur_emissions

def mission_bilan_carbone(df_mission):
    import pandas as pd
    rows_with_ges = []
    for _, row in df_mission.iterrows():
        print("premiere mission")
        transport = row["TRANSPORT"]
        ville_depart = row["VILLE_DEPART"]
        ville_destination = row["VILLE_DESTINATION"]
        GES = None
        if transport == "Avion":
            if ville_destination in list_with_no_avion:
                ville_avion = get_closest_city_with_avion(ville_destination)
                GES = train_GES(ville_destination, ville_avion)
                GES = 2 * avion_GES(ville_depart, ville_avion)
            else:
                GES = avion_GES(ville_depart, ville_destination)
        elif transport == "Taxi":
            GES = taxi_GES(ville_depart, ville_destination)
        elif transport == "Transports en commun":
            GES = commun_GES(ville_depart, ville_destination)
        elif transport == "Train":
            GES = train_GES(ville_depart, ville_destination)
        if GES is not None:
            GES *= 0.001
            rows_with_ges.append({**row.to_dict(), "GES": GES})
    if rows_with_ges:
        return pd.DataFrame(rows_with_ges)
    else:
        return df_mission
