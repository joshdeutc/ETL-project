from distance import *
from etl_warehouse import *

list_with_no_avion = ["Compiègne"]
geolocator = Nominatim(user_agent="mission_distance_calculator")

def get_closest_city_with_avion(ville_destination):
    closest_city_with_avion = {"Compiègne": "Paris"}
    return closest_city_with_avion.get(ville_destination, ville_destination)

def avion_GES(ville_depart, ville_destination, aller_retour):
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
    GES = distance * facteur_emissions
    if aller_retour == "oui":
        GES = GES * 2

    return GES

def taxi_GES(ville_depart, ville_destination, aller_retour):
    if ville_depart == ville_destination:
        distance = 5
    else:
        distance = calculate_distance(ville_depart, ville_destination, geolocator)
    facteur_emissions = 0.2156
    GES = distance * 1.2 * 2 * facteur_emissions
    if aller_retour == "oui":
        GES = GES * 2
    
    return GES

def commun_GES(ville_depart, ville_destination, aller_retour):
    if ville_depart == ville_destination:
        distance = 5
    else:
        distance = calculate_distance(ville_depart, ville_destination, geolocator)
    #facteur d'emission = moyenne entre Tramway > 250 000 habitants + rer + Bus > 250 000 habitants
    facteur_emissions = 0.047
    #coeff_multiplicateur = moyenne entre 1,2 pour le RER, 1,7 pour le métro et 1,5 pour le bus
    coeff_multiplicateur = (1.2 + 1.7 + 1.5) / 3
    GES = distance * coeff_multiplicateur * facteur_emissions
    if aller_retour == "oui":
        GES = GES * 2

    return GES

def train_GES(ville_depart, ville_destination, aller_retour):
    if ville_depart == ville_destination:
        distance = 5
    else :
        distance = calculate_distance(ville_depart, ville_destination, geolocator)
    if distance < 200:
        facteur_emissions = 0.018
    else:
        facteur_emissions = 0.0033
    GES = distance * 1.2 * facteur_emissions
    if aller_retour == "oui":
        GES = GES * 2

    return GES

def mission_bilan_carbone(df_mission):
    import pandas as pd
    rows_with_ges = []
    for _, row in df_mission.iterrows():
        print("mission numero :", _, "faite")
        transport = row["TRANSPORT"]
        ville_depart = row["VILLE_DEPART"]
        ville_destination = row["VILLE_DESTINATION"]
        aller_retour = row["ALLER_RETOUR"]
        GES = None
        if transport == "Avion":
            if ville_destination in list_with_no_avion:
                ville_avion = get_closest_city_with_avion(ville_destination)
                GES = train_GES(ville_destination, ville_avion, aller_retour)
                GES = avion_GES(ville_depart, ville_avion, aller_retour)
            else:
                GES = avion_GES(ville_depart, ville_destination, aller_retour)
        elif transport == "Taxi":
            GES = taxi_GES(ville_depart, ville_destination, aller_retour)
        elif transport == "Transports en commun":
            GES = commun_GES(ville_depart, ville_destination, aller_retour)
        elif transport == "Train":
            GES = train_GES(ville_depart, ville_destination, aller_retour)
        if GES is not None:
            GES *= 0.001
            rows_with_ges.append({**row.to_dict(), "GES": GES})
        else:
            print("erreur pour row = ", row)
    if rows_with_ges:
        return pd.DataFrame(rows_with_ges)
    else:
        return df_mission
