from distance import *
from etl_warehouse import *
list_with_no_avion = ["Compiègne"]
from pyspark.sql import SparkSession
# Initialiser Spark
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Europe/Paris")

def get_closest_city_with_avion(ville_destination):
    closest_city_with_avion = {"Compiègne": "Paris"}
    return closest_city_with_avion.get(ville_destination, ville_destination)  # default fallback

def avion_GES(ville_depart, ville_destination):
  distance = calculate_distance(ville_depart, ville_destination)
  if distance == None:
    print("Distance non trouvée", ville_depart, ville_destination)
    return 0
  else:
    distance += 95
  if distance < 1000:
    facteur_emissions = 0.2586
  elif distance < 3500:
    facteur_emissions = 0.1875
  else:
    facteur_emissions = 0.152
  GES = distance * facteur_emissions
  return GES

def taxi_GES(ville_depart, ville_destination):
  distance = calculate_distance(ville_depart, ville_destination)
  facteur_emissions = 0.2156 #motorisation moyenne
  GES = distance * 1,2 * (1 + 1 / 1) * facteur_emissions
  return GES

def commun_GES(ville_depart, ville_destination):
  """distance = calculate_distance(ville_depart, ville_destination)
  list_agglo_sup_250 = ["Paris", "NewYork"]
  list_agglo_inf_100 = ["Berlin", "London", "LosAngeles", "Shanghai"]
  list_agglo_100_250 = ["Paris", "NewYork"]
  if ville_depart in list_agglo_sup_250:
    facteur_emissions = 0.129
  elif ville_depart in list_agglo_inf_100:
    facteur_emissions = 0.146
  else:
    facteur_emissions = 0.137"""
  distance = calculate_distance(ville_depart, ville_destination)
  facteur_emissions = 0.129 #agglomeration supérieur à 250k habitants
  GES = distance * 1,5 * facteur_emissions
  return GES

def train_GES(ville_depart, ville_destination):
  distance = calculate_distance(ville_depart, ville_destination)
  if distance :  
    if distance < 200:
        facteur_emissions = 0.018
    else:
        facteur_emissions = 0.0033
    GES = distance * 1,2 * facteur_emissions
    return GES

def mission_bilan_carbone(df_mission):
    df_mission = spark.createDataFrame(df_mission)
    rows_with_ges = []
    for row in df_mission.collect():
        transport = row["TRANSPORT"]
        ville_depart = row["VILLE_DEPART"]
        ville_destination = row["VILLE_DESTINATION"]
        GES = None
        if transport == "Avion":
            if ville_destination in list_with_no_avion:
                ville_avion = get_closest_city_with_avion(ville_destination)
                GES = train_GES(ville_destination, ville_avion)
                GES =  2 * avion_GES(ville_depart, ville_avion)
            else:
              GES = avion_GES(ville_depart, ville_destination)

        elif transport == "Taxi":
          GES = taxi_GES(ville_depart, ville_destination)

        elif transport == "Transports en commun":
          GES = commun_GES(ville_depart, ville_destination)

        elif transport == "Train":
          GES = train_GES(ville_depart, ville_destination)

        # Ajouter la ligne si GES a pu être calculé
        if GES is not None:
          GES = GES * 0.001
          rows_with_ges.append(row + (GES,))

    # Créer un nouveau DataFrame avec la colonne GES
    if rows_with_ges:
        new_df = spark.createDataFrame(rows_with_ges, df_mission.columns + ["GES"])
        return new_df
    else:
        return None