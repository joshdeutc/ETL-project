from pyspark.sql import functions as F
import pyspark.pandas as ps
from etl_warehouse import *
from pyspark.sql import SparkSession
# Initialiser Spark
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Europe/Paris")

from pyspark.sql.functions import avg
psdf_informatique = ps.read_csv('BDD_BGES\BDD_BGES\materiel_informatique_impact.csv', sep=',', encoding='utf-8')
sdf_informatique = psdf_informatique.to_spark()
moyenne_ges = sdf_informatique.selectExpr("avg(Impact)").first()[0]
# Fonction pour récupérer le GES à partir du modèle et type de matériel
def materiel_informatique_GES(type_mat, modele_mat):
    if modele_mat != "modèle par défaut" and modele_mat != " ":
        # Filtrer sur le modèle spécifique
        first_row = sdf_informatique.filter(F.col("Modèle") == modele_mat).select(F.col("Impact")).first()
        if first_row is not None:  # Si une correspondance est trouvée
            return first_row[0]
    else:
        # Filtrer sur le type de matériel et le modèle par défaut
        first_row = sdf_informatique.filter(F.col("Type") == type_mat).filter(F.col("Modèle") == "modèle par défaut").select(F.col("Impact")).first()
        if first_row is not None:  # Si une correspondance est trouvée
            return first_row[0]
        else:
            return

    return None  # Aucun résultat trouvé

# Fonction pour ajouter le GES au DataFrame final
def tranform_with_GES(df_materiel):
    df_materiel = spark.createDataFrame(df_materiel)
    # Créer une liste pour les nouvelles lignes (avec ajout du GES)
    rows_with_ges = []

    # Parcourir toutes les lignes de final_sdf
    for row in df_materiel.collect():
        ges_value = materiel_informatique_GES(row["TYPE"], row["MODELE"])

        if ges_value is not None:
            ges_value = ges_value * 0.001
            # Ajouter la valeur GES à la ligne
            rows_with_ges.append(row + (ges_value,))

    # Créer un nouveau DataFrame avec la colonne GES ajoutée
    if rows_with_ges:
        new_df = spark.createDataFrame(rows_with_ges, df_materiel.columns + ["GES"])
        return new_df
    else:
        return df_materiel  # Si aucune ligne avec GES, retourner le DataFrame original