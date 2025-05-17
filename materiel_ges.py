from pyspark.sql import functions as F
import pandas as pd
from etl_warehouse import *

pdf_informatique = pd.read_csv('BDD_BGES/BDD_BGES/materiel_informatique_impact.csv', sep=',', encoding='utf-8')
moyenne_ges = pdf_informatique["Impact"].mean()

def materiel_informatique_GES(type_mat, modele_mat):
    if modele_mat != "modèle par défaut" and modele_mat.strip() != "":
        match = pdf_informatique[pdf_informatique["Modèle"] == modele_mat]
        if not match.empty:
            return match.iloc[0]["Impact"]
    else:
        match = pdf_informatique[
            (pdf_informatique["Type"] == type_mat) &
            (pdf_informatique["Modèle"] == "modèle par défaut")
        ]
        if not match.empty:
            return match.iloc[0]["Impact"]
        else:
            return moyenne_ges
    return None

def tranform_with_GES(df_materiel):
    rows_with_ges = []
    for _, row in df_materiel.iterrows():
        ges_value = materiel_informatique_GES(row["TYPE"], row["MODELE"])
        if ges_value is not None:
            ges_value *= 0.001
            row_dict = row.to_dict()
            row_dict["GES"] = ges_value
            rows_with_ges.append(row_dict)
    if rows_with_ges:
        return pd.DataFrame(rows_with_ges)
    else:
        return df_materiel
