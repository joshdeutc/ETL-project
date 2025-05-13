import os
import glob
import pandas as pd
import pytz
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Initialiser Spark
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Europe/Paris")

# Configuration des chemins
SOURCE_PATH = "BDD_BGES\\BDD_BGES"
DW_PATH = "DATA_WAREHOUSE"  # Chemin vers le Data Warehouse

# Créer les répertoires DW s'ils n'existent pas
os.makedirs(DW_PATH, exist_ok=True)
os.makedirs(os.path.join(DW_PATH, "FAITS_MISSION"), exist_ok=True)
os.makedirs(os.path.join(DW_PATH, "FAITS_MATERIEL"), exist_ok=True)
os.makedirs(os.path.join(DW_PATH, "DIM_DATE"), exist_ok=True)
os.makedirs(os.path.join(DW_PATH, "DIM_MISSION"), exist_ok=True)
os.makedirs(os.path.join(DW_PATH, "DIM_MATERIEL"), exist_ok=True)

# Mappage des villes vers leurs fuseaux horaires
FUSEAUX_VILLES = {
    "BERLIN": "Europe/Berlin",
    "PARIS": "Europe/Paris",
    "LONDON": "Europe/London",
    "LOSANGELES": "America/Los_Angeles",
    "NEWYORK": "America/New_York",
    "SHANGHAI": "Asia/Shanghai",
}

def table_existe(table_path):
    """Vérifie si une table existe déjà"""
    return os.path.exists(table_path)

def convertir_date_fuseau_paris(date_original, ville_source):
    """Convertit une date du fuseau horaire source au fuseau horaire de Paris"""
    # Obtenir le fuseau horaire source
    tz_source = pytz.timezone(FUSEAUX_VILLES.get(ville_source, "UTC"))
    # Obtenir le fuseau horaire de Paris
    tz_paris = pytz.timezone("Europe/Paris")
    
    # Localiser la date dans son fuseau d'origine puis la convertir en fuseau Paris
    date_localisee = tz_source.localize(pd.to_datetime(date_original), is_dst=None)
    date_paris = date_localisee.astimezone(tz_paris)
    
    return date_paris

def chercher_fichiers_avec_conversion_fuseau(date_str, type_donnees):
    """Recherche les fichiers correspondant à une date en tenant compte des fuseaux horaires"""
    date_paris = pd.to_datetime(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}")
    
    # On va regarder le jour précédent et suivant pour tenir compte des fuseaux horaires
    dates_a_verifier = [
        (date_paris - timedelta(days=1)).strftime("%Y%m%d"),
        date_str,
        (date_paris + timedelta(days=1)).strftime("%Y%m%d")
    ]
    
    fichiers_trouves = []
    
    for date_check in dates_a_verifier:
        for ville in FUSEAUX_VILLES.keys():
            if type_donnees == "MISSION":
                fichier_path = f"{SOURCE_PATH}\\BDD_BGES_{ville}\\BDD_BGES_{ville}_MISSION\\MISSION_{date_check}.txt"
            else:  # MATERIEL_INFORMATIQUE
                fichier_path = f"{SOURCE_PATH}\\BDD_BGES_{ville}\\BDD_BGES_{ville}_INFORMATIQUE\\MATERIEL_INFORMATIQUE_{date_check}.txt"
                
            if os.path.exists(fichier_path):
                fichiers_trouves.append((fichier_path, ville, date_check))
    
    return fichiers_trouves

def lire_fichiers_source(date_str, type_donnees):
    """Lit les fichiers sources en tenant compte des fuseaux horaires"""
    fichiers_trouves = chercher_fichiers_avec_conversion_fuseau(date_str, type_donnees)
    
    if not fichiers_trouves:
        raise FileNotFoundError(f"Aucune donnée trouvée pour {type_donnees} à la date {date_str}")
    
    all_dfs = []
    date_paris_cible = pd.to_datetime(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}")
    
    for fichier_path, ville, date_fichier in fichiers_trouves:
        try:
            df = pd.read_csv(fichier_path, sep=';')
            df['VILLE_SOURCE'] = ville
            df['DATE_FICHIER'] = date_fichier
            
            # Normaliser les dates selon le fuseau horaire
            col_date = 'DATE_MISSION' if type_donnees == 'MISSION' else 'DATE_ACHAT'
            df[col_date] = pd.to_datetime(df[col_date])
            
            # Créer une colonne avec la date convertie au fuseau de Paris
            df['DATE_PARIS'] = df.apply(
                lambda row: convertir_date_fuseau_paris(row[col_date], row['VILLE_SOURCE']), 
                axis=1
            )
            
            # Filtrer pour ne garder que les lignes dont la date Paris correspond à la date cible
            df_filtered = df[df['DATE_PARIS'].dt.date == date_paris_cible.date()]
            
            if not df_filtered.empty:
                all_dfs.append(df_filtered)
                print(f"Fichier chargé et filtré: {fichier_path} - {len(df_filtered)} lignes")
            
        except Exception as e:
            print(f"Erreur lors du chargement {fichier_path}: {e}")
    
    if not all_dfs:
        raise FileNotFoundError(f"Aucune donnée correspondant à la date {date_str} après conversion des fuseaux horaires")
    
    # Combiner tous les dataframes
    return pd.concat(all_dfs, ignore_index=True)

def creer_dimension_date(dates_paris):
    """Crée ou met à jour la table dimension date"""
    dimension_date_path = os.path.join(DW_PATH, "DIM_DATE", "dimension_date.csv")
    
    # Créer une liste de dates uniques
    dates_uniques = pd.DataFrame({'DATE_PARIS': pd.Series(dates_paris).dt.date.unique()})
    dates_uniques = pd.DataFrame(dates_uniques['DATE_PARIS'])
    
    # Enrichir avec des attributs de date
    dates_uniques['KeyDate'] = dates_uniques['DATE_PARIS'].apply(lambda x: x.strftime('%Y%m%d'))
    dates_uniques['ANNEE'] = dates_uniques['DATE_PARIS'].apply(lambda x: x.year)
    dates_uniques['MOIS'] = dates_uniques['DATE_PARIS'].apply(lambda x: x.month)
    dates_uniques['JOUR'] = dates_uniques['DATE_PARIS'].apply(lambda x: x.day)
    dates_uniques['JOUR_SEMAINE'] = dates_uniques['DATE_PARIS'].apply(lambda x: x.weekday())
    dates_uniques['NOM_JOUR'] = dates_uniques['DATE_PARIS'].apply(lambda x: x.strftime('%A'))  # Nom du jour
    dates_uniques['TRIMESTRE'] = dates_uniques['DATE_PARIS'].apply(lambda x: (x.month - 1) // 3 + 1)
    
    # Si la table existe déjà, fusionner avec de nouvelles dates
    if table_existe(dimension_date_path):
        dim_date_existante = pd.read_csv(dimension_date_path)
        dim_date_existante['DATE_PARIS'] = pd.to_datetime(dim_date_existante['DATE_PARIS']).dt.date
        
        # Identifier les nouvelles dates
        dates_existantes = set(dim_date_existante['KeyDate'])
        nouvelles_dates = dates_uniques[~dates_uniques['KeyDate'].isin(dates_existantes)]
        
        # Fusionner si de nouvelles dates existent
        if len(nouvelles_dates) > 0:
            dim_date_maj = pd.concat([dim_date_existante, nouvelles_dates], ignore_index=True)
            dim_date_maj.to_csv(dimension_date_path, index=False)
            print(f"Table dimension DATE mise à jour avec {len(nouvelles_dates)} nouvelles dates")
    else:
        # Créer la table
        dates_uniques.to_csv(dimension_date_path, index=False)
        print(f"Table dimension DATE créée avec {len(dates_uniques)} dates")
    
    return dates_uniques

def creer_dimension_mission(df_missions):
    """Crée ou met à jour la table dimension mission"""
    dimension_mission_path = os.path.join(DW_PATH, "DIM_MISSION", "dimension_mission.csv")
    
    # Colonnes à conserver pour la dimension
    cols_mission = ['ID_MISSION', 'TYPE_MISSION', 'VILLE_DEPART', 'PAYS_DEPART', 
                   'VILLE_DESTINATION', 'PAYS_DESTINATION', 'TRANSPORT', 'ALLER_RETOUR']
    dim_mission_nouvelles = df_missions[cols_mission].drop_duplicates()
    
    # Si la table existe déjà, fusionner avec de nouvelles missions
    if table_existe(dimension_mission_path):
        dim_mission_existante = pd.read_csv(dimension_mission_path)
        
        # Identifier les nouvelles missions
        missions_existantes = set(dim_mission_existante['ID_MISSION'])
        nouvelles_missions = dim_mission_nouvelles[~dim_mission_nouvelles['ID_MISSION'].isin(missions_existantes)]
        
        # Fusionner si de nouvelles missions existent
        if len(nouvelles_missions) > 0:
            dim_mission_maj = pd.concat([dim_mission_existante, nouvelles_missions], ignore_index=True)
            dim_mission_maj.to_csv(dimension_mission_path, index=False)
            print(f"Table dimension MISSION mise à jour avec {len(nouvelles_missions)} nouvelles missions")
    else:
        # Créer la table
        dim_mission_nouvelles.to_csv(dimension_mission_path, index=False)
        print(f"Table dimension MISSION créée avec {len(dim_mission_nouvelles)} missions")
    
    return dim_mission_nouvelles

def creer_dimension_materiel(df_materiel):
    """Crée ou met à jour la table dimension matériel"""
    dimension_materiel_path = os.path.join(DW_PATH, "DIM_MATERIEL", "dimension_materiel.csv")
    
    # Colonnes à conserver pour la dimension
    cols_materiel = ['ID_MATERIELINFO', 'TYPE', 'MODELE']
    dim_materiel_nouvelles = df_materiel[cols_materiel].drop_duplicates()
    
    # Si la table existe déjà, fusionner avec de nouveaux matériels
    if table_existe(dimension_materiel_path):
        dim_materiel_existante = pd.read_csv(dimension_materiel_path)
        
        # Identifier les nouveaux matériels
        materiels_existants = set(dim_materiel_existante['ID_MATERIELINFO'])
        nouveaux_materiels = dim_materiel_nouvelles[~dim_materiel_nouvelles['ID_MATERIELINFO'].isin(materiels_existants)]
        
        # Fusionner si de nouveaux matériels existent
        if len(nouveaux_materiels) > 0:
            dim_materiel_maj = pd.concat([dim_materiel_existante, nouveaux_materiels], ignore_index=True)
            dim_materiel_maj.to_csv(dimension_materiel_path, index=False)
            print(f"Table dimension MATERIEL mise à jour avec {len(nouveaux_materiels)} nouveaux matériels")
    else:
        # Créer la table
        dim_materiel_nouvelles.to_csv(dimension_materiel_path, index=False)
        print(f"Table dimension MATERIEL créée avec {len(dim_materiel_nouvelles)} matériels")
    
    return dim_materiel_nouvelles

# Modifiez les fonctions créer_table_fait_mission et créer_table_fait_materiel

def creer_table_fait_mission(date_str):
    """Crée ou met à jour la table de faits mission"""
    output_path = os.path.join(DW_PATH, "FAITS_MISSION", "faits_mission.csv")
    
    # Extraire les données
    try:
        df_missions = lire_fichiers_source(date_str, "MISSION")
        
        # Créer les dimensions associées
        creer_dimension_date(df_missions['DATE_PARIS'])
        creer_dimension_mission(df_missions)
        
        # Créer la table de faits simplifiée
        df_missions['KeyDate'] = df_missions['DATE_PARIS'].dt.strftime('%Y%m%d')
        faits_mission = df_missions[['ID_MISSION', 'ID_PERSONNEL', 'KeyDate']].copy()
        
        # Convertir explicitement toutes les colonnes en string
        faits_mission['ID_MISSION'] = faits_mission['ID_MISSION'].astype(str)
        faits_mission['ID_PERSONNEL'] = faits_mission['ID_PERSONNEL'].astype(str)
        faits_mission['KeyDate'] = faits_mission['KeyDate'].astype(str)
        
        # Si la table existe déjà, fusionner avec de nouvelles lignes
        if table_existe(output_path):
            print(f"La table de faits MISSION existe déjà, mise à jour...")
            df_existant = pd.read_csv(output_path)
            
            # Convertir explicitement toutes les colonnes en string
            df_existant['ID_MISSION'] = df_existant['ID_MISSION'].astype(str)
            df_existant['ID_PERSONNEL'] = df_existant['ID_PERSONNEL'].astype(str)
            df_existant['KeyDate'] = df_existant['KeyDate'].astype(str)
            
            # Créer une clé composite pour identifier les lignes uniques
            df_existant['COMPOSITE_KEY'] = df_existant['ID_MISSION'] + '_' + df_existant['ID_PERSONNEL'] + '_' + df_existant['KeyDate']
            faits_mission['COMPOSITE_KEY'] = faits_mission['ID_MISSION'] + '_' + faits_mission['ID_PERSONNEL'] + '_' + faits_mission['KeyDate']
            
            # Identifier les nouvelles lignes
            keys_existantes = set(df_existant['COMPOSITE_KEY'])
            nouvelles_lignes = faits_mission[~faits_mission['COMPOSITE_KEY'].isin(keys_existantes)]
            
            if len(nouvelles_lignes) > 0:
                # Supprimer la colonne composite avant la sauvegarde
                nouvelles_lignes = nouvelles_lignes.drop('COMPOSITE_KEY', axis=1)
                df_existant = df_existant.drop('COMPOSITE_KEY', axis=1)
                
                # Concaténer avec les données existantes
                df_maj = pd.concat([df_existant, nouvelles_lignes], ignore_index=True)
                df_maj.to_csv(output_path, index=False)
                print(f"Table de faits MISSION mise à jour avec {len(nouvelles_lignes)} nouvelles lignes pour la date {date_str}")
            else:
                print(f"Aucune nouvelle ligne à ajouter pour la date {date_str}")
        else:
            # Sinon on crée une nouvelle table
            faits_mission.to_csv(output_path, index=False)
            print(f"Table de faits MISSION créée avec {len(faits_mission)} lignes pour la date {date_str}")
            
    except Exception as e:
        print(f"Erreur lors de la création/mise à jour de la table MISSION: {e}")
        # For debugging
        import traceback
        print(traceback.format_exc())

def creer_table_fait_materiel(date_str):
    """Crée ou met à jour la table de faits matériel"""
    output_path = os.path.join(DW_PATH, "FAITS_MATERIEL", "faits_materiel.csv")
    
    # Extraire les données
    try:
        df_materiel = lire_fichiers_source(date_str, "MATERIEL_INFORMATIQUE")
        
        # Créer les dimensions associées
        creer_dimension_date(df_materiel['DATE_PARIS'])
        creer_dimension_materiel(df_materiel)
        
        # Créer la table de faits simplifiée
        df_materiel['KeyDate'] = df_materiel['DATE_PARIS'].dt.strftime('%Y%m%d')
        faits_materiel = df_materiel[['ID_MATERIELINFO', 'ID_PERSONNEL', 'KeyDate']].copy()
        
        # Convertir explicitement toutes les colonnes en string
        faits_materiel['ID_MATERIELINFO'] = faits_materiel['ID_MATERIELINFO'].astype(str)
        faits_materiel['ID_PERSONNEL'] = faits_materiel['ID_PERSONNEL'].astype(str)
        faits_materiel['KeyDate'] = faits_materiel['KeyDate'].astype(str)
        
        # Si la table existe déjà, fusionner avec de nouvelles lignes
        if table_existe(output_path):
            print(f"La table de faits MATERIEL existe déjà, mise à jour...")
            df_existant = pd.read_csv(output_path)
            
            # Convertir explicitement toutes les colonnes en string
            df_existant['ID_MATERIELINFO'] = df_existant['ID_MATERIELINFO'].astype(str)
            df_existant['ID_PERSONNEL'] = df_existant['ID_PERSONNEL'].astype(str)
            df_existant['KeyDate'] = df_existant['KeyDate'].astype(str)
            
            # Créer une clé composite pour identifier les lignes uniques
            df_existant['COMPOSITE_KEY'] = df_existant['ID_MATERIELINFO'] + '_' + df_existant['ID_PERSONNEL'] + '_' + df_existant['KeyDate']
            faits_materiel['COMPOSITE_KEY'] = faits_materiel['ID_MATERIELINFO'] + '_' + faits_materiel['ID_PERSONNEL'] + '_' + faits_materiel['KeyDate']
            
            # Identifier les nouvelles lignes
            keys_existantes = set(df_existant['COMPOSITE_KEY'])
            nouvelles_lignes = faits_materiel[~faits_materiel['COMPOSITE_KEY'].isin(keys_existantes)]
            
            if len(nouvelles_lignes) > 0:
                # Supprimer la colonne composite avant la sauvegarde
                nouvelles_lignes = nouvelles_lignes.drop('COMPOSITE_KEY', axis=1)
                df_existant = df_existant.drop('COMPOSITE_KEY', axis=1)
                
                # Concaténer avec les données existantes
                df_maj = pd.concat([df_existant, nouvelles_lignes], ignore_index=True)
                df_maj.to_csv(output_path, index=False)
                print(f"Table de faits MATERIEL mise à jour avec {len(nouvelles_lignes)} nouvelles lignes pour la date {date_str}")
            else:
                print(f"Aucune nouvelle ligne à ajouter pour la date {date_str}")
        else:
            # Sinon on crée une nouvelle table
            faits_materiel.to_csv(output_path, index=False)
            print(f"Table de faits MATERIEL créée avec {len(faits_materiel)} lignes pour la date {date_str}")
            
    except Exception as e:
        print(f"Erreur lors de la création/mise à jour de la table MATERIEL: {e}")
        # For debugging
        import traceback
        print(traceback.format_exc())

def table_existe(table_path):
    """Vérifie si une table existe déjà"""
    return os.path.exists(table_path)

# Vous devez également mettre à jour la fonction executer_etl_pour_date
def executer_etl_pour_date(date_str):
    """Exécute le processus ETL complet pour une date donnée"""
    print(f"Démarrage de l'ETL pour la date: {date_str}")
    
    try:
        creer_table_fait_mission(date_str)
    except Exception as e:
        print(f"Erreur lors de la mise à jour de la table MISSION: {e}")
    
    try:
        creer_table_fait_materiel(date_str)
    except Exception as e:
        print(f"Erreur lors de la mise à jour de la table MATERIEL: {e}")
    
    print(f"ETL terminé pour la date: {date_str}")


# Option 1: Process the entire year 2024
def process_full_year():
    # Generate all dates in 2024 (leap year, 366 days)
    all_dates = pd.date_range(start='2024-01-01', end='2024-12-31')
    
    # Format dates as YYYYMMDD
    formatted_dates = [date.strftime('%Y%m%d') for date in all_dates]
    
    # Count successful ETL processes
    success_count = 0
    
    # Process each date
    print(f"Starting ETL for all {len(formatted_dates)} days in 2024...")
    for date_str in formatted_dates:
        try:
            print(f"\nProcessing date {date_str} ({success_count+1}/{len(formatted_dates)})")
            executer_etl_pour_date(date_str)
            success_count += 1
        except Exception as e:
            print(f"Failed to process date {date_str}: {e}")
    
    print(f"\nETL completed for {success_count} out of {len(formatted_dates)} days in 2024")