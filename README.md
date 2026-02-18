# NF26 - Projet BGES (Bilan Gaz a Effet de Serre)

Projet ETL realise dans le cadre de l'UV **NF26** a l'**UTC** (Universite de Technologie de Compiegne).

Il consiste a extraire, transformer et charger des donnees de missions professionnelles et de materiel informatique provenant de 6 bureaux internationaux, afin d'analyser leur **empreinte carbone** (emissions de GES).

## Structure du projet

```
ETL-project/
|
|-- src/                             # Code source Python
|   |-- etl_warehouse.py             #   Pipeline ETL principal
|   |-- mission_ges.py               #   Calcul des emissions GES des missions
|   |-- materiel_ges.py              #   Calcul des emissions GES du materiel
|   |-- distance.py                  #   Calcul des distances geographiques
|   |-- app.py                       #   Dashboard interactif Streamlit
|
|-- data/
|   |-- sources/                     # Donnees brutes (BDD_BGES)
|   |   |-- BDD_BGES/
|   |       |-- materiel_informatique_impact.csv
|   |       |-- BDD_BGES_BERLIN/
|   |       |-- BDD_BGES_LONDON/
|   |       |-- BDD_BGES_LOSANGELES/
|   |       |-- BDD_BGES_NEWYORK/
|   |       |-- BDD_BGES_PARIS/
|   |       |-- BDD_BGES_SHANGHAI/
|   |
|   |-- warehouse/                   # Entrepot de donnees (schema en etoile)
|       |-- DIM_DATE/                #   Dimension temporelle
|       |-- DIM_MISSION/             #   Dimension missions
|       |-- DIM_MATERIEL/            #   Dimension materiel informatique
|       |-- DIM_PERSONNEL/           #   Dimension personnel
|       |-- FAITS_MISSION/           #   Table de faits missions
|       |-- FAITS_MATERIEL/          #   Table de faits materiel
|
|-- notebooks/
|   |-- projetnf26.ipynb             # Notebook Jupyter d'analyse
|
|-- cache/                           # Cache de geocodage
|   |-- coordinates_cache.csv        #   Coordonnees GPS des villes
|   |-- distances_cache.csv          #   Distances entre paires de villes
|
|-- docs/                            # Documentation
|   |-- NF26_Questions_Projet_BGES.pdf
|   |-- NF26_Slides_Projet_BGES.pdf
|
|-- figures/                         # Graphiques exportes
|   |-- Figure_18.pdf
|   |-- figure_19.pdf
|   |-- figure_20.pdf
|
|-- requirements.txt                 # Dependances Python
|-- .gitignore
|-- README.md
```

## Architecture ETL

Le projet suit une architecture **ETL classique** avec un **schema en etoile** :

1. **Extract** : Lecture des fichiers `.txt` quotidiens (separes par `;`) depuis les 6 bureaux internationaux
2. **Transform** :
   - Conversion des fuseaux horaires locaux vers le fuseau de Paris
   - Calcul des distances geographiques entre villes (via Geopy/OpenStreetMap)
   - Calcul des emissions GES selon le mode de transport et la distance
   - Calcul de l'impact carbone du materiel informatique
3. **Load** : Chargement incrementiel dans un Data Warehouse en schema en etoile (CSV)

### Bureaux couverts

| Ville        | Fuseau horaire    |
|--------------|-------------------|
| Paris        | Europe/Paris      |
| Berlin       | Europe/Berlin     |
| London       | Europe/London     |
| New York     | America/New_York  |
| Los Angeles  | America/Los_Angeles |
| Shanghai     | Asia/Shanghai     |

### Facteurs d'emission (modes de transport)

| Mode de transport    | Facteur d'emission (kg CO2/km) |
|----------------------|-------------------------------|
| Avion                | 0.152 - 0.259 (selon distance)|
| Train                | 0.003 - 0.018 (selon distance)|
| Taxi                 | 0.216                         |
| Transports en commun | 0.047                         |

## Technologies utilisees

- **Python 3.11+**
- **Pandas** - Manipulation et transformation des donnees
- **PySpark** - Traitement distribue (configure, optionnel)
- **Geopy** - Geocodage et calcul de distances
- **Streamlit** - Dashboard web interactif
- **Matplotlib / Seaborn** - Visualisation de donnees
- **Jupyter Notebook** - Analyse exploratoire

## Installation

### Prerequis

- Python 3.11 ou superieur
- pip (gestionnaire de paquets Python)

### Installer les dependances

```bash
pip install -r requirements.txt
```

## Utilisation

### 1. Executer le pipeline ETL

Le script principal charge les donnees sources et alimente le Data Warehouse :

```bash
python src/etl_warehouse.py
```

Cela va :
- Lire les fichiers de missions et de materiel depuis `data/sources/`
- Calculer les emissions GES pour chaque enregistrement
- Alimenter les tables de dimensions et de faits dans `data/warehouse/`

### 2. Lancer le dashboard Streamlit

```bash
streamlit run src/app.py
```

Le dashboard permet de visualiser :
- L'impact carbone mensuel par mode de transport et par site
- L'empreinte carbone globale mensuelle de l'organisation
- L'impact des conferences/seminaires par ville de depart
- Les destinations les plus impactantes en emissions carbone

### 3. Analyser via le notebook

```bash
jupyter notebook notebooks/projetnf26.ipynb
```

Le notebook contient l'analyse exploratoire complete et les reponses aux questions du projet.

## Donnees

- **Periode couverte** : Avril - Decembre 2024
- **~2 400 fichiers sources** repartis sur les 6 bureaux
- **~20 600 enregistrements** de missions dans le Data Warehouse
- Format source : fichiers `.txt` separes par `;`

## Auteurs

Projet realise dans le cadre de l'UV NF26 - UTC.
