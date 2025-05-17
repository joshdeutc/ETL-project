import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
import matplotlib
import os

# Configurer matplotlib pour générer des PDF
matplotlib.use('Agg')

# Fonction pour créer le dossier de sortie s'il n'existe pas
def ensure_output_dir():
    if not os.path.exists('output_figures'):
        os.makedirs('output_figures')

# Configuration de la session Spark
@st.cache_resource
def get_spark_session():
    return SparkSession.builder \
        .appName("Analyse Carbone") \
        .getOrCreate()

# Charger les données
@st.cache_data
def load_data(spark):
    # Lecture des fichiers avec les séparateurs appropriés
    dim_personnel = spark.read.csv("DATA_WAREHOUSE/DIM_PERSONNEL/dimension_personnel.csv", 
                                header=True, inferSchema=True, sep=";")
    dim_mission = spark.read.csv("DATA_WAREHOUSE/DIM_MISSION/dimension_mission.csv", 
                               header=True, inferSchema=True)
    faits_mission = spark.read.csv("DATA_WAREHOUSE/FAITS_MISSION/faits_mission.csv", 
                                 header=True, inferSchema=True)
    dim_date = spark.read.csv("DATA_WAREHOUSE/DIM_DATE/dimension_date.csv", 
                            header=True, inferSchema=True)
    
    # Création des vues temporaires
    dim_personnel.createOrReplaceTempView("personnel")
    dim_mission.createOrReplaceTempView("mission")
    faits_mission.createOrReplaceTempView("faits_mission")
    dim_date.createOrReplaceTempView("dates")
    
    return spark

# Application Streamlit
def main():
    st.title("Analyse d'Impact Carbone")
    
    # Initialiser Spark
    spark = get_spark_session()
    load_data(spark)
    ensure_output_dir()
    
    # Question 18: Les 5 missions les plus impactantes sur le site de Paris
    st.header("Question 18: Les 5 missions les plus impactantes sur le site de Paris")
    
    q18_query = """
    SELECT m.ID_MISSION, m.TYPE_MISSION, m.VILLE_DESTINATION, m.TRANSPORT, m.GES
    FROM mission m
    JOIN faits_mission f ON m.ID_MISSION = f.ID_MISSION
    WHERE m.VILLE_DEPART = 'Paris'
    ORDER BY m.GES DESC
    LIMIT 5
    """
    
    q18_result = spark.sql(q18_query).toPandas()
    
    # Créer le graphique
    fig18, ax18 = plt.subplots(figsize=(10, 6))
    bars = ax18.bar(q18_result['ID_MISSION'], q18_result['GES'], color='skyblue')
    
    # Annoter chaque barre avec le type de mission et la destination
    for i, bar in enumerate(bars):
        mission_info = f"{q18_result.iloc[i]['TYPE_MISSION']} → {q18_result.iloc[i]['VILLE_DESTINATION']}\n({q18_result.iloc[i]['TRANSPORT']})"
        ax18.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                 mission_info, ha='center', va='bottom', rotation=0, fontsize=8)
    
    ax18.set_ylabel('Impact Carbone (GES)')
    ax18.set_title('Les 5 missions les plus impactantes au départ de Paris')
    plt.xticks([])  # Hide x-axis labels as they're replaced by annotations
    
    st.pyplot(fig18)
    plt.savefig('output_figures/q18_missions_paris.pdf')
    plt.close(fig18)
    
    # Question 19: Comparer l'impact carbone mensuel des missions par type de transport et par site
    st.header("Question 19: Impact carbone mensuel des missions par transport et par site")
    
    q19_query = """
    SELECT d.MOIS, m.VILLE_DEPART, m.TRANSPORT, SUM(m.GES) as total_impact
    FROM mission m
    JOIN faits_mission f ON m.ID_MISSION = f.ID_MISSION
    JOIN dates d ON f.KeyDate = d.KeyDate
    WHERE d.ANNEE = 2024
    GROUP BY d.MOIS, m.VILLE_DEPART, m.TRANSPORT
    ORDER BY d.MOIS, m.VILLE_DEPART
    """
    
    q19_result = spark.sql(q19_query).toPandas()
    
    # Créer une figure pour chaque mois
    months = sorted(q19_result['MOIS'].unique())
    
    # Create a combined figure for all months
    fig19, axes = plt.subplots(3, 4, figsize=(20, 15))  # 3x4 grid for 12 months
    axes = axes.flatten()
    
    for i, month in enumerate(range(1, 13)):  # Months 1-12
        month_data = q19_result[q19_result['MOIS'] == month]
        if month_data.empty:
            axes[i].text(0.5, 0.5, f"Pas de données pour le mois {month}", 
                        ha='center', va='center')
            axes[i].set_title(f"Mois {month}")
            continue
            
        # Pivot data for stacked bar chart
        pivot_data = month_data.pivot_table(
            index='VILLE_DEPART', 
            columns='TRANSPORT', 
            values='total_impact', 
            aggfunc='sum'
        ).fillna(0)
        
        # Plot stacked bar chart
        pivot_data.plot(kind='bar', stacked=True, ax=axes[i], colormap='viridis')
        axes[i].set_title(f"Mois {month}")
        axes[i].set_ylabel('Impact Carbone (GES)')
        axes[i].legend(loc='upper right', fontsize='small')
        axes[i].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    st.pyplot(fig19)
    plt.savefig('output_figures/q19_impact_by_transport_site.pdf')
    plt.close(fig19)
    
    # Question 20: Impact carbone global mensuel de l'organisation
    st.header("Question 20: Impact carbone global mensuel de l'organisation")
    
    q20_query = """
    SELECT 
        d.MOIS,
        SUM(CASE WHEN m.ID_MISSION IS NOT NULL THEN m.GES ELSE 0 END) as impact_missions,
        SUM(CASE WHEN mat.ID_MATERIELINFO IS NOT NULL THEN mat.GES ELSE 0 END) as impact_materiel,
        SUM(COALESCE(m.GES, 0) + COALESCE(mat.GES, 0)) as impact_total
    FROM dates d
    LEFT JOIN faits_mission fm ON d.KeyDate = fm.KeyDate
    LEFT JOIN mission m ON fm.ID_MISSION = m.ID_MISSION
    LEFT JOIN faits_materiel fmat ON d.KeyDate = fmat.KeyDate
    LEFT JOIN materiel mat ON fmat.ID_MATERIELINFO = mat.ID_MATERIELINFO
    WHERE d.ANNEE = 2024
    GROUP BY d.MOIS
    ORDER BY d.MOIS
    """
    
    q20_result = spark.sql(q20_query).toPandas()
    
    # Create the stacked area chart
    fig20, ax20 = plt.subplots(figsize=(12, 8))
    
    # Ensure we have data for all 12 months (fill with 0 if missing)
    months_df = pd.DataFrame({'MOIS': range(1, 13)})
    q20_result = pd.merge(months_df, q20_result, on='MOIS', how='left').fillna(0)
    q20_result = q20_result.sort_values('MOIS')
    
    # Plot stacked area chart
    ax20.fill_between(q20_result['MOIS'], 0, q20_result['impact_missions'], 
                     label='Impact des Missions', alpha=0.7, color='skyblue')
    ax20.fill_between(q20_result['MOIS'], q20_result['impact_missions'], 
                     q20_result['impact_total'], 
                     label='Impact du Matériel', alpha=0.7, color='salmon')
    
    # Add total line
    ax20.plot(q20_result['MOIS'], q20_result['impact_total'], 'k-', 
             label='Impact Total', linewidth=2)
    
    # Add data point annotations
    for i, row in q20_result.iterrows():
        ax20.annotate(f"{row['impact_total']:.1f}", 
                     (row['MOIS'], row['impact_total']),
                     xytext=(0, 10), textcoords='offset points',
                     ha='center', fontsize=8)
    
    ax20.set_xlabel('Mois')
    ax20.set_ylabel('Impact Carbone (GES)')
    ax20.set_title('Impact Carbone Global Mensuel de l\'Organisation en 2024')
    ax20.set_xticks(range(1, 13))
    ax20.set_xticklabels(['Jan', 'Fév', 'Mars', 'Avr', 'Mai', 'Juin', 
                         'Juil', 'Août', 'Sept', 'Oct', 'Nov', 'Déc'])
    ax20.legend()
    ax20.grid(axis='y', linestyle='--', alpha=0.7)
    
    st.pyplot(fig20)
    plt.savefig('output_figures/q20_global_carbon_impact.pdf')
    plt.close(fig20)
    
    st.success("Toutes les figures ont été générées et enregistrées en format PDF dans le dossier 'output_figures'.")

if __name__ == "__main__":
    main()