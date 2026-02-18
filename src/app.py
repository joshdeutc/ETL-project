import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

st.set_page_config(page_title="Dashboard Carbone", layout="wide")
st.title("📊 Dashboard Carbone - Organisation (à partir de dimension_mission.csv)")

@st.cache_data
def load_data():
    df_mission = pd.read_csv("DATA_WAREHOUSE/DIM_MISSION/dimension_mission.csv", encoding='utf-8')
    df_faits = pd.read_csv("DATA_WAREHOUSE/FAITS_MISSION/faits_mission.csv", encoding='utf-8')
    df_dates = pd.read_csv("DATA_WAREHOUSE/DIM_DATE/dimension_date.csv", encoding='utf-8')

    df_all = df_faits.merge(df_mission, on="ID_MISSION", how="left")
    df_all = df_all.merge(df_dates, on="KeyDate", how="left")

    df_all['VILLE_DEPART'] = df_all['VILLE_DEPART'].str.upper()
    df_all['VILLE_DESTINATION'] = df_all['VILLE_DESTINATION'].str.upper()
    df_all['TRANSPORT'] = df_all['TRANSPORT'].str.title()
    df_all['TYPE_MISSION'] = df_all['TYPE_MISSION'].str.title()

    return df_all

df = load_data()

ANNEE = 2024
mois_list = list(range(1, 13))
ville_list = sorted(df['VILLE_DEPART'].dropna().unique().tolist())
transport_list = sorted(df['TRANSPORT'].dropna().unique().tolist())
type_mission_list = sorted(df['TYPE_MISSION'].dropna().unique().tolist())

st.title("Dashboard Carbone - Organisation")

question = st.sidebar.selectbox(
    "Choisissez une question à visualiser",
    [
        "Impact carbone mensuel par transport et site (Q19)",
        "Impact carbone global mensuel de l’organisation (Q20)",
        "Impact carbone séminaires Los Angeles (Q13)",
        "Destination la plus impactante (Q16)"
    ]
)

# Q19 - Transport et site
if question == "Impact carbone mensuel par transport et site (Q19)":
    st.header("Impact carbone mensuel par type de transport et site (Q19)")

    mois_sel = st.multiselect("Sélectionnez les mois", mois_list, default=mois_list)
    ville_sel = st.multiselect("Sélectionnez les villes de départ", ville_list, default=ville_list)
    transport_sel = st.multiselect("Sélectionnez les types de transport", transport_list, default=transport_list)

    df_filtered = df[
        (df['ANNEE'] == ANNEE) &
        (df['MOIS'].isin(mois_sel)) &
        (df['VILLE_DEPART'].isin(ville_sel)) &
        (df['TRANSPORT'].isin(transport_sel))
    ].copy()

    if df_filtered.empty:
        st.warning("Aucune donnée disponible pour les filtres sélectionnés.")
    else:
        for ville in ville_sel:
            ville_df = df_filtered[df_filtered['VILLE_DEPART'] == ville].copy()
            if not ville_df.empty:
                plt.figure(figsize=(10, 5))
                sns.barplot(
                    data=ville_df,
                    x="MOIS", y="GES",
                    hue="TRANSPORT",
                    estimator=np.sum,
                    errorbar=None
                )
                plt.yscale("log")
                plt.title(f"Impact carbone (CO2 en tonne) par transport – {ville}")
                plt.xlabel("Mois")
                plt.ylabel("GES (échelle log)")
                st.pyplot(plt.gcf())
                plt.clf()

# Q20 - Global organisation
elif question == "Impact carbone global mensuel de l’organisation (Q20)":
    st.header("Impact carbone global mensuel de l’organisation (Q20)")

    df_filtered = df[df['ANNEE'] == ANNEE].copy()
    df_grouped = df_filtered.groupby('MOIS')['GES'].sum().reindex(mois_list, fill_value=0).reset_index()

    plt.figure(figsize=(10, 4))
    sns.lineplot(data=df_grouped, x='MOIS', y='GES', marker='o')
    plt.title("Impact carbone global mensuel (CO2 en tonne)")
    plt.xlabel("Mois")
    plt.ylabel("GES")
    st.pyplot(plt.gcf())
    plt.clf()

# Q13 - Séminaires selon ville de départ
elif question == "Impact carbone séminaires Los Angeles (Q13)":
    st.header("Impact carbone des séminaires organisés par ville de départ (Q13)")

    villes_possible = ['BERLIN', 'LONDON', 'LOS ANGELES', 'NEW-YORK', 'PARIS', 'SHANGHAI']
    ville_sel = st.selectbox("Sélectionnez une ville de départ", villes_possible, index=villes_possible.index("LOS ANGELES"))
    types_possible = ['Conférence', 'Développement', 'Formation', 'Réunion', 'Rencontre entreprises']
    type_sel = st.selectbox("Sélectionnez une ville de départ", types_possible, index=types_possible.index("Conférence"))
    mois_sel = st.multiselect("Sélectionnez les mois", mois_list, default=[7])

    df_filtered = df[
        (df['ANNEE'] == ANNEE) &
        (df['MOIS'].isin(mois_sel)) &
        (df['VILLE_DEPART'] == ville_sel) &
        (df['TYPE_MISSION'] == type_sel)
    ].copy()

    if df_filtered.empty:
        st.warning(f"Aucune mission trouvée depuis {ville_sel} sur les mois sélectionnés.")
    else:
        impact = df_filtered['GES'].sum()
        st.success(
            f"Impact carbone total des séminaires partant de **{ville_sel}** : "
            f"**{impact:.2f} GES**"
        )

# Q16 - Destination la plus impactante (filtrable)
elif question == "Destination la plus impactante (Q16)":
    st.header("Destination la plus impactante (Q16)")

    mois_sel = st.multiselect("Sélectionnez les mois", mois_list, default=list(range(5, 11)))

    df_filtered = df[
        (df['ANNEE'] == 2024) &
        (df['MOIS'].isin(mois_sel))
    ].copy()

    if df_filtered.empty:
        st.warning("Aucune donnée disponible pour les mois sélectionnés.")
    else:
        df_grouped = df_filtered.groupby("VILLE_DESTINATION")['GES'].sum().reset_index()
        top_dest = df_grouped.loc[df_grouped['GES'].idxmax()]

        st.success(
            f"Destination la plus impactante pour {', '.join(map(str, mois_sel))} : "
            f"**{top_dest['VILLE_DESTINATION']}** avec **{top_dest['GES']:.2f} GES**."
        )
