import streamlit as st
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pandas as pd
import altair as alt
import os
from streamlit_option_menu import option_menu
import streamlit.components.v1 as components
import plotly.express as px
try:
    import pycountry
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pycountry'])
    import pycountry

# —————————————————————————————————————————————————————————————————————————————————
# Fonctions utilitaires placées AVANT toute utilisation
# —————————————————————————————————————————————————————————————————————————————————
def iso2_to_iso3(iso2):
    try:
        return pycountry.countries.get(alpha_2=iso2.upper()).alpha_3
    except:
        return None

def map_job_to_category(job_title):
    # Mapping manuel, à compléter selon les besoins
    job_title = job_title.lower()
    if any(x in job_title for x in ['ai', 'artificial intelligence', 'ml', 'machine learning']):
        return 'AI'
    elif 'data engineer' in job_title or 'data eng' in job_title:
        return 'Data Engineer'
    elif 'data scientist' in job_title or 'data science' in job_title:
        return 'Data Science'
    elif 'analyst' in job_title:
        return 'Data Analyst'
    elif 'architect' in job_title:
        return 'Data Architect'
    elif 'manager' in job_title:
        return 'Manager'
    elif 'product' in job_title:
        return 'Product'
    elif 'cloud' in job_title:
        return 'Cloud'
    elif 'director' in job_title:
        return 'Director'
    elif 'engineer' in job_title:
        return 'Engineer'
    else:
        return 'Autre'

def remove_salary_outliers(df):
    df_clean = pd.DataFrame()
    for country, group in df.groupby('country'):
        if group['salary_in_usd'].count() < 3:
            df_clean = pd.concat([df_clean, group], ignore_index=True)
            continue
        q1 = group['salary_in_usd'].quantile(0.25)
        q3 = group['salary_in_usd'].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        filtered = group[(group['salary_in_usd'] >= lower) & (group['salary_in_usd'] <= upper)]
        df_clean = pd.concat([df_clean, filtered], ignore_index=True)
    # Ajout de la colonne catégorie de poste
    if 'job_title' in df_clean.columns:
        df_clean['job_category'] = df_clean['job_title'].apply(map_job_to_category)
    return df_clean

# —————————————————————————————————————————————————————————————————————————————————
# 1) CONFIGURATION GLOBALE & CSS
# —————————————————————————————————————————————————————————————————————————————————

# Page config
st.set_page_config(
    page_title="Data Science Salary Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# On injecte un peu de CSS pour un thème sombre ultra-moderne et des KPIs ultra-lisibles
st.markdown(
    """
    <style>
      html, body, .stApp {
        background: linear-gradient(135deg, #18122B 0%, #393053 100%) !important;
        color: #fff !important;
      }
      .stApp {
        min-height: 100vh;
      }
      .css-18e3th9, .css-1d391kg, .css-1v3fvcr {
        background: rgba(30,34,42,0.95) !important;
        color: #fff !important;
        border-radius: 1rem;
        box-shadow: 0 4px 32px 0 #00000055;
      }
      .css-2trqyj h1, .css-2trqyj h2, h1, h2 {
        color: #A259F7 !important;
        text-align: center !important;
        letter-spacing: 2px;
        text-shadow: 0 2px 16px #A259F755, 0 1px 0 #fff2;
      }
      .css-1hynsf2, .stMarkdown, .stText, .stDataFrame, .stTable {
        color: #fff !important;
      }
      /* KPIs ultra-lisibles */
      .stMetric {
        background: linear-gradient(90deg, #7C3AED33 0%, #06D6A033 100%);
        border-radius: 1rem;
        box-shadow: 0 2px 16px #7C3AED44;
        padding: 1rem 0.5rem;
        margin-bottom: 0.5rem;
      }
      .stMetricLabel, .stMetricValue, .element-container .stMetric label, .element-container .stMetric div {
        color: #fff !important;
        font-weight: bold !important;
        text-shadow: 0 2px 8px #000a;
        font-size: 1.3rem !important;
      }
      .stMetricValue {
        font-size: 2.2rem !important;
      }
      .stButton>button {
        background: linear-gradient(90deg, #7C3AED 0%, #06D6A0 100%);
        color: #fff;
        border: none;
        border-radius: 0.5rem;
        font-weight: bold;
        box-shadow: 0 2px 8px #7C3AED55;
      }
      .stDataFrame, .stTable {
        background: #232946 !important;
        color: #fff !important;
        border-radius: 0.5rem;
      }
      .stSidebarContent {
        background: #232946 !important;
        color: #fff !important;
      }
      .stDownloadButton>button {
        background: linear-gradient(90deg, #A259F7 0%, #06D6A0 100%);
        color: #fff;
        border: none;
        border-radius: 0.5rem;
        font-weight: bold;
        box-shadow: 0 2px 8px #A259F755;
      }
    </style>
    """,
    unsafe_allow_html=True
)

# Altair theme dark + palette personnalisée
def dark_theme():
    return {
        "config": {
            "background": "#1E222A",
            "title": {"color": "#ECEFF4"},
            "axis": {
                "domainColor": "#4C566A",
                "gridColor": "#3B4252",
                "tickColor": "#D8DEE9",
                "labelColor": "#D8DEE9",
                "titleColor": "#ECEFF4"
            },
            "legend": {"labelColor": "#D8DEE9", "titleColor": "#ECEFF4"}
        }
    }
alt.themes.register("dark_theme", dark_theme)
alt.themes.enable("dark_theme")

# —————————————————————————————————————————————————————————————————————————————————
# 2) INITIALISATION DE SPARK
# —————————————————————————————————————————————————————————————————————————————————

# Java (ajuster si besoin)
os.environ["JAVA_HOME"] = r"C:/Program Files/Java/jre1.8.0_202"
os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + ";" + os.environ["PATH"]

spark = SparkSession.builder \
    .appName("SalaryDashboard") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

@st.cache_resource
def load_data(path):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.withColumnRenamed("employee_residence", "country")
    return df.dropna(subset=["work_year", "salary", "country", "job_title"])

df = load_data("salaries.csv")

# —————————————————————————————————————————————————————————————————————————————————
# 3) FILTRES (MENU DÉDIÉ)
# —————————————————————————————————————————————————————————————————————————————————

with st.sidebar:
    st.header("Filtres")
    # Utilisation de RDD pour extraire les valeurs uniques
    years = df.select("work_year").rdd.map(lambda row: row[0]).distinct().collect()
    years = sorted([y for y in years if y is not None])
    countries = df.select("country").rdd.map(lambda row: row[0]).distinct().collect()
    countries = sorted([c for c in countries if c is not None])
    jobs = df.select("job_title").rdd.map(lambda row: row[0]).distinct().collect()
    jobs = sorted([j for j in jobs if j is not None])

    # Nouveau filtre expérience
    exp_levels = df.select("experience_level").rdd.map(lambda row: row[0]).distinct().collect()
    exp_levels = [e for e in exp_levels if e is not None]
    exp_labels = {'EN': 'Entry', 'MI': 'Mid', 'SE': 'Senior', 'EX': 'Exec'}
    exp_options = [exp_labels.get(e, e) for e in exp_levels]
    selected_exp = st.multiselect(
        label="Niveau d'expérience",
        options=exp_options,
        default=exp_options,
        help="Filtrer par niveau d'expérience (multi-sélection)"
    )

    selected_years = st.multiselect(
        label="Années",
        options=years,
        default=years,
        help="Filtrer par année (multi-sélection, recherche possible)"
    )
    selected_countries = st.multiselect(
        label="Pays",
        options=countries,
        default=countries,
        help="Filtrer par pays (multi-sélection, recherche possible)"
    )
    selected_jobs = st.multiselect(
        label="Poste",
        options=jobs,
        default=jobs,  # Par défaut, TOUS les postes sont sélectionnés
        help="Filtrer par poste (multi-sélection, recherche possible)"
    )

# Pour récupérer les valeurs sélectionnées côté Python, il faut utiliser st.session_state
# —————————————————————————————————————————————————————————————————————————————————
# 4) FILTRAGE & CONVERSION
# —————————————————————————————————————————————————————————————————————————————————

# Filtrage via Spark DataFrame (plus rapide que RDD)
# Conversion des labels expérience sélectionnés en codes
exp_label_to_code = {v: k for k, v in {'EN': 'Entry', 'MI': 'Mid', 'SE': 'Senior', 'EX': 'Exec'}.items()}
selected_exp_codes = [exp_label_to_code.get(e, e) for e in selected_exp]

filtered = df.filter(
    (col("work_year").isin(selected_years)) &
    (col("country").isin(selected_countries)) &
    (col("job_title").isin(selected_jobs)) &
    (col("experience_level").isin(selected_exp_codes))
)
pdf = filtered.toPandas()

# —————————————————————————————————————————————————————————————————————————————————
# 5) TOP KPIs
# —————————————————————————————————————————————————————————————————————————————————

# Application du nettoyage sur le DataFrame filtré
pdf = remove_salary_outliers(pdf)
pdf = pdf[pdf['country'] != 'CD']
pdf = pdf[~((pdf['country'] == 'DZ') & (pdf['salary_in_usd'] == 100000))]

# Calcul des KPIs sur le DataFrame nettoyé
import numpy as np
kpi1 = int(pdf['salary_in_usd'].mean()) if not pdf.empty else 0
kpi2 = int(pdf['salary_in_usd'].median()) if not pdf.empty else 0
kpi3 = int(pdf['salary_in_usd'].min()) if not pdf.empty else 0
kpi4 = int(pdf['salary_in_usd'].max()) if not pdf.empty else 0
kpi5 = pdf['country'].nunique() if not pdf.empty else 0
kpi6 = pdf['job_title'].nunique() if not pdf.empty else 0
kpi7 = pdf['job_category'].nunique() if 'job_category' in pdf.columns and not pdf.empty else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("💰 Salaire moyen (USD)", f"{kpi1:,}")
col2.metric("🔎 Salaire médian (USD)", f"{kpi2:,}")
col3.metric("⬇️ Min (USD)", f"{kpi3:,}")
col4.metric("⬆️ Max (USD)", f"{kpi4:,}")
col5, col6, col7 = st.columns(3)
col5.metric("🌍 Pays", kpi5)
col6.metric("👔 Postes", kpi6)
col7.metric("🗂️ Catégories de poste", kpi7)

# —————————————————————————————————————————————————————————————————————————————————
# 6) VISUALISATIONS INTERACTIVES
# —————————————————————————————————————————————————————————————————————————————————

st.title("🚀 Data Science Salary Dashboard")

# INTRO STORYTELLING
st.markdown("""
<div style='text-align:center; font-size:1.3rem; margin-bottom:1.5em;'>
<b>Bienvenue sur le dashboard interactif des salaires en Data Science !</b><br>
Explorez les tendances mondiales, comparez les postes, et découvrez les dynamiques du marché grâce à des visualisations modernes et des explications claires.
</div>
""", unsafe_allow_html=True)

# 6.0 Carte du monde des salaires moyens par pays (hors filtres)
st.markdown("""
<div style='color:#fff; background:rgba(0,0,0,0); font-size:1.1rem; text-align:center; margin-bottom:0.5em;'>
<b>🌍 Carte mondiale</b> : Visualisez d'un coup d'œil les pays où les salaires en Data Science sont les plus élevés. Passez la souris sur un pays pour voir le salaire moyen en USD. Cette carte n'est pas affectée par les filtres et donne une vision globale du marché.
</div>
""", unsafe_allow_html=True)

# On charge le dataset complet pour la carte
# La carte doit prendre en compte tous les filtres SAUF le filtre pays
full_df = load_data("salaries.csv").toPandas()
# Application des mêmes filtres que pdf, sauf pays
full_df = full_df[full_df['work_year'].isin(selected_years)]
full_df = full_df[full_df['job_title'].isin(selected_jobs)]
full_df = full_df[full_df['experience_level'].isin(selected_exp_codes)]
full_df = remove_salary_outliers(full_df)
full_df = full_df[full_df['country'] != 'CD']
full_df = full_df[~((full_df['country'] == 'DZ') & (full_df['salary_in_usd'] == 100000))]

country_salary_full = full_df.groupby('country')['salary_in_usd'].mean().reset_index()
country_salary_full['iso_alpha'] = country_salary_full['country'].apply(iso2_to_iso3)
country_salary_full = country_salary_full.dropna(subset=['iso_alpha'])
fig_map = px.choropleth(
    country_salary_full,
    locations='iso_alpha',
    color='salary_in_usd',
    color_continuous_scale=px.colors.sequential.Plasma,
    hover_name='country',
    labels={'salary_in_usd': 'Salaire moyen (USD)'},
    title='',
    template='plotly_dark',
    projection='natural earth',
    locationmode='ISO-3',
)
fig_map.update_layout(
    title_text='<b style="color:#fff">Carte mondiale des salaires moyens en Data Science (USD)</b>',
    title_x=0.5,
    geo=dict(bgcolor='rgba(0,0,0,0)', showframe=False, showcoastlines=True, coastlinecolor='#A259F7'),
    margin=dict(l=0, r=0, t=60, b=0),
    font=dict(color='#fff', size=16),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)
fig_map.update_traces(marker_line_color='#fff', marker_line_width=0.5)
st.markdown("""
<div style='margin-bottom: -2em'></div>
""", unsafe_allow_html=True)
st.plotly_chart(fig_map, use_container_width=True)
st.markdown("""
---
""", unsafe_allow_html=True)

# 6.1 Évolution du salaire moyen par année
st.markdown("""
#### 📈 Évolution du salaire moyen par année
*Comment les salaires évoluent-ils dans le temps ? Cette courbe montre la tendance du salaire moyen en Data Science selon l'année sélectionnée.*
""")
if not pdf.empty:
    avg_by_year = pdf.groupby("work_year")["salary_in_usd"].mean().reset_index()
    chart1 = (
        alt.Chart(avg_by_year)
           .mark_line(point=True, strokeWidth=3, color="#7C3AED")
           .encode(
               x=alt.X("work_year:O", title="Année"),
               y=alt.Y("salary_in_usd:Q", title="Salaire moyen (USD)"),
               tooltip=["work_year", alt.Tooltip("salary_in_usd", format=",.0f")]
           )
           .properties(width=600, height=350, title="Salaire moyen par année")
    )
    st.altair_chart(chart1, use_container_width=True)

# 6.2 Distribution des salaires (histogramme)
st.markdown("""
#### 📊 Distribution des salaires
*Quelle est la répartition des salaires ? L'histogramme ci-dessous permet d'identifier les fourchettes de rémunération les plus fréquentes.*
""")
if not pdf.empty:
    chart2 = (
        alt.Chart(pdf)
           .mark_bar(color="#06D6A0")
           .encode(
               alt.X("salary_in_usd:Q", bin=alt.Bin(maxbins=40), title="Salaire (USD)"),
               alt.Y("count()", title="Nombre"),
               tooltip=[alt.Tooltip("count()", title="Nombre")]
           )
           .properties(width=600, height=350, title="Distribution des salaires (USD)")
    )
    st.altair_chart(chart2, use_container_width=True)

# —————————————————————————————————————————————————————————————————————————————————
# 6.3 Graphique hiérarchique simple : top 10 catégories puis top 10 sous-postes via selectbox
st.markdown("""
#### 🏆 Top 10 des salaires par catégorie de poste
*Sélectionnez une catégorie pour explorer les sous-postes.*
""")
if not pdf.empty:
    # Top 10 catégories
    top_categories = (
        pdf.groupby("job_category")["salary_in_usd"]
           .mean().reset_index()
           .sort_values("salary_in_usd", ascending=False).head(10)
    )
    cat_options = ["(Aucune)"] + list(top_categories['job_category'])
    selected_cat = st.selectbox("Catégorie de poste", options=cat_options, index=0)
    if selected_cat == "(Aucune)":
        chart = (
            alt.Chart(top_categories)
               .mark_bar(color="#F59E42")
               .encode(
                   x=alt.X("salary_in_usd:Q", title="Salaire moyen (USD)"),
                   y=alt.Y("job_category:N", sort="-x", title="Catégorie de poste"),
                   tooltip=["job_category", alt.Tooltip("salary_in_usd", format=",.0f")],
               )
               .properties(width=700, height=400, title="Top 10 des catégories de poste")
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        sub_jobs = pdf[pdf['job_category'] == selected_cat]
        top_sub = (
            sub_jobs.groupby("job_title")["salary_in_usd"]
                .mean().reset_index()
                .sort_values("salary_in_usd", ascending=False).head(10)
        )
        chart = (
            alt.Chart(top_sub)
               .mark_bar(color="#7C3AED")
               .encode(
                   x=alt.X("salary_in_usd:Q", title="Salaire moyen (USD)"),
                   y=alt.Y("job_title:N", sort="-x", title="Poste"),
                   tooltip=["job_title", alt.Tooltip("salary_in_usd", format=",.0f")]
               )
               .properties(width=700, height=400, title=f"Top 10 des sous-postes pour {selected_cat}")
        )
        st.altair_chart(chart, use_container_width=True)


# 6.5 Salaire moyen par niveau d’expérience
st.markdown("""
#### 🎓 Salaire moyen par niveau d'expérience
*L'expérience paie-t-elle vraiment ? Comparez le salaire moyen selon le niveau d'ancienneté (Entry, Mid, Senior, Exec).*""")
if not pdf.empty and 'experience_level' in pdf.columns:
    exp_salary = pdf.groupby('experience_level')['salary_in_usd'].mean().reset_index()
    exp_labels = {'EN': 'Entry', 'MI': 'Mid', 'SE': 'Senior', 'EX': 'Exec'}
    exp_salary['experience_level'] = exp_salary['experience_level'].map(exp_labels).fillna(exp_salary['experience_level'])
    chart_exp = (
        alt.Chart(exp_salary)
           .mark_bar(color="#3B82F6")
           .encode(
               x=alt.X('experience_level:N', title='Niveau d\'expérience'),
               y=alt.Y('salary_in_usd:Q', title='Salaire moyen (USD)'),
               tooltip=['experience_level', alt.Tooltip('salary_in_usd', format=",.0f")]
           )
           .properties(width=400, height=350, title="Salaire moyen par niveau d'expérience")
    )
    st.altair_chart(chart_exp, use_container_width=True)

# 6.6 Salaire moyen par taille d’entreprise
st.markdown("""
#### 🏢 Salaire moyen par taille d'entreprise
*Les grandes entreprises paient-elles mieux ? Ce graphique compare le salaire moyen selon la taille de la société.*""")
if not pdf.empty and 'company_size' in pdf.columns:
    size_labels = {'S': 'Small', 'M': 'Medium', 'L': 'Large'}
    size_salary = pdf.groupby('company_size')['salary_in_usd'].mean().reset_index()
    size_salary['company_size'] = size_salary['company_size'].map(size_labels).fillna(size_salary['company_size'])
    chart_size = (
        alt.Chart(size_salary)
           .mark_bar(color="#A259F7")
           .encode(
               x=alt.X('company_size:N', title="Taille d'entreprise"),
               y=alt.Y('salary_in_usd:Q', title='Salaire moyen (USD)'),
               tooltip=['company_size', alt.Tooltip('salary_in_usd', format=",.0f")]
           )
           .properties(width=400, height=350, title="Salaire moyen par taille d'entreprise")
    )
    st.altair_chart(chart_size, use_container_width=True)

# 6.8 Répartition remote/hybride/présentiel (remote_ratio)
st.markdown("""
#### 🏠 Répartition Remote / Hybride / Présentiel
*Le télétravail est-il la norme ? Visualisez la part des emplois 100% remote, hybrides ou en présentiel.*""")
if not pdf.empty and 'remote_ratio' in pdf.columns:
    remote_labels = {0: 'Présentiel', 50: 'Hybride', 100: 'Remote'}
    remote_counts = pdf['remote_ratio'].map(remote_labels).fillna(pdf['remote_ratio']).value_counts().reset_index()
    remote_counts.columns = ['Mode', 'Nombre']
    fig_remote = px.pie(remote_counts, names='Mode', values='Nombre', color_discrete_sequence=px.colors.sequential.Plasma, title='Répartition Remote/Hybride/Présentiel')
    fig_remote.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='#fff',
        legend_font_color='#fff',
        legend_title_font_color='#fff',
        title_font_color='#fff',
    )
    st.plotly_chart(fig_remote, use_container_width=True)

# 6.9 Table interactive et téléchargement
st.markdown("""
#### 📋 Données filtrées
*Retrouvez ici le détail des données selon vos filtres. Vous pouvez trier, rechercher, et télécharger le résultat pour vos propres analyses !*
""")
if not pdf.empty:
    st.markdown('### 📋 Données filtrées')
    st.dataframe(pdf, use_container_width=True)
    st.download_button('📥 Télécharger les données filtrées (CSV)', data=pdf.to_csv(index=False), file_name='filtered_salaries.csv', mime='text/csv')


# —————————————————————————————————————————————————————————————————————————————————
# 7) FOOTER
# —————————————————————————————————————————————————————————————————————————————————

st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#616E88;'>\
    Développé avec ❤ par un Data Scientist — PySpark & Streamlit</p>",
    unsafe_allow_html=True
)

# CONCLUSION STORYTELLING
st.markdown("""
---
<div style='text-align:center; font-size:1.1rem; margin-top:2em;'>
<b>Merci d'avoir exploré le marché de la Data Science avec ce dashboard !</b><br>
N'hésitez pas à jouer avec les filtres pour révéler de nouveaux insights, et à partager vos découvertes.<br>
<em>La donnée, c'est le pouvoir. 🚀</em>
</div>
""", unsafe_allow_html=True)
