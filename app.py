import streamlit as st
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pandas as pd
import altair as alt
import os

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

# On injecte un peu de CSS pour un thème sombre
st.markdown(
    """
    <style>
      /* Fond global */
      .stApp {
        background-color: #0E1117;
        color: #ECEFF4;
      }
      /* Conteneur principal */
      .css-18e3th9 { 
        background-color: #1E222A; 
        padding: 1rem 2rem;
        border-radius: 0.5rem;
      }
      /* Sidebar */
      .css-1d391kg, .css-1v3fvcr {
        background-color: #1E222A;
        color: #ECEFF4;
      }
      /* Titre */
      .css-2trqyj h1, .css-2trqyj h2 {
        color: #88C0D0;
      }
      /* Textes */
      .css-1hynsf2 {
        color: #D8DEE9;
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
# 3) SIDEBAR & FILTRES
# —————————————————————————————————————————————————————————————————————————————————

st.sidebar.header("📋 Filtres")

years = df.select("work_year").distinct().orderBy("work_year") \
          .rdd.flatMap(lambda x: x).collect()
selected_years = st.sidebar.multiselect("Années", years, default=years)

countries = df.select("country").distinct().orderBy("country") \
              .rdd.flatMap(lambda x: x).collect()
selected_countries = st.sidebar.multiselect("Pays", countries, default=countries)

jobs = df.select("job_title").distinct().orderBy("job_title") \
         .rdd.flatMap(lambda x: x).collect()
selected_jobs = st.sidebar.multiselect(
    "Poste", jobs, default=jobs[:5]
)

# —————————————————————————————————————————————————————————————————————————————————
# 4) FILTRAGE & CONVERSION
# —————————————————————————————————————————————————————————————————————————————————

filtered = df.filter(
    col("work_year").isin(selected_years) &
    col("country").isin(selected_countries) &
    col("job_title").isin(selected_jobs)
)

def to_pandas(_sdf):
    return _sdf.toPandas()

pdf = to_pandas(filtered)

# —————————————————————————————————————————————————————————————————————————————————
# 5) VISUALISATIONS
# —————————————————————————————————————————————————————————————————————————————————

st.title("🚀 Data Science Salary Dashboard")

# 5.1 Salaire moyen par année
avg_by_year = pdf.groupby("work_year")["salary"].mean().reset_index()
chart1 = (
    alt.Chart(avg_by_year)
       .mark_line(point=True, strokeWidth=3)
       .encode(
           x=alt.X("work_year:O", title="Année"),
           y=alt.Y("salary:Q", title="Salaire moyen"),
           tooltip=["work_year", alt.Tooltip("salary", format=",.2f")]
       )
       .properties(width=600, height=350, title="Salaire moyen par année")
)
st.altair_chart(chart1, use_container_width=True)

# 5.2 Distribution des salaires
chart2 = (
    alt.Chart(pdf)
       .mark_bar()
       .encode(
           alt.X("salary:Q", bin=alt.Bin(maxbins=40), title="Salaire"),
           alt.Y("count()", title="Nombre"),
           tooltip=[alt.Tooltip("count()", title="Nombre")]
       )
       .properties(width=600, height=350, title="Distribution des salaires")
)
st.altair_chart(chart2, use_container_width=True)

# 5.3 Salaire moyen par pays
avg_by_country = (pdf.groupby("country")["salary"]
                    .mean().reset_index()
                    .sort_values("salary", ascending=False))
chart3 = (
    alt.Chart(avg_by_country)
       .mark_bar()
       .encode(
           x=alt.X("salary:Q", title="Salaire moyen"),
           y=alt.Y("country:N", sort="-x", title="Pays"),
           tooltip=["country", alt.Tooltip("salary", format=",.2f")]
       )
       .properties(width=600, height=700, title="Salaire moyen par pays")
)
st.altair_chart(chart3, use_container_width=True)

# —————————————————————————————————————————————————————————————————————————————————
# 6) FOOTER
# —————————————————————————————————————————————————————————————————————————————————

st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#616E88;'>\
    Développé avec ❤ par un Data Scientist — PySpark & Streamlit</p>",
    unsafe_allow_html=True
)
