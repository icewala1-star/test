import streamlit as st
import pandas as pd
from pipeline import run_pipeline
from datetime import datetime

st.set_page_config(page_title="Satellite Dashboard", layout="wide")

st.title("🛰️ Satellite Data Engineering Dashboard")

st.write("Live data from CelesTrak")

# ----------------------
# RUN PIPELINE (CACHED)
# ----------------------
@st.cache_data(ttl=60)
def load_data():
    return run_pipeline()


df, ingestion_time = load_data()

# ----------------------
# METRICS
# ----------------------
col1, col2, col3 = st.columns(3)

col1.metric("Total Satellites", len(df))
col2.metric("Ingestion Time (s)", round(ingestion_time, 3))
col3.metric("Last Updated", datetime.utcnow().strftime("%H:%M:%S"))

# ----------------------
# MAP
# ----------------------
st.subheader("🌍 Satellite Locations")

map_df = df[["lat", "lon"]].dropna()
st.map(map_df)

# ----------------------
# TABLE VIEW
# ----------------------
st.subheader("📊 Satellite Data")

st.dataframe(df[["name", "lat", "lon", "timestamp"]])

# ----------------------
# SELECT SATELLITE
# ----------------------
st.subheader("🔍 Satellite Explorer")

selected = st.selectbox("Choose a satellite", df["name"])

sat_data = df[df["name"] == selected]

st.write(sat_data)

# ----------------------
# RAW DATA TOGGLE
# ----------------------
if st.checkbox("Show raw data"):
    st.dataframe(df)

if df.empty:
    st.error("Failed to fetch satellite data. Try again later.")
    st.stop()
