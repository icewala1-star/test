import streamlit as st
from pipeline import run_pipeline
from datetime import datetime, UTC

st.set_page_config(page_title="Satellite Dashboard", layout="wide")

st.title("🛰️ Satellite Data Engineering Dashboard")

st.markdown("""
This app demonstrates a mini data pipeline:
- Ingestion (CelesTrak)
- Processing (TLE parsing)
- Transformation (position computation)
- Visualization (Streamlit)
""")

# ----------------------
# CACHE (2 HOURS)
# ----------------------
@st.cache_data(ttl=7200)
def load_data():
    return run_pipeline()


df, ingestion_time = load_data()

# ----------------------
# VALIDATION (PREVENT CRASH)
# ----------------------
required_cols = ["name", "lat", "lon", "timestamp"]

if df.empty or not all(col in df.columns for col in required_cols):
    st.error("⚠️ Failed to load satellite data.")
    
    with st.expander("🔍 Debug Info"):
        st.write("Columns:", df.columns.tolist())
        st.write("Shape:", df.shape)

    st.stop()

# ----------------------
# METRICS
# ----------------------
col1, col2, col3 = st.columns(3)

col1.metric("Total Satellites", len(df))
col2.metric("Ingestion Time (s)", round(ingestion_time, 3))
col3.metric("Last Updated", datetime.now(UTC).strftime("%H:%M:%S"))

# ----------------------
# REFRESH BUTTON
# ----------------------
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ----------------------
# MAP
# ----------------------
st.subheader("🌍 Satellite Locations")

map_df = df[["lat", "lon"]].dropna()

if not map_df.empty:
    st.map(map_df)
else:
    st.warning("No valid location data available.")

# ----------------------
# TABLE
# ----------------------
st.subheader("📊 Satellite Data")

st.dataframe(df[["name", "lat", "lon", "timestamp"]])

# ----------------------
# SATELLITE SELECTOR
# ----------------------
st.subheader("🔍 Satellite Explorer")

selected = st.selectbox("Choose a satellite", df["name"])

sat_data = df[df["name"] == selected]

st.write(sat_data)

# ----------------------
# DEBUG (OPTIONAL)
# ----------------------
with st.expander("⚙️ Debug Data"):
    st.write("Columns:", df.columns.tolist())
    st.write("Shape:", df.shape)
