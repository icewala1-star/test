import streamlit as st
from pipeline import run_pipeline
from datetime import datetime, UTC
import plotly.graph_objects as go

# ----------------------
# PAGE CONFIG
# ----------------------
st.set_page_config(page_title="Satellite Dashboard", layout="wide")

st.title("🛰️ Satellite Data Engineering Dashboard")

st.markdown("""
This app demonstrates a mini data pipeline:

- Ingestion (CelesTrak)
- Processing (TLE parsing)
- Transformation (position computation)
- Visualization (Streamlit + 3D Globe)
""")

# ----------------------
# CACHE (2 HOURS)
# ----------------------
@st.cache_data(ttl=7200)
def load_data():
    return run_pipeline()

df, ingestion_time = load_data()

# ----------------------
# VALIDATION
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
# 3D GLOBE VISUALIZATION
# ----------------------
st.subheader("🌍 3D Satellite Globe")

plot_df = df.dropna(subset=["lat", "lon"])

if not plot_df.empty:

    fig = go.Figure()

    fig.add_trace(go.Scattergeo(
        lon=plot_df["lon"],
        lat=plot_df["lat"],
        text=plot_df["name"],
        mode="markers",
        marker=dict(
            size=8,
            color="cyan",
            line=dict(width=1)
        ),
        hovertemplate=
        "🛰️ <b>%{text}</b><br>" +
        "Lat: %{lat:.2f}<br>" +
        "Lon: %{lon:.2f}<br>" +
        "<extra></extra>"
    ))

    fig.update_layout(
        geo=dict(
            projection_type="orthographic",  # 3D globe
            showland=True,
            landcolor="rgb(20, 20, 20)",
            showocean=True,
            oceancolor="rgb(10, 10, 30)",
            showcountries=True,
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        height=600
    )

    st.plotly_chart(fig, use_container_width=True)

else:
    st.warning("No satellite positions available")

# ----------------------
# TABLE VIEW
# ----------------------
st.subheader("📊 Satellite Data")

st.dataframe(df[["name", "lat", "lon", "timestamp"]])

# ----------------------
# SATELLITE EXPLORER
# ----------------------
st.subheader("🔍 Satellite Explorer")

selected = st.selectbox("Choose a satellite", df["name"])

sat_data = df[df["name"] == selected]

st.write(sat_data)

# ----------------------
# DEBUG PANEL
# ----------------------
with st.expander("⚙️ Debug Data"):
    st.write("Columns:", df.columns.tolist())
    st.write("Shape:", df.shape)
    st.write(df.head())
