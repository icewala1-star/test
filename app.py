import streamlit as st
from pipeline import run_pipeline
from datetime import datetime, UTC
import plotly.graph_objects as go

# PAGE CONFIG
# ----------------------
st.set_page_config(page_title="Satellite Dashboard", layout="wide")

st.title("🛰️ Satellite Dashboard")

st.markdown("""
This app demonstrates a mini data pipeline:

- Ingestion (CelesTrak)
- Processing (TLE parsing)
- Transformation (position computation)
- Visualization (Streamlit)
""")

# CACHE (2 HOURS)
# ----------------------
@st.cache_data(ttl=7200)
def load_data():
    return run_pipeline()

df, ingestion_time = load_data()

# VALIDATION
# ----------------------
required_cols = ["name", "lat", "lon", "timestamp"]

if df.empty or not all(col in df.columns for col in required_cols):
    st.error("⚠️ Failed to load satellite data.")

    with st.expander("🔍 Debug Info"):
        st.write("Columns:", df.columns.tolist())
        st.write("Shape:", df.shape)

    st.stop()

# METRICS
# ----------------------
col1, col2, col3 = st.columns(3)

col1.metric("Total Satellites", len(df))
col2.metric("Ingestion Time (s)", round(ingestion_time, 3))
col3.metric("Last Updated", datetime.now(UTC).strftime("%H:%M:%S"))

# REFRESH BUTTON
# ----------------------
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# 3D GLOBE VISUALIZATION
# ----------------------
st.subheader("🌍 3D Satellite Globe")

plot_df = df.dropna(subset=["lat", "lon"])

if not plot_df.empty:

    fig = go.Figure()

    # GLOW LAYER (halo effect)
    # ----------------------
    fig.add_trace(go.Scattergeo(
        lon=plot_df["lon"],
        lat=plot_df["lat"],
        mode="markers",
        marker=dict(
            size=18,
            color="rgba(0, 150, 255, 0.25)",
        ),
        hoverinfo="skip",
        showlegend=False
    ))

    # MAIN SATELLITE POINTS
    # ----------------------
    fig.add_trace(go.Scattergeo(
        lon=plot_df["lon"],
        lat=plot_df["lat"],
        text=plot_df["name"],
        mode="markers",

        marker=dict(
            size=8,
            color="red",
            line=dict(width=1, color="black")
        ),

        hovertemplate=
        "🛰️ <b>%{text}</b><br>" +
        "Lat: %{lat:.2f}<br>" +
        "Lon: %{lon:.2f}<br>" +
        "<extra></extra>"
    ))

    # MAP STYLE (clean + transparent)
    # ----------------------
    fig.update_layout(
    geo=dict(
        projection_type="orthographic",

        # 🔥 KEY FIX
        domain=dict(x=[0.05, 0.95], y=[0.05, 0.95]),

        showland=True,
        landcolor="rgb(240, 240, 240)",

        showocean=True,
        oceancolor="rgb(200, 220, 255)",

        showcountries=True,
        showcoastlines=True,
        coastlinecolor="gray",

        bgcolor="rgba(0,0,0,0)"
    ),

    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",

    margin=dict(l=0, r=0, t=0, b=0),
    height=850
)

    st.plotly_chart(fig, width="stretch")
else:
    st.warning("No satellite positions available")

# TABLE VIEW
# ----------------------
st.subheader("📊 Satellite Data")

st.dataframe(df[["name", "lat", "lon", "timestamp"]])

# SATELLITE EXPLORER
# ----------------------
st.subheader("🔍 Satellite Explorer")

selected = st.selectbox("Choose a satellite", df["name"])

sat_data = df[df["name"] == selected]

st.write(sat_data)

# DEBUG PANEL
# ----------------------
with st.expander("⚙️ Debug Data"):
    st.write("Columns:", df.columns.tolist())
    st.write("Shape:", df.shape)
    st.write(df.head())
