import requests
import pandas as pd
from datetime import datetime, UTC
from sgp4.api import Satrec, jday

URL = "https://celestrak.org/NORAD/elements/stations.txt"


# ----------------------
# INGESTION
# ----------------------
def fetch_tle():
    try:
        start_time = datetime.now(UTC)
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.text
        end_time = datetime.now(UTC)

        return data, (end_time - start_time).total_seconds()

    except Exception as e:
        print("Fetch error:", e)
        return None, 0


# ----------------------
# PROCESSING
# ----------------------
def parse_tle(tle_text):
    import pandas as pd

    columns = ["name", "line1", "line2"]

    if not tle_text:
        return pd.DataFrame(columns=columns)

    # Clean lines
    lines = [l.strip() for l in tle_text.split("\n") if l.strip()]

    records = []

    i = 0
    while i < len(lines) - 2:
        name = lines[i]
        l1 = lines[i + 1]
        l2 = lines[i + 2]

        # ✅ Strict validation
        if l1.startswith("1 ") and l2.startswith("2 "):
            records.append({
                "name": name,
                "line1": l1,
                "line2": l2
            })
            i += 3
        else:
            # Skip bad alignment
            i += 1

    return pd.DataFrame(records, columns=columns)


# ----------------------
# POSITION COMPUTATION
# ----------------------
def compute_position(row):
    try:
        sat = Satrec.twoline2rv(row["line1"], row["line2"])

        now = datetime.now(UTC)
        jd, fr = jday(
            now.year, now.month, now.day,
            now.hour, now.minute, now.second
        )

        e, r, v = sat.sgp4(jd, fr)

        return r  # x, y, z
    except:
        return (None, None, None)


def add_positions(df):
    if df.empty:
        return df

    positions = df.apply(compute_position, axis=1)

    df["x"] = [p[0] for p in positions]
    df["y"] = [p[1] for p in positions]
    df["z"] = [p[2] for p in positions]

    return df


# ----------------------
# LAT/LON (SIMPLIFIED)
# ----------------------
def add_lat_lon(df):
    if df.empty:
        df["lat"] = []
        df["lon"] = []
        return df

    df["lat"] = df["y"] % 180 - 90
    df["lon"] = df["x"] % 360 - 180

    return df


# ----------------------
# FULL PIPELINE
# ----------------------
def run_pipeline():
    raw_data, ingestion_time = fetch_tle()

    # Always return structured dataframe
    columns = [
        "name", "line1", "line2",
        "x", "y", "z",
        "lat", "lon", "timestamp"
    ]

    if not raw_data:
        return pd.DataFrame(columns=columns), ingestion_time

    df = parse_tle(raw_data)

    if df.empty:
        return pd.DataFrame(columns=columns), ingestion_time

    df = add_positions(df)
    df = add_lat_lon(df)

    df["timestamp"] = datetime.now(UTC)

    return df, ingestion_time
