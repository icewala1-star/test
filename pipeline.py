import requests
import pandas as pd
from datetime import datetime
from sgp4.api import Satrec, jday

URL = "https://celestrak.org/NORAD/elements/stations.txt"


# ----------------------
# INGESTION
# ----------------------
def fetch_tle():
    try:
        start_time = datetime.utcnow()
        response = requests.get(URL, timeout=10)
        data = response.text
        end_time = datetime.utcnow()

        return data, (end_time - start_time).total_seconds()
    except:
        return "", 0


# ----------------------
# PROCESSING
# ----------------------
def parse_tle(tle_text):
    lines = tle_text.strip().split("\n")
    records = []

    for i in range(0, len(lines), 3):
        try:
            name = lines[i].strip()
            l1 = lines[i+1]
            l2 = lines[i+2]

            records.append({
                "name": name,
                "line1": l1,
                "line2": l2
            })
        except:
            continue

    return pd.DataFrame(records)


# ----------------------
# TRANSFORMATION (POSITION)
# ----------------------
def compute_position(row):
    try:
        sat = Satrec.twoline2rv(row["line1"], row["line2"])

        now = datetime.utcnow()
        jd, fr = jday(
            now.year, now.month, now.day,
            now.hour, now.minute, now.second
        )

        e, r, v = sat.sgp4(jd, fr)

        return r  # x, y, z coordinates
    except:
        return (None, None, None)


def add_positions(df):
    positions = df.apply(compute_position, axis=1)
    df["x"] = [p[0] for p in positions]
    df["y"] = [p[1] for p in positions]
    df["z"] = [p[2] for p in positions]

    return df


# ----------------------
# SIMPLE GEO CONVERSION (APPROXIMATION)
# ----------------------
def add_lat_lon(df):
    # NOTE: This is a simplified conversion for demo purposes
    df["lat"] = df["y"] % 180 - 90
    df["lon"] = df["x"] % 360 - 180
    return df


# ----------------------
# FULL PIPELINE
# ----------------------
def run_pipeline():
    raw_data, ingestion_time = fetch_tle()

    df = parse_tle(raw_data)
    df = add_positions(df)
    df = add_lat_lon(df)

    df["timestamp"] = datetime.utcnow()

    return df, ingestion_time
