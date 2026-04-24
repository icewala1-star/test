import requests
import pandas as pd
import time
from datetime import datetime, UTC
from skyfield.api import EarthSatellite, load, wgs84

# ----------------------
# CONFIG
# ----------------------
URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=tle"

ts = load.timescale()

# ----------------------
# FALLBACK DATA (for reliability)
# ----------------------
FALLBACK_TLE = """ISS (ZARYA)
1 25544U 98067A   26113.61927549  .00008948  00000+0  17083-3 0  9996
2 25544  51.6320 210.1816 0006827 342.1760  17.8988 15.48912820563290
POISK
1 36086U 09060A   26113.61927549  .00008948  00000+0  17083-3 0  9994
2 36086  51.6320 210.1816 0006827 342.1760  17.8988 15.48912820563080
"""

# ----------------------
# INGESTION (with retry)
# ----------------------
def fetch_tle():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/plain"
    }

    for attempt in range(3):
        try:
            start_time = datetime.now(UTC)

            response = requests.get(URL, headers=headers, timeout=15)

            print(f"Attempt {attempt+1}")
            print("Status Code:", response.status_code)
            print("Content Length:", len(response.text))

            if response.status_code == 200 and len(response.text.strip()) > 100:
                end_time = datetime.now(UTC)
                return response.text, (end_time - start_time).total_seconds()

        except Exception as e:
            print(f"Attempt {attempt+1} failed:", e)

        time.sleep(2)

    print("❌ All retries failed, using fallback data")
    return FALLBACK_TLE, 0


# ----------------------
# PROCESSING (TLE parsing)
# ----------------------
def parse_tle(tle_text):
    records = []
    lines = tle_text.split("\n")

    i = 0
    while i < len(lines):
        try:
            name = lines[i].strip()
            l1 = lines[i + 1].strip()
            l2 = lines[i + 2].strip()

            if name and l1.startswith("1 ") and l2.startswith("2 "):
                records.append({
                    "name": name,
                    "line1": l1,
                    "line2": l2
                })
                i += 3
            else:
                i += 1

        except:
            break

    df = pd.DataFrame(records)

    print("Parsed rows:", len(df))
    return df


# ----------------------
# POSITION COMPUTATION (accurate)
# ----------------------
def compute_position(row):
    try:
        satellite = EarthSatellite(
            row["line1"],
            row["line2"],
            row["name"],
            ts
        )

        t = ts.now()
        geocentric = satellite.at(t)
        subpoint = wgs84.subpoint(geocentric)

        return (
            subpoint.latitude.degrees,
            subpoint.longitude.degrees,
            subpoint.elevation.km
        )

    except Exception as e:
        print("Position error:", e)
        return (None, None, None)


def add_positions(df):
    if df.empty:
        return df

    positions = df.apply(compute_position, axis=1)

    df["lat"] = [p[0] for p in positions]
    df["lon"] = [p[1] for p in positions]
    df["alt_km"] = [p[2] for p in positions]

    print("Positions computed:", len(df))
    return df


# ----------------------
# FULL PIPELINE
# ----------------------
def run_pipeline():
    raw_data, ingestion_time = fetch_tle()

    columns = [
        "name", "line1", "line2",
        "lat", "lon", "alt_km",
        "timestamp"
    ]

    if not raw_data:
        return pd.DataFrame(columns=columns), ingestion_time

    df = parse_tle(raw_data)

    if df.empty:
        return pd.DataFrame(columns=columns), ingestion_time

    df = add_positions(df)

    # Drop invalid rows
    df = df.dropna(subset=["lat", "lon"])

    # Add timestamp
    df["timestamp"] = datetime.now(UTC)

    return df, ingestion_time