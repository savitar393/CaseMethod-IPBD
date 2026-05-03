import os
import time
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
)

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "climate_region_daily.csv"

DEBUG_DIR = DATA_DIR / "source_pages" / "climate"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Match your price backfill window. You can increase this later.
DAYS_BACK = int(os.getenv("CLIMATE_DAYS_BACK", "120"))

PARAMETERS = [
    "PRECTOTCORR",  # precipitation, mm/day
    "T2M",          # temperature average, C
    "T2M_MAX",      # max temperature, C
    "T2M_MIN",      # min temperature, C
    "RH2M",         # relative humidity, %
    "WS10M",        # wind speed at 10m, m/s
]


def get_engine():
    return create_engine(DATABASE_URL)


def to_yyyymmdd(d):
    return d.strftime("%Y%m%d")


def to_float(value):
    try:
        if value is None:
            return None

        # NASA missing values sometimes appear as -999 or similar.
        value = float(value)

        if value <= -900:
            return None

        return value
    except Exception:
        return None


def calculate_flags(row):
    precipitation = row.get("precipitation_mm")
    tmax = row.get("temperature_max_c")

    dry_day_flag = 0
    heavy_rain_flag = 0
    heat_stress_flag = 0

    if precipitation is not None and precipitation < 1:
        dry_day_flag = 1

    if precipitation is not None and precipitation >= 50:
        heavy_rain_flag = 1

    if tmax is not None and tmax >= 34:
        heat_stress_flag = 1

    score = dry_day_flag + (heavy_rain_flag * 2) + heat_stress_flag

    if score >= 3:
        label = "High"
    elif score >= 1:
        label = "Medium"
    else:
        label = "Low"

    return dry_day_flag, heavy_rain_flag, heat_stress_flag, score, label


def fetch_province_locations():
    engine = get_engine()

    query = """
        SELECT
            province_name,
            latitude,
            longitude
        FROM dim_province_location
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY province_name;
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        raise RuntimeError(
            "No province coordinates found. Run scrape_province_locations.py first."
        )

    return df


def fetch_nasa_power_for_location(province_name, latitude, longitude, start_date, end_date):
    params = {
        "parameters": ",".join(PARAMETERS),
        "community": "AG",
        "longitude": longitude,
        "latitude": latitude,
        "start": to_yyyymmdd(start_date),
        "end": to_yyyymmdd(end_date),
        "format": "JSON",
    }

    print(f"Fetching NASA POWER climate: {province_name} ({latitude}, {longitude})")

    response = requests.get(
        NASA_POWER_URL,
        params=params,
        timeout=90,
    )

    response.raise_for_status()

    safe_name = province_name.lower().replace(" ", "_").replace("/", "_")
    debug_path = DEBUG_DIR / f"nasa_power_{safe_name}.json"
    debug_path.write_text(response.text, encoding="utf-8", errors="ignore")

    return response.json()


def parse_nasa_power_payload(payload, province_name, latitude, longitude):
    parameters = payload.get("properties", {}).get("parameter", {})

    precip = parameters.get("PRECTOTCORR", {})
    tavg = parameters.get("T2M", {})
    tmax = parameters.get("T2M_MAX", {})
    tmin = parameters.get("T2M_MIN", {})
    rh = parameters.get("RH2M", {})
    ws = parameters.get("WS10M", {})

    all_dates = sorted(set(precip.keys()) | set(tavg.keys()) | set(tmax.keys()))

    rows = []

    for date_key in all_dates:
        try:
            climate_date = pd.to_datetime(date_key, format="%Y%m%d").date()
        except Exception:
            continue

        row = {
            "climate_date": climate_date,
            "province_name": province_name,
            "latitude": latitude,
            "longitude": longitude,
            "precipitation_mm": to_float(precip.get(date_key)),
            "temperature_avg_c": to_float(tavg.get(date_key)),
            "temperature_max_c": to_float(tmax.get(date_key)),
            "temperature_min_c": to_float(tmin.get(date_key)),
            "relative_humidity_pct": to_float(rh.get(date_key)),
            "wind_speed_mps": to_float(ws.get(date_key)),
            "source": "NASA POWER Daily API",
        }

        (
            dry_day_flag,
            heavy_rain_flag,
            heat_stress_flag,
            climate_risk_score,
            climate_risk_label,
        ) = calculate_flags(row)

        row["dry_day_flag"] = dry_day_flag
        row["heavy_rain_flag"] = heavy_rain_flag
        row["heat_stress_flag"] = heat_stress_flag
        row["climate_risk_score"] = climate_risk_score
        row["climate_risk_label"] = climate_risk_label

        rows.append(row)

    return rows


def main():
    end_date = date.today()
    start_date = end_date - timedelta(days=DAYS_BACK)

    locations = fetch_province_locations()

    all_rows = []

    for _, loc in locations.iterrows():
        province_name = loc["province_name"]
        latitude = float(loc["latitude"])
        longitude = float(loc["longitude"])

        try:
            payload = fetch_nasa_power_for_location(
                province_name=province_name,
                latitude=latitude,
                longitude=longitude,
                start_date=start_date,
                end_date=end_date,
            )

            rows = parse_nasa_power_payload(
                payload=payload,
                province_name=province_name,
                latitude=latitude,
                longitude=longitude,
            )

            print(f"Rows extracted for {province_name}: {len(rows)}")
            all_rows.extend(rows)

            time.sleep(0.5)

        except Exception as e:
            print(f"Failed {province_name}: {e}")
            time.sleep(1)

    df = pd.DataFrame(all_rows)

    if df.empty:
        raise RuntimeError("No NASA POWER climate rows extracted.")

    df = df.drop_duplicates(
        subset=["climate_date", "province_name", "source"],
        keep="last",
    )

    df = df.sort_values(["climate_date", "province_name"])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nSaved climate data to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print("Province count:", df["province_name"].nunique())
    print("Date range:", df["climate_date"].min(), "to", df["climate_date"].max())
    print("\nRisk summary:")
    print(df["climate_risk_label"].value_counts())
    print("\nSample:")
    print(df.head(30).to_string(index=False))


if __name__ == "__main__":
    main()