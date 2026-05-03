import os
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
)

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "province_locations.csv"

URL = "https://api-panelhargav2.badanpangan.go.id/api/provinces"

PROVINCE_NAME_MAP = {
    "D I Yogyakarta": "DI Yogyakarta",
    "Dki Jakarta": "DKI Jakarta",
    "Kep. Bangka Belitung": "Kepulauan Bangka Belitung",
}


def normalize_province(name):
    name = str(name or "").strip()
    name = " ".join(name.split())
    name = name.title()

    if name.upper() == "DKI JAKARTA":
        name = "DKI Jakarta"

    return PROVINCE_NAME_MAP.get(name, name)


def parse_latlong(value):
    if not value:
        return None, None

    parts = str(value).split(",")

    if len(parts) != 2:
        return None, None

    try:
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
        return lat, lon
    except Exception:
        return None, None


def main():
    print(f"Fetching province locations: {URL}")

    response = requests.get(URL, params={"search": ""}, timeout=60)
    response.raise_for_status()

    payload = response.json()
    rows = []

    for item in payload.get("data", []):
        lat, lon = parse_latlong(item.get("latlong"))

        if lat is None or lon is None:
            continue

        rows.append({
            "province_name": normalize_province(item.get("nama")),
            "province_code": str(item.get("id")),
            "map_code": item.get("kode_map"),
            "latitude": lat,
            "longitude": lon,
            "source": "Badan Pangan Panel Harga Province API",
        })

    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("No province location rows extracted.")

    df = df.drop_duplicates(subset=["province_name"], keep="last")
    df = df.sort_values("province_name")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    engine = create_engine(DATABASE_URL)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_province_location
                    (
                        province_name,
                        province_code,
                        map_code,
                        latitude,
                        longitude,
                        source
                    )
                    VALUES
                    (
                        :province_name,
                        :province_code,
                        :map_code,
                        :latitude,
                        :longitude,
                        :source
                    )
                    ON CONFLICT (province_name)
                    DO UPDATE SET
                        province_code = EXCLUDED.province_code,
                        map_code = EXCLUDED.map_code,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        source = EXCLUDED.source
                """),
                row.to_dict()
            )

    print(f"Saved province locations to: {OUTPUT_PATH}")
    print(f"Loaded {len(df)} province locations into PostgreSQL.")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()