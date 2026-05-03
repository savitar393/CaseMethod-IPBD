import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
)

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
CLIMATE_CSV_PATH = DATA_DIR / "climate_region_daily.csv"


def get_engine():
    return create_engine(DATABASE_URL)


def none_if_nan(value):
    if pd.isna(value):
        return None
    return value


def load_climate_to_postgres():
    if not CLIMATE_CSV_PATH.exists():
        raise FileNotFoundError(f"Climate CSV not found: {CLIMATE_CSV_PATH}")

    df = pd.read_csv(CLIMATE_CSV_PATH)

    if df.empty:
        print("No climate rows to load.")
        return

    df["climate_date"] = pd.to_datetime(df["climate_date"], errors="coerce").dt.date

    numeric_cols = [
        "latitude",
        "longitude",
        "precipitation_mm",
        "temperature_avg_c",
        "temperature_max_c",
        "temperature_min_c",
        "relative_humidity_pct",
        "wind_speed_mps",
        "dry_day_flag",
        "heavy_rain_flag",
        "heat_stress_flag",
        "climate_risk_score",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["climate_date", "province_name", "source"])

    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO fact_climate_region_daily
                    (
                        climate_date,
                        province_name,
                        latitude,
                        longitude,
                        precipitation_mm,
                        temperature_avg_c,
                        temperature_max_c,
                        temperature_min_c,
                        relative_humidity_pct,
                        wind_speed_mps,
                        dry_day_flag,
                        heavy_rain_flag,
                        heat_stress_flag,
                        climate_risk_score,
                        climate_risk_label,
                        source
                    )
                    VALUES
                    (
                        :climate_date,
                        :province_name,
                        :latitude,
                        :longitude,
                        :precipitation_mm,
                        :temperature_avg_c,
                        :temperature_max_c,
                        :temperature_min_c,
                        :relative_humidity_pct,
                        :wind_speed_mps,
                        :dry_day_flag,
                        :heavy_rain_flag,
                        :heat_stress_flag,
                        :climate_risk_score,
                        :climate_risk_label,
                        :source
                    )
                    ON CONFLICT (climate_date, province_name, source)
                    DO UPDATE SET
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        precipitation_mm = EXCLUDED.precipitation_mm,
                        temperature_avg_c = EXCLUDED.temperature_avg_c,
                        temperature_max_c = EXCLUDED.temperature_max_c,
                        temperature_min_c = EXCLUDED.temperature_min_c,
                        relative_humidity_pct = EXCLUDED.relative_humidity_pct,
                        wind_speed_mps = EXCLUDED.wind_speed_mps,
                        dry_day_flag = EXCLUDED.dry_day_flag,
                        heavy_rain_flag = EXCLUDED.heavy_rain_flag,
                        heat_stress_flag = EXCLUDED.heat_stress_flag,
                        climate_risk_score = EXCLUDED.climate_risk_score,
                        climate_risk_label = EXCLUDED.climate_risk_label,
                        scraped_at = CURRENT_TIMESTAMP
                """),
                {
                    "climate_date": row["climate_date"],
                    "province_name": row["province_name"],
                    "latitude": none_if_nan(row["latitude"]),
                    "longitude": none_if_nan(row["longitude"]),
                    "precipitation_mm": none_if_nan(row["precipitation_mm"]),
                    "temperature_avg_c": none_if_nan(row["temperature_avg_c"]),
                    "temperature_max_c": none_if_nan(row["temperature_max_c"]),
                    "temperature_min_c": none_if_nan(row["temperature_min_c"]),
                    "relative_humidity_pct": none_if_nan(row["relative_humidity_pct"]),
                    "wind_speed_mps": none_if_nan(row["wind_speed_mps"]),
                    "dry_day_flag": none_if_nan(row["dry_day_flag"]),
                    "heavy_rain_flag": none_if_nan(row["heavy_rain_flag"]),
                    "heat_stress_flag": none_if_nan(row["heat_stress_flag"]),
                    "climate_risk_score": none_if_nan(row["climate_risk_score"]),
                    "climate_risk_label": none_if_nan(row["climate_risk_label"]),
                    "source": row["source"],
                }
            )

    print(f"Loaded {len(df)} climate rows into PostgreSQL.")


if __name__ == "__main__":
    load_climate_to_postgres()