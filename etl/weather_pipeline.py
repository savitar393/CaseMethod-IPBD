import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
)

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
WEATHER_CSV_PATH = DATA_DIR / "weather_region.csv"


def get_engine():
    return create_engine(DATABASE_URL)


def none_if_nan(value):
    if pd.isna(value):
        return None
    return value


def load_weather_to_postgres():
    if not WEATHER_CSV_PATH.exists():
        raise FileNotFoundError(f"Weather CSV not found: {WEATHER_CSV_PATH}")

    df = pd.read_csv(WEATHER_CSV_PATH)

    if df.empty:
        print("No weather rows to load.")
        return

    required_cols = [
        "weather_datetime_utc",
        "forecast_date",
        "province_name",
        "weather_code",
        "weather_desc",
        "temperature_min",
        "temperature_max",
        "temperature_avg",
        "humidity_min",
        "humidity_max",
        "humidity_avg",
        "wind_direction",
        "wind_speed",
        "weather_risk_label",
        "weather_risk_score",
        "source",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    df["weather_datetime_utc"] = pd.to_datetime(df["weather_datetime_utc"], errors="coerce")
    df["forecast_date"] = pd.to_datetime(df["forecast_date"], errors="coerce").dt.date

    df = df.dropna(subset=["weather_datetime_utc", "forecast_date", "province_name", "source"])

    numeric_cols = [
        "temperature_min",
        "temperature_max",
        "temperature_avg",
        "humidity_min",
        "humidity_max",
        "humidity_avg",
        "wind_speed",
        "weather_risk_score",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO fact_weather_region
                    (
                        weather_datetime_utc,
                        forecast_date,
                        province_name,
                        weather_code,
                        weather_desc,
                        temperature_min,
                        temperature_max,
                        temperature_avg,
                        humidity_min,
                        humidity_max,
                        humidity_avg,
                        wind_direction,
                        wind_speed,
                        weather_risk_label,
                        weather_risk_score,
                        source
                    )
                    VALUES
                    (
                        :weather_datetime_utc,
                        :forecast_date,
                        :province_name,
                        :weather_code,
                        :weather_desc,
                        :temperature_min,
                        :temperature_max,
                        :temperature_avg,
                        :humidity_min,
                        :humidity_max,
                        :humidity_avg,
                        :wind_direction,
                        :wind_speed,
                        :weather_risk_label,
                        :weather_risk_score,
                        :source
                    )
                    ON CONFLICT (weather_datetime_utc, province_name, weather_code, source)
                    DO UPDATE SET
                        weather_desc = EXCLUDED.weather_desc,
                        temperature_min = EXCLUDED.temperature_min,
                        temperature_max = EXCLUDED.temperature_max,
                        temperature_avg = EXCLUDED.temperature_avg,
                        humidity_min = EXCLUDED.humidity_min,
                        humidity_max = EXCLUDED.humidity_max,
                        humidity_avg = EXCLUDED.humidity_avg,
                        wind_direction = EXCLUDED.wind_direction,
                        wind_speed = EXCLUDED.wind_speed,
                        weather_risk_label = EXCLUDED.weather_risk_label,
                        weather_risk_score = EXCLUDED.weather_risk_score,
                        scraped_at = CURRENT_TIMESTAMP
                """),
                {
                    "weather_datetime_utc": row["weather_datetime_utc"].to_pydatetime(),
                    "forecast_date": row["forecast_date"],
                    "province_name": row["province_name"],
                    "weather_code": none_if_nan(row["weather_code"]),
                    "weather_desc": none_if_nan(row["weather_desc"]),
                    "temperature_min": none_if_nan(row["temperature_min"]),
                    "temperature_max": none_if_nan(row["temperature_max"]),
                    "temperature_avg": none_if_nan(row["temperature_avg"]),
                    "humidity_min": none_if_nan(row["humidity_min"]),
                    "humidity_max": none_if_nan(row["humidity_max"]),
                    "humidity_avg": none_if_nan(row["humidity_avg"]),
                    "wind_direction": none_if_nan(row["wind_direction"]),
                    "wind_speed": none_if_nan(row["wind_speed"]),
                    "weather_risk_label": none_if_nan(row["weather_risk_label"]),
                    "weather_risk_score": none_if_nan(row["weather_risk_score"]),
                    "source": row["source"],
                }
            )

    print(f"Loaded {len(df)} weather rows into PostgreSQL.")


if __name__ == "__main__":
    load_weather_to_postgres()