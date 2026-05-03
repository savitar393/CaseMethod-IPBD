import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
)

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
INFLATION_CSV_PATH = DATA_DIR / "inflation_region.csv"


def get_engine():
    return create_engine(DATABASE_URL)


def load_inflation_to_postgres():
    if not INFLATION_CSV_PATH.exists():
        raise FileNotFoundError(f"Inflation CSV not found: {INFLATION_CSV_PATH}")

    df = pd.read_csv(INFLATION_CSV_PATH)

    if df.empty:
        print("No inflation rows to load.")
        return

    required_cols = [
        "period_month",
        "province_name",
        "inflation_mtm",
        "inflation_yoy",
        "cpi",
        "source",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    df["period_month"] = pd.to_datetime(df["period_month"], errors="coerce")
    df["province_name"] = df["province_name"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip()

    for col in ["inflation_mtm", "inflation_yoy", "cpi"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["period_month", "province_name", "source"])

    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO fact_inflation_region
                    (
                        period_month,
                        province_name,
                        inflation_mtm,
                        inflation_yoy,
                        cpi,
                        source
                    )
                    VALUES
                    (
                        :period_month,
                        :province_name,
                        :inflation_mtm,
                        :inflation_yoy,
                        :cpi,
                        :source
                    )
                    ON CONFLICT (period_month, province_name)
                    DO UPDATE SET
                        inflation_mtm = EXCLUDED.inflation_mtm,
                        inflation_yoy = EXCLUDED.inflation_yoy,
                        cpi = EXCLUDED.cpi
                """),
                {
                    "period_month": row["period_month"].date(),
                    "province_name": row["province_name"],
                    "inflation_mtm": None if pd.isna(row["inflation_mtm"]) else row["inflation_mtm"],
                    "inflation_yoy": None if pd.isna(row["inflation_yoy"]) else row["inflation_yoy"],
                    "cpi": None if pd.isna(row["cpi"]) else row["cpi"],
                    "source": row["source"],
                }
            )

    print(f"Loaded {len(df)} inflation rows into PostgreSQL.")


if __name__ == "__main__":
    load_inflation_to_postgres()