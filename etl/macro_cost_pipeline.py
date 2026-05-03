import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
)

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
EXCHANGE_CSV_PATH = DATA_DIR / "exchange_rate_usd_idr.csv"
FUEL_CSV_PATH = DATA_DIR / "fuel_price_region.csv"


def get_engine():
    return create_engine(DATABASE_URL)


def none_if_nan(value):
    if pd.isna(value):
        return None
    return value


def load_exchange_rates(conn):
    if not EXCHANGE_CSV_PATH.exists():
        print(f"Skipping missing exchange CSV: {EXCHANGE_CSV_PATH}")
        return

    df = pd.read_csv(EXCHANGE_CSV_PATH)

    if df.empty:
        print("Exchange CSV empty.")
        return

    df["rate_date"] = pd.to_datetime(df["rate_date"], errors="coerce").dt.date
    df["rate_mid"] = pd.to_numeric(df["rate_mid"], errors="coerce")

    df = df.dropna(subset=["rate_date", "currency_pair", "rate_mid", "source"])

    for _, row in df.iterrows():
        conn.execute(
            text("""
                INSERT INTO fact_exchange_rate_usd_idr
                (
                    rate_date,
                    currency_pair,
                    rate_mid,
                    source
                )
                VALUES
                (
                    :rate_date,
                    :currency_pair,
                    :rate_mid,
                    :source
                )
                ON CONFLICT (rate_date, currency_pair, source)
                DO UPDATE SET
                    rate_mid = EXCLUDED.rate_mid,
                    scraped_at = CURRENT_TIMESTAMP
            """),
            {
                "rate_date": row["rate_date"],
                "currency_pair": row["currency_pair"],
                "rate_mid": row["rate_mid"],
                "source": row["source"],
            }
        )

    print(f"Loaded {len(df)} exchange-rate rows.")


def load_fuel_prices(conn):
    if not FUEL_CSV_PATH.exists():
        print(f"Skipping missing fuel CSV: {FUEL_CSV_PATH}")
        return

    df = pd.read_csv(FUEL_CSV_PATH)

    if df.empty:
        print("Fuel CSV empty.")
        return

    df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce").dt.date
    df["price_per_liter"] = pd.to_numeric(df["price_per_liter"], errors="coerce")

    df = df.dropna(
        subset=[
            "effective_date",
            "province_name",
            "product_name",
            "price_per_liter",
            "source",
        ]
    )

    for _, row in df.iterrows():
        conn.execute(
            text("""
                INSERT INTO fact_fuel_price_region
                (
                    effective_date,
                    province_name,
                    product_name,
                    price_per_liter,
                    source
                )
                VALUES
                (
                    :effective_date,
                    :province_name,
                    :product_name,
                    :price_per_liter,
                    :source
                )
                ON CONFLICT (effective_date, province_name, product_name, source)
                DO UPDATE SET
                    price_per_liter = EXCLUDED.price_per_liter,
                    scraped_at = CURRENT_TIMESTAMP
            """),
            {
                "effective_date": row["effective_date"],
                "province_name": row["province_name"],
                "product_name": row["product_name"],
                "price_per_liter": row["price_per_liter"],
                "source": row["source"],
            }
        )

    print(f"Loaded {len(df)} fuel-price rows.")


def main():
    engine = get_engine()

    with engine.begin() as conn:
        load_exchange_rates(conn)
        load_fuel_prices(conn)

    print("Macro-cost context loaded to PostgreSQL.")


if __name__ == "__main__":
    main()