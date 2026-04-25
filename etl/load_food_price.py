import pandas as pd
from sqlalchemy import create_engine, text

import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5433/panganwatch"
)
CSV_PATH = "data/sample_food_price.csv"

engine = create_engine(DATABASE_URL)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df["price_date"] = pd.to_datetime(df["price_date"]).dt.date
    df["province_name"] = df["province_name"].str.strip()
    df["city_name"] = df["city_name"].str.strip()
    df["commodity_name"] = df["commodity_name"].str.strip()
    df["unit"] = df["unit"].str.strip()
    df["source"] = df["source"].str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df = df.dropna(subset=["price_date", "province_name", "commodity_name", "price"])
    df = df.drop_duplicates()

    return df


def load_dimensions(df: pd.DataFrame):
    commodities = df[["commodity_name", "unit"]].drop_duplicates()
    regions = df[["province_name", "city_name"]].drop_duplicates()

    with engine.begin() as conn:
        for _, row in commodities.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_commodity (commodity_name, unit)
                    VALUES (:commodity_name, :unit)
                    ON CONFLICT (commodity_name) DO UPDATE
                    SET unit = EXCLUDED.unit
                """),
                {
                    "commodity_name": row["commodity_name"],
                    "unit": row["unit"]
                }
            )

        for _, row in regions.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_region (province_name, city_name)
                    VALUES (:province_name, :city_name)
                    ON CONFLICT (province_name, city_name) DO NOTHING
                """),
                {
                    "province_name": row["province_name"],
                    "city_name": row["city_name"]
                }
            )


def load_fact_food_price(df: pd.DataFrame):
    with engine.begin() as conn:
        for _, row in df.iterrows():
            commodity_id = conn.execute(
                text("""
                    SELECT commodity_id
                    FROM dim_commodity
                    WHERE commodity_name = :commodity_name
                """),
                {"commodity_name": row["commodity_name"]}
            ).scalar()

            region_id = conn.execute(
                text("""
                    SELECT region_id
                    FROM dim_region
                    WHERE province_name = :province_name
                    AND city_name = :city_name
                """),
                {
                    "province_name": row["province_name"],
                    "city_name": row["city_name"]
                }
            ).scalar()

            conn.execute(
                text("""
                    INSERT INTO fact_food_price
                    (commodity_id, region_id, price_date, price, source)
                    VALUES
                    (:commodity_id, :region_id, :price_date, :price, :source)
                    ON CONFLICT (commodity_id, region_id, price_date, source)
                    DO UPDATE SET price = EXCLUDED.price
                """),
                {
                    "commodity_id": commodity_id,
                    "region_id": region_id,
                    "price_date": row["price_date"],
                    "price": row["price"],
                    "source": row["source"]
                }
            )


def calculate_alerts():
    query = """
        SELECT
            f.price_date,
            c.commodity_id,
            r.region_id,
            c.commodity_name,
            r.province_name,
            r.city_name,
            f.price
        FROM fact_food_price f
        JOIN dim_commodity c ON f.commodity_id = c.commodity_id
        JOIN dim_region r ON f.region_id = r.region_id
        ORDER BY c.commodity_name, r.province_name, r.city_name, f.price_date
    """

    df = pd.read_sql(query, engine)
    df["previous_price"] = df.groupby(
        ["commodity_id", "region_id"]
    )["price"].shift(1)

    df["percentage_change"] = (
        (df["price"] - df["previous_price"]) / df["previous_price"] * 100
    )

    def assign_status(change):
        if pd.isna(change):
            return "No Previous Data"
        elif change < 5:
            return "Normal"
        elif change < 10:
            return "Watch"
        elif change < 20:
            return "Warning"
        else:
            return "Critical"

    df["alert_status"] = df["percentage_change"].apply(assign_status)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO fact_price_alert
                    (
                        commodity_id,
                        region_id,
                        price_date,
                        current_price,
                        previous_price,
                        percentage_change,
                        alert_status
                    )
                    VALUES
                    (
                        :commodity_id,
                        :region_id,
                        :price_date,
                        :current_price,
                        :previous_price,
                        :percentage_change,
                        :alert_status
                    )
                    ON CONFLICT (commodity_id, region_id, price_date)
                    DO UPDATE SET
                        current_price = EXCLUDED.current_price,
                        previous_price = EXCLUDED.previous_price,
                        percentage_change = EXCLUDED.percentage_change,
                        alert_status = EXCLUDED.alert_status
                """),
                {
                    "commodity_id": int(row["commodity_id"]),
                    "region_id": int(row["region_id"]),
                    "price_date": row["price_date"],
                    "current_price": float(row["price"]),
                    "previous_price": None if pd.isna(row["previous_price"]) else float(row["previous_price"]),
                    "percentage_change": None if pd.isna(row["percentage_change"]) else float(row["percentage_change"]),
                    "alert_status": row["alert_status"]
                }
            )


def main():
    print("Reading CSV...")
    df = pd.read_csv(CSV_PATH)

    print("Cleaning data...")
    df = clean_data(df)

    print("Loading dimension tables...")
    load_dimensions(df)

    print("Loading fact table...")
    load_fact_food_price(df)

    print("Calculating price alerts...")
    calculate_alerts()

    print("ETL finished successfully.")


if __name__ == "__main__":
    main()