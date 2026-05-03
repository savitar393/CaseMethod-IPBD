import os
from pathlib import Path

import boto3
import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:pangan_pass@localhost:5432/panganwatch"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "pangan-raw")

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data"))
if not DATA_DIR.exists():
    DATA_DIR = Path("data")

LOCAL_SOURCE_FILE = DATA_DIR / "sample_food_price.csv"
RAW_LOCAL_FILE = DATA_DIR / "raw" / "food_price_raw.csv"
PROCESSED_LOCAL_FILE = DATA_DIR / "processed" / "food_price_processed.csv"

RAW_OBJECT_KEY = "raw/food_price_raw.csv"
PROCESSED_OBJECT_KEY = "processed/food_price_processed.csv"


def get_engine():
    return create_engine(DATABASE_URL)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )


def ensure_minio_bucket():
    s3 = get_s3_client()

    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    return s3


def extract_to_minio():
    """
    Extract stage:
    Take the source CSV and upload it to MinIO as raw data.
    """
    if not LOCAL_SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {LOCAL_SOURCE_FILE}")

    RAW_LOCAL_FILE.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(LOCAL_SOURCE_FILE)
    df.to_csv(RAW_LOCAL_FILE, index=False)

    s3 = ensure_minio_bucket()
    s3.upload_file(str(RAW_LOCAL_FILE), MINIO_BUCKET, RAW_OBJECT_KEY)

    print(f"Raw data uploaded to MinIO: s3://{MINIO_BUCKET}/{RAW_OBJECT_KEY}")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform stage:
    Clean date, region, commodity, unit, source, and price columns.
    """
    df["price_date"] = pd.to_datetime(df["price_date"], errors="coerce").dt.date

    df["province_name"] = df["province_name"].astype(str).str.strip()
    df["city_name"] = df["city_name"].fillna("Unknown").astype(str).str.strip()
    df["commodity_name"] = df["commodity_name"].astype(str).str.strip()
    df["unit"] = df["unit"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Remove invalid price rows
    df = df[df["price"] > 0]

    # Prevent duplicate commodity-region-date rows from breaking alert calculation
    df = df.drop_duplicates(
        subset=[
            "price_date",
            "province_name",
            "city_name",
            "commodity_name",
        ],
        keep="last"
    )

    df = df.dropna(
        subset=[
            "price_date",
            "province_name",
            "city_name",
            "commodity_name",
            "price"
        ]
    )

    df = df.drop_duplicates()

    return df


def transform_from_minio():
    """
    Transform stage:
    Download raw data from MinIO, clean it, save processed CSV,
    then upload processed data back to MinIO.
    """
    RAW_LOCAL_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_LOCAL_FILE.parent.mkdir(parents=True, exist_ok=True)

    s3 = ensure_minio_bucket()
    s3.download_file(MINIO_BUCKET, RAW_OBJECT_KEY, str(RAW_LOCAL_FILE))

    df = pd.read_csv(RAW_LOCAL_FILE)
    df = clean_data(df)

    df.to_csv(PROCESSED_LOCAL_FILE, index=False)
    s3.upload_file(str(PROCESSED_LOCAL_FILE), MINIO_BUCKET, PROCESSED_OBJECT_KEY)

    print(f"Processed data uploaded to MinIO: s3://{MINIO_BUCKET}/{PROCESSED_OBJECT_KEY}")


def load_dimensions(df: pd.DataFrame):
    engine = get_engine()

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
    engine = get_engine()

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
                    "price": float(row["price"]),
                    "source": row["source"]
                }
            )


def load_to_postgres():
    """
    Load stage:
    Load processed CSV into PostgreSQL dimension and fact tables.
    """
    if not PROCESSED_LOCAL_FILE.exists():
        raise FileNotFoundError(f"Processed file not found: {PROCESSED_LOCAL_FILE}")

    df = pd.read_csv(PROCESSED_LOCAL_FILE)
    df = clean_data(df)

    load_dimensions(df)
    load_fact_food_price(df)

    print("Processed data loaded to PostgreSQL.")


def calculate_alerts():
    """
    Analysis stage:
    Calculate daily price changes and assign alert status.
    """
    engine = get_engine()

    query = """
        SELECT
            f.price_date,
            c.commodity_id,
            r.region_id,
            c.commodity_name,
            r.province_name,
            r.city_name,
            f.price,
            f.source
        FROM fact_food_price f
        JOIN dim_commodity c ON f.commodity_id = c.commodity_id
        JOIN dim_region r ON f.region_id = r.region_id
        ORDER BY c.commodity_name, r.province_name, r.city_name, f.source, f.price_date
    """

    df = pd.read_sql(query, engine)
    df["price_date"] = pd.to_datetime(df["price_date"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df["previous_price"] = df.groupby(
        ["commodity_id", "region_id", "source"]
    )["price"].shift(1)

    df["previous_date"] = df.groupby(
        ["commodity_id", "region_id", "source"]
    )["price_date"].shift(1)

    df["gap_days"] = (
        pd.to_datetime(df["price_date"]) - pd.to_datetime(df["previous_date"])
    ).dt.days


    def calculate_percentage_change(row):
        previous_price = row["previous_price"]
        gap_days = row["gap_days"]

        if pd.isna(previous_price):
            return None

        if previous_price <= 0:
            return None

        # Do not treat long gaps as daily price alerts
        if pd.isna(gap_days) or gap_days > 7:
            return None

        return ((row["price"] - previous_price) / previous_price) * 100


    df["percentage_change"] = df.apply(calculate_percentage_change, axis=1)

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
            price_date = row["price_date"].date()

            conn.execute(
            text("""
                INSERT INTO fact_price_alert
                (
                    commodity_id,
                    region_id,
                    price_date,
                    source,
                    current_price,
                    previous_price,
                    percentage_change,
                    gap_days,
                    alert_status
                )
                VALUES
                (
                    :commodity_id,
                    :region_id,
                    :price_date,
                    :source,
                    :current_price,
                    :previous_price,
                    :percentage_change,
                    :gap_days,
                    :alert_status
                )
                ON CONFLICT (commodity_id, region_id, price_date, source)
                DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    previous_price = EXCLUDED.previous_price,
                    percentage_change = EXCLUDED.percentage_change,
                    gap_days = EXCLUDED.gap_days,
                    alert_status = EXCLUDED.alert_status
            """),
            {
                "commodity_id": int(row["commodity_id"]),
                "region_id": int(row["region_id"]),
                "price_date": price_date,
                "source": row["source"],
                "current_price": float(row["price"]),
                "previous_price": None if pd.isna(row["previous_price"]) else float(row["previous_price"]),
                "percentage_change": None if pd.isna(row["percentage_change"]) else float(row["percentage_change"]),
                "gap_days": None if pd.isna(row["gap_days"]) else int(row["gap_days"]),
                "alert_status": row["alert_status"]
            }
        )

    print("Price alerts calculated.")


def run_full_pipeline():
    extract_to_minio()
    transform_from_minio()
    load_to_postgres()
    calculate_alerts()


if __name__ == "__main__":
    run_full_pipeline()