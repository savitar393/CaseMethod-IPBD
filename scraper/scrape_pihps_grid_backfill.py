import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "pihps_grid_history.csv"

DEBUG_DIR = DATA_DIR / "source_pages" / "pihps_grid_backfill"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.bi.go.id/hargapangan/WebSite/Home/GetGridData1"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.bi.go.id/hargapangan",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


MONTHS_EN = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


MONTHS_ID_SHORT = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "Mei": 5,
    "Jun": 6,
    "Jul": 7,
    "Agu": 8,
    "Sep": 9,
    "Okt": 10,
    "Nov": 11,
    "Des": 12,
}

DEFAULT_COMMODITY_IDS = "1,2,3,4,5,6,7,8,9,10"


def get_commodity_ids():
    raw = os.getenv("PIHPS_COMMODITY_IDS", DEFAULT_COMMODITY_IDS)
    return [
        int(item.strip())
        for item in raw.split(",")
        if item.strip()
    ]

def parse_pihps_short_date(value: str, fallback_date: date) -> str:
    """
    Convert PIHPS date like:
    '01 Mei 26' -> '2026-05-01'
    '30 Apr 26' -> '2026-04-30'
    """
    if not value:
        return fallback_date.isoformat()

    parts = str(value).strip().split()

    if len(parts) != 3:
        return fallback_date.isoformat()

    day_text, month_text, year_text = parts

    month = MONTHS_ID_SHORT.get(month_text)

    if month is None:
        return fallback_date.isoformat()

    year = int(year_text)

    if year < 100:
        year += 2000

    return date(year, month, int(day_text)).isoformat()


def format_pihps_date(d: date) -> str:
    return f"{MONTHS_EN[d.month]} {d.day}, {d.year}"


def extract_records_from_response(data, fallback_date: date):
    """
    Parse PIHPS GetGridData1 response.

    Actual response fields:
    - ProvID
    - Provinsi
    - Tanggal
    - Komoditas
    - Nilai
    - Percentage
    """
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        inner = data.get("data", [])

        if isinstance(inner, dict) and "data" in inner:
            records = inner["data"]
        else:
            records = inner
    else:
        records = []

    rows = []

    for item in records:
        if not isinstance(item, dict):
            continue

        commodity_name = item.get("Komoditas")
        price = item.get("Nilai")
        province_name = item.get("Provinsi")
        row_date = parse_pihps_short_date(item.get("Tanggal"), fallback_date)

        if commodity_name is None or price is None or province_name is None:
            continue

        rows.append({
            "price_date": row_date,
            "province_name": str(province_name).strip(),
            "city_name": str(province_name).strip(),
            "commodity_name": str(commodity_name).strip(),
            "unit": "kg",
            "price": price,
            "source": "PIHPS Grid Backfill",
        })

    return rows


def fetch_one_date(d: date, commodity_id: int):
    tanggal = format_pihps_date(d)

    params = {
        "tanggal": tanggal,
        "commodity": commodity_id,
        "priceType": 1,
        "isPasokan": 1,
        "jenis": 1,
        "periode": 1,
        "provId": 0,
    }

    print(f"Fetching PIHPS grid: {tanggal} | commodity={commodity_id}")

    response = requests.get(
        URL,
        headers=HEADERS,
        params=params,
        timeout=60,
    )

    print("Status:", response.status_code)

    debug_path = DEBUG_DIR / f"grid_{d.isoformat()}_commodity_{commodity_id}.json"
    debug_path.write_text(response.text, encoding="utf-8", errors="ignore")

    if response.status_code != 200:
        print(response.text[:300])
        return []

    try:
        data = response.json()
    except Exception:
        print("Failed to parse JSON.")
        return []

    rows = extract_records_from_response(data, d)

    print(f"Rows extracted: {len(rows)}")
    return rows


def main():
    end_date = date(2026, 5, 2)
    days_back = int(os.getenv("PIHPS_DAYS_BACK", "90"))
    start_date = end_date - timedelta(days=days_back - 1)

    commodity_ids = get_commodity_ids()

    print(f"Backfill date range: {start_date} to {end_date}")
    print(f"Commodity IDs: {commodity_ids}")

    all_rows = []

    current = start_date
    while current <= end_date:
        for commodity_id in commodity_ids:
            rows = fetch_one_date(current, commodity_id)
            all_rows.extend(rows)

            # polite delay
            time.sleep(0.25)

        current += timedelta(days=1)

    df = pd.DataFrame(all_rows)

    if df.empty:
        raise RuntimeError("No PIHPS grid rows extracted.")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price_date", "commodity_name", "price"])
    df = df[df["price"] >= 1000]

    invalid_names = {
        "harga",
        "per kg",
        "per liter",
        "rp",
    }

    df = df[~df["commodity_name"].str.lower().isin(invalid_names)]

    df = df.drop_duplicates(
        subset=[
            "price_date",
            "province_name",
            "city_name",
            "commodity_name",
            "source",
        ],
        keep="last",
    )

    df = df.sort_values(["commodity_name", "province_name", "price_date"])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nSaved PIHPS grid history to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print("Date range:", df["price_date"].min(), "to", df["price_date"].max())
    print("Commodity count:", df["commodity_name"].nunique())
    print("Province count:", df["province_name"].nunique())
    print("\nCommodity counts:")
    print(df["commodity_name"].value_counts())
    print("\nSource counts:")
    print(df["source"].value_counts())


if __name__ == "__main__":
    main()