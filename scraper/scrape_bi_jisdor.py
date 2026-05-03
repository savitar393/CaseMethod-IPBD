import os
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "exchange_rate_usd_idr.csv"

DEBUG_DIR = DATA_DIR / "source_pages" / "macro_cost"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.bi.go.id/id/statistik/informasi-kurs/jisdor/default.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

MONTH_MAP = {
    "januari": 1,
    "februari": 2,
    "maret": 3,
    "april": 4,
    "mei": 5,
    "juni": 6,
    "juli": 7,
    "agustus": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "desember": 12,
}


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_indonesian_date(text):
    text = clean_text(text).lower()
    parts = text.split()

    if len(parts) != 3:
        return None

    try:
        day = int(parts[0])
        month = MONTH_MAP.get(parts[1])
        year = int(parts[2])

        if not month:
            return None

        return datetime(year, month, day).date()
    except Exception:
        return None


def parse_rupiah(text):
    text = clean_text(text)

    if "rp" not in text.lower():
        return None

    text = re.sub(r"rp", "", text, flags=re.IGNORECASE)
    text = text.replace(" ", "")
    text = text.replace(".", "")
    text = text.replace(",", ".")

    try:
        return float(text)
    except Exception:
        return None


def extract_rows_from_html(html):
    soup = BeautifulSoup(html, "html.parser")

    rows = []

    for tr in soup.find_all("tr"):
        cells = [clean_text(td.get_text(" ")) for td in tr.find_all("td")]

        if len(cells) < 2:
            continue

        rate_date = parse_indonesian_date(cells[0])
        rate_mid = parse_rupiah(cells[1])

        if rate_date and rate_mid:
            rows.append({
                "rate_date": rate_date,
                "currency_pair": "USD/IDR",
                "rate_mid": rate_mid,
                "source": "Bank Indonesia JISDOR",
            })

    return rows


def main():
    print(f"Fetching BI JISDOR: {URL}")

    response = requests.get(URL, headers=HEADERS, timeout=90)
    response.raise_for_status()

    html = response.text

    debug_path = DEBUG_DIR / "bi_jisdor.html"
    debug_path.write_text(html, encoding="utf-8", errors="ignore")

    rows = extract_rows_from_html(html)

    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("No JISDOR rows extracted. Check debug HTML.")

    df = df.drop_duplicates(
        subset=["rate_date", "currency_pair", "source"],
        keep="last",
    )

    df = df.sort_values("rate_date")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved exchange rate data to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()