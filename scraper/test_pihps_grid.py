import json
import os
from pathlib import Path

import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
DEBUG_DIR = DATA_DIR / "source_pages" / "pihps_grid_test"
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


def test_date(tanggal: str):
    params = {
        "tanggal": tanggal,
        "commodity": 1,
        "priceType": 1,
        "isPasokan": 1,
        "jenis": 1,
        "periode": 1,
        "provId": 0,
    }

    print(f"\nTesting date: {tanggal}")

    response = requests.get(URL, headers=HEADERS, params=params, timeout=60)

    print("Status:", response.status_code)
    print("Content-Type:", response.headers.get("content-type"))
    print("Text sample:", response.text[:1000])

    output_path = DEBUG_DIR / f"grid_{tanggal.replace(',', '').replace(' ', '_')}.json"
    output_path.write_text(response.text, encoding="utf-8", errors="ignore")

    response.raise_for_status()

    try:
        data = response.json()
        print("JSON keys:", data.keys() if isinstance(data, dict) else type(data))
    except Exception:
        print("Could not parse JSON")


def main():
    test_dates = [
        "May 1, 2026",
        "April 30, 2026",
        "April 1, 2026",
        "March 1, 2026",
    ]

    for tanggal in test_dates:
        test_date(tanggal)


if __name__ == "__main__":
    main()