import os
from pathlib import Path

import pandas as pd
import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "pihps_grid_commodity_discovery.csv"

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


def extract_records(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        inner = data.get("data", [])
        if isinstance(inner, dict) and "data" in inner:
            return inner["data"]
        return inner

    return []


def main():
    rows = []

    # Test broad range first. We can narrow later.
    for commodity_id in range(1, 80):
        params = {
            "tanggal": "May 1, 2026",
            "commodity": commodity_id,
            "priceType": 1,
            "isPasokan": 1,
            "jenis": 1,
            "periode": 1,
            "provId": 0,
        }

        print(f"Testing commodity={commodity_id}")

        try:
            response = requests.get(URL, headers=HEADERS, params=params, timeout=60)
            if response.status_code != 200:
                rows.append({
                    "commodity_id": commodity_id,
                    "status": response.status_code,
                    "row_count": 0,
                    "commodities_found": "",
                })
                continue

            data = response.json()
            records = extract_records(data)

            commodity_names = sorted({
                str(item.get("Komoditas", "")).strip()
                for item in records
                if isinstance(item, dict) and item.get("Komoditas")
            })

            rows.append({
                "commodity_id": commodity_id,
                "status": response.status_code,
                "row_count": len(records),
                "commodities_found": " | ".join(commodity_names),
            })

        except Exception as e:
            rows.append({
                "commodity_id": commodity_id,
                "status": "error",
                "row_count": 0,
                "commodities_found": str(e),
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved discovery result to: {OUTPUT_PATH}")
    print(df[df["row_count"] > 0].to_string(index=False))


if __name__ == "__main__":
    main()