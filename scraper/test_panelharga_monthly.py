import json
import os
from pathlib import Path
from itertools import product

import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_DIR = DATA_DIR / "source_pages" / "panelharga_monthly_test"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = "https://api-panelhargav2.badanpangan.go.id/api"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "referer": "https://panelharga.badanpangan.go.id/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


CANDIDATE_PARAM_SETS = [
    {
        "level_harga_id": 3,
        "komoditas_id": 27,
        "province_id": "",
        "city_id": "",
        "period_date": "01/05/2026 - 01/05/2026",
    },
    {
        "level_harga_id": 3,
        "komoditas_id": 27,
        "province_id": "",
        "period_date": "01/05/2026 - 01/05/2026",
    },
    {
        "level_harga_id": 3,
        "komoditas_id": 27,
        "period_date": "01/05/2026 - 01/05/2026",
    },
    {
        "level_harga_id": 3,
        "komoditas_id": 27,
        "tanggal": "2026-05-01",
    },
    {
        "level_harga_id": 3,
        "komoditas_id": 27,
        "start_date": "2026-05-01",
        "end_date": "2026-05-01",
    },
    {
        "level_harga_id": 3,
        "komoditas_id": 27,
        "bulan": 5,
        "tahun": 2026,
    },
    {
        "level_harga_id": 3,
        "commodity_id": 27,
        "period_date": "01/05/2026 - 01/05/2026",
    },
]


def try_endpoint(path: str, params: dict, index: int):
    url = f"{API_BASE}{path}"

    print("\n" + "=" * 100)
    print(f"Test {index}")
    print("URL:", url)
    print("Params:", params)

    response = requests.get(
        url,
        headers=HEADERS,
        params=params,
        timeout=60,
    )

    print("Status:", response.status_code)
    print("Content-Type:", response.headers.get("content-type"))
    print("Text sample:", response.text[:700])

    output_path = OUTPUT_DIR / f"{index:03d}_{path.strip('/').replace('/', '_')}.txt"
    output_path.write_text(response.text, encoding="utf-8", errors="ignore")

    return response


def main():
    paths = [
        "/front/harga-pangan-bulanan-v2",
        "/front/harga-pangan-bulanan-v2/export",
    ]

    index = 1

    for path, params in product(paths, CANDIDATE_PARAM_SETS):
        try:
            try_endpoint(path, params, index)
        except Exception as e:
            print("Request failed:", e)

        index += 1


if __name__ == "__main__":
    main()