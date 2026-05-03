import json
import os
from pathlib import Path

import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
DEBUG_DIR = DATA_DIR / "source_pages" / "panelharga_api_test"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = "https://api-panelhargav2.badanpangan.go.id"


def load_api_key_from_discovery():
    """
    Try to read x-api-key from the previously captured Playwright request log.
    This avoids hardcoding the key inside the script.
    """
    candidates = [
        DATA_DIR / "source_pages" / "panelharga_headers" / "api_requests.json",
        Path("data/source_pages/panelharga_headers/api_requests.json"),
    ]

    for path in candidates:
        if not path.exists():
            continue

        requests_log = json.loads(path.read_text(encoding="utf-8"))

        for item in requests_log:
            headers = item.get("headers", {})
            api_key = headers.get("x-api-key")

            if api_key:
                return api_key

    return None


def get_headers():
    api_key = os.getenv("PANELHARGA_API_KEY") or load_api_key_from_discovery()

    if not api_key:
        raise RuntimeError(
            "No PANELHARGA_API_KEY found. "
            "Run discover_panelharga_headers.py first or set PANELHARGA_API_KEY."
        )

    return {
        "x-api-key": api_key,
        "accept": "application/json, text/plain, */*",
        "referer": "https://panelharga.badanpangan.go.id/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }


def print_json_structure(obj, indent=0, max_depth=3):
    prefix = "  " * indent

    if indent > max_depth:
        print(prefix + "...")
        return

    if isinstance(obj, dict):
        print(prefix + f"dict keys: {list(obj.keys())}")

        for key, value in list(obj.items())[:5]:
            print(prefix + f"- {key}: {type(value).__name__}")
            print_json_structure(value, indent + 1, max_depth)

    elif isinstance(obj, list):
        print(prefix + f"list length: {len(obj)}")

        if obj:
            print(prefix + f"first item type: {type(obj[0]).__name__}")
            print_json_structure(obj[0], indent + 1, max_depth)

    else:
        print(prefix + f"{repr(obj)[:100]}")


def test_endpoint(path, params=None, output_name="response.json"):
    url = f"{API_BASE}{path}"

    print(f"\nRequesting: {url}")
    print(f"Params: {params}")

    response = requests.get(
        url,
        headers=get_headers(),
        params=params,
        timeout=60,
    )

    print("Status:", response.status_code)
    print("Content-Type:", response.headers.get("content-type"))
    print("Text sample:", response.text[:500])

    output_path = DEBUG_DIR / output_name
    output_path.write_text(response.text, encoding="utf-8", errors="ignore")

    print(f"Saved raw response to: {output_path}")

    response.raise_for_status()

    data = response.json()

    print("\nJSON structure:")
    print_json_structure(data)

    return data


def main():
    # Metadata endpoint: province list
    test_endpoint(
        "/api/provinces",
        params={"search": ""},
        output_name="provinces.json",
    )

    # Metadata endpoint: retail commodities
    test_endpoint(
        "/api/cms/eceran",
        output_name="eceran_commodities.json",
    )

    # Main map endpoint discovered from network log
    test_endpoint(
        "/api/front/harga-peta-provinsi",
        params={
            "level_harga_id": 3,
            "komoditas_id": 109,
            "period_date": "01/05/2026 - 01/05/2026",
            "multi_status_map[0]": "",
            "multi_province_id[0]": "",
        },
        output_name="harga_peta_provinsi.json",
    )


if __name__ == "__main__":
    main()