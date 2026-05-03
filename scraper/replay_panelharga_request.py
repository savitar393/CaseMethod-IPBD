import json
import os
from pathlib import Path

import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
REQUEST_LOG = DATA_DIR / "source_pages" / "panelharga_headers" / "api_requests.json"
OUTPUT_DIR = DATA_DIR / "source_pages" / "panelharga_replay"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_target_request(keyword: str):
    if not REQUEST_LOG.exists():
        raise FileNotFoundError(f"Request log not found: {REQUEST_LOG}")

    logs = json.loads(REQUEST_LOG.read_text(encoding="utf-8"))

    for item in logs:
        if keyword in item["url"]:
            return item

    raise RuntimeError(f"No request found containing keyword: {keyword}")


def sanitize_headers(headers: dict):
    """
    Keep the browser headers, but remove headers that requests cannot/should not send.
    """
    blocked = {
        "host",
        "connection",
        "content-length",
    }

    clean = {}

    for key, value in headers.items():
        if key.lower() not in blocked:
            clean[key] = value

    # Add common browser context headers
    clean.setdefault("origin", "https://panelharga.badanpangan.go.id")
    clean.setdefault("referer", "https://panelharga.badanpangan.go.id/")
    clean.setdefault("accept", "application/json, text/plain, */*")

    return clean


def replay(keyword: str, output_name: str):
    req = load_target_request(keyword)

    url = req["url"]
    headers = sanitize_headers(req["headers"])

    print("Replaying exact URL:")
    print(url)

    print("\nHeaders containing API/auth info:")
    for key, value in headers.items():
        if any(token in key.lower() for token in ["api", "auth", "token", "key"]):
            print(f"{key}: {value[:12]}...{value[-8:] if len(value) > 20 else value}")

    response = requests.get(url, headers=headers, timeout=60)

    print("\nStatus:", response.status_code)
    print("Content-Type:", response.headers.get("content-type"))
    print("Text sample:", response.text[:1000])

    output_path = OUTPUT_DIR / output_name
    output_path.write_text(response.text, encoding="utf-8", errors="ignore")
    print(f"Saved to: {output_path}")


def main():
    replay(
        keyword="harga-peta-provinsi",
        output_name="harga_peta_provinsi_replay.json",
    )

    replay(
        keyword="harga-pangan-informasi",
        output_name="harga_pangan_informasi_replay.json",
    )


if __name__ == "__main__":
    main()