import json
import os
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
DEBUG_DIR = DATA_DIR / "source_pages" / "endpoint_discovery"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URLS = [
    "https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisionalDaerah",
    "https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisionalKomoditas",
    "https://panelharga.badanpangan.go.id/",
    "https://dev-panelharga.badanpangan.go.id/",
    "https://satudata.jakarta.go.id/open-data/detail/harga-rata-rata-bahan-pangan",
]


def is_interesting_url(url: str) -> bool:
    lower = url.lower()

    keywords = [
        "api",
        "json",
        "harga",
        "pangan",
        "komod",
        "prov",
        "kab",
        "download",
        "datatable",
        "table",
        "report",
        "laporan",
        "get",
        "list",
    ]

    return any(k in lower for k in keywords)


def safe_filename(url: str, index: int) -> str:
    parsed = urlparse(url)
    base = f"{parsed.netloc}_{parsed.path}".replace("/", "_").replace("\\", "_")
    base = "".join(c if c.isalnum() or c in "._-" else "_" for c in base)
    return f"{index:03d}_{base[:120]}"


def main():
    logs = []
    response_counter = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        page = browser.new_page(
            viewport={"width": 1366, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        def handle_response(response):
            nonlocal response_counter

            url = response.url
            status = response.status
            request = response.request
            resource_type = request.resource_type

            if not is_interesting_url(url):
                return

            content_type = response.headers.get("content-type", "")

            row = {
                "url": url,
                "status": status,
                "method": request.method,
                "resource_type": resource_type,
                "content_type": content_type,
            }

            try:
                body = response.text()
                row["body_length"] = len(body)
                row["body_sample"] = body[:500].replace("\n", " ")

                # Save likely useful responses
                if (
                    "json" in content_type.lower()
                    or body.strip().startswith("{")
                    or body.strip().startswith("[")
                    or "csv" in content_type.lower()
                    or "excel" in content_type.lower()
                    or "spreadsheet" in content_type.lower()
                ):
                    filename = safe_filename(url, response_counter)

                    if body.strip().startswith("{") or body.strip().startswith("["):
                        output_path = DEBUG_DIR / f"{filename}.json"
                    else:
                        output_path = DEBUG_DIR / f"{filename}.txt"

                    output_path.write_text(body, encoding="utf-8", errors="ignore")
                    row["saved_to"] = str(output_path)
                    response_counter += 1

            except Exception as e:
                row["body_error"] = str(e)

            logs.append(row)

        page.on("response", handle_response)

        for url in TARGET_URLS:
            print(f"Opening: {url}")

            try:
                page.goto(url, wait_until="networkidle", timeout=90000)
                page.wait_for_timeout(5000)

                screenshot_name = safe_filename(url, response_counter)
                screenshot_path = DEBUG_DIR / f"{screenshot_name}.png"
                page.screenshot(path=str(screenshot_path), full_page=True)

                print(f"Saved screenshot: {screenshot_path}")

            except Exception as e:
                print(f"Failed to open {url}: {e}")

        browser.close()

    df = pd.DataFrame(logs)

    output_csv = DEBUG_DIR / "network_log.csv"
    df.to_csv(output_csv, index=False)

    print(f"\nSaved network log to: {output_csv}")
    print(f"Total interesting responses: {len(df)}")

    if not df.empty:
        print("\nTop candidate URLs:")
        print(
            df[
                [
                    "status",
                    "method",
                    "resource_type",
                    "content_type",
                    "url",
                    "saved_to",
                ]
            ].head(30).to_string(index=False)
        )


if __name__ == "__main__":
    main()