import json
import os
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
DEBUG_DIR = DATA_DIR / "source_pages" / "pihps_discovery"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URLS = [
    "https://www.bi.go.id/hargapangan",
    "https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisionalDaerah",
    "https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisionalKomoditas",
]


def is_interesting_url(url: str) -> bool:
    lower = url.lower()
    keywords = [
        "hargapangan",
        "tabelharga",
        "get",
        "download",
        "excel",
        "csv",
        "json",
        "report",
        "harga",
        "komoditas",
        "provinsi",
        "kabupaten",
        "pasar",
    ]
    return any(k in lower for k in keywords)


def safe_name(url: str) -> str:
    parsed = urlparse(url)
    name = f"{parsed.netloc}_{parsed.path}".replace("/", "_")
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in name)[:150]


def main():
    logs = []

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

        def handle_request(request):
            if is_interesting_url(request.url):
                logs.append({
                    "event": "request",
                    "url": request.url,
                    "method": request.method,
                    "headers": request.headers,
                    "post_data": request.post_data,
                })

        def handle_response(response):
            if not is_interesting_url(response.url):
                return

            row = {
                "event": "response",
                "url": response.url,
                "status": response.status,
                "headers": response.headers,
            }

            try:
                text = response.text()
                row["body_length"] = len(text)
                row["body_sample"] = text[:1000].replace("\n", " ")
            except Exception as e:
                row["body_error"] = str(e)

            logs.append(row)

        page.on("request", handle_request)
        page.on("response", handle_response)

        for url in TARGET_URLS:
            print(f"Opening: {url}")

            try:
                page.goto(url, wait_until="networkidle", timeout=90000)
                page.wait_for_timeout(5000)

                # Try clicking report/download buttons if visible.
                for text in ["Lihat Laporan", "Download"]:
                    try:
                        locator = page.get_by_text(text, exact=False).first
                        if locator.count() > 0:
                            print(f"Trying click: {text}")
                            locator.click(timeout=5000)
                            page.wait_for_timeout(5000)
                    except Exception as e:
                        print(f"Could not click {text}: {e}")

                screenshot_path = DEBUG_DIR / f"{safe_name(url)}.png"
                page.screenshot(path=str(screenshot_path), full_page=True)
                print(f"Saved screenshot: {screenshot_path}")

            except Exception as e:
                print(f"Failed opening {url}: {e}")

        browser.close()

    output_json = DEBUG_DIR / "pihps_logs.json"
    output_json.write_text(json.dumps(logs, indent=2, ensure_ascii=False), encoding="utf-8")

    df = pd.DataFrame(logs)
    output_csv = DEBUG_DIR / "pihps_logs.csv"
    df.to_csv(output_csv, index=False)

    print(f"\nSaved logs to: {output_csv}")
    print(f"Rows: {len(df)}")

    if not df.empty:
        print(
            df[
                [
                    "event",
                    "status",
                    "method",
                    "url",
                    "body_sample",
                ]
            ].head(80).to_string(index=False)
        )


if __name__ == "__main__":
    main()