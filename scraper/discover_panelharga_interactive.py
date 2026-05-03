import json
import os
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
DEBUG_DIR = DATA_DIR / "source_pages" / "panelharga_interactive"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URLS = [
    "https://panelharga.badanpangan.go.id/harga-eceran",
    "https://panelharga.badanpangan.go.id/",
]


def is_target_url(url: str):
    lower = url.lower()
    return (
        "api-panelhargav2.badanpangan.go.id" in lower
        and (
            "harga-pangan-bulanan" in lower
            or "harga-pangan" in lower
            or "harga-peta" in lower
            or "komoditas" in lower
            or "provinces" in lower
        )
    )


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
            if is_target_url(request.url):
                logs.append({
                    "event": "request",
                    "url": request.url,
                    "method": request.method,
                    "headers": request.headers,
                    "post_data": request.post_data,
                })

        def handle_response(response):
            if not is_target_url(response.url):
                return

            row = {
                "event": "response",
                "url": response.url,
                "status": response.status,
                "headers": response.headers,
            }

            try:
                text = response.text()
                row["body_sample"] = text[:1200].replace("\n", " ")
                row["body_length"] = len(text)
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

                # Try scrolling and clicking visible buttons/tabs/dropdowns.
                page.mouse.wheel(0, 1600)
                page.wait_for_timeout(2000)

                texts_to_click = [
                    "Harga Eceran",
                    "Grafik",
                    "Tabel",
                    "Peta",
                    "Download",
                    "Cari",
                    "Lihat",
                    "Filter",
                ]

                for text in texts_to_click:
                    try:
                        locator = page.get_by_text(text, exact=False).first
                        if locator.count() > 0:
                            print(f"Trying click text: {text}")
                            locator.click(timeout=3000)
                            page.wait_for_timeout(3000)
                    except Exception:
                        pass

                screenshot_path = DEBUG_DIR / f"{url.replace('https://', '').replace('/', '_')}.png"
                page.screenshot(path=str(screenshot_path), full_page=True)
                print(f"Saved screenshot: {screenshot_path}")

            except Exception as e:
                print(f"Failed: {e}")

        browser.close()

    output_json = DEBUG_DIR / "interactive_logs.json"
    output_json.write_text(json.dumps(logs, indent=2, ensure_ascii=False), encoding="utf-8")

    df = pd.DataFrame(logs)
    output_csv = DEBUG_DIR / "interactive_logs.csv"
    df.to_csv(output_csv, index=False)

    print(f"\nSaved logs: {output_csv}")
    print(f"Rows: {len(df)}")

    if not df.empty:
        print(
            df[["event", "status", "url", "body_sample"]]
            .head(50)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()