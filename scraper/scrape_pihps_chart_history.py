import os
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import time
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

import pandas as pd
from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "pihps_chart_history.csv"

DEBUG_DIR = DATA_DIR / "source_pages" / "pihps_chart_history"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.bi.go.id/hargapangan"


def normalize_chart_row(item: dict) -> dict:
    return {
        "price_date": pd.to_datetime(item.get("date")).date().isoformat(),
        "province_name": "Nasional",
        "city_name": "Nasional",
        "commodity_name": str(item.get("name", "")).strip(),
        "unit": str(item.get("denomination") or "kg").strip(),
        "price": item.get("nominal"),
        "source": "PIHPS Chart History",
    }


def main():
    rows = []
    raw_responses = []

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
            url = response.url

            if "WebSite/Home/GetChartData" not in url:
                return

            # Skip one-row info widgets. We want full chart history responses.
            if "forInfo=true" in url:
                return

            try:
                data = response.json()
            except Exception:
                return

            chart_data = data.get("data", [])

            if not chart_data:
                return

            raw_responses.append({
                "url": url,
                "rows": len(chart_data),
                "sample": chart_data[:2],
            })

            for item in chart_data:
                try:
                    row = normalize_chart_row(item)

                    if row["commodity_name"] and row["price"] is not None:
                        rows.append(row)

                except Exception as e:
                    print(f"Skipping bad chart row: {e}")

        page.on("response", handle_response)

        def goto_with_retry(page, url, retries=3):
            last_error = None

            for attempt in range(1, retries + 1):
                try:
                    print(f"Opening attempt {attempt}: {url}")

                    page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )

                    page.wait_for_timeout(10000)
                    return True

                except Exception as e:
                    last_error = e
                    print(f"Attempt {attempt} failed: {e}")
                    time.sleep(5)

            print(f"All attempts failed. Last error: {last_error}")
            return False


        success = goto_with_retry(page, URL)

        if not success:
            browser.close()

            if OUTPUT_PATH.exists():
                print(f"PIHPS chart page failed, keeping existing file: {OUTPUT_PATH}")
                return

            raise RuntimeError("Failed to open PIHPS chart page and no previous CSV exists.")

        page.screenshot(
            path=str(DEBUG_DIR / "pihps_chart_history.png"),
            full_page=True
        )

        browser.close()

    if not rows:
        raise RuntimeError(
            "No PIHPS chart rows captured. Check screenshot/debug directory."
        )

    df = pd.DataFrame(rows)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price_date", "commodity_name", "price"])
    df = df[df["price"] >= 1000]

    invalid_names = {"harga", "per kg", "per liter", "rp"}
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

    df = df.sort_values(["commodity_name", "price_date"])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    (DEBUG_DIR / "captured_chart_responses.json").write_text(
        json.dumps(raw_responses, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"Saved PIHPS chart history to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print("Date counts:")
    print(df["price_date"].value_counts().sort_index())
    print("Commodity count:", df["commodity_name"].nunique())
    print(df.head(30))


if __name__ == "__main__":
    main()