import os
import re
from datetime import date
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "pihps_home_price.csv"
DEBUG_DIR = DATA_DIR / "source_pages" / "pihps_home"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.bi.go.id/hargapangan"


INVALID_NAMES = {
    "harga",
    "per kg",
    "per liter",
    "rp",
}


def parse_price(text: str):
    """
    Extract price from text like:
    Rp 15.000
    Rp. 15.000
    Rp15.000
    Rp. -
    """

    match = re.search(r"Rp\.?\s*([\d][\d\.\,]*)", text, re.IGNORECASE)

    if not match:
        return None

    raw = match.group(1)

    # Keep only numeric digits
    digits = re.sub(r"\D", "", raw)

    if not digits:
        return None

    return int(digits)


def is_commodity(text: str):
    text = text.strip()
    lower = text.lower()

    if lower in INVALID_NAMES:
        return False

    if len(text) < 4:
        return False

    blacklist = [
        "beranda",
        "tabel harga",
        "informasi",
        "faq",
        "tutorial",
        "hak cipta",
        "masuk",
        "histogram",
        "tampilan peta",
        "komoditas",
        "jenis pasar",
        "provinsi",
        "kabupaten",
        "periode",
        "tanggal",
    ]

    return not any(k in lower for k in blacklist)


def main():
    rows = []

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

        print(f"Opening: {URL}")
        page.goto(URL, wait_until="networkidle", timeout=90000)
        page.wait_for_timeout(5000)

        screenshot_path = DEBUG_DIR / "pihps_home.png"
        text_path = DEBUG_DIR / "pihps_home.txt"

        page.screenshot(path=str(screenshot_path), full_page=True)

        text = page.locator("body").inner_text()
        text_path.write_text(text, encoding="utf-8")

        lines = [line.strip() for line in text.splitlines() if line.strip()]

        last_commodity = None

        for line in lines:
            if is_commodity(line) and "Rp" not in line:
                last_commodity = line
                continue

            if "Rp" in line and last_commodity:
                price = parse_price(line)

                if price and price > 0:
                    rows.append({
                        "price_date": date.today().isoformat(),
                        "province_name": "Nasional",
                        "city_name": "Nasional",
                        "commodity_name": last_commodity,
                        "unit": "kg",
                        "price": price,
                        "source": "PIHPS Home Snapshot",
                    })

                last_commodity = None

        browser.close()

    df = pd.DataFrame(rows)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["commodity_name", "price"])
    df = df[df["price"] >= 1000]

    if df.empty:
        raise RuntimeError("No PIHPS homepage rows extracted. Check debug text/screenshot.")

    df = df.drop_duplicates(
        subset=["price_date", "province_name", "city_name", "commodity_name"]
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print(df.head(30))


if __name__ == "__main__":
    main()