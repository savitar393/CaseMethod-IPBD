import os
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))

OUTPUT_PATH = DATA_DIR / "sample_food_price.csv"
DEBUG_DIR = DATA_DIR / "source_pages"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

IPJ_URL = "https://infopangan.jakarta.go.id/"
BERITA_JAKARTA_URL = "https://m.beritajakarta.id/info-pangan"

INVALID_COMMODITY_NAMES = {
    "jakarta barat",
    "jakarta pusat",
    "jakarta selatan",
    "jakarta timur",
    "jakarta utara",
    "kepulauan seribu",
    "dki jakarta",
    "jakarta",
    "stabil"
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
}

MONTHS_ID = {
    "januari": 1,
    "februari": 2,
    "maret": 3,
    "april": 4,
    "mei": 5,
    "juni": 6,
    "juli": 7,
    "agustus": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "desember": 12,
}

def parse_price(price_text: str):
    """
    Convert:
    Rp 15.256/kg
    Rp. 12.347 / kg
    into:
    price = 15256, unit = kg
    """
    text = str(price_text).strip()

    price_match = re.search(r"Rp\.?\s*([\d\.\,]+)", text, re.IGNORECASE)
    if not price_match:
        return None, None

    raw_price = price_match.group(1)
    price = int(raw_price.replace(".", "").replace(",", ""))

    unit_match = re.search(r"/\s*([A-Za-z0-9]+)", text)
    unit = unit_match.group(1).lower() if unit_match else "kg"

    return price, unit


def normalize_commodity_name(name: str):
    name = str(name).strip()
    name = re.sub(r"\s+", " ", name)
    return name


def is_valid_commodity(name: str) -> bool:
    if not name:
        return False

    lower = normalize_commodity_name(name).lower()

    if len(lower) < 3:
        return False

    if lower in INVALID_COMMODITY_NAMES:
        return False

    blacklist_keywords = [
        "home",
        "metadata",
        "statistik",
        "pasar",
        "kontak",
        "copyright",
        "info pangan",
        "harga rata-rata",
        "komoditas",
        "search",
        "filter",
        "selengkapnya",
        "naik",
        "turun",
        "tetap",
        "rp",
        "website",
        "beranda",
        "pencarian",
        "sumber",
        "harga tanggal",
        "berbanding",
        "tautan",
        "copyright",
        "stabil"
    ]

    if any(keyword in lower for keyword in blacklist_keywords):
        return False

    if re.search(r"\d{4}", lower):
        return False

    return True


def scrape_ipj_rendered():
    """
    Real source 1:
    Scrape current rendered data from Info Pangan Jakarta.
    """
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            viewport={"width": 1366, "height": 900},
            user_agent=HEADERS["User-Agent"],
        )

        print(f"Opening IPJ: {IPJ_URL}")

        page.goto(IPJ_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(3000)

        screenshot_path = DEBUG_DIR / "ipj_rendered.png"
        text_path = DEBUG_DIR / "ipj_rendered.txt"

        page.screenshot(path=str(screenshot_path), full_page=True)

        page_text = page.locator("body").inner_text()
        text_path.write_text(page_text, encoding="utf-8")

        print(f"Saved rendered text: {text_path}")
        print(f"Saved screenshot: {screenshot_path}")

        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        last_candidate = None

        for line in lines:
            if is_valid_commodity(line):
                last_candidate = normalize_commodity_name(line)
                continue

            if "Rp" in line and last_candidate:
                price, unit = parse_price(line)

                if price is not None and price > 0:
                    rows.append({
                        "price_date": date.today().isoformat(),
                        "province_name": "DKI Jakarta",
                        "city_name": "Jakarta",
                        "commodity_name": last_candidate,
                        "unit": unit,
                        "price": price,
                        "source": "Info Pangan Jakarta",
                    })

                last_candidate = None

        browser.close()

    print(f"IPJ rows before final cleaning: {len(rows)}")
    return rows


def parse_indonesian_date(day: str, month_name: str, year: str):
    month = MONTHS_ID.get(month_name.lower())
    if month is None:
        return None

    return date(int(year), month, int(day))


def extract_page_dates(page_text: str):
    current_match = re.search(
        r"Harga tanggal\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})",
        page_text,
        re.IGNORECASE,
    )

    previous_match = re.search(
        r"harga sebelumnya tanggal\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})",
        page_text,
        re.IGNORECASE,
    )

    current_date = None
    previous_date = None

    if current_match:
        current_date = parse_indonesian_date(
            current_match.group(1),
            current_match.group(2),
            current_match.group(3),
        )

    if previous_match:
        previous_date = parse_indonesian_date(
            previous_match.group(1),
            previous_match.group(2),
            previous_match.group(3),
        )

    if current_date is None:
        current_date = date.today()

    if previous_date is None:
        previous_date = current_date - timedelta(days=1)

    return current_date.isoformat(), previous_date.isoformat()


def scrape_berita_jakarta_static():
    """
    Real source 2:
    Scrape static mobile Berita Jakarta Info Pangan page.

    This version emits two real rows per commodity:
    - current date price
    - previous date price
    """
    print(f"Opening Berita Jakarta: {BERITA_JAKARTA_URL}")

    response = requests.get(BERITA_JAKARTA_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    html_path = DEBUG_DIR / "berita_jakarta_info_pangan.html"
    html_path.write_text(response.text, encoding="utf-8")

    soup = BeautifulSoup(response.text, "html.parser")

    page_text = soup.get_text("\n", strip=True)
    text_path = DEBUG_DIR / "berita_jakarta_info_pangan.txt"
    text_path.write_text(page_text, encoding="utf-8")

    current_date, previous_date = extract_page_dates(page_text)

    print(f"Berita Jakarta current date: {current_date}")
    print(f"Berita Jakarta previous date: {previous_date}")

    rows = []
    lines = [line.strip() for line in page_text.splitlines() if line.strip()]

    current_commodity = None
    current_price = None
    current_unit = None

    for line in lines:
        if is_valid_commodity(line) and "Harga Sebelumnya" not in line:
            current_commodity = normalize_commodity_name(line)
            current_price = None
            current_unit = None
            continue

        if current_commodity and line.lower().startswith("rp"):
            price, unit = parse_price(line)
            if price is not None and price > 0:
                current_price = price
                current_unit = unit
            continue

        if current_commodity and "Harga Sebelumnya" in line:
            previous_price, previous_unit = parse_price(line)

            if current_price is not None and current_price > 0:
                rows.append({
                    "price_date": current_date,
                    "province_name": "DKI Jakarta",
                    "city_name": "Jakarta",
                    "commodity_name": current_commodity,
                    "unit": current_unit or previous_unit or "kg",
                    "price": current_price,
                    "source": "Berita Jakarta Info Pangan",
                })

            if previous_price is not None and previous_price > 0:
                rows.append({
                    "price_date": previous_date,
                    "province_name": "DKI Jakarta",
                    "city_name": "Jakarta",
                    "commodity_name": current_commodity,
                    "unit": previous_unit or current_unit or "kg",
                    "price": previous_price,
                    "source": "Berita Jakarta Info Pangan",
                })

            current_commodity = None
            current_price = None
            current_unit = None

    print(f"Berita Jakarta rows before final cleaning: {len(rows)}")
    return rows


def clean_scraped_rows(rows):
    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError(
            "No real scraped rows extracted. Check files in data/source_pages."
        )

    df["commodity_name"] = df["commodity_name"].apply(normalize_commodity_name)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df = df.dropna(subset=["price_date", "commodity_name", "price"])
    df = df[df["price"] > 0]

    df = df[df["commodity_name"].apply(is_valid_commodity)]

    # Prefer Info Pangan Jakarta when duplicate commodity exists,
    # otherwise keep Berita Jakarta as fallback/additional real source.
    source_priority = {
        "Info Pangan Jakarta": 1,
        "Berita Jakarta Info Pangan": 2,
    }

    df["source_priority"] = df["source"].map(source_priority).fillna(99)

    df = df.sort_values(["commodity_name", "source_priority"])

    df = df.drop_duplicates(
        subset=[
            "price_date",
            "province_name",
            "city_name",
            "commodity_name",
        ],
        keep="first",
    )

    df = df.drop(columns=["source_priority"])

    df = df.sort_values(["commodity_name"]).reset_index(drop=True)

    return df


def main():
    all_rows = []

    # Source 1: rendered IPJ
    try:
        all_rows.extend(scrape_ipj_rendered())
    except Exception as e:
        print(f"IPJ scraping failed: {e}")

    # Source 2: static Berita Jakarta fallback/additional source
    try:
        all_rows.extend(scrape_berita_jakarta_static())
    except Exception as e:
        print(f"Berita Jakarta scraping failed: {e}")

    df = clean_scraped_rows(all_rows)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nSaved real scraped data to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print("Sources:")
    print(df["source"].value_counts())
    print(df.head(30))


if __name__ == "__main__":
    main()