import os
import re
from pathlib import Path
from datetime import date

import pandas as pd
from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "fuel_price_region.csv"

DEBUG_DIR = DATA_DIR / "source_pages" / "macro_cost"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://pertaminapatraniaga.com/page/harga-terbaru-bbm"

PRODUCTS_GASOLINE = [
    "Pertamax Turbo",
    "Pertamax Green 95",
    "Pertamax",
    "Pertalite",
]

MONTH_MAP = {
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

PROVINCE_NAME_MAP = {
    "Bangka-Belitung": "Kepulauan Bangka Belitung",
    "DKI Jakarta": "DKI Jakarta",
}


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_effective_date(text):
    text = clean_text(text).lower()

    match = re.search(
        r"update\s+per\s+tanggal\s+(\d{1,2})\s+([a-z]+)\s+(20\d{2})",
        text,
    )

    if not match:
        return date.today()

    day = int(match.group(1))
    month = MONTH_MAP.get(match.group(2))
    year = int(match.group(3))

    if not month:
        return date.today()

    return date(year, month, day)


def normalize_province(value):
    text = clean_text(value)
    text = text.replace("Prov.", "").replace("Provinsi", "").strip()

    if text.upper() == "DKI JAKARTA":
        text = "DKI Jakarta"

    return PROVINCE_NAME_MAP.get(text, text)


def parse_price(value):
    value = clean_text(value)

    if value in ["", "-", "–"]:
        return None

    value = value.replace(".", "").replace(",", "")

    try:
        return float(value)
    except Exception:
        return None


def is_price_token(value):
    value = clean_text(value)
    return value == "-" or bool(re.fullmatch(r"\d{1,3}(,\d{3})+", value))


def parse_gasoline_from_text(text, effective_date):
    lines = [clean_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    rows = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Province rows only. Skip FTZ rows so dashboard remains province-level.
        if not line.startswith("Prov. "):
            i += 1
            continue

        province_name = normalize_province(line)

        price_tokens = []
        j = i + 1

        while j < len(lines) and len(price_tokens) < 4:
            if is_price_token(lines[j]):
                price_tokens.append(lines[j])
                j += 1
            else:
                break

        if len(price_tokens) == 4:
            for product_name, price_token in zip(PRODUCTS_GASOLINE, price_tokens):
                price = parse_price(price_token)

                if price is None:
                    continue

                rows.append({
                    "effective_date": effective_date,
                    "province_name": province_name,
                    "product_name": product_name,
                    "fuel_category": "Gasoline",
                    "price_per_liter": price,
                    "source": "Pertamina Patra Niaga Fuel Price",
                })

            i = j
        else:
            i += 1

    return rows


def main():
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

        print(f"Opening Pertamina fuel price page: {URL}")

        page.goto(URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(10000)

        text = page.locator("body").inner_text()
        html = page.content()

        (DEBUG_DIR / "pertamina_fuel_price.txt").write_text(
            text,
            encoding="utf-8",
            errors="ignore",
        )

        (DEBUG_DIR / "pertamina_fuel_price.html").write_text(
            html,
            encoding="utf-8",
            errors="ignore",
        )

        page.screenshot(
            path=str(DEBUG_DIR / "pertamina_fuel_price.png"),
            full_page=True,
        )

        browser.close()

    effective_date = parse_effective_date(text)
    rows = parse_gasoline_from_text(text, effective_date)

    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError(
            "No fuel price rows extracted from rendered text. "
            "Check data/source_pages/macro_cost/pertamina_fuel_price.txt"
        )

    df = df.drop_duplicates(
        subset=["effective_date", "province_name", "product_name", "source"],
        keep="last",
    )

    df = df.sort_values(["province_name", "product_name"])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nSaved fuel price data to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print("Effective date:", df["effective_date"].iloc[0])
    print("Province count:", df["province_name"].nunique())
    print("Product count:", df["product_name"].nunique())
    print("\nProduct summary:")
    print(df.groupby("product_name")["province_name"].nunique().sort_values(ascending=False))
    print("\nSample:")
    print(df.head(50).to_string(index=False))


if __name__ == "__main__":
    main()