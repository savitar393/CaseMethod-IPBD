import re
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


URL = "https://sp2kp.kemendag.go.id/"
OUTPUT_PATH = Path("data/sample_food_price.csv")


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
}


def clean_price(value: str):
    if value is None:
        return None

    value = str(value)
    value = value.replace("Rp", "")
    value = value.replace(".", "")
    value = value.replace(",", "")
    value = value.strip()

    digits = re.findall(r"\d+", value)

    if not digits:
        return None

    return int("".join(digits))


def scrape_with_pandas_read_html():
    """
    First attempt:
    Use pandas to read HTML tables directly.
    This works if the table is available in the server-rendered HTML.
    """
    tables = pd.read_html(URL)

    print(f"Found {len(tables)} HTML tables")

    for i, table in enumerate(tables):
        print(f"\nTable {i}")
        print(table.head())
        print(table.columns)

    return tables


def scrape_with_beautifulsoup():
    """
    Fallback attempt:
    Extract visible text from the page and look for commodity-price-like lines.
    This is less precise but useful for debugging.
    """
    response = requests.get(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text("\n", strip=True)

    debug_path = Path("data/source_pages/sp2kp_text.txt")
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    debug_path.write_text(text, encoding="utf-8")

    print(f"Saved extracted SP2KP text to {debug_path}")

    return text


def normalize_table_to_food_price(table: pd.DataFrame) -> pd.DataFrame:
    """
    Try to normalize SP2KP table into our pipeline format:
    price_date, province_name, city_name, commodity_name, unit, price, source
    """

    df = table.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    print("Normalized columns:", df.columns.tolist())

    # Try to guess columns
    commodity_col = None
    unit_col = None
    price_col = None

    for col in df.columns:
        if any(keyword in col for keyword in ["komoditas", "barang", "bahan", "nama"]):
            commodity_col = col
        if any(keyword in col for keyword in ["satuan", "unit"]):
            unit_col = col
        if any(keyword in col for keyword in ["harga", "hnt", "nasional"]):
            price_col = col

    if commodity_col is None:
        # fallback: first column
        commodity_col = df.columns[0]

    if price_col is None:
        # fallback: find column with most numeric-looking values
        best_col = None
        best_count = 0

        for col in df.columns:
            count = df[col].astype(str).str.contains(r"\d", regex=True).sum()
            if count > best_count and col != commodity_col:
                best_count = count
                best_col = col

        price_col = best_col

    if price_col is None:
        raise ValueError("Could not detect price column from table.")

    result = pd.DataFrame()
    result["price_date"] = date.today().isoformat()
    result["province_name"] = "Nasional"
    result["city_name"] = "Nasional"
    result["commodity_name"] = df[commodity_col].astype(str).str.strip()
    result["unit"] = df[unit_col].astype(str).str.strip() if unit_col else "kg"
    result["price"] = df[price_col].apply(clean_price)
    result["source"] = "SP2KP Kemendag"

    result = result.dropna(subset=["commodity_name", "price"])
    result = result[result["commodity_name"].str.len() > 2]

    return result


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    try:
        tables = scrape_with_pandas_read_html()

        if not tables:
            raise ValueError("No HTML tables found.")

        # Try every table until one can be normalized
        for table in tables:
            try:
                normalized = normalize_table_to_food_price(table)

                if len(normalized) > 0:
                    normalized.to_csv(OUTPUT_PATH, index=False)
                    print(f"\nSaved scraped data to {OUTPUT_PATH}")
                    print(normalized.head())
                    print(f"Rows: {len(normalized)}")
                    return

            except Exception as e:
                print(f"Could not normalize this table: {e}")

        raise ValueError("No suitable table found.")

    except Exception as e:
        print(f"\nTable scraping failed: {e}")
        print("Trying BeautifulSoup fallback for debugging...")
        scrape_with_beautifulsoup()
        print(
            "\nNo CSV was created. Check data/source_pages/sp2kp_text.txt "
            "to inspect the page content."
        )


if __name__ == "__main__":
    main()