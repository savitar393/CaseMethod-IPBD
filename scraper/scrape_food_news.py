import os
import re
from pathlib import Path
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus

import pandas as pd
import requests
import xml.etree.ElementTree as ET


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "news_food_price.csv"
DEBUG_DIR = DATA_DIR / "source_pages" / "food_news"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

QUERIES = [
    "harga beras Indonesia",
    "harga cabai Indonesia",
    "harga bawang Indonesia",
    "harga minyak goreng Indonesia",
    "harga daging ayam Indonesia",
    "harga telur ayam Indonesia",
    "harga pangan naik Indonesia",
    "stok pangan Indonesia",
    "pasokan pangan Indonesia",
    "distribusi pangan Indonesia",
    "operasi pasar pangan Indonesia",
    "Bulog beras harga pangan",
    "gagal panen pangan Indonesia",
    "banjir harga pangan Indonesia",
    "kekeringan harga pangan Indonesia",
    "harga BBM pangan Indonesia",
    "ongkos angkut pangan Indonesia",
    "biaya logistik pangan Indonesia",
    "distribusi pangan terganggu",
    "rantai pasok pangan Indonesia",
    "pelabuhan distribusi pangan Indonesia",
    "harga solar distribusi pangan",
    "rupiah melemah pangan impor"
]


COMMODITY_RULES = {
    "Beras": ["beras", "rice", "bulog", "gabah"],
    "Cabai": ["cabai", "cabe", "cabai merah", "cabai rawit", "chili"],
    "Bawang": ["bawang merah", "bawang putih", "bawang"],
    "Minyak Goreng": ["minyak goreng", "migor"],
    "Protein Hewani": ["daging ayam", "daging sapi", "telur ayam", "ayam", "sapi", "telur"],
    "Gula": ["gula pasir", "gula"],
    "Umum": ["pangan", "sembako", "inflasi pangan"],
}


EVENT_RULES = {
    "Price Increase": [
        "naik", "kenaikan", "melonjak", "meroket", "mahal", "tertinggi",
        "harga tinggi", "tembus"
    ],
    "Price Decrease": [
        "turun", "penurunan", "anjlok", "murah", "harga turun"
    ],
    "Supply Issue": [
        "stok menipis", "pasokan kurang", "pasokan terganggu", "kelangkaan",
        "langka", "distribusi terganggu", "hambatan distribusi"
    ],
    "Government Intervention": [
        "operasi pasar", "sidak", "stabilisasi", "bantuan pangan",
        "bulog", "het", "hpp", "subsidi", "intervensi"
    ],
    "Production/Harvest": [
        "panen", "panen raya", "gagal panen", "produksi", "petani", "lahan"
    ],
    "Weather/Climate": [
        "banjir", "kekeringan", "cuaca", "iklim", "hujan", "el nino",
        "la nina", "anomali cuaca"
    ],
    "Inflation/Macro": [
        "inflasi", "daya beli", "ihk", "volatile food"
    ],
    "Logistics/Fuel Cost": [
        "bbm", "solar", "pertalite", "pertamax", "ongkos angkut",
        "biaya logistik", "logistik", "transportasi", "pelabuhan",
        "pengiriman", "rantai pasok", "supply chain"
    ]
}


NEGATIVE_WORDS = [
    "naik", "melonjak", "meroket", "mahal", "langka", "krisis",
    "gagal panen", "banjir", "kekeringan", "terganggu", "menipis",
    "inflasi", "tembus", "tertinggi"
]

POSITIVE_WORDS = [
    "turun", "stabil", "terkendali", "aman", "cukup", "melimpah",
    "panen raya", "operasi pasar", "stabilisasi", "bantuan pangan"
]


def normalize_text(text: str) -> str:
    text = str(text or "").lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def classify_commodity(title: str) -> str:
    text = normalize_text(title)

    for group, keywords in COMMODITY_RULES.items():
        if any(keyword in text for keyword in keywords):
            return group

    return "Umum"


def classify_event(title: str):
    text = normalize_text(title)

    matched = []

    for category, keywords in EVENT_RULES.items():
        hit = [keyword for keyword in keywords if keyword in text]
        if hit:
            matched.extend(hit)
            return category, ", ".join(sorted(set(matched)))

    return "General News", ""


def classify_sentiment(title: str, event_category: str = None):
    text = normalize_text(title)

    negative_score = sum(1 for word in NEGATIVE_WORDS if word in text)
    positive_score = sum(1 for word in POSITIVE_WORDS if word in text)

    # Event-based fallback sentiment.
    # This helps when the event category is clear but sentiment keywords are weak.
    if event_category in ["Price Increase", "Supply Issue", "Weather/Climate", "Inflation/Macro"]:
        negative_score += 1

    if event_category in ["Price Decrease", "Government Intervention"]:
        positive_score += 1

    score = positive_score - negative_score

    if score > 0:
        return "Positive", score

    if score < 0:
        return "Negative", score

    return "Neutral", score


def parse_rss_date(value: str):
    if not value:
        return None

    try:
        return parsedate_to_datetime(value).replace(tzinfo=None)
    except Exception:
        return None


def scrape_google_news_rss(query: str, max_items: int = 20):
    encoded = quote_plus(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=id&gl=ID&ceid=ID:id"

    print(f"Fetching news: {query}")

    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    safe_name = re.sub(r"[^a-zA-Z0-9]+", "_", query.lower()).strip("_")
    debug_path = DEBUG_DIR / f"{safe_name}.xml"
    debug_path.write_text(response.text, encoding="utf-8", errors="ignore")

    root = ET.fromstring(response.content)
    channel = root.find("channel")

    rows = []

    if channel is None:
        return rows

    for item in channel.findall("item")[:max_items]:
        title = item.findtext("title")
        link = item.findtext("link")
        pub_date = item.findtext("pubDate")

        source_el = item.find("source")
        source_name = source_el.text if source_el is not None else "Google News"

        if not title:
            continue

        commodity_group = classify_commodity(title)
        event_category, matched_keywords = classify_event(title)
        sentiment_label, sentiment_score = classify_sentiment(title, event_category)

        rows.append({
            "title": title.strip(),
            "url": link,
            "source_name": source_name,
            "published_at": parse_rss_date(pub_date),
            "query_keyword": query,
            "commodity_group": commodity_group,
            "event_category": event_category,
            "sentiment_label": sentiment_label,
            "sentiment_score": sentiment_score,
            "matched_keywords": matched_keywords,
        })

    return rows


def main():
    all_rows = []

    for query in QUERIES:
        try:
            rows = scrape_google_news_rss(query)
            all_rows.extend(rows)
        except Exception as e:
            print(f"Failed query '{query}': {e}")

    df = pd.DataFrame(all_rows)

    if df.empty:
        raise RuntimeError("No news rows scraped.")

    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

    df = df.drop_duplicates(
        subset=["title", "published_at", "query_keyword"],
        keep="last"
    )

    df = df.sort_values("published_at", ascending=False, na_position="last")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved news data to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print("\nEvent categories:")
    print(df["event_category"].value_counts())
    print("\nSentiment:")
    print(df["sentiment_label"].value_counts())
    print(df.head(20))


if __name__ == "__main__":
    main()