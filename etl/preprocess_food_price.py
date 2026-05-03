import os
import re
from pathlib import Path

import pandas as pd


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))

INPUT_PATH = DATA_DIR / "sample_food_price.csv"
BACKUP_PATH = DATA_DIR / "sample_food_price_before_preprocess.csv"
OUTPUT_PATH = DATA_DIR / "sample_food_price.csv"

ANALYSIS_OUTPUT_PATH = DATA_DIR / "sample_food_price_analysis.csv"
QUALITY_REPORT_PATH = DATA_DIR / "food_price_quality_report.csv"


REQUIRED_COLUMNS = [
    "price_date",
    "province_name",
    "city_name",
    "commodity_name",
    "unit",
    "price",
    "source",
]


INVALID_COMMODITY_NAMES = {
    "harga",
    "per kg",
    "per liter",
    "rp",
    "stabil",
    "",
    "nan",
}


PROVINCE_NAME_MAP = {
    "D.I. Yogyakarta": "DI Yogyakarta",
    "Daerah Istimewa Yogyakarta": "DI Yogyakarta",
    "Daerah Khusus Ibukota Jakarta": "DKI Jakarta",
    "Jakarta": "DKI Jakarta",
    "Bangka Belitung": "Kepulauan Bangka Belitung",
}


def clean_text(value):
    if pd.isna(value):
        return ""

    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_unit(value):
    text = clean_text(value).lower()

    replacements = {
        "rp./kg": "kg",
        "rp/kg": "kg",
        "rp. / kg": "kg",
        "per kg": "kg",
        "kilogram": "kg",
        "kgs": "kg",
        "kg.": "kg",
        "liter": "liter",
        "ltr": "liter",
        "lt": "liter",
        "butir": "butir",
        "ekor": "ekor",
        "250gr": "250gr",
        "3kg": "3kg",
        "kms": "kemasan",
    }

    return replacements.get(text, text or "kg")


def normalize_source(value):
    text = clean_text(value)

    if "PIHPS Grid" in text:
        return "PIHPS Grid Backfill"

    if "PIHPS Chart" in text:
        return "PIHPS Chart History"

    if "Berita Jakarta" in text:
        return "Berita Jakarta Info Pangan"

    if "Info Pangan Jakarta" in text:
        return "Info Pangan Jakarta"

    return text


def normalize_province(value):
    text = clean_text(value)
    return PROVINCE_NAME_MAP.get(text, text)


def normalize_city(value, province):
    text = clean_text(value)

    if not text:
        return province

    if text == "Nasional":
        return "Nasional"

    return text


def assign_commodity_group(name):
    text = clean_text(name).lower()

    if "beras" in text:
        return "Beras"
    if "cabai" in text or "cabe" in text:
        return "Cabai"
    if "bawang" in text:
        return "Bawang"
    if "daging" in text or "ayam" in text or "telur" in text:
        return "Protein Hewani"
    if "minyak" in text:
        return "Minyak Goreng"
    if "gula" in text:
        return "Gula"
    if "gas" in text or "elpiji" in text:
        return "Energi Rumah Tangga"
    if "ikan" in text:
        return "Ikan"
    if "garam" in text or "tepung" in text:
        return "Bahan Pokok Lainnya"

    return "Lainnya"


def assign_region_level(province_name):
    if province_name == "Nasional":
        return "Nasional"

    return "Provinsi/Kota"


def assign_source_type(source):
    if source == "PIHPS Grid Backfill":
        return "Province Historical"
    if source == "PIHPS Chart History":
        return "National Historical"
    if source in ["Info Pangan Jakarta", "Berita Jakarta Info Pangan"]:
        return "DKI Jakarta Snapshot"

    return "Other"


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df_raw = pd.read_csv(INPUT_PATH)

    # Backup current combined raw file before overwriting.
    df_raw.to_csv(BACKUP_PATH, index=False)

    report_rows = []

    def add_report(step, rows):
        report_rows.append({"step": step, "rows": rows})

    add_report("raw_input", len(df_raw))

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df_raw.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df_raw[REQUIRED_COLUMNS].copy()

    # Basic standardization
    df["price_date"] = pd.to_datetime(df["price_date"], errors="coerce")
    df["province_name"] = df["province_name"].apply(normalize_province)
    df["city_name"] = df.apply(
        lambda row: normalize_city(row["city_name"], row["province_name"]),
        axis=1,
    )
    df["commodity_name"] = df["commodity_name"].apply(clean_text)
    df["unit"] = df["unit"].apply(normalize_unit)
    df["source"] = df["source"].apply(normalize_source)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    add_report("after_standardization", len(df))

    # Remove invalid rows
    df = df.dropna(subset=["price_date", "price"])
    add_report("after_drop_missing_date_price", len(df))

    df = df[df["commodity_name"].str.lower().isin(INVALID_COMMODITY_NAMES) == False]
    add_report("after_remove_invalid_commodity_names", len(df))

    df = df[df["price"] >= 1000]
    add_report("after_remove_price_under_1000", len(df))

    df = df[df["price"] <= 1_000_000]
    add_report("after_remove_price_over_1000000", len(df))

    # Deduplicate
    df = df.sort_values(["source", "province_name", "commodity_name", "price_date"])

    before_dedup = len(df)

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

    add_report("after_deduplication", len(df))
    add_report("duplicates_removed", before_dedup - len(df))

    # Enriched version for analysis/reporting
    analysis_df = df.copy()
    analysis_df["commodity_group"] = analysis_df["commodity_name"].apply(assign_commodity_group)
    analysis_df["region_level"] = analysis_df["province_name"].apply(assign_region_level)
    analysis_df["source_type"] = analysis_df["source"].apply(assign_source_type)

    # Keep ETL-safe file with only original required columns.
    df["price_date"] = df["price_date"].dt.date.astype(str)
    df = df[REQUIRED_COLUMNS]

    analysis_df["price_date"] = analysis_df["price_date"].dt.date.astype(str)

    df.to_csv(OUTPUT_PATH, index=False)
    analysis_df.to_csv(ANALYSIS_OUTPUT_PATH, index=False)
    pd.DataFrame(report_rows).to_csv(QUALITY_REPORT_PATH, index=False)

    print(f"Preprocessed ETL-safe data saved to: {OUTPUT_PATH}")
    print(f"Analysis copy saved to: {ANALYSIS_OUTPUT_PATH}")
    print(f"Quality report saved to: {QUALITY_REPORT_PATH}")
    print(f"Final rows: {len(df)}")
    print("\nSource counts:")
    print(df["source"].value_counts())
    print("\nDate coverage:")
    print(df["price_date"].value_counts().sort_index().tail(20))


if __name__ == "__main__":
    main()