import os
from pathlib import Path

import pandas as pd


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))

INPUT_FILES = [
    DATA_DIR / "sample_food_price.csv",
    # DATA_DIR / "pihps_chart_history.csv",
    DATA_DIR / "pihps_grid_history.csv",
]

OUTPUT_FILE = DATA_DIR / "sample_food_price.csv"


def main():
    frames = []

    for path in INPUT_FILES:
        if path.exists():
            print(f"Reading: {path}")
            df = pd.read_csv(path)
            frames.append(df)
        else:
            print(f"Skipping missing file: {path}")

    if not frames:
        raise RuntimeError("No input files found.")

    combined = pd.concat(frames, ignore_index=True)

    required_cols = [
        "price_date",
        "province_name",
        "city_name",
        "commodity_name",
        "unit",
        "price",
        "source",
    ]

    combined = combined[required_cols]

    combined["commodity_name"] = combined["commodity_name"].astype(str).str.strip()
    combined["province_name"] = combined["province_name"].astype(str).str.strip()
    combined["city_name"] = combined["city_name"].astype(str).str.strip()
    combined["unit"] = combined["unit"].astype(str).str.strip()
    combined["source"] = combined["source"].astype(str).str.strip()
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")

    combined = combined.dropna(subset=["price_date", "commodity_name", "price"])
    combined = combined[combined["price"] >= 1000]

    invalid_names = {
        "harga",
        "per kg",
        "per liter",
        "rp",
        "stabil",
    }

    combined = combined[
        ~combined["commodity_name"].str.lower().isin(invalid_names)
    ]

    combined = combined.drop_duplicates(
        subset=[
            "price_date",
            "province_name",
            "city_name",
            "commodity_name",
            "source",
        ],
        keep="last",
    )

    combined = combined.sort_values(
        ["source", "province_name", "commodity_name", "price_date"]
    )

    combined.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved combined real data to: {OUTPUT_FILE}")
    print(f"Rows: {len(combined)}")
    print(combined["source"].value_counts())
    print(combined["price_date"].value_counts().sort_index())


if __name__ == "__main__":
    main()