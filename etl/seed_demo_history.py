import random
from datetime import date, timedelta

import pandas as pd

from food_price_pipeline import (
    clean_data,
    load_dimensions,
    load_fact_food_price,
    calculate_alerts,
)


random.seed(42)


COMMODITIES = {
    "Beras IR. I (IR 64)": 15390,
    "Beras IR. II (IR 64) Ramos": 14445,
    "Beras IR. III (IR 64)": 13705,
    "Beras Setra I/Premium": 17145,
    "Cabe Merah Keriting": 50450,
    "Cabe Merah Besar (TW)": 58367,
    "Cabe Rawit Merah": 69200,
    "Cabe Rawit Hijau": 56517,
    "Bawang Merah": 51400,
    "Minyak Goreng (Kuning/Curah)": 22050,
}

REGIONS = {
    "DKI Jakarta": "Jakarta",
    "Jawa Barat": "Bandung",
    "Jawa Tengah": "Semarang",
    "Jawa Timur": "Surabaya",
    "Banten": "Serang",
}

START_DATE = date(2026, 4, 1)
DAYS = 30


def generate_demo_history() -> pd.DataFrame:
    rows = []

    for commodity_name, base_price in COMMODITIES.items():
        for province_name, city_name in REGIONS.items():
            current_price = base_price * random.uniform(0.90, 1.12)

            for i in range(DAYS):
                price_date = START_DATE + timedelta(days=i)

                daily_change = random.uniform(-0.025, 0.035)

                if "Cabe" in commodity_name:
                    daily_change += random.uniform(-0.02, 0.05)

                if province_name in ["DKI Jakarta", "Banten"]:
                    region_multiplier = 1.04
                elif province_name in ["Jawa Tengah"]:
                    region_multiplier = 0.96
                else:
                    region_multiplier = 1.00

                current_price = current_price * (1 + daily_change)
                final_price = round((current_price * region_multiplier) / 100) * 100

                rows.append({
                    "price_date": price_date.isoformat(),
                    "province_name": province_name,
                    "city_name": city_name,
                    "commodity_name": commodity_name,
                    "unit": "liter" if "Minyak" in commodity_name else "kg",
                    "price": int(final_price),
                    "source": "Historical Demo Scenario",
                })

    return pd.DataFrame(rows)


def main():
    print("Generating historical demo data...")
    df = generate_demo_history()

    print(f"Generated rows: {len(df)}")
    print("Cleaning demo data...")
    df = clean_data(df)

    print("Loading dimensions...")
    load_dimensions(df)

    print("Loading fact_food_price...")
    load_fact_food_price(df)

    print("Calculating alerts...")
    calculate_alerts()

    print("Historical demo data loaded successfully.")


if __name__ == "__main__":
    main()