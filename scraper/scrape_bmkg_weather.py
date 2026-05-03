import os
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "weather_region.csv"
LOCATIONS_OUTPUT_PATH = DATA_DIR / "weather_locations.csv"

DEBUG_DIR = DATA_DIR / "source_pages" / "bmkg_weather"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

BMKG_API_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca"
WILAYAH_API_BASE = "https://wilayah.id/api"

MAX_PROVINCES = int(os.getenv("BMKG_MAX_PROVINCES", "40"))
REQUEST_DELAY = float(os.getenv("BMKG_REQUEST_DELAY", "1.2"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


PROVINCE_NAME_MAP = {
    "Nanggroe Aceh Darussalam": "Aceh",
    "D I Yogyakarta": "DI Yogyakarta",
    "Daerah Istimewa Yogyakarta": "DI Yogyakarta",
    "Dki Jakarta": "DKI Jakarta",
}


def clean_text(value):
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


def normalize_province(value):
    text = clean_text(value)
    text = " ".join(text.split())

    if not text:
        return ""

    text = text.title()

    if text.upper() == "DKI JAKARTA":
        text = "DKI Jakarta"

    return PROVINCE_NAME_MAP.get(text, text)


def to_float(value):
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def parse_datetime(value):
    value = clean_text(value)

    if not value:
        return None

    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            pass

    try:
        return pd.to_datetime(value, errors="coerce").to_pydatetime()
    except Exception:
        return None


def weather_risk_from_desc(desc, temp, humidity, wind_speed):
    text = clean_text(desc).lower()
    score = 0

    if "hujan" in text:
        score += 1

    if "lebat" in text or "petir" in text or "badai" in text:
        score += 2

    if temp is not None and temp >= 34:
        score += 1

    if humidity is not None and humidity >= 90:
        score += 1

    if wind_speed is not None and wind_speed >= 25:
        score += 1

    if score >= 3:
        return "High", score

    if score >= 1:
        return "Medium", score

    return "Low", score


def get_json(url):
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return response.json()


def wilayah_get(path):
    url = f"{WILAYAH_API_BASE}/{path}"
    return get_json(url).get("data", [])


def fetch_weather(adm4_code):
    params = {"adm4": adm4_code}

    response = requests.get(
        BMKG_API_URL,
        params=params,
        headers=HEADERS,
        timeout=60,
    )

    response.raise_for_status()

    debug_path = DEBUG_DIR / f"bmkg_{adm4_code}.json"
    debug_path.write_text(response.text, encoding="utf-8", errors="ignore")

    return response.json()


def has_valid_weather(data):
    try:
        return bool(data.get("data", [])[0].get("cuaca"))
    except Exception:
        return False


def choose_candidate_regencies(regencies):
    if not regencies:
        return []

    # Prefer Kota / provincial capital-like entries if available.
    cities = [r for r in regencies if "kota" in r.get("name", "").lower()]
    others = [r for r in regencies if r not in cities]

    return (cities + others)[:5]


def discover_representative_locations():
    print("Discovering representative ADM4 locations using wilayah.id...")

    provinces = wilayah_get("provinces.json")[:MAX_PROVINCES]
    selected = []

    for province in provinces:
        province_code = province["code"]
        province_name = normalize_province(province["name"])

        print(f"\nProvince: {province_name} ({province_code})")

        try:
            regencies = choose_candidate_regencies(
                wilayah_get(f"regencies/{province_code}.json")
            )
        except Exception as e:
            print(f"  Failed to get regencies: {e}")
            continue

        found = False

        for regency in regencies:
            regency_code = regency["code"]
            regency_name = regency["name"]

            try:
                districts = wilayah_get(f"districts/{regency_code}.json")[:5]
            except Exception:
                continue

            for district in districts:
                district_code = district["code"]
                district_name = district["name"]

                try:
                    villages = wilayah_get(f"villages/{district_code}.json")[:5]
                except Exception:
                    continue

                for village in villages:
                    adm4_code = village["code"]
                    village_name = village["name"]

                    try:
                        print(f"  Testing ADM4 {adm4_code} - {village_name}")
                        data = fetch_weather(adm4_code)
                        time.sleep(REQUEST_DELAY)

                        if has_valid_weather(data):
                            selected.append({
                                "province_name": province_name,
                                "province_code": province_code,
                                "regency_name": regency_name,
                                "district_name": district_name,
                                "village_name": village_name,
                                "adm4_code": adm4_code,
                            })

                            print(f"  OK: {adm4_code} {village_name}")
                            found = True
                            break

                    except Exception as e:
                        print(f"  Failed {adm4_code}: {e}")
                        time.sleep(REQUEST_DELAY)

                if found:
                    break

            if found:
                break

        if not found:
            print(f"  No valid BMKG weather location found for {province_name}")

    locations_df = pd.DataFrame(selected)
    locations_df.to_csv(LOCATIONS_OUTPUT_PATH, index=False)

    print(f"\nSaved discovered locations to: {LOCATIONS_OUTPUT_PATH}")
    print(locations_df.to_string(index=False))

    return selected


def parse_weather_rows(data, fallback_location):
    rows = []

    lokasi = data.get("lokasi", {}) or {}

    province_name = normalize_province(
        lokasi.get("provinsi") or fallback_location["province_name"]
    )

    city_name = (
        clean_text(lokasi.get("kotkab"))
        or fallback_location.get("regency_name")
        or province_name
    )

    adm4_code = fallback_location["adm4_code"]

    forecast_blocks = data.get("data", [])

    for block in forecast_blocks:
        daily_groups = block.get("cuaca", [])

        for daily_group in daily_groups:
            if not isinstance(daily_group, list):
                continue

            for item in daily_group:
                utc_dt = parse_datetime(item.get("utc_datetime"))
                local_dt = parse_datetime(item.get("local_datetime"))

                dt = utc_dt or local_dt

                if dt is None:
                    continue

                temp = to_float(item.get("t"))
                humidity = to_float(item.get("hu"))
                wind_speed = to_float(item.get("ws"))

                weather_desc = clean_text(item.get("weather_desc"))
                weather_code = clean_text(
                    item.get("weather")
                    or item.get("weather_code")
                    or item.get("kode_cuaca")
                    or weather_desc
                )

                risk_label, risk_score = weather_risk_from_desc(
                    weather_desc,
                    temp,
                    humidity,
                    wind_speed,
                )

                rows.append({
                    "weather_datetime_utc": dt,
                    "forecast_date": dt.date(),
                    "province_name": province_name,
                    "weather_code": weather_code,
                    "weather_desc": weather_desc,
                    "temperature_min": temp,
                    "temperature_max": temp,
                    "temperature_avg": temp,
                    "humidity_min": humidity,
                    "humidity_max": humidity,
                    "humidity_avg": humidity,
                    "wind_direction": clean_text(item.get("wd")),
                    "wind_speed": wind_speed,
                    "weather_risk_label": risk_label,
                    "weather_risk_score": risk_score,
                    "source": "BMKG Public Weather Forecast API",
                })

    return rows


def main():
    locations = discover_representative_locations()

    if not locations:
        raise RuntimeError("No valid BMKG ADM4 locations discovered.")

    all_rows = []

    for location in locations:
        adm4_code = location["adm4_code"]

        try:
            print(f"\nFetching final BMKG weather for {location['province_name']} - {adm4_code}")
            data = fetch_weather(adm4_code)
            rows = parse_weather_rows(data, location)
            print(f"Rows extracted: {len(rows)}")
            all_rows.extend(rows)
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"Failed final fetch for {adm4_code}: {e}")

    df = pd.DataFrame(all_rows)

    if df.empty:
        raise RuntimeError("No BMKG weather rows extracted.")

    df = df.drop_duplicates(
        subset=[
            "weather_datetime_utc",
            "province_name",
            "weather_code",
            "source",
        ],
        keep="last",
    )

    df = df.sort_values(["forecast_date", "province_name", "weather_datetime_utc"])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nSaved BMKG weather data to: {OUTPUT_PATH}")
    print(f"Rows: {len(df)}")
    print("\nDate coverage:")
    print(df["forecast_date"].value_counts().sort_index())
    print("\nProvince count:", df["province_name"].nunique())
    print("\nRisk summary:")
    print(df["weather_risk_label"].value_counts())
    print("\nSample:")
    print(df.head(30).to_string(index=False))


if __name__ == "__main__":
    main()