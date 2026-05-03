import os
import re
import json
from pathlib import Path

import pandas as pd
import requests


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
OUTPUT_PATH = DATA_DIR / "inflation_region.csv"

DEBUG_DIR = DATA_DIR / "source_pages" / "bps_inflation"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

YOY_URL = "https://www.bps.go.id/id/statistics-table/2/MjI2MyMy/inflasi-year-on-year--maret-2026.html"
MTM_URL = "https://www.bps.go.id/id/statistics-table/2/MSMy/inflasi--umum-.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
}


PROVINCE_CODE_MAP = {
    "11": "Aceh",
    "12": "Sumatera Utara",
    "13": "Sumatera Barat",
    "14": "Riau",
    "15": "Jambi",
    "16": "Sumatera Selatan",
    "17": "Bengkulu",
    "18": "Lampung",
    "19": "Kepulauan Bangka Belitung",
    "21": "Kepulauan Riau",
    "31": "DKI Jakarta",
    "32": "Jawa Barat",
    "33": "Jawa Tengah",
    "34": "DI Yogyakarta",
    "35": "Jawa Timur",
    "36": "Banten",
    "51": "Bali",
    "52": "Nusa Tenggara Barat",
    "53": "Nusa Tenggara Timur",
    "61": "Kalimantan Barat",
    "62": "Kalimantan Tengah",
    "63": "Kalimantan Selatan",
    "64": "Kalimantan Timur",
    "65": "Kalimantan Utara",
    "71": "Sulawesi Utara",
    "72": "Sulawesi Tengah",
    "73": "Sulawesi Selatan",
    "74": "Sulawesi Tenggara",
    "75": "Gorontalo",
    "76": "Sulawesi Barat",
    "81": "Maluku",
    "82": "Maluku Utara",
    "91": "Papua Barat",
    "92": "Papua Barat Daya",
    "94": "Papua",
    "95": "Papua Selatan",
    "96": "Papua Tengah",
    "97": "Papua Pegunungan",
    "99": "Indonesia",
}


def clean_text(value):
    if pd.isna(value):
        return ""

    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_province_label(label):
    text = clean_text(label).upper()
    text = text.replace("PROV. ", "")
    text = text.replace("PROV ", "")
    text = text.replace("PROVINSI ", "")

    manual = {
        "ACEH": "Aceh",
        "SUMATERA UTARA": "Sumatera Utara",
        "SUMATERA BARAT": "Sumatera Barat",
        "RIAU": "Riau",
        "JAMBI": "Jambi",
        "SUMATERA SELATAN": "Sumatera Selatan",
        "BENGKULU": "Bengkulu",
        "LAMPUNG": "Lampung",
        "KEPULAUAN BANGKA BELITUNG": "Kepulauan Bangka Belitung",
        "KEP. BANGKA BELITUNG": "Kepulauan Bangka Belitung",
        "KEPULAUAN RIAU": "Kepulauan Riau",
        "DKI JAKARTA": "DKI Jakarta",
        "JAWA BARAT": "Jawa Barat",
        "JAWA TENGAH": "Jawa Tengah",
        "DI YOGYAKARTA": "DI Yogyakarta",
        "D I YOGYAKARTA": "DI Yogyakarta",
        "JAWA TIMUR": "Jawa Timur",
        "BANTEN": "Banten",
        "BALI": "Bali",
        "NUSA TENGGARA BARAT": "Nusa Tenggara Barat",
        "NUSA TENGGARA TIMUR": "Nusa Tenggara Timur",
        "KALIMANTAN BARAT": "Kalimantan Barat",
        "KALIMANTAN TENGAH": "Kalimantan Tengah",
        "KALIMANTAN SELATAN": "Kalimantan Selatan",
        "KALIMANTAN TIMUR": "Kalimantan Timur",
        "KALIMANTAN UTARA": "Kalimantan Utara",
        "SULAWESI UTARA": "Sulawesi Utara",
        "SULAWESI TENGAH": "Sulawesi Tengah",
        "SULAWESI SELATAN": "Sulawesi Selatan",
        "SULAWESI TENGGARA": "Sulawesi Tenggara",
        "GORONTALO": "Gorontalo",
        "SULAWESI BARAT": "Sulawesi Barat",
        "MALUKU": "Maluku",
        "MALUKU UTARA": "Maluku Utara",
        "PAPUA BARAT": "Papua Barat",
        "PAPUA BARAT DAYA": "Papua Barat Daya",
        "PAPUA": "Papua",
        "PAPUA SELATAN": "Papua Selatan",
        "PAPUA TENGAH": "Papua Tengah",
        "PAPUA PEGUNUNGAN": "Papua Pegunungan",
        "INDONESIA": "Indonesia",
    }

    return manual.get(text, text.title())


def fetch_html(url, page_name):
    print(f"Fetching BPS page: {url}")

    response = requests.get(url, headers=HEADERS, timeout=90)
    response.raise_for_status()

    html = response.text

    debug_path = DEBUG_DIR / f"{page_name}.html"
    debug_path.write_text(html, encoding="utf-8", errors="ignore")

    print(f"Saved HTML debug file: {debug_path}")

    return html


def extract_bps_payload(html):
    """
    BPS Next.js page may fail to render the visible table,
    but the data is still embedded in escaped JSON inside the HTML.

    We extract:
    \"data\":{...},\"dict\":
    """

    start_marker = r'\"data\":{'
    end_marker = r',\"dict\":'

    start = html.find(start_marker)

    if start == -1:
        raise RuntimeError("Could not find escaped BPS data payload marker.")

    start += len(r'\"data\":')
    end = html.find(end_marker, start)

    if end == -1:
        raise RuntimeError("Could not find end of BPS data payload.")

    raw_escaped_json = html[start:end]

    # Convert escaped JSON string into normal JSON.
    decoded_json = raw_escaped_json.encode("utf-8").decode("unicode_escape")

    payload = json.loads(decoded_json)

    if payload.get("status") != "OK":
        raise RuntimeError(f"BPS payload status is not OK: {payload.get('status')}")

    if "datacontent" not in payload:
        raise RuntimeError("BPS payload has no datacontent.")

    return payload


def build_rows_from_payload(payload, metric_type):
    """
    BPS datacontent keys are concatenations of:
    vervar + var + turvar + tahun + turtahun

    Example for y-on-y:
    1100 + 2263 + 0 + 126 + 1
    = 1100226301261

    Instead of slicing keys manually, we generate valid combinations
    from BPS metadata and look them up in datacontent.
    """

    rows = []

    datacontent = payload.get("datacontent", {})
    vervars = payload.get("vervar", [])
    variables = payload.get("var", [])
    turvars = payload.get("turvar", [])
    years = payload.get("tahun", [])
    months = payload.get("turtahun", [])

    for vervar in vervars:
        region_code = str(vervar.get("val"))
        region_label = clean_text(vervar.get("label"))

        for variable in variables:
            var_code = str(variable.get("val"))
            variable_label = clean_text(variable.get("label"))

            for turvar in turvars:
                turvar_code = str(turvar.get("val"))

                for year_obj in years:
                    year_code = str(year_obj.get("val"))
                    year_label = str(year_obj.get("label"))

                    if not year_label.isdigit():
                        continue

                    year = int(year_label)

                    for month_obj in months:
                        month_code = str(month_obj.get("val"))
                        month_label = clean_text(month_obj.get("label"))

                        if month_label.lower() == "tahunan":
                            continue

                        try:
                            month_num = int(month_code)
                        except Exception:
                            continue

                        if month_num < 1 or month_num > 12:
                            continue

                        key = f"{region_code}{var_code}{turvar_code}{year_code}{month_code}"

                        if key not in datacontent:
                            continue

                        value = datacontent.get(key)

                        if value is None:
                            continue

                        period_month = f"{year}-{month_num:02d}-01"

                        if metric_type == "yoy":
                            province_name = normalize_province_label(region_label)

                            rows.append({
                                "period_month": period_month,
                                "province_name": province_name,
                                "city_name": None,
                                "inflation_mtm": None,
                                "inflation_yoy": float(value),
                                "cpi": None,
                                "source": "BPS Inflation Y-on-Y Province Table",
                            })

                        elif metric_type == "mtm":
                            # MTM table is city-level, e.g. Kota Inflasi.
                            # We infer province from the first two digits of the BPS region code,
                            # then aggregate city-level MTM into province average later.
                            province_code = region_code[:2]
                            province_name = PROVINCE_CODE_MAP.get(province_code)

                            if not province_name:
                                continue

                            rows.append({
                                "period_month": period_month,
                                "province_name": province_name,
                                "city_name": normalize_province_label(region_label),
                                "inflation_mtm": float(value),
                                "inflation_yoy": None,
                                "cpi": None,
                                "source": "BPS Inflation M-to-M City Table",
                            })

    return rows


def aggregate_mtm_city_to_province(df_mtm):
    if df_mtm.empty:
        return df_mtm

    grouped = (
        df_mtm
        .groupby(["period_month", "province_name"], as_index=False)
        .agg(
            inflation_mtm=("inflation_mtm", "mean"),
            cpi=("cpi", "max"),
            source=("source", lambda x: "BPS Inflation M-to-M City Table Aggregated to Province"),
        )
    )

    grouped["inflation_yoy"] = None

    return grouped[
        [
            "period_month",
            "province_name",
            "inflation_mtm",
            "inflation_yoy",
            "cpi",
            "source",
        ]
    ]


def main():
    yoy_html = fetch_html(YOY_URL, "bps_inflation_yoy")
    mtm_html = fetch_html(MTM_URL, "bps_inflation_mtm")

    yoy_payload = extract_bps_payload(yoy_html)
    mtm_payload = extract_bps_payload(mtm_html)

    yoy_rows = build_rows_from_payload(yoy_payload, metric_type="yoy")
    mtm_rows = build_rows_from_payload(mtm_payload, metric_type="mtm")

    df_yoy = pd.DataFrame(yoy_rows)
    df_mtm_city = pd.DataFrame(mtm_rows)

    print(f"YOY raw rows: {len(df_yoy)}")
    print(f"MTM city raw rows: {len(df_mtm_city)}")

    if df_yoy.empty and df_mtm_city.empty:
        raise RuntimeError("No inflation rows extracted from BPS embedded payload.")

    df_mtm_province = aggregate_mtm_city_to_province(df_mtm_city)

    # Prepare YOY at province level.
    if not df_yoy.empty:
        df_yoy_province = df_yoy[
            [
                "period_month",
                "province_name",
                "inflation_mtm",
                "inflation_yoy",
                "cpi",
                "source",
            ]
        ].copy()
    else:
        df_yoy_province = pd.DataFrame(
            columns=[
                "period_month",
                "province_name",
                "inflation_mtm",
                "inflation_yoy",
                "cpi",
                "source",
            ]
        )

    combined = pd.concat([df_yoy_province, df_mtm_province], ignore_index=True)

    final = (
        combined
        .groupby(["period_month", "province_name"], as_index=False)
        .agg(
            inflation_mtm=("inflation_mtm", "max"),
            inflation_yoy=("inflation_yoy", "max"),
            cpi=("cpi", "max"),
            source=("source", lambda x: " | ".join(sorted(set(x.dropna())))),
        )
    )

    final["inflation_mtm"] = pd.to_numeric(final["inflation_mtm"], errors="coerce").round(2)
    final["inflation_yoy"] = pd.to_numeric(final["inflation_yoy"], errors="coerce").round(2)

    final = final.sort_values(["period_month", "province_name"])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(OUTPUT_PATH, index=False)

    print(f"\nSaved scraped BPS inflation data to: {OUTPUT_PATH}")
    print(f"Rows: {len(final)}")
    print("\nMonth coverage:")
    print(final["period_month"].value_counts().sort_index())
    print("\nProvince count:", final["province_name"].nunique())
    print("\nSample:")
    print(final.head(40).to_string(index=False))


if __name__ == "__main__":
    main()