import os
from pathlib import Path

import pandas as pd


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
LOG_PATH = DATA_DIR / "source_pages" / "pihps_discovery" / "pihps_logs.csv"
OUTPUT_PATH = DATA_DIR / "source_pages" / "pihps_discovery" / "pihps_candidate_logs.csv"


STATIC_EXTENSIONS = (
    ".css", ".js", ".png", ".jpg", ".jpeg", ".svg", ".woff", ".woff2",
    ".ttf", ".eot", ".ico", ".map"
)


def is_candidate(row):
    url = str(row.get("url", "")).lower()
    body = str(row.get("body_sample", "")).lower()

    if any(url.endswith(ext) for ext in STATIC_EXTENSIONS):
        return False

    if any(token in url for token in [
        "tabelharga",
        "download",
        "get",
        "report",
        "grid",
        "json",
        "komoditas",
        "provinsi",
        "pasar",
        "harga",
    ]):
        return True

    if any(token in body for token in [
        "price",
        "harga",
        "komoditas",
        "provinsi",
        "download",
        "grid",
        "data",
    ]):
        return True

    return False


def main():
    df = pd.read_csv(LOG_PATH)

    candidates = df[df.apply(is_candidate, axis=1)].copy()

    cols = ["event", "status", "method", "url", "body_length", "body_sample"]
    cols = [c for c in cols if c in candidates.columns]

    candidates = candidates[cols].drop_duplicates()

    candidates.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved candidates to: {OUTPUT_PATH}")
    print(f"Candidate rows: {len(candidates)}")

    pd.set_option("display.max_colwidth", 180)
    print(candidates.head(80).to_string(index=False))


if __name__ == "__main__":
    main()