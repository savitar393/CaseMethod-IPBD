from pathlib import Path


JS_DIR = Path("/opt/airflow/data/source_pages/panelharga_js")

TERMS = [
    "api-panelhargav2",
    "harga-peta-provinsi",
    "harga-pangan-informasi",
    "x-api-key",
    "apikey",
    "api_key",
    "authorization",
    "bearer",
    "CryptoJS",
    "encrypt",
    "decrypt",
    "sha256",
    "md5",
]


def show_context(text: str, index: int, window: int = 350):
    start = max(0, index - window)
    end = min(len(text), index + window)
    return text[start:end].replace("\n", " ")


def main():
    if not JS_DIR.exists():
        raise FileNotFoundError(f"JS directory not found: {JS_DIR}")

    found_any = False

    for path in JS_DIR.glob("*.js"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        lower_text = text.lower()

        for term in TERMS:
            idx = lower_text.find(term.lower())

            if idx != -1:
                found_any = True
                print("\n" + "=" * 100)
                print(f"FILE: {path.name}")
                print(f"TERM: {term}")
                print("-" * 100)
                print(show_context(text, idx))

    if not found_any:
        print("No target terms found in saved JS files.")


if __name__ == "__main__":
    main()