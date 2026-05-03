import os
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
DEBUG_DIR = DATA_DIR / "source_pages" / "panelharga_js"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://panelharga.badanpangan.go.id/"


def safe_name(url: str) -> str:
    parsed = urlparse(url)
    name = f"{parsed.netloc}_{parsed.path}".replace("/", "_")
    name = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return name[:150]


def main():
    saved = []

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

        def handle_response(response):
            url = response.url
            content_type = response.headers.get("content-type", "")

            if "javascript" not in content_type.lower():
                return

            if "panelharga.badanpangan.go.id" not in url:
                return

            try:
                body = response.text()
            except Exception:
                return

            output_path = DEBUG_DIR / f"{safe_name(url)}.js"
            output_path.write_text(body, encoding="utf-8", errors="ignore")
            saved.append(str(output_path))

        page.on("response", handle_response)

        print(f"Opening: {TARGET_URL}")
        page.goto(TARGET_URL, wait_until="networkidle", timeout=90000)
        page.wait_for_timeout(8000)

        browser.close()

    print("Saved JS files:")
    for item in saved:
        print(item)


if __name__ == "__main__":
    main()