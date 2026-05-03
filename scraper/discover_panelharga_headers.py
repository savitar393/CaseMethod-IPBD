import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
DEBUG_DIR = DATA_DIR / "source_pages" / "panelharga_headers"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://panelharga.badanpangan.go.id/"


def is_api_url(url: str) -> bool:
    return "api-panelhargav2.badanpangan.go.id" in url


def main():
    request_logs = []
    response_logs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        page = context.new_page()

        def handle_request(request):
            url = request.url

            if not is_api_url(url):
                return

            request_logs.append({
                "url": url,
                "method": request.method,
                "headers": request.headers,
                "post_data": request.post_data,
            })

        def handle_response(response):
            url = response.url

            if not is_api_url(url):
                return

            row = {
                "url": url,
                "status": response.status,
                "headers": response.headers,
            }

            try:
                body = response.text()
                row["body_sample"] = body[:1000]
            except Exception as e:
                row["body_error"] = str(e)

            response_logs.append(row)

        page.on("request", handle_request)
        page.on("response", handle_response)

        print(f"Opening: {TARGET_URL}")
        page.goto(TARGET_URL, wait_until="networkidle", timeout=90000)
        page.wait_for_timeout(8000)

        # Save browser storage too
        local_storage = page.evaluate("""
            () => {
                const data = {};
                for (let i = 0; i < localStorage.length; i++) {
                    const key = localStorage.key(i);
                    data[key] = localStorage.getItem(key);
                }
                return data;
            }
        """)

        session_storage = page.evaluate("""
            () => {
                const data = {};
                for (let i = 0; i < sessionStorage.length; i++) {
                    const key = sessionStorage.key(i);
                    data[key] = sessionStorage.getItem(key);
                }
                return data;
            }
        """)

        page.screenshot(
            path=str(DEBUG_DIR / "panelharga_home.png"),
            full_page=True
        )

        browser.close()

    (DEBUG_DIR / "api_requests.json").write_text(
        json.dumps(request_logs, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    (DEBUG_DIR / "api_responses.json").write_text(
        json.dumps(response_logs, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    (DEBUG_DIR / "local_storage.json").write_text(
        json.dumps(local_storage, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    (DEBUG_DIR / "session_storage.json").write_text(
        json.dumps(session_storage, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"Saved logs to: {DEBUG_DIR}")
    print(f"API requests captured: {len(request_logs)}")
    print(f"API responses captured: {len(response_logs)}")

    for req in request_logs:
        print("\nURL:", req["url"])
        print("Method:", req["method"])

        interesting_headers = {
            k: v for k, v in req["headers"].items()
            if any(token in k.lower() for token in [
                "api",
                "key",
                "auth",
                "token",
                "bearer",
                "x-",
            ])
        }

        print("Interesting headers:")
        print(json.dumps(interesting_headers, indent=2))


if __name__ == "__main__":
    main()