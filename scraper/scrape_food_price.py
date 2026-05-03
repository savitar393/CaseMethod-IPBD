import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path


DATA_GO_URL = "https://data.go.id/dataset/dataset/harga-rata-rata-bahan-pangan"
SATUDATA_JAKARTA_URL = "https://satudata.jakarta.go.id/open-data/detail/harga-rata-rata-bahan-pangan"

OUTPUT_DIR = Path("data/source_pages")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
}


def fetch_page(url: str, output_name: str) -> str | None:
    print(f"\nFetching: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        print(f"Status code: {response.status_code}")

        if response.status_code != 200:
            print(f"Failed to fetch page: HTTP {response.status_code}")
            return None

        html = response.text
        output_path = OUTPUT_DIR / output_name
        output_path.write_text(html, encoding="utf-8")

        print(f"Saved HTML to: {output_path}")
        return html

    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None


def discover_links(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        href = urljoin(base_url, a["href"])

        text_lower = text.lower()
        href_lower = href.lower()

        interesting = (
            "download" in text_lower
            or "csv" in text_lower
            or "json" in text_lower
            or "xlsx" in text_lower
            or "satudata" in href_lower
            or ".csv" in href_lower
            or ".json" in href_lower
            or ".xlsx" in href_lower
        )

        if interesting:
            links.append({"text": text, "url": href})

    return links


def main():
    pages = [
        ("data_go.html", DATA_GO_URL),
        ("satudata_jakarta.html", SATUDATA_JAKARTA_URL),
    ]

    all_links = []

    for output_name, url in pages:
        html = fetch_page(url, output_name)

        if html:
            links = discover_links(html, url)

            print("\nDiscovered links:")
            if not links:
                print("No direct download links found in static HTML.")
            else:
                for item in links:
                    print(f"- {item['text']} -> {item['url']}")
                    all_links.append(item)

    print("\nSummary:")
    print(f"Total discovered links: {len(all_links)}")

    if len(all_links) == 0:
        print(
            "\nLikely reason: the dataset page is rendered dynamically by JavaScript. "
            "Next step: inspect the Network tab or use a direct API/download endpoint."
        )


if __name__ == "__main__":
    main()