from playwright.sync_api import sync_playwright


URL = "https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisionalDaerah"


def main():
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

        page.goto(URL, wait_until="networkidle", timeout=90000)
        page.wait_for_timeout(5000)

        print("\nINPUTS")
        inputs = page.locator("input").evaluate_all("""
            els => els.map(e => ({
                type: e.type,
                id: e.id,
                name: e.name,
                placeholder: e.placeholder,
                value: e.value,
                class: e.className
            }))
        """)
        for item in inputs:
            print(item)

        print("\nSELECTS")
        selects = page.locator("select").evaluate_all("""
            els => els.map(e => ({
                id: e.id,
                name: e.name,
                value: e.value,
                class: e.className,
                options: Array.from(e.options).slice(0, 10).map(o => ({
                    text: o.text,
                    value: o.value
                }))
            }))
        """)
        for item in selects:
            print(item)

        print("\nBUTTONS")
        buttons = page.locator("button, a").evaluate_all("""
            els => els.map(e => ({
                tag: e.tagName,
                id: e.id,
                text: e.innerText,
                href: e.href,
                class: e.className
            })).filter(x => x.text || x.href)
        """)
        for item in buttons[:100]:
            print(item)

        browser.close()


if __name__ == "__main__":
    main()