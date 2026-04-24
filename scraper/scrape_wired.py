import json
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from webdriver_manager.chrome import ChromeDriverManager


OUTPUT_PATH = Path("data/wired_articles.json")
MIN_ARTICLES = 100

START_URLS = [
    "https://www.wired.com/",
    "https://www.wired.com/category/science/",
    "https://www.wired.com/category/security/",
    "https://www.wired.com/category/business/",
    "https://www.wired.com/category/gear/",
]


def create_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    # Important: do not wait for every ad/image/script to fully load.
    options.page_load_strategy = "eager"

    # Optional but useful: reduce heavy media loading.
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    options.add_experimental_option("prefs", prefs)

    # Use headless only if needed.
    # options.add_argument("--headless=new")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Important: prevent Selenium from waiting forever.
    driver.set_page_load_timeout(35)
    driver.set_script_timeout(35)

    return driver


def safe_text(driver, selector):
    try:
        return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception:
        return ""


def safe_meta(driver, name):
    try:
        return driver.find_element(By.CSS_SELECTOR, f'meta[name="{name}"]').get_attribute("content").strip()
    except Exception:
        return ""


def collect_article_urls(driver):
    urls = set()

    for start_url in START_URLS:
        print(f"Opening: {start_url}")
        driver.get(start_url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        # Scroll several times so more articles load.
        for _ in range(8):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/story/"]')
            for link in links:
                href = link.get_attribute("href")
                if href:
                    full_url = urljoin("https://www.wired.com", href)
                    urls.add(full_url.split("?")[0])

            print(f"Collected URLs so far: {len(urls)}")

            if len(urls) >= MIN_ARTICLES:
                break

        if len(urls) >= MIN_ARTICLES:
            break

    return list(urls)[:MIN_ARTICLES]


def scrape_article(driver, url):
    try:
        driver.get(url)

    except TimeoutException:
        print(f"Page load timeout, stopping page load: {url}")
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    title = safe_text(driver, "h1")
    description = safe_meta(driver, "description")

    author = safe_meta(driver, "author")

    if not author:
        author = safe_text(driver, '[rel="author"]')

    if not author:
        author = "By Unknown"

    if not author.lower().startswith("by"):
        author = f"By {author}"

    return {
        "title": title,
        "url": url,
        "description": description,
        "author": author,
        "scraped_at": datetime.now().isoformat(),
        "source": "Wired.com",
    }


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    driver = create_driver()
    articles = []

    try:
        urls = collect_article_urls(driver)

        for i, url in enumerate(urls, start=1):
            try:
                article = scrape_article(driver, url)

                if article["title"] and article["url"]:
                    articles.append(article)
                    print(f"[{i}] Scraped: {article['title']}")

            except WebDriverException as e:
                print(f"Failed scraping {url}: {e}")

                try:
                    driver.quit()
                except Exception:
                    pass

                driver = create_driver()

            except Exception as e:
                print(f"Failed scraping {url}: {e}")

        session = {
            "session_id": f"wired_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            "articles_count": len(articles),
            "articles": articles,
        }

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(session, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(articles)} articles to {OUTPUT_PATH}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()