from config import (
    URLS,
    WAYBACK_ENDPOINT,
    BILL_C59_ROYAL_ASSENT_DATE,
    LINK_CSV_FIELDS,
    WAYBACK_PREFIX,
)
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import time
import os
import random

driver = webdriver.Chrome()
driver.implicitly_wait(5)
date = datetime.now()
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0; +https://example.org/contact)"
    }
)


def fetch_wayback_url(url, max_retries=5):
    for attempt in range(max_retries):
        try:
            payload = {
                "url": url,
                "output": "json",
                "to": BILL_C59_ROYAL_ASSENT_DATE,
                "limit": -1,
            }
            res = session.get(WAYBACK_ENDPOINT, params=payload, timeout=25)
            res.raise_for_status()

            if res.status_code == 200:
                data = res.json()
                header = data[0]
                values = data[1]
                record = dict(zip(header, values))
                return f"{WAYBACK_PREFIX}/{record['timestamp']}/{record['original']}"
            elif res.status_code == 429:
                wait = 2**attempt + random.uniform(0, 3)
                print(f"Rate limited (429). Waiting {wait:.1f}s before retrying...")
                time.sleep(wait)
            else:
                print(f"Unexpected {res.status_code}: {res.text}")
                break
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            time.sleep(2)


def append_csv(new_rows, is_archive):
    link_csv = (
        "output/links/wayback_article_links.csv"
        if is_archive
        else "output/links/article_links.csv"
    )

    with open(link_csv, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=LINK_CSV_FIELDS)
        if os.stat(link_csv).st_size == 0:
            writer.writeheader()
        writer.writerows(new_rows)


def fetch_suncor_article_urls(url, is_archive):
    driver.get(url)
    link_elems = driver.find_elements(By.CLASS_NAME, "download-embed__link")
    show_all_btn = driver.find_element(By.CLASS_NAME, "accordion-group__button")
    driver.execute_script("arguments[0].scrollIntoView(true);", show_all_btn)
    driver.execute_script("arguments[0].click();", show_all_btn)
    accordion_group = driver.find_element(By.CLASS_NAME, "accordion-group__items")
    link_elems.extend(accordion_group.find_elements(By.TAG_NAME, "a"))
    new_rows = [
        {
            "Organization": "Suncor Energy",
            "Link": link_elem.get_attribute("href"),
            "Date Scraped": date.strftime("%m/%d/%Y"),
            "Type": "pdf",
        }
        for link_elem in link_elems
    ]
    append_csv(new_rows, is_archive)


def fetch_pembina_article_urls(url, is_archive):
    driver.get(url)
    link_elems = driver.find_elements(By.CLASS_NAME, "news-item")
    new_rows = [
        {
            "Organization": "Pembina Pipeline",
            "Link": link_elem.get_attribute("href"),
            "Date Scraped": date.strftime("%m/%d/%Y"),
            "Type": "html",
        }
        for link_elem in link_elems
    ]
    append_csv(new_rows, is_archive)


def fetch_imperial_article_urls(url, is_archive):
    driver.get(url)
    if not is_archive:
        close_popup_btn = driver.find_element(By.CLASS_NAME, "fancybox-close-small")
        close_popup_btn.click()
    year_filter = driver.find_element(By.ID, "newsYear")
    year_options = year_filter.find_elements(By.TAG_NAME, "option")
    new_rows = []
    for year in year_options:
        driver.execute_script(
            """
        var select = arguments[0];
        var value = arguments[1];
        select.value = value;
        select.dispatchEvent(new Event('change'));
        """,
            year_filter,
            year.get_attribute("value"),
        )
        time.sleep(2)
        while True:
            try:
                next_page_btn = driver.find_element(By.CLASS_NAME, "pager-next")
                if "pager-disabled" in next_page_btn.get_attribute("class"):
                    break
                driver.execute_script(
                    "arguments[0].scrollIntoView(true);", next_page_btn
                )
                driver.execute_script("arguments[0].click()", next_page_btn)
            except Exception as ex:
                print(f"An error occurred while trying to scrape {url}: {ex.args}")
        year_link_elems = driver.find_elements(By.CLASS_NAME, "module_headline-link")
        new_rows.extend(
            [
                {
                    "Organization": "Imperial Oil",
                    "Link": link_elem.get_attribute("href"),
                    "Date Scraped": date.strftime("%m/%d/%Y"),
                    "Type": "html",
                }
                for link_elem in year_link_elems
            ]
        )
    append_csv(new_rows, is_archive)


def fetch_enbridge_article_urls(url, is_archive, is_root):
    driver.get(url)
    if not is_archive and is_root:
        try:
            shadow_host = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "usercentrics-root"))
            )
            shadow_root = driver.execute_script(
                "return arguments[0].shadowRoot", shadow_host
            )

            accept_cookies = shadow_root.find_element(
                By.CSS_SELECTOR, "button[data-testid='uc-accept-all-button']"
            )
            accept_cookies.click()
        except:
            print(f"An error occurred while trying to accept site cookies for {url}")
    news_items_div = driver.find_element(By.CLASS_NAME, "news-items")
    link_elems = news_items_div.find_elements(By.TAG_NAME, "a")
    new_rows = [
        {
            "Organization": "Enbridge",
            "Link": link_elem.get_attribute("href"),
            "Date Scraped": date.strftime("%m/%d/%Y"),
            "Type": "html",
        }
        for link_elem in link_elems
    ]
    if is_root:
        year_tab_div = driver.find_element(By.CLASS_NAME, "year-tabs")
        year_tabs = year_tab_div.find_elements(By.TAG_NAME, "a")
        year_urls = [year_tab.get_attribute("href") for year_tab in year_tabs]
        year_urls = year_urls[1:]
        if is_archive:
            year_urls = [fetch_wayback_url(year_url) for year_url in year_urls]
        new_rows.extend(
            row
            for year_url in year_urls
            for row in fetch_enbridge_article_urls(year_url, is_archive, False)
        )
        append_csv(new_rows, is_archive)
    return new_rows


def fetch_cnrl_article_urls(url, is_archive):
    driver.get(url)
    accept_cookies_btn = driver.find_element(By.CLASS_NAME, "cky-btn-accept")
    accept_cookies_btn.click()
    container_div = driver.find_element(By.CLASS_NAME, "wp-block-nf-cnrl-tabs")
    new_rows = []
    link_elems = container_div.find_elements(By.TAG_NAME, "cnrl-news-release-card")
    new_rows.extend(
        [
            {
                "Organization": "Canadian Natural Resources",
                "Link": f"https://www.cnrl.com{link_elem.get_attribute('link')}",
                "Date Scraped": date.strftime("%m/%d/%Y"),
                "Type": "pdf",
            }
            for link_elem in link_elems
        ]
    )
    append_csv(new_rows, is_archive)


def fetch_urls():
    for org, url in URLS.items():
        URLS[org]["archived"] = fetch_wayback_url(url["current"])

    for org, url in URLS.items():
        match org:
            case "Suncor Energy":
                fetch_suncor_article_urls(url["current"], False)
                fetch_suncor_article_urls(url["archived"], True)
                print("Fetched Suncor Energy URLs!")
            case "Pembina Pipeline":
                fetch_pembina_article_urls(url["current"], False)
                fetch_pembina_article_urls(url["archived"], True)
                print("Fetched Pembina Pipeline URLs!")
            case "Imperial Oil":
                fetch_imperial_article_urls(url["current"], False)
                fetch_imperial_article_urls(url["archived"], True)
                print("Fetched Imperial Oil URLs!")
            case "Enbridge":
                fetch_enbridge_article_urls(url["current"], False, True)
                fetch_enbridge_article_urls(url["archived"], True, True)
                print("Fetched Enbridge URLs!")
            case "Canadian Natural Resources":
                fetch_cnrl_article_urls(url["current"], False)
                fetch_cnrl_article_urls(url["archived"], True)
                print("Fetched Canadian Natural Resources URLs!")
            case _:
                print(f"URL fetching for {org} not implemented yet!")

    print("Fetched all article URLs!")
    driver.quit()
