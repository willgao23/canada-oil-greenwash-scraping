from config import (
    URLS,
    WAYBACK_ENDPOINT,
    BILL_C59_ROYAL_ASSENT_DATE,
    LINK_CSV_FIELDS,
    WAYBACK_PREFIX,
    WAYBACK_ORGS,
)
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
from datetime import datetime
import time
import os
import random
import pandas as pd
from tqdm import tqdm
import os, os.path

driver = webdriver.Chrome()
driver.implicitly_wait(5)
date = datetime.now()
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0; +https://example.org/contact)"
    }
)


def fetch_wayback_url(url, max_retries=5, to=None, limit=-1):
    for attempt in range(max_retries):
        try:
            payload = {
                "url": url,
                "output": "json",
                "to": to,
                "limit": limit,
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
        english_links = driver.find_elements(By.CSS_SELECTOR, "div.module_item.en")
        year_link_elems = []
        for english_link in english_links:
            year_link_elems.append(
                english_link.find_element(By.CLASS_NAME, "module_headline-link")
            )
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
            year_urls = [
                fetch_wayback_url(year_url, to=BILL_C59_ROYAL_ASSENT_DATE)
                for year_url in year_urls
            ]
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


def fetch_shell_article_urls(url, is_archive, is_root):
    driver.get(url)
    if is_archive:
        body_container_div = driver.find_element(By.CLASS_NAME, "promo-list__base")
        item_container_divs = body_container_div.find_elements(
            By.CLASS_NAME, "promo-list__text"
        )
        link_elems = [
            item_container_div.find_element(By.TAG_NAME, "a")
            for item_container_div in item_container_divs
        ]
        new_rows = [
            {
                "Organization": "Shell Canada",
                "Link": link_elem.get_attribute("href"),
                "Date Scraped": date.strftime("%m/%d/%Y"),
                "Type": "html",
            }
            for link_elem in link_elems
        ]
        if is_root:
            expand_archive = driver.find_element(
                By.CLASS_NAME, "expandable-list__item "
            )
            expand_archive.click()
            container_div = driver.find_element(
                By.CLASS_NAME, "expandable-list__item-body"
            )
            archive_link_elems = container_div.find_elements(By.TAG_NAME, "a")
            archive_urls = [
                archive_link_elem.get_attribute("href")
                for archive_link_elem in archive_link_elems
            ]
            archive_urls = [
                fetch_wayback_url(archive_url) for archive_url in archive_urls
            ]
            new_rows.extend(
                row
                for archive_url in archive_urls
                for row in fetch_shell_article_urls(archive_url, is_archive, False)
            )
            append_csv(new_rows, is_archive)
        return new_rows
    else:
        container_divs = driver.find_elements(
            By.CSS_SELECTOR, "div[data-name='PressRelease']"
        )
        link_elems = [
            container_div.find_element(By.TAG_NAME, "a")
            for container_div in container_divs
        ]
        new_rows = [
            {
                "Organization": "Shell Canada",
                "Link": link_elem.get_attribute("href"),
                "Date Scraped": date.strftime("%m/%d/%Y"),
                "Type": "html",
            }
            for link_elem in link_elems
        ]
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
            case "Shell Canada":
                fetch_shell_article_urls(url["current"], False, True)
                fetch_shell_article_urls(url["archived"], True, True)
                print("Fetched Shell Canada URLs!")
            case _:
                print(f"URL fetching for {org} not implemented yet!")

    print("Fetched all article URLs!")
    driver.quit()


def download_pdfs(links, is_archive):
    pdf_links_df = links[links["Type"] == "pdf"]
    pdf_orgs = list(pdf_links_df["Organization"].unique())
    for org in pdf_orgs:
        dir_path = (
            f"output\pdfs\{org}\\archived"
            if is_archive
            else f"output\pdfs\{org}\current"
        )
        download_dir = os.path.abspath(dir_path)
        os.makedirs(download_dir, exist_ok=True)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "plugins.always_open_pdf_externally": True,
                "profile.default_content_setting_values.automatic_downloads": 1,
                "safebrowsing.enabled": False,
                "safebrowsing.disable_extension_blacklist": True,
            },
        )
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument(
            "--unsafely-treat-insecure-origin-as-secure=http://archive.org,http://wayback.com"
        )
        driver = webdriver.Chrome(options=chrome_options)
        pdf_links = pdf_links_df[pdf_links_df["Organization"] == org]["Link"].to_list()

        start = (
            len(
                [
                    entry
                    for entry in os.listdir(f".\{dir_path}")
                    if os.path.isfile(os.path.join(dir_path, entry))
                ]
            )
            - 1
        )
        for pdf_link in tqdm(pdf_links[start:]):
            if is_archive:
                try:
                    driver.get(pdf_link)
                    iframe = driver.find_element(By.ID, "playback")
                    driver.switch_to.frame(iframe)
                    save_btn = driver.find_element(By.ID, "open-button")
                    save_btn.click()
                    time.sleep(2)
                except:
                    print(f"Error downloading pdf: {pdf_link}")
                finally:
                    driver.switch_to.default_content()
            else:
                driver.get(pdf_link)
                time.sleep(1)
        driver.quit()


def fetch_unhosted_wayback_links(archived_links):
    file_path = f"output/links/unhosted_wayback_links.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Wayback Link", "Link"])

    for org in WAYBACK_ORGS:
        links = archived_links[archived_links["Organization"] == org]["Link"].to_list()
        for link in tqdm(links):
            wayback_link = fetch_wayback_url(link, limit=1)
            try:
                with open(file_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if link:
                        writer.writerow([wayback_link, link])
            except Exception as e:
                print(f"An error occurred: {e}")


def merge_unhosted_wayback():
    unhosted_df = pd.read_csv("output/links/unhosted_wayback_links.csv")
    archived_links = pd.read_csv("output/links/wayback_article_links.csv")
    merged_df = pd.merge(
        archived_links,
        unhosted_df,
        on="Link",
        how="left",
        suffixes=("", "_new"),
    )
    merged_df["Link"] = merged_df["Wayback Link"].fillna(merged_df["Link"])
    final_df = merged_df.drop(columns=["Wayback Link"])
    final_df.to_csv("output/links/merged_wayback_article_links.csv", index=False)


def fetch_pdfs():
    curr_links = pd.read_csv("output/links/article_links.csv")
    archived_links = pd.read_csv("output/links/wayback_article_links.csv")
    # fetch_unhosted_wayback_links(archived_links)
    # merge_unhosted_wayback()
    updated_archived_links = pd.read_csv(
        "output/links/merged_wayback_article_links.csv"
    )
    # download_pdfs(curr_links, False)
    download_pdfs(updated_archived_links, True)
