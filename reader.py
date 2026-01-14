import pandas as pd
from config import ORG_NAMES, ARTICLE_CSV_FIELDS, WAYBACK_PREFIX, WAYBACK_ENDPOINT
import csv
import os
import string
from datetime import datetime
from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
import time
import random
import re
import spacy
from spacy_layout import spaCyLayout
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

options = Options()
options.page_load_strategy = "eager"
driver = webdriver.Chrome(options=options)
driver.implicitly_wait(5)
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0; +https://example.org/contact)"
    }
)
nlp = spacy.load("en_core_web_sm")
layout = spaCyLayout(nlp)


def get_url_with_retry(url, max_retries=1):
    retries = 0
    wait_time = 5
    while retries <= max_retries:
        try:
            print(f"Attempt {retries + 1}: Loading {url}")
            driver.get(url)
            return True
        except:
            retries += 1
            if retries <= max_retries:
                time.sleep(wait_time)
                wait_time *= 2
                print("Timeout reached. Reloading...")
            else:
                print("Still not loading. Skipping...")
                return False


def read_suncor_articles(urls, is_archive):
    new_rows = []
    for url in tqdm(urls):
        pdf_name = re.findall("([^\/?]+)(?=\?)", url)[0]
        pdf_loc = (
            f"./output/pdfs/Suncor Energy/archived/{pdf_name}"
            if is_archive
            else f"./output/pdfs/Suncor Energy/current/{pdf_name}"
        )
        doc = layout(pdf_loc)
        new_rows.append(
            {
                "Organization": "Suncor Energy",
                "Link": url,
                "Content": doc.text,
            }
        )
    append_csv(new_rows, is_archive)


def read_pembina_articles(urls, is_archive):
    new_rows = []
    for url in tqdm(urls):
        try:
            resp = session.get(url, timeout=10)
            soup = BeautifulSoup(resp.text, features="html.parser")
            title = soup.find("h1", class_="large-text")
            content_container = soup.find("div", class_="news-body")
            content_blocks = content_container.find_all("p", recursive=False)
            full_content = [title.text]
            if len(content_blocks) == 0:
                lower_content_container = content_container.find("div")
                if lower_content_container:
                    content_blocks = lower_content_container.find_all(
                        "p", recursive=False
                    )
                else:
                    full_content.append(content_container.text.strip())
            for content_block in content_blocks:
                content = content_block.text.strip()
                content = content.replace("\n", " ")
                full_content.append(content)
            new_rows.append(
                {
                    "Organization": "Pembina Pipeline",
                    "Link": url,
                    "Content": "\n".join(full_content),
                }
            )
        except Exception as e:
            print(f"{e}: {url}")
            continue
    append_csv(new_rows, is_archive)


def read_imperial_articles(urls, is_archive):
    new_rows = []
    unread = [12, 26, 31, 35, 42, 60, 68, 75, 80, 108, 116]
    for url in tqdm(unread):
        try:
            max_retries = 5 if is_archive else 1
            get_url_with_retry(urls[url], max_retries)
            try:
                close_popup_btn = WebDriverWait(driver, 0.5).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[contains(@class, 'fancybox-close-small')]")
                    )
                )
                close_popup_btn.click()
            except:
                print(f"No disclaimer pop-up: {url}")
            title = driver.find_element(
                By.XPATH, "//h3[contains(@class, 'module-details_title')]"
            ).get_attribute("innerText")
            content_container = driver.find_element(By.CLASS_NAME, "module_body")
            html_content = content_container.get_attribute("innerHTML")
            soup = BeautifulSoup(html_content, features="html.parser")
            inner_div = soup.find("div", class_="q4default")
            content_blocks = (
                inner_div.find_all(["p", "ul", "ol", "div"], recursive=False)
                if inner_div
                else soup.find_all(["p", "ul", "ol", "div"], recursive=False)
            )
            full_content = [title]
            for content_block in content_blocks:
                if (
                    content_block.name == "table"
                    or "table-wrapper" in content_block.get("class", [])
                ):
                    continue
                li_elems = content_block.find_all("li", recursive=True)
                if li_elems:
                    for li_elem in li_elems:
                        content = li_elem.text.strip()
                        content = content.replace("\n", " ")
                        content = re.sub("\s+", " ", content)
                        full_content.append(content)
                    continue
                content = content_block.text.strip()
                content = content.replace("\n", " ")
                content = re.sub("\s+", " ", content)
                full_content.append(content)
            new_rows.append(
                {
                    "Organization": "Imperial Oil",
                    "Link": url,
                    "Content": "\n".join(full_content),
                }
            )
        except Exception as e:
            print(f"{e}: {url}")
            continue
    append_csv(new_rows, is_archive)


def append_csv(new_rows, is_archive):
    article_csv = (
        "output/content/raw_wayback_content.csv"
        if is_archive
        else "output/content/raw_content.csv"
    )

    with open(article_csv, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=ARTICLE_CSV_FIELDS)
        if os.stat(article_csv).st_size == 0:
            writer.writeheader()
        writer.writerows(new_rows)


def read_urls():
    curr_to_read = pd.read_csv("output/links/article_links.csv")
    archived_to_read = pd.read_csv("output/links/merged_wayback_article_links.csv")
    for org in ORG_NAMES:
        curr_org_links = curr_to_read[curr_to_read["Organization"] == org][
            "Link"
        ].to_list()
        archived_org_links = archived_to_read[archived_to_read["Organization"] == org][
            "Link"
        ].to_list()
        match org:
            case "Suncor Energy":
                read_suncor_articles(curr_org_links, False)
                read_suncor_articles(archived_org_links, True)
            case "Pembina Pipeline":
                read_pembina_articles(curr_org_links, False)
                read_pembina_articles(archived_org_links, True)
            case "Imperial Oil":
                read_imperial_articles(curr_org_links, False)
                read_imperial_articles(archived_org_links, True)
            case _:
                print(f"Article reading for {org} not implemented yet!")
    driver.quit()
