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
        try:
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
        except Exception as e:
            print(f"{e}: {url}")
            continue
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
    for url in tqdm(urls):
        try:
            max_retries = 5 if is_archive else 1
            get_url_with_retry(url, max_retries)
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


def read_enbridge_articles(urls, is_archive):
    new_rows = []
    for url in tqdm(urls):
        try:
            resp = session.get(url, timeout=10)
            soup = BeautifulSoup(resp.text, features="html.parser")
            main = soup.find("main")
            title = main.find("h1", id="startMainContent")
            content_container = main.find("div", recursive=False)
            content_blocks = content_container.find_all(
                ["p", "ul", "ol"], recursive=False
            )
            full_content = [title.text]
            for content_block in content_blocks:
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
                    "Organization": "Enbridge",
                    "Link": url,
                    "Content": "\n".join(full_content),
                }
            )
        except Exception as e:
            print(f"{e}: {url}")
            continue
    append_csv(new_rows, is_archive)


def read_cnrl_articles(urls, is_archive):
    new_rows = []
    for url in tqdm(urls):
        try:
            pdf_name = re.findall("[^\/]+$", url)[0]
            pdf_loc = (
                f"./output/pdfs/Canadian Natural Resources/archived/{pdf_name}"
                if is_archive
                else f"./output/pdfs/Canadian Natural Resources/current/{pdf_name}"
            )

            doc = layout(pdf_loc)
            new_rows.append(
                {
                    "Organization": "Canadian Natural Resources",
                    "Link": url,
                    "Content": doc.text,
                }
            )
        except Exception as e:
            print(f"{e}: {url}")
            continue
    append_csv(new_rows, is_archive)


def read_shell_articles(urls, is_archive):
    new_rows = []
    for url in tqdm(urls):
        try:
            max_retries = 5 if is_archive else 1
            get_url_with_retry(url, max_retries)
            try:
                if is_archive:
                    accept_cookies_btn = WebDriverWait(driver, 0.5).until(
                        EC.element_to_be_clickable(
                            (
                                By.ID,
                                "_evidon-banner-acceptbutton",
                            )
                        )
                    )
                    accept_cookies_btn.click()
                else:
                    time.sleep(2)
                    script = """
                    const root = document.querySelector('consent-banner')
                                .shadowRoot
                    const buttons = Array.from(root.querySelectorAll('button'));
                    const acceptBtn = buttons.find(btn => btn.innerText.includes('Accept optional cookies'));

                    if (acceptBtn) {
                        acceptBtn.click();
                        return "Clicked successfully";
                    } else {
                        return "Button not found";
                    }
                    """
                    driver.execute_script(script)
            except:
                print(f"No cookies banner: {url}")
            content_container = driver.find_element(By.ID, "main")
            html_content = content_container.get_attribute("innerHTML")
            soup = BeautifulSoup(html_content, features="html.parser")
            if is_archive:
                header_container = soup.find("div", class_="page-header__body")
                title = header_container.find("h1")
                blurb = soup.find_all(
                    lambda tag: tag.name == "p"
                    and "page-header__date" not in tag.get("class", [])
                )[0]
                content_containers = soup.find_all(
                    lambda tag: (
                        tag.name == "div"
                        and all(
                            c in tag.get("class", [])
                            for c in ["textimage", "parbase", "section"]
                        )
                        and any(
                            re.search(r"basecomponent", c) for c in tag.get("class", [])
                        )
                    )
                )
            else:
                header_container = soup.find("div", attrs={"data-name": "PageHeader"})
                title = header_container.find("h1")
                blurb = header_container.find("p")
                content_containers = soup.select("div[data-name*='PromoSimple']")
            full_content = [title.text.strip(), blurb.text.strip()]
            for content_container in content_containers:
                content = content_container.find_all(["p", "ul", "ol", "h3"])
                for elem in content:
                    li_elems = elem.find_all("li", recursive=True)
                    if li_elems:
                        for li_elem in li_elems:
                            content = li_elem.text.strip()
                            content = content.replace("\n", " ")
                            content = re.sub("\s+", " ", content)
                            full_content.append(content)
                        continue
                    content = elem.text.strip()
                    content = content.replace("\n", " ")
                    content = re.sub("\s+", " ", content)
                    full_content.append(content)
            new_rows.append(
                {
                    "Organization": "Shell Canada",
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
            case "Enbridge":
                read_enbridge_articles(curr_org_links, False)
                read_enbridge_articles(archived_org_links, True)
            case "Canadian Natural Resources":
                read_cnrl_articles(curr_org_links, False)
                read_cnrl_articles(archived_org_links, True)
            case "Shell Canada":
                read_shell_articles(curr_org_links, False)
                read_shell_articles(archived_org_links, True)
            case _:
                print(f"Article reading for {org} not implemented yet!")
    driver.quit()
