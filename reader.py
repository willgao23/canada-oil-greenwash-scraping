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

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0; +https://example.org/contact)"
    }
)
nlp = spacy.load("en_core_web_sm")
layout = spaCyLayout(nlp)


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
    return


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


def fetch_wayback_url(url, max_retries=5):
    for attempt in range(max_retries):
        try:
            payload = {
                "url": url,
                "output": "json",
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
            # case "Suncor Energy":
            # read_suncor_articles(curr_org_links, False)
            # read_suncor_articles(archived_org_links, True)
            case "Pembina Pipeline":
                read_pembina_articles(curr_org_links, False)
                read_pembina_articles(archived_org_links, True)
            case _:
                print(f"Article reading for {org} not implemented yet!")
