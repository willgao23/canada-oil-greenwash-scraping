import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import csv
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import os
import re
from src.config import ANALYZE_CSV_FIELDS, VAGO_URL, VAGUENESS_TYPES

driver = webdriver.Chrome()
driver.implicitly_wait(5)
date = datetime.now()
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0; +https://example.org/contact)"
    }
)


def vagueness_detect(green_claims):
    claims_data = green_claims.to_dict("records")
    driver.get(VAGO_URL)
    with open(
        "../../output/analyzed/vagueness_analyzed.csv",
        "a",
        newline="",
        encoding="utf-8",
    ) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=ANALYZE_CSV_FIELDS)
        if os.stat("../../output/analyzed/vagueness_analyzed.csv").st_size == 0:
            writer.writeheader()

        for row in tqdm(claims_data):
            sentence = row["Sentence"]
            text_entry = driver.find_element(By.ID, "corpus-form-textarea")
            text_entry.clear()
            text_entry.send_keys(sentence)
            analyze_btn = driver.find_element(By.ID, "corpus-form-btn-next")
            driver.execute_script("arguments[0].click();", analyze_btn)
            WebDriverWait(driver, 10).until(
                lambda d: "d-none"
                in d.find_element(By.CSS_SELECTOR, ".loader.py-5").get_attribute(
                    "class"
                )
            )
            for i in range(len(VAGUENESS_TYPES)):
                vague_word_p = driver.find_element(
                    By.CLASS_NAME, f"bg-{VAGUENESS_TYPES[i].split(' ')[0].lower()}"
                )
                inner_text = vague_word_p.text.lower()
                pattern = rf"{VAGUENESS_TYPES[i].lower()}:\s*(.*)"
                match = re.search(pattern, inner_text)
                words = match.group(1).strip() if match else None
                row[f"{VAGUENESS_TYPES[i]} Words"] = words

            final_row = {k: v for k, v in row.items() if k in ANALYZE_CSV_FIELDS}
            writer.writerow(final_row)


if __name__ == "__main__":
    labelled_df = pd.read_csv("../../output/all_labelled.csv")
    green_claims = labelled_df[labelled_df["Label"] == 1]
    vagueness_detect(green_claims)
