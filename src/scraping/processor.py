import sys
import os, os.path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import re
import unicodedata
import csv
import spacy
import re
from config import SENTENCE_CSV_FIELDS, MIN_CHARS

nlp = spacy.load("en_core_web_lg")


def clean_sentence(text):
    if not isinstance(text, str) or text.strip() == "":
        return ""
    # Unicode normalisation
    text = unicodedata.normalize("NFKC", text)
    # Remove non-printable characters
    text = "".join(c for c in text if unicodedata.category(c) != "Cc" or c in "\n\t")
    # Standardise quotes
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    # Remove URLs, email addresses, and phone numbers
    text = re.sub(r"(?:https?://|www\.)\S+|[\w-]+\.(?:com|org|net|ca|io)\S*", "", text)
    text = re.sub(r"\S+@\S+\.\S+", "", text)
    text = re.sub(r"(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", "", text)
    # Fix PDF hyphenation breaks
    text = re.sub(r"(\w+)-\s+(\w+)", r"\1\2", text)
    # Remove symbols
    text = re.sub(r"[™®©]", "", text)
    # Remove standalone page numbers
    text = re.sub(r"\bPage\s*\d+(\s*of\s*\d+)?\b", "", text, flags=re.IGNORECASE)
    # Remove bullet points
    text = re.sub(r"\s*[|•·▪▸►]\s*", " ", text)
    # Reduce repeated punct to a single character
    text = re.sub(r"\.{3,}", "...", text)
    text = re.sub(r"-{2,}", "-", text)
    # Reduce repeated whitespace to a single space
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_valid(text):
    if not isinstance(text, str):
        return False
    if len(text) < MIN_CHARS:
        return False
    if not re.search(r"[a-zA-Z]{3,}", text):
        return False
    return True


def split_sentences(org, link, content, is_archive, seen):
    paragraphs = content.split("\n")
    rows = []
    for paragraph in paragraphs:
        doc = nlp(paragraph)
        sentences = [sent.text.strip() for sent in doc.sents]
        sentences = filter(is_valid, [clean_sentence(sent) for sent in sentences])
        for sent in sentences:
            key = (sent, org)
            if key not in seen:
                seen.add(key)
                rows.append({"Organization": org, "Link": link, "Sentence": sent})
    append_csv(rows, is_archive)


def append_csv(new_rows, is_archive):
    link_csv = (
        "../../output/processed/test_wayback_sentences.csv"
        if is_archive
        else "../../output/processed/test_sentences.csv"
    )

    with open(link_csv, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=SENTENCE_CSV_FIELDS)
        if os.stat(link_csv).st_size == 0:
            writer.writeheader()
        writer.writerows(new_rows)


def process_raw_content():
    raw_content_df = pd.read_csv("../../output/content/raw_content.csv")
    raw_content_df["Content"] = raw_content_df["Content"].str.replace(
        r"\n+", "\n", regex=True
    )
    seen = set()
    raw_content_df.apply(
        lambda row: split_sentences(
            row["Organization"], row["Link"], row["Content"], False, seen
        ),
        axis=1,
    )

    raw_wayback_content_df = pd.read_csv("../../output/content/raw_wayback_content.csv")
    raw_wayback_content_df["Content"] = raw_wayback_content_df["Content"].str.replace(
        r"\n+", "\n", regex=True
    )
    wayback_seen = set()
    raw_wayback_content_df.apply(
        lambda row: split_sentences(
            row["Organization"], row["Link"], row["Content"], True, wayback_seen
        ),
        axis=1,
    )
