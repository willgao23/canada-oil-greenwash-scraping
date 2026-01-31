import numpy as np
import pandas as pd
import os
import csv
import nltk
import re
from config import SENTENCE_CSV_FIELDS

nltk.download("punkt_tab")


def find_unique(org, link, sentences, seen):
    unique = set()
    for s in sentences:
        s = s.strip()
        if s not in seen and re.search(r"[A-Za-z]", s) and len(s.split()) > 1:
            seen.add(s)
            unique.add(s)
    return [{"Organization": org, "Link": link, "Sentence": u} for u in unique]


def split_sentences(org, link, content, is_archive, seen):
    paragraphs = content.split("\n")
    sentences = [nltk.tokenize.sent_tokenize(p, "english") for p in paragraphs]
    rows = [find_unique(org, link, s, seen) for s in sentences]
    for r in rows:
        append_csv(r, is_archive)


def append_csv(new_rows, is_archive):
    link_csv = (
        "output/processed/wayback_sentences.csv"
        if is_archive
        else "output/processed/sentences.csv"
    )

    with open(link_csv, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=SENTENCE_CSV_FIELDS)
        if os.stat(link_csv).st_size == 0:
            writer.writeheader()
        writer.writerows(new_rows)


def process_raw_content():
    raw_content_df = pd.read_csv("output/content/raw_content.csv")
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

    raw_wayback_content_df = pd.read_csv("output/content/raw_wayback_content.csv")
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
