import pandas as pd
import numpy as np
import os
import csv
from config import LABEL_CSV_FIELDS

NUM_TRAIN = 1000


def prompt_user_labelling(to_label):
    with open("output/labelled/labelled.csv", "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=LABEL_CSV_FIELDS)
        if os.stat("output/labelled/labelled.csv").st_size == 0:
            writer.writeheader()
        for index, row in to_label.iterrows():
            label = input(
                f"Label this sentence as 0 (not a green claim) / 1 (green claim) (or q to quit):\n{row['Sentence']}\n> "
            ).strip()

            if label.lower() == "q":
                print("Quitting...")
                break

            if label not in ["0", "1"]:
                print("Invalid input. Please enter 0 or 1.")
                continue

            label = int(label)
            print(f"You labeled it as: {label}")
            row["Label"] = label
            writer.writerow(row.to_dict())
    return


def label_data():
    sentences_df = pd.read_csv("output/processed/sentences.csv")
    wayback_sentences_df = pd.read_csv("output/processed/wayback_sentences.csv")
    sentences_df["isWayback"] = False
    wayback_sentences_df["isWayback"] = True
    combined_df = pd.concat([sentences_df, wayback_sentences_df], ignore_index=True)
    combined_df["Label"] = np.nan
    combined_df = combined_df.sample(frac=1, random_state=42).reset_index(drop=True)
    prompt_user_labelling(combined_df.iloc[:NUM_TRAIN])
    combined_df.iloc[NUM_TRAIN:].to_csv(
        "output/labelled/unlabelled.csv", index=False, sep=",", encoding="utf-8"
    )


if __name__ == "__main__":
    label_data()
