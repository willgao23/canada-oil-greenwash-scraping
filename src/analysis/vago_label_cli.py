import sys
import os, os.path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import csv
from config import ANALYZE_CSV_FIELDS


def prompt_user_labelling(to_label):
    with open(
        "../../output/analyzed/manual_vagueness_analyzed.csv",
        "a",
        newline="",
        encoding="utf-8",
    ) as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=ANALYZE_CSV_FIELDS + ["Vagueness Label"]
        )
        if os.stat("../../output/analyzed/manual_vagueness_analyzed.csv").st_size == 0:
            writer.writeheader()
        for _, row in to_label.iterrows():
            label = input(
                f"Label this sentence as 0 (not vague) / 1 (vague) / 2 (not a green claim) (or q to quit):\n{row['Sentence']}\n> "
            ).strip()

            if label.lower() == "q":
                print("Quitting...")
                break

            if label not in ["0", "1", "2"]:
                print("Invalid input. Please enter 0, 1, or 2.")
                continue

            label = int(label)
            print(f"You labeled it as: {label}")
            row["Vagueness Label"] = label
            writer.writerow(row.to_dict())
    return


def label_data():
    df = pd.read_csv("../../output/analyzed/vagueness_analyzed.csv")
    df["Vagueness Label"] = -1
    df = df.sample(frac=1).reset_index(drop=True)
    label_amt = int(len(df.index) * 0.15)
    print(f"Sentences to label: {label_amt}")
    prompt_user_labelling(df.iloc[:label_amt])


if __name__ == "__main__":
    label_data()
