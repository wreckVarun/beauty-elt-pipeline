"""Generate a SYNTHETIC reviews file for local development and CI smoke tests.

It is NOT real data. It mirrors the column layout of the Kaggle reviews CSVs and
uses real product_ids from product_info.csv, so the whole pipeline can be
exercised without Kaggle credentials. It also injects a few known defects
(duplicate rows, blank text, out-of-range ratings, unknown product ids) so the
raw-layer dbt tests have something to catch.

  python -m pipeline.make_dev_fixture --products path/product_info.csv --out fixture_dir
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

TEMPLATES = {
    "packaging": ["The pump broke after a week.", "Arrived leaking, the cap was cracked.",
                  "Packaging is cheap and the tube splits."],
    "scent": ["The fragrance is way too strong.", "Smells like chemicals, I couldn't stand it.",
              "Scent gave me a headache."],
    "skin_reaction": ["Broke me out in tiny bumps.", "Made my skin red and itchy.",
                      "Burned on application, had to wash it off."],
    "texture": ["Too greasy and pilled under makeup.", "Sticky texture that never absorbs."],
    "ineffective": ["Did nothing for my dark spots.", "No difference after a month of use."],
    "price_value": ["Way overpriced for the size.", "Tiny jar for the price."],
}
POSITIVE = ["Love this, my skin feels so soft.", "Holy grail product, repurchasing.",
            "Gentle and hydrating, no complaints.", "Great value and works well."]
NEUTRAL = ["It's okay, nothing special.", "Does the job but not amazing."]

COLS = ["", "author_id", "rating", "is_recommended", "helpfulness", "total_feedback_count",
        "total_neg_feedback_count", "total_pos_feedback_count", "submission_time", "review_text",
        "review_title", "skin_tone", "eye_color", "skin_type", "hair_color", "product_id",
        "product_name", "brand_name", "price_usd"]


def build(products: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    skincare = products[products["primary_category"] == "Skincare"]
    prods = skincare.sample(n=min(1500, len(skincare)), random_state=seed)
    p = prods.iloc[rng.integers(0, len(prods), n)].reset_index(drop=True)

    rating = rng.choice([1, 2, 3, 4, 5], n, p=[0.07, 0.07, 0.10, 0.20, 0.56])
    # cheaper products skew slightly lower so price-vs-rating has some signal to find
    cheap = p["price_usd"].fillna(30) < 20
    rating = np.where(cheap & (rng.random(n) < 0.08), np.maximum(rating - 1, 1), rating)

    kinds = list(TEMPLATES)
    text, title = [], []
    for r in rating:
        if r <= 2 or (r == 3 and rng.random() < 0.5):
            k = kinds[rng.integers(len(kinds))]
            text.append(TEMPLATES[k][rng.integers(len(TEMPLATES[k]))])
            title.append("Disappointed")
        elif r == 3:
            text.append(NEUTRAL[rng.integers(len(NEUTRAL))]); title.append("Meh")
        else:
            text.append(POSITIVE[rng.integers(len(POSITIVE))]); title.append("Love it")

    days = pd.to_datetime("2022-01-01") + pd.to_timedelta(rng.integers(0, 445, n), unit="D")
    fb = rng.poisson(2, n)
    neg = rng.binomial(fb, 0.2)
    df = pd.DataFrame({
        "": np.arange(n),
        "author_id": rng.integers(1_000_000, 9_999_999, n).astype(str),
        "rating": rating,
        "is_recommended": (rating >= 4).astype(float),
        "helpfulness": np.where(fb > 0, (fb - neg) / np.maximum(fb, 1), np.nan),
        "total_feedback_count": fb,
        "total_neg_feedback_count": neg,
        "total_pos_feedback_count": fb - neg,
        "submission_time": days.strftime("%Y-%m-%d"),
        "review_text": text,
        "review_title": title,
        "skin_tone": rng.choice(["light", "medium", "tan", "deep", None], n),
        "eye_color": rng.choice(["brown", "blue", "green", "hazel", None], n),
        "skin_type": rng.choice(["dry", "oily", "combination", "normal", None], n),
        "hair_color": rng.choice(["black", "brown", "blonde", "red", None], n),
        "product_id": p["product_id"].values,
        "product_name": p["product_name"].values,
        "brand_name": p["brand_name"].values,
        "price_usd": p["price_usd"].values,
    })

    # known defects for the raw-layer tests to catch
    dupes = df.sample(n=max(1, n // 200), random_state=seed)
    df = pd.concat([df, dupes], ignore_index=True)
    bad = df.sample(n=40, random_state=seed + 1).index
    df.loc[bad[:15], "review_text"] = None
    df.loc[bad[15:25], "rating"] = 0
    df.loc[bad[25:40], "product_id"] = "P_UNKNOWN"
    return df[COLS]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--products", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=60_000)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    products = pd.read_csv(a.products)
    shutil.copy(a.products, out / "product_info.csv")
    df = build(products, a.n, a.seed)
    half = len(df) // 2
    df.iloc[:half].to_csv(out / "reviews_synthetic_0.csv", index=False)
    df.iloc[half:].to_csv(out / "reviews_synthetic_1.csv", index=False)
    (out / "SYNTHETIC_DATA_README.txt").write_text(
        "These review files are synthetic, generated by pipeline/make_dev_fixture.py. "
        "Do not report numbers from them as real findings.\n")
    print(f"wrote {len(df):,} synthetic reviews for real products -> {out}")


if __name__ == "__main__":
    main()
