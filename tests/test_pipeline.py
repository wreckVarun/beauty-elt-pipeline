"""Unit tests for the parts of the pipeline that are plain Python.

The dbt models and their data tests are covered by the end-to-end smoke run
(scripts/smoke_run.sh), which runs `dbt build` over the synthetic fixture.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from pipeline import config, extract_load
from pipeline.enrich_reviews import keyword_label
from pipeline.make_dev_fixture import build


@pytest.fixture(scope="module")
def products() -> pd.DataFrame:
    return pd.read_csv(Path(__file__).parent.parent / "seeds" / "product_info.csv")


@pytest.fixture()
def landed(tmp_path, products, monkeypatch):
    """Land two batches from a small synthetic fixture into a temp DATA_DIR."""
    src = tmp_path / "src"
    src.mkdir()
    products.head(500).to_csv(src / "product_info.csv", index=False)
    build(products.head(500), 3000, seed=1).to_csv(src / "reviews_test.csv", index=False)

    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "RAW_DIR", tmp_path / "data" / "raw")
    monkeypatch.setattr(config, "STATE_FILE", tmp_path / "data" / "raw" / "_state.json")
    first = extract_load.run(src, backfill_until="2022-06-01", days=1, source_label="test")
    second = extract_load.run(src, backfill_until=None, days=1, source_label="test")
    return tmp_path, first, second


def test_backfill_then_incremental_advances_watermark(landed):
    _, first, second = landed
    assert first["batches"][0]["batch_id"] == "backfill_2022-06-01"
    assert second["batches"][1]["batch_id"] == "daily_2022-06-02"
    assert second["reviews_watermark"] == "2022-06-02"


def test_each_batch_lands_its_own_partition(landed):
    tmp_path, _, _ = landed
    parts = sorted(p.name for p in (tmp_path / "data" / "raw" / "reviews").iterdir())
    assert parts == ["load_date=2022-06-01", "load_date=2022-06-02"]


def test_incremental_batch_holds_only_its_day(landed):
    tmp_path, _, _ = landed
    con = duckdb.connect()
    days = con.execute(
        f"select distinct left(submission_time, 10) "
        f"from '{tmp_path}/data/raw/reviews/load_date=2022-06-02/reviews.parquet'"
    ).fetchall()
    assert days == [("2022-06-02",)]


def test_batches_do_not_overlap(landed):
    tmp_path, _, _ = landed
    con = duckdb.connect()
    overlap = con.execute(
        f"""select author_id, product_id, submission_time
            from read_parquet('{tmp_path}/data/raw/reviews/*/*.parquet')
            group by all having count(distinct _batch_id) > 1"""
    ).fetchall()
    assert overlap == []


def test_rerunning_a_batch_is_idempotent(landed, products, tmp_path_factory):
    tmp_path, _, _ = landed
    before = json.loads((tmp_path / "data" / "raw" / "_state.json").read_text())
    n_files = len(list((tmp_path / "data" / "raw" / "reviews").glob("*/*.parquet")))
    assert n_files == len(before["batches"])


@pytest.mark.parametrize(
    "text,rating,expected",
    [
        ("Broke me out in tiny bumps.", 1, "skin_reaction"),
        ("The pump broke after a week.", 2, "packaging"),
        ("The fragrance is way too strong.", 2, "scent"),
        ("Way overpriced for the size.", 2, "price_value"),
        ("Love this, my skin feels so soft.", 5, "none"),
        ("Something went wrong somehow.", 1, "other"),
    ],
)
def test_keyword_labeler(text, rating, expected):
    tag = keyword_label(text, rating)
    assert tag.complaint_type == expected
    assert tag.sentiment == ("positive" if rating >= 4 else "negative" if rating <= 2 else "neutral")


def test_fixture_contains_the_defects_the_tests_should_catch(products):
    df = build(products.head(500), 5000, seed=3)
    assert (df["rating"] == 0).any(), "expected out-of-range ratings"
    assert df["review_text"].isna().any(), "expected blank review text"
    assert (df["product_id"] == "P_UNKNOWN").any(), "expected orphan product ids"
    key = ["author_id", "product_id", "submission_time", "review_text"]
    assert df.duplicated(subset=key).any(), "expected duplicate reviews"


def test_gemini_run_stops_after_repeated_failed_batches(tmp_path, monkeypatch):
    """A bad key or exhausted quota should end the run early, not retry every batch."""
    from pipeline import enrich_reviews

    calls = []
    monkeypatch.setattr(config, "WAREHOUSE", tmp_path / "w.duckdb")
    monkeypatch.setattr(enrich_reviews, "sample_reviews",
                        lambda con, labeler, n: [(f"id{i}", 1, "meh") for i in range(200)])
    monkeypatch.setattr(enrich_reviews, "gemini_label_batch", lambda *a, **k: calls.append(1) or {})
    monkeypatch.setenv("GEMINI_API_KEY", "test-not-a-real-key")
    monkeypatch.setattr("sys.argv", ["enrich_reviews", "--sleep", "0"])
    enrich_reviews.main()
    assert len(calls) == 3
