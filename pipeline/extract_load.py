"""Extract + Load step of the ELT pipeline.

Pulls the Sephora products & reviews CSVs (Kaggle, or a local folder) and lands
them untouched as Parquet under data/raw/, partitioned by load_date. All values
are kept as VARCHAR on purpose: typing and cleaning happen later in dbt (the "T").

Incremental loads are simulated with a watermark on review submission date:
  * first run   -> backfill every review up to --backfill-until (one partition)
  * every run after -> land the next --days day(s) of reviews after the watermark
Products are landed as a full snapshot on every run.

Usage:
  python -m pipeline.extract_load --source kaggle
  python -m pipeline.extract_load --source dir --source-dir path/to/csvs
"""
from __future__ import annotations

import argparse
import json
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from pipeline import config

REVIEW_DATE_EXPR = "try_cast(left(submission_time, 10) as date)"


def resolve_source(source: str, source_dir: str | None) -> Path:
    if source == "kaggle":
        import kagglehub  # public dataset: downloads anonymously; KAGGLE_USERNAME / KAGGLE_KEY optional

        return Path(kagglehub.dataset_download(config.KAGGLE_DATASET))
    if not source_dir:
        raise SystemExit("--source dir needs --source-dir")
    return Path(source_dir)


def load_state() -> dict:
    if config.STATE_FILE.exists():
        return json.loads(config.STATE_FILE.read_text())
    return {"reviews_watermark": None, "batches": []}


def save_state(state: dict) -> None:
    config.STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    config.STATE_FILE.write_text(json.dumps(state, indent=2))


def write_partition(con, select_sql: str, table: str, load_date: str, batch_id: str) -> int:
    out_dir = config.RAW_DIR / table / f"load_date={load_date}"
    if out_dir.exists():  # re-running a batch overwrites it (idempotent)
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    out = out_dir / f"{table}.parquet"
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    con.execute(
        f"""copy (
              select *, '{batch_id}' as _batch_id, '{loaded_at}' as _loaded_at
              from ({select_sql})
            ) to '{out}' (format parquet, compression zstd)"""
    )
    return con.execute(f"select count(*) from '{out}'").fetchone()[0]


def run(source_root: Path, backfill_until: str | None, days: int, source_label: str) -> dict:
    con = duckdb.connect()
    review_files = sorted(str(p) for p in source_root.glob("reviews*.csv"))
    product_file = source_root / "product_info.csv"
    if not review_files or not product_file.exists():
        raise SystemExit(f"Expected product_info.csv and reviews*.csv in {source_root}")

    files_sql = "[" + ", ".join(f"'{f}'" for f in review_files) + "]"
    con.execute(
        f"""create view src_reviews as
            select * from read_csv({files_sql}, header=true, all_varchar=true,
                                   union_by_name=true, filename=true)"""
    )
    min_d, max_d = con.execute(
        f"select min({REVIEW_DATE_EXPR}), max({REVIEW_DATE_EXPR}) from src_reviews"
    ).fetchone()

    state = load_state()
    wm = state["reviews_watermark"]
    if wm is None:
        cutoff = date.fromisoformat(backfill_until) if backfill_until else max_d - timedelta(days=30)
        lo, hi, kind = None, cutoff, "backfill"
    else:
        lo = date.fromisoformat(wm)
        if lo >= max_d:
            print(f"Source exhausted: watermark {wm} >= latest review date {max_d}. Nothing to load.")
            return state
        hi, kind = min(lo + timedelta(days=days), max_d), "daily"

    where = f"{REVIEW_DATE_EXPR} <= date '{hi}'"
    if lo is not None:
        where += f" and {REVIEW_DATE_EXPR} > date '{lo}'"
    # Rows with an unparseable date can never be windowed; land them with the backfill
    # so the raw-layer tests see (and log) them instead of silently dropping them.
    if kind == "backfill":
        where = f"({where}) or {REVIEW_DATE_EXPR} is null"

    load_date = hi.isoformat()
    batch_id = f"{kind}_{load_date}"
    n_reviews = write_partition(con, f"select * from src_reviews where {where}", "reviews", load_date, batch_id)
    n_products = write_partition(
        con,
        f"select * from read_csv('{product_file}', header=true, all_varchar=true)",
        "products",
        load_date,
        batch_id,
    )

    state["reviews_watermark"] = load_date
    state["source"] = source_label
    state["source_review_date_range"] = [str(min_d), str(max_d)]
    state["batches"].append(
        {"batch_id": batch_id, "reviews": n_reviews, "products": n_products,
         "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    )
    save_state(state)
    print(f"[{batch_id}] landed {n_reviews:,} reviews and {n_products:,} products -> {config.RAW_DIR}")
    return state


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["kaggle", "dir"], default="kaggle")
    p.add_argument("--source-dir")
    p.add_argument("--backfill-until", help="YYYY-MM-DD cutoff for the first (backfill) load")
    p.add_argument("--days", type=int, default=1, help="days of reviews per incremental batch")
    p.add_argument("--label", help="label recorded in state (e.g. 'kaggle', 'synthetic-fixture')")
    a = p.parse_args()
    root = resolve_source(a.source, a.source_dir)
    run(root, a.backfill_until, a.days, a.label or a.source)


if __name__ == "__main__":
    main()
