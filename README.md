# Beauty Product Analytics ELT Pipeline

An ELT pipeline over the Sephora products and reviews dataset: raw CSVs land as Parquet in
daily batches, dbt models them on DuckDB into a star schema with 35 data tests, a Gemini
step tags a 2,000-review sample by sentiment and complaint type, and a static dashboard
shows top complaints by brand and price vs rating. A GitHub Actions workflow runs the whole
thing daily and publishes the dashboard to
[wreckvarun.github.io/beauty-elt-pipeline](https://wreckvarun.github.io/beauty-elt-pipeline/).

## Latest run on the real data

The scheduled run on 8 Oct 2026 pulled the full Kaggle dump (no credentials needed; it is a
public dataset) and backfilled everything up to 2023-02-19:

| | |
| --- | --- |
| reviews landed (raw) | **1,071,473** (submitted 2008-08-28 → 2023-03-21) |
| reviews in `fct_reviews` | **1,070,952** after de-duplication |
| products / brands | 8,494 / 304 (140 brands have reviews) |
| dbt build | 33 data tests on this pass: 31 pass, **2 warn**, 0 errors |
| run time | about a minute for load + dbt on a laptop |

The two warnings are real problems in the source, logged row by row and not blocking the load:

* **1,433 reviews with blank text** (`not_blank` on `raw.reviews.review_text`). They are kept
  for ratings but can never be tagged by the LLM.
* **385 groups of duplicate reviews**, 906 rows in all: the same author, product, timestamp and
  text landed more than once. `stg_reviews` keeps the first copy, dropping 521 rows.

```
Kaggle CSVs ──> data/raw/<table>/load_date=YYYY-MM-DD/*.parquet   (pipeline/extract_load.py)
                       │
                       ├─ stg_products, stg_reviews                (dbt, typing + de-dup)
                       ├─ dim_brands, dim_products, fct_reviews    (dbt, star schema)
                       │
Gemini API ──> llm.review_tags ──> mart_brand_complaints           (pipeline/enrich_reviews.py)
                       │
                       └─> reports/dashboard.html                  (dashboard/build_dashboard.py)
```

## Quick start

No network or keys needed — this runs the whole pipeline on synthetic reviews generated
against the real product catalogue in `seeds/`:

```bash
pip install -r requirements.txt
./scripts/smoke_run.sh          # two batches, dbt build, keyword enrichment, dashboard
open reports/dashboard.html
```

With the real data (downloads anonymously from Kaggle, ~150 MB) and, optionally, a Gemini key:

```bash
export GEMINI_API_KEY=...                      # https://aistudio.google.com/apikey (optional)
./run_pipeline.sh --source kaggle              # first run: backfill to 30 days before the newest review
./run_pipeline.sh --source kaggle --days 1     # later runs: one day each
```

Without a key, `ENRICH_LABELER=keyword ./run_pipeline.sh ...` fills the complaints chart with
the offline baseline instead.

## How it works

### 1. Extract + Load — `pipeline/extract_load.py`

Downloads the dataset ([`nadyinky/sephora-products-and-skincare-reviews`](https://www.kaggle.com/datasets/nadyinky/sephora-products-and-skincare-reviews),
8,494 products across 304 brands, ~1M reviews) and writes it straight to Parquet under
`data/raw/`, Hive-partitioned by `load_date`, every column still a string. Nothing is
cleaned here — that is the "T", and it lives in dbt.

The dataset is a one-off dump, so daily arrival is **simulated** with a watermark on review
submission date, kept in `data/raw/_state.json`:

* first run backfills everything up to `--backfill-until` into one partition;
* each later run lands the next `--days` day(s) of reviews after the watermark.

Products are re-landed as a full snapshot each run; `stg_products` reads only the newest
snapshot. Re-running a batch overwrites its partition, so a retried run is idempotent.
Each row carries `_batch_id` and `_loaded_at` for lineage.

### 2. Transform — `beauty_dbt/`

Six models, `dbt-duckdb` against `data/warehouse.duckdb`:

| model | layer | grain |
| --- | --- | --- |
| `stg_products` | staging | one row per product, typed, latest snapshot |
| `stg_reviews` | staging | one row per review, typed, de-duplicated on an md5 surrogate key |
| `dim_brands` | mart | one row per brand, with product count and median price |
| `dim_products` | mart | one row per product, with a price band, FK to `dim_brands` |
| `fct_reviews` | mart | one row per review — the fact table, FKs to both dimensions |
| `mart_brand_complaints` | mart | one row per brand × complaint type, from the LLM tags |

`fct_reviews` is **incremental** (`delete+insert` on `review_id`): each run only reads load
partitions newer than what the table already holds, so a daily run costs one day of data.

### 3. Data quality — 35 tests, failures logged

`dbt build` runs 35 data tests across two layers (6 on the raw source, 3 on the LLM tags, 26 on the models).

* **Raw layer, severity `warn`** — these describe the source, so they never block a load:
  null `product_id`, blank `review_text`, ratings outside 1–5, duplicate review rows,
  reviews pointing at a product that is not in the catalogue, prices outside $0–2000.
* **Model layer, severity `error`** — these are the contract the marts promise: uniqueness
  and not-null on every key, accepted values for ratings and price bands, referential
  integrity from `fct_reviews` to both dimensions, `helpfulness` within 0–1.

Three generic tests are custom: `accepted_range`, `unique_combination`, `not_blank`
(`beauty_dbt/tests/generic/`).

Every failing row is kept, not just counted: `store_failures` writes it to the `audit`
schema, then `pipeline/export_test_failures.py` exports each one to
`data/test_failures/<date>/<test>.csv` and appends a summary row to
`audit_log.test_failure_log`, giving a run-over-run history of data-quality issues. The
latest run's warnings also appear at the bottom of the dashboard.

### 4. LLM enrichment — `pipeline/enrich_reviews.py`

Draws a deterministic sample of 2,000 reviews stratified by star rating (400 per star) and
asks Gemini (`gemini-3.8-flash`, override with `GEMINI_MODEL`; structured output via a Pydantic response schema, batches
of 20) for three fields per review: `sentiment`, `complaint_type` (packaging, scent,
skin_reaction, texture, ineffective, price_value, other, none) and a short supporting quote.
Results append to `llm.review_tags`, which `mart_brand_complaints` joins back onto the fact
table, so the tags are queryable alongside everything else.

Reviews already tagged by the same labeler are skipped, so an interrupted or rate-limited
run resumes where it stopped; failed batches are simply retried next run. Without
`GEMINI_API_KEY` the step exits cleanly instead of failing the pipeline.

`--labeler keyword` swaps in an offline regex baseline. It exists so CI and anyone without
an API key can exercise the enrichment path end to end; its rows are tagged
`labeler = 'keyword_baseline'` and kept separate from Gemini's in every mart.

### 5. Dashboard — `dashboard/build_dashboard.py`

A single self-contained `reports/dashboard.html` (Plotly): KPI strip, complaints by brand
stacked by complaint type, price vs average rating (log price, point size by review count,
price-band averages annotated), reviews loaded per batch, and the latest run's data-quality
warnings.

### 6. Schedule — `.github/workflows/`

`daily_pipeline.yml` runs at 06:00 UTC daily (and on demand), restores `data/` from the
Actions cache so the watermark and warehouse carry across runs, runs `run_pipeline.sh`, and
uploads the dashboard and any failed-record CSVs as artifacts, then deploys the dashboard to
GitHub Pages. The only secret it needs is `GEMINI_API_KEY`; the Kaggle download is anonymous.
`GEMINI_SLEEP_SECONDS` (repo variable, default 6) paces Gemini calls for the free tier, and a
run stops tagging after 3 batches in a row fail (bad key, daily quota exhausted) so the
remainder is picked up the next day instead of burning the job on retries.

The backfill stops 30 days short of the newest review, so the daily loads have about a month
of real days to replay; after that, extract logs "Source exhausted" and the rest of the
pipeline re-runs over the same data.

`ci.yml` runs `pytest` and the synthetic end-to-end smoke run on every push and PR.

## Tests

```bash
pytest -q              # 13 unit tests: watermark logic, partitioning, idempotency, labeler, Gemini stop
./scripts/smoke_run.sh # end-to-end: two batches, dbt build (35 tests), enrichment, dashboard
```

## Layout

```
pipeline/        extract_load.py, enrich_reviews.py, export_test_failures.py, make_dev_fixture.py
beauty_dbt/      dbt project: models/{staging,marts}, custom generic tests, profiles.yml
dashboard/       build_dashboard.py
scripts/         smoke_run.sh
seeds/           product_info.csv (real catalogue, used by the synthetic fixture)
tests/           pytest unit tests
data/            generated: raw parquet, warehouse.duckdb, test_failures/   (gitignored)
reports/         generated: dashboard.html                                   (gitignored)
```

## Notes on the data

* `seeds/product_info.csv` is the real product catalogue (8,494 products, 304 brands).
* The review files are **not** in the repo; they come from Kaggle at run time. Scheduled runs
  use the real reviews; only CI and `smoke_run.sh` use the synthetic fixture.
* `pipeline/make_dev_fixture.py` generates **synthetic** reviews for real products so the
  pipeline can run without credentials. It deliberately injects duplicates, blank text,
  out-of-range ratings and orphan product ids so the data-quality tests have something to
  catch. A dashboard built from it carries a banner saying so — numbers from a synthetic
  run are not findings about real products.
* Daily arrival is simulated from a static dump, as described above; the dataset is not a
  live feed.
