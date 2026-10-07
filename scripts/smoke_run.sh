#!/usr/bin/env bash
# End-to-end run against synthetic review data: no Kaggle credentials, no Gemini key needed.
set -euo pipefail
cd "$(dirname "$0")/.."
export DATA_DIR="${DATA_DIR:-$PWD/data}"
FIXTURE="${FIXTURE_DIR:-$(mktemp -d)/fixture}"

"${PYTHON:-python3}" -m pipeline.make_dev_fixture --products "${PRODUCTS_CSV:-seeds/product_info.csv}" --out "$FIXTURE" --n "${N:-20000}"
rm -rf "$DATA_DIR"
ENRICH_LABELER=keyword ./run_pipeline.sh --source dir --source-dir "$FIXTURE" --label synthetic-fixture
# one more day, to prove the incremental path
ENRICH_LABELER=keyword ./run_pipeline.sh --source dir --source-dir "$FIXTURE" --label synthetic-fixture
test -s reports/dashboard.html
echo "smoke run OK"
