#!/usr/bin/env bash
# One full daily run: Extract+Load -> dbt build -> LLM enrichment -> complaint mart -> dashboard.
# Usage: ./run_pipeline.sh [extract_load args...]   e.g. ./run_pipeline.sh --source kaggle
set -euo pipefail
cd "$(dirname "$0")"
export DATA_DIR="${DATA_DIR:-$PWD/data}"
DBT="dbt --no-use-colors"
PY="${PYTHON:-python3}"
DBT_ARGS="--project-dir beauty_dbt --profiles-dir beauty_dbt"

echo "== 1/5 extract + load raw parquet"
"$PY" -m pipeline.extract_load "$@"

echo "== 2/5 dbt build (staging, star schema, tests)"
status=0
$DBT build $DBT_ARGS --exclude mart_brand_complaints || status=$?
"$PY" -m pipeline.export_test_failures
[ $status -eq 0 ] || { echo "dbt build failed"; exit $status; }

echo "== 3/5 LLM enrichment (${ENRICH_LABELER:-gemini})"
"$PY" -m pipeline.enrich_reviews --labeler "${ENRICH_LABELER:-gemini}"

echo "== 4/5 complaint mart"
$DBT build $DBT_ARGS --select source:llm mart_brand_complaints
"$PY" -m pipeline.export_test_failures

echo "== 5/5 dashboard"
"$PY" -m dashboard.build_dashboard
