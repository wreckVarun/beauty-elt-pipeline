"""Shared paths and settings. Everything is driven by env vars so the same code
runs locally and in GitHub Actions."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", ROOT / "data")).resolve()
RAW_DIR = DATA_DIR / "raw"
STATE_FILE = RAW_DIR / "_state.json"
WAREHOUSE = DATA_DIR / "warehouse.duckdb"
FAILURES_DIR = DATA_DIR / "test_failures"
DBT_DIR = ROOT / "beauty_dbt"
REPORTS_DIR = ROOT / "reports"

KAGGLE_DATASET = "nadyinky/sephora-products-and-skincare-reviews"

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
ENRICH_SAMPLE_SIZE = int(os.environ.get("ENRICH_SAMPLE_SIZE", "2000"))
