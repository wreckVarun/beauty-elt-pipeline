"""LLM enrichment: tag a fixed sample of reviews with sentiment and complaint type.

Reads fct_reviews from the DuckDB warehouse, draws a deterministic sample stratified by
star rating (default 2,000 reviews, 400 per star), sends them to the Gemini API in
batches with a JSON response schema, and appends the tags to llm.review_tags.

Re-running is cheap: reviews already tagged by the same labeler are skipped, so an
interrupted run resumes where it stopped.

  GEMINI_API_KEY=... python -m pipeline.enrich_reviews            # Gemini (default)
  python -m pipeline.enrich_reviews --labeler keyword              # offline keyword baseline

Without GEMINI_API_KEY the Gemini labeler exits cleanly without tagging anything.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Literal

import duckdb
from pydantic import BaseModel

from pipeline import config

COMPLAINT_TYPES = ["packaging", "scent", "skin_reaction", "texture", "ineffective", "price_value", "other", "none"]
Sentiment = Literal["positive", "neutral", "negative"]
Complaint = Literal["packaging", "scent", "skin_reaction", "texture", "ineffective", "price_value", "other", "none"]

DDL = """
create schema if not exists llm;
create table if not exists llm.review_tags (
  review_id varchar, labeler varchar, model varchar, sentiment varchar,
  complaint_type varchar, is_complaint boolean, evidence varchar, tagged_at timestamp);
"""

PROMPT = """You label Sephora product reviews for a beauty analytics team.
For each review return:
- sentiment: positive, neutral or negative (overall tone of the review)
- complaint_type: the main problem the reviewer reports, one of
  packaging (pump, cap, leaks, broken container), scent (smell or fragrance),
  skin_reaction (breakouts, irritation, redness, burning, allergy),
  texture (greasy, sticky, pilling, consistency), ineffective (didn't work, no results),
  price_value (overpriced, too small for the price), other (any other problem),
  none (no complaint)
- evidence: the shortest quote (max 12 words) from the review supporting complaint_type, or "" if none.
Return one item per review, keeping each review's id.

Reviews:
{reviews}"""


class ReviewTag(BaseModel):
    id: str
    sentiment: Sentiment
    complaint_type: Complaint
    evidence: str


def sample_reviews(con, labeler: str, n: int, seed: int = 42) -> list[tuple[str, int, str]]:
    per_star = n // 5
    return con.execute(
        f"""
        with candidates as (
            select review_id, rating, coalesce(review_title || '. ', '') || review_text as text,
                   row_number() over (partition by rating order by hash(review_id || '{seed}')) as rn
            from marts.fct_reviews
            where review_text is not null and length(review_text) >= 20
        )
        select c.review_id, c.rating, left(c.text, 1500)
        from candidates c
        where c.rn <= {per_star}
          and c.review_id not in (select review_id from llm.review_tags where labeler = ?)
        order by c.rating, c.rn
        """,
        [labeler],
    ).fetchall()


# --- labelers -----------------------------------------------------------------------

KEYWORDS = [
    ("skin_reaction", r"break ?out|broke (me )?out|irritat|red(ness)?\b|rash|burn|itch|allerg|bumps|sting"),
    ("packaging", r"pump|packag|bottle|tube|\bcap\b|leak|lid|dispenser|container|jar broke"),
    ("scent", r"smell|scent|fragrance|odor|perfume"),
    ("texture", r"greasy|sticky|pill|oily|thick|tacky|texture"),
    ("price_value", r"overpriced|expensive|price|not worth|tiny"),
    ("ineffective", r"did nothing|no difference|didn'?t (work|do)|no results|waste"),
]


def keyword_label(text: str, rating: int) -> ReviewTag:
    t = text.lower()
    sentiment = "positive" if rating >= 4 else "negative" if rating <= 2 else "neutral"
    if rating >= 4:
        return ReviewTag(id="", sentiment=sentiment, complaint_type="none", evidence="")
    for kind, pat in KEYWORDS:
        m = re.search(pat, t)
        if m:
            return ReviewTag(id="", sentiment=sentiment, complaint_type=kind, evidence=m.group(0))
    return ReviewTag(id="", sentiment=sentiment, complaint_type="other" if rating <= 2 else "none", evidence="")


def gemini_label_batch(client, model: str, batch: list[tuple[str, int, str]], retries: int = 5) -> dict[int, ReviewTag]:
    """Returns {position in batch: tag}. Reviews the model skipped are simply absent."""
    from google.genai import types

    lines = "\n".join(f"[r{i}] {text}" for i, (_, _, text) in enumerate(batch))
    cfg = types.GenerateContentConfig(
        response_mime_type="application/json", response_schema=list[ReviewTag], temperature=0
    )
    for attempt in range(retries):
        try:
            resp = client.models.generate_content(model=model, contents=PROMPT.format(reviews=lines), config=cfg)
            tags = resp.parsed or []
            by_id = {t.id: t for t in tags}
            return {i: by_id[f"r{i}"] for i in range(len(batch)) if f"r{i}" in by_id}
        except Exception as e:  # rate limits / transient 5xx
            if getattr(e, "code", None) in (400, 401, 403, 404):  # bad request/key/model: retrying won't help
                print(f"  Gemini call failed ({e.__class__.__name__}: {e})", file=sys.stderr)
                break
            wait = 2 ** attempt * 5
            print(f"  Gemini call failed ({e.__class__.__name__}: {e}); retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    print("  giving up on this batch; it will be retried on the next run", file=sys.stderr)
    return {}


def write_tags(con, labeler: str, model: str, batch, tags: dict[int, ReviewTag]) -> int:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i, tag in tags.items():
        review_id = batch[i][0]
        rows.append((review_id, labeler, model, tag.sentiment, tag.complaint_type,
                     tag.complaint_type != "none", tag.evidence[:200], now))
    if not rows:  # a batch Gemini gave up on; duckdb rejects an empty executemany
        return 0
    con.executemany("insert into llm.review_tags values (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--labeler", choices=["gemini", "keyword"], default="gemini")
    ap.add_argument("--sample-size", type=int, default=config.ENRICH_SAMPLE_SIZE)
    ap.add_argument("--batch-size", type=int, default=20)
    ap.add_argument("--sleep", type=float, default=float(os.environ.get("GEMINI_SLEEP_SECONDS", "1")),
                    help="pause between Gemini calls, to stay under free-tier rate limits")
    ap.add_argument("--max-failed-batches", type=int, default=3,
                    help="stop after this many Gemini batches in a row fail (bad key, daily quota)")
    a = ap.parse_args()

    con = duckdb.connect(str(config.WAREHOUSE))
    con.execute(DDL)

    if a.labeler == "gemini":
        key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not key:
            print("GEMINI_API_KEY not set; skipping LLM enrichment.")
            return
        from google import genai

        client = genai.Client(api_key=key)
        labeler, model = "gemini", config.GEMINI_MODEL
    else:
        client, labeler, model = None, "keyword_baseline", "regex-v1"

    todo = sample_reviews(con, labeler, a.sample_size)
    print(f"{len(todo):,} reviews left to tag with {labeler} ({model})")
    written = failed_in_a_row = 0
    for i in range(0, len(todo), a.batch_size):
        batch = todo[i : i + a.batch_size]
        if client is None:
            tags = {j: keyword_label(text, rating) for j, (_, rating, text) in enumerate(batch)}
        else:
            tags = gemini_label_batch(client, model, batch)
            time.sleep(a.sleep)
            # A bad key or an exhausted daily quota fails every batch; stop instead of
            # spending ~2.5 min of retries on each one. The rest is picked up next run.
            failed_in_a_row = 0 if tags else failed_in_a_row + 1
            if failed_in_a_row >= a.max_failed_batches:
                print(f"  {failed_in_a_row} batches in a row failed; stopping, the rest will be tagged next run",
                      file=sys.stderr)
                break
        written += write_tags(con, labeler, model, batch, tags)
        if client is not None:
            print(f"  tagged {written:,}/{len(todo):,}")
    total = con.execute("select count(*) from llm.review_tags where labeler = ?", [labeler]).fetchone()[0]
    print(f"done: wrote {written:,} tags this run; llm.review_tags now holds {total:,} {labeler} rows")


if __name__ == "__main__":
    main()
