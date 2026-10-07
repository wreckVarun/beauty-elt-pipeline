"""Build the static HTML dashboard from the warehouse.

Two panels the resume calls for:
  1. Top complaints by brand (from the LLM-tagged sample)
  2. Price vs rating across the catalogue
plus a pipeline-health strip (rows loaded per batch, data-quality warnings).

  python -m dashboard.build_dashboard   ->  reports/dashboard.html
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb
import plotly.graph_objects as go
import plotly.io as pio

from pipeline import config

# colour-blind-safe categorical palette, one colour per complaint type
PALETTE = ["#4C78A8", "#F58518", "#54A24B", "#E45756", "#72B7B2", "#B279A2", "#9D755D", "#BAB0AC"]
TEMPLATE = "plotly_white"


def q(con, sql, params=None):
    return con.execute(sql, params or []).df()


def fig_brand_complaints(con, labeler: str, top_n: int = 12) -> go.Figure:
    df = q(con, """
        with top_brands as (
            select brand_id from marts.mart_brand_complaints
            where labeler = ? and complaint_type <> 'none'
            group by brand_id order by sum(n_reviews) desc limit ?
        )
        select brand_name, complaint_type, n_reviews, share_of_brand_reviews
        from marts.mart_brand_complaints
        where labeler = ? and complaint_type <> 'none' and brand_id in (select brand_id from top_brands)
    """, [labeler, top_n, labeler])
    order = df.groupby("brand_name")["n_reviews"].sum().sort_values().index.tolist()
    fig = go.Figure()
    for i, ctype in enumerate(sorted(df["complaint_type"].unique())):
        d = df[df["complaint_type"] == ctype].set_index("brand_name").reindex(order).fillna(0)
        fig.add_bar(y=order, x=d["n_reviews"], name=ctype, orientation="h",
                    marker_color=PALETTE[i % len(PALETTE)],
                    hovertemplate="%{y}<br>" + ctype + ": %{x} reviews<extra></extra>")
    fig.update_layout(barmode="stack", template=TEMPLATE, height=460,
                      title="Complaints by brand (tagged review sample)",
                      xaxis_title="tagged reviews mentioning a problem", yaxis_title=None,
                      legend_title="complaint type", margin=dict(l=10, r=10, t=50, b=10))
    return fig


def fig_price_vs_rating(con) -> go.Figure:
    df = q(con, """
        select p.price_usd, avg(f.rating) as avg_rating, count(*) as n_reviews, p.product_name, b.brand_name
        from marts.fct_reviews f
        join marts.dim_products p using (product_id)
        join marts.dim_brands b on b.brand_id = p.brand_id
        where p.price_usd is not null
        group by p.product_id, p.price_usd, p.product_name, b.brand_name
        having count(*) >= 5
    """)
    band = q(con, """
        select p.price_band, avg(f.rating) as avg_rating, count(*) as n_reviews
        from marts.fct_reviews f join marts.dim_products p using (product_id)
        where p.price_band is not null group by p.price_band order by p.price_band
    """)
    fig = go.Figure()
    fig.add_scatter(x=df["price_usd"], y=df["avg_rating"], mode="markers",
                    marker=dict(size=(df["n_reviews"] ** 0.4).clip(4, 22), color=PALETTE[0], opacity=0.45,
                                line=dict(width=0)),
                    name="product",
                    text=df["brand_name"] + " — " + df["product_name"],
                    hovertemplate="%{text}<br>$%{x:.0f} · avg %{y:.2f}★<extra></extra>")
    fig.add_scatter(x=[None], y=[None], mode="lines", line=dict(color=PALETTE[1], width=3), name="price band average")
    for _, r in band.iterrows():
        fig.add_annotation(x=0, y=r["avg_rating"], xref="paper", yanchor="bottom", showarrow=False,
                           text=f"{r['price_band'][3:]}: {r['avg_rating']:.2f}★", font=dict(color=PALETTE[1], size=11))
    fig.update_layout(template=TEMPLATE, height=460, title="Price vs average rating (products with 5+ reviews)",
                      xaxis_title="price (USD, log scale)", yaxis_title="average rating",
                      xaxis_type="log", margin=dict(l=10, r=10, t=50, b=10))
    return fig


def fig_loads(con) -> go.Figure:
    df = q(con, "select batch_id, load_date, count(*) as n from marts.fct_reviews group by 1, 2 order by 2")
    fig = go.Figure(go.Bar(x=df["load_date"], y=df["n"], marker_color=PALETTE[2],
                           hovertemplate="%{x|%Y-%m-%d}: %{y:,} reviews<extra></extra>"))
    fig.update_layout(template=TEMPLATE, height=280, title="Reviews loaded per batch",
                      xaxis_title="load date", yaxis_title="reviews", margin=dict(l=10, r=10, t=50, b=10))
    return fig


def main() -> None:
    con = duckdb.connect(str(config.WAREHOUSE), read_only=True)
    labelers = q(con, "select labeler, count(*) n from marts.mart_brand_complaints group by 1 order by n desc")
    labeler = labelers["labeler"].iloc[0] if len(labelers) else None

    kpis = q(con, """
        select (select count(*) from marts.fct_reviews)                         as reviews,
               (select count(*) from marts.dim_products)                        as products,
               (select count(*) from marts.dim_brands)                          as brands,
               (select count(distinct batch_id) from marts.fct_reviews)         as batches,
               (select round(avg(rating), 2) from marts.fct_reviews)            as avg_rating
    """).iloc[0]
    tags = q(con, "select labeler, model, count(*) n from llm.review_tags group by 1, 2")
    try:
        failures = q(con, """select test_name, status, failures from audit_log.test_failure_log
                             where run_at = (select max(run_at) from audit_log.test_failure_log)
                             order by failures desc""")
    except duckdb.CatalogException:
        failures = None
    state = json.loads(config.STATE_FILE.read_text()) if config.STATE_FILE.exists() else {}
    source_note = state.get("source", "unknown")

    figs = [fig_price_vs_rating(con), fig_loads(con)]
    if labeler:
        figs.insert(0, fig_brand_complaints(con, labeler))
    body = "\n".join(pio.to_html(f, include_plotlyjs=("cdn" if i == 0 else False), full_html=False)
                     for i, f in enumerate(figs))

    warn_rows = "".join(
        f"<tr><td>{r.test_name}</td><td class='{r.status}'>{r.status}</td><td>{r.failures:,}</td></tr>"
        for r in failures.itertuples()) if failures is not None and len(failures) else \
        "<tr><td colspan='3'>No test failures in the latest run.</td></tr>"
    tag_note = " · ".join(f"{r.n:,} reviews tagged by {r.labeler} ({r.model})" for r in tags.itertuples()) or \
        "no LLM enrichment has run yet"
    banner = ("" if source_note != "synthetic-fixture" else
              "<p class='banner'>Review data in this build is <b>synthetic</b> (pipeline/make_dev_fixture.py). "
              "Numbers here are for exercising the pipeline, not real findings.</p>")

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Beauty Product Analytics</title>
<style>
  :root {{ color-scheme: light; --fg:#1b1f23; --muted:#5b6670; --line:#e3e7eb; --bg:#f7f8fa; }}
  body {{ margin:0; padding:24px 16px 48px; background:var(--bg); color:var(--fg);
         font:15px/1.5 -apple-system, "Segoe UI", Roboto, sans-serif; }}
  .wrap {{ max-width:1080px; margin:0 auto; }}
  h1 {{ font-size:24px; margin:0 0 4px; }}
  .sub {{ color:var(--muted); margin:0 0 20px; }}
  .kpis {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:12px; margin-bottom:20px; }}
  .kpi {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:14px 16px; }}
  .kpi b {{ display:block; font-size:22px; }}
  .kpi span {{ color:var(--muted); font-size:13px; }}
  .card {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:8px; margin-bottom:20px; }}
  table {{ width:100%; border-collapse:collapse; font-size:14px; }}
  th, td {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); }}
  td.warn {{ color:#9a6700; font-weight:600; }} td.fail {{ color:#b42318; font-weight:600; }}
  .banner {{ background:#fff4e5; border:1px solid #f0c38e; border-radius:8px; padding:10px 14px; }}
  footer {{ color:var(--muted); font-size:13px; }}
</style></head>
<body><div class="wrap">
<h1>Beauty Product Analytics</h1>
<p class="sub">Built from the Sephora products &amp; reviews dataset by the dbt + DuckDB ELT pipeline.
  Source: {source_note}.</p>
{banner}
<div class="kpis">
  <div class="kpi"><b>{kpis.reviews:,}</b><span>reviews in fact table</span></div>
  <div class="kpi"><b>{kpis.products:,}</b><span>products</span></div>
  <div class="kpi"><b>{kpis.brands:,}</b><span>brands</span></div>
  <div class="kpi"><b>{kpis.batches:,}</b><span>load batches</span></div>
  <div class="kpi"><b>{kpis.avg_rating}</b><span>average rating</span></div>
</div>
<div class="card">{body}</div>
<div class="card" style="padding:14px 16px">
  <h2 style="font-size:17px;margin:0 0 8px">Data quality, latest run</h2>
  <table><thead><tr><th>test</th><th>status</th><th>rows flagged</th></tr></thead><tbody>{warn_rows}</tbody></table>
</div>
<footer>LLM enrichment: {tag_note}. Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC.</footer>
</div></body></html>"""

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS_DIR / "dashboard.html"
    out.write_text(html)
    print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
