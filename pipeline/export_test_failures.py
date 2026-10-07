"""Log failed dbt test records for review.

dbt is configured with store_failures, so every failing row of every test lands in a
table in the `audit` schema. After `dbt build`, this script reads run_results.json,
and for each test that warned or failed:
  * exports its failing rows to data/test_failures/<run date>/<test>.csv
  * appends a summary row to audit_log.test_failure_log in the warehouse
so there is a durable history of data-quality issues across daily runs.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb

from pipeline import config


def main() -> int:
    target = config.DBT_DIR / "target"
    results = json.loads((target / "run_results.json").read_text())
    manifest = json.loads((target / "manifest.json").read_text())
    run_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    out_dir = config.FAILURES_DIR / run_at.strftime("%Y-%m-%d")

    con = duckdb.connect(str(config.WAREHOUSE))
    con.execute("create schema if not exists audit_log")
    con.execute(
        """create table if not exists audit_log.test_failure_log (
             run_at timestamp, test_name varchar, status varchar, failures bigint,
             audit_table varchar, export_path varchar)"""
    )

    n_tests, flagged = 0, []
    for r in results["results"]:
        uid = r["unique_id"]
        if not uid.startswith("test."):
            continue
        n_tests += 1
        if r["status"] not in ("warn", "fail") or not r.get("failures"):
            continue
        node = manifest["nodes"][uid]
        relation = node.get("relation_name")
        export = None
        if relation:
            out_dir.mkdir(parents=True, exist_ok=True)
            export = out_dir / f"{node['name']}.csv"
            con.execute(f"copy (select * from {relation} limit 10000) to '{export}' (header)")
        con.execute(
            "insert into audit_log.test_failure_log values (?, ?, ?, ?, ?, ?)",
            [run_at, node["name"], r["status"], r["failures"], relation, str(export) if export else None],
        )
        flagged.append((r["status"], r["failures"], node["name"]))

    print(f"{n_tests} dbt tests ran; {len(flagged)} flagged records")
    for status, failures, name in sorted(flagged):
        print(f"  {status.upper():4} {failures:>7,}  {name}")
    if flagged:
        print(f"failed records exported to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
