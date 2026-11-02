"""Explicit synthetic publication tests in an isolated staging dataset ONLY.

Uses the existing Action credentials. No Sheets/provider reads, permission
changes, model promotion or production-dataset writes. A real source-policy
capture is a separate required staging run after these transport tests pass.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import re
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import archive_coherent_snapshot as co  # noqa: E402


def target(project, dataset):
    if not re.fullmatch(r"tfb_archive_staging_[A-Za-z0-9_]{1,128}", dataset):
        raise co.Refused("staging validation requires an isolated tfb_archive_staging_* dataset")
    co.promotion_sql(project + "." + dataset, "dashboard_stage_" + "0" * 64)


def fixture(source="90001", attempt=2):
    now = dt.datetime.now(dt.timezone.utc)
    capture = co.digest({"synthetic-staging-only": uuid.uuid4().hex})
    receipt = {"schema": "tfb.synthetic-publication-test/v1", "capture_id": capture,
               "source_run_id": source, "source_attempt": attempt, "captured_at": now.isoformat(),
               "snapshot_date": now.date().isoformat(), "tab_digests": {},
               "capture_contract": {"accepted": False, "synthetic_fixture": True,
                                    "trading_permission_granted": False}, "feed_promoted": False}
    rows = []
    for tab in co.CAPTURE_TABS:
        grid = [["Symbol", "Name"], ["TFB_STAGING_ONLY", "Synthetic publication test, not investment data"]]
        got, _ = co.arc.build_rows(tab, grid, 0, receipt["snapshot_date"], receipt["captured_at"], "staging-test")
        receipt["tab_digests"][tab] = co.digest(grid)
        for row in got:
            row.update(capture_id=capture, source_run_id=None)
        rows.extend(got)
    receipt["row_count"] = len(rows)
    rows.append({"snapshot_date": receipt["snapshot_date"], "captured_at": receipt["captured_at"],
                 "run_id": "staging-test", "tab": "_Capture_Receipt", "row_num": 0, "cells": [],
                 "record_json": co.encoded(receipt).decode(), "capture_id": capture, "source_run_id": source})
    return rows, receipt


def query(session, project, location, sql):
    job = "tfb_staging_read_" + uuid.uuid4().hex
    config = {"query": {"query": sql, "useLegacySql": False, "useQueryCache": False}}
    co.run_job(session, project, location, job, config)
    result = session.get(f"{co.arc.BQ_API}/projects/{project}/queries/{job}?location={location}&maxResults=100", timeout=60)
    if result.status_code != 200 or result.json().get("pageToken"):
        raise co.Unknown("staging query readback unavailable")
    return [[cell.get("v") for cell in row["f"]] for row in result.json().get("rows", [])]


def signature(session, project, dataset, location, day):
    parts = []
    for tab, _ in co.arc.TABS:
        table = co.arc.table_name(tab)
        parts.append(f"SELECT '{table}', COUNT(*), TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(t), '\\n' ORDER BY row_num))) "
                     f"FROM `{project}.{dataset}.{table}` AS t WHERE snapshot_date = DATE '{day}'")
    return sorted(query(session, project, location, " UNION ALL ".join(parts)))


def expect_failure(session, project, location, job, config, marker):
    try:
        co.run_job(session, project, location, job, config)
    except co.Refused:
        actual = co.get_job(session, project, location, job)
        if marker in actual.get("status", {}).get("errorResult", {}).get("message", ""):
            return
    raise co.Refused("staging negative did not reach its expected policy/rollback assertion")


def run(session, project, dataset, location):
    target(project, dataset)
    if not co.arc.dataset_exists(session, project, dataset):
        raise co.Refused("create the private staging dataset first; no IAM change is performed here")
    rows, receipt = fixture()
    co.publish(session, project, dataset, location, rows, receipt)
    before = signature(session, project, dataset, location, receipt["snapshot_date"])
    co.publish(session, project, dataset, location, rows, receipt)
    if signature(session, project, dataset, location, receipt["snapshot_date"]) != before:
        raise co.Refused("idempotent replay changed source tables")
    rollback = {"query": {"useLegacySql": False, "query": "BEGIN TRANSACTION; "
        f"DELETE FROM `{project}.{dataset}.my_portfolio` WHERE snapshot_date = DATE '{receipt['snapshot_date']}'; "
        "ASSERT FALSE AS 'TFB_STAGING_ROLLBACK_EXPECTED'; COMMIT TRANSACTION;"}}
    expect_failure(session, project, location, "tfb_staging_rollback_" + receipt["capture_id"], rollback,
                   "TFB_STAGING_ROLLBACK_EXPECTED")
    old_rows, old = fixture(source="90000", attempt=1)
    try:
        co.publish(session, project, dataset, location, old_rows, old)
    except co.Refused:
        actual = co.get_job(session, project, location, "tfb_capture_commit_" + old["capture_id"])
        if "newer capture already published" not in actual.get("status", {}).get("errorResult", {}).get("message", ""):
            raise
    else:
        raise co.Refused("older source was not refused")
    if signature(session, project, dataset, location, receipt["snapshot_date"]) != before:
        raise co.Refused("negative test changed source tables")
    sql = (ROOT / "docs/evidence/dashboard_refresh_status_query.sql").read_text()
    sql = sql.replace("`project-2e3530e6-2311-4aa3-b36.tfb_archive.", f"`{project}.{dataset}.")
    if "`project-2e3530e6-2311-4aa3-b36.tfb_archive." in sql:
        raise co.Refused("status query still points at the production dataset")
    if len(query(session, project, location, sql)) != 1:
        raise co.Refused("singleton status unavailable before first diagnostic")
    out = {"published": False, "outcome": "withheld", "reason": "STAGING_SOURCE_FAILURE", "diagnostic": {
        "reason_codes": ["PORTFOLIO_CONSUMED_COUNT_MISMATCH"],
        "display": {"consumed_holdings": 6, "published_holdings": 2, "trading_permission_granted": False}}}
    co.publish_attempt(session, project, dataset, location, out, "90001")
    if len(query(session, project, location, sql)) != 1:
        raise co.Refused("latest diagnostic status unavailable")
    return {"synthetic_only": True, "dataset": dataset, "replay": "PASS", "rollback": "PASS",
            "older_source_refused": "PASS", "diagnostic_readback": "PASS", "status_query": "PASS",
            "real_source_policy_capture": "PENDING", "trading_permission_granted": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    project, dataset = co.arc._env("TFB_BQ_PROJECT"), co.arc._env("TFB_BQ_DATASET")
    target(project, dataset)  # Refuse production before authenticating.
    if not co.armed() or not co.arc.archive_enabled():
        raise co.Refused("staging validation remains behind both archive gates")
    if not args.write:
        print(json.dumps({"dry_run": True, "dataset": dataset, "synthetic_only": True}))
        return 0
    result = run(co.arc.make_session(), project, dataset, co.arc._env("TFB_BQ_LOCATION", "US"))
    Path("coherent_staging_validation.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
