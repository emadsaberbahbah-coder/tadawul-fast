"""Gated, read-only capture and atomic BigQuery publication after a full sync.

The legacy archive path is unchanged while TFB_BQ_AFTER_SYNC is off. This path
never writes Sheets, runs a provider, or releases a recommendation. It requires
the source audits to validate the *captured* source cohort, then
rechecks the captured source grids before publishing. Independent news/calendar/
learning tabs retain their own content and capture times; they are not claimed
to have been produced by the sync. Every capture is retained in private history.

BigQuery load/query submissions have deterministic job IDs. An uncertain POST
or upload is reconciled through jobs.get, never blindly submitted again. All
current-day archive partitions and the history/receipt commit in one transaction.
No portfolio cache or scoring change is made here.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.secret_redaction import safe_error_text  # noqa: E402
from scripts import archive_to_bigquery as arc  # noqa: E402
from scripts import await_cockpit_receipts as awaiter  # noqa: E402
from scripts import finalize_decision_publication as final  # noqa: E402
from scripts import release_decision_feed as release  # noqa: E402

SCRIPT_VERSION = "1.0.0"
GATE = "TFB_BQ_AFTER_SYNC"
HISTORY = "dashboard_snapshot_rows"
RECEIPTS = "dashboard_snapshot_receipts"
ATTEMPTS = "dashboard_refresh_attempts"
CRITICAL = frozenset(final.REQUIRED_PAGES | {
    "Top_10_Investments", "Portfolio_Decision", "_Status", "_Portfolio_CostBasis"})
SOURCE_OUTPUTS = frozenset(final.REQUIRED_PAGES | {"Top_10_Investments", "Portfolio_Decision"})
CAPTURE_TABS = tuple(dict.fromkeys([name for name, _ in arc.TABS] + sorted(CRITICAL)))
MAX_BYTES = 128 * 1024 * 1024
HISTORY_SCHEMA = arc.SCHEMA + [
    {"name": "capture_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source_run_id", "type": "STRING", "mode": "NULLABLE"},
]
RECEIPT_SCHEMA = [
    {"name": "capture_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source_run_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source_attempt", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "receipt_json", "type": "STRING", "mode": "REQUIRED"},
    {"name": "committed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]
ATTEMPT_SCHEMA = [
    {"name": "attempt_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "attempted_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "source_run_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "capture_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "REQUIRED"},
    {"name": "reason", "type": "STRING", "mode": "REQUIRED"},
    {"name": "diagnostic_json", "type": "STRING", "mode": "REQUIRED"},
]


class Refused(ValueError):
    """No production publication is permitted."""
    def __init__(self, message, *, details=None):
        super().__init__(message)
        self.details = details or {}


class Unknown(RuntimeError):
    """A job may exist or have committed; recover its exact ID before retry."""


def armed() -> bool:
    return os.getenv(GATE, "").strip().lower() in {"1", "on", "true", "yes"}


def wait_for_receipts(session, sid, source, *, max_seconds=2700,
                      clock=time.monotonic, sleep=time.sleep):
    """Bounded read-only wait; timeout/stopped never mean accepted."""
    end = clock() + min(max(int(max_seconds), 0), 2700)
    while True:
        grids = {tab: get_grid(session, sid, tab) for tab in
                 ("_Status", "Portfolio_Decision", "Top_10_Investments")}
        verdict = awaiter.evaluate_wait(source, slice_grid(grids["_Status"], "L1:M60"),
                                        slice_grid(grids["Portfolio_Decision"], "A1:B3"),
                                        slice_grid(grids["Top_10_Investments"], "A1:B3"))
        if verdict["state"] == "consumed" and all(verdict[name].get("state") == "OK" for name in ("pf", "t10")):
            return
        if verdict["state"] == "stop" or clock() >= end:
            info = display_evidence(grids)
            codes = ["SOURCE_CONSUMPTION_UNPROVEN"]
            if info["consumed_holdings"] != info["published_holdings"]:
                codes.append("PORTFOLIO_CONSUMED_COUNT_MISMATCH")
            raise Refused("cockpit receipts not accepted: " + verdict["reason"],
                          details={"reason_codes": codes, "display": info})
        sleep(min(60, max(0, end - clock())))


def encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def display_evidence(grids: dict) -> dict:
    """Display state is evidence, never a trading permission or a source clock."""
    pf = final.freshness._cell(grids.get("Portfolio_Decision", []), 1, 1)
    top10 = final.freshness._cell(grids.get("Top_10_Investments", []), 1, 1)
    holdings = re.findall(r"\bholdings\s+([0-9]+)\b", str(pf), re.I)
    selected = re.findall(r"\bSelected\s*=\s*([0-9]+)\s+exec\b", str(top10), re.I)
    published = final.freshness.parse_status_grid(grids.get("_Status", [])).get("My_Portfolio")
    states = {name: awaiter.surface_receipt(grids.get(tab, [])) for name, tab in
              (("portfolio", "Portfolio_Decision"), ("top10", "Top_10_Investments"))}
    flags = sorted(set(re.findall(r"\b(?:WITHHELD|NO_NEW_ENTRY|LEARNING_PENDING|NOT_DECIDABLE|INSUFFICIENT_EVIDENCE)\b",
                                 str(pf) + " " + str(top10), re.I)))
    feed = None
    try:
        key_grid = slice_grid(grids.get("_Status", []), "L1:M60")
        slot = final.publication_slot(key_grid)
        feed = str(key_grid[slot - 1][1]).split("|", 1)[0].strip()
    except (TypeError, ValueError, IndexError):
        pass
    return {"surfaces": states, "business_flags": flags, "decision_feed_state": feed,
            "consumed_holdings": int(holdings[0]) if len(holdings) == 1 else None,
            "published_holdings": published.rows if published else None,
            "executable_count": int(selected[0]) if len(selected) == 1 else None,
            "trading_permission_granted": False}


async def audit_capture(sid, *, reader, registry=None, publish=False, run_id):
    """Run source contracts directly; do not call the release/certification API."""
    if publish:
        raise Refused("dashboard auditor cannot write a decision feed")
    scope = final.load_scope()
    rules, policy = final.final_policy(approved_scope=scope)
    policy.update(expected_source_run=run_id, approved_scope=scope.receipt())
    full, decision = await asyncio.gather(
        final.coverage.run(sid, 20000, reader=reader, registry=registry,
                           policy_rules=rules, approved_scope=scope),
        final.freshness.run_live(sid, reader=reader, approved_scope=scope,
                                **{key: value for key, value in policy.items() if key != "approved_scope"}),
    )
    return {"run_id": run_id, "policy": policy, "audit_exit_codes": [full.code, decision.exit_code],
            "audits": {"coverage": full.payload(), "decision": decision.payload()},
            "feed_promoted": False}


def dashboard_admission(result: dict, source: str, grids: dict) -> dict:
    """Source/clock/account integrity only. Economic withholding is displayable.

    Native route status WITHHELD denotes a blocked SOURCE, not an economic
    output. Its source-readiness WITHHELD cannot be overridden by zero tickets.
    Economic `output: WITHHELD`, NO_NEW_ENTRY and learning-pending text with an
    OK route/current VERIFIED source remain eligible and are copied unchanged.
    The auditor's unfortunately named `decision.executable` is never consulted.
    """
    reasons, info = [], display_evidence(grids)
    audits = result.get("audits", {})
    coverage, decision = audits.get("coverage", {}), audits.get("decision", {})
    if result.get("run_id") != source or result.get("policy", {}).get("expected_source_run") != source:
        reasons.append("SOURCE_COHORT_UNPROVEN")
    if (result.get("audit_exit_codes") != [0, 0]
            or coverage.get("summary", {}).get("exit_code") != 0
            or decision.get("summary", {}).get("exit_code") != 0
            or decision.get("findings") != []):
        reasons.append("SOURCE_CONTRACT_FAILED")
    pages = coverage.get("pages", [])
    if len(pages) != len(final.REQUIRED_PAGES) or {page.get("page") for page in pages} != final.REQUIRED_PAGES:
        reasons.append("SOURCE_SCHEMA_OR_BOUNDS_UNPROVEN")
    try:
        scope = final.load_scope()
        if result.get("policy", {}).get("approved_scope") != scope.receipt():
            raise ValueError("policy differs")
        for page in scope.pages:
            for receipt in (next(p.get("approved_scope") for p in pages if p.get("page") == page),
                            decision.get("source_status", {}).get(page, {}).get("approved_scope")):
                if (not final.receipt_matches_policy(receipt, policy=scope)
                        or not final.complete_membership_proven(receipt, page, scope)):
                    raise ValueError("membership/bounds differ")
        if decision.get("top10_pool_reconciliation", {}).get("valid") is not True:
            raise ValueError("pool unproven")
    except (ValueError, TypeError, KeyError, StopIteration):
        reasons.append("SOURCE_MEMBERSHIP_OR_POOL_UNPROVEN")
    publications = final.freshness._source_publication_runs(grids.get("_Status", []))
    if any(publications.get(page) != source for page in (*final.freshness.POOL_PAGES, "My_Portfolio")):
        reasons.append("SOURCE_PUBLICATION_COHORT")
    if any(surface.get("consumed_run") != source or surface.get("state") != "OK"
           for surface in info["surfaces"].values()):
        reasons.append("SOURCE_CONSUMPTION_UNPROVEN")
    if (info["consumed_holdings"] is None or info["published_holdings"] is None
            or info["consumed_holdings"] != info["published_holdings"]):
        reasons.append("PORTFOLIO_CONSUMED_COUNT_MISMATCH")
    if info["business_flags"] and info["executable_count"] != 0:
        reasons.append("BUSINESS_WITHHELD_EXECUTABLE_COUNT_UNPROVEN")
    reasons.extend(item.get("code", "UNKNOWN_SOURCE_FINDING") for item in decision.get("findings", []))
    return {"schema": "tfb.dashboard-admission/v1", "accepted": not reasons,
            "reason_codes": sorted(set(reasons)), "display": info,
            "trading_permission_granted": False}


def get_grid(session, sid: str, tab: str) -> list:
    # Whole populated values response, with explicit failure at either bound;
    # never infer the extent from allocated rowCount or silently truncate it.
    rng = quote("'" + tab.replace("'", "''") + "'", safe="")
    response = session.get(
        f"{arc.SHEETS_API}/{sid}/values/{rng}?majorDimension=ROWS"
        "&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING", timeout=300)
    if response.status_code != 200:
        raise Refused(f"read {tab}: HTTP {response.status_code}")
    grid = response.json().get("values", [])
    if not isinstance(grid, list) or any(not isinstance(row, list) for row in grid):
        raise Refused(f"read {tab}: values unavailable")
    if len(grid) > arc.MAX_ROWS_PER_TAB or any(len(row) > arc.MAX_COLS for row in grid):
        raise Refused(f"read {tab}: capture bound exceeded")
    if len(encoded(grid)) > MAX_BYTES:
        raise Refused(f"read {tab}: byte bound exceeded")
    return grid


def slice_grid(grid: list, a1: str) -> list:
    """Frozen reader for only the audit's supported, bounded A1 shapes."""
    match = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)?:([A-Z]+)([1-9][0-9]*)?", a1)
    if not match:
        raise Refused("unsupported frozen audit range")
    def col(text):
        result = 0
        for c in text:
            result = result * 26 + ord(c) - 64
        return result - 1
    lo, start, hi, end = match.groups()
    left, right = col(lo), col(hi) + 1
    rows = grid[int(start or 1) - 1:int(end) if end else None]
    return [copy.deepcopy(row[left:right]) for row in rows]


def frozen_reader(grids: dict, sid: str):
    def read(sheet, a1):
        if sheet != sid or "!" not in a1:
            raise Refused("frozen reader identity mismatch")
        tab, bounds = a1.split("!", 1)
        tab = tab.strip("'")
        if tab not in grids:
            raise Refused("audit requested an uncaptured tab")
        return slice_grid(grids[tab], bounds)
    return read


def table_schema_ok(body: dict, schema: list, *, partition=True) -> bool:
    fields = body.get("schema", {}).get("fields", [])
    typed = lambda rows: [(f.get("name"), f.get("type"), f.get("mode", "NULLABLE")) for f in rows]
    return (typed(fields) == typed(schema)
            and (not partition or (body.get("timePartitioning", {}).get("field") == "snapshot_date"
                                   and body.get("timePartitioning", {}).get("type") == "DAY")))


def ensure_table(session, project, dataset, table, schema, *, partition=True, expires=None):
    url = f"{arc.BQ_API}/projects/{project}/datasets/{dataset}/tables"
    response = session.get(f"{url}/{table}", timeout=60)
    if response.status_code == 404:
        body = {"tableReference": {"projectId": project, "datasetId": dataset, "tableId": table},
                "schema": {"fields": schema},
                "description": "Private TFB coherent dashboard capture; no broker permission."}
        if partition:
            body["timePartitioning"] = {"type": "DAY", "field": "snapshot_date"}
        if expires is not None:
            body["expirationTime"] = str(expires)
        created = session.post(url, json=body, timeout=60)
        if created.status_code not in (200, 201, 409):
            raise Refused(f"create {table}: HTTP {created.status_code}")
        # A successful POST or a racing 409 is not schema evidence.
        response = session.get(f"{url}/{table}", timeout=60)
    identity = {"projectId": project, "datasetId": dataset, "tableId": table}
    if (response.status_code != 200 or response.json().get("tableReference") != identity
            or not table_schema_ok(response.json(), schema, partition=partition)):
        raise Refused(f"{table}: schema or partition unavailable/incompatible")


def get_job(session, project, location, job_id):
    response = session.get(f"{arc.BQ_API}/projects/{project}/jobs/{job_id}?location={location}", timeout=60)
    if response.status_code == 404:
        return None
    if response.status_code != 200:
        raise Unknown(f"job {job_id}: readback unavailable")
    return response.json()


def await_job(session, project, location, job_id, config, *, clock=time.monotonic, sleep=time.sleep):
    end = clock() + 900
    while True:
        body = get_job(session, project, location, job_id)
        if body is None:
            raise Unknown(f"job {job_id}: submission result unknown; no replay")
        ref = body.get("jobReference", {})
        if ref.get("projectId") != project or ref.get("jobId") != job_id or ref.get("location") != location:
            raise Refused("job identity unavailable or conflicting")
        for kind, wanted in config.items():
            actual = body.get("configuration", {}).get(kind, {})
            if any(actual.get(key) != value for key, value in wanted.items()):
                raise Refused("job configuration unavailable or conflicting")
        status = body.get("status", {})
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise Refused(f"job {job_id}: failed; result not accepted")
            return body
        if clock() >= end:
            raise Unknown(f"job {job_id}: completion unknown; no replay")
        sleep(2)


def run_job(session, project, location, job_id, config, data=None):
    """One submission/upload only; GET reconciles every uncertain result."""
    existing = get_job(session, project, location, job_id)
    if existing is not None:
        return await_job(session, project, location, job_id, config)
    body = {"jobReference": {"projectId": project, "jobId": job_id, "location": location},
            "configuration": config}
    try:
        if data is None:
            response = session.post(f"{arc.BQ_API}/projects/{project}/jobs", json=body, timeout=60)
            if response.status_code not in (200, 201, 409):
                raise Unknown(f"job {job_id}: submission unconfirmed")
        else:
            response = session.post(f"{arc.BQ_UPLOAD}/projects/{project}/jobs?uploadType=resumable",
                                    json=body, timeout=60,
                                    headers={"X-Upload-Content-Type": "application/octet-stream",
                                             "X-Upload-Content-Length": str(len(data))})
            if response.status_code not in (200, 201):
                raise Unknown(f"job {job_id}: load submission unconfirmed")
            url = (getattr(response, "headers", {}) or {}).get("Location")
            if not url:
                raise Unknown(f"job {job_id}: upload URL unavailable")
            uploaded = session.put(url, data=data, timeout=600,
                                   headers={"Content-Type": "application/octet-stream"})
            if uploaded.status_code not in (200, 201):
                raise Unknown(f"job {job_id}: upload unconfirmed")
    except Exception:
        # It may have committed before the response was lost. No re-POST/PUT.
        return await_job(session, project, location, job_id, config)
    return await_job(session, project, location, job_id, config)


def promotion_sql(ds: str, stage: str) -> str:
    """All changing literals are query parameters, identifiers are allowlisted."""
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,1023}", ds):
        raise Refused("invalid dataset identifier")
    if not re.fullmatch(r"dashboard_stage_[0-9a-f]{64}", stage):
        raise Refused("invalid staging identifier")
    columns = ", ".join(f"`{field['name']}`" for field in arc.SCHEMA)
    lines = [f"DECLARE r STRING DEFAULT (SELECT record_json FROM `{ds}.{stage}` "
             "WHERE tab = '_Capture_Receipt' AND capture_id = @capture);",
             "DECLARE d DATE DEFAULT SAFE_CAST(JSON_VALUE(r, '$.snapshot_date') AS DATE);",
             "DECLARE t TIMESTAMP DEFAULT SAFE_CAST(JSON_VALUE(r, '$.captured_at') AS TIMESTAMP);",
             "DECLARE a INT64 DEFAULT SAFE_CAST(JSON_VALUE(r, '$.source_attempt') AS INT64);",
             "ASSERT r IS NOT NULL AND d IS NOT NULL AND t IS NOT NULL AND a > 0 "
             "AND JSON_VALUE(r, '$.capture_id') = @capture AND JSON_VALUE(r, '$.source_run_id') = @source "
             "AS 'staged receipt is unavailable';",
             "BEGIN TRANSACTION;",
             f"IF NOT EXISTS (SELECT 1 FROM `{ds}.{RECEIPTS}` WHERE capture_id = @capture) THEN",
             f"ASSERT NOT EXISTS (SELECT 1 FROM `{ds}.{RECEIPTS}` WHERE "
             "SAFE_CAST(source_run_id AS INT64) > CAST(@source AS INT64) OR "
             "(source_run_id = @source AND (source_attempt > a OR "
             "(source_attempt = a AND captured_at > t)))) AS 'newer capture already published';"]
    for tab, _ in arc.TABS:
        table = arc.table_name(tab)
        lines.extend([f"DELETE FROM `{ds}.{table}` WHERE snapshot_date = d;",
                      f"INSERT INTO `{ds}.{table}` ({columns}) SELECT {columns} "
                      f"FROM `{ds}.{stage}` WHERE tab = '{tab}';"])
    lines.extend([
        f"INSERT INTO `{ds}.{HISTORY}` SELECT * FROM `{ds}.{stage}`;",
        f"INSERT INTO `{ds}.{RECEIPTS}` VALUES (@capture, @source, a, t, d, r, CURRENT_TIMESTAMP());",
        "END IF;", "COMMIT TRANSACTION;"])
    return "\n".join(lines)


def publish(session, project, dataset, location, rows, receipt):
    capture = receipt["capture_id"]
    stage = "dashboard_stage_" + capture
    sql = promotion_sql(f"{project}.{dataset}", stage)
    for tab, _ in arc.TABS:
        ensure_table(session, project, dataset, arc.table_name(tab), arc.SCHEMA)
    ensure_table(session, project, dataset, HISTORY, HISTORY_SCHEMA)
    ensure_table(session, project, dataset, RECEIPTS, RECEIPT_SCHEMA)
    ensure_table(session, project, dataset, ATTEMPTS, ATTEMPT_SCHEMA)
    ensure_table(session, project, dataset, stage, HISTORY_SCHEMA,
                 expires=int(time.time() * 1000) + 7 * 86400000)
    load = {"load": {"destinationTable": {"projectId": project, "datasetId": dataset, "tableId": stage},
                     "sourceFormat": "NEWLINE_DELIMITED_JSON", "writeDisposition": "WRITE_EMPTY",
                     "createDisposition": "CREATE_NEVER", "schema": {"fields": HISTORY_SCHEMA}, "maxBadRecords": 0}}
    run_job(session, project, location, "tfb_capture_load_" + capture, load, arc.ndjson(rows))
    params = {"capture": ("STRING", capture), "source": ("STRING", receipt["source_run_id"])}
    query = {"query": {"query": sql,
                       "useLegacySql": False, "parameterMode": "NAMED",
                       "queryParameters": [{"name": name, "parameterType": {"type": typ},
                                            "parameterValue": {"value": str(value)}}
                                           for name, (typ, value) in params.items()]}}
    run_job(session, project, location, "tfb_capture_commit_" + capture, query)
    verify = {"query": {"query": f"SELECT receipt_json, CAST((SELECT COUNT(*) FROM `{project}.{dataset}.{HISTORY}` "
                         "WHERE capture_id = @capture AND tab != '_Capture_Receipt') AS STRING) "
                         f", (SELECT capture_id FROM `{project}.{dataset}.{RECEIPTS}` "
                         "ORDER BY SAFE_CAST(source_run_id AS INT64) DESC, source_attempt DESC, captured_at DESC LIMIT 1) "
                         f"FROM `{project}.{dataset}.{RECEIPTS}` WHERE capture_id = @capture",
                         "useLegacySql": False, "useQueryCache": False, "parameterMode": "NAMED",
                         "queryParameters": [query["query"]["queryParameters"][0]]}}
    # Only this read-only query is new on each invocation: reusing an old query
    # result would falsely claim an old committed capture was still current.
    job_id = "tfb_capture_verify_" + capture + "_" + uuid.uuid4().hex[:16]
    try:
        run_job(session, project, location, job_id, verify)
        result = session.get(f"{arc.BQ_API}/projects/{project}/queries/{job_id}?location={location}&maxResults=2", timeout=60)
    except Exception:
        raise Unknown("capture commit confirmed; current receipt verification unavailable") from None
    if result.status_code != 200:
        raise Unknown("committed capture receipt readback unavailable")
    try:
        values = result.json().get("rows", [])
        if len(values) != 1 or result.json().get("pageToken"):
            raise ValueError("incomplete")
        persisted = json.loads(values[0]["f"][0]["v"])
        if (persisted.get("capture_id") != capture or persisted.get("tab_digests") != receipt["tab_digests"]
                or int(values[0]["f"][1]["v"]) != receipt["row_count"]):
            raise ValueError("does not reconcile")
        return persisted, values[0]["f"][2]["v"] == capture
    except (TypeError, ValueError, KeyError, IndexError):
        raise Unknown("committed capture receipt readback incomplete or inconsistent") from None


def publish_attempt(session, project, dataset, location, out, source):
    """Metadata only, also when a source capture is refused. No raw promotion.

    Looker must show this separately from the last coherent data receipt. A
    diagnostic storage outage is UNKNOWN, not a successful dashboard refresh.
    """
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,1023}", project + "." + dataset):
        raise Refused("diagnostic dataset identifier is invalid")
    now = dt.datetime.now(dt.timezone.utc)
    run = arc._env("GITHUB_RUN_ID")
    attempt_key = run + ":" + arc._env("GITHUB_RUN_ATTEMPT", "1") if run else "local:" + uuid.uuid4().hex
    attempt_id = hashlib.sha256(attempt_key.encode()).hexdigest()
    receipt = out.get("receipt", {})
    details = out.get("diagnostic") or receipt.get("capture_contract") or {}
    state = ("DATA_CURRENT" if out.get("published") else "SUPERSEDED" if out.get("committed")
             else "UNKNOWN" if out.get("outcome") in {"unknown", "fault"} else "WITHHELD")
    row = {"attempt_id": attempt_id, "snapshot_date": now.date().isoformat(),
           "attempted_at": now.isoformat(), "source_run_id": source if source and source.isdigit() else None,
           "capture_id": receipt.get("capture_id"), "state": state,
           "reason": out.get("reason", ""), "diagnostic_json": encoded({
               "capture_admitted": receipt.get("capture_contract", {}).get("accepted") is True,
               "details": details, "feed_promoted": False, "trading_permission_granted": False}).decode()}
    # The empty receipt table lets the diagnostic query safely LEFT JOIN before
    # the first successful capture; no source tables or source rows are created.
    ensure_table(session, project, dataset, RECEIPTS, RECEIPT_SCHEMA)
    ensure_table(session, project, dataset, ATTEMPTS, ATTEMPT_SCHEMA)
    load = {"load": {"destinationTable": {"projectId": project, "datasetId": dataset, "tableId": ATTEMPTS},
                     "sourceFormat": "NEWLINE_DELIMITED_JSON", "writeDisposition": "WRITE_APPEND",
                     "createDisposition": "CREATE_NEVER", "schema": {"fields": ATTEMPT_SCHEMA}, "maxBadRecords": 0}}
    run_job(session, project, location, "tfb_refresh_attempt_" + attempt_id, load, arc.ndjson([row]))
    query = {"query": {"query": "SELECT attempt_id, CAST(attempted_at AS STRING), state, source_run_id, capture_id, reason, diagnostic_json "
                       f"FROM `{project}.{dataset}.{ATTEMPTS}` WHERE attempt_id = @attempt LIMIT 2",
                       "useLegacySql": False, "useQueryCache": False, "parameterMode": "NAMED",
                       "queryParameters": [{"name": "attempt", "parameterType": {"type": "STRING"},
                                            "parameterValue": {"value": attempt_id}}]}}
    job = "tfb_refresh_attempt_check_" + attempt_id
    run_job(session, project, location, job, query)
    check = session.get(f"{arc.BQ_API}/projects/{project}/queries/{job}?location={location}&maxResults=2", timeout=60)
    values = check.json().get("rows", []) if check.status_code == 200 else []
    if len(values) != 1 or check.json().get("pageToken"):
        raise Unknown("diagnostic receipt readback unavailable or duplicate")
    actual = {key: cell.get("v") for key, cell in zip(
        ("attempt_id", "attempted_at", "state", "source_run_id", "capture_id", "reason", "diagnostic_json"), values[0]["f"])}
    if any(actual.get(key) != row[key] for key in row if key not in {"snapshot_date", "attempted_at"}):
        raise Unknown("diagnostic identity/content differs; not replacing the prior attempt")
    return {"attempt_id": attempt_id, "attempted_at": actual["attempted_at"], "state": actual["state"], "readback": "VERIFIED"}


def capture(session, sid, source, *, inflight=release.github_inflight,
            provenance=release.github_provenance, audit=audit_capture,
            admission=dashboard_admission, now=None, wait=False):
    if not re.fullmatch(r"[1-9][0-9]*", source):
        raise Refused("source run is invalid")
    raw_proof = provenance(source)
    proof = release.check_provenance(source, raw_proof)
    if not re.fullmatch(r"[0-9a-f]{40}", str(proof.get("head_sha", ""))):
        raise Refused("source code identity is unavailable")
    if raw_proof.get("run", {}).get("conclusion") not in ("success", "failure"):
        raise Refused("source did not finish normally")
    if not proof.get("ok") or inflight():
        raise Refused("source provenance or writer state is not accepted")
    if wait:
        wait_for_receipts(session, sid, source)
        if inflight():
            raise Refused("production writer started while awaiting receipts")
    now = now or dt.datetime.now(dt.timezone.utc)
    captured = now.astimezone(dt.timezone.utc).isoformat()
    grids, times = {}, {}
    for tab in CAPTURE_TABS:
        grids[tab] = get_grid(session, sid, tab)
        times[tab] = dt.datetime.now(dt.timezone.utc).isoformat()
    if sum(len(encoded(grid)) for grid in grids.values()) > MAX_BYTES:
        raise Refused("total capture byte bound exceeded")
    # Audit EXACTLY the values that will be archived. Date-only presentation
    # must fail the existing precision policy, never borrow a later serial read.
    audit_result = asyncio.run(audit(sid, reader=frozen_reader(grids, sid), publish=False, run_id=source))
    verdict = admission(audit_result, source, grids)
    if verdict.get("accepted") is not True:
        raise Refused("captured cohort withheld: " + ",".join(verdict["reason_codes"]), details=verdict)
    for tab in sorted(CRITICAL):
        if get_grid(session, sid, tab) != grids[tab]:
            raise Refused("source changed during capture: " + tab)
    if inflight():
        raise Refused("production writer started during capture")
    day = dt.datetime.fromisoformat(captured).date().isoformat()
    receipt = {"schema": "tfb.dashboard-capture/v1", "source_run_id": source,
               "source_attempt": proof["run_attempt"], "captured_at": captured,
               "source_sha": proof["head_sha"], "capture_code_sha": arc._env("GITHUB_SHA", "local"),
               "audit_policy": audit_result["policy"],
               "capture_contract": verdict,
               "snapshot_date": day, "archive_run_id": os.getenv("GITHUB_RUN_ID", "local"),
               "tab_digests": {tab: digest(grid) for tab, grid in grids.items()},
               "tab_captured_at": times, "cohort_bound_tabs": sorted(CRITICAL),
               "audit_exit_codes": audit_result["audit_exit_codes"], "feed_promoted": False}
    # Content-addressed across retries: no wall clock / runner ID in identity.
    # A reused stage retains its ORIGINAL capture times via _Capture_Receipt.
    receipt["capture_id"] = digest({"source": source, "attempt": proof["run_attempt"],
                                    "source_sha": receipt["source_sha"],
                                    "capture_code_sha": receipt["capture_code_sha"],
                                    "policy": receipt["audit_policy"], "version": SCRIPT_VERSION,
                                    "capture_contract": verdict,
                                    "digests": receipt["tab_digests"]})
    rows = []
    headers = dict(arc.TABS)
    for tab in CAPTURE_TABS:
        header = headers.get(tab, 0)
        got, counts = arc.build_rows(tab, grids[tab], header, day, times[tab], receipt["archive_run_id"])
        if counts["truncated"]:
            raise Refused("archive row bound exceeded")
        for row in got:
            row.update(capture_id=receipt["capture_id"], source_run_id=source if tab in SOURCE_OUTPUTS else None)
        rows.extend(got)
    receipt["row_count"] = len(rows)
    rows.append({"snapshot_date": day, "captured_at": captured, "run_id": receipt["archive_run_id"],
                 "tab": "_Capture_Receipt", "row_num": 0, "cells": [], "record_json": encoded(receipt).decode(),
                 "capture_id": receipt["capture_id"], "source_run_id": source})
    return rows, receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", default="auto")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--tabs", default="")
    parser.add_argument("--json-out", default="coherent_archive.json")
    args = parser.parse_args(argv)
    out = {"script_version": SCRIPT_VERSION, "gate": armed(), "published": False, "exit_code": 0}
    if not armed() or not arc.archive_enabled():
        out["gate"] = False
        print(json.dumps(out))
        return 0
    session, source = None, args.source_run
    sid, project = arc._env("DEFAULT_SPREADSHEET_ID"), arc._env("TFB_BQ_PROJECT")
    dataset, location = arc._env("TFB_BQ_DATASET", "tfb_archive"), arc._env("TFB_BQ_LOCATION", "US")
    try:
        if not sid or not project or args.tabs.strip():
            raise Refused("coherent capture requires configured project/sheet and all tabs")
        session = arc.make_session()
        source = args.source_run
        if source == "auto":
            source, why = release.discover_source(slice_grid(get_grid(session, sid, "_Status"), "L1:M60"))
            if not source:
                raise Refused("current source is unavailable: " + str(why))
        for attempt in range(2):
            try:
                rows, receipt = capture(session, sid, source, wait=True)
                out["capture_attempts"] = attempt + 1
                break
            except Refused as exc:
                # A release/native render can update _Status after its receipt
                # landed. One fresh READ-only capture is safe; never retry a
                # writer/provenance/coverage fault or any BigQuery mutation.
                if attempt or not str(exc).startswith("source changed during capture:"):
                    raise
                out["retry_reason"] = str(exc)
        out.update(receipt=receipt)
        if args.write:
            out["receipt"], out["current_at_readback"] = publish(session, project, dataset, location, rows, receipt)
            out["committed"] = True
            out["published"] = out["current_at_readback"]
            out["outcome"] = "current" if out["published"] else "already_committed_superseded"
    except Refused as exc:
        out.update(exit_code=2, outcome="withheld", reason=safe_error_text(exc), diagnostic=exc.details)
    except Exception as exc:
        out.update(exit_code=3, outcome="unknown" if isinstance(exc, Unknown) else "fault",
                   reason=safe_error_text(exc))
    if args.write and session is not None and project:
        try:
            out["latest_diagnostic"] = publish_attempt(session, project, dataset, location, out, source)
        except Exception as exc:
            out["latest_diagnostic"] = {"state": "UNKNOWN", "reason": safe_error_text(exc)}
            out["exit_code"] = 3
    elif args.write:
        out["latest_diagnostic"] = {"state": "UNKNOWN", "reason": "configuration/session unavailable"}
    Path(args.json_out).write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(out, ensure_ascii=False))
    return out["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
