#!/usr/bin/env python3
# scripts/archive_to_bigquery.py
"""
================================================================================
TFB BigQuery Archive - v1.0.0 (program register C8, Phase 1)
================================================================================
WHY (owner decision 27 Oct 2026): the decision workbook overwrites most of
its learning surfaces every day - the Top-10 board, Calendar_Events, the
nightly _Board_Scorecard, My_Portfolio, Portfolio_Decision - and the logs
(_Selection_Log, Signal_History, Performance_Log, _Run_Log) live inside a
spreadsheet with a cell budget. The self-correcting program needs the
history of what the board said and what happened next, queryable over
months (champion vs challenger, post-earnings tags, scorecard drift), and
Looker Studio dashboards on top. A BigQuery project with billing enabled
(no 60-day sandbox expiry) now exists: project-2e3530e6-2311-4aa3-b36,
dataset tfb_archive (US).

WHAT: a READ-ONLY daily copy. For each tab in TABS (or --tabs) the script
reads every value once (Sheets API values.get; UNFORMATTED values, dates
as the sheet's text), turns each non-empty row into
    {snapshot_date, captured_at, run_id, tab, row_num, cells[], record_json}
and loads the rows into <dataset>.<table>$YYYYMMDD with WRITE_TRUNCATE: a
re-run on the same day replaces that day's copy, every other day stays as
it was. snapshot_date = the UTC calendar date of the capture (the scheduled
23:05 UTC run lands after the US close and the nightly scorecard).
cells = the row exactly as the sheet holds it (positional, lossless);
record_json = {header: value} (non-empty values only) for tabs whose header
row is known, so ad-hoc SQL can say JSON_VALUE(record_json, '$."Symbol"').
Tables are created on first use, DAY-partitioned on snapshot_date, one
fixed schema for every tab. NOTHING is written to the workbook; the live
system never reads BigQuery.

GATE: TFB_BQ_ARCHIVE = on|1|true|yes arms the script (default off here -
the workflow arms it; kill with the repo Variable TFB_BQ_ARCHIVE=off).
Without --write the script is a dry run: it reads the tabs and prints what
it would load, and calls BigQuery only to check the dataset exists.

ENV: TFB_BQ_PROJECT, TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION
(default US), DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS (service
account JSON or base64; the same account the sync uses, granted BigQuery
Data Editor + Job User on the project), GITHUB_RUN_ID.
CLI: --write | --dry-run (default) ; --tabs A,B ; --date YYYY-MM-DD.
Exit: 0 all tabs done, 1 any tab failed (the others are still archived),
2 configuration missing.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import base64
import datetime as _dt
import json
import os
import re
import sys
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

__version__ = "1.0.0"

# (tab, header_row): header_row is the 1-based row holding column names, 0 when
# the tab is a multi-zone layout (title rows, several tables) - cells only.
TABS: Tuple[Tuple[str, int], ...] = (
    ("Top_10_Investments", 0),
    ("_Selection_Log", 1),
    ("_Shadow_Rank_Log", 1),
    ("_Board_Scorecard", 0),
    ("Calendar_Events", 1),
    ("Signal_History", 1),
    ("Performance_Log", 0),     # a summary block sits above / beside its table
    ("My_Portfolio", 1),
    ("Portfolio_Decision", 0),
    ("_Run_Log", 1),
)
MAX_ROWS_PER_TAB = 250_000
MAX_COLS = 400
SCOPES = ("https://www.googleapis.com/auth/spreadsheets.readonly",
          "https://www.googleapis.com/auth/bigquery")
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"
BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
BQ_UPLOAD = "https://bigquery.googleapis.com/upload/bigquery/v2"
SCHEMA: List[Dict[str, str]] = [
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED",
     "description": "UTC calendar date of the capture (one partition per day)"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED",
     "description": "when the archive read the tab"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE",
     "description": "GitHub Actions run id of the capture"},
    {"name": "tab", "type": "STRING", "mode": "REQUIRED",
     "description": "workbook tab name"},
    {"name": "row_num", "type": "INTEGER", "mode": "REQUIRED",
     "description": "1-based sheet row"},
    {"name": "cells", "type": "STRING", "mode": "REPEATED",
     "description": "the row as the sheet held it (unformatted values as text)"},
    {"name": "record_json", "type": "STRING", "mode": "NULLABLE",
     "description": "{header: value} for tabs with a header row, non-empty values only"},
]


def _out(msg: str) -> None:
    sys.stderr.write(f"[bq_archive v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def archive_enabled() -> bool:
    return _env("TFB_BQ_ARCHIVE").lower() in ("on", "1", "true", "yes")


def table_name(tab: str) -> str:
    """'_Selection_Log' -> 'selection_log' (BigQuery-safe, stable)."""
    name = re.sub(r"[^a-z0-9_]+", "_", str(tab).strip().lower()).strip("_")
    return name or "tab"


def cell_text(value: Any) -> str:
    """One sheet value as text: '' for blanks, TRUE/FALSE for booleans,
    integral floats without '.0', other numbers via repr."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return str(value)
        return str(int(value)) if value.is_integer() and abs(value) < 1e15 else repr(value)
    return str(value)


def header_names(row: Sequence[Any]) -> List[str]:
    """Column names from a header row: blank -> col_N, duplicates -> name_2..."""
    names: List[str] = []
    seen: Dict[str, int] = {}
    for i, raw in enumerate(row):
        base = cell_text(raw).strip() or f"col_{i + 1}"
        n = seen.get(base, 0) + 1
        seen[base] = n
        names.append(base if n == 1 else f"{base}_{n}")
    return names


def build_rows(tab: str, values: Sequence[Sequence[Any]], header_row: int,
               snapshot_date: str, captured_at: str, run_id: str
               ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """The archive rows of one tab (empty rows skipped) and its counts."""
    rows: List[Dict[str, Any]] = []
    names: List[str] = []
    if header_row and len(values) >= header_row:
        names = header_names(list(values[header_row - 1])[:MAX_COLS])
    cells_total = 0
    truncated = 0
    for i, raw in enumerate(values):
        if i >= MAX_ROWS_PER_TAB:
            truncated = len(values) - MAX_ROWS_PER_TAB
            break
        cells = [cell_text(v) for v in list(raw)[:MAX_COLS]]
        if not any(c != "" for c in cells):
            continue
        rec: Dict[str, Any] = {
            "snapshot_date": snapshot_date, "captured_at": captured_at,
            "run_id": run_id or None, "tab": tab, "row_num": i + 1, "cells": cells,
            "record_json": None,
        }
        if names and i + 1 > header_row:
            extra = len(names)
            mapping: Dict[str, str] = {}
            for j, c in enumerate(cells):
                if c == "":
                    continue
                key = names[j] if j < len(names) else f"col_{j + 1}"
                if j >= extra and key in mapping:
                    key = f"{key}_{j + 1}"
                mapping[key] = c
            rec["record_json"] = json.dumps(mapping, ensure_ascii=False, sort_keys=True)
        cells_total += len(cells)
        rows.append(rec)
    return rows, {"rows": len(rows), "cells": cells_total, "truncated": truncated}


def ndjson(rows: Iterable[Dict[str, Any]]) -> bytes:
    return "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n"
                   for r in rows).encode("utf-8")


# ----------------------------------------------------------------------------- #
# HTTP (an AuthorizedSession-like object: .get / .post / .put returning
# responses with .status_code, .json(), .text, .headers)
# ----------------------------------------------------------------------------- #
def _credentials_info() -> Optional[Dict[str, Any]]:
    raw = _env("GOOGLE_SHEETS_CREDENTIALS") or _env("GOOGLE_CREDENTIALS")
    if not raw:
        return None
    try:
        if not raw.startswith("{"):
            raw = base64.b64decode(raw).decode("utf-8")
        info = json.loads(raw)
        return info if isinstance(info, dict) else None
    except Exception:
        return None


def make_session():  # pragma: no cover - needs google-auth and a real key
    from google.oauth2 import service_account
    from google.auth.transport.requests import AuthorizedSession
    info = _credentials_info()
    if not info:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")
    creds = service_account.Credentials.from_service_account_info(info, scopes=list(SCOPES))
    return AuthorizedSession(creds)


def _err(resp: Any) -> str:
    try:
        body = resp.json()
        msg = ((body or {}).get("error") or {}).get("message") or ""
    except Exception:
        msg = ""
    return f"HTTP {getattr(resp, 'status_code', '?')}" + (f": {msg[:300]}" if msg else "")


def read_tab(session: Any, sid: str, tab: str) -> List[List[Any]]:
    rng = quote("'" + tab.replace("'", "''") + "'", safe="")
    url = (f"{SHEETS_API}/{sid}/values/{rng}?majorDimension=ROWS"
           "&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING")
    resp = session.get(url, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"read {tab}: {_err(resp)}")
    return list((resp.json() or {}).get("values") or [])


def dataset_exists(session: Any, project: str, dataset: str) -> bool:
    resp = session.get(f"{BQ_API}/projects/{project}/datasets/{dataset}", timeout=60)
    if resp.status_code == 200:
        return True
    if resp.status_code == 404:
        return False
    raise RuntimeError(f"dataset {project}.{dataset}: {_err(resp)}")


def ensure_table(session: Any, project: str, dataset: str, table: str, tab: str) -> bool:
    """True when the table was created now, False when it existed."""
    base = f"{BQ_API}/projects/{project}/datasets/{dataset}/tables"
    resp = session.get(f"{base}/{table}", timeout=60)
    if resp.status_code == 200:
        return False
    if resp.status_code != 404:
        raise RuntimeError(f"table {table}: {_err(resp)}")
    body = {
        "tableReference": {"projectId": project, "datasetId": dataset, "tableId": table},
        "schema": {"fields": SCHEMA},
        "timePartitioning": {"type": "DAY", "field": "snapshot_date"},
        "description": (f"TFB archive of the workbook tab '{tab}': one partition per "
                        "snapshot_date (UTC capture date). cells = the row as the sheet held "
                        "it; record_json = header -> value when the tab has a header row. "
                        f"Written by scripts/archive_to_bigquery.py v{__version__}."),
    }
    resp = session.post(base, json=body, timeout=60)
    if resp.status_code not in (200, 201):
        if resp.status_code == 409:   # created by a concurrent run
            return False
        raise RuntimeError(f"create {table}: {_err(resp)}")
    return True


def load_partition(session: Any, project: str, dataset: str, table: str, day: str,
                   location: str, data: bytes, run_id: str, poll_s: float = 2.0,
                   timeout_s: float = 600.0) -> Dict[str, Any]:
    """Load NDJSON `data` into table$YYYYMMDD with WRITE_TRUNCATE; wait for it."""
    job_id = "tfb_archive_%s_%s_%s_%s" % (table, day.replace("-", ""),
                                          re.sub(r"[^0-9A-Za-z_-]", "", run_id or "local") or "local",
                                          uuid.uuid4().hex[:8])
    config = {
        "jobReference": {"projectId": project, "jobId": job_id, "location": location},
        "configuration": {"load": {
            "destinationTable": {"projectId": project, "datasetId": dataset,
                                 "tableId": f"{table}${day.replace('-', '')}"},
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_NEVER",
            "schema": {"fields": SCHEMA},
            "maxBadRecords": 0,
        }},
    }
    start = session.post(f"{BQ_UPLOAD}/projects/{project}/jobs?uploadType=resumable",
                         json=config, timeout=60,
                         headers={"X-Upload-Content-Type": "application/octet-stream",
                                  "X-Upload-Content-Length": str(len(data))})
    if start.status_code not in (200, 201):
        raise RuntimeError(f"load {table}: start {_err(start)}")
    upload_url = (getattr(start, "headers", {}) or {}).get("Location") or \
        (getattr(start, "headers", {}) or {}).get("location")
    if not upload_url:
        raise RuntimeError(f"load {table}: no upload session")
    put = session.put(upload_url, data=data, timeout=600,
                      headers={"Content-Type": "application/octet-stream"})
    if put.status_code not in (200, 201):
        raise RuntimeError(f"load {table}: upload {_err(put)}")
    deadline = time.monotonic() + timeout_s
    job = put.json() or {}
    while True:
        status = (job.get("status") or {})
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(f"load {table}: {status['errorResult'].get('message', 'job failed')[:300]}")
            return job
        if time.monotonic() > deadline:
            raise RuntimeError(f"load {table}: job {job_id} not done after {int(timeout_s)}s")
        time.sleep(poll_s)
        resp = session.get(f"{BQ_API}/projects/{project}/jobs/{job_id}?location={location}", timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"load {table}: poll {_err(resp)}")
        job = resp.json() or {}


# ----------------------------------------------------------------------------- #
# Main
# ----------------------------------------------------------------------------- #
def run(session: Any, *, sid: str, project: str, dataset: str, location: str,
        tabs: Sequence[Tuple[str, int]], snapshot_date: str, captured_at: str,
        run_id: str, write: bool) -> int:
    try:
        if not dataset_exists(session, project, dataset):
            _out(f"ERROR: dataset {project}.{dataset} not found")
            return 2
    except Exception as exc:   # e.g. 403 before the IAM grant
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    failed: List[str] = []
    total_rows = 0
    for tab, header_row in tabs:
        table = table_name(tab)
        try:
            values = read_tab(session, sid, tab)
            rows, n = build_rows(tab, values, header_row, snapshot_date, captured_at, run_id)
            total_rows += n["rows"]
            target = f"{dataset}.{table}${snapshot_date.replace('-', '')}"
            note = f" | {n['truncated']} row(s) over the cap left out" if n["truncated"] else ""
            if not write:
                _out(f"{tab} -> {target}: {n['rows']} rows ({n['cells']} cells){note} | dry run")
                continue
            created = ensure_table(session, project, dataset, table, tab)
            if rows:
                job = load_partition(session, project, dataset, table, snapshot_date, location,
                                     ndjson(rows), run_id)
                stats = ((job.get("statistics") or {}).get("load") or {})
                loaded = stats.get("outputRows", str(n["rows"]))
            else:
                loaded = "0 (nothing to load; the day's partition left as it was)"
            _out(f"{tab} -> {target}: {n['rows']} rows ({n['cells']} cells){note} | loaded {loaded}"
                 + (" | table created" if created else ""))
        except Exception as exc:  # one tab never stops the others
            failed.append(tab)
            _out(f"ERROR {tab}: {str(exc)[:400]}")
    _out(f"done: {len(tabs) - len(failed)}/{len(tabs)} tab(s), {total_rows} rows, "
         f"snapshot {snapshot_date}{'' if write else ' (dry run)'}"
         + (f"; FAILED: {', '.join(failed)}" if failed else ""))
    return 1 if failed else 0


def _select_tabs(spec: str) -> List[Tuple[str, int]]:
    if not spec.strip():
        return list(TABS)
    known = dict(TABS)
    out: List[Tuple[str, int]] = []
    for name in [s.strip() for s in spec.split(",") if s.strip()]:
        out.append((name, known.get(name, 1)))
    return out


def main(argv: Optional[Sequence[str]] = None, session: Any = None,
         now: Optional[_dt.datetime] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB BigQuery archive (read-only daily copy)")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="load into BigQuery")
    mode.add_argument("--dry-run", action="store_true", help="read and report only (default)")
    ap.add_argument("--tabs", default="", help="comma-separated tab names (default: TABS)")
    ap.add_argument("--date", default="", help="snapshot date YYYY-MM-DD (default: today UTC)")
    args = ap.parse_args(argv)
    if not archive_enabled():
        _out("TFB_BQ_ARCHIVE is off - nothing read, nothing written")
        return 0
    sid = _env("DEFAULT_SPREADSHEET_ID")
    project = _env("TFB_BQ_PROJECT")
    dataset = _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not sid or not project:
        _out("ERROR: DEFAULT_SPREADSHEET_ID and TFB_BQ_PROJECT are required")
        return 2
    now = now or _dt.datetime.now(_dt.timezone.utc)
    snapshot_date = args.date.strip() or now.date().isoformat()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", snapshot_date):
        _out(f"ERROR: --date must be YYYY-MM-DD, got {snapshot_date!r}")
        return 2
    captured_at = now.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    session = session or make_session()
    return run(session, sid=sid, project=project, dataset=dataset, location=location,
               tabs=_select_tabs(args.tabs), snapshot_date=snapshot_date,
               captured_at=captured_at, run_id=_env("GITHUB_RUN_ID"), write=bool(args.write))


if __name__ == "__main__":
    sys.exit(main())
