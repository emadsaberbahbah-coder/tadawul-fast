#!/usr/bin/env python3
"""
scripts/plan_archive.py
================================================================================
TFB plan archive - each Today Action Plan run, kept as rows in BigQuery
--------------------------------------------------------------------------------
v1.0.0 (2026-11-13)
  Owner, 13 Nov 22:44 Riyadh: "add action plan to the IBKR Dashboard". The
  dashboard reads BigQuery, and the plan was written only to its workbook tab.
  After the Today_Action_Plan page is written, its steps, its five day tiles
  and one summary row are appended to <dataset>.<TFB_TAP_BQ_TABLE> (default
  today_action_plan), all under one run_at. The dashboard shows the latest
  run's rows; the history is the record of what the plan said and when, beside
  what then happened.

  It COPIES: every value is the page's own text for that run (the step's
  instruction, levels and reason; the tile's label, value and note). It decides
  nothing, and nothing here places, changes or cancels an order.

SAFETY
  Off unless TFB_TAP_BQ is on (the workflow maps on). The page is written first;
  an archive fault is reported in the run log and never fails or changes the page.
  Rows go in with insertId = run id + row, so a retried call does not double them.

ENV
  TFB_TAP_BQ (off | on), TFB_TAP_BQ_TABLE (today_action_plan),
  TFB_BQ_PROJECT, TFB_BQ_DATASET, TFB_BQ_LOCATION (US), GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT.
================================================================================
"""
from __future__ import annotations

import datetime as _dt
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

__version__ = "1.0.0"

BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
MAX_TEXT = 1000                                                    # one cell's text, cut here

SCHEMA: List[Dict[str, str]] = [
    {"name": "run_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when the page was built (UTC)"},
    {"name": "plan_date", "type": "DATE", "mode": "REQUIRED", "description": "the Riyadh day the page is for"},
    {"name": "run_id", "type": "STRING", "description": "the workflow run that built it"},
    {"name": "version", "type": "STRING", "description": "today_action_plan version"},
    {"name": "row_type", "type": "STRING", "description": "step | tile | summary"},
    {"name": "seq", "type": "INTEGER", "description": "the step's number on the page, or the tile's place (1-5)"},
    {"name": "kind", "type": "STRING", "description": "a step's kind: sell, profit, stop, broker, review, buy, skip, news, nofund, placed, verify"},
    {"name": "label", "type": "STRING", "description": "the step's chip (SELL, BUY ...) or the tile's label"},
    {"name": "symbol", "type": "STRING", "description": "the step's symbol"},
    {"name": "instruction", "type": "STRING", "description": "the step's instruction, the tile's value or the summary line"},
    {"name": "levels", "type": "STRING", "description": "the step's levels, the tile's note, or the old sources"},
    {"name": "why", "type": "STRING", "description": "the step's reason, or the pages not read"},
    {"name": "source", "type": "STRING", "description": "the page the step was copied from"},
    {"name": "tone", "type": "STRING", "description": "a tile's tone (plain, good, warn, bad); the summary's (ok, alert)"},
]


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_TAP_BQ", "off").lower() in ("1", "true", "yes", "on")


def _t(v: Any) -> str:
    s = "" if v is None else str(v)
    return s if len(s) <= MAX_TEXT else s[:MAX_TEXT - 1] + "…"


def run_id(now: _dt.datetime) -> str:
    rid, att = _env("GITHUB_RUN_ID"), _env("GITHUB_RUN_ATTEMPT", "1")
    return "%s-%s" % (rid, att) if rid else "local-" + now.strftime("%Y%m%dT%H%M%SZ")


def plan_rows(steps: Sequence[Dict[str, Any]], tiles: Sequence[Sequence[str]], summary: str, stale: Sequence[str],
              missing: Sequence[str], now: _dt.datetime, rid: str, version: str,
              alert: Optional[bool] = None) -> List[Dict[str, Any]]:
    """The run as rows. `steps` carry "kind" and "label" (the page's chip); `tiles` are (label, value, note, tone).
    `alert` is the page's own alert state (its summary band); without it, a stale or unread source is the alert."""
    base = {"run_at": now.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "plan_date": now.astimezone(RIYADH).strftime("%Y-%m-%d"), "run_id": rid, "version": version}
    out: List[Dict[str, Any]] = [dict(base, row_type="summary", seq=0, kind="", label="", symbol="",
                                      instruction=_t(summary), levels=_t(", ".join(stale)), why=_t(", ".join(missing)),
                                      source="", tone="alert" if (alert or stale or missing) else "ok")]
    for i, s in enumerate(steps, 1):
        out.append(dict(base, row_type="step", seq=i, kind=_t(s.get("kind")), label=_t(s.get("label")),
                        symbol=_t(s.get("symbol")), instruction=_t(s.get("do")), levels=_t(s.get("levels")),
                        why=_t(s.get("why")), source=_t(s.get("source")), tone=""))
    for i, t in enumerate(tiles, 1):
        lab, val, note, tone = (list(t) + ["", "", "", ""])[:4]
        out.append(dict(base, row_type="tile", seq=i, kind="", label=_t(lab), symbol="", instruction=_t(val),
                        levels=_t(note), why="", source="", tone=_t(tone)))
    return out


def _err(resp: Any) -> str:
    try:
        return "HTTP %s %s" % (resp.status_code, str((resp.json().get("error") or {}).get("message", ""))[:200])
    except Exception:  # noqa: BLE001
        return "HTTP %s" % getattr(resp, "status_code", "?")


def ensure_table(session: Any, project: str, dataset: str, table: str) -> bool:
    """True when the table was created now, False when it was there."""
    base = "%s/projects/%s/datasets/%s/tables" % (BQ_API, project, dataset)
    resp = session.get("%s/%s" % (base, table), timeout=60)
    if resp.status_code == 200:
        return False
    if resp.status_code != 404:
        raise RuntimeError("table %s: %s" % (table, _err(resp)))
    body = {"tableReference": {"projectId": project, "datasetId": dataset, "tableId": table},
            "schema": {"fields": SCHEMA}, "timePartitioning": {"type": "DAY", "field": "plan_date"},
            "description": "TFB Today Action Plan: every run's steps, day tiles and summary, as the page showed them. "
                           "Written by scripts/plan_archive.py v%s." % __version__}
    resp = session.post(base, json=body, timeout=60)
    if resp.status_code in (200, 201):
        return True
    if resp.status_code == 409:                                     # created by a concurrent run
        return False
    raise RuntimeError("create %s: %s" % (table, _err(resp)))


def insert(session: Any, project: str, dataset: str, table: str, rows: Sequence[Dict[str, Any]],
           sleep: Callable[[float], None] = time.sleep, tries: int = 4) -> int:
    """Stream `rows`; a table created a moment ago can answer 404 for a while, so that answer is waited out."""
    url = "%s/projects/%s/datasets/%s/tables/%s/insertAll" % (BQ_API, project, dataset, table)
    body = {"rows": [{"insertId": "%s-%s-%s" % (r["run_id"], r["row_type"], r["seq"]), "json": r} for r in rows]}
    for attempt in range(tries):
        resp = session.post(url, json=body, timeout=60)
        if resp.status_code == 404 and attempt < tries - 1:
            sleep(15.0)
            continue
        if resp.status_code != 200:
            raise RuntimeError("insert %s: %s" % (table, _err(resp)))
        errs = (resp.json() or {}).get("insertErrors") or []
        if errs:
            first = ((errs[0].get("errors") or [{}])[0] or {}).get("message", "")
            raise RuntimeError("insert %s: %d row(s) refused: %s" % (table, len(errs), str(first)[:200]))
        return len(rows)
    raise RuntimeError("insert %s: the table was not ready" % table)


def archive(rows: Sequence[Dict[str, Any]], out: Callable[[str], None], session: Any = None,
            sleep: Callable[[float], None] = time.sleep) -> Optional[int]:
    """Append the rows; returns how many went in, or None when off or refused. Never raises."""
    if not enabled():
        return None
    project, dataset = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET")
    table = _env("TFB_TAP_BQ_TABLE", "today_action_plan")
    if not (_ID_RE.match(project) and _ID_RE.match(dataset) and _ID_RE.match(table)):
        out("plan archive not written: TFB_BQ_PROJECT / TFB_BQ_DATASET / TFB_TAP_BQ_TABLE missing or malformed")
        return None
    try:
        if session is None:
            from scripts.archive_to_bigquery import make_session  # noqa: PLC0415
            session = make_session()
        made = ensure_table(session, project, dataset, table)
        n = insert(session, project, dataset, table, rows, sleep=sleep)
    except Exception as e:  # noqa: BLE001 - the page is already written; the archive only reports
        out("plan archive not written: %s: %s" % (type(e).__name__, str(e)[:240]))
        return None
    out("plan archive: %d row(s) appended to %s.%s%s" % (n, dataset, table, " (table created)" if made else ""))
    return n
