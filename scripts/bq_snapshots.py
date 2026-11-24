#!/usr/bin/env python3
"""
scripts/bq_snapshots.py
================================================================================
TFB BigQuery snapshots - the heavy views, computed once after each collector instead of on every read
--------------------------------------------------------------------------------
v1.0.0 (2026-11-24)
  WHY: owner, 24 Nov 2026 13:07 Riyadh, in Arabic: how can BigQuery be used more, faster and more accurately? Item 2
       of the answer; 13:53: "Let's go with next". Three views are read many times a day and each read recomputes
       them from the archive: v_universe_latest (about 235 MB a read; News Now reads it twice an hour, once through
       v_symbol_news_class_now), v_news_understanding (about 367 MB; twice per News Board run) and
       v_news_trend_learning (about 362 MB; the News Board and Learning Watch). Their sources change only when a
       collector loads (the archive at night, the deep news and bars in the morning, the market series), so each
       read between two loads recomputes the same answer.
  WHAT: after each collector run (workflow_run of the archive, the deep data and the market series; or dispatched)
       each view of SNAPSHOTS is computed ONCE into its table snap_<name> (a query job, WRITE_TRUNCATE, clustered
       for the readers' filters), with the column snapshot_at. A reader asks source_for(): the snapshot when it is
       there and younger than MAX_AGE_H hours, else the view itself - so a missing or stale snapshot costs only the
       old price, never a wrong page.
  COST: about 1 GB per refresh (the three views once), a few MB per read afterwards instead of 0.25-0.37 GB.
  READS: the three views (SELECT only). WRITES: only the tables snap_universe_latest, snap_news_understanding and
       snap_news_trend_learning. Research data only: the live system never reads BigQuery.
  ROLLBACK: repository Variable TFB_BQ_SNAPSHOTS=off (no refresh; the snapshots age past MAX_AGE_H and every reader
       goes back to the views by itself).
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
import sys
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"

BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
MAX_AGE_H = 30                       # a snapshot older than this is not read (one missed night still reads fresh)
# table -> (view, clustering)
SNAPSHOTS: Dict[str, Tuple[str, List[str]]] = {
    "snap_universe_latest": ("v_universe_latest", ["symbol"]),
    "snap_news_understanding": ("v_news_understanding", ["symbol"]),
    "snap_news_trend_learning": ("v_news_trend_learning", ["period", "state"]),
}
VIEW_TO_SNAP = {view: table for table, (view, _) in SNAPSHOTS.items()}
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _out(msg: str) -> None:
    print("[bq_snapshots %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_BQ_SNAPSHOTS", "on").lower() not in ("off", "0", "false", "no")


def snapshot_sql(ds: str, view: str) -> str:
    return "SELECT *, CURRENT_TIMESTAMP() AS snapshot_at FROM `%s.%s`" % (ds, view)


def freshness_sql(ds: str, table: str) -> str:
    return ("SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(snapshot_at), MINUTE) AS age_min FROM `%s.%s`"
            % (ds, table))


def source_for(view: str, session: Any, project: str, location: str, ds: str, query: Any,
               max_age_h: float = MAX_AGE_H, note: Optional[Callable[[str], None]] = None) -> str:
    """The name to read `view` from: its snapshot when it exists and is younger than max_age_h hours, else the view.
    One small read (the snapshot's snapshot_at column only); any refusal falls back to the view."""
    table = VIEW_TO_SNAP.get(view)
    if not table:
        return view
    try:
        rows = query(session, project, location, freshness_sql(ds, table))
        age = float(rows[0].get("age_min")) if rows and rows[0].get("age_min") is not None else None
    except Exception:  # noqa: BLE001 - not there yet, or refused: the view answers
        age = None
    if age is not None and age <= max_age_h * 60:
        return table
    if note:
        note("snapshot %s %s: reading %s" % (table, "missing" if age is None else "%.0f h old" % (age / 60), view))
    return view


def run_snapshot(session: Any, project: str, dataset: str, location: str, table: str, view: str,
                 cluster: Sequence[str], run_id: str, poll_s: float = 2.0, timeout_s: float = 600.0,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> Tuple[int, int]:
    """One query job: the view into its table, replaced whole. (rows, bytes processed)."""
    job_id = "tfb_snap_%s_%s_%s" % (table, re.sub(r"[^0-9A-Za-z_-]", "", run_id or "local") or "local",
                                     uuid.uuid4().hex[:8])
    body = {
        "jobReference": {"projectId": project, "jobId": job_id, "location": location},
        "configuration": {"query": {
            "query": snapshot_sql("%s.%s" % (project, dataset), view), "useLegacySql": False,
            "destinationTable": {"projectId": project, "datasetId": dataset, "tableId": table},
            "writeDisposition": "WRITE_TRUNCATE", "createDisposition": "CREATE_IF_NEEDED",
            "clustering": {"fields": list(cluster)},
        }, "labels": {"tfb": "snapshot"}},
    }
    resp = session.post("%s/projects/%s/jobs" % (BQ_API, project), json=body, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError("snapshot %s: HTTP %s" % (table, resp.status_code))
    job = resp.json() or {}
    deadline = clock() + timeout_s
    while True:
        status = job.get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError("snapshot %s: %s" % (table, str(status["errorResult"].get("message", "failed"))[:300]))
            stats = job.get("statistics") or {}
            q = stats.get("query") or {}
            return (int(float(q.get("numDmlAffectedRows") or (stats.get("load") or {}).get("outputRows") or 0)),
                    int(float(q.get("totalBytesProcessed") or 0)))
        if clock() > deadline:
            raise RuntimeError("snapshot %s: job not done after %ds" % (table, int(timeout_s)))
        sleep(poll_s)
        r = session.get("%s/projects/%s/jobs/%s?location=%s" % (BQ_API, project, job_id, location), timeout=60)
        if r.status_code != 200:
            raise RuntimeError("snapshot %s: poll HTTP %s" % (table, r.status_code))
        job = r.json() or {}


def run_once(dry_run: bool, session=None, only: Sequence[str] = (), sleep: Callable[[float], None] = time.sleep
             ) -> int:
    if not enabled():
        _out("TFB_BQ_SNAPSHOTS is off: nothing refreshed (readers go back to the views as the snapshots age)")
        return 0
    project, dataset = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not _ID_RE.match(project) or not _ID_RE.match(dataset):
        _out("ERROR: TFB_BQ_PROJECT / TFB_BQ_DATASET missing or malformed")
        return 2
    tables = [t for t in SNAPSHOTS if not only or t in only or SNAPSHOTS[t][0] in only]
    if dry_run:
        for t in tables:
            _out("dry run: would refresh %s from %s" % (t, SNAPSHOTS[t][0]))
        return 0
    if session is None:
        from scripts.archive_to_bigquery import make_session  # noqa: PLC0415
        session = make_session()
    failed = 0
    for t in tables:
        view, cluster = SNAPSHOTS[t]
        try:
            _rows, processed = run_snapshot(session, project, dataset, location, t, view, cluster,
                                            _env("GITHUB_RUN_ID", "local"), sleep=sleep)
            _out("refreshed %s from %s (%.2f GB processed)" % (t, view, processed / 1e9))
        except Exception as e:  # noqa: BLE001 - one snapshot never stops the others; its readers use the view
            failed += 1
            _out("ERROR: %s was not refreshed (%s); its readers use %s" % (t, str(e)[:200], view))
    _out("done: %d of %d snapshot(s) refreshed" % (len(tables) - failed, len(tables)))
    return 1 if failed else 0


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB BigQuery snapshots of the heavy views")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="", help="comma list of snapshot tables or views")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, only=[x.strip() for x in a.only.split(",") if x.strip()], **kw)


if __name__ == "__main__":
    sys.exit(main())
