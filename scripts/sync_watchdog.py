#!/usr/bin/env python3
"""
scripts/sync_watchdog.py
================================================================================
TFB SYNC WATCHDOG -- start a full sync only when none has started for too long
================================================================================
v1.0.0 (2026-11-10)

WHY: daily_sync's own cron ("17 4,12,20" UTC) is delivered 3-9 hours late by
  GitHub (7-10 Nov 2026: the 04:17 slot started 11:09, 11:21 and 12:59 UTC; the
  20:17 slot 23:40, 23:46 and 02:46 UTC; one 12:17 slot never started). Every
  sync takes the decision feed down at its first minute and restores it only
  after every page, the audit and the cockpit render (62-133 minutes measured),
  so a sync that starts at a random hour darkens the board at a random hour:
  - 9 Nov: the 04:17 UTC slot started 15:59 Riyadh and the board was dark
    through the US open (17:30 Riyadh);
  - 10 Nov: the 20:17 UTC slot started 05:46 Riyadh, the 06:22 Riyadh dispatch
    queued behind it, the first run verified at 07:59:47 and the queued run
    superseded it two seconds later.
  daily_sync 1.0.13 therefore drops its cron: the Apps Script dispatcher
  (24_Sync_Dispatch, on the minute) is the clock. This watchdog is the safety
  net for a dispatcher that stops (an expired token, a removed trigger).
WHAT: lists the newest production runs of daily_sync.yml (one filtered read
  per event: schedule and workflow_dispatch, so CI runs never crowd them out). When one is queued or running, or the newest one was
  created at most TFB_SYNC_WATCHDOG_MAX_GAP_H hours ago (default 10, clamped
  4..48; the dispatcher's slots are 8 hours apart), it does nothing. Otherwise
  it dispatches one full sync (run_mode=full_sync, include_portfolio=true),
  the same request the dispatcher sends.
GATE: TFB_SYNC_WATCHDOG = off | observe (say what it would do) | on. The
  workflow maps on. An unreadable run list never dispatches (exit 1, visible).
ROLLBACK: set the repository variable TFB_SYNC_WATCHDOG=off, or restore the
  cron in daily_sync.yml.
--------------------------------------------------------------------------------
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import sys
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"

WORKFLOW_FILE = "daily_sync.yml"
PRODUCTION_EVENTS = ("schedule", "workflow_dispatch")
ACTIVE_STATUSES = ("queued", "in_progress", "pending", "waiting", "requested")
DISPATCH_INPUTS = {"run_mode": "full_sync", "include_portfolio": "true"}
UTC = _dt.timezone.utc


def _out(msg: str) -> None:
    print(f"[sync_watchdog v{__version__}] {msg}", flush=True)


def mode() -> str:
    v = (os.getenv("TFB_SYNC_WATCHDOG") or "off").strip().lower()
    return v if v in ("off", "observe", "on") else "off"


def max_gap_h() -> float:
    try:
        v = float((os.getenv("TFB_SYNC_WATCHDOG_MAX_GAP_H") or "10").strip())
    except Exception:
        v = 10.0
    if v != v:
        v = 10.0
    return max(4.0, min(48.0, v))


def _parse(ts: Any) -> Optional[_dt.datetime]:
    try:
        return _dt.datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except Exception:
        return None


def decide(runs: Sequence[Dict[str, Any]], now: _dt.datetime, gap_h: float) -> Tuple[str, str]:
    """PURE: ("dispatch" | "skip" | "unknown", reason) from the newest runs of
    daily_sync.yml. Only production events count. A queued or running sync, or
    one created at most gap_h hours ago, means skip. No readable production
    run is "unknown": the caller never dispatches on it."""
    prod = [r for r in (runs or []) if r.get("event") in PRODUCTION_EVENTS]
    if not prod:
        return "unknown", "no production run of daily_sync.yml in the list"
    active = [r for r in prod if str(r.get("status") or "").lower() in ACTIVE_STATUSES]
    if active:
        return "skip", "a sync is %s (run %s)" % (active[0].get("status"), active[0].get("id"))
    stamps = [(_parse(r.get("created_at")), r) for r in prod]
    stamps = [(t, r) for t, r in stamps if t is not None]
    if not stamps:
        return "unknown", "no readable created_at on the production runs"
    newest_t, newest = max(stamps, key=lambda x: x[0])
    age_h = (now - newest_t).total_seconds() / 3600.0
    if age_h < 0:
        return "unknown", "newest run %s is dated in the future" % newest.get("id")
    if age_h <= gap_h:
        return "skip", "newest sync (run %s, %s) started %.1f h ago (limit %.1f h)" % (
            newest.get("id"), newest.get("event"), age_h, gap_h)
    return "dispatch", "no sync has started for %.1f h (limit %.1f h; newest run %s)" % (
        age_h, gap_h, newest.get("id"))


def _api(method: str, path: str, token: str, body: Optional[Dict[str, Any]] = None,
         opener=urllib.request.urlopen) -> Tuple[int, Any]:  # pragma: no cover - network
    base = (os.getenv("GITHUB_API_URL") or "https://api.github.com").rstrip("/")
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={
        "Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "tfb-sync-watchdog"})
    with opener(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8") or "null"
        return int(resp.status), json.loads(raw)


def main(argv: Optional[Sequence[str]] = None, *, api=_api, now: Optional[_dt.datetime] = None) -> int:
    m = mode()
    gap = max_gap_h()
    if m == "off":
        _out("off (TFB_SYNC_WATCHDOG) - nothing read, nothing dispatched")
        return 0
    repo = (os.getenv("GITHUB_REPOSITORY") or "").strip()
    token = (os.getenv("GITHUB_TOKEN") or "").strip()
    ref = (os.getenv("TFB_SYNC_WATCHDOG_REF") or "main").strip() or "main"
    if not repo or not token:
        _out("ERROR: GITHUB_REPOSITORY or GITHUB_TOKEN missing - nothing dispatched")
        return 1
    # One filtered read per production event: daily_sync.yml also runs on every
    # push and pull request, so an unfiltered first page can hold CI runs only
    # (review on PR #950) and the watchdog would then never see a sync.
    runs: List[Dict[str, Any]] = []
    try:
        for ev in PRODUCTION_EVENTS:
            _code, payload = api(
                "GET", f"/repos/{repo}/actions/workflows/{WORKFLOW_FILE}/runs?event={ev}&per_page=10", token)
            runs.extend(list((payload or {}).get("workflow_runs") or []))
    except Exception as e:  # noqa: BLE001
        _out(f"ERROR: cannot list the sync runs ({type(e).__name__}) - nothing dispatched")
        return 1
    verdict, why = decide(runs, now or _dt.datetime.now(UTC), gap)
    _out(f"mode {m} | {verdict}: {why}")
    if verdict == "unknown":
        return 1
    if verdict != "dispatch":
        return 0
    if m == "observe":
        _out("observe: would dispatch one full sync")
        return 0
    try:
        code, _ = api("POST", f"/repos/{repo}/actions/workflows/{WORKFLOW_FILE}/dispatches", token,
                      {"ref": ref, "inputs": dict(DISPATCH_INPUTS)})
    except Exception as e:  # noqa: BLE001
        _out(f"ERROR: dispatch failed ({type(e).__name__})")
        return 1
    _out(f"dispatched one full sync on {ref} (HTTP {code})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
