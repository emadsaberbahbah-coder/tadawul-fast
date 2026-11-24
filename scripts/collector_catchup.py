#!/usr/bin/env python3
"""
scripts/collector_catchup.py
================================================================================
TFB COLLECTOR CATCH-UP -- start a nightly data collector that missed its slot
================================================================================
v1.0.0 (2026-11-24)

WHY (owner, 24 Nov 2026 09:08 Riyadh: "full session review ... deep check
  healthy review ... add today action system need to developed"). The morning
  check of 24 Nov found the code healthy but the night's data late, because
  GitHub delivers the collectors' crons hours late or not at all:
  - bq_archive (cron 23:05 UTC) ran at 03:40, 02:58, 03:53 UTC on 21-23 Nov
    and had not started by 06:10 UTC on 24 Nov;
  - bq_markets (22:35 UTC) ran at 03:03 UTC on 23 Nov and had not started for
    the 23 Nov slot, so SPY's last close in BigQuery was Friday 20 Nov and
    Monday's NVDA day was missing from the news-and-trend views;
  - news_archive (14:00 and 20:00 UTC) skipped the 14:00 slot of 23 Nov;
  - bq_symbol_deep (00:20 UTC) starts about 07:30 UTC every day.
  Every forward record that started counting on 24 Nov reads these tables, so
  late data delays every check. The full sync already has a watchdog
  (scripts/sync_watchdog.py); the collectors had none.
WHAT: for each collector in COLLECTORS, the latest slot whose grace has passed
  (slot + grace_h <= now, weekdays honoured, never older than LOOKBACK_H) is
  checked against the collector's runs (schedule and workflow_dispatch events;
  a run counts from the slot's time on):
  - a run queued or in progress -> skip (it is coming);
  - a successful run since the slot -> ok;
  - no run since the slot -> missed: dispatch one write run;
  - one failed run since the slot -> dispatch one retry;
  - two or more runs since the slot, the newest not a success -> report only
    (a fault a retry will not fix; never a loop).
  Every collector rewrites its day's partition (WRITE_TRUNCATE), so a late
  scheduled run arriving after a catch-up run only rewrites the same day.
GATE: TFB_COLLECTOR_CATCHUP = off | observe (say what it would do; default in
  the workflow) | on (dispatch). An unreadable run list never dispatches for
  that collector (reported, exit 1 at the end so it shows red).
LOGS: workflow names, slots, run ids and counts only - no amount, quantity,
  account or holding.
ROLLBACK: the repository variable TFB_COLLECTOR_CATCHUP=off.
Not a licensed financial adviser; the owner decides and executes every trade.
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

UTC = _dt.timezone.utc
PRODUCTION_EVENTS = ("schedule", "workflow_dispatch")
ACTIVE_STATUSES = ("queued", "in_progress", "pending", "waiting", "requested")
LOOKBACK_H = 32.0          # a slot older than this is left alone (24 h between slots + the longest grace)

# file, the cron slots (UTC hour, minute), weekdays (Mon=0 .. Sun=6; None = every day),
# grace hours after the slot before it counts as missed, the dispatch inputs of a write run
COLLECTORS: Tuple[Dict[str, Any], ...] = (
    {"file": "bq_markets.yml", "slots": ((22, 35),), "days": None, "grace_h": 3.0,
     "inputs": {"write": "true"}},
    {"file": "bq_archive.yml", "slots": ((23, 5),), "days": None, "grace_h": 5.0,
     "inputs": {"write": "true"}},
    {"file": "bq_options.yml", "slots": ((23, 30),), "days": (0, 1, 2, 3, 4), "grace_h": 4.0,
     "inputs": {"write": "true"}},
    {"file": "bq_symbol_deep.yml", "slots": ((0, 20),), "days": None, "grace_h": 6.0,
     "inputs": {"write": "true"}},
    {"file": "news_archive.yml", "slots": ((14, 0), (20, 0)), "days": None, "grace_h": 3.0,
     "inputs": {}},
)


def _out(msg: str) -> None:
    print(f"[collector_catchup v{__version__}] {msg}", flush=True)


def mode() -> str:
    v = (os.getenv("TFB_COLLECTOR_CATCHUP") or "off").strip().lower()
    return v if v in ("off", "observe", "on") else "off"


def _parse(ts: Any) -> Optional[_dt.datetime]:
    try:
        return _dt.datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except Exception:
        return None


def due_slot(spec: Dict[str, Any], now: _dt.datetime) -> Optional[_dt.datetime]:
    """PURE: the latest slot of `spec` whose grace has passed (slot + grace <= now),
    on an allowed weekday, at most LOOKBACK_H old; None when there is none."""
    grace = _dt.timedelta(hours=float(spec["grace_h"]))
    best: Optional[_dt.datetime] = None
    for back in range(0, 3):
        day = (now - _dt.timedelta(days=back)).date()
        if spec.get("days") is not None and day.weekday() not in spec["days"]:
            continue
        for hh, mm in spec["slots"]:
            slot = _dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=UTC)
            if slot + grace <= now and (now - slot).total_seconds() <= LOOKBACK_H * 3600:
                if best is None or slot > best:
                    best = slot
    return best


def decide(spec: Dict[str, Any], runs: Sequence[Dict[str, Any]], now: _dt.datetime) -> Tuple[str, str]:
    """PURE: ("dispatch" | "ok" | "skip" | "wait" | "stuck", reason) for one collector."""
    slot = due_slot(spec, now)
    if slot is None:
        return "wait", "no slot past its grace in the last %d h" % int(LOOKBACK_H)
    prod = [r for r in (runs or []) if r.get("event") in PRODUCTION_EVENTS]
    active = [r for r in prod if str(r.get("status") or "").lower() in ACTIVE_STATUSES]
    if active:
        return "skip", "run %s is %s" % (active[0].get("id"), active[0].get("status"))
    since = [(t, r) for t, r in ((_parse(r.get("created_at")), r) for r in prod) if t is not None and t >= slot]
    slot_txt = slot.strftime("%Y-%m-%d %H:%M UTC")
    if not since:
        return "dispatch", "no run since the %s slot (grace %.0f h)" % (slot_txt, spec["grace_h"])
    since.sort(key=lambda x: x[0])
    newest = since[-1][1]
    if str(newest.get("conclusion") or "").lower() == "success":
        return "ok", "run %s (%s) succeeded after the %s slot" % (newest.get("id"), newest.get("event"), slot_txt)
    if len(since) == 1:
        return "dispatch", "the only run since the %s slot (%s) ended %s: one retry" % (
            slot_txt, newest.get("id"), newest.get("conclusion"))
    return "stuck", "%d runs since the %s slot, the newest (%s) ended %s: not retried again" % (
        len(since), slot_txt, newest.get("id"), newest.get("conclusion"))


def _api(method: str, path: str, token: str, body: Optional[Dict[str, Any]] = None,
         opener=urllib.request.urlopen) -> Tuple[int, Any]:  # pragma: no cover - network
    base = (os.getenv("GITHUB_API_URL") or "https://api.github.com").rstrip("/")
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={
        "Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "tfb-collector-catchup"})
    with opener(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8") or "null"
        return int(resp.status), json.loads(raw)


def _summary(lines: List[str]) -> None:
    path = (os.getenv("GITHUB_STEP_SUMMARY") or "").strip()
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except Exception:  # noqa: BLE001
        pass


def main(argv: Optional[Sequence[str]] = None, *, api=_api, now: Optional[_dt.datetime] = None) -> int:
    m = mode()
    if m == "off":
        _out("off (TFB_COLLECTOR_CATCHUP) - nothing read, nothing dispatched")
        return 0
    repo = (os.getenv("GITHUB_REPOSITORY") or "").strip()
    token = (os.getenv("GITHUB_TOKEN") or "").strip()
    ref = (os.getenv("TFB_COLLECTOR_CATCHUP_REF") or "main").strip() or "main"
    if not repo or not token:
        _out("ERROR: GITHUB_REPOSITORY or GITHUB_TOKEN missing - nothing dispatched")
        return 1
    t = now or _dt.datetime.now(UTC)
    rc = 0
    table = ["### Collector catch-up (%s, mode %s)" % (t.strftime("%Y-%m-%d %H:%M UTC"), m), "",
             "| Collector | Verdict | Why |", "| --- | --- | --- |"]
    for spec in COLLECTORS:
        wf = spec["file"]
        runs: List[Dict[str, Any]] = []
        try:
            for ev in PRODUCTION_EVENTS:
                _c, payload = api("GET", f"/repos/{repo}/actions/workflows/{wf}/runs?event={ev}&per_page=10", token)
                runs.extend(list((payload or {}).get("workflow_runs") or []))
        except Exception as e:  # noqa: BLE001
            _out(f"{wf}: ERROR cannot list runs ({type(e).__name__}) - nothing dispatched")
            table.append(f"| {wf} | unreadable | {type(e).__name__} |")
            rc = 1
            continue
        verdict, why = decide(spec, runs, t)
        if verdict == "dispatch":
            if m == "observe":
                verdict, why = "would dispatch", why + " (observe)"
            else:
                try:
                    code, _ = api("POST", f"/repos/{repo}/actions/workflows/{wf}/dispatches", token,
                                  {"ref": ref, "inputs": dict(spec["inputs"])})
                    verdict, why = "dispatched", why + f" (HTTP {code})"
                except Exception as e:  # noqa: BLE001
                    verdict, why = "dispatch failed", why + f" ({type(e).__name__})"
                    rc = 1
        if verdict == "stuck":
            rc = 1
        _out(f"{wf}: {verdict}: {why}")
        table.append(f"| {wf} | {verdict} | {why} |")
    _summary(table)
    return rc


if __name__ == "__main__":
    sys.exit(main())
