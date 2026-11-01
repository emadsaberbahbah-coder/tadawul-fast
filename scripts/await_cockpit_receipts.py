#!/usr/bin/env python3
"""Dispatch the audited decision-feed release as soon as both cockpits consumed
a completed production sync (bounded, read-only wait).

VERSION 1.1.0 (2026-11-02) -- WAIT-ONLY MODE (--wait-only): THE SYNC-RENDER
HANDSHAKE. Nothing removed; the v1.0.0 dispatch mode is byte-identical.
Workflow gate vars.TFB_SYNC_AWAIT_RENDER, default OFF (step-level 'if').

WHY (GitHub Actions run history, 1-2 Nov 2026; owner, 2 Nov: "the system
costs a lot ... improve today, results tomorrow")
-----------------------------------------------------------------------
Every production sync ends with timing-only red runs even when the data is
good: the final job 'Audit final coverage and decision clocks' judges the
native cockpit BEFORE the asynchronous Apps Script render of
Top_10_Investments / Portfolio_Decision has consumed this run (v1.0.0 below:
"the final job vetoes pre-render, so a healthy sync still ends 'failure'
today"), and decision_surface_freshness.yml audits at the same moment
(T10_OLDER_THAN_SOURCE / T10_POOL_RECONCILIATION_UNPROVEN). Seven to eight
minutes later both cockpits read 'source readiness VERIFIED (run <id>)' and
the after-sync release goes green (12:33Z and 19:13Z on 1 Nov). The red
runs hide real failures and burn runner minutes.

WHAT (this mode decides only WHEN the audit reads, never WHAT it decides)
- --wait-only --run-id <this run>: the SAME read-only poll (read_surfaces:
  one batchGet of _Status L1:M60, Portfolio_Decision!A1:B3 and
  Top_10_Investments!A1:B3) and the SAME receipt reader (surface_receipt ->
  audit_decision_surface_freshness._consumed_source_run on cell B2).
- Exit 0 as soon as BOTH cockpits carry exactly one receipt for --run-id;
  exit 0 WITH a ::notice:: when the bounded wait ends without one (default
  --max-minutes 15, hard cap 20, larger values are clamped; poll every
  30..90 s, default 60), when the cohort moved on (a cockpit receipt or the
  Global_Markets feed names a NEWER run), or after three consecutive read
  faults. The audit that follows decides; this mode only delays it.
  Never dispatches, never probes GitHub, never checks provenance (the run
  is this run, still in progress), never writes the workbook, never raises.
- Exit 2 only for configuration errors: run id or sheet id missing or
  invalid, a non-numeric --max-minutes or --poll-s, or no Sheets service
  (credentials).
- The JSON report (--json-out) carries waited_s, polls, consumed_at_s and
  consumed_at_utc per cockpit, faults, and derived fields only (no
  status-line text), plus the notice text that was printed.
- Workflow wiring (both default OFF; the step never fails its job):
  daily_sync.yml v1.0.9 runs it right before 'Audit final coverage and
  decision clocks' and decision_surface_freshness.yml v1.1.0 right before
  'Audit decision surfaces', each with '|| true', gated on
  vars.TFB_SYNC_AWAIT_RENDER (1/true/yes/on); vars.TFB_SYNC_AWAIT_RENDER_MIN
  (default 15) bounds the wait. Every audit judgement, threshold and veto,
  --publish-blockers included, stays exactly as it was.

VERSION 1.0.0 (2026-10-29) -- NEW SCRIPT: RELEASE AFTER SYNC
(readiness brief 28 Oct 14:47 Riyadh, priority 1: "final decision after
sources settle, bounded recovery tied to source progress").
Gate TFB_RELEASE_AFTER_SYNC, default OFF.

WHY (GitHub Actions run history, 28 Oct 2026)
---------------------------------------------
release_decision_feed.yml is the only path that can promote 'TFB Decision
Feed' to EXECUTABLE. It runs on six fixed Riyadh slots or by hand. On 28 Oct
GitHub delivered this repository's crons 1-6 hours late (sync slot 15:17 ->
21:22 Riyadh; release slots 15:45 -> 19:50, 20:45 -> 21:29 and 00:54), so a
cohort that both cockpits had already consumed waited for the next late slot
or for an operator. Sync 40744567865 completed at 19:31Z; its release was
dispatched by hand at 21:07Z (40769338335, EXECUTABLE until 05:24 Riyadh):
96 minutes of the 480-minute window spent with the board reading FEED NOT
ACTIONABLE for a cohort that passed every strict audit.

WHAT (this script decides only WHEN, never WHETHER)
- Run by release_after_sync.yml on workflow_run 'completed' of the Advanced
  Sync, for a main-branch schedule/workflow_dispatch run that concluded
  success or failure (the final job vetoes pre-render, so a healthy sync
  still ends 'failure' today). TFB_RELEASE_AFTER_SYNC off (default): exit 0
  before any I/O; the workflow job is skipped as well.
- Once: SOURCE provenance with the release script's own rule
  (release_decision_feed.check_provenance: a completed production
  daily_sync on main whose final job executed). Not releasable -> stop, no
  dispatch.
- Then polls READ-ONLY (default every 90 s, at most 45 min, hard cap 55):
  ONE batchGet of the three bounded ranges the release and the strict
  decision audit read: _Status L1:M60 (finalize.KEY_RANGE),
  Portfolio_Decision!A1:B3 and Top_10_Investments!A1:B3, with the audit's
  own receipt parser (audit_decision_surface_freshness._consumed_source_run
  on cell B2) and the release's own key-block readers.
  Stop, no dispatch (evidence that the cohort itself moved, never a clock):
    'TFB Feed Global_Markets' or 'TFB Decision Feed' carries another run
      (newer: superseded; older: this run's final veto never landed, so the
      decision chain will not render Top_10 for it); the feed is missing or
      unreadable; the feed is already EXECUTABLE for SOURCE.
  Ready (both cockpits carry exactly one 'source readiness VERIFIED (run
    SOURCE)' receipt) -> probe GitHub, at most once per PROBE_MIN_S:
      the release's in-flight probe of the production writers, and every
      release_decision_feed.yml run not yet completed (any event or mode).
    Either busy -> keep waiting, never dispatch: the release would defer
      behind a writer, and a dispatch queued behind a running release can
      cancel a PENDING kill or re-verify a live release while the cockpit
      re-renders (its B2 reads 'status: running...' for the render, the
      strict re-verification fails and the release script withdraws the
      release for good: release_window_consumed).
    Both clear -> ONE workflow_dispatch of release_decision_feed.yml on main
      with source_run_id=SOURCE, mode=release. The release then re-runs
      every strict audit, gate (TFB_DECISION_RELEASE, the sticky KILL read
      when THAT run starts), probe and compare-and-set in its own private
      concurrency group. Nothing here can write EXECUTABLE.
  Deadline: no dispatch, outcome timeout (the scheduled slots still apply).
- Never writes the workbook. Stores and prints derived fields only (run
  tokens, declaration kind, parsed last-run stamp and state, counts,
  first-seen seconds), never status-line text.

REVIEW FIXES (2026-10-29, independent review, same unreleased v1.0.0)
- A busy production writer no longer ENDS the attempt: page_refresh_recovery,
  grid_capacity_repair or a health_only sync never start an attempt of
  their own, so ending on them dropped the cohort. Busy now means "do not
  dispatch yet"; only feed evidence (a newer run token) stops the wait.
- Release runs that have not completed block the dispatch (above).
- No blind retry of the dispatch POST (it is not idempotent): a transport
  fault or 5xx is re-attempted only on a later ready poll, after a fresh
  probe that would see the run the first POST may have created. A 4xx, or a
  second failure, fails (exit 3).
- GitHub is probed only on ready polls, at most once per PROBE_MIN_S
  (7 calls per probe): about 70 calls in a 45-minute attempt at most,
  instead of 6 per poll.

EXIT CODES
  0 dispatched, gate off, or a documented stop / timeout (the workflow
    summary carries a warning for the last two)
  3 invalid input, three consecutive read faults or probe faults
    (credentials, layout, transport), or the dispatch failed
  --wait-only (v1.1.0): 0 consumed, or any bounded end with a ::notice::
    (timeout, cohort moved on, read faults, unexpected error);
    2 configuration error only (run id, sheet id, bounds, credentials)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.secret_redaction import safe_error_text  # noqa: E402
from scripts import audit_decision_surface_freshness as freshness  # noqa: E402
from scripts import finalize_decision_publication as final  # noqa: E402
from scripts import release_decision_feed as release  # noqa: E402

SCRIPT_VERSION = "1.1.0"
GATE_ENV = "TFB_RELEASE_AFTER_SYNC"
WAIT_ENV = "TFB_RELEASE_AFTER_SYNC_WAIT_MIN"
RELEASE_WORKFLOW = "release_decision_feed.yml"
RELEASE_REF = "main"
DEFAULT_WAIT_MIN = 45
MAX_WAIT_MIN = 55
DEFAULT_POLL_S = 90
MIN_POLL_S = 30
MAX_POLL_S = 600
PROBE_MIN_S = 270
MAX_FAULTS = 3
MAX_DISPATCH_FAILURES = 2
# v1.1.0 --wait-only: the sync-render handshake (workflow gate, step-level 'if')
WAIT_ONLY_MODE = "wait_only"
WAIT_ONLY_GATE_VAR = "TFB_SYNC_AWAIT_RENDER"
WAIT_ONLY_BOUND_VAR = "TFB_SYNC_AWAIT_RENDER_MIN"
WAIT_ONLY_DEFAULT_MIN = 15
WAIT_ONLY_MAX_MIN = 20
WAIT_ONLY_DEFAULT_POLL_S = 60
WAIT_ONLY_MIN_POLL_S = 30
WAIT_ONLY_MAX_POLL_S = 90
WAIT_ONLY_NOTICE_PREFIX = "::notice::Await render (wait-only): "
PF_RANGE = "Portfolio_Decision!A1:B3"
T10_RANGE = "Top_10_Investments!A1:B3"
RANGES = (final.KEY_RANGE, PF_RANGE, T10_RANGE)
_WITHHELD_RE = re.compile(r"(?<![a-z0-9_])source\s+readiness\s+WITHHELD\b", re.I)
_INT_RE = re.compile(r"[0-9]{1,4}", re.ASCII)


# --------------------------------------------------------------------------- gate
def gate_armed() -> bool:
    """TFB_RELEASE_AFTER_SYNC, default OFF (the release script's vocabulary)."""
    return (os.getenv(GATE_ENV) or "0").strip().lower() in release._ARMED


# --------------------------------------------------------------------------- pure
def surface_receipt(grid: Any) -> dict:
    """PURE. What the strict decision audit will read from one cockpit (cell
    B2, the audit's own parsers): the consumed run, the declaration kind, the
    last-run stamp and the embedded status. Never the status-line text."""
    cell = freshness._cell(grid, 1, 1) if isinstance(grid, list) else None
    text = "" if cell is None else str(cell)
    stamp, state = freshness.parse_surface_status(text)
    consumed = freshness._consumed_source_run(text)
    declarations = len(freshness.SOURCE_READINESS_RE.findall(text))
    if consumed is not None:
        kind = "verified"
    elif declarations == 0:
        kind = "none"
    elif declarations == 1 and _WITHHELD_RE.search(text):
        kind = "withheld"
    else:
        kind = "unproven"
    return {"consumed_run": consumed, "declaration": kind,
            "run_riyadh": stamp.isoformat(sep=" ", timespec="seconds") if stamp else None,
            "state": state or None}


def _decision_value(key_grid: Any) -> Optional[str]:
    """The release script's own step-1 read of the decision feed slot.
    Raises on a layout fault (finalize.publication_slot)."""
    slot = final.publication_slot(key_grid)
    pair = release._pair(key_grid[slot - 1] if slot - 1 < len(key_grid) else [])
    return str(pair[1]) if str(pair[0]).strip() else None


def _moved(run: str, source: str, older_reason: str) -> str:
    return "superseded_by_newer_cohort" if int(run) > int(source) else older_reason


def evaluate(source: str, key_grid: Any, pf_grid: Any, t10_grid: Any) -> dict:
    """PURE. One poll -> {'state': 'ready'|'wait'|'stop', 'reason', derived}.

    Raises on a key-block layout fault so the caller counts it as a fault.
    'ready' is about the workbook only; the caller still probes GitHub."""
    verdict = {"state": "wait", "reason": "receipts_pending",
               "in_flight": None, "release_runs_active": None,
               "gm_run": None, "decision_run": None, "decision_executable": None,
               "pf": surface_receipt(pf_grid), "t10": surface_receipt(t10_grid)}
    value = _decision_value(key_grid)
    gm_run, why = release.discover_source(key_grid)
    verdict["gm_run"] = gm_run
    if gm_run is None:
        verdict.update(state="stop", reason=why or "source_auto_unresolved")
        return verdict
    if gm_run != source:
        verdict.update(state="stop", reason=_moved(gm_run, source, "gm_feed_not_this_run"))
        return verdict
    run = release._single_run(value) if value is not None else None
    verdict["decision_run"] = run
    verdict["decision_executable"] = bool(value is not None and release.reads_executable(value))
    if run is None:
        verdict.update(state="stop", reason="decision_feed_unreadable")
        return verdict
    if run != source:
        verdict.update(state="stop", reason=_moved(run, source, "decision_feed_not_this_run"))
        return verdict
    if verdict["decision_executable"]:
        verdict.update(state="stop", reason="already_executable")
        return verdict
    if verdict["pf"]["consumed_run"] == source and verdict["t10"]["consumed_run"] == source:
        verdict.update(state="ready", reason="both_receipts_verified")
    return verdict


# --------------------------------------------------------------------------- I/O
def read_surfaces(service: Any, sheet_id: str) -> tuple:
    """ONE bounded batchGet of the three ranges (read-only)."""
    response = service.spreadsheets().values().batchGet(
        spreadsheetId=sheet_id, ranges=list(RANGES),
        valueRenderOption="UNFORMATTED_VALUE").execute()
    items = response.get("valueRanges") if isinstance(response, dict) else None
    if not isinstance(items, list) or len(items) != len(RANGES):
        raise ValueError("batchGet returned an unexpected shape")
    grids = []
    for item in items:
        values = item.get("values", []) if isinstance(item, dict) else None
        if not isinstance(values, list):
            raise ValueError("batchGet range values are malformed")
        grids.append([list(row) if isinstance(row, (list, tuple)) else [row] for row in values])
    return tuple(grids)


def github_release_active(*, http_get: Optional[Callable[[str], Any]] = None) -> list:
    """Release runs not yet completed (any event, any mode: release, sweep,
    kill). Raises on any API fault. A dispatch now would queue behind them."""
    get = http_get or release._github_json
    data = get("/repos/{repo}/actions/workflows/%s/runs?per_page=30" % RELEASE_WORKFLOW)
    runs = data.get("workflow_runs") if isinstance(data, dict) else None
    if not isinstance(runs, list):
        raise ValueError("release run listing is malformed")
    active = []
    for item in runs:
        if not isinstance(item, dict):
            raise ValueError("release run entry is malformed")
        if item.get("status") != "completed":
            active.append({"id": item.get("id"), "event": item.get("event"),
                           "status": item.get("status")})
    return active


def github_dispatch(source: str, *, opener: Optional[Callable[..., Any]] = None) -> int:
    """POST one workflow_dispatch of release_decision_feed.yml on main with the
    job token (actions: write). Returns the HTTP status; raises otherwise."""
    token = (os.getenv("GITHUB_TOKEN") or "").strip()
    repo = (os.getenv("GITHUB_REPOSITORY") or "").strip()
    api = (os.getenv("GITHUB_API_URL") or "https://api.github.com").strip().rstrip("/")
    if not token or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise RuntimeError("GitHub API context unavailable (GITHUB_TOKEN/GITHUB_REPOSITORY)")
    if not api.startswith("https://"):
        raise RuntimeError("GitHub API URL must use https")
    if not release.SOURCE_RE.fullmatch(str(source)):
        raise ValueError("dispatch source must be one positive run ID")
    body = json.dumps({"ref": RELEASE_REF,
                       "inputs": {"source_run_id": str(source), "mode": "release"}},
                      separators=(",", ":")).encode("ascii")
    request = urllib.request.Request(
        api + "/repos/" + repo + "/actions/workflows/" + RELEASE_WORKFLOW + "/dispatches",
        data=body, method="POST", headers={
            "Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "Content-Type": "application/json",
            "User-Agent": "tfb-await-cockpit-receipts"})
    with (opener or urllib.request.urlopen)(request, timeout=20) as response:  # noqa: S310
        status = int(getattr(response, "status", 0) or 0)
    if status not in (200, 204):
        raise RuntimeError("dispatch returned HTTP %d" % status)
    return status


def _retry_later(exc: BaseException) -> bool:
    """A transport fault or 5xx may or may not have created the run: only a
    later ready poll, after a fresh probe, may dispatch again."""
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code >= 500
    return isinstance(exc, (urllib.error.URLError, TimeoutError, ConnectionError))


# --------------------------------------------------------------------------- run
def _utc(clock: Callable[[], datetime]) -> str:
    return clock().astimezone(timezone.utc).isoformat(timespec="seconds")


def _finish(out: dict, code: int, outcome: str, *reasons: str) -> dict:
    out["exit_code"] = code
    out["outcome"] = outcome
    for reason in reasons:
        if reason and reason not in out["reasons"]:
            out["reasons"].append(reason)
    return out


def _bounded_int(value: Any, low: int, high: int) -> Optional[int]:
    text = str(value if value is not None else "").strip()
    if not _INT_RE.fullmatch(text):
        return None
    number = int(text)
    return number if low <= number <= high else None


def _as_list(value: Any, what: str) -> list:
    if not isinstance(value, list):
        raise TypeError(what + " returned a non-list")
    return value


def run(sheet_id: Any, source_run_id: Any, *, wait_min: Any = None, poll_s: Any = None,
        service: Any = None, inflight: Optional[Callable[[], Any]] = None,
        release_active: Optional[Callable[[], Any]] = None,
        provenance: Optional[Callable[[str], Any]] = None,
        dispatch: Optional[Callable[[str], Any]] = None,
        sleep: Callable[[float], Any] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now_utc: Optional[Callable[[], datetime]] = None) -> dict:
    clock = now_utc or (lambda: datetime.now(timezone.utc))
    probe_writers = inflight or release.github_inflight
    probe_releases = release_active or github_release_active
    fetch_provenance = provenance or release.github_provenance
    send = dispatch or github_dispatch
    armed = gate_armed()
    source = source_run_id.strip() if isinstance(source_run_id, str) else None
    wait = _bounded_int(DEFAULT_WAIT_MIN if wait_min is None else wait_min, 1, MAX_WAIT_MIN)
    poll = _bounded_int(DEFAULT_POLL_S if poll_s is None else poll_s, MIN_POLL_S, MAX_POLL_S)
    out: dict = {
        "script_version": SCRIPT_VERSION, "gate": armed,
        "source_run_id": source if source and release.SOURCE_RE.fullmatch(source) else None,
        "release_workflow": RELEASE_WORKFLOW, "wait_min": wait, "poll_s": poll,
        "started_utc": _utc(clock), "finished_utc": None, "waited_s": 0, "polls": 0,
        "probes": 0, "source_completed_utc": None, "provenance": None, "last": None,
        "first_seen_s": {"pf": None, "t10": None, "ready": None}, "faults": 0,
        "last_fault": None, "dispatch_attempts": 0, "dispatched": False,
        "dispatch_status": None, "dispatch_error": None,
        "reasons": [], "exit_code": 3, "outcome": None,
    }
    started = monotonic()

    def elapsed() -> int:
        return int(round(max(0.0, monotonic() - started)))

    def done(code: int, outcome: str, *reasons: str) -> dict:
        out["finished_utc"] = _utc(clock)
        out["waited_s"] = elapsed()
        return _finish(out, code, outcome, *reasons)

    def fault(exc: BaseException) -> None:
        out["faults"] += 1
        out["last_fault"] = safe_error_text(exc)

    # 0. gate and identity, before any I/O
    if not armed:
        return done(0, "gate_off", "gate_off")
    if out["source_run_id"] is None:
        return done(3, "refused", "source_run_invalid")
    if source == (os.getenv("GITHUB_RUN_ID") or "").strip():
        return done(3, "refused", "source_is_this_run")
    if not isinstance(sheet_id, str) or not sheet_id.strip():
        return done(3, "refused", "sheet_id_missing")
    if wait is None:
        return done(3, "refused", "wait_min_invalid")
    if poll is None:
        return done(3, "refused", "poll_s_invalid")
    deadline = started + wait * 60

    # 1. provenance of SOURCE, the release script's own rule (bounded retries)
    misses = 0
    while True:
        try:
            raw = fetch_provenance(source)
            prov = release.check_provenance(source, raw)
            break
        except Exception as exc:  # noqa: BLE001 - transport faults retry, then fail
            fault(exc)
            misses += 1
            if misses >= MAX_FAULTS:
                return done(3, "fault", "provenance_unavailable")
            sleep(poll)
    out["provenance"] = {key: prov.get(key) for key in (
        "ok", "reasons", "event", "conclusion", "final_job_conclusion", "run_attempt")}
    raw_run = raw.get("run") if isinstance(raw, dict) else None
    completed = raw_run.get("updated_at") if isinstance(raw_run, dict) else None
    out["source_completed_utc"] = completed if isinstance(completed, str) else None
    if not prov.get("ok"):
        return done(0, "stopped", "source_not_releasable", *prov.get("reasons", []))

    # 2. bounded poll: one batchGet per poll; GitHub only on ready polls
    if service is None:
        try:
            from integrations.google_sheets_service import get_sheets_service
            service = get_sheets_service()
        except Exception as exc:  # noqa: BLE001
            fault(exc)
            return done(3, "fault", "sheets_service_unavailable")
    read_misses = probe_misses = failures = 0
    last_probe: Optional[float] = None
    blocked: Optional[str] = None
    while True:
        out["polls"] += 1
        verdict = None
        try:
            grids = read_surfaces(service, sheet_id)
            verdict = evaluate(source, grids[0], grids[1], grids[2])
            read_misses = 0
        except Exception as exc:  # noqa: BLE001 - counted; three in a row fail
            fault(exc)
            read_misses += 1
            if read_misses >= MAX_FAULTS:
                return done(3, "fault", "read_faults")
        if verdict is not None:
            for name in ("pf", "t10"):
                if out["first_seen_s"][name] is None and verdict[name]["consumed_run"] == source:
                    out["first_seen_s"][name] = elapsed()
            if verdict["state"] == "stop":
                out["last"] = verdict
                return done(0, "stopped", verdict["reason"])
            if verdict["state"] != "ready":
                blocked = None
            else:
                if out["first_seen_s"]["ready"] is None:
                    out["first_seen_s"]["ready"] = elapsed()
                if last_probe is not None and monotonic() - last_probe < PROBE_MIN_S:
                    blocked = blocked or "probe_backoff"
                else:
                    last_probe = monotonic()
                    out["probes"] += 1
                    try:
                        writers = _as_list(probe_writers(), "in-flight probe")
                        releases = _as_list(probe_releases(), "release-run probe")
                        probe_misses = 0
                    except Exception as exc:  # noqa: BLE001 - counted; three in a row fail
                        fault(exc)
                        probe_misses += 1
                        if probe_misses >= MAX_FAULTS:
                            out["last"] = verdict
                            return done(3, "fault", "probe_faults")
                        writers = releases = None
                        blocked = "probe_unavailable"
                    if writers is not None:
                        verdict["in_flight"] = len(writers)
                        verdict["release_runs_active"] = len(releases)
                        if writers:
                            blocked = "production_run_in_flight"
                        elif releases:
                            blocked = "release_run_active"
                        else:
                            out["dispatch_attempts"] += 1
                            try:
                                out["dispatch_status"] = send(source)
                            except Exception as exc:  # noqa: BLE001
                                out["dispatch_error"] = safe_error_text(exc)
                                failures += 1
                                if not _retry_later(exc) or failures >= MAX_DISPATCH_FAILURES:
                                    out["last"] = verdict
                                    return done(3, "dispatch_failed", "dispatch_failed")
                                blocked = "dispatch_unconfirmed"
                            else:
                                out["dispatched"] = True
                                out["dispatch_error"] = None
                                out["last"] = verdict
                                return done(0, "dispatched", "both_receipts_verified")
                verdict.update(state="wait", reason=blocked or "probe_backoff")
            out["last"] = verdict
        if monotonic() + poll > deadline:
            last = out["last"] or {}
            if last.get("state") == "wait" and last.get("reason") != "receipts_pending":
                return done(0, "timeout", "ready_but_blocked", str(last.get("reason")))
            return done(0, "timeout", "receipts_timeout")
        sleep(poll)


# --------------------------------------------------------------------------- wait-only (v1.1.0)
def _newer(run: Optional[str], run_id: str) -> bool:
    """A receipt or feed token that names a run NEWER than --run-id."""
    return run is not None and release.SOURCE_RE.fullmatch(run) is not None and int(run) > int(run_id)


def evaluate_wait(run_id: str, key_grid: Any, pf_grid: Any, t10_grid: Any) -> dict:
    """PURE (v1.1.0). One wait-only poll ->
    {'state': 'consumed'|'wait'|'stop', 'reason', 'gm_run', 'pf', 't10'}.

    The SAME receipt reader as the dispatch mode (surface_receipt -> the
    strict audit's _consumed_source_run on cell B2). The decision feed is not
    read: before this run's final audit it still carries the previous run's
    veto or supersede, which is not evidence about the render. The
    Global_Markets feed token (the release script's discover_source, which
    never raises) only ends the wait early when it names a NEWER run."""
    verdict = {"state": "wait", "reason": "receipts_pending", "gm_run": None,
               "pf": surface_receipt(pf_grid), "t10": surface_receipt(t10_grid)}
    gm_run, _why = release.discover_source(key_grid)
    verdict["gm_run"] = gm_run
    moved = [name for name in ("pf", "t10") if _newer(verdict[name]["consumed_run"], run_id)]
    if _newer(gm_run, run_id):
        moved.append("gm")
    if moved:
        verdict.update(state="stop", reason="superseded_by_newer_cohort", moved=moved)
        return verdict
    if verdict["pf"]["consumed_run"] == run_id and verdict["t10"]["consumed_run"] == run_id:
        verdict.update(state="consumed", reason="both_receipts_verified")
    return verdict


def _wait_bound(value: Any) -> tuple[Optional[int], bool]:
    """(minutes, clamped): 1..WAIT_ONLY_MAX_MIN; larger values are clamped to
    the hard cap (recorded), non-numeric values are a configuration error."""
    text = str(WAIT_ONLY_DEFAULT_MIN if value is None else value).strip()
    if not _INT_RE.fullmatch(text) or int(text) < 1:
        return None, False
    number = int(text)
    return min(number, WAIT_ONLY_MAX_MIN), number > WAIT_ONLY_MAX_MIN


def _wait_notice(out: dict) -> str:
    """Derived fields only (never status-line text); printed as ::notice::."""
    seen = out["consumed_at_s"]
    missing = [name for name in ("pf", "t10") if seen[name] is None]
    where = {"pf": "Portfolio_Decision", "t10": "Top_10_Investments"}
    tail = " after %d s (%d polls); the audit that follows decides." % (out["waited_s"], out["polls"])
    if out["outcome"] == "timeout":
        return ("no receipt for run %s from %s" % (
            out["run_id"], " and ".join(where[name] for name in missing) or "either cockpit") + tail)
    if out["outcome"] == "stopped":
        moved = (out.get("last") or {}).get("moved") or []
        return "the cohort moved on (a newer run in %s)" % ",".join(str(item) for item in moved) + tail
    if out["outcome"] == "read_faults":
        return "%d consecutive read faults (%s)" % (MAX_FAULTS, out["last_fault"]) + tail
    return "%s (%s); the audit that follows decides." % (out["outcome"], ",".join(out["reasons"]))


def wait_only(sheet_id: Any, run_id: Any, *, max_min: Any = None, poll_s: Any = None,
              service: Any = None, sleep: Callable[[float], Any] = time.sleep,
              monotonic: Callable[[], float] = time.monotonic,
              now_utc: Optional[Callable[[], datetime]] = None) -> dict:
    """v1.1.0. Wait (read-only, bounded) until both cockpits carry the receipt
    for run_id. Never dispatches, never probes GitHub, never raises."""
    clock = now_utc or (lambda: datetime.now(timezone.utc))
    run = run_id.strip() if isinstance(run_id, str) else None
    bound, clamped = _wait_bound(max_min)
    poll = _bounded_int(WAIT_ONLY_DEFAULT_POLL_S if poll_s is None else poll_s,
                        WAIT_ONLY_MIN_POLL_S, WAIT_ONLY_MAX_POLL_S)
    out: dict = {
        "script_version": SCRIPT_VERSION, "mode": WAIT_ONLY_MODE,
        "gate_variable": WAIT_ONLY_GATE_VAR,
        "run_id": run if run and release.SOURCE_RE.fullmatch(run) else None,
        "max_min": bound, "max_min_clamped": clamped, "poll_s": poll,
        "started_utc": _utc(clock), "finished_utc": None, "waited_s": 0, "polls": 0,
        "consumed": {"pf": False, "t10": False},
        "consumed_at_s": {"pf": None, "t10": None},
        "consumed_at_utc": {"pf": None, "t10": None},
        "last": None, "faults": 0, "last_fault": None,
        "dispatch_attempts": 0, "dispatched": False,
        "reasons": [], "exit_code": 2, "outcome": None, "notice": None,
    }
    started = monotonic()

    def elapsed() -> int:
        return int(round(max(0.0, monotonic() - started)))

    def done(code: int, outcome: str, *reasons: str) -> dict:
        out["finished_utc"] = _utc(clock)
        out["waited_s"] = elapsed()
        _finish(out, code, outcome, *reasons)
        if outcome != "consumed":
            out["notice"] = _wait_notice(out)
        return out

    def fault(exc: BaseException) -> None:
        out["faults"] += 1
        out["last_fault"] = safe_error_text(exc)

    # 0. configuration, before any I/O (exit 2 is the only non-zero exit)
    if out["run_id"] is None:
        return done(2, "refused", "run_id_invalid")
    if not isinstance(sheet_id, str) or not sheet_id.strip():
        return done(2, "refused", "sheet_id_missing")
    if bound is None:
        return done(2, "refused", "max_minutes_invalid")
    if poll is None:
        return done(2, "refused", "poll_s_invalid")
    if clamped:
        out["reasons"].append("max_minutes_clamped")
    deadline = started + bound * 60
    if service is None:
        try:
            from integrations.google_sheets_service import get_sheets_service
            service = get_sheets_service()
        except Exception as exc:  # noqa: BLE001
            fault(exc)
            return done(2, "refused", "sheets_service_unavailable")

    # 1. bounded poll: one batchGet per poll, nothing else
    misses = 0
    while True:
        out["polls"] += 1
        verdict = None
        try:
            grids = read_surfaces(service, sheet_id)
            verdict = evaluate_wait(run, grids[0], grids[1], grids[2])
            misses = 0
        except Exception as exc:  # noqa: BLE001 - counted; three in a row end the wait
            fault(exc)
            misses += 1
            if misses >= MAX_FAULTS:
                return done(0, "read_faults", "read_faults")
        if verdict is not None:
            for name in ("pf", "t10"):
                if not out["consumed"][name] and verdict[name]["consumed_run"] == run:
                    out["consumed"][name] = True
                    out["consumed_at_s"][name] = elapsed()
                    out["consumed_at_utc"][name] = _utc(clock)
            out["last"] = verdict
            if verdict["state"] == "consumed":
                return done(0, "consumed", "both_receipts_verified")
            if verdict["state"] == "stop":
                return done(0, "stopped", verdict["reason"])
        if monotonic() + poll > deadline:
            return done(0, "timeout", "receipts_timeout")
        sleep(poll)


# --------------------------------------------------------------------------- CLI
_PRINT_KEYS = ("script_version", "gate", "source_run_id", "outcome", "exit_code", "reasons",
               "polls", "probes", "waited_s", "first_seen_s", "dispatch_attempts", "dispatched",
               "dispatch_status", "dispatch_error", "faults", "last_fault")
_WAIT_PRINT_KEYS = ("script_version", "mode", "run_id", "outcome", "exit_code", "reasons",
                    "polls", "waited_s", "max_min", "poll_s", "consumed_at_s", "dispatched",
                    "faults", "last_fault")


def _main_wait_only(args) -> int:
    """v1.1.0: the handshake never raises and fails only on configuration."""
    try:
        result = wait_only(args.sheet_id, args.run_id, max_min=args.max_minutes, poll_s=args.poll_s)
    except Exception as exc:  # noqa: BLE001 - the audit that follows decides
        result = {"script_version": SCRIPT_VERSION, "mode": WAIT_ONLY_MODE, "run_id": None,
                  "exit_code": 0, "outcome": "fatal", "fatal": safe_error_text(exc),
                  "dispatched": False, "reasons": ["fatal"], "polls": None, "waited_s": None,
                  "consumed_at_s": {"pf": None, "t10": None}, "faults": None, "last_fault": None,
                  "notice": "unexpected error (%s); the audit that follows decides."
                            % safe_error_text(exc)}
    text = json.dumps(result, ensure_ascii=True, default=str, indent=2)
    Path(args.json_out).write_text(text + "\n", encoding="utf-8")
    print(json.dumps({key: result.get(key) for key in _WAIT_PRINT_KEYS}, ensure_ascii=True))
    if result.get("notice"):
        print(WAIT_ONLY_NOTICE_PREFIX + str(result["notice"]))
    return int(result["exit_code"])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--sheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID", ""))
    parser.add_argument("--source-run-id", default=None,
                        help="the completed Advanced Sync run that triggered this attempt")
    parser.add_argument("--wait-min", default=os.getenv(WAIT_ENV) or str(DEFAULT_WAIT_MIN))
    # v1.1.0: no literal default, so each mode resolves its own (90 s / 60 s).
    parser.add_argument("--poll-s", default=None)
    parser.add_argument("--json-out", default="await_cockpit_receipts.json")
    # v1.1.0 --wait-only: the sync-render handshake (read-only, never dispatches)
    parser.add_argument("--wait-only", action="store_true",
                        help="wait (read-only) for both cockpit receipts of --run-id; never dispatch")
    parser.add_argument("--run-id", default=None,
                        help="--wait-only: the run whose receipts are awaited (this run)")
    parser.add_argument("--max-minutes", default=str(WAIT_ONLY_DEFAULT_MIN),
                        help="--wait-only: bounded wait in minutes (hard cap %d)" % WAIT_ONLY_MAX_MIN)
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        return 3
    if args.wait_only:
        return _main_wait_only(args)
    try:
        result = run(args.sheet_id, args.source_run_id, wait_min=args.wait_min, poll_s=args.poll_s)
    except Exception as exc:  # noqa: BLE001
        result = {"script_version": SCRIPT_VERSION, "exit_code": 3, "outcome": "fatal",
                  "fatal": safe_error_text(exc), "dispatched": False, "reasons": ["fatal"]}
    text = json.dumps(result, ensure_ascii=True, default=str, indent=2)
    Path(args.json_out).write_text(text + "\n", encoding="utf-8")
    print(json.dumps({key: result.get(key) for key in _PRINT_KEYS}, ensure_ascii=True))
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
