#!/usr/bin/env python3
"""Release one audited decision-feed cell from the final veto to EXECUTABLE.

VERSION 1.3.1 (2026-10-24, renumbered 2026-10-31; authored as 1.2.1) -- A
SCHEDULED SLOT THAT LANDS INSIDE A SYNC DEFERS (gate
TFB_RELEASE_INFLIGHT_DEFER, default OFF = v1.3.0 byte-identical apart from
script_version). Nothing removed; every acceptance rule, gate, write and
exit code of v1.3.0 is unchanged when the gate is off.

RENUMBERED 2026-10-31 (merge of main #873-#881): main #877 shipped 1.3.0
(render settle, TFB_RELEASE_RENDER_SETTLE, default off) and reserved 1.2.1
for this PR. Because the merged module carries 1.3.0's code, this build is
1.3.1 over it; its block sits above main's 1.3.0 block, which is verbatim.
The two gates touch different steps: this one only steps 1, 4 and 5 (before
any audit), the render settle only step 6b (after the strict audits). With
both off the module is v1.3.0. The text below is as authored on 2026-10-24
(its 'v1.2.0' is the off base it had then).


WHY (2026-10-24)
----------------
The v1.2.0 commit sheet promised that 'a slot that lands inside a sync
defers and retries next slot'. It does not: the prior classification (step
4) and the provenance check (step 5) refuse with exit 3 BEFORE the in-flight
probe (step 5b) can defer, and the one key-block read has no transport
retry. The 05:30 cron slot fires about 09:40-10:30Z and lands inside the delayed
morning daily_sync on most days. Of the 6 red release runs since 19 Oct (as
of 2026-10-24 14:30Z), 3 were refusals inside a running sync and 1 a Sheets
read timeout during one; the other 2 (39245386428, 40102546721) are
dispatched genuine rejections (exit 2 not_accepted) and stay red.
docs/reviews/github-run-census-20261024.md recommends 'align timing; safely
retry reads where appropriate':
- 40083081692 (created 2026-10-24 09:43:28Z) refused superseded_by_run while
  daily_sync 40079965055 (08:56-09:53Z) was in flight; 39938800176 (created
  2026-10-23 10:23:50Z) the same during 39931603565 (09:24-10:35Z).
- 39763157033 (created 2026-10-22 10:26:51Z) refused [source_not_completed,
  final_job_not_executed]: its source, daily_sync 39755653571, was still
  running (completed 10:28:54Z).
- 39584712312 (created 2026-10-21 10:27:24Z) layout_fault: 'TimeoutError: The
  read operation timed out' on the bare key-block read during 39576807720.
Every one wrote nothing (fail closed) and the next slot re-reads everything,
so no number was wrong; but a daily false red on the ARMED promotion path
looks exactly like the refusals that matter (compare_and_set_failed,
release_unconfirmed, kill_unconfirmed, sweep_unconfirmed).

WHAT CHANGED (only when TFB_RELEASE_INFLIGHT_DEFER is armed with the _ARMED
words AND mode=release AND source=auto AND GITHUB_EVENT_NAME=schedule):
- step 1: the one read-only KEY_RANGE get calls execute(num_retries=2), so a
  socket timeout, connection reset, 429 or 5xx is retried by the Sheets
  client (writes are never retried). This also covers the sticky-variable
  kill and the in-release sweep on such a slot (they run inside mode=release
  after this read); mode=sweep and mode=kill get no retry. Off: execute()
  exactly as before.
- step 4: a refusal with reason superseded_by_run whose prior is EXACTLY the
  supersede grammar defers (exit 1, outcome deferred, reasons
  [superseded_by_run, production_run_in_flight]) only when SOURCE is older
  than the superseding run AND the in-flight probe succeeds AND lists the
  superseding run id as busy.
- step 5: a provenance refusal defers only when its reasons include
  source_not_completed, are a subset of {source_not_completed,
  final_job_not_executed}, and the probe succeeds AND lists SOURCE as busy.
- Everything else keeps exit 3: a probe error, a busy list without that run,
  any other refusal reason, final_job_not_executed alone, a dispatched run
  and an explicit numeric source. The deferral writes nothing and runs no
  audit, like production_run_in_flight; out gains 'inflight_defer' (the run
  checked, whether it was busy, any probe error) only when the gate applies.
ROLLBACK: unset the repository variable TFB_RELEASE_INFLIGHT_DEFER (or set
it to anything outside 1/true/yes/on).
VERSION 1.3.0 (2026-10-29) -- A COCKPIT CAUGHT MID-RENDER SETTLES INSTEAD OF
FAILING (gate TFB_RELEASE_RENDER_SETTLE, default OFF = v1.2.0 byte-identical
apart from script_version). Nothing removed; every acceptance rule, gate,
write and exit code is unchanged when the gate is off. (1.2.1 is claimed by
open PR #831.)

WHY (2026-10-29, independent review of PR #876, reproduced on this file's
own test fixtures)
----------------------------------------------------------------------------
Both cockpits overwrite their status cell (B2) with 'Last run <now> |
status: running...' as the FIRST write of every render
(16_Decision_Top10.gs dt10StatusLine_, 10_My_Portfolio.gs pdStatusLine_)
and restore the full line with the receipt only at the end. A release run
whose strict audit reads B2 during that window sees T10_STATUS_NOT_OK (or
PF_STATUS_NOT_OK) 'Embedded status is RUNNING, not OK.', the consumption
finding and the pool-count findings that follow from the short line, and:
- re-verifying a LIVE release of SOURCE (every slot after a promotion does
  this, and the decision chain re-renders Top_10 right after every
  promotion; the 4-hourly wrapper and the IBKR capture watcher render too),
  armed, it WITHDRAWS the release:
    NOT_ACTIONABLE(final_publication) | run=N | ... | decision_failed |
    release=<this run>
  and classify_prior then refuses every later release of N
  (release_window_consumed) until the next sync - hours of a verified
  cohort lost to a two-minute render;
- a FIRST release of a consumed cohort simply fails (exit 2); a dispatched
  attempt ends red and the cohort waits for the next late slot.
The render is a transient state the cockpit itself declares, not a verdict.

WHAT CHANGED (only when TFB_RELEASE_RENDER_SETTLE is armed with the _ARMED
words; mode=release only - sweep and kill run no audit):
- step 6b, new PURE settle_candidates(result): the cockpits whose strict
  decision finding is exactly T10_STATUS_NOT_OK / PF_STATUS_NOT_OK on its
  own surface with 'Embedded status is RUNNING, not OK.' - and only when
  the coverage audit passed AND EVERY decision finding is one that the
  running line itself produces on a running cockpit (Top_10: STATUS_NOT_OK,
  SOURCE_CONSUMPTION_UNPROVEN, POOL_COUNT_MISSING, POOL_RECONCILIATION_
  UNPROVEN; Portfolio_Decision: STATUS_NOT_OK, SOURCE_CONSUMPTION_UNPROVEN;
  measured on this file's fixtures). Any other finding (a stale board, a
  failed status, a cohort, membership or structure finding) settles
  nothing, so the attempt is judged exactly as today.
- When the strict audits fail and the render explains every finding: wait
  RENDER_SETTLE_S (150 s, once: RENDER_SETTLE_MAX = 1), re-run the in-flight
  probe (busy or probe error -> exit 1 deferred, zero writes: a live
  release that failed ONLY on the render stays live, the next slot
  re-verifies), then re-run the SAME strict audits (fresh reads, the clock
  read again) and judge that result instead. A cockpit still mid-render is
  judged exactly as today (fail closed: an armed re-verification still
  withdraws).
- out gains 'render_settle' (attempt, cockpits, wait, the first attempt's
  reasons) only when a settle happened; out['reasons'] then carries the
  final attempt's audit reasons. The printed line is unchanged; the
  workflow summary prints a render_settle row only when that key exists.
REVIEW FIXES (2026-10-29, independent review, same unreleased v1.3.0)
- The first cut settled on ANY RUNNING finding, so a live release that also
  carried a real failure could be KEPT when a writer started or the probe
  failed during the settle (reproduced: 1.2.0 withdrew it, the first cut
  deferred and kept it). Now every finding must be explained by the render.
- One settle instead of two: TFB_DECISION_RELEASE and the sticky KILL are
  read when the run starts, so each settle extends the time a newly set
  KILL is not seen by this run; one 150 s settle bounds it to about three
  minutes.
ROLLBACK: unset the repository variable TFB_RELEASE_RENDER_SETTLE (or set it
to anything outside 1/true/yes/on).

VERSION 1.2.0 (2026-10-19) -- SOURCE DISCOVERY FOR SCHEDULED ATTEMPTS.
Nothing removed; every acceptance rule, gate, write and exit code of v1.1.0
is unchanged.

WHY (2026-10-19, live Top_10 export 11:29 Riyadh)
--------------------------------------------------
The board has rendered 'FEED NOT ACTIONABLE ... 0 EXECUTABLE' on every
cockpit run since 15 Oct although 11-12 names qualify on merit: the feed is
downgrade-only (the final job vetoes before the cockpits consume the cohort)
and the only promotion path is this script, dispatched BY HAND with the
sync run id. Nobody dispatched it, so the stopper was procedural: the
release needs an operator at the right minute, after the cockpits re-render
and before the 480-minute clock, several times a day.
- --source-run-id auto: the cohort is read from the SAME bounded _Status
  L:M block the script already reads (one read, no new range): the single
  run= token of 'TFB Feed Global_Markets', the last page the sync writes.
  That is exactly the cohort the cockpits' receipts must name, so a wrong
  guess cannot pass the strict audits. Unresolved (key missing, repeated,
  or without exactly one run= token) -> refused, source_auto_unresolved
  family, zero writes. mode=kill with an unresolved cohort still retracts a
  live EXECUTABLE using the run token of the value it retracts (the sticky
  kill variable must never be blocked by a cell layout).
- Everything after discovery is the v1.1.0 code path: classify_prior,
  provenance of SOURCE from the GitHub API (completed production daily_sync
  on main, final job executed), the in-flight probe, the REAL strict audits
  bound to SOURCE, the release window, compare-and-set, exact readback.
  The gate semantics are untouched: TFB_DECISION_RELEASE off => dry-run
  report, zero writes; the arming prerequisites in the P0-1 commit sheet
  still apply before the owner arms it.
- The workflow (release_decision_feed.yml v1.1.0) now also runs on a
  schedule after the cockpit render slots with source=auto. Until armed,
  each scheduled run is the commit sheet's "zero-code observe experiment"
  performed automatically: its artifact shows accepted/reasons.
  out["source_mode"] records 'auto' or 'explicit'.

VERSION 1.1.0 (2026-10-13) -- SUPERSEDE AT WRITER START (Codex P1 on PR #763,
"bind execution permission to the audited cohort"). Nothing removed.

WHY (2026-10-13)
----------------
With TFB_DECISION_RELEASE armed, a release of run N stayed EXECUTABLE while a
later production writer (a scheduled or dispatched daily_sync, a rerun attempt
of N, the inline recovery job or page_refresh_recovery.yml) re-stamped the
pages: run_dashboard_sync.py leaves an EXECUTABLE feed alone when its own
composite is ready, and the cockpit's dt10UvParse_ never compares the feed's
run= with its cohort. So an unaudited cohort N+1 could render under N's
permission until N+1's final job re-vetoed it.
- New mode 'supersede' (--current-run <GITHUB_RUN_ID>, attempt read from
  GITHUB_RUN_ATTEMPT), run by a gated step at the start of EVERY page-writing
  production job, after its credentials step and before its first write. If
  ANY consumer would read the current feed as EXECUTABLE (reads_executable,
  the widest head rule), it writes exactly ONE downgrade through the
  UNCHANGED finalize.publish_blocker:
    NOT_ACTIONABLE(final_publication) | run=<released run> | <now +03:00> |
    superseded_by_run=<current run>.<attempt>
  and otherwise writes nothing. It never promotes. The released run stays in
  run=, so classify_prior refuses a re-release of it.
- Exit 0 on a no-op or a confirmed downgrade (exact readback, or a re-read
  showing that a parallel leg already downgraded it, key still present);
  exit 3 on any read, layout or readback fault. FAIL CLOSED: the workflow step fails, so every
  later default-success step of that job, including its page write, is
  skipped. A supersede fault therefore never lets a leg write pages under a
  live release.
- classify_prior: a value in the EXACT supersede grammar is eligible only for
  a SOURCE newer than the superseded run AND not older than the superseding
  run (reason superseded_older_run); everything else carrying
  superseded_by_run stays refused. Without this, a superseding run whose own
  final job passed (and therefore wrote no veto) could never be released.
- mode=release, armed: refuse (release_gate_not_canonical, zero I/O) when the
  raw TFB_DECISION_RELEASE value carries surrounding whitespace. The supersede
  step's condition matches the armed words case-insensitively but cannot
  strip, so this keeps "a promotion is possible" => "the supersede step runs".

VERSION 1.0.0 (2026-10-13) -- NEW SCRIPT: review blocker 1 / plan item P0-1 /
owner decision D1 (the explicit, audited release step).

WHY (2026-10-13)
----------------
Since #751 the decision feed ('TFB Decision Feed', _Status L:M) is
downgrade-only. scripts/run_dashboard_sync.py never writes EXECUTABLE, never
creates the key and never replaces a NOT_ACTIONABLE(final_publication) veto;
scripts/finalize_decision_publication.py only writes NOT_ACTIONABLE values.
Once the final job vetoes a run, nothing can ever return the feed to
EXECUTABLE, even after every strict audit passes (review blocker 1). The sync
header already promises "an explicit accepted release"; it was never built.
This script is that release. It edits NOTHING else: the finalizer, the sync,
both audits and the Apps Script stay byte-identical, so the sync stays
downgrade-only and every acceptance rule stays in exactly one place.

HOW (fail closed at every step)
- The strict audits are the REAL finalize.run(run_id=SOURCE, publish=False,
  max_rows=20000). SOURCE is the completed daily_sync run whose cohort the
  cockpits consumed. It is never GITHUB_RUN_ID (in this workflow that is the
  release run, and every cohort check would fail) and never 'local' (which
  silently selects the NON-strict audit). accepted() refuses any result whose
  policy.expected_source_run is not SOURCE.
- Gates (vocabulary 1/true/yes/on, the verifier's _ARMED):
  TFB_DECISION_RELEASE (default off): off => mode=release is a dry-run report
  with ZERO writes. TFB_DECISION_RELEASE_KILL (default off): no promotion,
  ever; any EXECUTABLE is retracted. Kill and sweep downgrades only reduce
  permission, so they are allowed whatever TFB_DECISION_RELEASE says, and they
  write only through the unchanged finalize.publish_blocker. A promotion also
  requires GITHUB_ACTIONS=true and a numeric GITHUB_RUN_ID.
- Value grammar: head token exactly EXECUTABLE; exactly ONE space-separated
  'YYYY-mm-dd HH:MM:SS+03:00' stamp, which is the EXPIRY ANCHOR = the OLDEST
  audited decision input (My_Portfolio publication, Portfolio_Decision run,
  Top_10 run, from the strict decision payload). The cockpit's 480-minute
  clock (DT10_UV_MAX_AGE_MIN, apps_script/16_Decision_Top10.gs) therefore
  equals the audit's 8 h decision-age cap; a release whose anchor + 480 min is
  already past is refused. Every other datetime is T-form, which the cockpit
  timestamp regex cannot match. One run=<SOURCE>; release=<GITHUB_RUN_ID>;
  attempt=<SOURCE run_attempt>.
- Prior value classification, provenance of SOURCE (daily_sync.yml on main,
  schedule|workflow_dispatch, completed, final job executed), an in-flight
  probe of the production writers (re-probed immediately before the write),
  compare-and-set of the slot and prior value, one RAW update and exact
  readback. A conflict or a read/transport fault BEFORE the update is
  attempted is a refusal with zero writes (the release window is kept); any
  failure once the update was attempted leaves a best-effort
  release_unconfirmed downgrade.
- Runs OUTSIDE the tadawul-production-write lease (finalize's docstring says
  it runs under that lease). Deliberate: joining the lease group cancels a
  pending production sync (track_performance.yml documents this). The
  private group tfb-decision-release, the fail-closed in-flight probe and the
  compare-and-set bound the residual race to seconds; see the commit sheet.

REVIEW FIXES (2026-10-13, same unreleased v1.0.0, nothing removed)
- publish_release: a fault in the compare-and-set re-read (transport error,
  layout fault) now raises ReleaseConflict, so run() refuses with ZERO writes
  instead of writing a release_unconfirmed downgrade that would spend the
  run's release window (classify_prior -> release_window_consumed) for a
  write that was never attempted. The update and the readback keep the
  "may have landed" downgrade.
- Gate off, mode=release: when the strict re-verification of a LIVE release
  of SOURCE fails, the downgrade the armed path would write is reported as
  would_downgrade (still zero writes). Disarming never retracts a live
  release; only mode=kill / mode=sweep (or an armed failed re-verification)
  write the downgrade.

EXIT CODES
  0 released, already released, dry-run accepted, swept or killed;
    supersede: nothing to supersede, or the downgrade is confirmed
  1 deferred (in-flight run or probe/provenance unavailable) or warning-only
    (v1.3.1, gate on: also a scheduled auto attempt that lands inside the
    sync that superseded or produced its cohort)
  2 audits failed
  3 refused, compare-and-set/readback failure, layout fault or fatal error;
    supersede: invalid run/attempt, read/layout fault or unconfirmed downgrade
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.secret_redaction import safe_error_text  # noqa: E402
from scripts import audit_decision_surface_freshness as freshness  # noqa: E402
from scripts import finalize_decision_publication as final  # noqa: E402

SCRIPT_VERSION = "1.3.1"
RELEASE_ENV = "TFB_DECISION_RELEASE"
# v1.3.0: a cockpit caught mid-render settles instead of failing (default off).
RENDER_SETTLE_ENV = "TFB_RELEASE_RENDER_SETTLE"
RENDER_SETTLE_S = 150
RENDER_SETTLE_MAX = 1
_RENDER_STATUS_CODES = {"T10_STATUS_NOT_OK": "Top_10_Investments",
                        "PF_STATUS_NOT_OK": "Portfolio_Decision"}
_RENDER_RUNNING_MESSAGE = "Embedded status is RUNNING, not OK."
# The findings a 'status: running...' line itself produces on its cockpit.
_RENDER_EXPLAINED = {
    "Top_10_Investments": frozenset({"T10_STATUS_NOT_OK", "T10_SOURCE_CONSUMPTION_UNPROVEN",
                                     "T10_POOL_COUNT_MISSING", "T10_POOL_RECONCILIATION_UNPROVEN"}),
    "Portfolio_Decision": frozenset({"PF_STATUS_NOT_OK", "PF_SOURCE_CONSUMPTION_UNPROVEN"}),
}
# v1.3.1 (authored as v1.2.1): scheduled-slot deferral and read retry (default off).
INFLIGHT_DEFER_ENV = "TFB_RELEASE_INFLIGHT_DEFER"
KEY_READ_RETRIES = 2
_INFLIGHT_PROVENANCE = frozenset({"source_not_completed", "final_job_not_executed"})
# v1.2.0: --source-run-id auto reads the cohort from this _Status L:M key.
SOURCE_AUTO = "auto"
GM_FEED_KEY = "TFB Feed Global_Markets"
KILL_ENV = "TFB_DECISION_RELEASE_KILL"
_ARMED = {"1", "true", "yes", "on"}
MODES = ("release", "sweep", "kill")
# v1.1.0: writer-start supersede. Deliberately NOT in MODES: run() (the
# release entry point) keeps refusing it; main() routes it to supersede().
SUPERSEDE_MODE = "supersede"
# DT10_UV_MAX_AGE_MIN / DT10_UV_FUTURE_SKEW_MIN in apps_script/16_Decision_Top10.gs.
# Not configurable: the cockpit honours 480 minutes from the first stamp
# whatever this script says (tests read both constants out of the .gs).
RELEASE_TTL_MIN = 480
FUTURE_SKEW_MIN = 15
RIYADH = timezone(timedelta(hours=3))
SOURCE_RE = re.compile(r"[1-9][0-9]*", re.ASCII)
DOWNGRADE_PREFIX = "NOT_ACTIONABLE(final_publication) | "
# Mirror of the dt10UvParse_ timestamp regex (FIRST match anywhere).
GAS_TS_RE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:([+-])(\d{2}):?(\d{2}))?", re.ASCII)
_GAS_NA_RE = re.compile(r"NOT_ACTIONABLE\((.*)\)")
_T = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+03:00"
RELEASE_VALUE_RE = re.compile(
    r"EXECUTABLE \| run=(?P<run>[1-9][0-9]*)"
    r" \| (?P<anchor>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+03:00)"
    r" \| issued=(?P<issued>" + _T + r")"
    r" \| expires=(?P<expires>" + _T + r")"
    r" \| ttl_min=(?P<ttl>[1-9][0-9]*)"
    r" \| release=(?P<release>[1-9][0-9]*)"
    r" \| attempt=(?P<attempt>[1-9][0-9]*)"
    r" \| receipts=T10:(?P=run),PF:(?P=run)"
    r" \| t10_at=(?P<t10>" + _T + r")"
    r" \| pf_at=(?P<pf>" + _T + r")"
    r" \| mp_at=(?P<mp>" + _T + r")"
    r" \| audits=0,0", re.ASCII)
_RELEASE_TOKEN_RE = re.compile(r"(?<![a-z0-9_])release\s*=", re.I)
_SUPERSEDED_RE = re.compile(r"superseded_by_run", re.I)
# v1.1.0: the one value supersede() may write (validated before the write).
# run= is the superseded run, or 'unknown' when the EXECUTABLE it replaced did
# not carry exactly one numeric run token.
SUPERSEDE_VALUE_RE = re.compile(
    r"NOT_ACTIONABLE\(final_publication\) \| run=(?P<run>[1-9][0-9]*|unknown)"
    r" \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+03:00"
    r" \| superseded_by_run=(?P<by>[1-9][0-9]*)\.(?P<attempt>[1-9][0-9]*)", re.ASCII)
_PAYLOAD_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", re.ASCII)

# Production writers whose in-flight runs defer a release (fail closed).
INFLIGHT_WORKFLOWS = ("daily_sync.yml", "page_refresh_recovery.yml", "grid_capacity_repair.yml")
PRODUCTION_EVENTS = ("schedule", "workflow_dispatch")
SOURCE_WORKFLOW_PATH = ".github/workflows/daily_sync.yml"
SOURCE_BRANCH = "main"
FINAL_JOB_NAME = "Verify final decision publication"
_API_MAX_BYTES = 4 * 1024 * 1024


# --------------------------------------------------------------------------- gates
def release_enabled() -> bool:
    """TFB_DECISION_RELEASE, default OFF: off => dry-run report, zero writes."""
    return (os.getenv(RELEASE_ENV) or "0").strip().lower() in _ARMED


def release_killed() -> bool:
    """TFB_DECISION_RELEASE_KILL, default OFF: on => never promote, retract."""
    return (os.getenv(KILL_ENV) or "0").strip().lower() in _ARMED


def render_settle_enabled() -> bool:
    """v1.3.0 TFB_RELEASE_RENDER_SETTLE, default OFF: on => a cockpit caught
    mid-render is re-audited after a bounded settle instead of failing."""
    return (os.getenv(RENDER_SETTLE_ENV) or "0").strip().lower() in _ARMED


def inflight_defer_enabled() -> bool:
    """v1.3.1 TFB_RELEASE_INFLIGHT_DEFER, default OFF (see the WHY)."""
    return (os.getenv(INFLIGHT_DEFER_ENV) or "0").strip().lower() in _ARMED


def _defer_for_run(out: dict, probe: Callable[[], Any], run_id: str) -> bool:
    """v1.3.1: True only when the in-flight probe succeeds and lists run_id
    as busy. Records what it checked in out['inflight_defer']."""
    count, busy, error = _probe_inflight(probe)
    hit = error is None and any(isinstance(item, dict) and str(item.get("id")) == run_id
                                for item in busy)
    out["inflight_defer"] = {"run": run_id, "busy": hit, "probe_error": error}
    if hit:
        out["in_flight"] = busy
        out["deferred"] = True
    return hit


# --------------------------------------------------------------------------- clocks
def riyadh_stamp(dt: datetime) -> str:
    """Exactly finalize's stamp shape: 'YYYY-mm-dd HH:MM:SS+03:00'."""
    return dt.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M:%S") + "+03:00"


def t_stamp(dt: datetime) -> str:
    """T-form datetime the cockpit timestamp regex can never read."""
    return dt.astimezone(RIYADH).strftime("%Y-%m-%dT%H:%M:%S") + "+03:00"


def _parse_t(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S%z")


def _payload_stamp(text: Any) -> Optional[datetime]:
    """Audit payload stamps are Riyadh wall-clock 'YYYY-mm-dd HH:MM:SS'."""
    if not isinstance(text, str) or not _PAYLOAD_TS_RE.fullmatch(text):
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=RIYADH)
    except ValueError:
        return None


def _js_round(value: float) -> int:
    return int(math.floor(value + 0.5))


def _js_utc_ms(y: int, mo: int, d: int, h: int, mi: int, s: int) -> float:
    """JavaScript Date.UTC semantics, including field overflow."""
    if 0 <= y <= 99:
        y += 1900
    year = y + (mo - 1) // 12
    month = (mo - 1) % 12 + 1
    base = datetime(year, month, 1, tzinfo=timezone.utc)
    return (base + timedelta(days=d - 1, hours=h, minutes=mi, seconds=s)).timestamp() * 1000.0


def parse_feed(value: Any, now_utc: datetime) -> dict:
    """PURE mirror of dt10UvParse_ (16_Decision_Top10.gs): state, reason, age."""
    out = {"state": "NOT_ACTIONABLE", "reason": "", "age_min": None}
    text = str(value or "").strip()
    if not text:
        out["reason"] = "no verdict published (arm TFB_SYNC_UPSTREAM_VERDICT)"
        return out
    token = text.split("|")[0].strip()
    match = _GAS_NA_RE.fullmatch(token)
    ts = GAS_TS_RE.search(text)
    if not ts:
        out["reason"] = "verdict timestamp unreadable"
        return out
    try:
        utc = _js_utc_ms(*(int(ts.group(i)) for i in range(1, 7)))
    except (ValueError, OverflowError):
        out["reason"] = "verdict timestamp unreadable"
        return out
    now_ms = now_utc.timestamp() * 1000.0
    if ts.group(7):
        offset = int(ts.group(8)) * 60 + int(ts.group(9))
        if ts.group(7) == "-":
            offset = -offset
        age = (now_ms - (utc - offset * 60000)) / 60000
        reason_future = "m in the future \u2014 clock mismatch; fail-closed"
    else:
        as_utc = (now_ms - utc) / 60000
        as_riyadh = as_utc + 180
        candidates = [a for a in (as_riyadh, as_utc) if a >= -FUTURE_SKEW_MIN]
        age = min(candidates) if candidates else max(as_riyadh, as_utc)
        reason_future = "m in the future \u2014 clock/timezone mismatch; fail-closed"
    out["age_min"] = age
    if age < -FUTURE_SKEW_MIN:
        out["reason"] = "verdict timestamp " + str(_js_round(-age)) + reason_future
        return out
    if age > RELEASE_TTL_MIN:
        out["reason"] = "verdict stale (" + str(_js_round(age)) + "m old)"
        return out
    if token == "EXECUTABLE":
        out["state"] = "EXECUTABLE"
        return out
    out["reason"] = match.group(1) if match else token
    return out


# --------------------------------------------------------------------------- values
def reads_executable(value: Any) -> bool:
    """Any consumer would read this as EXECUTABLE (digest/acceptance are the
    widest: upper-cased head prefix; the cockpit needs the exact token)."""
    return str(value or "").split("|", 1)[0].strip().upper().startswith("EXECUTABLE")


def _run_tokens(value: Any) -> list[str]:
    return freshness.SOURCE_RUN_RE.findall(str(value or ""))


def _single_run(value: Any) -> Optional[str]:
    tokens = _run_tokens(value)
    return tokens[0] if len(tokens) == 1 and SOURCE_RE.fullmatch(tokens[0]) else None


def discover_source(grid: Any) -> tuple[Optional[str], Optional[str]]:
    """PURE (v1.2.0). The cohort the cockpits are expected to have consumed:
    the single run= token of the 'TFB Feed Global_Markets' value in the
    bounded _Status L:M block (the last page the sync writes). Returns
    (run, None) or (None, reason). Never guesses: a missing or repeated key,
    or a value without exactly one run= token, is unresolved."""
    if not isinstance(grid, list):
        return None, "source_auto_layout_invalid"
    hits = []
    for row in grid:
        pair = _pair(row)
        if str(pair[0]).strip().casefold() == GM_FEED_KEY.casefold():
            hits.append(str(pair[1]))
    if not hits:
        return None, "source_auto_key_missing"
    if len(hits) > 1:
        return None, "source_auto_key_duplicated"
    run = _single_run(hits[0])
    if run is None:
        return None, "source_auto_run_unreadable"
    return run, None


def compose_release_value(source: str, *, anchor: datetime, issued: datetime,
                          release_run: str, run_attempt: int, t10_at: datetime,
                          pf_at: datetime, mp_at: datetime) -> str:
    """PURE. The one EXECUTABLE value this script may ever write."""
    expires = anchor + timedelta(minutes=RELEASE_TTL_MIN)
    value = " | ".join((
        "EXECUTABLE", "run=" + source, riyadh_stamp(anchor),
        "issued=" + t_stamp(issued), "expires=" + t_stamp(expires),
        "ttl_min=%d" % RELEASE_TTL_MIN, "release=" + str(release_run),
        "attempt=%d" % run_attempt, "receipts=T10:%s,PF:%s" % (source, source),
        "t10_at=" + t_stamp(t10_at), "pf_at=" + t_stamp(pf_at), "mp_at=" + t_stamp(mp_at),
        "audits=0,0"))
    match = RELEASE_VALUE_RE.fullmatch(value)
    if (match is None or GAS_TS_RE.findall(value) != [GAS_TS_RE.search(value).groups()]
            or _run_tokens(value) != [source]):
        raise ValueError("release value grammar violated")
    return value


def downgrade_value(run_token: str, now: datetime, reason: str, release_token: str) -> str:
    reason = re.sub(r"[^A-Za-z0-9_,.:-]", "_", str(reason or "release_refused"))[:200]
    return (DOWNGRADE_PREFIX + "run=" + run_token + " | " + riyadh_stamp(now) + " | "
            + reason + " | release=" + release_token)


def supersede_value(run_token: str, now: datetime, current_run: str, attempt: str) -> str:
    """PURE (v1.1.0). The one downgrade supersede() may write."""
    value = (DOWNGRADE_PREFIX + "run=" + str(run_token) + " | " + riyadh_stamp(now)
             + " | superseded_by_run=" + str(current_run) + "." + str(attempt))
    if SUPERSEDE_VALUE_RE.fullmatch(value) is None or reads_executable(value):
        raise ValueError("supersede value grammar violated")
    return value


def sweep_reason(value: str, now: datetime) -> Optional[str]:
    """None while a well-formed release is live; else the downgrade reason."""
    match = RELEASE_VALUE_RE.fullmatch(value or "")
    if match is None:
        return "release_invalid"
    try:
        anchor = datetime.strptime(match.group("anchor"), "%Y-%m-%d %H:%M:%S%z")
        expires = _parse_t(match.group("expires"))
    except ValueError:
        return "release_invalid"
    if int(match.group("ttl")) != RELEASE_TTL_MIN or expires != anchor + timedelta(minutes=RELEASE_TTL_MIN):
        return "release_invalid"
    parsed = parse_feed(value, now)
    if now > expires or (parsed["age_min"] is not None and parsed["age_min"] > RELEASE_TTL_MIN):
        return "release_expired"
    if parsed["state"] != "EXECUTABLE":
        return "release_invalid"
    return None


def classify_prior(prior: Optional[str], source: str) -> tuple[str, str]:
    """('eligible'|'already_released'|'refused', reason). Amendment (5)."""
    text = "" if prior is None else str(prior).strip()
    if not text:
        return "eligible", "absent"
    if _SUPERSEDED_RE.search(text):
        # v1.1.0: only the EXACT supersede grammar can be eligible, and only
        # for a SOURCE newer than the superseded run (never a re-release) and
        # not older than the superseding run (its cohort came after the
        # downgrade). Every other superseded_by_run value stays refused.
        sup = SUPERSEDE_VALUE_RE.fullmatch(text)
        if (sup is not None and int(source) >= int(sup.group("by"))
                and (sup.group("run") == "unknown" or int(source) > int(sup.group("run")))):
            return "eligible", "superseded_older_run"
        return "refused", "superseded_by_run"
    head = text.split("|", 1)[0].strip()
    tokens = _run_tokens(text)
    run = tokens[0] if len(tokens) == 1 and SOURCE_RE.fullmatch(tokens[0]) else None
    if head == "EXECUTABLE":
        if run != source:
            return "refused", "executable_other_run"
        if RELEASE_VALUE_RE.fullmatch(text):
            return "already_released", "same_run_release"
        return "refused", "foreign_executable"
    if not _GAS_NA_RE.fullmatch(head):
        return "refused", "unrecognised_head"
    if not tokens:
        return "refused", "owner_hold_no_run"
    if run is None:
        return "refused", "ambiguous_run"
    if int(run) > int(source):
        return "refused", "newer_run"
    if int(run) == int(source):
        if _RELEASE_TOKEN_RE.search(text):
            return "refused", "release_window_consumed"
        return "eligible", "same_run_veto"
    return "eligible", "older_run_veto"


# --------------------------------------------------------------------------- acceptance
def accepted(result: Any, source: str) -> tuple[bool, list[str]]:
    """PURE. No thresholds of its own: the strict finalize verdict only."""
    if not isinstance(result, dict):
        return False, ["result_invalid"]
    reasons = []
    if type(result.get("exit_code")) is not int or result.get("exit_code") != 0:
        reasons.append("exit_code_nonzero")
    if result.get("audit_exit_codes") != [0, 0]:
        reasons.append("audit_exit_codes")
    if result.get("blockers") != []:
        reasons.append("blockers_present")
    if result.get("run_id") != source:
        reasons.append("run_id_not_source")
    policy = result.get("policy") if isinstance(result.get("policy"), dict) else {}
    if policy.get("expected_source_run") != source:
        reasons.append("not_strict")
    audits = result.get("audits") if isinstance(result.get("audits"), dict) else {}
    decision = audits.get("decision") if isinstance(audits.get("decision"), dict) else {}
    coverage = audits.get("coverage") if isinstance(audits.get("coverage"), dict) else {}
    if decision.get("executable") is not True:
        reasons.append("decision_not_executable")
    for name, payload in (("coverage", coverage), ("decision", decision)):
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        if type(summary.get("exit_code")) is not int or summary.get("exit_code") != 0:
            reasons.append(name + "_summary_not_zero")
    if decision.get("findings") != []:
        reasons.append("decision_findings_present")
    if not decision.get("top10_run_riyadh") or not decision.get("portfolio_run_riyadh"):
        reasons.append("cockpit_run_stamps_missing")
    return not reasons, reasons


def settle_candidates(result: Any) -> list[str]:
    """PURE (v1.3.0). The cockpits whose own status line says a render is in
    progress (T10_STATUS_NOT_OK / PF_STATUS_NOT_OK on its own surface with
    'Embedded status is RUNNING, not OK.'), returned only when the coverage
    audit passed and EVERY decision finding is one that running line itself
    produces on a running cockpit (_RENDER_EXPLAINED). Anything else (another
    state, any other finding, a malformed payload, a failed coverage audit)
    -> [] (no settle: judged exactly as v1.2.0)."""
    if not isinstance(result, dict):
        return []
    codes = result.get("audit_exit_codes")
    if not isinstance(codes, list) or len(codes) != 2 or type(codes[0]) is not int or codes[0] != 0:
        return []
    audits = result.get("audits")
    decision = audits.get("decision") if isinstance(audits, dict) else None
    findings = decision.get("findings") if isinstance(decision, dict) else None
    if not isinstance(findings, list) or not findings:
        return []
    running = []
    for finding in findings:
        if not isinstance(finding, dict):
            return []
        surface = _RENDER_STATUS_CODES.get(finding.get("code"))
        if (surface and finding.get("message") == _RENDER_RUNNING_MESSAGE
                and finding.get("surface") == surface and surface not in running):
            running.append(surface)
    for finding in findings:
        surface = finding.get("surface")
        if surface not in running or finding.get("code") not in _RENDER_EXPLAINED[surface]:
            return []
    return running


def release_window(result: dict, now: datetime) -> tuple[Optional[dict], list[str]]:
    """Expiry anchor = OLDEST audited decision input; refuse when spent."""
    decision = result.get("audits", {}).get("decision", {})
    stamps = {name: _payload_stamp(decision.get(key)) for name, key in (
        ("mp_at", "my_portfolio_updated_riyadh"), ("pf_at", "portfolio_run_riyadh"),
        ("t10_at", "top10_run_riyadh"))}
    reasons = []
    if any(stamp is None for stamp in stamps.values()):
        return None, ["anchor_unavailable"]
    cap = result.get("policy", {}).get("decision_max_age_h")
    if (isinstance(cap, bool) or not isinstance(cap, (int, float)) or not math.isfinite(cap)
            or cap * 60 < RELEASE_TTL_MIN):
        # A tightened decision-age cap would be outlived by the cockpit's
        # fixed 480-minute clock: never publish beyond the audited cap.
        reasons.append("decision_age_cap_below_cockpit_clock")
    anchor = min(stamps.values())
    expires = anchor + timedelta(minutes=RELEASE_TTL_MIN)
    if now >= expires:
        reasons.append("anchor_expired")
    if anchor > now + timedelta(minutes=FUTURE_SKEW_MIN):
        reasons.append("anchor_future")
    return {"anchor": anchor, "expires": expires, **stamps}, reasons


# --------------------------------------------------------------------------- GitHub
def _github_json(path: str, *, timeout: float = 20.0) -> Any:
    """GET one GitHub REST resource with the job token (actions: read)."""
    token = (os.getenv("GITHUB_TOKEN") or "").strip()
    repo = (os.getenv("GITHUB_REPOSITORY") or "").strip()
    api = (os.getenv("GITHUB_API_URL") or "https://api.github.com").strip().rstrip("/")
    if not token or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise RuntimeError("GitHub API context unavailable (GITHUB_TOKEN/GITHUB_REPOSITORY)")
    if not api.startswith("https://"):
        raise RuntimeError("GitHub API URL must use https")
    request = urllib.request.Request(api + path.replace("{repo}", repo), headers={
        "Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "tfb-release-decision-feed"})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - https only
        body = response.read(_API_MAX_BYTES + 1)
    if len(body) > _API_MAX_BYTES:
        raise RuntimeError("GitHub API response too large")
    return json.loads(body.decode("utf-8"))


def github_inflight(*, http_get: Optional[Callable[[str], Any]] = None) -> list[dict]:
    """Production writer runs not yet completed. Raises on any API fault.

    Only schedule/workflow_dispatch runs write the workbook; push and PR runs
    of the same workflows are CI and are ignored. Every status other than
    completed (queued, in_progress, waiting, pending, requested, ...) is busy.
    """
    get = http_get or _github_json
    busy, seen = [], set()
    for workflow in INFLIGHT_WORKFLOWS:
        for event in PRODUCTION_EVENTS:
            data = get("/repos/{repo}/actions/workflows/%s/runs?event=%s&per_page=50"
                       % (workflow, event))
            runs = data.get("workflow_runs") if isinstance(data, dict) else None
            if not isinstance(runs, list):
                raise ValueError("workflow run listing is malformed")
            for item in runs:
                if not isinstance(item, dict):
                    raise ValueError("workflow run entry is malformed")
                if item.get("event") not in PRODUCTION_EVENTS or item.get("status") == "completed":
                    continue
                key = (workflow, item.get("id"))
                if key not in seen:
                    seen.add(key)
                    busy.append({"workflow": workflow, "id": item.get("id"),
                                 "event": item.get("event"), "status": item.get("status")})
    return busy


def github_provenance(source: str, *, http_get: Optional[Callable[[str], Any]] = None) -> dict:
    get = http_get or _github_json
    run = get("/repos/{repo}/actions/runs/%s" % source)
    jobs = get("/repos/{repo}/actions/runs/%s/jobs?per_page=100" % source)
    return {"run": run, "jobs": jobs.get("jobs") if isinstance(jobs, dict) else None}


def check_provenance(source: str, data: Any) -> dict:
    """PURE. SOURCE must be a completed production daily_sync run on main whose
    'Verify final decision publication' job executed (success or failure)."""
    run = data.get("run") if isinstance(data, dict) else None
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(run, dict) or not isinstance(jobs, list):
        return {"ok": False, "reasons": ["provenance_malformed"]}
    reasons = []
    if str(run.get("id")) != source:
        reasons.append("run_id_mismatch")
    if str(run.get("path") or "").split("@", 1)[0] != SOURCE_WORKFLOW_PATH:
        reasons.append("not_daily_sync")
    if run.get("head_branch") != SOURCE_BRANCH:
        reasons.append("not_main")
    if run.get("event") not in PRODUCTION_EVENTS:
        reasons.append("event_not_production")
    if run.get("status") != "completed":
        reasons.append("source_not_completed")
    attempt = run.get("run_attempt")
    if type(attempt) is not int or attempt < 1:
        reasons.append("run_attempt_invalid")
    finals = [job for job in jobs if isinstance(job, dict)
              and str(job.get("name") or "").strip().endswith(FINAL_JOB_NAME)]
    executed = [job for job in finals if job.get("status") == "completed"
                and job.get("conclusion") in ("success", "failure")]
    if len(finals) != 1 or len(executed) != 1:
        reasons.append("final_job_not_executed")
    return {"ok": not reasons, "reasons": reasons, "head_sha": run.get("head_sha"),
            "run_attempt": attempt if type(attempt) is int else None, "event": run.get("event"),
            "conclusion": run.get("conclusion"),
            "final_job_conclusion": executed[0].get("conclusion") if len(executed) == 1 else None}


def _probe_inflight(probe: Callable[[], Any]) -> tuple[Optional[int], list, Optional[str]]:
    try:
        busy = probe()
        if isinstance(busy, bool):
            raise TypeError("in-flight probe returned a bool")
        if isinstance(busy, int):
            return busy, [], None
        if isinstance(busy, (list, tuple)):
            return len(busy), list(busy), None
        raise TypeError("in-flight probe returned an invalid value")
    except Exception as exc:  # noqa: BLE001 - every probe fault defers
        return None, [], safe_error_text(exc)


# --------------------------------------------------------------------------- writer
def _pair(row: Any) -> tuple:
    values = list(row[:2]) if isinstance(row, list) else []
    values += [""] * (2 - len(values))
    return tuple("" if value is None else value for value in values)


class ReleaseConflict(ValueError):
    """Refused before any write: guard or compare-and-set failure."""
    reason = "compare_and_set_failed"


def publish_release(service: Any, spreadsheet_id: str, value: str, *, expected_slot: int,
                    expected_pair: tuple, fin: Any = final) -> None:
    """Compare-and-set, ONE bounded RAW update, exact readback."""
    if not isinstance(value, str) or not RELEASE_VALUE_RE.fullmatch(value):
        raise ReleaseConflict("release writer refuses a value outside the release grammar")
    try:
        # Nothing has been attempted yet: ANY fault here (transport, layout)
        # is a refusal with zero writes, never a "may have landed" downgrade.
        values = service.spreadsheets().values()
        grid = values.get(spreadsheetId=spreadsheet_id, range=fin.KEY_RANGE,
                          valueRenderOption="UNFORMATTED_VALUE").execute().get("values")
        slot = fin.publication_slot(grid)
        current = _pair(grid[slot - 1] if slot - 1 < len(grid) else [])
    except Exception as exc:  # noqa: BLE001 - pre-write: nothing can have landed
        conflict = ReleaseConflict("compare-and-set read failed before any write: "
                                   + safe_error_text(exc))
        conflict.reason = "compare_and_set_read_failed"
        raise conflict from exc
    if slot != expected_slot or current != tuple(expected_pair):
        raise ReleaseConflict("decision feed changed since it was read; release refused")
    target = f"'_Status'!L{slot}:M{slot}"
    expected = [[fin.KEY, value]]
    values.update(spreadsheetId=spreadsheet_id, range=target,
                  valueInputOption="RAW", body={"values": expected}).execute()
    actual = values.get(spreadsheetId=spreadsheet_id, range=target,
                        valueRenderOption="UNFORMATTED_VALUE").execute().get("values")
    if actual != expected:
        raise RuntimeError("decision feed release unconfirmed")


# --------------------------------------------------------------------------- run
def _finish(out: dict, code: int, outcome: str, *reasons: str) -> dict:
    out["exit_code"] = code
    out["outcome"] = outcome
    for reason in reasons:
        if reason and reason not in out["reasons"]:
            out["reasons"].append(reason)
    return out


async def run(sheet_id: str, source_run_id: Any, *, mode: str = "release", reader=None,
              registry=None, service=None, now_utc: Any = None,
              inflight: Optional[Callable[[], Any]] = None,
              provenance: Optional[Callable[[str], Any]] = None,
              release_run_id: Optional[str] = None, finalize: Any = None,
              settle_sleep: Optional[Callable[[float], Any]] = None) -> dict:
    fin = finalize or final
    if callable(now_utc):
        clock = now_utc
    elif now_utc is not None:
        clock = (lambda fixed=now_utc: fixed)
    else:
        clock = (lambda: datetime.now(timezone.utc))
    probe = inflight or github_inflight
    fetch_provenance = provenance or github_provenance
    armed, kill_var = release_enabled(), release_killed()
    in_actions = (os.getenv("GITHUB_ACTIONS") or "").strip().lower() == "true"
    release_id = str(release_run_id if release_run_id is not None
                     else os.getenv("GITHUB_RUN_ID", "")).strip()
    release_numeric = bool(SOURCE_RE.fullmatch(release_id))
    release_token = release_id if release_numeric else "manual"
    source = source_run_id if isinstance(source_run_id, str) else None
    # v1.2.0: 'auto' defers the cohort to the first (and only) key-block read.
    auto = source == SOURCE_AUTO
    kill = kill_var or mode == "kill"
    out: dict = {
        "script_version": SCRIPT_VERSION, "mode": mode,
        "source_run_id": None if auto else source,
        "source_mode": "auto" if auto else "explicit",
        "release_run_id": release_id or None,
        "release_run_attempt": (os.getenv("GITHUB_RUN_ATTEMPT") or None),
        "release_enabled": armed, "kill_active": kill, "github_actions": in_actions,
        "dry_run": mode == "release" and not armed and not kill, "slot": None, "prior": None, "prior_class": None,
        "provenance": None, "in_flight": [], "deferred": False, "accepted": False,
        "reasons": [], "value": None, "would_release": None, "released": False,
        "expires": None, "downgrade": None, "would_downgrade": None,
        "feed_promoted": False, "blocker_published": False,
        "display_pending_cockpit_refresh": False, "exit_code": 3, "outcome": None,
    }
    # 0. identity and context, before any I/O
    if not auto and (source is None or not SOURCE_RE.fullmatch(source)):
        out["source_run_id"] = None
        return _finish(out, 3, "refused", "source_run_invalid")
    if mode not in MODES:
        return _finish(out, 3, "refused", "mode_invalid")
    if not isinstance(sheet_id, str) or not sheet_id.strip():
        return _finish(out, 3, "refused", "sheet_id_missing")
    if mode == "release" and not kill and armed and not (in_actions and release_numeric):
        return _finish(out, 3, "refused", "promotion_requires_github_actions")
    raw_gate = os.getenv(RELEASE_ENV) or ""
    if mode == "release" and not kill and armed and raw_gate != raw_gate.strip():
        # v1.1.0: the writer-start supersede step matches the armed words
        # case-insensitively but cannot strip whitespace; never promote under a
        # gate spelling that would leave that step skipped.
        return _finish(out, 3, "refused", "release_gate_not_canonical")

    # v1.3.1: the gate applies to scheduled source=auto release attempts only.
    slot_defer = (inflight_defer_enabled() and mode == "release" and auto
                  and (os.getenv("GITHUB_EVENT_NAME") or "").strip() == "schedule")

    # 1. one bounded read of the global key block
    try:
        if service is None:
            from integrations.google_sheets_service import get_sheets_service
            service = get_sheets_service()
        request = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=fin.KEY_RANGE,
            valueRenderOption="UNFORMATTED_VALUE")
        grid = (request.execute(num_retries=KEY_READ_RETRIES) if slot_defer
                else request.execute()).get("values")
        slot = fin.publication_slot(grid)
    except Exception as exc:  # noqa: BLE001 - layout or transport fault
        out["fatal"] = safe_error_text(exc)
        return _finish(out, 3, "layout_fault", "layout_or_read_failed")
    pair = _pair(grid[slot - 1] if slot - 1 < len(grid) else [])
    prior = str(pair[1]) if str(pair[0]).strip() else None
    out["slot"], out["prior"] = slot, prior
    if auto:
        # v1.2.0: the cohort comes from the same bounded block, never from
        # GITHUB_RUN_ID. A kill may still retract using the token of the
        # value it retracts; every other mode refuses an unresolved cohort.
        source, why = discover_source(grid)
        if source is None and kill and prior is not None and _single_run(prior):
            source, why = _single_run(prior), None
        if source is None:
            return _finish(out, 3, "refused", why or "source_auto_unresolved")
        out["source_run_id"] = source

    def downgrade(run_token: str, reason: str) -> bool:
        value = downgrade_value(run_token, clock(), reason, release_token)
        try:
            fin.publish_blocker(service, sheet_id, value)
        except Exception as exc:  # noqa: BLE001
            out["publication_error"] = safe_error_text(exc)
            return False
        out["downgrade"], out["blocker_published"] = value, True
        return True

    executable_prior = prior is not None and reads_executable(prior)
    prior_run = _single_run(prior) if prior is not None else None

    # 2. kill: never promote; retract any EXECUTABLE (allowed with the gate off)
    if kill:
        if executable_prior and not downgrade(prior_run or source, "release_killed"):
            return _finish(out, 3, "kill_unconfirmed", "release_killed")
        return _finish(out, 0 if mode in ("kill", "sweep") else 3, "killed", "release_killed")

    # 3. sweep: retract an expired or malformed EXECUTABLE
    effective_prior = prior
    if executable_prior:
        reason = sweep_reason(prior, clock())
        if reason:
            out["reasons"].append(reason)
            if mode == "sweep" or armed:
                if not downgrade(prior_run or source, reason):
                    return _finish(out, 3, "sweep_unconfirmed", reason)
                effective_prior = out["downgrade"]
                pair = (fin.KEY, effective_prior)
            else:
                effective_prior = downgrade_value(prior_run or source, clock(), reason, release_token)
                out["would_downgrade"] = effective_prior
    if mode == "sweep":
        return _finish(out, 0, "swept" if out["downgrade"] else "nothing_to_sweep")

    # 4. prior classification: refusals spend no audit and write nothing
    prior_class, why = classify_prior(effective_prior, source)
    out["prior_class"] = prior_class
    if prior_class == "refused":
        if slot_defer and why == "superseded_by_run":
            # v1.3.1: the superseding sync is still writing; the next slot
            # reads its cohort. Exact supersede grammar only, and only while
            # SOURCE is OLDER than the superseding run (a re-release, or a
            # superseding run that is not newer than the cohort, stays 3).
            sup = SUPERSEDE_VALUE_RE.fullmatch(str(effective_prior or "").strip())
            if (sup is not None and int(source) < int(sup.group("by"))
                    and _defer_for_run(out, probe, sup.group("by"))):
                return _finish(out, 1, "deferred", why, "production_run_in_flight")
        return _finish(out, 3, "refused", why)

    # 5. provenance of SOURCE and the in-flight probe (fail closed)
    try:
        prov = check_provenance(source, fetch_provenance(source))
    except Exception as exc:  # noqa: BLE001
        out["provenance"] = {"ok": False, "error": safe_error_text(exc)}
        out["deferred"] = True
        return _finish(out, 1, "deferred", "provenance_unavailable")
    out["provenance"] = prov
    if not prov["ok"]:
        if (slot_defer and "source_not_completed" in prov["reasons"]
                and set(prov["reasons"]) <= _INFLIGHT_PROVENANCE
                and _defer_for_run(out, probe, source)):
            # v1.3.1: SOURCE itself is still running (its final job pending).
            return _finish(out, 1, "deferred", *prov["reasons"], "production_run_in_flight")
        return _finish(out, 3, "refused", *prov["reasons"])
    count, busy, error = _probe_inflight(probe)
    out["in_flight"] = busy
    if error is not None or count:
        out["deferred"] = True
        if error is not None:
            out["probe_error"] = error
        return _finish(out, 1, "deferred", "probe_unavailable" if error is not None else "production_run_in_flight")

    # 6. the REAL strict audits, bound to SOURCE (never GITHUB_RUN_ID, never 'local')
    pre_audit_reasons = list(out["reasons"])   # v1.3.0: the base for a settled re-audit

    async def strict_audits():
        # v1.3.0: the ONE call site (case_n22), shared by the step-6b re-audit.
        return await fin.run(sheet_id, reader=reader, registry=registry, publish=False,
                             max_rows=20000, run_id=source)
    try:
        result = await strict_audits()
    except Exception as exc:  # noqa: BLE001
        out["fatal"] = safe_error_text(exc)
        return _finish(out, 3, "fatal", "finalize_error")
    for key, item in result.items():
        if key in ("script_version", "exit_code"):
            out["finalize_" + key] = item
        elif key not in ("feed_promoted", "blocker_published"):
            out[key] = item
    ok, reasons = accepted(result, source)
    window = None
    if ok:
        window, window_reasons = release_window(result, clock())
        reasons += window_reasons
        ok = not window_reasons
    out["accepted"] = ok
    out["reasons"] += [reason for reason in reasons if reason not in out["reasons"]]

    # 6b. v1.3.0 (TFB_RELEASE_RENDER_SETTLE, default off): a cockpit caught
    #     mid-render is a transient state it declares itself, not a verdict.
    #     Bounded: at most RENDER_SETTLE_MAX settles, each followed by a fresh
    #     in-flight probe and the SAME strict audits; then judged as today.
    settles = 0
    while not ok and settles < RENDER_SETTLE_MAX and render_settle_enabled():
        running = settle_candidates(result)
        if not running:
            break
        settles += 1
        out.setdefault("render_settle", []).append({
            "attempt": settles, "cockpits": running, "wait_s": RENDER_SETTLE_S,
            "reasons": [reason for reason in reasons]})
        await (settle_sleep or asyncio.sleep)(RENDER_SETTLE_S)
        count, busy, error = _probe_inflight(probe)
        if error is not None or count:
            out["in_flight"] = busy
            out["deferred"] = True
            out["reasons"] = list(pre_audit_reasons)
            if error is not None:
                out["probe_error"] = error
            return _finish(out, 1, "deferred",
                           "probe_unavailable" if error is not None else "production_run_in_flight")
        try:
            result = await strict_audits()
        except Exception as exc:  # noqa: BLE001
            out["fatal"] = safe_error_text(exc)
            return _finish(out, 3, "fatal", "finalize_error")
        for key, item in result.items():
            if key in ("script_version", "exit_code"):
                out["finalize_" + key] = item
            elif key not in ("feed_promoted", "blocker_published"):
                out[key] = item
        ok, reasons = accepted(result, source)
        window = None
        if ok:
            window, window_reasons = release_window(result, clock())
            reasons += window_reasons
            ok = not window_reasons
        out["accepted"] = ok
        out["reasons"] = pre_audit_reasons + [reason for reason in reasons
                                              if reason not in pre_audit_reasons]

    # 7. not accepted: never promote; retract this run's own release when armed
    #    (gate off: report it as would_downgrade, zero writes)
    if not ok:
        code = result.get("exit_code")
        code = code if type(code) is int and code in (1, 2, 3) else 3
        if executable_prior and prior_run == source:
            blockers = result.get("blockers") or reasons or ["release_reverify_failed"]
            why = ",".join(str(item) for item in blockers)
            if armed:
                if not downgrade(source, why):
                    code = 3
            else:
                # Gate off: report only. Disarming never retracts a live release.
                out["would_downgrade"] = downgrade_value(source, clock(), why, release_token)
        return _finish(out, code, "not_accepted")

    value = None
    if release_numeric:
        value = compose_release_value(
            source, anchor=window["anchor"], issued=clock(), release_run=release_id,
            run_attempt=prov["run_attempt"], t10_at=window["t10_at"],
            pf_at=window["pf_at"], mp_at=window["mp_at"])
    else:
        out["reasons"].append("release_run_id_unavailable_value_not_composed")
    out["expires"] = t_stamp(window["expires"])
    out["anchor"] = riyadh_stamp(window["anchor"])

    # 8. already released: no write, no expiry extension
    if prior_class == "already_released":
        out["value"] = prior
        return _finish(out, 0, "already_released")

    # 9. gate off: dry-run report of the exact value
    if not armed:
        out["would_release"] = value
        return _finish(out, 0, "dry_run_accepted")

    # 10. armed: re-probe, compare-and-set, one write, exact readback
    count, busy, error = _probe_inflight(probe)
    out["in_flight"] = busy
    if error is not None or count:
        out["deferred"] = True
        if error is not None:
            out["probe_error"] = error
        return _finish(out, 1, "deferred", "probe_unavailable" if error is not None else "production_run_in_flight")
    out["value"] = value
    try:
        publish_release(service, sheet_id, value, expected_slot=slot, expected_pair=pair, fin=fin)
    except ReleaseConflict as exc:
        out["publication_error"] = safe_error_text(exc)
        return _finish(out, 3, "refused", getattr(exc, "reason", "compare_and_set_failed"))
    except Exception as exc:  # noqa: BLE001 - the write may have landed
        out["publication_error"] = safe_error_text(exc)
        downgrade(source, "release_unconfirmed")
        return _finish(out, 3, "release_unconfirmed", "release_unconfirmed")
    out["released"] = out["feed_promoted"] = True
    out["display_pending_cockpit_refresh"] = True
    return _finish(out, 0, "released")


# --------------------------------------------------------------------------- supersede
def _read_feed(service: Any, sheet_id: str, fin: Any) -> tuple[int, Optional[str]]:
    """One bounded read of the key block: (slot, value or None when absent)."""
    grid = service.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=fin.KEY_RANGE,
        valueRenderOption="UNFORMATTED_VALUE").execute().get("values")
    slot = fin.publication_slot(grid)
    pair = _pair(grid[slot - 1] if slot - 1 < len(grid) else [])
    return slot, (str(pair[1]) if str(pair[0]).strip() else None)


def supersede(sheet_id: str, current_run_id: Any, *, attempt: Any = None, service: Any = None,
              now_utc: Any = None, finalize: Any = None) -> dict:
    """v1.1.0 writer-start supersede. Downgrade-only, never promotes.

    Run at the start of every page-writing production job, before its first
    write. If any consumer would read the feed as EXECUTABLE, write ONE
    downgrade through the unchanged finalize.publish_blocker; otherwise write
    nothing. Exit 0: nothing to supersede, or the downgrade is confirmed.
    Exit 3: invalid identity, read/layout fault, or unconfirmed downgrade (the
    calling step fails, so the job writes no pages under a live release).
    """
    fin = finalize or final
    if callable(now_utc):
        clock = now_utc
    elif now_utc is not None:
        clock = (lambda fixed=now_utc: fixed)
    else:
        clock = (lambda: datetime.now(timezone.utc))
    run_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "") if attempt is None else attempt
    current = current_run_id if isinstance(current_run_id, str) else None
    run_attempt = run_attempt if isinstance(run_attempt, str) else None
    out: dict = {
        "script_version": SCRIPT_VERSION, "mode": SUPERSEDE_MODE,
        "current_run_id": current, "current_run_attempt": run_attempt,
        "slot": None, "prior": None, "prior_executable": None, "superseded_run": None,
        "downgrade": None, "blocker_published": False, "feed_promoted": False,
        "released": False, "reasons": [], "exit_code": 3, "outcome": None,
    }
    # 0. identity, before any I/O
    if current is None or not SOURCE_RE.fullmatch(current):
        out["current_run_id"] = None
        return _finish(out, 3, "refused", "current_run_invalid")
    if run_attempt is None or not SOURCE_RE.fullmatch(run_attempt):
        out["current_run_attempt"] = None
        return _finish(out, 3, "refused", "run_attempt_invalid")
    if not isinstance(sheet_id, str) or not sheet_id.strip():
        return _finish(out, 3, "refused", "sheet_id_missing")
    # 1. one bounded read of the global key block
    try:
        if service is None:
            from integrations.google_sheets_service import get_sheets_service
            service = get_sheets_service()
        slot, prior = _read_feed(service, sheet_id, fin)
    except Exception as exc:  # noqa: BLE001 - layout or transport fault: fail closed
        out["fatal"] = safe_error_text(exc)
        return _finish(out, 3, "layout_fault", "layout_or_read_failed")
    out["slot"], out["prior"] = slot, prior
    out["prior_executable"] = executable = prior is not None and reads_executable(prior)
    if not executable:
        return _finish(out, 0, "nothing_to_supersede")
    # 2. exactly one downgrade, keeping the released run in run=
    run_token = _single_run(prior) or "unknown"
    out["superseded_run"] = run_token
    value = supersede_value(run_token, clock(), current, run_attempt)
    try:
        fin.publish_blocker(service, sheet_id, value)
    except Exception as exc:  # noqa: BLE001 - the write may or may not have landed
        out["publication_error"] = safe_error_text(exc)
        # A parallel leg of the same run may have downgraded the cell between
        # this leg's update and its readback. What must hold before any page
        # write is only that NO consumer reads EXECUTABLE: re-read once.
        try:
            _slot, after = _read_feed(service, sheet_id, fin)
        except Exception as again:  # noqa: BLE001
            out["reread_error"] = safe_error_text(again)
            return _finish(out, 3, "supersede_unconfirmed", "supersede_unconfirmed")
        out["after"] = after
        # An absent key is not a confirmed downgrade (the brief and the digest
        # fall back to their pre-feed paths on it): fail closed.
        if after is None or reads_executable(after):
            return _finish(out, 3, "supersede_unconfirmed", "supersede_unconfirmed")
        return _finish(out, 0, "superseded_confirmed_by_reread", "readback_raced")
    out["downgrade"], out["blocker_published"] = value, True
    return _finish(out, 0, "superseded")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--sheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID", ""))
    # v1.1.0: no longer required at the parser (mode=supersede takes
    # --current-run instead); every other mode still exits 3 without it.
    parser.add_argument("--source-run-id", default=None,
                        help="completed daily_sync run ID, or 'auto' (v1.2.0: the run= token of "
                             "the _Status 'TFB Feed Global_Markets' value); never GITHUB_RUN_ID")
    parser.add_argument("--mode", default="release")
    parser.add_argument("--current-run", default=None,
                        help="mode=supersede only: this writer's own run ID")
    parser.add_argument("--json-out", default="release_decision_feed.json")
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        return 3
    if args.mode == SUPERSEDE_MODE:
        try:
            result = supersede(args.sheet_id, args.current_run)
        except Exception as exc:  # noqa: BLE001
            result = {"script_version": SCRIPT_VERSION, "mode": SUPERSEDE_MODE, "exit_code": 3,
                      "outcome": "fatal", "fatal": safe_error_text(exc),
                      "feed_promoted": False, "released": False}
        text = json.dumps(result, ensure_ascii=True, default=str, indent=2)
        Path(args.json_out).write_text(text + "\n", encoding="utf-8")
        print(json.dumps({key: result.get(key) for key in (
            "script_version", "mode", "exit_code", "outcome", "current_run_id",
            "current_run_attempt", "prior_executable", "superseded_run", "downgrade",
            "reasons", "blocker_published", "feed_promoted", "fatal", "publication_error")},
            ensure_ascii=True))
        return result["exit_code"]
    if args.source_run_id is None:
        print("release_decision_feed.py: error: the following arguments are required: "
              "--source-run-id", file=sys.stderr)
        return 3
    try:
        result = asyncio.run(run(args.sheet_id, args.source_run_id, mode=args.mode))
    except Exception as exc:  # noqa: BLE001
        result = {"script_version": SCRIPT_VERSION, "exit_code": 3, "outcome": "fatal",
                  "fatal": safe_error_text(exc), "feed_promoted": False, "released": False}
    text = json.dumps(result, ensure_ascii=True, default=str, indent=2)
    Path(args.json_out).write_text(text + "\n", encoding="utf-8")
    print(json.dumps({key: result.get(key) for key in (
        "script_version", "mode", "exit_code", "outcome", "prior_class", "accepted",
        "released", "deferred", "expires", "audit_exit_codes", "blockers", "reasons",
        "blocker_published", "feed_promoted", "fatal", "publication_error")}, ensure_ascii=True))
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
