#!/usr/bin/env python3
"""tests/test_release_decision_feed.py - P0-1 decision-feed RELEASE harness.

VERSION 1.0.0 (2026-10-13) -- review blocker 1 / P0-1 / owner decision D1.

WHY: scripts/release_decision_feed.py v1.0.0 is the only path that may ever
write EXECUTABLE into 'TFB Decision Feed'. This harness drives the REAL
finalize.run (strict audits bound to SOURCE), the REAL audit_surfaces cohort
logic (through a clock-injecting adapter in place of the live reader), the
REAL finalize.publish_blocker / publication_slot, the REAL sync
_write_upstream_verdict, the REAL feed consumers (send_digest, run_daily_brief)
and the REAL cockpit parser dt10UvParse_ (node, the .gs loaded in a vm).
Only the Sheets values API (tests.test_final_decision_publication.GoogleValues),
the coverage report (built by the real coverage auditor) and the GitHub probes
are fixtures. Offline: no network, no credentials, no live Sheet.

WALK  W0 seed veto, W1 gate off dry-run, W2 armed release, W3 re-dispatch,
      W3b sync legs after a release, W4 sweep at 480 min / +1 s, W5 re-release.
NEG   N1-N25 (identity, cohort, audits, classification, CAS, readback, layout,
      probes, provenance, kill, context, env policy, constants, workflow YAML
      and env parity, gate-off zero writes, expiry anchor, CLI, registration,
      pre-write read fault).
GOLD  G1-G6 against base copies (TFB_TEST_FINALIZE_BASE, TFB_TEST_SYNC_BASE;
      sibling files in the same directory for G4-G6). SKIP when unset.
      G1-G4 are base-PROPERTY checks: the finalizer, sync, audits and .gs are
      byte-identical at HEAD, so they hold on a HEAD copy too. G5 (the N24
      registration absent from the base verifier) and G6 (the W2 armed walk
      run against the base tree's release_decision_feed.py, which is EMPTY at
      dda1211) are the DISCRIMINATING negatives: both FAIL when the env points
      at HEAD copies.
NODE  parse_feed parity with the real dt10UvParse_ (SKIP when node is absent).

REVIEW FIXES (2026-10-13): N24 pins a lower bound on the verifier version, not
an exact value (a routine later pin bump must not turn this lane red); N25
pre-write read fault keeps the release window; N17 checks would_downgrade;
G5 made discriminating; G6 added.

Run: pytest -q tests/test_release_decision_feed.py
  or python3 tests/test_release_decision_feed.py      (prints PASS k/k)
"""
from __future__ import annotations

import ast
import asyncio
import copy
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import finalize_decision_publication as final  # noqa: E402
from scripts import release_decision_feed as R  # noqa: E402
from tests.test_decision_surface_freshness import NOW  # noqa: E402
from tests.test_final_decision_publication import (  # noqa: E402
    GoogleValues, complete_source_grids, current_publication_grids, reports,
)
from tests.test_full_refresh_coverage import symbol_column, synthetic_approved_scope  # noqa: E402

GAS = ROOT / "apps_script" / "16_Decision_Top10.gs"
RELEASE_YML = ROOT / ".github" / "workflows" / "release_decision_feed.yml"
DAILY_YML = ROOT / ".github" / "workflows" / "daily_sync.yml"
FINALIZE_BASE = os.environ.get("TFB_TEST_FINALIZE_BASE", "")
SYNC_BASE = os.environ.get("TFB_TEST_SYNC_BASE", "")
SOURCE = "90001"
RELEASE_RUN = "777001"
RIYADH = timezone(timedelta(hours=3))
ANCHOR = datetime(2026, 7, 31, 20, 0, 0, tzinfo=RIYADH)   # My_Portfolio publication
W0_VETO = ("NOT_ACTIONABLE(final_publication) | run=90001 | 2026-07-31 20:10:00+03:00 | "
           "coverage_failed,decision_failed")
W2_VALUE = ("EXECUTABLE | run=90001 | 2026-07-31 20:00:00+03:00 | issued=2026-07-31T21:00:00+03:00"
            " | expires=2026-08-01T04:00:00+03:00 | ttl_min=480 | release=777001 | attempt=1"
            " | receipts=T10:90001,PF:90001 | t10_at=2026-07-31T20:10:00+03:00"
            " | pf_at=2026-07-31T20:05:00+03:00 | mp_at=2026-07-31T20:00:00+03:00 | audits=0,0")
FINAL_JOB_DISPLAY = "\U0001f6e1\ufe0f Verify final decision publication"
SYNC_VETO_RE = re.compile(r"^\s*NOT_ACTIONABLE\s*\(\s*final_publication\s*\)", re.I)
CLEAR_ENV = ("TFB_DECISION_RELEASE", "TFB_DECISION_RELEASE_KILL", "TFB_DECISION_SOURCE_MAX_AGE_H",
             "TFB_DECISION_SURFACE_MAX_AGE_H", "TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO", "APP_TOKEN",
             "GITHUB_TOKEN", "GITHUB_REPOSITORY", "TFB_SYNC_UPSTREAM_VERDICT",
             "TFB_SYNC_VERDICT_PAGES", "TFB_SYNC_VERDICT_MAX_AGE_MIN")
_PHYSICAL: dict = {}


# --------------------------------------------------------------------------- fixtures
def _physical():
    if "grids" not in _PHYSICAL:
        _PHYSICAL["grids"] = complete_source_grids()
    return copy.deepcopy(_PHYSICAL["grids"])


def provenance_ok(source, **changes):
    run = {"id": int(source), "path": ".github/workflows/daily_sync.yml", "head_branch": "main",
           "event": "schedule", "status": "completed", "conclusion": "failure", "run_attempt": 1,
           "head_sha": "0" * 40}
    run.update(changes.pop("run", {}))
    job = {"name": FINAL_JOB_DISPLAY, "status": "completed", "conclusion": "failure"}
    job.update(changes.pop("job", {}))
    return {"run": run, "jobs": changes.pop("jobs", [{"name": "sync-dashboard", "status": "completed",
                                                        "conclusion": "success"}, job])}


class Ctx(SimpleNamespace):
    def release(self, *, mode="release", source=SOURCE, service=None, finalize=None):
        return asyncio.run(R.run("synthetic-sheet", source, mode=mode,
                                 service=service if service is not None else self.service,
                                 now_utc=self.clock["now"], inflight=self.inflight,
                                 provenance=self.provenance, finalize=finalize))

    def updates(self, service=None):
        return [kw for kind, kw in (service or self.service).calls if kind == "update"]


def setup(mp, tmp_path, *, coverage="clean", grids=None, feed=W0_VETO, absent=False):
    tmp_path.mkdir(parents=True, exist_ok=True)
    scope = synthetic_approved_scope(tmp_path)
    mp.setattr(final, "load_scope", lambda: scope)
    for name in CLEAR_ENV:
        mp.delenv(name, raising=False)
    for page in final.MARKET_FLOORS:
        for prefix in ("TFB_EXPECTED_MIN_ROWS_", "TFB_REFRESH_MIN_FRESH_PCT_", "TFB_REFRESH_MAX_AGE_H_"):
            mp.delenv(prefix + page.upper(), raising=False)
    mp.setenv("GITHUB_ACTIONS", "true")
    mp.setenv("GITHUB_RUN_ID", RELEASE_RUN)
    mp.setenv("GITHUB_RUN_ATTEMPT", "1")
    full, _decision = reports(coverage_failure=coverage == "failed", warning=coverage == "warning")
    ctx = Ctx(scope=scope, clock={"now": NOW}, captured={"coverage": [], "decision": []},
              grids=grids or current_publication_grids, probe_calls=[], probe_results=[],
              provenance_calls=[])

    async def coverage_run(*args, **kwargs):
        ctx.captured["coverage"].append(kwargs)
        return full

    async def live_adapter(spreadsheet_id, reader=None, **kwargs):
        # The real auditor and cohort logic; only the clock and grids are injected.
        ctx.captured["decision"].append(kwargs)
        physical = _physical()
        return final.freshness.audit_surfaces(
            *ctx.grids(), now_utc=ctx.clock["now"], min_rows=kwargs["min_rows"],
            market_max_age_h=kwargs["market_max_age_h"], decision_max_age_h=kwargs["decision_max_age_h"],
            min_fresh_percent=kwargs["min_fresh_percent"],
            expected_source_run=kwargs.get("expected_source_run"),
            approved_scope=kwargs["approved_scope"], source_grids=physical,
            source_columns={page: symbol_column(grid) for page, grid in physical.items()})

    mp.setattr(final.coverage, "run", coverage_run)
    mp.setattr(final.freshness, "run_live", live_adapter)

    def inflight():
        ctx.probe_calls.append(ctx.clock["now"])
        if ctx.probe_results:
            item = ctx.probe_results.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        return []

    def provenance(source):
        ctx.provenance_calls.append(source)
        return provenance_ok(source)

    ctx.inflight, ctx.provenance = inflight, provenance
    ctx.service = GoogleValues(feed=not absent)
    if not absent:
        ctx.service.rows[3][11:13] = [final.KEY, feed]
    return ctx


def released(mp, tmp_path):
    """W0 + W2: the armed release of SOURCE at NOW."""
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release()
    assert result["outcome"] == "released", result["reasons"]
    ctx.service.calls.clear()
    return ctx, result


def grids_for_run(run):
    def factory():
        return tuple(json.loads(json.dumps(current_publication_grids()).replace("90001", run)))
    return factory


def live_release_value(run=SOURCE, anchor=ANCHOR, release_run=RELEASE_RUN):
    return R.compose_release_value(run, anchor=anchor, issued=anchor + timedelta(minutes=60),
                                   release_run=release_run, run_attempt=1,
                                   t10_at=anchor + timedelta(minutes=10),
                                   pf_at=anchor + timedelta(minutes=5), mp_at=anchor)


def _load_base(path, name):
    """A base copy under its own module name (registered: dataclasses need it)."""
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def sync_module(path=None):
    if path is None:
        from scripts import run_dashboard_sync as module
        return module
    return _load_base(path, "tfb_release_base_sync")


def finalize_module(path):
    return _load_base(path, "tfb_release_base_finalize")


def run_sync_verdict(module, mp, service, *, fresh_pages=()):
    mp.setenv("TFB_SYNC_UPSTREAM_VERDICT", "1")
    mp.setenv("TFB_SYNC_CAPACITY_STATUS", "0")
    mp.setenv("GITHUB_RUN_ID", "777002")
    for page in fresh_pages:
        slot = next(i for i in range(60) if not str(service.rows[i][11]).strip())
        service.rows[slot][11:13] = ["TFB Feed " + page,
                                     "OK | cov=100 | run=777002 | " + module._status_ts_str()]
    module._write_upstream_verdict(SimpleNamespace(_get_service=lambda: service), "synthetic-sheet", [])


def gas_constant(name):
    match = re.search(r"var " + name + r" = (\d+);", GAS.read_text(encoding="utf-8"))
    assert match, name
    return int(match.group(1))


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


NODE_SCRIPT = r"""
const fs = require('fs'), vm = require('vm');
const ctx = {Logger:{log(){}}, Session:{getScriptTimeZone(){return 'Asia/Riyadh';}}, Utilities:{},
  PropertiesService:{getScriptProperties(){return {getProperty(){return null;}};}}};
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), ctx, {filename: '16_Decision_Top10.gs'});
const cases = JSON.parse(fs.readFileSync(0, 'utf8'));
process.stdout.write(JSON.stringify(cases.map(c => ctx.dt10UvParse_(c.value, c.now_ms))));
"""


def node_parse(cases):
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "dt10_parse.js"
        script.write_text(NODE_SCRIPT, encoding="utf-8")
        payload = json.dumps([{"value": value, "now_ms": now.timestamp() * 1000} for value, now in cases])
        proc = subprocess.run([node, str(script), str(GAS)], input=payload, capture_output=True,
                              text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-2000:]
    return json.loads(proc.stdout)


def workflow(path):
    data = __import__("yaml").safe_load(path.read_text(encoding="utf-8"))
    data["on"] = data.pop(True, data.get("on"))
    return data


# =========================================================================== WALK
def case_w1_gate_off_dry_run_reads_only(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    before = copy.deepcopy(ctx.service.rows)
    result = ctx.release()
    assert result["exit_code"] == 0 and result["outcome"] == "dry_run_accepted", result["reasons"]
    assert result["accepted"] and result["dry_run"] and not result["released"]
    assert result["would_release"] == W2_VALUE and result["prior_class"] == "eligible"
    assert [kind for kind, _ in ctx.service.calls] == ["get"]
    assert ctx.service.rows == before
    assert ctx.captured["decision"][0]["expected_source_run"] == SOURCE
    assert result["policy"]["expected_source_run"] == SOURCE and result["run_id"] == SOURCE
    assert result["audit_exit_codes"] == [0, 0] and result["finalize_exit_code"] == 0


def case_w2_armed_release_one_bounded_write(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release()
    assert result["exit_code"] == 0 and result["outcome"] == "released", result["reasons"]
    calls = ctx.service.calls
    assert [(kind, kw["range"]) for kind, kw in calls] == [
        ("get", final.KEY_RANGE), ("get", final.KEY_RANGE),
        ("update", "'_Status'!L4:M4"), ("get", "'_Status'!L4:M4")]
    assert calls[2][1]["valueInputOption"] == "RAW"
    assert calls[2][1]["body"] == {"values": [[final.KEY, W2_VALUE]]}
    value = ctx.service.rows[3][12]
    assert value == W2_VALUE and R.RELEASE_VALUE_RE.fullmatch(value)
    assert [m.group(0) for m in R.GAS_TS_RE.finditer(value)] == ["2026-07-31 20:00:00+03:00"]
    assert "expires=2026-08-01T04:00:00+03:00" in value
    assert final.freshness.SOURCE_RUN_RE.findall(value) == [SOURCE]
    assert result["expires"] == "2026-08-01T04:00:00+03:00" and result["anchor"] == "2026-07-31 20:00:00+03:00"
    assert result["feed_promoted"] and result["display_pending_cockpit_refresh"]
    assert len(ctx.probe_calls) == 2 and ctx.provenance_calls == [SOURCE]
    from scripts import run_daily_brief, send_digest
    assert send_digest._decision_feed_state({"tfb decision feed": value})[0] == "EXECUTABLE"
    assert run_daily_brief._feed_execution_veto(value) is False
    assert not SYNC_VETO_RE.match(value)   # a later non-ready sync leg can still downgrade it
    for minutes, state in ((0, "EXECUTABLE"), (480, "EXECUTABLE"), (481, "NOT_ACTIONABLE")):
        assert R.parse_feed(value, ANCHOR + timedelta(minutes=minutes))["state"] == state


def case_w3_redispatch_already_released_no_extension(mp, tmp_path):
    ctx, first = released(mp, tmp_path)
    ctx.clock["now"] = NOW + timedelta(minutes=10)
    result = ctx.release()
    assert result["exit_code"] == 0 and result["outcome"] == "already_released"
    assert result["prior_class"] == "already_released" and result["value"] == W2_VALUE
    assert ctx.updates() == [] and ctx.service.rows[3][12] == W2_VALUE
    assert result["expires"] == first["expires"]


def case_w3b_sync_legs_downgrade_or_leave_release(mp, tmp_path):
    module = sync_module()
    ctx, _ = released(mp, tmp_path)
    run_sync_verdict(module, mp, ctx.service)            # page keys missing
    assert ctx.service.rows[3][12].startswith("NOT_ACTIONABLE(missing:ML) | run=777002 | ")
    ctx2, _ = released(mp, tmp_path / "b")
    run_sync_verdict(module, mp, ctx2.service, fresh_pages=module._upstream_verdict_pages())
    assert ctx2.service.rows[3][12] == W2_VALUE
    diag = [row[12] for row in ctx2.service.rows[:60] if row[11] == "TFB Upstream Feed Diagnostic"]
    assert len(diag) == 1 and diag[0].startswith("UPSTREAM_READY | run=777002")


def case_w4_sweep_480_inclusive_then_downgrade_preserved(mp, tmp_path):
    ctx, _ = released(mp, tmp_path)
    mp.delenv("TFB_DECISION_RELEASE")                  # sweep needs no release gate
    ctx.clock["now"] = ANCHOR + timedelta(minutes=480)
    result = ctx.release(mode="sweep")
    assert result["exit_code"] == 0 and result["outcome"] == "nothing_to_sweep" and ctx.updates() == []
    ctx.clock["now"] = ANCHOR + timedelta(minutes=480, seconds=1)
    result = ctx.release(mode="sweep")
    expected = ("NOT_ACTIONABLE(final_publication) | run=90001 | 2026-08-01 04:00:01+03:00 | "
                "release_expired | release=777001")
    assert result["exit_code"] == 0 and result["outcome"] == "swept"
    assert [u["body"]["values"][0][1] for u in ctx.updates()] == [expected]
    assert ctx.service.rows[3][12] == expected and ctx.probe_calls[2:] == []
    run_sync_verdict(sync_module(), mp, ctx.service)     # the sync preserves the downgrade
    assert ctx.service.rows[3][12] == expected


def case_w5_rerelease_same_run_refused(mp, tmp_path):
    ctx, _ = released(mp, tmp_path)
    ctx.clock["now"] = ANCHOR + timedelta(minutes=481)
    ctx.release(mode="sweep")
    ctx.service.calls.clear()
    audits = len(ctx.captured["coverage"])
    result = ctx.release()
    assert result["exit_code"] == 3 and result["reasons"] == ["release_window_consumed"]
    assert ctx.updates() == [] and len(ctx.captured["coverage"]) == audits


# =========================================================================== NEGATIVES
def case_n1_invalid_source_zero_io(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    for source in ("", "local", "0", "0123", " 90001", "abc", "9e3", None, 90001, "90001\n", "-1"):
        result = ctx.release(source=source)
        assert result["exit_code"] == 3 and result["reasons"] == ["source_run_invalid"], source
    assert ctx.service.calls == [] and ctx.captured == {"coverage": [], "decision": []}
    assert ctx.probe_calls == [] and ctx.provenance_calls == []


def case_n2_sheet_cohort_of_another_run_fails(mp, tmp_path):
    ctx = setup(mp, tmp_path, grids=grids_for_run("90002"))
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release()
    assert result["exit_code"] == 2 and not result["accepted"] and ctx.updates() == []
    codes = {f["code"] for f in result["audits"]["decision"]["findings"]}
    assert "SOURCE_PUBLICATION_COHORT" in codes


def case_n3_release_run_id_as_source_fails_cohort(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release(source=RELEASE_RUN)
    assert result["exit_code"] == 2 and ctx.updates() == []
    codes = {f["code"] for f in result["audits"]["decision"]["findings"]}
    assert codes == {"PF_SOURCE_PUBLICATION_COHORT", "PF_SOURCE_CONSUMPTION_UNPROVEN",
                     "SOURCE_PUBLICATION_COHORT", "T10_SOURCE_CONSUMPTION_UNPROVEN"}


def case_n4_warning_only_audits_defer_exit_1(mp, tmp_path):
    ctx = setup(mp, tmp_path, coverage="warning")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release()
    assert result["exit_code"] == 1 and not result["accepted"] and ctx.updates() == []


def case_n5_failing_audits_retract_only_own_release(mp, tmp_path):
    ctx = setup(mp, tmp_path, coverage="failed")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release()
    assert result["exit_code"] == 2 and ctx.updates() == []
    ctx = setup(mp, tmp_path / "b", coverage="failed", feed=live_release_value())
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release()
    assert result["exit_code"] == 2 and result["prior_class"] == "already_released"
    assert [u["body"]["values"][0][1] for u in ctx.updates()] == [
        "NOT_ACTIONABLE(final_publication) | run=90001 | 2026-07-31 21:00:00+03:00 | "
        "coverage_failed | release=777001"]


def case_n6_local_non_strict_result_rejected(mp, tmp_path):
    setup(mp, tmp_path)
    result = asyncio.run(final.run("synthetic-sheet", publish=False, max_rows=20000, run_id="local"))
    assert result["exit_code"] == 0 and "expected_source_run" not in result["policy"]
    ok, reasons = R.accepted(result, SOURCE)
    assert not ok and "not_strict" in reasons and "run_id_not_source" in reasons
    strict = asyncio.run(final.run("synthetic-sheet", publish=False, max_rows=20000, run_id=SOURCE))
    assert R.accepted(strict, SOURCE) == (True, [])
    assert R.accepted(strict, "90002")[0] is False


def case_n7_prior_classification_matrix(mp, tmp_path):
    other_live = live_release_value(run="90000")
    expect = {
        None: ("eligible", "absent"), "": ("eligible", "absent"),
        W0_VETO: ("eligible", "same_run_veto"),
        "NOT_ACTIONABLE(stale_cov:GM) | run=90001 | 2026-07-31 20:10:00+03:00 | ML:OK": ("eligible", "same_run_veto"),
        "NOT_ACTIONABLE(final_publication) | run=90000 | x": ("eligible", "older_run_veto"),
        "NOT_ACTIONABLE(final_publication) | run=90000 | x | release_expired | release=5": ("eligible", "older_run_veto"),
        "NOT_ACTIONABLE(final_publication) | run=90002 | x": ("refused", "newer_run"),
        "NOT_ACTIONABLE(owner hold)": ("refused", "owner_hold_no_run"),
        "NOT_ACTIONABLE(final_publication) | run=local | x": ("refused", "ambiguous_run"),
        "NOT_ACTIONABLE(final_publication) | run=90000 run=90001": ("refused", "ambiguous_run"),
        "NOT_ACTIONABLE(final_publication) | run=90001 | x | release_killed | release=777001": ("refused", "release_window_consumed"),
        "NOT_ACTIONABLE(final_publication) | run=90000 | superseded_by_run=90001": ("refused", "superseded_by_run"),
        "not_actionable(final_publication) | run=90000": ("refused", "unrecognised_head"),
        "ACTIONABLE | run=90001": ("refused", "unrecognised_head"),
        "stale": ("refused", "unrecognised_head"),
        "executable | run=90001": ("refused", "unrecognised_head"),
        "EXECUTABLE(release) | run=90001": ("refused", "unrecognised_head"),
        other_live: ("refused", "executable_other_run"),
        "EXECUTABLE | run=90001 | 2026-07-31 20:00:00+03:00 | legacy": ("refused", "foreign_executable"),
        live_release_value(): ("already_released", "same_run_release"),
    }
    for prior, verdict in expect.items():
        assert R.classify_prior(prior, SOURCE) == verdict, prior
    # End to end: refusals spend no audit and write nothing (non-EXECUTABLE heads).
    for prior in ("NOT_ACTIONABLE(final_publication) | run=90002 | x", "NOT_ACTIONABLE(owner hold)",
                  "ACTIONABLE | run=90001", other_live):
        ctx = setup(mp, tmp_path / str(abs(hash(prior))), feed=prior)
        mp.setenv("TFB_DECISION_RELEASE", "1")
        result = ctx.release()
        assert result["exit_code"] == 3 and result["prior_class"] == "refused", prior
        assert ctx.updates() == [] and ctx.captured["coverage"] == [] and ctx.probe_calls == []
    # A malformed EXECUTABLE that age-blind consumers honour is retracted when armed,
    # reported only in the dry-run, and never released.
    for armed in (False, True):
        ctx = setup(mp, tmp_path / ("lower%s" % armed), feed="executable | run=90001 | 2026-07-31 20:00:00+03:00")
        if armed:
            mp.setenv("TFB_DECISION_RELEASE", "1")
        else:
            mp.delenv("TFB_DECISION_RELEASE", raising=False)
        result = ctx.release()
        assert result["exit_code"] == 3 and result["reasons"][-1] == "release_window_consumed"
        assert "release_invalid" in result["reasons"]
        assert len(ctx.updates()) == (1 if armed else 0)
        assert (result["would_downgrade"] is None) is armed
    # Older-run veto is released; an absent key lands in the first blank slot.
    ctx = setup(mp, tmp_path / "older", feed="NOT_ACTIONABLE(final_publication) | run=90000 | x")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    assert ctx.release()["outcome"] == "released" and ctx.service.rows[3][12] == W2_VALUE
    ctx = setup(mp, tmp_path / "absent", absent=True)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    result = ctx.release()
    assert result["outcome"] == "released" and result["slot"] == 3
    assert ctx.service.rows[2][11:13] == [final.KEY, W2_VALUE]


def case_n8_compare_and_set_conflict(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")

    class Conflict(GoogleValues):
        reads = 0

        def get(self, **kwargs):
            if kwargs["range"] == final.KEY_RANGE:
                Conflict.reads += 1
                if Conflict.reads == 2:
                    self.rows[3][12] = "NOT_ACTIONABLE(final_publication) | run=90003 | new veto"
            return super().get(**kwargs)

    service = Conflict()
    service.rows[3][11:13] = [final.KEY, W0_VETO]
    result = ctx.release(service=service)
    assert result["exit_code"] == 3 and "compare_and_set_failed" in result["reasons"]
    assert ctx.updates(service) == [] and service.rows[3][12].endswith("new veto")


def case_n9_readback_mismatch_unconfirmed_downgrade(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    service = GoogleValues(readback=[[final.KEY, "something else"]])
    service.rows[3][11:13] = [final.KEY, W0_VETO]
    result = ctx.release(service=service)
    assert result["exit_code"] == 3 and result["outcome"] == "release_unconfirmed"
    values = [u["body"]["values"][0][1] for u in ctx.updates(service)]
    assert values[0] == W2_VALUE and len(values) == 2
    assert values[1].startswith("NOT_ACTIONABLE(final_publication) | run=90001 | ")
    assert values[1].endswith(" | release_unconfirmed | release=777001")


LAYOUTS = [
    None, [], [["wrong block", "anything"]],
    [["Backend URL", "x"], ["Backend URL", "duplicate"]],
    [["Backend URL", "x"], [final.KEY, "x"], [" tfb decision feed ", "y"]],
    [["Backend URL", "x"], ["", "orphaned financial value"]],
    [["Backend URL", "x"], ["", 0]], [["Backend URL", "x"], ["", False]],
    [["Backend URL", "x", "moved column"]], [["Backend URL", "x"], "bad row"],
    [["Backend URL", "x"]] + [[str(index), "occupied"] for index in range(59)],
    [["Backend URL", "x"]] + [[] for _ in range(60)],
]


def case_n10_layout_faults_refuse(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    for grid in LAYOUTS:
        calls = []

        class Layout:
            def spreadsheets(self):
                return self

            def values(self):
                return self

            def get(self, **kwargs):
                calls.append(("get", kwargs))
                return SimpleNamespace(execute=lambda: {"values": copy.deepcopy(grid)})

            def update(self, **kwargs):
                calls.append(("update", kwargs))
                raise AssertionError("no write on a layout fault")

        result = ctx.release(service=Layout())
        assert result["exit_code"] == 3 and result["outcome"] == "layout_fault", grid
        assert [kind for kind, _ in calls] == ["get"]
    assert ctx.captured["coverage"] == []


def case_n11_probes_and_provenance_fail_closed(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    for item in ([{"workflow": "daily_sync.yml", "id": 1, "event": "schedule", "status": "waiting"}],
                 1, RuntimeError("api down"), "bad", True):
        ctx.probe_results[:] = [item]
        result = ctx.release()
        assert result["exit_code"] == 1 and result["deferred"], item
        assert ctx.captured["coverage"] == [] and ctx.updates() == []
    ctx.probe_results[:] = [[], [{"workflow": "page_refresh_recovery.yml", "id": 2,
                                  "event": "workflow_dispatch", "status": "pending"}]]
    result = ctx.release()       # second probe, immediately before the write
    assert result["exit_code"] == 1 and result["accepted"] and ctx.updates() == []
    ctx.probe_results[:] = [[], RuntimeError("api down")]
    result = ctx.release()
    assert result["exit_code"] == 1 and result["deferred"] and ctx.updates() == []
    audits = len(ctx.captured["coverage"])

    def failing(source):
        raise RuntimeError("api down")
    ctx.provenance = failing
    result = ctx.release()
    assert result["exit_code"] == 1 and result["reasons"] == ["provenance_unavailable"]
    for changes, reason in (({"run": {"event": "push"}}, "event_not_production"),
                            ({"run": {"status": "in_progress"}}, "source_not_completed"),
                            ({"run": {"path": ".github/workflows/page_refresh_recovery.yml"}}, "not_daily_sync"),
                            ({"run": {"head_branch": "review"}}, "not_main"),
                            ({"job": {"conclusion": "skipped"}}, "final_job_not_executed")):
        ctx.provenance = lambda source, changes=changes: provenance_ok(source, **copy.deepcopy(changes))
        result = ctx.release()
        assert result["exit_code"] == 3 and reason in result["reasons"], reason
    assert len(ctx.captured["coverage"]) == audits and ctx.updates() == []


def case_n12_kill_retracts_never_promotes(mp, tmp_path):
    ctx = setup(mp, tmp_path, feed=live_release_value())
    result = ctx.release(mode="kill")               # gate off: a downgrade is still allowed
    assert result["exit_code"] == 0 and result["outcome"] == "killed"
    assert [u["body"]["values"][0][1] for u in ctx.updates()] == [
        "NOT_ACTIONABLE(final_publication) | run=90001 | 2026-07-31 21:00:00+03:00 | "
        "release_killed | release=777001"]
    ctx = setup(mp, tmp_path / "veto")
    result = ctx.release(mode="kill")
    assert result["exit_code"] == 0 and ctx.updates() == []
    ctx = setup(mp, tmp_path / "armed")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    mp.setenv("TFB_DECISION_RELEASE_KILL", "1")
    result = ctx.release()
    assert result["exit_code"] == 3 and result["outcome"] == "killed" and not result["released"]
    assert ctx.updates() == [] and ctx.captured["coverage"] == [] and ctx.service.rows[3][12] == W0_VETO


def case_n13_armed_promotion_requires_actions_context(mp, tmp_path):
    for env, value in (("GITHUB_ACTIONS", None), ("GITHUB_ACTIONS", "false"),
                       ("GITHUB_RUN_ID", None), ("GITHUB_RUN_ID", "local")):
        ctx = setup(mp, tmp_path / (env + str(value)))
        mp.setenv("TFB_DECISION_RELEASE", "1")
        if value is None:
            mp.delenv(env)
        else:
            mp.setenv(env, value)
        result = ctx.release()
        assert result["exit_code"] == 3 and result["reasons"] == ["promotion_requires_github_actions"]
        assert ctx.service.calls == [] and ctx.captured["coverage"] == []


def case_n14_environment_cannot_weaken_or_outlive_policy(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    for page in final.MARKET_FLOORS:
        suffix = page.upper()
        mp.setenv("TFB_EXPECTED_MIN_ROWS_" + suffix, "1")
        mp.setenv("TFB_REFRESH_MIN_FRESH_PCT_" + suffix, "1")
        mp.setenv("TFB_REFRESH_MAX_AGE_H_" + suffix, "999")
    for name in ("TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO", "TFB_DECISION_SOURCE_MAX_AGE_H",
                 "TFB_DECISION_SURFACE_MAX_AGE_H"):
        mp.setenv(name, "999")
    result = ctx.release()
    assert result["exit_code"] == 0 and result["accepted"]
    rules, policy = final.final_policy(approved_scope=ctx.scope)
    assert ctx.captured["coverage"][0]["policy_rules"] == rules
    decision = ctx.captured["decision"][0]
    assert decision["min_rows"] == policy["min_rows"] and decision["decision_max_age_h"] == 8
    assert decision["market_max_age_h"] == 30 and decision["expected_source_run"] == SOURCE
    mp.setenv("TFB_DECISION_SURFACE_MAX_AGE_H", "4")  # a tightened cap is never outlived
    result = ctx.release()
    assert result["exit_code"] == 3 and "decision_age_cap_below_cockpit_clock" in result["reasons"]
    assert ctx.updates() == []


def case_n15_ttl_and_skew_equal_cockpit_and_audit_cap(mp, tmp_path):
    assert R.RELEASE_TTL_MIN == gas_constant("DT10_UV_MAX_AGE_MIN") == 480
    assert R.FUTURE_SKEW_MIN == gas_constant("DT10_UV_FUTURE_SKEW_MIN")
    setup(mp, tmp_path)
    _rules, policy = final.final_policy()
    assert policy["decision_max_age_h"] * 60 == R.RELEASE_TTL_MIN


def case_n16_workflow_contract_and_env_parity(mp, tmp_path):
    text = RELEASE_YML.read_text(encoding="utf-8")
    assert all(ord(ch) < 128 for ch in text)
    rel, daily = workflow(RELEASE_YML), workflow(DAILY_YML)
    assert list(rel["on"]) == ["workflow_dispatch"]
    inputs = rel["on"]["workflow_dispatch"]["inputs"]
    assert inputs["source_run_id"]["required"] is True and "default" not in inputs["source_run_id"]
    assert inputs["mode"]["options"] == ["release", "sweep", "kill"]
    assert rel["concurrency"] == {"group": "tfb-decision-release", "cancel-in-progress": False}
    assert "production-write" not in json.dumps(rel["concurrency"])
    assert rel["permissions"] == {"contents": "read", "actions": "read"}
    job = rel["jobs"]["release-decision-feed"]
    assert list(rel["jobs"]) == ["release-decision-feed"] and "concurrency" not in job
    assert job["runs-on"] == "ubuntu-24.04" and job["environment"] == "production"
    final_job = daily["jobs"]["verify-final-decision-publication"]
    gates = {"TFB_DECISION_RELEASE": "${{ vars.TFB_DECISION_RELEASE || '0' }}",
             "TFB_DECISION_RELEASE_KILL": "${{ vars.TFB_DECISION_RELEASE_KILL || '0' }}"}
    expected = {**final_job["env"], **gates,
                "TARGET_SHEET_ID": "${{ vars.DEFAULT_SPREADSHEET_ID }}"}
    assert final_job["env"]["TARGET_SHEET_ID"] == \
        "${{ github.event.inputs.sheet_id || vars.DEFAULT_SPREADSHEET_ID }}"
    assert job["env"] == expected
    assert {k for k in final_job["env"] if k.startswith(("TFB_DECISION_", "TFB_EXPECTED_MIN_ROWS_"))} >= {
        "TFB_DECISION_SOURCE_MAX_AGE_H", "TFB_DECISION_SURFACE_MAX_AGE_H",
        "TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS", "TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS",
        "TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX", "TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS"}
    daily_text = DAILY_YML.read_text(encoding="utf-8")
    for line in daily_text.splitlines():
        if re.match(r"^      TFB_(DECISION|EXPECTED_MIN_ROWS)_", line) and line in daily_text.split(
                "  verify-final-decision-publication:")[1].split("    steps:")[0]:
            assert line in text, line
    steps = {step["name"]: step for step in job["steps"]}
    for step in job["steps"]:
        assert "${{" not in step.get("run", ""), step["name"]   # inputs reach the shell via env only
    run = steps["Release or retract the decision feed"]
    assert run["env"]["SOURCE_RUN_ID"] == "${{ inputs.source_run_id }}"
    assert run["env"]["GITHUB_TOKEN"] == "${{ github.token }}"
    assert '--source-run-id "$SOURCE_RUN_ID"' in run["run"] and "GITHUB_RUN_ID" not in run["run"]
    assert "github.run_id" not in json.dumps(job["env"]) and "github.run_id" not in json.dumps(run["env"])
    checkout = steps["Checkout this workflow's ref"]["with"]
    assert checkout == {"ref": "${{ github.sha }}", "fetch-depth": 1, "persist-credentials": False}
    assert 'refs/heads/main' in steps["Validate dispatch inputs"]["run"]
    daily_steps = {step["name"]: step for step in final_job["steps"]}
    assert steps["Configure release-audit credentials"]["run"] == \
        daily_steps["Configure final-audit credentials"]["run"]
    assert steps["Configure release-audit credentials"]["env"] == \
        daily_steps["Configure final-audit credentials"]["env"]
    assert steps["Secure credential cleanup"]["run"] == daily_steps["Secure credential cleanup"]["run"]
    assert steps["Upload release evidence"]["with"]["name"] == "release-decision-feed-${{ github.run_id }}"
    assert steps["Upload release evidence"]["uses"] == daily_steps["Upload final publication evidence"]["uses"]


def case_n17_gate_off_release_mode_never_writes(mp, tmp_path):
    expired = live_release_value(anchor=NOW.astimezone(RIYADH) - timedelta(minutes=600))
    for coverage, feed in (("clean", W0_VETO), ("failed", W0_VETO), ("failed", live_release_value()),
                           ("clean", expired), ("clean", "executable | run=90001 | 2026-07-31 20:00:00+03:00")):
        ctx = setup(mp, tmp_path / str(abs(hash((coverage, feed)))), coverage=coverage, feed=feed)
        before = copy.deepcopy(ctx.service.rows)
        result = ctx.release()
        assert ctx.updates() == [] and ctx.service.rows == before, (coverage, feed)
        if (coverage, feed) == ("failed", live_release_value()):
            # Disarming never retracts a live release: the retraction is only reported.
            assert result["exit_code"] == 2 and result["downgrade"] is None
            assert result["would_downgrade"] == (
                "NOT_ACTIONABLE(final_publication) | run=90001 | 2026-07-31 21:00:00+03:00 | "
                "coverage_failed | release=777001")
        elif feed == W0_VETO:
            assert result["would_downgrade"] is None, (coverage, feed)
    ctx = setup(mp, tmp_path / "sweep", feed=expired)        # sweep still allowed with the gate off
    result = ctx.release(mode="sweep")
    assert result["outcome"] == "swept" and len(ctx.updates()) == 1


def case_n18_expiry_anchor_is_oldest_input(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    ctx.clock["now"] = ANCHOR + timedelta(minutes=479)      # inputs 7h59m old
    result = ctx.release()
    assert result["outcome"] == "released", result["reasons"]
    value = ctx.service.rows[3][12]
    expires = datetime.strptime(R.RELEASE_VALUE_RE.fullmatch(value).group("expires"), "%Y-%m-%dT%H:%M:%S%z")
    assert expires - ctx.clock["now"] == timedelta(minutes=1)
    assert R.parse_feed(value, ctx.clock["now"] + timedelta(minutes=1))["state"] == "EXECUTABLE"
    assert R.parse_feed(value, ctx.clock["now"] + timedelta(minutes=2))["state"] == "NOT_ACTIONABLE"
    ctx = setup(mp, tmp_path / "spent")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    ctx.clock["now"] = ANCHOR + timedelta(minutes=480)      # the audit still passes (8h is not > 8h)
    result = ctx.release()
    assert result["audit_exit_codes"] == [0, 0] and result["exit_code"] == 3
    assert result["reasons"] == ["anchor_expired"] and ctx.updates() == []


def case_n19_inflight_probe_filters_ci_and_counts_waiting(mp, tmp_path):
    listings = {
        ("daily_sync.yml", "schedule"): [{"id": 1, "event": "schedule", "status": "completed"},
                                         {"id": 9, "event": "push", "status": "in_progress"}],
        ("daily_sync.yml", "workflow_dispatch"): [{"id": 2, "event": "workflow_dispatch", "status": "waiting"}],
        ("page_refresh_recovery.yml", "schedule"): [{"id": 3, "event": "pull_request", "status": "queued"}],
        ("page_refresh_recovery.yml", "workflow_dispatch"): [{"id": 4, "event": "workflow_dispatch", "status": "pending"}],
        ("grid_capacity_repair.yml", "workflow_dispatch"): [{"id": 5, "event": "workflow_dispatch", "status": "requested"}],
    }
    paths = []

    def http_get(path):
        paths.append(path)
        match = re.fullmatch(r"/repos/\{repo\}/actions/workflows/([a-z_]+\.yml)/runs\?event=([a-z_]+)&per_page=50", path)
        assert match, path
        return {"workflow_runs": copy.deepcopy(listings.get(match.groups(), []))}

    busy = R.github_inflight(http_get=http_get)
    assert sorted((b["id"], b["status"]) for b in busy) == [(2, "waiting"), (4, "pending"), (5, "requested")]
    assert len(paths) == 6
    ci_only = {key: [dict(item, event="push") for item in runs] for key, runs in listings.items()}
    assert R.github_inflight(http_get=lambda p: {"workflow_runs": ci_only.get(tuple(
        re.fullmatch(r".*/workflows/(.+)/runs\?event=(.+)&per_page=50", p).groups()), [])}) == []
    for bad in ({}, {"workflow_runs": None}, {"workflow_runs": ["x"]}, []):
        with pytest.raises(ValueError):
            R.github_inflight(http_get=lambda p, bad=bad: bad)


def case_n20_provenance_rules(mp, tmp_path):
    assert R.check_provenance(SOURCE, provenance_ok(SOURCE))["ok"]
    good = R.check_provenance(SOURCE, provenance_ok(SOURCE, run={"event": "workflow_dispatch",
                                                               "path": ".github/workflows/daily_sync.yml@refs/heads/main"}))
    assert good["ok"] and good["run_attempt"] == 1 and good["final_job_conclusion"] == "failure"
    for changes, reason in (({"run": {"id": 90002}}, "run_id_mismatch"),
                            ({"run": {"run_attempt": 0}}, "run_attempt_invalid"),
                            ({"run": {"event": "pull_request"}}, "event_not_production"),
                            ({"job": {"status": "in_progress", "conclusion": None}}, "final_job_not_executed"),
                            ({"jobs": []}, "final_job_not_executed")):
        verdict = R.check_provenance(SOURCE, provenance_ok(SOURCE, **changes))
        assert not verdict["ok"] and reason in verdict["reasons"], reason
    assert R.check_provenance(SOURCE, {"run": None, "jobs": []})["reasons"] == ["provenance_malformed"]
    for name in ("GITHUB_TOKEN", "GITHUB_REPOSITORY"):
        mp.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="unavailable"):
        R.github_provenance(SOURCE)          # no token: fail closed before any request


def case_n21_cockpit_parser_parity_on_real_gas(mp, tmp_path):
    value = live_release_value()
    battery = [(value, ANCHOR + timedelta(minutes=m)) for m in (-16, -15, 0, 1, 240, 480, 481, 600)]
    battery += [(value, ANCHOR + timedelta(minutes=480, seconds=1))]
    battery += [(v, ANCHOR + timedelta(minutes=30)) for v in (
        value.lower(), "EXECUTABLE(release) " + value[10:], value.replace("2026-07-31 20:00:00+03:00", "2026-07-31T20:00:00+03:00"),
        "NOT_ACTIONABLE(final_publication) | run=90001 | 2026-07-31 20:10:00+03:00 | x",
        "EXECUTABLE | run=1 | 2026-07-31 20:00:00", "EXECUTABLE | run=1 | 2026-07-31 17:00:00",
        "", "EXECUTABLE | no stamp", "stale")]
    # The T-form expires= is never read as the stamp: far-future expiry, stale anchor.
    far = value.replace("expires=2026-08-01T04:00:00+03:00", "expires=2030-01-01T00:00:00+03:00")
    battery += [(far, ANCHOR + timedelta(minutes=481))]
    native = node_parse(battery)
    for (raw, now), gas in zip(battery, native):
        mine = R.parse_feed(raw, now)
        assert mine["state"] == gas["state"] and mine["reason"] == gas["reason"], (raw, now, gas, mine)
        assert (mine["age_min"] is None) == (gas["ageMin"] is None)
        if gas["ageMin"] is not None:
            assert abs(mine["age_min"] - gas["ageMin"]) < 1e-6
    states = [gas["state"] for gas in native]
    assert states[:9] == ["NOT_ACTIONABLE", "EXECUTABLE", "EXECUTABLE", "EXECUTABLE", "EXECUTABLE",
                          "EXECUTABLE", "NOT_ACTIONABLE", "NOT_ACTIONABLE", "NOT_ACTIONABLE"]
    assert states[9:13] == ["NOT_ACTIONABLE"] * 4                  # lower, parenthesised, T-only, veto
    assert native[11]["reason"] == "verdict timestamp unreadable"   # a T-form is never the stamp
    assert native[-1]["state"] == "NOT_ACTIONABLE" and "stale (481m" in native[-1]["reason"]


def case_n22_release_source_never_from_github_run_id(mp, tmp_path):
    tree = ast.parse((ROOT / "scripts" / "release_decision_feed.py").read_text(encoding="utf-8"))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and node.func.attr == "run"
             and isinstance(node.func.value, ast.Name) and node.func.value.id == "fin"]
    assert len(calls) == 1
    kw = {k.arg: k.value for k in calls[0].keywords}
    assert isinstance(kw["run_id"], ast.Name) and kw["run_id"].id == "source"
    assert isinstance(kw["publish"], ast.Constant) and kw["publish"].value is False
    assert isinstance(kw["max_rows"], ast.Constant) and kw["max_rows"].value == 20000
    assert "service" not in kw
    for path in (ROOT / "scripts" / "release_decision_feed.py", Path(__file__).resolve()):
        assert all(ord(ch) < 128 for ch in path.read_text(encoding="utf-8")), path
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    env_reads = [ast.unparse(node) for node in ast.walk(main) if isinstance(node, (ast.Call, ast.Subscript))
                 and "environ" in ast.unparse(node) or isinstance(node, ast.Call) and "getenv" in ast.unparse(node.func)]
    assert env_reads and not [read for read in env_reads if "GITHUB_RUN_ID" in read], env_reads


def case_n23_cli_contract(mp, tmp_path):
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    out = tmp_path / "release.json"
    assert R.main(["--sheet-id", "synthetic-sheet", "--json-out", str(out)]) == 3   # no source: 3, not argparse 2
    assert not out.exists()
    assert R.main(["--sheet-id", "synthetic-sheet", "--source-run-id", "local", "--json-out", str(out)]) == 3
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["reasons"] == ["source_run_invalid"] and payload["released"] is False
    assert R.main(["--sheet-id", "synthetic-sheet", "--source-run-id", SOURCE, "--mode", "promote",
                   "--json-out", str(out)]) == 3
    assert json.loads(out.read_text(encoding="utf-8"))["reasons"] == ["mode_invalid"]
    assert ctx.service.calls == []


def case_n24_verifier_registration(mp, tmp_path):
    from scripts import verify_deployment as V
    # Lower bound only: the verifier is re-pinned often, and a later routine
    # bump must not turn this harness red (tests/test_verify_manifest_pins.py
    # owns pin freshness). The registration asserts below are what P0-1 needs.
    assert tuple(int(part) for part in V.SCRIPT_VERSION.split(".")) >= (1, 0, 56), V.SCRIPT_VERSION
    assert ("release_decision_feed", "1.0.0") in {(n, v) for n, v, _ in V.SCRIPTS}
    flags = {name: (default, kill) for name, default, _meaning, kill in V.FLAGS}
    assert flags["TFB_DECISION_RELEASE"] == ("0", False)
    assert flags["TFB_DECISION_RELEASE_KILL"] == ("0", False)
    assert {"TFB_DECISION_RELEASE", "TFB_DECISION_RELEASE_KILL"} <= V._WORKFLOW_SCOPED
    for name in ("TFB_DECISION_RELEASE", "TFB_DECISION_RELEASE_KILL"):
        mp.delenv(name, raising=False)
    rows = {row["flag"]: row for row in V.check_flags()}
    for name in ("TFB_DECISION_RELEASE", "TFB_DECISION_RELEASE_KILL"):
        assert rows[name]["scope"] == "workflow" and not rows[name]["armed"] and not rows[name]["kill_switch"]
    mp.setenv("TFB_DECISION_RELEASE", "on")
    assert R.release_enabled() and {r["flag"]: r for r in V.check_flags()}["TFB_DECISION_RELEASE"]["armed"]
    for word in ("0", "off", "false", "enforce", ""):
        mp.setenv("TFB_DECISION_RELEASE", word)
        assert not R.release_enabled()


def case_n25_prewrite_read_fault_zero_writes_window_kept(mp, tmp_path):
    """A transport/layout fault on the compare-and-set re-read happens BEFORE any
    update: refuse with zero writes and keep the run's release window."""
    ctx = setup(mp, tmp_path)
    mp.setenv("TFB_DECISION_RELEASE", "1")

    class FlakyRead(GoogleValues):
        def __init__(self, fault, **kwargs):
            super().__init__(**kwargs)
            self.fault, self.key_reads = fault, 0

        def get(self, **kwargs):
            if kwargs["range"] == final.KEY_RANGE:
                self.key_reads += 1
                if self.key_reads == 2 and self.fault == "transport":
                    raise ConnectionError("synthetic transient 503 on the compare-and-set read")
                if self.key_reads == 2 and self.fault == "layout":
                    self.calls.append(("get", copy.deepcopy(kwargs)))
                    return SimpleNamespace(execute=lambda: {"values": None})
            return super().get(**kwargs)

    for fault in ("transport", "layout"):
        service = FlakyRead(fault)
        service.rows[3][11:13] = [final.KEY, W0_VETO]
        result = ctx.release(service=service)
        assert result["exit_code"] == 3 and result["outcome"] == "refused", (fault, result["reasons"])
        assert result["reasons"] == ["compare_and_set_read_failed"], (fault, result["reasons"])
        assert "before any write" in result["publication_error"]
        assert ctx.updates(service) == [] and not result["blocker_published"], fault
        assert service.rows[3][12] == W0_VETO and not result["released"]
        # The window is NOT consumed: once the fault clears, the same run releases.
        service.fault = None
        again = ctx.release(service=service)
        assert again["outcome"] == "released" and again["prior_class"] == "eligible", again["reasons"]
        assert service.rows[3][12] == W2_VALUE
    # Contrast: a fault once the update was attempted still leaves the
    # best-effort release_unconfirmed downgrade (the write may have landed).
    service = GoogleValues(write_error=True)
    service.rows[3][11:13] = [final.KEY, W0_VETO]
    result = ctx.release(service=service)
    assert result["outcome"] == "release_unconfirmed"
    assert [u["body"]["values"][0][1] for u in ctx.updates(service)][0] == W2_VALUE


# =========================================================================== GOLDEN
def _require_base():
    if not FINALIZE_BASE or not os.path.exists(FINALIZE_BASE):
        pytest.skip("TFB_TEST_FINALIZE_BASE unset")
    if not SYNC_BASE or not os.path.exists(SYNC_BASE):
        pytest.skip("TFB_TEST_SYNC_BASE unset")


def case_g1_base_finalize_cannot_release(mp, tmp_path):
    _require_base()
    base = finalize_module(FINALIZE_BASE)
    ctx = setup(mp, tmp_path)
    mp.setattr(base, "load_scope", lambda: ctx.scope)
    mp.setenv("TFB_DECISION_RELEASE", "1")
    before = copy.deepcopy(ctx.service.rows)
    result = asyncio.run(base.run("synthetic-sheet", service=ctx.service, publish=True,
                                  max_rows=20000, run_id=SOURCE))
    assert result["exit_code"] == 0 and result["blockers"] == [] and result["feed_promoted"] is False
    assert ctx.service.calls == [] and ctx.service.rows == before
    # The W2 gate-on expectation fails on base: the cell is still the veto.
    assert not R.reads_executable(ctx.service.rows[3][12]) and ctx.service.rows[3][12] == W0_VETO
    assert not hasattr(base, "publish_release") and "EXECUTABLE |" not in Path(FINALIZE_BASE).read_text()


def case_g2_base_sync_never_lifts_the_veto(mp, tmp_path):
    _require_base()
    base = sync_module(SYNC_BASE)
    ctx = setup(mp, tmp_path)
    run_sync_verdict(base, mp, ctx.service, fresh_pages=base._upstream_verdict_pages())
    assert ctx.service.rows[3][12] == W0_VETO
    diag = [row[12] for row in ctx.service.rows[:60] if row[11] == "TFB Upstream Feed Diagnostic"]
    assert len(diag) == 1 and diag[0].startswith("UPSTREAM_READY")


def case_g3_release_with_base_finalize_is_identical(mp, tmp_path):
    _require_base()
    base = finalize_module(FINALIZE_BASE)
    ctx = setup(mp, tmp_path)
    mp.setattr(base, "load_scope", lambda: ctx.scope)
    current = ctx.release()
    swapped = ctx.release(finalize=base)
    assert json.dumps(current, sort_keys=True, default=str) == json.dumps(swapped, sort_keys=True, default=str)
    assert current["accepted"] is swapped["accepted"] is True and ctx.updates() == []


def case_g4_protected_sources_byte_identical(mp, tmp_path):
    _require_base()
    pairs = [(ROOT / "scripts" / "finalize_decision_publication.py", Path(FINALIZE_BASE)),
             (ROOT / "scripts" / "run_dashboard_sync.py", Path(SYNC_BASE))]
    base_dir = Path(FINALIZE_BASE).parent
    for rel in ("scripts/audit_decision_surface_freshness.py", "scripts/audit_full_refresh_coverage.py",
                "apps_script/16_Decision_Top10.gs", "apps_script/11_Manual_Refresh_Coordinator.gs",
                "apps_script/24_Sync_Dispatch.gs"):
        if (base_dir / Path(rel).name).exists():
            pairs.append((ROOT / rel, base_dir / Path(rel).name))
    for current, base in pairs:
        digest = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (current, base)]
        assert digest[0] == digest[1], current
    print("G4 byte-identical: %d protected files" % len(pairs))


def case_g5_verifier_ast_inventory_zero_removed(mp, tmp_path):
    _require_base()
    base = Path(FINALIZE_BASE).parent / "verify_deployment.py"
    if not base.exists():
        pytest.skip("base verify_deployment.py copy not supplied")

    def inventory(path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        return {node.name for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    before, after = inventory(base), inventory(ROOT / "scripts" / "verify_deployment.py")
    assert before - after == set() and after - before == set()
    # DISCRIMINATING: the N24 registration holds on this tree and FAILS on base
    # (read by AST literal, no import). A HEAD copy in the base dir fails here.
    names = {"TFB_DECISION_RELEASE", "TFB_DECISION_RELEASE_KILL"}
    here, then = registration(ROOT / "scripts" / "verify_deployment.py"), registration(base)
    assert ("release_decision_feed", "1.0.0") in here["scripts"] and names <= here["flags"]
    assert names <= here["workflow_scoped"]
    assert "release_decision_feed" not in {name for name, _ in then["scripts"]}, \
        "base verifier already pins release_decision_feed: TFB_TEST_FINALIZE_BASE is not a base copy"
    assert not names & then["flags"] and not names & then["workflow_scoped"]


def registration(path):
    """SCRIPTS (name, pin), FLAGS names and _WORKFLOW_SCOPED, by AST literal."""
    found = {}
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        targets = node.targets if isinstance(node, ast.Assign) else (
            [node.target] if isinstance(node, ast.AnnAssign) else [])
        for target in targets:
            if isinstance(target, ast.Name) and target.id in ("SCRIPTS", "FLAGS", "_WORKFLOW_SCOPED"):
                found[target.id] = ast.literal_eval(node.value)
    return {"scripts": {(row[0], row[1]) for row in found["SCRIPTS"]},
            "flags": {row[0] for row in found["FLAGS"]}, "workflow_scoped": set(found["_WORKFLOW_SCOPED"])}


def case_g6_armed_walk_fails_on_base_tree(mp, tmp_path):
    """The DISCRIMINATING gate-on golden negative: W2 (gate armed) run against
    the base tree. scripts/release_decision_feed.py does not exist at dda1211,
    so `git show dda1211:scripts/release_decision_feed.py > <base>/release_decision_feed.py`
    leaves an EMPTY file: the base tree has no release step, and the armed walk
    reduces to the base final job plus the base sync legs, which never lift the
    veto. A HEAD copy in its place releases the feed and FAILS this case."""
    _require_base()
    base_copy = Path(FINALIZE_BASE).parent / "release_decision_feed.py"
    if not base_copy.exists():
        pytest.skip("base release_decision_feed.py copy not supplied (git show dda1211:"
                    "scripts/release_decision_feed.py > <base dir>/release_decision_feed.py)")
    # Positive control: the identical armed walk on THIS tree yields the W2 cell.
    ctx = setup(mp, tmp_path / "head")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    assert ctx.release()["outcome"] == "released" and ctx.service.rows[3][12] == W2_VALUE
    # The same walk on the base tree.
    ctx = setup(mp, tmp_path / "base")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    base_run = None
    if base_copy.read_text(encoding="utf-8").strip():
        base_run = getattr(_load_base(base_copy, "tfb_release_base_release"), "run", None)
    if base_run is not None:
        asyncio.run(base_run("synthetic-sheet", SOURCE, mode="release", service=ctx.service,
                             now_utc=ctx.clock["now"], inflight=ctx.inflight, provenance=ctx.provenance))
    else:
        base = finalize_module(FINALIZE_BASE)
        mp.setattr(base, "load_scope", lambda: ctx.scope)
        result = asyncio.run(base.run("synthetic-sheet", service=ctx.service, publish=True,
                                      max_rows=20000, run_id=SOURCE))
        assert result["exit_code"] == 0 and result["feed_promoted"] is False
        sync = sync_module(SYNC_BASE)
        run_sync_verdict(sync, mp, ctx.service, fresh_pages=sync._upstream_verdict_pages())
    cell = ctx.service.rows[3][12]
    # The W2 expectation (an EXECUTABLE cell) FAILS on base.
    assert cell != W2_VALUE and not R.reads_executable(cell), \
        "the base tree released the feed: the base dir holds a HEAD copy, not dda1211"
    assert cell == W0_VETO
    assert not [u for u in ctx.updates() if u["body"]["values"][0][0] == final.KEY]


CASES = [value for _name, value in sorted(
    ((name, value) for name, value in globals().items() if name.startswith("case_") and callable(value)),
    key=lambda kv: ({"w": 0, "n": 1, "g": 2}[kv[0][5]],
                    int(re.sub(r"\D", "", kv[0].split("_")[1]) or 0), kv[0]))]


@pytest.mark.parametrize("case", CASES, ids=[case.__name__[5:] for case in CASES])
def test_release_decision_feed(case, monkeypatch, tmp_path):
    case(monkeypatch, tmp_path)


def _main() -> int:
    passed = failed = skipped = 0
    for case in CASES:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            try:
                case(mp, Path(tmp))
            except pytest.skip.Exception as exc:
                skipped += 1
                print("SKIP " + case.__name__ + " :: " + str(exc))
                continue
            except Exception:  # noqa: BLE001
                failed += 1
                print("FAIL " + case.__name__)
                traceback.print_exc()
                continue
        passed += 1
        print("ok   " + case.__name__)
    total = passed + failed
    print("PASS %d/%d%s" % (passed, total, ("  (skipped %d)" % skipped) if skipped else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
