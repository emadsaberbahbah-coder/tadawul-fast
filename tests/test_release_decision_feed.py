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

VERSION 1.1.0 (2026-10-13) -- Codex P1 findings on PR #763.
WHY: (1) a release of run N stayed EXECUTABLE while a later production writer
re-stamped the pages; scripts/release_decision_feed.py v1.1.0 adds a gated
writer-start 'supersede' mode wired into every page-writing job. (2) the
age-blind consumers (send_digest, run_daily_brief, tfb_acceptance) honoured an
expired release; each now enforces the release's expires= token.
NEW   W6 the superseding run is releasable after its own final job passed;
      N26-N36 supersede no-op / downgrade / identity / faults / eligibility /
      CLI / workflow structure and gate semantics / consumer expiry, lockstep
      and inertness. N24 and G5 now read the pin from R.SCRIPT_VERSION; W2
      reads the consumers at the release instant (its 2026-07-31 window is
      long past in real time).
GOLD  G7-G10 against PRE-CHANGE copies (TFB_TEST_SUPERSEDE_BASE_DIR: the
      0825561 copies of release_decision_feed.py, send_digest.py,
      run_daily_brief.py, tfb_acceptance.py, daily_sync.yml,
      page_refresh_recovery.yml, grid_capacity_repair.yml). Each runs a NEW
      case's expectation on the base copy and requires it to FAIL, and G8/G9
      also prove the off-path identity (yaml minus the added steps, and the
      consumers on today's value shapes) against the same copies. SKIP when
      unset; all four FAIL when the directory holds HEAD copies.

VERSION 1.1.1 (2026-10-13) -- DT10 v1.13.8 (arming prerequisite 2) stacked.
WHY: apps_script/16_Decision_Top10.gs now changes on purpose (Script Property
DT10_FEED_COHORT_BIND, default off), so the G4 byte-identity pair for that
file would turn red whenever its dda1211 sibling copy is supplied. G4 keeps
byte identity for every other protected file and accepts a different .gs
ONLY when tests/js/dt10_feed_cohort_binding.test.js, run against that same
base copy (DT10_BASE), passes with nothing skipped: default-off verdict,
render and refresh identity (D1-D3), zero removals and exactly the designed
additions (D4) and its golden negative (G1). Nothing else changed.

VERSION 1.1.2 (2026-10-13) -- review fixes on the DT10 v1.13.8 stack.
WHY: (1) G4 ran the .gs harness inside its loop, so when node was missing
pytest.skip ended the case before 11_Manual_Refresh_Coordinator.gs and
24_Sync_Dispatch.gs were compared: drift in those two went unreported. G4 now
compares every other pair first and runs the .gs proof after the loop, so a
missing node skips only that proof. (2) the harness's census check (D4) did
not confine ADDED code to the designed regions, so an unrelated behavioural
line elsewhere in the .gs passed. G4 now also requires the harness's new D5
(the delivered file minus the designed v1.13.8 regions equals the base copy
byte for byte) and its message says only what D5 checks.

VERSION 1.1.3 (2026-10-18) -- source-target receipt before writer-start veto.
N34 permits exactly the validated local receipt command between primary
credentials and supersede. The veto remains before every page write, and the
other writer lanes still supersede immediately after credentials. N34b rejects
changed/conditional receipts, extra commands and reordered intervening steps.

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
# v1.1.0: pre-change (0825561) copies for the supersede / expiry golden negatives.
PREV_DIR = os.environ.get("TFB_TEST_SUPERSEDE_BASE_DIR", "")
RECOVERY_YML = ROOT / ".github" / "workflows" / "page_refresh_recovery.yml"
GRID_YML = ROOT / ".github" / "workflows" / "grid_capacity_repair.yml"
SUPERSEDE_STEP = "Supersede released decision feed (P0-1)"
TARGET_RECEIPT_STEP = "Record the resolved source target before writing"
TARGET_RECEIPT_COMMAND = ('python scripts/sync_target_receipt.py write '
                          '--group "$SYNC_GROUP" --output sync_target.json')
CURRENT_RUN = "90002"
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


# --------------------------------------------------------------------------- v1.1.0 helpers
def pin_expiry_clock(mp, module, now):
    """Read a consumer's release expiry at a fixed instant (integration path kept)."""
    original = getattr(module._feed_release_expiry, "tfb_original", module._feed_release_expiry)

    def pinned(raw, _now=None):
        return original(raw, now)
    pinned.tfb_original = original
    mp.setattr(module, "_feed_release_expiry", pinned)


def supersede(ctx, *, current=CURRENT_RUN, attempt="1", service=None, module=None):
    return (module or R).supersede("synthetic-sheet", current, attempt=attempt,
                                   service=service if service is not None else ctx.service,
                                   now_utc=ctx.clock["now"])


def supersede_value(run, by=CURRENT_RUN, attempt="1", stamp="2026-07-31 21:10:00+03:00"):
    return ("NOT_ACTIONABLE(final_publication) | run=%s | %s | superseded_by_run=%s.%s"
            % (run, stamp, by, attempt))


class _CI(str):
    """A GitHub expression string: == / != compare case-insensitively."""
    def __eq__(self, other):
        return str(self).casefold() == str(other).casefold()

    def __ne__(self, other):
        return not self.__eq__(other)

    __hash__ = str.__hash__


def evaluate_step_if(expression, *, variable, event, run_mode="full_sync"):
    """Evaluate the small Actions subset the supersede step uses (contains,
    fromJSON, &&, ||, ==, !=) with GitHub's case-insensitive string semantics."""
    assert expression.startswith("${{") and expression.endswith("}}"), expression
    source = expression[3:-2].strip().replace("&&", " and ").replace("||", " or ")
    tree = ast.parse(source, mode="eval")
    allowed = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare, ast.Eq, ast.NotEq,
               ast.Constant, ast.Name, ast.Load, ast.Attribute, ast.Call)
    for node in ast.walk(tree):
        assert isinstance(node, allowed), type(node).__name__
        if isinstance(node, ast.Call):
            assert isinstance(node.func, ast.Name) and node.func.id in {"contains", "fromJSON"}
    scope = {
        "vars": SimpleNamespace(TFB_DECISION_RELEASE=_CI(variable)),
        "github": SimpleNamespace(event_name=_CI(event)),
        "env": SimpleNamespace(RUN_MODE=_CI(run_mode)),
        "fromJSON": lambda text: [_CI(item) for item in json.loads(text)],
        "contains": lambda items, item: any(_CI(x) == item for x in items),
    }
    return bool(eval(compile(tree, "<step if>", "eval"), {"__builtins__": {}}, scope))


# (file, job) -> (first page-write command, production events, health_only exempt)
PAGE_WRITERS = {
    ("daily_sync.yml", "sync-dashboard"): ("python scripts/run_dashboard_sync.py",
                                           ("schedule", "workflow_dispatch"), True),
    ("daily_sync.yml", "recover-missing-market-pages"): ("python scripts/run_inline_page_recovery.py",
                                                         ("schedule", "workflow_dispatch"), False),
    ("page_refresh_recovery.yml", "recover-page"): ("python scripts/run_dashboard_sync.py",
                                                    ("workflow_dispatch",), False),
}
WRITER_COMMANDS = ("python scripts/run_dashboard_sync.py", "python scripts/run_inline_page_recovery.py")
EXPECTED_STEP_IF = {
    True: ("${{ contains(fromJSON('[\"1\",\"true\",\"yes\",\"on\"]'), vars.TFB_DECISION_RELEASE)"
           " && (github.event_name == 'schedule' || github.event_name == 'workflow_dispatch')"
           " && env.RUN_MODE != 'health_only' }}"),
    False: ("${{ contains(fromJSON('[\"1\",\"true\",\"yes\",\"on\"]'), vars.TFB_DECISION_RELEASE)"
            " && (github.event_name == 'schedule' || github.event_name == 'workflow_dispatch') }}"),
    "recovery": ("${{ contains(fromJSON('[\"1\",\"true\",\"yes\",\"on\"]'), vars.TFB_DECISION_RELEASE)"
                 " && github.event_name == 'workflow_dispatch' }}"),
}


def _load_yaml(path):
    return __import__("yaml").safe_load(Path(path).read_text(encoding="utf-8"))


def page_writer_jobs(directory):
    """Every (file, job) whose steps invoke a market-page writer."""
    found = set()
    for path in sorted(Path(directory).glob("*.yml")):
        for name, job in (_load_yaml(path).get("jobs") or {}).items():
            runs = [str(step.get("run", "")) for step in (job.get("steps") or []) if isinstance(step, dict)]
            if any(command in run for run in runs for command in WRITER_COMMANDS):
                found.add((path.name, name))
    return found


def supersede_workflow_expectation(directory):
    """NEW-CASE EXPECTATION (N35; G8 requires it to FAIL on the base copies)."""
    directory = Path(directory)
    assert page_writer_jobs(directory) == set(PAGE_WRITERS), page_writer_jobs(directory)
    for (file_name, job_name), (command, events, has_mode) in PAGE_WRITERS.items():
        job = _load_yaml(directory / file_name)["jobs"][job_name]
        steps = job["steps"]
        names = [step.get("name") for step in steps]
        assert names.count(SUPERSEDE_STEP) == 1, (file_name, job_name, "supersede step missing")
        sup = names.index(SUPERSEDE_STEP)
        creds = [i for i, step in enumerate(steps)
                 if "GOOGLE_APPLICATION_CREDENTIALS=" in str(step.get("run", ""))
                 and "GITHUB_ENV" in str(step.get("run", ""))]
        writes = [i for i, step in enumerate(steps) if command in str(step.get("run", ""))]
        assert len(creds) == 1 and writes, (file_name, job_name)
        next_step = creds[0] + 1
        if (file_name, job_name) == ("daily_sync.yml", "sync-dashboard"):
            # The sole permitted pre-veto operation validates and records the
            # already resolved target locally. Pin the entire step contract so
            # another command, action, condition or ignored failure cannot slip
            # into this boundary under the receipt's name.
            receipt = steps[next_step]
            assert set(receipt) == {"name", "env", "run"}, (file_name, job_name, receipt)
            assert receipt["name"] == TARGET_RECEIPT_STEP, (file_name, job_name, receipt)
            assert receipt["env"] == {"SYNC_GROUP": "${{ matrix.group }}"}, receipt["env"]
            command_line = " ".join(receipt["run"].replace("\\\n", "").split())
            assert command_line == TARGET_RECEIPT_COMMAND, command_line
            next_step += 1
        assert sup == next_step and sup < writes[0], (file_name, job_name, creds, sup, writes)
        step = steps[sup]
        key = "recovery" if file_name == "page_refresh_recovery.yml" else has_mode
        assert step["if"] == EXPECTED_STEP_IF[key], (file_name, job_name, step["if"])
        assert set(step) == {"name", "if", "timeout-minutes", "run"}, set(step)
        assert step["timeout-minutes"] == 5
        run = step["run"]
        assert "${{" not in run and "set -euo pipefail" in run
        assert "python scripts/release_decision_feed.py --mode supersede" in run
        assert '--current-run "$GITHUB_RUN_ID"' in run and '--sheet-id "$TARGET_SHEET_ID"' in run
        assert '--json-out "$RUNNER_TEMP/' in run
        write = steps[writes[0]]
        assert not write.get("continue-on-error"), (file_name, job_name)
        assert not re.search(r"always\(\)|failure\(\)|cancelled\(\)", str(write.get("if", ""))), write.get("if")
        assert job["runs-on"] == "ubuntu-24.04"
        # Gate semantics: runs exactly when the release script reads the gate
        # as armed (no whitespace), on a production event, and not health_only.
        for variable in ("", "0", "1", "true", "TRUE", "True", "yes", "YES", "on", "On", "off",
                         "false", "no", "2", "enforce", " 1", "1 ", "on "):
            for event in ("schedule", "workflow_dispatch", "push", "pull_request", "workflow_run"):
                for mode in ("full_sync", "health_only", "single_key"):
                    expected = (variable.lower() in R._ARMED and event in events
                                and not (has_mode and mode == "health_only"))
                    assert evaluate_step_if(step["if"], variable=variable, event=event,
                                            run_mode=mode) is expected, (job_name, variable, event, mode)


def strip_supersede_steps(document):
    removed = 0
    for job in (document.get("jobs") or {}).values():
        steps = job.get("steps") or []
        kept = [step for step in steps if step.get("name") != SUPERSEDE_STEP]
        removed += len(steps) - len(kept)
        job["steps"] = kept
    return document, removed


def status_rows(value):
    """A minimal '_Status' grid carrying the feed in L:M (tfb_acceptance layout)."""
    return [["Page", "Last Updated", "Status", "Message", "", "", "", "", "", "", "", "Global Key", "Value"],
            ["", "", "", "", "", "", "", "", "", "", "", "TFB Decision Feed", value]]


def _d10(acc, value):
    return {c.cid: c for c in acc.check_feed_truth(acc._MemSource({"_Status": status_rows(value)}))}["D10-4c"]


def malformed_expiry_values(live):
    expires = R.RELEASE_VALUE_RE.fullmatch(live).group("expires")
    return [live.replace("expires=" + expires, "expires=x" + expires),
            live.replace("expires=" + expires, "expires=" + expires.replace("T", " ")),
            live.replace("expires=" + expires, "expires=" + expires[:-6] + "+04:00"),
            live + " | expires=2030-01-01T00:00:00+03:00",
            "EXECUTABLE | run=1 | 2026-07-31 20:00:00+03:00 | expires=",
            "EXECUTABLE | run=1 | 2026-07-31 20:00:00+03:00 | expires=2026-13-01T00:00:00+03:00"]


def consumer_expiry_expectation(sd, rdb, acc):
    """NEW-CASE EXPECTATION (N36; G9 requires it to FAIL on the base copies),
    read at the REAL clock: a release one hour into its window is live, the
    fixture's 2026-07-31 release is long expired, a malformed expires= fails
    closed in every consumer."""
    real = datetime.now(timezone.utc).astimezone(RIYADH).replace(microsecond=0)
    live = live_release_value(anchor=real - timedelta(minutes=60))
    expired = live_release_value()
    assert sd._decision_feed_state({"tfb decision feed": live}) == ("EXECUTABLE", "", live)
    assert sd._decision_feed_state({"tfb decision feed": expired}) == (
        "NOT_ACTIONABLE", "release_expired", expired), "send_digest honours an expired release"
    assert rdb._feed_execution_veto(live) is False
    assert rdb._feed_execution_veto(expired) is True, "run_daily_brief honours an expired release"
    assert _d10(acc, live).verdict == "PASS"
    assert (_d10(acc, expired).verdict, _d10(acc, expired).measured) == ("WARN", "release_expired"), \
        "tfb_acceptance passes an expired release"
    for bad in malformed_expiry_values(live):
        assert sd._decision_feed_state({"tfb decision feed": bad}) == (
            "NOT_ACTIONABLE", "release_expiry_invalid", bad), bad
        assert rdb._feed_execution_veto(bad) is True, bad
        assert (_d10(acc, bad).verdict, _d10(acc, bad).measured) == ("FAIL", "release_expiry_invalid"), bad


# Value shapes that exist today (no expires= token): every consumer must answer
# exactly as before the change (G9 compares against the pre-change copies).
TODAY_SHAPES = (
    "", "   ", "EXECUTABLE", "EXECUTABLE | run=1", " EXECUTABLE | run=1 ",
    "EXECUTABLE | run=90001 | 2026-07-31 20:00:00+03:00",
    "EXECUTABLE | run=90001 | 2026-07-31 20:00:00 | ML:OK GM:OK CFX:OK MF:OK",
    "executable | run=1", "EXECUTABLE(release) | run=1", "EXECUTABLE | no stamp", W0_VETO,
    "NOT_ACTIONABLE(missing:ML) | run=777002 | 2026-07-31 20:10:00+03:00 | ML:MISSING GM:OK",
    "NOT_ACTIONABLE(final_publication) | run=90001 | 2026-08-01 04:00:01+03:00 | release_expired | release=777001",
    "NOT_ACTIONABLE(final_publication) | run=90001 | 2026-07-31 21:00:00+03:00 | release_killed | release=777001",
    supersede_value("90001"), supersede_value("unknown"), "NOT_ACTIONABLE(owner hold)",
    "NOT ACTIONABLE | x", "WITHHELD | x", "NOT_ACTIONABLE(partial:GM) | run=1", "UPSTREAM_READY | run=1",
    "stale", "ACTIONABLE | run=1", "EXECUTABLE | run=1 | expired | release_expired",
)


def consumer_outputs(sd, rdb, acc):
    """Every consumer surface the change touches, on TODAY_SHAPES."""
    out = []
    model = {"sell": [{"note": "n", "sar": 5.0, "shares": 2, "stop": 1.0, "tp1": 2.0, "tp2": 3.0}],
             "trim": [{"note": "t", "sar": 1.0, "shares": 1, "stop": 1.0, "tp1": 2.0, "tp2": 3.0}],
             "add": [], "block": [], "freed_cash": 7.0}
    for value in TODAY_SHAPES:
        d = copy.deepcopy(model)
        rdb._apply_final_feed_veto(d, value)
        out.append({
            "value": value,
            "digest": sd._decision_feed_state({"tfb decision feed": value}),
            "veto": rdb._feed_execution_veto(value),
            "extract": rdb._extract_final_feed([["TFB Decision Feed", value]]),
            "extract_pair": rdb._extract_final_feed([["TFB Decision Feed", "EXECUTABLE | run=0"],
                                                     ["TFB Decision Feed", value]]),
            "top10": rdb._top10_execution_veto([["Status:", "output: EXECUTABLE"]], value),
            "model": d,
            "acceptance": [c.row() for c in acc.check_feed_truth(acc._MemSource({"_Status": status_rows(value)}))],
        })
    out.append({"digest_absent": sd._decision_feed_state({})})
    return out


def supersede_cli_expectation(module, mp, tmp_path):
    """NEW-CASE EXPECTATION (N34; G7 requires it to FAIL on the base copy): the
    exact workflow command line downgrades a live release of SOURCE."""
    ctx, _ = released(mp, tmp_path)
    import integrations.google_sheets_service as gss
    mp.setattr(gss, "get_sheets_service", lambda *args, **kwargs: ctx.service)
    mp.setenv("GITHUB_RUN_ATTEMPT", "1")
    out = tmp_path / "supersede.json"
    code = module.main(["--mode", "supersede", "--current-run", CURRENT_RUN,
                        "--sheet-id", "synthetic-sheet", "--json-out", str(out)])
    cell = ctx.service.rows[3][12]
    assert code == 0, "supersede CLI exit %r" % (code,)
    assert cell.startswith("NOT_ACTIONABLE(final_publication) | run=90001 | "), cell
    assert cell.endswith(" | superseded_by_run=90002.1") and not R.reads_executable(cell), cell
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["outcome"] == "superseded" and payload["downgrade"] == cell
    return ctx


def supersede_eligibility_expectation(module):
    """NEW-CASE EXPECTATION (N31; G10 requires it to FAIL on the base copy)."""
    value = supersede_value("90001")
    assert module.classify_prior(value, CURRENT_RUN) == ("eligible", "superseded_older_run"), \
        "the superseding run is not releasable"
    assert module.classify_prior(value, SOURCE) == ("refused", "superseded_by_run")


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
    # v1.1.0: the consumers enforce expires=; read them at the release instant
    # (this fixture's 2026-07-31 window is long past in real time).
    for module in (send_digest, run_daily_brief):
        pin_expiry_clock(mp, module, NOW)
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
    # v1.1.0: the pin follows the module (1.0.0 -> 1.1.0 in the same commit).
    assert ("release_decision_feed", R.SCRIPT_VERSION) in {(n, v) for n, v, _ in V.SCRIPTS}
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


# =========================================================================== v1.1.0 SUPERSEDE
def case_w6_superseding_run_releasable_after_its_final_job_passed(mp, tmp_path):
    """Release 90001, run 90002 supersedes it at writer start, 90002's final job
    passes (so it writes no veto): 90002 must still be releasable, 90001 not."""
    ctx, _ = released(mp, tmp_path)
    ctx.clock["now"] = NOW + timedelta(minutes=10)
    result = supersede(ctx)
    expected = supersede_value("90001", stamp="2026-07-31 21:10:00+03:00")
    assert result["exit_code"] == 0 and result["outcome"] == "superseded", result
    assert ctx.service.rows[3][12] == expected
    run_sync_verdict(sync_module(), mp, ctx.service)        # the sync preserves the downgrade
    assert ctx.service.rows[3][12] == expected
    mp.setenv("GITHUB_RUN_ID", RELEASE_RUN)
    ctx.service.calls.clear()
    again = ctx.release()                                    # re-release of the superseded run
    assert again["exit_code"] == 3 and again["reasons"] == ["superseded_by_run"]
    assert ctx.updates() == [] and len(ctx.captured["coverage"]) == 1
    ctx.grids = grids_for_run(CURRENT_RUN)                   # the 90002 cohort, audits pass
    ctx.clock["now"] = NOW + timedelta(minutes=30)
    result = ctx.release(source=CURRENT_RUN)
    assert result["outcome"] == "released" and result["prior_class"] == "eligible", result["reasons"]
    cell = ctx.service.rows[3][12]
    assert R.RELEASE_VALUE_RE.fullmatch(cell) and R._single_run(cell) == CURRENT_RUN


def case_n26_supersede_noop_writes_nothing(mp, tmp_path):
    shapes = [W0_VETO, "NOT_ACTIONABLE(owner hold)", "NOT_ACTIONABLE(missing:ML) | run=1 | x",
              supersede_value("90001"), "not_actionable(final_publication) | run=1",
              "NOT_ACTIONABLE(final_publication) | run=90001 | x | release_expired | release=777001",
              "", "stale", "ACTIONABLE | run=1", "UPSTREAM_READY | run=1"]
    for index, feed in enumerate(shapes + [None]):
        ctx = setup(mp, tmp_path / str(index), feed=feed or "", absent=feed is None)
        before = copy.deepcopy(ctx.service.rows)
        result = supersede(ctx)
        assert result["exit_code"] == 0 and result["outcome"] == "nothing_to_supersede", (feed, result)
        assert [kind for kind, _ in ctx.service.calls] == ["get"], feed
        assert ctx.service.rows == before and result["downgrade"] is None
        assert result["prior_executable"] is False and not result["feed_promoted"]
    assert R.SUPERSEDE_MODE not in R.MODES     # run() keeps refusing it: never a promotion path
    ctx = setup(mp, tmp_path / "run")
    mp.setenv("TFB_DECISION_RELEASE", "1")
    assert ctx.release(mode="supersede")["reasons"] == ["mode_invalid"] and ctx.service.calls == []


def case_n27_supersede_downgrades_live_and_legacy_executable(mp, tmp_path):
    ctx, _ = released(mp, tmp_path)
    ctx.clock["now"] = NOW + timedelta(minutes=5)
    result = supersede(ctx)
    value = supersede_value("90001", stamp="2026-07-31 21:05:00+03:00")
    assert result["exit_code"] == 0 and result["outcome"] == "superseded" and result["downgrade"] == value
    assert result["superseded_run"] == SOURCE and result["blocker_published"] and not result["feed_promoted"]
    assert [(kind, kw["range"]) for kind, kw in ctx.service.calls] == [
        ("get", final.KEY_RANGE), ("get", final.KEY_RANGE), ("update", "'_Status'!L4:M4"),
        ("get", "'_Status'!L4:M4")]
    assert ctx.updates()[0]["valueInputOption"] == "RAW" and ctx.service.rows[3][12] == value
    assert R.SUPERSEDE_VALUE_RE.fullmatch(value) and SYNC_VETO_RE.match(value)
    assert R.parse_feed(value, ctx.clock["now"])["state"] == "NOT_ACTIONABLE"
    from scripts import run_daily_brief, send_digest
    assert send_digest._decision_feed_state({"tfb decision feed": value})[0] == "NOT_ACTIONABLE"
    assert run_daily_brief._feed_execution_veto(value) is True
    assert R.classify_prior(value, SOURCE) == ("refused", "superseded_by_run")
    ctx.service.calls.clear()                                 # second leg of the same run: no-op
    assert supersede(ctx)["outcome"] == "nothing_to_supersede" and ctx.updates() == []
    # Legacy and malformed EXECUTABLE shapes every age-blind consumer honours.
    legacy = {"EXECUTABLE | run=90001 | 2026-07-31 20:00:00+03:00": "90001",
              "executable | run=90001 | 2026-07-31 20:00:00+03:00": "90001",
              "EXECUTABLE(release) | run=77 | x": "77", "EXECUTABLE": "unknown",
              "EXECUTABLE | no stamp": "unknown", "EXECUTABLE | run=1 run=2": "unknown",
              "EXECUTABLE | run=local | x": "unknown", live_release_value(run="90000"): "90000"}
    for index, (feed, run) in enumerate(legacy.items()):
        ctx = setup(mp, tmp_path / ("legacy%d" % index), feed=feed)
        result = supersede(ctx, attempt="2")
        expected = supersede_value(run, attempt="2", stamp="2026-07-31 21:00:00+03:00")
        assert result["exit_code"] == 0 and result["downgrade"] == expected, (feed, result)
        assert [u["body"]["values"][0][1] for u in ctx.updates()] == [expected]
    # A rerun attempt of the RELEASED run supersedes its own release too.
    ctx, _ = released(mp, tmp_path / "rerun")
    result = supersede(ctx, current=SOURCE, attempt="2")
    assert ctx.service.rows[3][12] == supersede_value("90001", by=SOURCE, attempt="2",
                                                      stamp="2026-07-31 21:00:00+03:00")
    assert R.classify_prior(ctx.service.rows[3][12], SOURCE) == ("refused", "superseded_by_run")


def case_n28_supersede_identity_zero_io(mp, tmp_path):
    ctx, _ = released(mp, tmp_path)
    for current in ("", "local", "0", "0123", " 90002", "9e3", None, 90002, "90002\n", "-1"):
        result = supersede(ctx, current=current)
        assert result["exit_code"] == 3 and result["reasons"] == ["current_run_invalid"], current
    for attempt in ("", "0", "x", "01", " 1", 1):
        result = supersede(ctx, attempt=attempt)
        assert result["exit_code"] == 3 and result["reasons"] == ["run_attempt_invalid"], attempt
    mp.delenv("GITHUB_RUN_ATTEMPT", raising=False)
    result = R.supersede("synthetic-sheet", CURRENT_RUN, service=ctx.service, now_utc=ctx.clock["now"])
    assert result["reasons"] == ["run_attempt_invalid"]          # read from GITHUB_RUN_ATTEMPT
    mp.setenv("GITHUB_RUN_ATTEMPT", "3")
    for sheet in ("", "  ", None):
        result = R.supersede(sheet, CURRENT_RUN, service=ctx.service, now_utc=ctx.clock["now"])
        assert result["exit_code"] == 3 and result["reasons"] == ["sheet_id_missing"]
    assert ctx.service.calls == [] and ctx.service.rows[3][12] == W2_VALUE
    result = R.supersede("synthetic-sheet", CURRENT_RUN, service=ctx.service, now_utc=ctx.clock["now"])
    assert result["downgrade"].endswith("superseded_by_run=90002.3")


def case_n29_supersede_faults_fail_closed(mp, tmp_path):
    ctx, _ = released(mp, tmp_path)
    for grid in LAYOUTS:
        calls = []

        class Layout:
            def spreadsheets(self):
                return self

            def values(self):
                return self

            def get(self, **kwargs):
                calls.append("get")
                return SimpleNamespace(execute=lambda: {"values": copy.deepcopy(grid)})

            def update(self, **kwargs):
                raise AssertionError("no write on a layout fault")

        result = supersede(ctx, service=Layout())
        assert result["exit_code"] == 3 and result["outcome"] == "layout_fault", grid
        assert calls == ["get"]

    class Down(GoogleValues):
        def get(self, **kwargs):
            raise ConnectionError("synthetic 503")
    result = supersede(ctx, service=Down())
    assert result["exit_code"] == 3 and result["reasons"] == ["layout_or_read_failed"]
    # The update fails and the cell still reads EXECUTABLE: unconfirmed, exit 3.
    service = GoogleValues(write_error=True)
    service.rows[3][11:13] = [final.KEY, W2_VALUE]
    result = supersede(ctx, service=service)
    assert result["exit_code"] == 3 and result["outcome"] == "supersede_unconfirmed"
    assert service.rows[3][12] == W2_VALUE and not result["blocker_published"]

    class RereadDown(GoogleValues):
        def get(self, **kwargs):
            if self.write_error and kwargs["range"] == final.KEY_RANGE and \
                    any(kind == "update" for kind, _ in self.calls):
                raise ConnectionError("synthetic 503 on the re-read")
            return super().get(**kwargs)
    service = RereadDown(write_error=True)
    service.rows[3][11:13] = [final.KEY, W2_VALUE]
    result = supersede(ctx, service=service)
    assert result["exit_code"] == 3 and "reread_error" in result

    class PeerRace(GoogleValues):
        """A parallel leg of the same run lands its own downgrade between this
        leg's update and its readback: publish_blocker raises, the re-read shows
        a non-EXECUTABLE value, and the leg may proceed."""
        def update(self, **kwargs):
            request = super().update(**kwargs)

            def write():
                result = request.execute()
                self.rows[3][12] = supersede_value("90001", stamp="2026-07-31 21:00:01+03:00")
                return result
            return SimpleNamespace(execute=write)
    service = PeerRace()
    service.rows[3][11:13] = [final.KEY, W2_VALUE]
    result = supersede(ctx, service=service)
    assert result["exit_code"] == 0 and result["outcome"] == "superseded_confirmed_by_reread", result
    assert result["reasons"] == ["readback_raced"] and not R.reads_executable(service.rows[3][12])

    class KeyVanished(GoogleValues):
        """The key row is cleared between the update and the readback: the
        re-read finds no feed at all, which is NOT a confirmed downgrade."""
        def update(self, **kwargs):
            request = super().update(**kwargs)

            def write():
                result = request.execute()
                self.rows[3][11:13] = ["", ""]
                return result
            return SimpleNamespace(execute=write)
    service = KeyVanished()
    service.rows[3][11:13] = [final.KEY, W2_VALUE]
    vanished = supersede(ctx, service=service)
    assert vanished["exit_code"] == 3 and vanished["outcome"] == "supersede_unconfirmed", vanished
    # The workflow step fails on exit 3: the supersede code path never returns 1 or 2.
    assert {supersede(ctx, current="x")["exit_code"], result["exit_code"]} <= {0, 3}


def case_n30_release_gate_spelling_never_outruns_the_supersede_step(mp, tmp_path):
    for word in (" 1", "1 ", " on", "TRUE "):
        ctx = setup(mp, tmp_path / repr(word))
        mp.setenv("TFB_DECISION_RELEASE", word)
        assert R.release_enabled()
        result = ctx.release()
        assert result["exit_code"] == 3 and result["reasons"] == ["release_gate_not_canonical"], word
        assert ctx.service.calls == [] and ctx.captured["coverage"] == [] and ctx.probe_calls == []
        assert not evaluate_step_if(EXPECTED_STEP_IF[True], variable=word, event="schedule")
    for word in ("1", "true", "TRUE", "Yes", "on"):
        ctx = setup(mp, tmp_path / ("ok" + word))
        mp.setenv("TFB_DECISION_RELEASE", word)
        assert ctx.release()["outcome"] == "released", word
        assert evaluate_step_if(EXPECTED_STEP_IF[True], variable=word, event="schedule")


def case_n31_superseded_value_eligibility_matrix(mp, tmp_path):
    supersede_eligibility_expectation(R)
    expect = {
        (supersede_value("90001"), "90002"): ("eligible", "superseded_older_run"),
        (supersede_value("90001"), "90003"): ("eligible", "superseded_older_run"),
        (supersede_value("90001"), "90001"): ("refused", "superseded_by_run"),
        (supersede_value("90001"), "90000"): ("refused", "superseded_by_run"),
        (supersede_value("90000"), "90001"): ("refused", "superseded_by_run"),   # older than the superseder
        (supersede_value("unknown"), "90002"): ("eligible", "superseded_older_run"),
        (supersede_value("unknown"), "90001"): ("refused", "superseded_by_run"),
        (supersede_value("90001", by="90001", attempt="2"), "90001"): ("refused", "superseded_by_run"),
        (supersede_value("90001", by="90001", attempt="2"), "90002"): ("eligible", "superseded_older_run"),
        # Not the exact grammar: refused exactly as in v1.0.0.
        ("NOT_ACTIONABLE(final_publication) | run=90000 | superseded_by_run=90001", "90001"):
            ("refused", "superseded_by_run"),
        (supersede_value("90001").replace("90002.1", "90002"), "90002"): ("refused", "superseded_by_run"),
        (supersede_value("90001") + " | x", "90002"): ("refused", "superseded_by_run"),
        (supersede_value("90001").lower(), "90002"): ("refused", "superseded_by_run"),
        (supersede_value("90001").replace("21:10:00", "21:10"), "90002"): ("refused", "superseded_by_run"),
        (supersede_value("local"), "90002"): ("refused", "superseded_by_run"),
        ("EXECUTABLE | run=90001 | superseded_by_run=90002.1", "90002"): ("refused", "superseded_by_run"),
    }
    for (prior, source), verdict in expect.items():
        assert R.classify_prior(prior, source) == verdict, (prior, source)
    with pytest.raises(ValueError):
        R.supersede_value("local", NOW, CURRENT_RUN, "1")


def case_n32_supersede_never_promotes_ast(mp, tmp_path):
    tree = ast.parse((ROOT / "scripts" / "release_decision_feed.py").read_text(encoding="utf-8"))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "supersede")
    calls = [ast.unparse(n.func) for n in ast.walk(fn) if isinstance(n, ast.Call)]
    assert "fin.publish_blocker" in calls and calls.count("fin.publish_blocker") == 1
    assert not [c for c in calls if c.endswith((".update", "publish_release", "compose_release_value", "fin.run"))]
    assert "EXECUTABLE |" not in ast.unparse(fn)


def case_n33_supersede_cli_contract(mp, tmp_path):
    supersede_cli_expectation(R, mp, tmp_path)
    out = tmp_path / "bad.json"
    assert R.main(["--mode", "supersede", "--sheet-id", "s", "--json-out", str(out)]) == 3
    assert json.loads(out.read_text(encoding="utf-8"))["reasons"] == ["current_run_invalid"]
    assert R.main(["--mode", "release", "--sheet-id", "s", "--json-out", str(tmp_path / "x.json")]) == 3
    assert not (tmp_path / "x.json").exists()                 # unchanged: no source, no JSON


def case_n34_supersede_wired_into_every_page_writer(mp, tmp_path):
    workflows = ROOT / ".github" / "workflows"
    supersede_workflow_expectation(workflows)
    for name in ("release_decision_feed.yml", "grid_capacity_repair.yml"):
        text = (workflows / name).read_text(encoding="utf-8")
        assert SUPERSEDE_STEP not in text and "--mode supersede" not in text, name
    # grid_capacity_repair.yml: no step, by design. It only shrinks empty grid
    # rows above data extent + buffer and prunes old _Run_Log rows; it never
    # writes a page cell or a run= token, so it cannot re-stamp a cohort. The
    # release still defers while it is in flight.
    tool = (ROOT / "scripts" / "repair_grid_capacity.py").read_text(encoding="utf-8")
    assert "run=" not in tool and "GITHUB_RUN_ID" not in tool and "_Status" not in tool
    assert "grid_capacity_repair.yml" in R.INFLIGHT_WORKFLOWS
    for path in (DAILY_YML, RECOVERY_YML):
        text = path.read_text(encoding="utf-8")
        added = [line for line in text.splitlines() if "Supersede" in line or "supersede" in line]
        assert added and all(ord(ch) < 128 for line in added for ch in line)


def case_n34b_target_receipt_cannot_delay_or_bypass_supersede(mp, tmp_path):
    # JSON is valid YAML. These temporary workflow copies are parsed only;
    # none of their commands, including the deliberately bad ones, are run.
    documents = {name: _load_yaml(ROOT / ".github/workflows" / name)
                 for name in {file_name for file_name, _ in PAGE_WRITERS}}
    original = documents["daily_sync.yml"]["jobs"]["sync-dashboard"]["steps"]
    receipt_index = next(i for i, step in enumerate(original)
                         if step.get("name") == TARGET_RECEIPT_STEP)
    receipt = original[receipt_index]
    extra = {"name": "Unexpected pre-veto I/O", "run": "curl https://unexpected.invalid"}
    mutations = {
        "missing": [],
        "extra-before": [extra, receipt],
        "extra-after": [receipt, extra],
        "duplicate": [receipt, receipt],
        "extra-command": [dict(receipt, run=receipt["run"] + "\ncurl https://unexpected.invalid\n")],
        "different-command": [dict(receipt, run=receipt["run"].replace(" write ", " resolve "))],
        "conditional": [dict(receipt, **{"if": "${{ false }}"})],
        "ignored-failure": [dict(receipt, **{"continue-on-error": True})],
        "different-target": [dict(receipt, env={"SYNC_GROUP": "core-pages"})],
    }
    for label, replacement in mutations.items():
        directory = tmp_path / label
        directory.mkdir()
        changed = copy.deepcopy(documents)
        steps = changed["daily_sync.yml"]["jobs"]["sync-dashboard"]["steps"]
        steps[receipt_index:receipt_index + 1] = copy.deepcopy(replacement)
        for name, document in changed.items():
            (directory / name).write_text(json.dumps(document), encoding="utf-8")
        with pytest.raises(AssertionError):
            supersede_workflow_expectation(directory)


def case_n35_consumer_expiry_boundaries_and_lockstep(mp, tmp_path):
    from scripts import run_daily_brief, send_digest, tfb_acceptance
    consumer_expiry_expectation(send_digest, run_daily_brief, tfb_acceptance)
    value = live_release_value()
    expires = R._parse_t(R.RELEASE_VALUE_RE.fullmatch(value).group("expires"))
    helpers = [m._feed_release_expiry for m in (send_digest, run_daily_brief, tfb_acceptance)]
    for helper in helpers:
        assert helper(value, expires - timedelta(seconds=1)) == ""
        assert helper(value, expires) == ""                     # inclusive, like the sweep
        assert helper(value, expires + timedelta(seconds=1)) == "release_expired"
        assert helper(value, expires.replace(tzinfo=None)) == "release_expiry_invalid"
        assert helper(value.replace("expires=", "EXPIRES="), expires + timedelta(seconds=1)) == "release_expired"
        for shape in TODAY_SHAPES:
            assert helper(shape, expires) == "", shape         # inert on every value of today
        for minutes in (0, 1, 240, 479, 480, 481, 600):
            at = ANCHOR + timedelta(minutes=minutes)
            for extra in (0, 1):
                when = at + timedelta(seconds=extra)
                assert (helper(value, when) == "release_expired") is \
                    (R.sweep_reason(value, when) == "release_expired"), (minutes, extra)
    # Lockstep: the helper and its two constants are byte-identical in all three.
    sources = []
    for module in (send_digest, run_daily_brief, tfb_acceptance):
        text = Path(module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(text)
        parts = [ast.get_source_segment(text, node) for node in tree.body
                 if (isinstance(node, ast.FunctionDef) and node.name == "_feed_release_expiry")
                 or (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in (
                     "_FEED_EXPIRES_RE", "_FEED_EXPIRES_FORM") for t in node.targets))]
        assert len(parts) == 3, module.__name__
        sources.append(parts)
    assert sources[0] == sources[1] == sources[2]
    # Consumers at the release instant: live; one second after expires=: not.
    for module in (send_digest, run_daily_brief, tfb_acceptance):
        pin_expiry_clock(mp, module, expires)
    assert send_digest._decision_feed_state({"tfb decision feed": value})[0] == "EXECUTABLE"
    assert run_daily_brief._feed_execution_veto(value) is False and _d10(tfb_acceptance, value).verdict == "PASS"
    for module in (send_digest, run_daily_brief, tfb_acceptance):
        pin_expiry_clock(mp, module, expires + timedelta(seconds=1))
    assert send_digest._decision_feed_state({"tfb decision feed": value})[1] == "release_expired"
    assert run_daily_brief._feed_execution_veto(value) is True
    d = {"sell": [{"note": "n", "sar": 1.0}], "trim": [], "add": [], "block": [], "freed_cash": 1.0}
    run_daily_brief._apply_final_feed_veto(d, value)
    assert d["execution_veto"] == "release_expired: " + value and d["sell"] == []
    assert run_daily_brief._top10_execution_veto([], value) == "Final decision feed: release_expired: " + value


def case_n36_consumer_import_graph_unchanged(mp, tmp_path):
    for name in ("send_digest", "run_daily_brief", "tfb_acceptance"):
        text = (ROOT / "scripts" / (name + ".py")).read_text(encoding="utf-8")
        tree = ast.parse(text)
        top = {ast.unparse(node) for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))}
        assert not [line for line in top if "release_decision_feed" in line or "scripts" in line], (name, top)
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_feed_release_expiry")
        imports = [ast.unparse(n) for n in ast.walk(fn) if isinstance(n, (ast.Import, ast.ImportFrom))]
        assert imports == ["from datetime import datetime as _datetime, timezone as _timezone"], imports


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
    identical, gas_base = 0, None
    for current, base in pairs:
        digest = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (current, base)]
        if digest[0] != digest[1] and current == GAS:
            gas_base = base   # v1.1.2: proved after the loop, so a missing node skips only that proof
            continue
        assert digest[0] == digest[1], current
        identical += 1
    print("G4 byte-identical: %d protected files" % identical)
    if gas_base is not None:
        _gas_changed_only_by_feed_cohort_binding(gas_base)   # v1.1.1


def _gas_changed_only_by_feed_cohort_binding(base):
    """v1.1.1 (2026-10-13): DT10 v1.13.8 changes the .gs on purpose. Accept the
    difference only through the binding harness run against this base copy.
    v1.1.2 (2026-10-13): also require the harness's D5, which proves that the
    historical v1.13.8 file minus its designed regions equals this base copy.
    v1.13.9: the harness reverses only its enumerated cash-label/UTC adapter
    edits, pins the complete historical SHA256, and mutation-tests that boundary
    before running the unchanged historical D5 guard. Current feed tests still
    execute the delivered file."""
    node = node_binary()
    if node is None:
        pytest.skip("node is required to prove the DT10 v1.13.8 default-off identity")
    harness = ROOT / "tests" / "js" / "dt10_feed_cohort_binding.test.js"
    env = dict(os.environ, DT10_BASE=str(base), TFB_PYTHON=sys.executable)
    env.pop("DT10_DELIVERED", None)
    proc = subprocess.run([node, str(harness)], env=env, capture_output=True, text=True,
                          timeout=600, cwd=str(ROOT))
    lines = proc.stdout.strip().splitlines()
    assert proc.returncode == 0 and lines and lines[-1].startswith("PASS ") \
        and "SKIP " not in proc.stdout, proc.stdout[-3000:] + proc.stderr[-2000:]
    assert "\nPASS D5 " in "\n" + proc.stdout, "harness D5 (designed regions only) did not run"   # v1.1.2
    print("G4 16_Decision_Top10.gs: v1.13.9 cash-label/UTC edits match the pinned "
          "v1.13.8 fixture; historical feed binding regions pass harness D5; %s" % lines[-1])


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
    assert ("release_decision_feed", R.SCRIPT_VERSION) in here["scripts"] and names <= here["flags"]
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


# --------------------------------------------------------------------------- v1.1.0 GOLDEN
PREV_FILES = ("release_decision_feed.py", "send_digest.py", "run_daily_brief.py", "tfb_acceptance.py",
              "daily_sync.yml", "page_refresh_recovery.yml", "grid_capacity_repair.yml")


def _require_prev():
    if not PREV_DIR or not os.path.isdir(PREV_DIR):
        pytest.skip("TFB_TEST_SUPERSEDE_BASE_DIR unset")
    missing = [name for name in PREV_FILES if not (Path(PREV_DIR) / name).exists()]
    if missing:
        pytest.skip("pre-change copies missing: %s" % missing)
    return Path(PREV_DIR)


def _prev_module(name):
    return _load_base(Path(PREV_DIR) / (name + ".py"), "tfb_prev_" + name)


def must_fail(expectation, *args, match=""):
    """Run a NEW case's expectation on the pre-change copies: it must FAIL.
    Raises AssertionError (a counted FAIL in the script runner too) when it
    passes, i.e. when the directory holds HEAD copies."""
    try:
        expectation(*args)
    except AssertionError as exc:
        assert match in str(exc), (match, str(exc))
        return str(exc)
    raise AssertionError(expectation.__name__ + " PASSED on the pre-change copies: "
                         "TFB_TEST_SUPERSEDE_BASE_DIR holds HEAD copies, not 0825561")


def case_g7_supersede_cli_fails_on_prechange_release_script(mp, tmp_path):
    """N33's expectation (the workflow's exact command line downgrades a live
    release) against the pre-change script: no supersede mode, so the release
    stays EXECUTABLE. A HEAD copy in the directory passes it and FAILS here."""
    _require_prev()
    supersede_cli_expectation(R, mp, tmp_path / "head")      # positive control
    base = _prev_module("release_decision_feed")
    must_fail(supersede_cli_expectation, base, mp, tmp_path / "base", match="supersede CLI exit 3")
    assert not hasattr(base, "supersede"), "the pre-change dir holds a HEAD copy"


def case_g8_workflows_prechange_lack_the_step_and_off_path_identity(mp, tmp_path):
    """N34's expectation fails on the pre-change workflows (no supersede step),
    and HEAD minus the added steps equals them exactly (yaml.safe_load), so an
    unset variable leaves every other line where it was."""
    prev = _require_prev()
    must_fail(supersede_workflow_expectation, prev, match="supersede step missing")
    total = 0
    for name, expected_removed in (("daily_sync.yml", 2), ("page_refresh_recovery.yml", 1),
                                   ("grid_capacity_repair.yml", 0)):
        current, removed = strip_supersede_steps(_load_yaml(ROOT / ".github" / "workflows" / name))
        assert removed == expected_removed, (name, removed)
        assert current == _load_yaml(prev / name), name + ": differs beyond the added steps"
        total += removed
    assert total == 3


def case_g9_consumers_prechange_honour_expired_release_and_today_identical(mp, tmp_path):
    """N35's expectation fails on the pre-change consumers (an expired release
    reads as EXECUTABLE), and on every value shape of today the HEAD consumers
    answer exactly as the pre-change copies."""
    _require_prev()
    from scripts import run_daily_brief, send_digest, tfb_acceptance
    base = [_prev_module(name) for name in ("send_digest", "run_daily_brief", "tfb_acceptance")]
    must_fail(consumer_expiry_expectation, *base, match="send_digest honours an expired release")
    assert not hasattr(base[0], "_feed_release_expiry"), "the pre-change dir holds a HEAD copy"
    here = consumer_outputs(send_digest, run_daily_brief, tfb_acceptance)
    then = consumer_outputs(*base)
    assert json.dumps(here, sort_keys=True, default=str) == json.dumps(then, sort_keys=True, default=str)
    print("G9 identical on %d value shapes" % len(TODAY_SHAPES))


def case_g10_eligibility_fails_on_prechange_release_script(mp, tmp_path):
    """N31's expectation (the superseding run is releasable) fails on the
    pre-change classify_prior, which refused every superseded_by_run value;
    the re-release refusal holds on both."""
    _require_prev()
    base = _prev_module("release_decision_feed")
    must_fail(supersede_eligibility_expectation, base, match="superseding run is not releasable")
    assert base.classify_prior(supersede_value("90001"), SOURCE) == ("refused", "superseded_by_run")


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
