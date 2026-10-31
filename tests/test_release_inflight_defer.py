#!/usr/bin/env python3
"""tests/test_release_inflight_defer.py - release_decision_feed v1.2.1
TFB_RELEASE_INFLIGHT_DEFER: a scheduled source=auto release attempt that lands
inside the daily_sync which superseded or produced its cohort defers (exit 1)
instead of refusing (exit 3), and the one key-block read retries transport
faults. Off (default) is v1.2.0 byte for byte apart from script_version.

Renumbered 2026-10-31 (merge of main #873-#881): the module is v1.3.1 over
main's v1.3.0 (#877, render settle, default off). The off base is now
main's v1.3.0 at 05b51ebe; with both gates unset it returns the same
results as v1.2.0, so OFF_GOLDEN is unchanged (I5 proves it against that
base copy).

WHY (2026-10-24)
----------------
4 of the 5 red release runs since 19 Oct were this collision, not a
rejection: 40083081692 (24 Oct) and 39938800176 (23 Oct) refused
superseded_by_run while the superseding daily_sync was in flight;
39763157033 (22 Oct) refused [source_not_completed, final_job_not_executed]
while its source sync was still running; 39584712312 (21 Oct) died on
'TimeoutError: The read operation timed out' on the bare key-block read.

Driven through the REAL run() with an in-memory key block, injected
provenance / in-flight probes, and (I3) the REAL googleapiclient HttpRequest
built from the bundled sheets.v4 discovery document. No network, no workbook.

Cases
  I1  armed + schedule + superseded_by_run + superseding run busy -> exit 1
      deferred [superseded_by_run, production_run_in_flight]; zero writes,
      no provenance, no audit; also with TFB_DECISION_RELEASE=1 (production)
  I2  armed + schedule + [source_not_completed, final_job_not_executed] +
      SOURCE busy -> exit 1 deferred; zero writes, no audit
  I3  key read: the real HttpRequest raising TimeoutError once is retried
      when armed (2 HTTP calls) and fails exit 3 layout_fault when off;
      three timeouts still fail closed when armed
  I4  negatives that keep exit 3 with the gate armed: dispatched run,
      explicit source, nothing in flight, an unrelated run in flight, probe
      error, non-exact supersede grammar, a re-release (SOURCE not older
      than the superseding run) and a superseding run older than SOURCE,
      final_job_not_executed alone, not_main, SOURCE not busy; mode=sweep and
      mode=kill read without retry; the sticky KILL variable on a scheduled
      release reads with the retry and is otherwise unchanged
  I5  off (unset and 9 non-armed spellings, plus armed on a dispatch) ==
      the v1.2.0 base on the full JSON result, the read call, the probe and
      provenance calls and the writes, over every fixture; checked against
      OFF_GOLDEN digests everywhere (they are proven against the base copy
      when it is available)
  I6  armed vocabulary: 1/true/yes/on (any case, padded) only
  N1  workflow: step-level mapping, job env unchanged; version floor,
      verifier pin, WHY order
  N2  added lines ASCII; no function removed; exactly two added

Golden negative: TFB_TEST_RDF_DELIVERED=<base copy> runs every case against
v1.2.0 as the delivered module (all but I5 fail; N2 skips). Base copy: TFB_TEST_RDF_BASE=<path>, else
`git show 05b51ebe:scripts/release_decision_feed.py` (v1.3.0 since the
2026-10-31 merge; 83283c61, v1.2.0, before it).

Run:  python3 -m pytest -q tests/test_release_inflight_defer.py
  or  python3 tests/test_release_inflight_defer.py   (PASS k/k + digest)
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import finalize_decision_publication as final  # noqa: E402

SCRIPT = ROOT / "scripts/release_decision_feed.py"
DELIVERED = Path(os.getenv("TFB_TEST_RDF_DELIVERED", "").strip() or SCRIPT)
BASE_SHA = "05b51ebe"   # main 2026-10-31; release_decision_feed.py is v1.3.0 there (was 83283c61, v1.2.0)
GATE = "TFB_RELEASE_INFLIGHT_DEFER"
ENV_NAMES = (GATE, "GITHUB_EVENT_NAME", "TFB_DECISION_RELEASE", "TFB_DECISION_RELEASE_KILL",
             "GITHUB_TOKEN", "GITHUB_REPOSITORY", "GITHUB_ACTIONS", "GITHUB_RUN_ID",
             "GITHUB_RUN_ATTEMPT")
OLD, NEW = "39931603565", "40079965055"
NOW = datetime(2026, 10, 24, 9, 43, 30, tzinfo=timezone.utc)
SUP = ("NOT_ACTIONABLE(final_publication) | run=" + OLD +
       " | 2026-10-24 12:00:05+03:00 | superseded_by_run=" + NEW + ".1")
VETO_OLD = ("NOT_ACTIONABLE(final_publication) | run=" + OLD +
            " | 2026-10-23 13:30:00+03:00 | coverage_failed")


def _load(name, path):
    saved = list(sys.path)   # a module loaded from a temp dir inserts its parents
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
    finally:
        sys.path[:] = saved
    return mod


R = _load("rdf_inflight_delivered", DELIVERED)
_BASE = {}


def _base_or_none():
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_RDF_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(
                    ["git", "show", BASE_SHA + ":scripts/release_decision_feed.py"],
                    cwd=ROOT, capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "rdf_v120_base.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["mod"] = _load("rdf_inflight_base", path) if path else None
    return _BASE["mod"]


def base():
    if _base_or_none() is None:
        pytest.skip("base copy unavailable (set TFB_TEST_RDF_BASE)")
    return _BASE["mod"]


# ------------------------------------------------------------ fixtures -----
def grid(*, gm_run=OLD, feed=SUP):
    rows = [["Backend URL", "https://synthetic.invalid"], ["Last Global Update", "stamp"],
            ["TFB Feed Market_Leaders", "OK | run=" + gm_run + " | 2026-10-24 11:32:39+03:00"],
            ["TFB Feed Global_Markets",
             "PARTIAL_FRESH | cov=99.6 | run=" + gm_run + " | 2026-10-24 11:58:44+03:00"]]
    if feed is not None:
        rows.append([final.KEY, feed])
    return rows


class _Req:
    def __init__(self, fn, log):
        self._fn, self._log = fn, log

    def execute(self, num_retries=None):
        self._log.append(num_retries)
        return self._fn()


class Service:
    """values().get/update over one in-memory key block; records the read."""

    def __init__(self, rows):
        self.rows = [list(r) for r in rows]
        self.calls, self.read_retries = [], []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **kw):
        self.calls.append(("get", copy.deepcopy(kw)))
        return _Req(lambda: {"values": copy.deepcopy(self.rows)}, self.read_retries)

    def update(self, **kw):
        self.calls.append(("update", copy.deepcopy(kw)))
        return _Req(lambda: {"updatedCells": 2}, [])

    def updates(self):
        return [kw for kind, kw in self.calls if kind == "update"]


def prov_data(run_id, *, status="completed", branch="main", final_status="completed",
              final_conclusion="failure"):
    return {"run": {"id": int(run_id), "path": ".github/workflows/daily_sync.yml",
                    "head_branch": branch, "event": "schedule", "status": status,
                    "run_attempt": 1, "head_sha": "a" * 40, "conclusion": None},
            "jobs": [{"name": "Verify final decision publication", "status": final_status,
                      "conclusion": final_conclusion}]}


def busy(*ids):
    return [{"workflow": "daily_sync.yml", "id": int(i), "event": "schedule",
             "status": "in_progress"} for i in ids]


class Boom(RuntimeError):
    pass


def _no_finalize():
    class F:
        KEY, KEY_RANGE = final.KEY, final.KEY_RANGE
        publication_slot = staticmethod(final.publication_slot)

        @staticmethod
        async def run(*a, **k):
            raise AssertionError("the strict audit must not run on these paths")

        @staticmethod
        def publish_blocker(*a, **k):
            raise AssertionError("no write may happen on these paths")
    return F


def drive(mod, rows, source="auto", *, mode="release", gate=None, event="schedule",
          inflight=None, provenance=None, service=None, release_armed=False, kill_var=False):
    saved = {k: os.environ.get(k) for k in ENV_NAMES}
    for k in ENV_NAMES:
        os.environ.pop(k, None)
    os.environ.update({"GITHUB_ACTIONS": "true", "GITHUB_RUN_ID": "777003",
                       "GITHUB_RUN_ATTEMPT": "1"})
    if gate is not None:
        os.environ[GATE] = gate
    if release_armed:
        os.environ["TFB_DECISION_RELEASE"] = "1"
    if kill_var:
        os.environ["TFB_DECISION_RELEASE_KILL"] = "1"
    if event is not None:
        os.environ["GITHUB_EVENT_NAME"] = event
    seen = {"provenance": [], "probe": 0}

    def prov(run_id):
        seen["provenance"].append(run_id)
        if provenance is None:
            raise RuntimeError("provenance unavailable (offline)")
        return provenance(run_id)

    def probe():
        seen["probe"] += 1
        value = [] if inflight is None else inflight
        if isinstance(value, Exception):
            raise value
        return copy.deepcopy(value)

    svc = service if service is not None else Service(rows)
    try:
        out = asyncio.run(mod.run("synthetic-sheet", source, mode=mode, service=svc,
                                  now_utc=NOW, inflight=probe, provenance=prov,
                                  finalize=_no_finalize()))
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return out, seen, svc


def norm(out):
    o = json.loads(json.dumps(out, sort_keys=True, default=str))
    o.pop("script_version", None)
    return o


# ---------------------------------------------------------------- I1 -----
def test_i1_superseded_by_the_running_sync_defers():
    out, seen, svc = drive(R, grid(), gate="1", inflight=busy(NEW))
    assert (out["exit_code"], out["outcome"]) == (1, "deferred")
    assert out["reasons"] == ["superseded_by_run", "production_run_in_flight"]
    assert out["deferred"] is True and out["prior_class"] == "refused"
    assert out["inflight_defer"] == {"run": NEW, "busy": True, "probe_error": None}
    assert [b["id"] for b in out["in_flight"]] == [int(NEW)]
    assert svc.updates() == [] and seen["provenance"] == [] and seen["probe"] == 1
    assert out["feed_promoted"] is False and out["blocker_published"] is False
    assert svc.read_retries == [R.KEY_READ_RETRIES] and R.KEY_READ_RETRIES == 2
    armed, _s, svc = drive(R, grid(), gate="1", inflight=busy(NEW), release_armed=True)
    assert armed["release_enabled"] is True and armed["dry_run"] is False
    assert (armed["exit_code"], armed["outcome"]) == (1, "deferred") and svc.updates() == []


# ---------------------------------------------------------------- I2 -----
def test_i2_source_still_running_defers():
    rows = grid(gm_run=NEW, feed=VETO_OLD)
    prov = lambda rid: prov_data(rid, status="in_progress", final_status="queued",
                                 final_conclusion=None)
    out, seen, svc = drive(R, rows, gate="on", inflight=busy(NEW), provenance=prov)
    assert (out["exit_code"], out["outcome"]) == (1, "deferred")
    assert out["reasons"] == ["source_not_completed", "final_job_not_executed",
                              "production_run_in_flight"]
    assert out["source_run_id"] == NEW and seen["provenance"] == [NEW]
    assert out["inflight_defer"]["run"] == NEW and out["inflight_defer"]["busy"] is True
    assert svc.updates() == []
    armed, _s, svc = drive(R, rows, gate="on", inflight=busy(NEW), provenance=prov,
                           release_armed=True)
    assert (armed["exit_code"], armed["outcome"]) == (1, "deferred") and svc.updates() == []


# ---------------------------------------------------------------- I3 -----
def _real_sheets(fails):
    """A REAL googleapiclient service whose transport raises TimeoutError
    for the first `fails` requests, then serves the key block."""
    httplib2 = pytest.importorskip("httplib2")
    discovery = pytest.importorskip("googleapiclient.discovery")
    doc = (Path(discovery.__file__).parent / "discovery_cache" / "documents"
           / "sheets.v4.json").read_text(encoding="utf-8")

    class Flaky:
        def __init__(self):
            self.calls = 0

        def request(self, uri, method="GET", body=None, headers=None, **kw):
            self.calls += 1
            if self.calls <= fails:
                raise TimeoutError("The read operation timed out")
            return (httplib2.Response({"status": "200"}),
                    json.dumps({"values": grid(feed=VETO_OLD)}).encode("utf-8"))
    http = Flaky()
    return discovery.build_from_document(doc, http=http), http


def _quiet_backoff():
    gh = pytest.importorskip("googleapiclient.http")
    return gh.time, gh.time.sleep


def test_i3_key_read_retries_a_timeout_only_when_armed():
    tmod, real_sleep = _quiet_backoff()
    tmod.sleep = lambda s: None
    try:
        svc, http = _real_sheets(1)
        on, seen, _ = drive(R, None, gate="1", service=svc)
        # the read survived; the run went on to the v1.2.0 provenance step
        assert http.calls == 2 and seen["provenance"] == [OLD]
        assert (on["exit_code"], on["outcome"]) == (1, "deferred")
        assert on["reasons"] == ["provenance_unavailable"]
        svc, http = _real_sheets(1)
        off, seen, _ = drive(R, None, service=svc)
        assert http.calls == 1 and seen["provenance"] == []
        assert (off["exit_code"], off["outcome"]) == (3, "layout_fault")
        assert "TimeoutError" in off["fatal"]
        svc, http = _real_sheets(3)
        dead, _seen, _ = drive(R, None, gate="1", service=svc)
        assert http.calls == 3 and (dead["exit_code"], dead["outcome"]) == (3, "layout_fault")
    finally:
        tmod.sleep = real_sleep


# ---------------------------------------------------------------- I4 -----
def _prov_completed(rid):
    return prov_data(rid, status="completed", final_status="queued", final_conclusion=None)


NEGATIVES = {
    "dispatched": dict(rows=grid(), event="workflow_dispatch", inflight=busy(NEW)),
    "no_event": dict(rows=grid(), event=None, inflight=busy(NEW)),
    "explicit_source": dict(rows=grid(), source=OLD, inflight=busy(NEW)),
    "nothing_in_flight": dict(rows=grid(), inflight=[]),
    "unrelated_in_flight": dict(rows=grid(), inflight=busy("40000000001")),
    "probe_error": dict(rows=grid(), inflight=Boom("api down")),
    "re_release": dict(rows=grid(gm_run=NEW, feed="NOT_ACTIONABLE(final_publication) | run=" + NEW
                                 + " | 2026-10-24 12:00:05+03:00 | superseded_by_run=" + NEW + ".2"),
                       inflight=busy(NEW)),
    "older_superseder": dict(rows=grid(gm_run=NEW, feed="NOT_ACTIONABLE(final_publication) | run=" + NEW
                                       + " | 2026-10-24 12:00:05+03:00 | superseded_by_run=" + OLD + ".2"),
                             inflight=busy(OLD)),
    "loose_supersede": dict(rows=grid(feed="NOT_ACTIONABLE(final_publication) | run=" + OLD
                                      + " | superseded_by_run=" + NEW + ".1"),
                            inflight=busy(NEW)),
    "final_job_alone": dict(rows=grid(gm_run=NEW, feed=VETO_OLD), inflight=busy(NEW),
                            provenance=_prov_completed),
    "not_main": dict(rows=grid(gm_run=NEW, feed=VETO_OLD), inflight=busy(NEW),
                     provenance=lambda rid: prov_data(rid, status="in_progress", branch="dev",
                                                      final_status="queued",
                                                      final_conclusion=None)),
    "source_not_busy": dict(rows=grid(gm_run=NEW, feed=VETO_OLD), inflight=busy(OLD),
                            provenance=lambda rid: prov_data(rid, status="in_progress",
                                                             final_status="queued",
                                                             final_conclusion=None)),
}


def _drive_case(mod, name, gate):
    c = dict(NEGATIVES[name])
    rows = c.pop("rows")
    return drive(mod, rows, gate=gate, **c)


def test_i4_negatives_keep_exit_3():
    for name in NEGATIVES:
        for release_armed in (False, True):
            c = dict(NEGATIVES[name])
            rows = c.pop("rows")
            out, _seen, svc = drive(R, rows, gate="1", release_armed=release_armed, **c)
            assert out["exit_code"] == 3 and out["outcome"] == "refused", (name, out["reasons"])
            assert "production_run_in_flight" not in out["reasons"], name
            assert out["deferred"] is False and svc.updates() == [], name
    for name in ("re_release", "older_superseder"):   # refused before any probe
        out, seen, _v = _drive_case(R, name, "1")
        assert seen["probe"] == 0 and "inflight_defer" not in out, name
    out, _s, _v = _drive_case(R, "probe_error", "1")
    assert out["inflight_defer"]["busy"] is False and "api down" in out["inflight_defer"]["probe_error"]
    out, _s, _v = _drive_case(R, "dispatched", "1")
    assert "inflight_defer" not in out            # the gate never applies to a dispatch
    for mode in ("sweep", "kill"):
        out, _s, svc = drive(R, grid(feed=VETO_OLD), mode=mode, gate="1", inflight=busy(NEW))
        assert out["exit_code"] == 0 and "inflight_defer" not in out and svc.updates() == [], mode
        assert svc.read_retries == [None], mode        # retries are for the release attempt only
    # the sticky KILL variable runs inside mode=release: the read retries,
    # and everything else is what the gate-off run gives
    live = ("EXECUTABLE | run=" + OLD + " | 2026-10-24 11:58:44+03:00"
            " | issued=2026-10-24T12:10:00+03:00 | expires=2026-10-24T19:58:44+03:00"
            " | ttl_min=480 | release=777001 | attempt=1 | receipts=T10:" + OLD + ",PF:" + OLD
            + " | t10_at=2026-10-24T12:05:00+03:00 | pf_at=2026-10-24T12:00:00+03:00"
            " | mp_at=2026-10-24T11:58:44+03:00 | audits=0,0")
    on, _s, svc_on = drive(R, grid(feed=live), gate="1", kill_var=True, release_armed=True)
    off, _s, svc_off = drive(R, grid(feed=live), kill_var=True, release_armed=True)
    assert svc_on.read_retries == [2] and svc_off.read_retries == [None]
    assert norm(on) == norm(off) and on["kill_active"] is True


# ---------------------------------------------------------------- I5 -----
OFF_GATES = (None, "", "0", "off", "false", "no", "enforce", "observe", " 1x", "2")
FIXTURES = {
    "i1": dict(rows=grid(), inflight=busy(NEW)),
    "i2": dict(rows=grid(gm_run=NEW, feed=VETO_OLD), inflight=busy(NEW),
               provenance=lambda rid: prov_data(rid, status="in_progress", final_status="queued",
                                                final_conclusion=None)),
    "eligible_offline": dict(rows=grid(feed=VETO_OLD), inflight=busy(NEW)),
    **NEGATIVES,
}
# v1.2.0 (main 83283c61) results per fixture, gate unset: sha256[:16] of the
# normalised JSON, the key-read request and retry argument, the probe and
# provenance calls and the writes. I5 proves them against the
# base copy when it is available and checks the delivered module everywhere.
OFF_GOLDEN = {
              "i1": "d8853a395b31fefd",
              "i2": "3a8327746b0ee4bd",
              "eligible_offline": "e19a073d3d01cac9",
              "dispatched": "d8853a395b31fefd",
              "no_event": "d8853a395b31fefd",
              "explicit_source": "8f42320bc0b6cdb8",
              "nothing_in_flight": "d8853a395b31fefd",
              "unrelated_in_flight": "d8853a395b31fefd",
              "probe_error": "d8853a395b31fefd",
              "re_release": "c69346df28e81d42",
              "older_superseder": "3e6044aa9cacbcfe",
              "loose_supersede": "fcb3b84530a7f389",
              "final_job_alone": "28ec8c96d31646ce",
              "not_main": "d4b4d6d00196eb7a",
              "source_not_busy": "3a8327746b0ee4bd"}


def _snapshot(mod, name, gate, *, event_override=False):
    c = dict(FIXTURES[name])
    rows = c.pop("rows")
    if event_override:
        c["event"] = "workflow_dispatch"
    out, seen, svc = drive(mod, rows, gate=gate, **c)
    return {"out": norm(out), "read": svc.read_retries, "provenance": seen["provenance"],
            "probe": seen["probe"], "updates": svc.updates(),
            "gets": [kw for kind, kw in svc.calls if kind == "get"]}


def _digest(snap):
    return hashlib.sha256(json.dumps(snap, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def test_i5_off_is_byte_identical_to_the_v120_base():
    b = _base_or_none()
    assert set(OFF_GOLDEN) == set(FIXTURES)
    for name in FIXTURES:
        ref = None
        if b is not None:
            ref = _snapshot(b, name, None)
            assert _digest(ref) == OFF_GOLDEN[name], (name, _digest(ref))
        for gate in OFF_GATES:
            got = _snapshot(R, name, gate)
            assert got["read"] == [None], (name, gate)        # execute() exactly as before
            if ref is not None:
                assert got == ref, (name, gate)
            assert _digest(got) == OFF_GOLDEN[name], (name, gate)
        # armed, but the attempt is a dispatch: the gate does not apply, and
        # v1.2.0 reads neither the gate nor the event, so the answer is the
        # gate-unset one
        got = _snapshot(R, name, "1", event_override=True)
        assert got["read"] == [None] and _digest(got) == OFF_GOLDEN[name], name


# ---------------------------------------------------------------- I6 -----
def test_i6_armed_vocabulary():
    for word in ("1", "true", "yes", "on", " ON ", "True", "YES"):
        out, _s, _v = drive(R, grid(), gate=word, inflight=busy(NEW))
        assert out["exit_code"] == 1, word
    for word in OFF_GATES:
        out, _s, _v = drive(R, grid(), gate=word, inflight=busy(NEW))
        assert out["exit_code"] == 3, word


# ---------------------------------------------------------------- N1 -----
def test_n1_workflow_mapping_version_and_pin():
    import yaml
    wf_path = ROOT / ".github/workflows/release_decision_feed.yml"
    text = wf_path.read_text(encoding="utf-8")
    assert all(ord(ch) < 128 for ch in text)
    wf = yaml.safe_load(text)
    job = wf["jobs"]["release-decision-feed"]
    step = {s["name"]: s for s in job["steps"]}["Release or retract the decision feed"]
    assert step["env"][GATE] == "${{ vars.TFB_RELEASE_INFLIGHT_DEFER || '0' }}"
    assert GATE not in job["env"]                        # job env stays final env + 2 gates
    assert step["env"]["EVENT_NAME"] == "${{ github.event_name }}"
    assert '"$EVENT_NAME" == "schedule" && ( "$code" -eq 1 || "$code" -eq 2 )' in step["run"]
    assert tuple(int(x) for x in R.SCRIPT_VERSION.split(".")) >= (1, 2, 1)
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("release_decision_feed", "%s",' % R.SCRIPT_VERSION in vd
    src = DELIVERED.read_text(encoding="utf-8")
    # 2026-10-31: authored as 1.2.1; renumbered 1.3.1 above main's 1.3.0 block.
    assert (src.index("VERSION 1.3.1 (2026-10-24") < src.index("VERSION 1.3.0 (2026-10-29)")
            < src.index("VERSION 1.2.0 (2026-10-19)"))
    assert R.INFLIGHT_DEFER_ENV == GATE and R.KEY_READ_RETRIES == 2


# ---------------------------------------------------------------- N2 -----
def test_n2_added_lines_are_ascii_and_nothing_removed():
    b = base()
    if R.SCRIPT_VERSION != "1.3.1":   # 2026-10-31: was 1.2.1 over 83283c61
        pytest.skip("exact 05b51ebe -> 1.3.1 diff checks apply to 1.3.1 only")
    import ast
    import difflib
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = DELIVERED.read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")
               and ln[1:].strip()]
    assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()][:3]
    assert removed == [
        'SCRIPT_VERSION = "1.3.0"',
        '        grid = service.spreadsheets().values().get(',
        '            valueRenderOption="UNFORMATTED_VALUE").execute().get("values")',
    ], removed

    def fns(text):
        return {n.name for n in ast.walk(ast.parse(text))
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    assert not fns("\n".join(old)) - fns("\n".join(new))
    assert fns("\n".join(new)) - fns("\n".join(old)) == {"inflight_defer_enabled", "_defer_for_run"}


def _digest_all(results):
    parts = ["%s=%s" % kv for kv in sorted(results.items())]
    parts.append(R.SCRIPT_VERSION)
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:16]


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    passed = skipped = 0
    results = {}
    for name, fn in tests:
        try:
            fn()
            passed += 1
            results[name] = "ok"
            print("ok  ", name)
        except pytest.skip.Exception as exc:
            skipped += 1
            results[name] = "skip"
            print("SKIP", name, "::", exc)
    print(f"PASS {passed}/{len(tests)}"
          + (f" (skipped {skipped})" if skipped else "")
          + f" digest {_digest_all(results)}")
