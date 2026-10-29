#!/usr/bin/env python3
"""tests/test_release_render_settle.py - release_decision_feed v1.3.0: a
cockpit caught mid-render settles instead of failing (TFB_RELEASE_RENDER_SETTLE,
default off).

WHY (2026-10-29)
----------------
Both cockpits write 'Last run <now> | status: running...' into B2 as the first
write of every render. A release run whose strict audit reads B2 then fails;
an ARMED re-verification of a live release withdraws it with a release= token
and every later release of that run is refused (release_window_consumed).
The independent review of PR #876 reproduced this on the release's own
fixtures; R1 below is that reproduction on the unchanged (gate-off) path.

  R1  golden negative, gate off: a live release re-verified while Top_10
      renders is withdrawn and its window is consumed (the defect, unchanged)
  R2  gate on, same race: one 120 s settle, the re-audit passes, the live
      release stays (already_released, zero writes)
  R3  Portfolio_Decision mid-render, and both cockpits mid-render
  R4  still mid-render after two settles: judged exactly as today (an armed
      re-verification still withdraws: fail closed)
  R5  a FIRST release caught mid-render is released after the settle
  R6  no settle for any other failure: coverage failed, a stale board, an
      ERROR or unknown status, a missing receipt with status OK
  R7  a writer (or a probe fault) during the settle defers with zero writes
  R8  a strict-audit fault on the re-run is fatal with zero writes
  R9  settle_candidates is exact (code, surface, message, coverage passed)
  R10 gate vocabulary
  R11 off-path identity against the untouched 1.2.0 module (git show 729a316e;
      SKIP on a shallow checkout)
  R12 workflow: the gate is mapped in the release STEP env only (job env
      parity with the final job holds), summary keys unchanged, bounded time

Runs standalone (python tests/test_release_render_settle.py -> PASS k/k,
SKIPs reported) and under pytest. Offline fakes only.
"""
from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import re
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import finalize_decision_publication as final  # noqa: E402
from scripts import release_decision_feed as R  # noqa: E402
from tests import test_release_decision_feed as T  # noqa: E402

BASE_SHA = "729a316e9315c127e2b0e4e970e8c338d8c1dd95"   # main with release_decision_feed 1.2.0
RELEASE_YML = ROOT / ".github" / "workflows" / "release_decision_feed.yml"
DAILY_YML = ROOT / ".github" / "workflows" / "daily_sync.yml"
ELLIPSIS = chr(0x2026)
T10_RUNNING = "Last run 2026-07-31 21:05:00 | status: running" + ELLIPSIS
PF_RUNNING = "Last run 2026-07-31 21:05:00 | status: running" + ELLIPSIS


# --------------------------------------------------------------------------- fixtures
class Grids:
    """The decision audit's grids per call: the first `running` calls see the
    cockpit(s) mid-render, later calls see the finished board."""

    def __init__(self, running=1, *, t10=True, pf=False, base=None):
        self.running, self.t10, self.pf, self.calls = running, t10, pf, 0
        self.base = base or T.current_publication_grids

    def __call__(self):
        self.calls += 1
        sources, portfolio, top10 = (copy.deepcopy(list(item)) for item in self.base())
        if self.calls <= self.running:
            if self.t10:
                top10[1][1] = T10_RUNNING
            if self.pf:
                portfolio[1][1] = PF_RUNNING
        return sources, portfolio, top10


class Sleeps:
    def __init__(self):
        self.seconds = []

    async def __call__(self, seconds):
        self.seconds.append(seconds)


def release(ctx, module=R, *, sleeps=None, mode="release"):
    kwargs = dict(mode=mode, service=ctx.service, now_utc=ctx.clock["now"],
                  inflight=ctx.inflight, provenance=ctx.provenance)
    if sleeps is not None:
        kwargs["settle_sleep"] = sleeps
    return asyncio.run(module.run("synthetic-sheet", T.SOURCE, **kwargs))


def arm(mp, *, settle=True, release_gate=True):
    if release_gate:
        mp.setenv("TFB_DECISION_RELEASE", "1")
    if settle:
        mp.setenv("TFB_RELEASE_RENDER_SETTLE", "1")
    else:
        mp.delenv("TFB_RELEASE_RENDER_SETTLE", raising=False)


def feed(ctx):
    return ctx.service.rows[3][12]


_WALL_CLOCK_KEYS = {"generated_at_utc"}    # the coverage report stamps real time


def stable(value):
    """The result without wall-clock stamps, for run-to-run comparison."""
    if isinstance(value, dict):
        return {key: stable(item) for key, item in value.items() if key not in _WALL_CLOCK_KEYS}
    if isinstance(value, list):
        return [stable(item) for item in value]
    return value


# --------------------------------------------------------------------------- cases
def case_r1_golden_negative_gate_off_withdraws_the_live_release(mp, tmp_path):
    grids = Grids(running=99)
    ctx = T.setup(mp, tmp_path, feed=T.live_release_value(), grids=grids)
    arm(mp, settle=False)
    sleeps = Sleeps()
    result = release(ctx, sleeps=sleeps)
    assert result["prior_class"] == "already_released" and result["outcome"] == "not_accepted"
    assert result["exit_code"] == 2 and "render_settle" not in result
    codes = [item["code"] for item in result["audits"]["decision"]["findings"]]
    assert "T10_STATUS_NOT_OK" in codes and "T10_SOURCE_CONSUMPTION_UNPROVEN" in codes
    written = [item["body"]["values"][0][1] for item in ctx.updates()]
    assert len(written) == 1 and written[0].endswith("| decision_failed | release=" + T.RELEASE_RUN)
    assert R.classify_prior(feed(ctx), T.SOURCE) == ("refused", "release_window_consumed")
    assert sleeps.seconds == [] and grids.calls == 1


def case_r2_gate_on_settles_and_keeps_the_live_release(mp, tmp_path):
    grids = Grids(running=1)
    live = T.live_release_value()
    ctx = T.setup(mp, tmp_path, feed=live, grids=grids)
    arm(mp)
    sleeps = Sleeps()
    result = release(ctx, sleeps=sleeps)
    assert result["exit_code"] == 0 and result["outcome"] == "already_released", result["reasons"]
    assert ctx.updates() == [] and feed(ctx) == live
    assert sleeps.seconds == [R.RENDER_SETTLE_S] and grids.calls == 2
    settle = result["render_settle"]
    assert len(settle) == 1 and settle[0]["attempt"] == 1 and settle[0]["wait_s"] == 120
    assert settle[0]["cockpits"] == ["Top_10_Investments"]
    assert "decision_findings_present" in settle[0]["reasons"]
    assert result["accepted"] is True and result["reasons"] == []
    assert len(ctx.probe_calls) == 2           # step 5 and the post-settle probe


def case_r3_portfolio_and_both_cockpits_mid_render(mp, tmp_path):
    for t10, pf, cockpits in ((False, True, ["Portfolio_Decision"]),
                              (True, True, ["Portfolio_Decision", "Top_10_Investments"])):
        grids = Grids(running=1, t10=t10, pf=pf)
        live = T.live_release_value()
        ctx = T.setup(mp, tmp_path / ("p" + str(int(t10))), feed=live, grids=grids)
        arm(mp)
        result = release(ctx, sleeps=Sleeps())
        assert result["outcome"] == "already_released" and ctx.updates() == [], cockpits
        assert sorted(result["render_settle"][0]["cockpits"]) == cockpits


def case_r4_still_mid_render_after_two_settles_fails_closed(mp, tmp_path):
    grids = Grids(running=3)
    ctx = T.setup(mp, tmp_path, feed=T.live_release_value(), grids=grids)
    arm(mp)
    sleeps = Sleeps()
    result = release(ctx, sleeps=sleeps)
    assert result["outcome"] == "not_accepted" and result["exit_code"] == 2
    assert sleeps.seconds == [120, 120] and grids.calls == 3
    assert [item["attempt"] for item in result["render_settle"]] == [1, 2]
    written = [item["body"]["values"][0][1] for item in ctx.updates()]
    assert len(written) == 1 and "release=" + T.RELEASE_RUN in written[0]
    assert R.classify_prior(feed(ctx), T.SOURCE) == ("refused", "release_window_consumed")


def case_r5_first_release_mid_render_is_released_after_the_settle(mp, tmp_path):
    grids = Grids(running=1)
    ctx = T.setup(mp, tmp_path, feed=T.W0_VETO, grids=grids)
    arm(mp)
    sleeps = Sleeps()
    result = release(ctx, sleeps=sleeps)
    assert result["outcome"] == "released" and result["exit_code"] == 0, result["reasons"]
    assert sleeps.seconds == [120] and len(ctx.updates()) == 1
    assert R.RELEASE_VALUE_RE.fullmatch(feed(ctx))
    # the same race with the gate off: a plain failure, no write
    grids = Grids(running=1)
    ctx = T.setup(mp, tmp_path / "off", feed=T.W0_VETO, grids=grids)
    arm(mp, settle=False)
    result = release(ctx, sleeps=Sleeps())
    assert result["outcome"] == "not_accepted" and ctx.updates() == [] and grids.calls == 1


def case_r6_no_settle_for_any_other_failure(mp, tmp_path):
    def variant(text):
        def build():
            sources, portfolio, top10 = (copy.deepcopy(list(item)) for item in T.current_publication_grids())
            top10[1][1] = text
            return sources, portfolio, top10
        return build
    ok_line = T.current_publication_grids()[2][1][1]
    cases = [
        dict(coverage="failed", grids=Grids(running=99)),                       # coverage gate
        dict(grids=variant(ok_line.replace("status: ok", "status: error"))),     # ERROR
        dict(grids=variant(ok_line.replace("status: ok", "status: running_x"))),  # not RUNNING
        dict(grids=variant(re.sub(r" \| source readiness VERIFIED \(run \d+\)", "", ok_line))),
        dict(grids=variant(ok_line.replace("2026-07-31", "2026-07-30"))),        # stale board
        dict(grids=variant("Last run 2026-07-31 21:05:00")),                    # no status at all
    ]
    for index, extra in enumerate(cases):
        for settle in (True, False):
            ctx = T.setup(mp, tmp_path / ("c%d%d" % (index, settle)), feed=T.live_release_value(),
                          **extra)
            arm(mp, settle=settle)
            sleeps = Sleeps()
            result = release(ctx, sleeps=sleeps)
            assert sleeps.seconds == [] and "render_settle" not in result, (index, settle)
            assert result["outcome"] == "not_accepted", (index, settle, result["outcome"])
            if settle:
                on = result
            else:
                off = result
        assert stable(on) == stable(off), index


def case_r7_writer_or_probe_fault_during_the_settle_defers(mp, tmp_path):
    busy = [{"workflow": "daily_sync.yml", "id": 90003, "event": "schedule", "status": "queued"}]
    for answer, reason in ((busy, "production_run_in_flight"),
                           (RuntimeError("GitHub API 502"), "probe_unavailable")):
        live = T.live_release_value()
        ctx = T.setup(mp, tmp_path / reason, feed=live, grids=Grids(running=1))
        ctx.probe_results.extend([[], answer])     # step 5 clear, post-settle busy / fault
        arm(mp)
        result = release(ctx, sleeps=Sleeps())
        assert result["exit_code"] == 1 and result["outcome"] == "deferred", reason
        assert result["reasons"] == [reason] and result["deferred"] is True
        assert ctx.updates() == [] and feed(ctx) == live


def case_r8_audit_fault_on_the_re_run_is_fatal_without_writes(mp, tmp_path):
    live = T.live_release_value()
    ctx = T.setup(mp, tmp_path, feed=live, grids=Grids(running=1))
    arm(mp)
    calls = {"n": 0}
    real_run = final.run

    async def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("Sheets read timed out")
        return await real_run(*args, **kwargs)
    mp.setattr(final, "run", flaky)
    result = release(ctx, sleeps=Sleeps())
    assert result["exit_code"] == 3 and result["outcome"] == "fatal"
    assert "finalize_error" in result["reasons"] and ctx.updates() == [] and feed(ctx) == live


def case_r9_settle_candidates_is_exact(mp, tmp_path):
    def result(findings, codes=(0, 2)):
        return {"audit_exit_codes": list(codes), "audits": {"decision": {"findings": findings}}}
    run_t10 = {"severity": "FAIL", "code": "T10_STATUS_NOT_OK", "surface": "Top_10_Investments",
               "message": "Embedded status is RUNNING, not OK."}
    run_pf = dict(run_t10, code="PF_STATUS_NOT_OK", surface="Portfolio_Decision")
    assert R.settle_candidates(result([run_t10])) == ["Top_10_Investments"]
    assert R.settle_candidates(result([run_pf, run_t10, run_t10])) == ["Portfolio_Decision",
                                                                       "Top_10_Investments"]
    for findings in ([dict(run_t10, message="Embedded status is ERROR, not OK.")],
                     [dict(run_t10, message="Embedded status is unknown, not OK.")],
                     [dict(run_t10, surface="Portfolio_Decision")],
                     [dict(run_t10, code="T10_RUN_STALE")],
                     ["not-a-dict"], []):
        assert R.settle_candidates(result(findings)) == [], findings
    for codes in ((1, 2), (2, 2), (3, 3), ("0", 2), (0,), ()):
        assert R.settle_candidates(result([run_t10], codes)) == [], codes
    for bad in (None, [], {}, {"audit_exit_codes": [0, 2]},
                {"audit_exit_codes": [0, 2], "audits": {"decision": {"findings": None}}}):
        assert R.settle_candidates(bad) == [], bad
    # the message is the strict audit's own wording for a RUNNING status line
    from scripts import audit_decision_surface_freshness as F
    assert F.parse_surface_status(T10_RUNNING)[1] == "RUNNING"
    source = Path(F.__file__).read_text(encoding="utf-8")
    assert 'f"Embedded status is {top10_state or \'unknown\'}, not OK."' in source
    assert 'f"Embedded status is {portfolio_state or \'unknown\'}, not OK."' in source
    gs = (ROOT / "apps_script" / "16_Decision_Top10.gs").read_text(encoding="utf-8")
    assert "statusCell.setValue(dt10StatusLine_('running\\u2026', ''));" in gs
    pd = (ROOT / "apps_script" / "10_My_Portfolio.gs").read_text(encoding="utf-8")
    assert "statusCell.setValue(pdStatusLine_('running\\u2026', ''));" in pd


def case_r10_gate_vocabulary(mp, tmp_path):
    for word, armed in (("1", True), ("true", True), ("YES", True), (" on ", True),
                        ("", False), ("0", False), ("off", False), ("enabled", False),
                        ("observe", False), (None, False)):
        if word is None:
            mp.delenv("TFB_RELEASE_RENDER_SETTLE", raising=False)
        else:
            mp.setenv("TFB_RELEASE_RENDER_SETTLE", word)
        assert R.render_settle_enabled() is armed, word
    assert R.RENDER_SETTLE_S == 120 and R.RENDER_SETTLE_MAX == 2


def _base_module(tmp_path):
    try:
        text = subprocess.run(["git", "show", BASE_SHA + ":scripts/release_decision_feed.py"],
                              cwd=ROOT, capture_output=True, text=True, check=True, timeout=30).stdout
    except Exception as exc:  # noqa: BLE001 - shallow checkout or no git
        pytest.skip("base copy unavailable: " + type(exc).__name__)
    if 'SCRIPT_VERSION = "1.2.0"' not in text:
        pytest.skip("base copy is not release_decision_feed 1.2.0")
    path = Path(tmp_path) / "release_base_120.py"
    path.write_text(text, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("release_base_120", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def case_r11_off_path_identity_against_the_untouched_base(mp, tmp_path):
    base = _base_module(tmp_path)
    scenarios = [("live_running", T.live_release_value(), lambda: Grids(running=99)),
                 ("first_running", T.W0_VETO, lambda: Grids(running=99)),
                 ("first_clean", T.W0_VETO, lambda: None),
                 ("live_clean", T.live_release_value(), lambda: None)]
    for name, prior, grids in scenarios:
        results, writes = [], []
        for module in (base, R):
            for settle in ((False,) if module is base else (False, True)):
                ctx = T.setup(mp, tmp_path / (name + module.__name__ + str(settle)),
                              feed=prior, grids=grids())
                arm(mp, settle=settle)
                if module is R and settle and name.endswith("running"):
                    continue          # the gate-on behaviour is R2/R5, not identity
                out = release(ctx, module, sleeps=Sleeps() if module is R else None)
                out.pop("script_version", None)
                results.append(json.dumps(stable(out), sort_keys=True, default=str))
                writes.append(json.dumps(ctx.updates(), sort_keys=True, default=str))
        assert len(set(results)) == 1, name
        assert len(set(writes)) == 1, name


def case_r12_workflow_mapping_and_bounds(mp, tmp_path):
    rel, daily = T.workflow(RELEASE_YML), T.workflow(DAILY_YML)
    job = rel["jobs"]["release-decision-feed"]
    steps = {step["name"]: step for step in job["steps"]}
    run = steps["Release or retract the decision feed"]
    assert run["env"]["TFB_RELEASE_RENDER_SETTLE"] == "${{ vars.TFB_RELEASE_RENDER_SETTLE || '0' }}"
    assert "TFB_RELEASE_RENDER_SETTLE" not in job["env"]
    final_env = daily["jobs"]["verify-final-decision-publication"]["env"]
    assert "TFB_RELEASE_RENDER_SETTLE" not in final_env
    assert "render_settle" not in steps["Publish release summary"]["run"]
    assert R.RENDER_SETTLE_S * R.RENDER_SETTLE_MAX <= 5 * 60 < job["timeout-minutes"] * 60
    text = RELEASE_YML.read_text(encoding="utf-8")
    assert all(ord(ch) < 128 for ch in text)
    script = (ROOT / "scripts" / "release_decision_feed.py").read_text(encoding="utf-8")
    for path_text in (script, Path(__file__).read_text(encoding="utf-8")):
        assert all(ord(ch) < 128 for ch in path_text)
    verifier = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("release_decision_feed", "' + R.SCRIPT_VERSION + '"' in verifier


# --------------------------------------------------------------------------- runner
CASES = [value for _name, value in sorted(
    ((name, value) for name, value in globals().items() if name.startswith("case_r") and callable(value)),
    key=lambda kv: int(re.sub(r"\D", "", kv[0].split("_")[1]) or 0))]


@pytest.mark.parametrize("case", CASES, ids=[case.__name__[5:] for case in CASES])
def test_release_render_settle(case, monkeypatch, tmp_path):
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
    total = passed + failed
    print(("PASS" if failed == 0 else "FAIL") + " %d/%d" % (passed, total)
          + (" (%d skipped)" % skipped if skipped else ""))
    return 0 if failed == 0 and total + skipped == len(CASES) else 1


if __name__ == "__main__":
    sys.exit(_main())
