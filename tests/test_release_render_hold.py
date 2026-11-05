#!/usr/bin/env python3
"""tests/test_release_render_hold.py - release_decision_feed v1.4.0: a live
release whose re-verification fails only because a cockpit is still
mid-render is HELD (exit 1 deferred, zero writes) instead of withdrawn
(TFB_RELEASE_RENDER_HOLD, default off).

WHY (3 Nov 2026)
----------------
The live release of run 41566014627 (09:55 Riyadh) was re-verified while
Top_10_Investments rendered (10:03-10:07, the decision chain): the v1.3.0
settle waited 150 s once, the re-audit still failed - the IBKR capture
watcher was re-rendering Portfolio_Decision (10:08:14-10:08:29) - and the
armed run withdrew the release at 10:08:41 (decision_failed,
release=41593890530); release_window_consumed then refuses that run until
the next sync. H1 below is that shape on the release's own fixtures with
the gate off (unchanged behaviour).

  H1  golden negative, gate off (every off word): still mid-render after the
      settle -> withdrawn, window consumed (v1.3.0 R4, unchanged)
  H2  gate on, same race: held - exit 1 deferred, reason
      render_still_running, zero writes, the live value (expiry, release=
      token) untouched, render_hold evidence
  H3  gate on without the settle gate: held on the first audit (no wait);
      Portfolio_Decision and both cockpits mid-render
  H4  the next re-verification after a hold: the finished board keeps the
      release (already_released, zero writes); a render that died (its
      running stamp past the bound) is withdrawn
  H5  the age bound is exact: a render start 10:00 min old is held, 10:01
      withdrawn; 15 min ahead held, beyond that the audit's own
      T10_RUN_FUTURE withdraws; a clock that moves during the settle is
      judged at the new time
  H6  no hold for any other failure, alone or next to a running cockpit
      (the R6 matrix): identical to the gate off, results and writes
  H7  render_hold is exact on real strict-audit results: every guard
  H8  a FIRST release, a kill, the sticky kill, a sweep and an expired live
      value are judged exactly as with the gate off
  H9  the dry run (TFB_DECISION_RELEASE off) reports the same hold instead
      of would_downgrade
  H10 gate vocabulary and constants (the bound against the .gs LIMIT note)
  H11 off-path identity against the untouched 1.3.0 module (git show
      6900b2ae; SKIP on a shallow checkout); gate-on identity wherever the
      hold does not apply
  H12 workflow: the gate is mapped in the release STEP env only (job env
      parity with the final job holds); the summary prints render_hold only
      when the key exists and is otherwise unchanged
  H13 the 3 Nov cascade: the first audit catches Top_10 mid-render, the
      re-audit after the settle catches Portfolio_Decision mid-render; gate
      off withdraws (what happened), gate on holds and the next
      re-verification keeps the release

Runs standalone (python tests/test_release_render_hold.py -> PASS k/k,
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
import textwrap
import traceback
from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import finalize_decision_publication as final  # noqa: E402
from scripts import release_decision_feed as R  # noqa: E402
from tests import test_release_decision_feed as T  # noqa: E402
from tests import test_release_render_settle as S  # noqa: E402

HOLD_BASE_SHA = "6900b2aef2d495fcb45083735c78b2697cd7c2e4"   # main with release_decision_feed 1.3.0
HOLD_ENV = "TFB_RELEASE_RENDER_HOLD"
OFF_WORDS = (None, "", "0", "off", "observe", "enabled", "hold")
ON_WORDS = ("1", "true", "YES", " on ")
RELEASE_STEP = "Release or retract the decision feed"


# --------------------------------------------------------------------------- fixtures
def running_line(stamp):
    return "Last run " + stamp + " | status: running" + S.ELLIPSIS


class Board:
    """The decision audit's grids per call: calls 1..running see the given B2
    lines (None keeps the finished line), later calls see the finished board."""

    def __init__(self, running=1, *, t10=S.T10_RUNNING, pf=None):
        self.running, self.t10, self.pf, self.calls = running, t10, pf, 0

    def __call__(self):
        self.calls += 1
        sources, portfolio, top10 = (copy.deepcopy(list(item)) for item in T.current_publication_grids())
        if self.calls <= self.running:
            if self.t10 is not None:
                top10[1][1] = self.t10
            if self.pf is not None:
                portfolio[1][1] = self.pf
        return sources, portfolio, top10


def arm(mp, *, hold="1", settle=True, release_gate=True):
    S.arm(mp, settle=settle, release_gate=release_gate)
    if not release_gate:
        mp.delenv("TFB_DECISION_RELEASE", raising=False)
    if hold is None:
        mp.delenv(HOLD_ENV, raising=False)
    else:
        mp.setenv(HOLD_ENV, hold)


def written(ctx):
    return [item["body"]["values"][0][1] for item in ctx.updates()]


def withdrawn(ctx):
    values = written(ctx)
    return (len(values) == 1 and values[0].endswith("| decision_failed | release=" + T.RELEASE_RUN)
            and R.classify_prior(S.feed(ctx), T.SOURCE) == ("refused", "release_window_consumed"))


def strict_result(mp, tmp_path, board):
    """One real strict-audit result (fin.run) on the release's own fixtures."""
    T.setup(mp, tmp_path, feed=T.live_release_value(), grids=board)
    return asyncio.run(final.run("synthetic-sheet", reader=None, registry=None, publish=False,
                                 max_rows=20000, run_id=T.SOURCE))


def codes_of(result):
    return [item["code"] for item in result["audits"]["decision"]["findings"]]


# --------------------------------------------------------------------------- cases
def case_h1_golden_negative_gate_off_withdraws(mp, tmp_path):
    for index, word in enumerate(OFF_WORDS):
        board = Board(running=2)
        ctx = T.setup(mp, tmp_path / ("w%d" % index), feed=T.live_release_value(), grids=board)
        arm(mp, hold=word)
        result = S.release(ctx, sleeps=S.Sleeps())
        assert result["outcome"] == "not_accepted" and result["exit_code"] == 2, word
        assert "render_hold" not in result and result["deferred"] is False, word
        assert [item["attempt"] for item in result["render_settle"]] == [1] and board.calls == 2
        assert withdrawn(ctx), word


def case_h2_gate_on_holds_the_live_release(mp, tmp_path):
    for word in ON_WORDS:
        board = Board(running=2)
        live = T.live_release_value()
        ctx = T.setup(mp, tmp_path / ("w" + word.strip()), feed=live, grids=board)
        arm(mp, hold=word)
        sleeps = S.Sleeps()
        result = S.release(ctx, sleeps=sleeps)
        assert result["exit_code"] == 1 and result["outcome"] == "deferred", (word, result["reasons"])
        assert result["reasons"] == ["render_still_running"] and result["deferred"] is True
        assert result["accepted"] is False and result["released"] is False
        assert result["downgrade"] is None and result["would_downgrade"] is None
        assert result["blocker_published"] is False and result["feed_promoted"] is False
        assert ctx.updates() == [] and S.feed(ctx) == live
        assert R.classify_prior(S.feed(ctx), T.SOURCE) == ("already_released", "same_run_release")
        assert sleeps.seconds == [150] and board.calls == 2 and len(ctx.probe_calls) == 2
        assert result["render_settle"][0]["cockpits"] == ["Top_10_Investments"]
        hold = result["render_hold"]
        assert set(hold) == {"cockpits", "render_started", "age_min", "max_age_min", "reasons"}
        assert hold["cockpits"] == ["Top_10_Investments"] and hold["max_age_min"] == 10
        assert hold["render_started"] == {"Top_10_Investments": "2026-07-31 21:05:00+03:00"}
        assert hold["age_min"] == {"Top_10_Investments": -5.0}      # NOW is 21:00 Riyadh
        assert "decision_findings_present" in hold["reasons"]
        assert set(hold["reasons"]) <= R._RENDER_HOLD_REASONS
        assert "T10_STATUS_NOT_OK" in codes_of(result)


def case_h3_hold_without_the_settle_and_both_cockpits(mp, tmp_path):
    pf_running = running_line("2026-07-31 20:58:00")
    for index, (t10, pf, cockpits) in enumerate((
            (S.T10_RUNNING, None, ["Top_10_Investments"]),
            (None, pf_running, ["Portfolio_Decision"]),
            (S.T10_RUNNING, pf_running, ["Portfolio_Decision", "Top_10_Investments"]))):
        board = Board(running=99, t10=t10, pf=pf)
        live = T.live_release_value()
        ctx = T.setup(mp, tmp_path / ("c%d" % index), feed=live, grids=board)
        arm(mp, settle=False)
        sleeps = S.Sleeps()
        result = S.release(ctx, sleeps=sleeps)
        assert result["outcome"] == "deferred" and result["exit_code"] == 1, cockpits
        assert sleeps.seconds == [] and board.calls == 1 and "render_settle" not in result
        assert ctx.updates() == [] and S.feed(ctx) == live
        assert sorted(result["render_hold"]["cockpits"]) == cockpits
        if pf is not None:
            assert result["render_hold"]["age_min"]["Portfolio_Decision"] == 2.0


def case_h4_the_next_re_verification_after_a_hold(mp, tmp_path):
    live = T.live_release_value()
    # run 1 holds; run 2 reads the finished board and keeps the release as it is
    ctx = T.setup(mp, tmp_path / "kept", feed=live, grids=Board(running=2))
    arm(mp)
    assert S.release(ctx, sleeps=S.Sleeps())["outcome"] == "deferred" and ctx.updates() == []
    ctx.grids = Board(running=0)
    ctx.clock["now"] = T.NOW + timedelta(minutes=12)
    second = S.release(ctx, sleeps=S.Sleeps())
    assert second["outcome"] == "already_released" and second["exit_code"] == 0, second["reasons"]
    assert ctx.updates() == [] and S.feed(ctx) == live            # same value, same expiry
    assert "render_hold" not in second
    # run 1 holds; the render then died (an Apps Script 6-minute kill leaves B2
    # at 'running...'): the next re-verification withdraws, as v1.3.0 does
    ctx = T.setup(mp, tmp_path / "dead", feed=live, grids=Board(running=99))
    arm(mp)
    assert S.release(ctx, sleeps=S.Sleeps())["outcome"] == "deferred" and ctx.updates() == []
    ctx.clock["now"] = T.NOW + timedelta(minutes=16)              # render start 11 min ago
    third = S.release(ctx, sleeps=S.Sleeps())
    assert third["outcome"] == "not_accepted" and "render_hold" not in third
    assert R.settle_candidates(third) == ["Top_10_Investments"]  # only the age refused the hold
    assert withdrawn(ctx)


def case_h5_the_age_bound_is_exact(mp, tmp_path):
    cases = [("2026-07-31 20:50:00", "deferred"),        # render start exactly 10:00 min ago
             ("2026-07-31 20:49:59", "not_accepted"),    # 10:01 -> a dead render
             ("2026-07-31 20:30:00", "not_accepted"),    # 30 min -> a dead render
             ("2026-07-31 21:15:00", "deferred"),        # 15 min ahead: the skew both sides allow
             ("2026-07-31 21:15:01", "not_accepted")]    # beyond: the audit's own T10_RUN_FUTURE
    for index, (stamp, outcome) in enumerate(cases):
        live = T.live_release_value()
        ctx = T.setup(mp, tmp_path / ("a%d" % index), feed=live,
                      grids=Board(running=99, t10=running_line(stamp)))
        arm(mp, settle=False)
        result = S.release(ctx, sleeps=S.Sleeps())
        assert result["outcome"] == outcome, (stamp, result["outcome"], result["reasons"])
        if outcome == "deferred":
            assert ctx.updates() == [] and S.feed(ctx) == live, stamp
        else:
            assert withdrawn(ctx) and "render_hold" not in result, stamp
            if stamp.endswith("21:15:01"):
                assert "T10_RUN_FUTURE" in codes_of(result)
            else:
                assert R.settle_candidates(result) == ["Top_10_Investments"], stamp
    # the clock moves during the 150 s settle: judged at the new time
    for start, outcome in ((12, "deferred"), (13, "not_accepted")):
        ctx = T.setup(mp, tmp_path / ("m%d" % start), feed=T.live_release_value(), grids=Board(running=2))
        ctx.clock["now"] = T.NOW + timedelta(minutes=start)        # render start 7 / 8 min ago

        def move(seconds, ctx=ctx):
            ctx.clock["now"] = ctx.clock["now"] + timedelta(seconds=seconds)
        arm(mp)
        result = S.release(ctx, sleeps=S.Sleeps(move), moving_clock=True)
        assert result["outcome"] == outcome, (start, result["reasons"])
        assert result["render_settle"][0]["wait_s"] == 150
        if outcome == "deferred":
            assert result["render_hold"]["age_min"] == {"Top_10_Investments": 9.5}
        else:
            assert withdrawn(ctx)


def case_h6_no_hold_for_any_other_failure(mp, tmp_path):
    def variant(text):
        def build():
            sources, portfolio, top10 = (copy.deepcopy(list(item)) for item in T.current_publication_grids())
            top10[1][1] = text
            return sources, portfolio, top10
        return build
    ok_line = T.current_publication_grids()[2][1][1]
    pf_line = T.current_publication_grids()[1][1][1]

    def running_t10_with_pf(text):
        def build():
            sources, portfolio, top10 = (copy.deepcopy(list(item)) for item in T.current_publication_grids())
            top10[1][1] = S.T10_RUNNING
            portfolio[1][1] = text
            return sources, portfolio, top10
        return build
    cases = [
        dict(coverage="failed", grids=Board(running=99)),                       # coverage gate
        dict(grids=variant(ok_line.replace("status: ok", "status: error"))),     # ERROR
        dict(grids=variant(ok_line.replace("status: ok", "status: running_x"))),  # not RUNNING
        dict(grids=variant(re.sub(r" \| source readiness VERIFIED \(run \d+\)", "", ok_line))),
        dict(grids=variant(ok_line.replace("2026-07-31", "2026-07-30"))),        # stale board
        dict(grids=variant("Last run 2026-07-31 21:05:00")),                    # no status at all
        dict(grids=running_t10_with_pf(pf_line.replace("status: ok", "status: failed"))),
        dict(grids=running_t10_with_pf(pf_line.replace("2026-07-31", "2026-07-30"))),
        dict(grids=running_t10_with_pf(re.sub(r" \| source readiness VERIFIED \(run \d+\)", "",
                                              pf_line))),
    ]
    busy = [{"workflow": "daily_sync.yml", "id": 90003, "event": "schedule", "status": "queued"}]
    for index, extra in enumerate(cases):
        for writer in (False, True):
            for settle in (True, False):
                outcomes = {}
                for hold in ("1", None):
                    ctx = T.setup(mp, tmp_path / ("c%d%d%d%d" % (index, writer, settle, hold is None)),
                                  feed=T.live_release_value(), **extra)
                    if writer:          # a writer queued right after step 5's probe
                        ctx.probe_results.extend([[], busy])
                    arm(mp, hold=hold, settle=settle)
                    result = S.release(ctx, sleeps=S.Sleeps())
                    assert "render_hold" not in result and result["outcome"] == "not_accepted", (index, hold)
                    outcomes[hold] = (S.stable(result), ctx.updates())
                assert outcomes["1"][0] == outcomes[None][0], (index, writer, settle)
                assert outcomes["1"][1] == outcomes[None][1] and len(outcomes["1"][1]) == 1, (index, writer)


def case_h7_render_hold_is_exact(mp, tmp_path):
    live = T.live_release_value()
    base = strict_result(mp, tmp_path / "t10", Board(running=99))
    assert base["exit_code"] == 2 and base["audit_exit_codes"] == [0, 2]
    assert base["blockers"] == ["decision_failed"]
    expected = {"cockpits": ["Top_10_Investments"],
                "render_started": {"Top_10_Investments": "2026-07-31 21:05:00+03:00"},
                "age_min": {"Top_10_Investments": -5.0}, "max_age_min": 10,
                "reasons": R.accepted(base, T.SOURCE)[1]}
    assert R.render_hold(base, T.SOURCE, live, T.NOW) == expected
    pf = strict_result(mp, tmp_path / "pf", Board(running=99, t10=None,
                                                  pf=running_line("2026-07-31 20:58:00")))
    assert R.render_hold(pf, T.SOURCE, live, T.NOW)["cockpits"] == ["Portfolio_Decision"]
    clean = strict_result(mp, tmp_path / "clean", Board(running=0))
    assert R.accepted(clean, T.SOURCE)[0] is True and R.render_hold(clean, T.SOURCE, live, T.NOW) is None

    def finding(code, message="synthetic"):
        return {"severity": "FAIL", "code": code, "surface": "Top_10_Investments", "message": message}
    mutations = {
        "exit_3": lambda r: r.update(exit_code=3),
        "exit_bool": lambda r: r.update(exit_code=True),
        "coverage_failed": lambda r: r.update(audit_exit_codes=[2, 2]),
        "decision_fatal": lambda r: r.update(audit_exit_codes=[0, 3]),
        "blockers_more": lambda r: r.update(blockers=["coverage_failed", "decision_failed"]),
        "blockers_none": lambda r: r.update(blockers=[]),
        "run_id_other": lambda r: r.update(run_id="90002"),
        "not_strict": lambda r: r["policy"].update(expected_source_run=None),
        "coverage_summary": lambda r: r["audits"]["coverage"]["summary"].update(exit_code=1),
        "stamp_missing": lambda r: r["audits"]["decision"].update(top10_run_riyadh=None),
        "stamp_t_form": lambda r: r["audits"]["decision"].update(top10_run_riyadh="2026-07-31T21:05:00"),
        "stamp_old": lambda r: r["audits"]["decision"].update(top10_run_riyadh="2026-07-31 20:49:59"),
        "stamp_future": lambda r: r["audits"]["decision"].update(top10_run_riyadh="2026-07-31 21:15:01"),
        "extra_finding": lambda r: r["audits"]["decision"]["findings"].append(finding("T10_RUN_STALE")),
        "not_running": lambda r: [item.update(message="Embedded status is FAILED, not OK.")
                                  for item in r["audits"]["decision"]["findings"]
                                  if item["code"] == "T10_STATUS_NOT_OK"],
    }
    for name, mutate in mutations.items():
        item = copy.deepcopy(base)
        mutate(item)
        assert R.render_hold(item, T.SOURCE, live, T.NOW) is None, name
    # the prior must be the LIVE, well-formed, unexpired release of SOURCE
    later = T.ANCHOR + timedelta(minutes=481)
    fresh = copy.deepcopy(base)
    fresh["audits"]["decision"]["top10_run_riyadh"] = (later - timedelta(minutes=1)).astimezone(
        R.RIYADH).strftime("%Y-%m-%d %H:%M:%S")
    assert R.render_hold(fresh, T.SOURCE, live, later) is None                   # expired
    relive = T.live_release_value(anchor=later - timedelta(minutes=60))
    assert R.render_hold(fresh, T.SOURCE, relive, later) is not None             # control
    for prior in (T.live_release_value(run="90002"), live.replace("ttl_min=480", "ttl_min=479"),
                  T.W0_VETO, "", None, 90001):
        assert R.render_hold(base, T.SOURCE, prior, T.NOW) is None, prior
    for bad in (None, [], "x", {}):
        assert R.render_hold(bad, T.SOURCE, live, T.NOW) is None, bad
    assert R.render_hold(base, 90001, live, T.NOW) is None
    assert R.render_hold(base, "90002", live, T.NOW) is None
    assert R.render_hold(base, T.SOURCE, live, T.NOW.replace(tzinfo=None)) is None
    assert R.render_hold(base, T.SOURCE, live, "2026-07-31 21:00:00") is None
    snapshot = copy.deepcopy(base)                 # pure: its inputs are never changed
    R.render_hold(base, T.SOURCE, live, T.NOW)
    assert base == snapshot


def case_h8_first_release_kill_sweep_and_expiry_are_unchanged(mp, tmp_path):
    live = T.live_release_value()
    later = T.ANCHOR + timedelta(minutes=481)                    # the live value has expired
    scenarios = [
        ("first_running", T.W0_VETO, dict(), None, "not_accepted", 0),
        ("kill_mode", live, dict(mode="kill"), None, "killed", 1),
        ("kill_var", live, dict(), "kill_var", "killed", 1),
        ("sweep_expired", live, dict(mode="sweep"), later, "swept", 1),
        ("release_expired", live, dict(), later, "refused", 1),
    ]
    for name, prior, kwargs, special, outcome, writes in scenarios:
        seen = []
        for hold in ("1", None):
            ctx = T.setup(mp, tmp_path / (name + str(hold is None)), feed=prior, grids=Board(running=99))
            arm(mp, hold=hold)
            if special == "kill_var":
                mp.setenv("TFB_DECISION_RELEASE_KILL", "1")
            elif special is not None:
                ctx.clock["now"] = special
            result = S.release(ctx, sleeps=S.Sleeps(), **kwargs)
            assert result["outcome"] == outcome and len(ctx.updates()) == writes, (name, hold, result["outcome"])
            assert "render_hold" not in result, name
            seen.append((S.stable(result), ctx.updates()))
            mp.delenv("TFB_DECISION_RELEASE_KILL", raising=False)
        assert seen[0] == seen[1], name


def case_h9_dry_run_reports_the_hold(mp, tmp_path):
    live = T.live_release_value()
    for hold, outcome in (("1", "deferred"), (None, "not_accepted")):
        ctx = T.setup(mp, tmp_path / str(hold), feed=live, grids=Board(running=99))
        arm(mp, hold=hold, release_gate=False)
        result = S.release(ctx, sleeps=S.Sleeps())
        assert result["dry_run"] is True and result["release_enabled"] is False
        assert result["outcome"] == outcome and ctx.updates() == [] and S.feed(ctx) == live, hold
        if hold:
            assert result["exit_code"] == 1 and result["would_downgrade"] is None
            assert result["render_hold"]["cockpits"] == ["Top_10_Investments"]
        else:
            assert result["exit_code"] == 2 and "render_hold" not in result
            assert result["would_downgrade"].endswith("| decision_failed | release=" + T.RELEASE_RUN)


def case_h10_gate_vocabulary_and_constants(mp, tmp_path):
    for word, armed in (("1", True), ("true", True), ("YES", True), (" on ", True),
                        ("", False), ("0", False), ("off", False), ("enabled", False),
                        ("observe", False), ("hold", False), (None, False)):
        if word is None:
            mp.delenv(HOLD_ENV, raising=False)
        else:
            mp.setenv(HOLD_ENV, word)
        assert R.render_hold_enabled() is armed, word
    assert R.RENDER_HOLD_ENV == HOLD_ENV and R.RENDER_HOLD_MAX_AGE_MIN == 10
    # the bound: the cockpits' own note on the 6-minute Apps Script kill, plus
    # the strict audits' read time; far below the 8-hour decision clock
    gs = (ROOT / "apps_script" / "16_Decision_Top10.gs").read_text(encoding="utf-8")
    assert "hard timeout (6 min)" in gs and "still shows RUNNING" in gs
    assert 6 < R.RENDER_HOLD_MAX_AGE_MIN <= 10 < R.RELEASE_TTL_MIN
    assert R.FUTURE_SKEW_MIN == 15
    # the stamp keys are the decision audit's own payload fields
    from scripts import audit_decision_surface_freshness as F
    assert set(R._RENDER_STAMP_KEYS.values()) <= set(F.DecisionSurfaceReport.__dataclass_fields__)
    assert set(R._RENDER_STAMP_KEYS) == set(R._RENDER_EXPLAINED) == set(R._RENDER_STATUS_CODES.values())
    # held reasons are accepted()'s own words, never identity, strictness,
    # coverage or missing-stamp reasons
    vocabulary = set(R.accepted({}, T.SOURCE)[1])
    assert R._RENDER_HOLD_REASONS <= vocabulary
    assert vocabulary - R._RENDER_HOLD_REASONS == {
        "run_id_not_source", "not_strict", "coverage_summary_not_zero", "cockpit_run_stamps_missing"}
    # 2026-11-03 (#831 merge of main #898-#906): a floor; #831's 1.4.1 carries
    # this hold unchanged (H12 still pins the verifier to R.SCRIPT_VERSION).
    assert tuple(int(part) for part in R.SCRIPT_VERSION.split(".")) >= (1, 4, 0)


def _base_module(tmp_path):
    try:
        text = subprocess.run(["git", "show", HOLD_BASE_SHA + ":scripts/release_decision_feed.py"],
                              cwd=ROOT, capture_output=True, text=True, check=True, timeout=30).stdout
    except Exception as exc:  # noqa: BLE001 - shallow checkout or no git
        pytest.skip("base copy unavailable: " + type(exc).__name__)
    if 'SCRIPT_VERSION = "1.3.0"' not in text:
        pytest.skip("base copy is not release_decision_feed 1.3.0")
    path = Path(tmp_path) / "release_base_130.py"
    path.write_text(text, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("release_base_130", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def case_h11_off_path_identity_against_the_untouched_base(mp, tmp_path):
    base = _base_module(tmp_path)
    stale = T.current_publication_grids()[2][1][1].replace("2026-07-31", "2026-07-30")

    def stale_board():
        sources, portfolio, top10 = (copy.deepcopy(list(item)) for item in T.current_publication_grids())
        top10[1][1] = stale
        return sources, portfolio, top10
    live = T.live_release_value()
    # (name, prior, grids, settle, the hold applies when armed)
    scenarios = [("live_running_settled", live, lambda: Board(running=2), True, True),
                 ("live_running_unsettled", live, lambda: Board(running=99), False, True),
                 ("live_settled_clean", live, lambda: Board(running=1), True, False),
                 ("live_dead_render", live, lambda: Board(running=99, t10=running_line(
                     "2026-07-31 20:30:00")), True, False),
                 ("live_stale_board", live, lambda: stale_board, True, False),
                 ("live_clean", live, lambda: None, True, False),
                 ("first_running", T.W0_VETO, lambda: Board(running=99), True, False),
                 ("first_settled", T.W0_VETO, lambda: Board(running=1), True, False),
                 ("first_clean", T.W0_VETO, lambda: None, False, False)]
    for name, prior, grids, settle, applies in scenarios:
        results, writes = {}, {}
        for label, module, hold in (("base", base, None), ("off", R, None), ("on", R, "1")):
            if label == "on" and applies:
                continue                 # the gate-on behaviour is H2/H3, not identity
            ctx = T.setup(mp, tmp_path / (name + label), feed=prior, grids=grids())
            arm(mp, hold=hold, settle=settle)
            out = S.release(ctx, module, sleeps=S.Sleeps())
            out.pop("script_version", None)
            results[label] = json.dumps(S.stable(out), sort_keys=True, default=str)
            writes[label] = json.dumps(ctx.updates(), sort_keys=True, default=str)
        assert len(set(results.values())) == 1, (name, sorted(results))
        assert len(set(writes.values())) == 1, name


def _render_summary(code, result, work):
    work.mkdir(parents=True, exist_ok=True)
    (work / "release_decision_feed.json").write_text(json.dumps(result), encoding="utf-8")
    out = work / "summary.md"
    body = code.split("python - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    env = {"GITHUB_STEP_SUMMARY": str(out), "TFB_DECISION_RELEASE": "1",
           "TFB_DECISION_RELEASE_KILL": "0", "PATH": "/usr/bin:/bin"}
    subprocess.run([sys.executable, "-c", textwrap.dedent(body)], cwd=work, env=env, check=True, timeout=60)
    return out.read_text(encoding="utf-8")


def case_h12_workflow_mapping_and_summary(mp, tmp_path):
    rel, daily = T.workflow(S.RELEASE_YML), T.workflow(S.DAILY_YML)
    job = rel["jobs"]["release-decision-feed"]
    steps = {step["name"]: step for step in job["steps"]}
    run = steps[RELEASE_STEP]
    assert run["env"][HOLD_ENV] == "${{ vars.TFB_RELEASE_RENDER_HOLD || '0' }}"
    assert run["env"]["TFB_RELEASE_RENDER_SETTLE"] == "${{ vars.TFB_RELEASE_RENDER_SETTLE || '0' }}"
    assert HOLD_ENV not in job["env"]
    assert HOLD_ENV not in daily["jobs"]["verify-final-decision-publication"]["env"]
    for name, step in steps.items():
        if name != RELEASE_STEP:
            assert HOLD_ENV not in json.dumps(step.get("env", {})), name
    # a hold is the script's exit 1: green-warning on the schedule, red on a dispatch
    assert '"$EVENT_NAME" == "schedule" && ( "$code" -eq 1 || "$code" -eq 2 )' in run["run"]
    text = S.RELEASE_YML.read_text(encoding="utf-8")
    assert "# VERSION 1.4.0 (2026-11-03) -- RENDER HOLD MAPPING (release_decision_feed 1.4.0)" in text
    assert "# VERSION 1.3.0 (2026-11-02) -- DEFER WHILE A PRODUCTION SYNC IS RUNNING" in text
    assert all(ord(ch) < 128 for ch in text)
    for path in (ROOT / "scripts" / "release_decision_feed.py", Path(__file__)):
        assert all(ord(ch) < 128 for ch in path.read_text(encoding="utf-8")), path.name
    verifier = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("release_decision_feed", "' + R.SCRIPT_VERSION + '"' in verifier
    summary = steps["Publish release summary"]["run"]
    assert "if 'render_hold' in result:" in summary and "${{" not in summary
    plain = {"mode": "release", "exit_code": 0, "outcome": "released", "reasons": []}
    settled = dict(plain, render_settle=[{"attempt": 1, "cockpits": ["Top_10_Investments"],
                                          "wait_s": 150, "reasons": ["decision_failed"]}])
    held = dict(settled, exit_code=1, outcome="deferred", reasons=["render_still_running"],
                render_hold={"cockpits": ["Top_10_Investments"], "max_age_min": 10,
                             "render_started": {"Top_10_Investments": "2026-11-03 10:07:30+03:00"},
                             "age_min": {"Top_10_Investments": 1.2}, "reasons": ["decision_findings_present"]})
    out_plain = _render_summary(summary, plain, tmp_path / "plain")
    out_settled = _render_summary(summary, settled, tmp_path / "settled")
    out_held = _render_summary(summary, held, tmp_path / "held")
    assert "render_hold" not in out_plain and "render_hold" not in out_settled
    assert out_held.count("| render_hold | ") == 1 and "| render_settle | " in out_held
    try:
        base_text = subprocess.run(["git", "show", HOLD_BASE_SHA + ":.github/workflows/release_decision_feed.yml"],
                                   cwd=ROOT, capture_output=True, text=True, check=True, timeout=30).stdout
    except Exception:  # noqa: BLE001 - shallow checkout: the in-file assertions above still hold
        return
    import yaml
    base_steps = {step["name"]: step for step in yaml.safe_load(base_text)["jobs"]["release-decision-feed"]["steps"]}
    base_summary = base_steps["Publish release summary"]["run"]
    assert _render_summary(base_summary, plain, tmp_path / "base_plain") == out_plain
    assert _render_summary(base_summary, settled, tmp_path / "base_settled") == out_settled
    # every other step is byte-identical to 1.3.0; the release step differs by the one env line
    for name, step in base_steps.items():
        if name not in (RELEASE_STEP, "Publish release summary"):
            assert steps[name] == step, name
    base_run = dict(base_steps[RELEASE_STEP], env=dict(base_steps[RELEASE_STEP]["env"]))
    # 2026-11-03 (#831 merge): #831's TFB_RELEASE_INFLIGHT_DEFER line (workflow
    # 1.4.1) sits next to this one in the same step env.
    mine = dict(run, env={key: value for key, value in run["env"].items()
                          if key not in (HOLD_ENV, "TFB_RELEASE_INFLIGHT_DEFER")})
    assert mine == base_run
    assert yaml.safe_load(base_text)["jobs"]["release-decision-feed"]["env"] == job["env"]


class Cascade:
    """Per-call B2 lines (t10, pf) for the first calls, then the finished board."""

    def __init__(self, *calls):
        self.lines, self.calls = list(calls), 0

    def __call__(self):
        self.calls += 1
        sources, portfolio, top10 = (copy.deepcopy(list(item)) for item in T.current_publication_grids())
        if self.calls <= len(self.lines):
            t10, pf = self.lines[self.calls - 1]
            if t10 is not None:
                top10[1][1] = t10
            if pf is not None:
                portfolio[1][1] = pf
        return sources, portfolio, top10


def case_h13_the_3_nov_cascade(mp, tmp_path):
    t10_running = running_line("2026-07-31 21:03:00")      # the decision chain's Top_10 render
    pf_running = running_line("2026-07-31 21:08:14")       # the capture watcher's Portfolio_Decision
    for hold, outcome in ((None, "not_accepted"), ("1", "deferred")):
        live = T.live_release_value()
        board = Cascade((t10_running, None), (None, pf_running))
        ctx = T.setup(mp, tmp_path / str(hold), feed=live, grids=board)
        ctx.clock["now"] = T.NOW + timedelta(minutes=6)       # 21:06, Top_10 started 3 min ago

        def move(seconds, ctx=ctx):
            ctx.clock["now"] = ctx.clock["now"] + timedelta(seconds=seconds)
        arm(mp, hold=hold)
        result = S.release(ctx, sleeps=S.Sleeps(move), moving_clock=True)
        assert result["render_settle"][0]["cockpits"] == ["Top_10_Investments"]
        assert result["outcome"] == outcome, (hold, result["reasons"])
        assert {"PF_STATUS_NOT_OK", "PF_SOURCE_CONSUMPTION_UNPROVEN"} <= set(codes_of(result))
        if hold is None:                       # what happened on 3 Nov at 10:08:41
            assert withdrawn(ctx)
            continue
        assert ctx.updates() == [] and S.feed(ctx) == live
        assert result["render_hold"]["cockpits"] == ["Portfolio_Decision"]
        assert result["render_hold"]["age_min"] == {"Portfolio_Decision": 0.27}   # 21:08:30 - 21:08:14
        # the next slot re-verifies the finished boards and keeps the release as it is
        ctx.clock["now"] = T.NOW + timedelta(minutes=20)
        again = S.release(ctx, sleeps=S.Sleeps())
        assert again["outcome"] == "already_released" and again["exit_code"] == 0, again["reasons"]
        assert ctx.updates() == [] and S.feed(ctx) == live


# --------------------------------------------------------------------------- runner
CASES = [value for _name, value in sorted(
    ((name, value) for name, value in globals().items() if name.startswith("case_h") and callable(value)),
    key=lambda kv: int(re.sub(r"\D", "", kv[0].split("_")[1]) or 0))]


@pytest.mark.parametrize("case", CASES, ids=[case.__name__[5:] for case in CASES])
def test_release_render_hold(case, monkeypatch, tmp_path):
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
