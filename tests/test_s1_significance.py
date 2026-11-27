"""run_shadow_scorer 1.9.8 [S1-SIGNIFICANCE] - a PASS says whether its net alpha could be chance.

WHY (full project review of 27 Nov 2026): S1_Gate read PASS, "Tranche 1 may be authorized", on 37 scored days with no
test of chance, and every page that copies the gate repeated it. The scorer now reads the day-by-day net alpha
(challenger minus benchmark, net of cost) on the days both were scored, and on a PASS that is not evidence its 'why'
line says so. Observe only: criteria, statuses and verdict are untouched.

  S1 the net daily return comes from the stored Cum Index (cost in it) on scored days from the evidence epoch
  S2 the test: mean, t, the 20-day floor, the t floor of 2, coverage, evidence
  S3 the gate: off = untouched; observe = verdict and statuses untouched, the 'why' of a PASS that is not evidence
  S4 the token line and the mode reader
  S5 the wiring in main(), the workflow mapping and the rollback
  S6 no script parses the 'why' text; the REAL main() runs under observe in tests/test_s1_criteria_v2_p204.py (C8)
"""
from __future__ import annotations

import copy
import importlib.util
import math
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "scripts" / "run_shadow_scorer.py"


def _load():
    spec = importlib.util.spec_from_file_location("s1_sig", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


s1 = _load()
CH, BE, CP = s1.CHALLENGER, s1.BENCHMARK, s1.CHAMPION


def row(day, basket, ret, idx, note="n=5"):
    return {"date": day, "basket": basket, "daily_return": ret, "cum_index": idx, "note": note}


def series(basket, start_idx, net_returns, first_day=1, month="2026-09"):
    """A seed row, then one scored row per net return (the index carries the cost)."""
    out = [row("%s-%02d" % (month, first_day), basket, None, start_idx, "n=5 seeded")]
    idx = start_idx
    for k, r in enumerate(net_returns, start=first_day + 1):
        idx = round(idx * (1.0 + r / 100.0), 6)
        out.append(row("%s-%02d" % (month, k), basket, r, idx))
    return out


# ----------------------------------------------------------------------------- S1 the net daily return
def test_s1_net_return_is_the_index_move_on_scored_days_only():
    h = [
        row("2026-07-19", CH, None, 100.0, "n=5 seeded"),       # before the epoch: carries the index only
        row("2026-07-20", CH, 0.0, 100.0),                      # the fabricated-flat day, before the epoch: never counts
        row("2026-07-21", CH, 1.0, 100.8),                      # gross +1.0, drag 0.2 -> the index moved +0.8
        row("2026-07-22", CH, None, 100.8, "DAY_EXCLUDED_INFRA fresh=0/5 stale=5"),
        row("2026-07-23", CH, None, 100.8, "DAY_NON_TRADING no-venue-session fresh=0/5"),
        row("2026-07-24", CH, -0.5, 100.296),                   # paired against the carried index
        row("2026-07-24", CH, 9.9, 110.0),                      # a second row of the date: the first one stands
        row("2026-07-21", BE, 0.3, 100.3),                      # another basket is not read
    ]
    got = s1.net_daily_returns(h, CH)
    assert sorted(got) == ["2026-07-21", "2026-07-24"]
    assert got["2026-07-21"] == pytest.approx(0.8) and got["2026-07-24"] == pytest.approx(-0.5)
    # the epoch is the scorer's own (a pre-epoch row can never be evidence)
    assert s1.EVIDENCE_EPOCH.isoformat() == "2026-07-21"
    # compounding the net returns gives back the index move over the scored days
    assert 100.0 * (1 + got["2026-07-21"] / 100) * (1 + got["2026-07-24"] / 100) == pytest.approx(100.296)
    # no base, no return; junk rows are skipped, never raised on
    assert s1.net_daily_returns([row("2026-08-03", CH, 1.0, 101.0)], CH) == {}
    assert s1.net_daily_returns([{"basket": CH}, None and {}, row("2026-08-03", CH, None, 100.0),
                                 {"basket": CH, "date": "2026-08-04", "daily_return": 1.0, "cum_index": "x"},
                                 row("2026-08-05", CH, 2.0, 102.0)], CH) == {"2026-08-05": pytest.approx(2.0)}
    assert s1.net_daily_returns(None, CH) == {} and s1.net_daily_returns([], BE) == {}


# ----------------------------------------------------------------------------- S2 the test
def _t(diffs):
    n = len(diffs)
    m = sum(diffs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in diffs) / (n - 1))
    return m / (sd / math.sqrt(n))


def test_s2_the_test_and_its_floors():
    # 24 paired days: the challenger beats the benchmark by 0.5 / 0.1 points alternating -> a clear edge
    chal = [0.9, 0.5] * 12
    bench = [0.4, 0.4] * 12
    h = series(CH, 100.0, chal) + series(BE, 100.0, bench)
    sig = s1.significance(h, None, 3)
    diffs = [0.5, 0.1] * 12
    assert sig["n"] == 24 and sig["mean_pp"] == pytest.approx(0.3, abs=1e-3)
    assert sig["t"] == pytest.approx(round(_t(diffs), 2), abs=0.02) and sig["t"] > 2
    assert sig == {**sig, "excluded": 3, "coverage": "ok", "reading": "SIGNIFICANT", "evidence": True}
    # the same edge with more days excluded than scored: significant, but not evidence
    thin = s1.significance(h, None, 25)
    assert thin["reading"] == "SIGNIFICANT" and thin["coverage"] == "thin" and thin["evidence"] is False
    assert s1.significance(h, None, 24)["coverage"] == "ok"          # equal is still ok: MORE excluded than scored
    # 19 paired days: too few for a test, whatever the t
    few = s1.significance(series(CH, 100.0, chal[:19]) + series(BE, 100.0, bench[:19]), None, 0)
    assert few["n"] == 19 and few["reading"] == "TOO_FEW_DAYS" and few["evidence"] is False and few["t"] > 2
    assert s1.S1_SIG_MIN_DAYS == 20 and s1.S1_SIG_T == 2.0
    assert s1.significance(series(CH, 100.0, chal[:20]) + series(BE, 100.0, bench[:20]), None, 0)["reading"] == "SIGNIFICANT"
    # a noisy edge: the mean is positive and large, the t is not 2
    noisy_c = [3.0, -2.6] * 12
    noisy = s1.significance(series(CH, 100.0, noisy_c) + series(BE, 100.0, [0.0] * 24), None, 0)
    assert noisy["mean_pp"] == pytest.approx(0.2, abs=1e-3) and 0 < noisy["t"] < 2
    assert noisy["reading"] == "NOT_SIGNIFICANT" and noisy["evidence"] is False
    # a clear NEGATIVE alpha is never 'significant' here: the reading is one-sided (an edge, not a difference)
    neg = s1.significance(series(CH, 100.0, bench) + series(BE, 100.0, chal), None, 0)
    assert neg["t"] < -2 and neg["reading"] == "NOT_SIGNIFICANT" and neg["evidence"] is False
    # only days BOTH baskets were scored are paired
    part = s1.significance(series(CH, 100.0, chal) + series(BE, 100.0, bench[:10]), None, 0)
    assert part["n"] == 10 and part["reading"] == "TOO_FEW_DAYS"
    # no spread, one day, nothing: no t, never a crash
    flat = s1.significance(series(CH, 100.0, bench) + series(BE, 100.0, bench), None, 0)      # the same path: alpha 0 every day
    assert flat["n"] == 24 and flat["t"] is None and flat["mean_pp"] == 0 and flat["reading"] == "NOT_SIGNIFICANT"
    one = s1.significance(series(CH, 100.0, [0.5]) + series(BE, 100.0, [0.2]), None, None)
    assert one == {"n": 1, "mean_pp": pytest.approx(0.3, abs=1e-3), "t": None, "excluded": 0, "coverage": "ok",
                   "reading": "TOO_FEW_DAYS", "evidence": False}
    assert s1.significance([], None, None) == {"n": 0, "mean_pp": None, "t": None, "excluded": 0, "coverage": "ok",
                                              "reading": "TOO_FEW_DAYS", "evidence": False}
    assert s1.significance(None, None, -4)["excluded"] == 0


def test_s2_today_is_part_of_the_evidence():
    """main() scores today before the gate is judged: today's two rows count when both carry a return."""
    chal, bench = [0.9, 0.5] * 10, [0.4, 0.4] * 10                       # 20 days in history, today the 21st
    h = series(CH, 100.0, chal[:19]) + series(BE, 100.0, bench[:19])
    assert s1.significance(h, None, 0)["n"] == 19
    last_c, last_b = h[19]["cum_index"], h[-1]["cum_index"]
    today = [row("2026-09-28", CH, 0.5, round(last_c * 1.005, 6)), row("2026-09-28", BE, 0.4, round(last_b * 1.004, 6))]
    with_today = s1.significance(h, today, 0)
    assert with_today["n"] == 20 and with_today["reading"] == "SIGNIFICANT"
    # an excluded day (no return) adds nothing
    excluded = [row("2026-09-28", CH, None, last_c), row("2026-09-28", BE, None, last_b)]
    assert s1.significance(h, excluded, 1)["n"] == 19
    assert h == series(CH, 100.0, chal[:19]) + series(BE, 100.0, bench[:19])          # the history is not touched


# ----------------------------------------------------------------------------- S3 the gate
PASS_GATE = {"verdict": "PASS", "why": "all six criteria met — Tranche 1 may be authorized",
             "criteria": [{"id": i, "name": "c%d" % i, "status": "PASS", "detail": "d%d" % i} for i in range(1, 7)]}
LIVE = {"n": 37, "mean_pp": 0.1434, "t": 0.47, "excluded": 61, "coverage": "thin", "reading": "NOT_SIGNIFICANT",
        "evidence": False}                                                            # Shadow_History of 26 Nov 2026


def test_s3_a_pass_that_is_not_evidence_says_so():
    g = s1.apply_significance(PASS_GATE, LIVE, "observe")
    assert g is not PASS_GATE and PASS_GATE["why"] == "all six criteria met — Tranche 1 may be authorized"
    assert g["verdict"] == "PASS" and g["criteria"] == PASS_GATE["criteria"]          # nothing is demoted
    assert g["why"] == ("all six criteria met — Tranche 1 may be authorized | significance (observe): daily net alpha "
                        "t=+0.47 over 37 paired scored days, 61 excluded: not yet evidence of an edge; the six "
                        "criteria do not test it")
    assert g["significance"] == LIVE and g["significance"] is not LIVE
    # off: a deep copy, byte for byte the 1.9.7 gate
    off = s1.apply_significance(PASS_GATE, LIVE, "off")
    assert off == PASS_GATE and off is not PASS_GATE and "significance" not in off
    assert s1.apply_significance(PASS_GATE, LIVE, "enforce") == PASS_GATE              # no enforce mode exists
    # too few days
    few = dict(LIVE, n=12, t=3.1, excluded=0, coverage="ok", reading="TOO_FEW_DAYS")
    assert s1.apply_significance(PASS_GATE, few, "observe")["why"].endswith(
        "| significance (observe): only 12 paired scored days (20 needed for a test): not yet evidence of an edge; "
        "the six criteria do not test it")
    # significant, but more days excluded than scored
    thin = dict(LIVE, t=2.4, reading="SIGNIFICANT")
    assert s1.apply_significance(PASS_GATE, thin, "observe")["why"].endswith(
        "| significance (observe): daily net alpha t=+2.40 over 37 paired scored days, 61 excluded: more days "
        "excluded than scored, coverage too thin to rely on")
    # evidence: the 'why' line is left as it is, the reading still rides on the gate
    ev = dict(LIVE, t=2.4, excluded=5, coverage="ok", reading="SIGNIFICANT", evidence=True)
    g_ev = s1.apply_significance(PASS_GATE, ev, "observe")
    assert g_ev["why"] == PASS_GATE["why"] and g_ev["significance"]["evidence"] is True
    assert s1.significance_words(ev) == ("daily net alpha t=+2.40 over 37 paired scored days, 5 excluded: a clear "
                                         "edge on these days")
    assert s1.significance_words(dict(LIVE, t=None, excluded=0)).startswith("daily net alpha t=n/a over 37 paired scored days:")


@pytest.mark.parametrize("verdict,why", [("NOT_DECIDABLE", "criteria 1 still pending"),
                                         ("FAIL", "a criterion is breached — promotion frozen (§15)")])
def test_s3_only_a_pass_is_annotated(verdict, why):
    gate = dict(copy.deepcopy(PASS_GATE), verdict=verdict, why=why)
    g = s1.apply_significance(gate, LIVE, "observe")
    assert g["why"] == why and g["verdict"] == verdict and g["significance"] == LIVE


def test_s3_the_real_gate_keeps_its_six_criteria():
    """evaluate_s1 is not touched: the same PASS, then the reading on top."""
    gate = s1.evaluate_s1(37, [], 4.30, "PASS", True, True, "append-only integrity intact", True,
                          excluded_days=57, calibration_detail="mean |err| 4.97pp vs band 10.00pp over n=2021")
    assert gate["verdict"] == "PASS" and gate["why"] == "all six criteria met — Tranche 1 may be authorized"
    g = s1.apply_significance(gate, LIVE, "observe")
    assert [c["status"] for c in g["criteria"]] == ["PASS"] * 6 and [c["detail"] for c in g["criteria"]] == [
        c["detail"] for c in gate["criteria"]]
    assert g["verdict"] == "PASS" and "not yet evidence of an edge" in g["why"]


# ----------------------------------------------------------------------------- S4 the line and the mode
def test_s4_token_line_and_mode(monkeypatch):
    assert s1.significance_line(LIVE, "observe") == (
        "[S1-SIGNIFICANCE v1.9.8] mode=observe n=37 mean=+0.143pp t=+0.47 excluded=61 coverage=thin "
        "reading=NOT_SIGNIFICANT evidence=no")
    empty = s1.significance([], None, None)
    assert s1.significance_line(empty, "observe") == (
        "[S1-SIGNIFICANCE v1.9.8] mode=observe n=0 mean=n/a t=n/a excluded=0 coverage=ok reading=TOO_FEW_DAYS "
        "evidence=no")
    assert s1.significance_line(dict(LIVE, evidence=True), "observe").endswith("evidence=yes")
    for raw, mode in ((None, "observe"), ("", "observe"), ("observe", "observe"), ("junk", "observe"),
                      ("enforce", "observe"), ("off", "off"), (" OFF ", "off"), ("0", "off"), ("false", "off"),
                      ("No", "off")):
        if raw is None:
            monkeypatch.delenv("TFB_S1_SIGNIFICANCE", raising=False)
        else:
            monkeypatch.setenv("TFB_S1_SIGNIFICANCE", raw)
        assert s1._significance_mode() == mode, raw


# ----------------------------------------------------------------------------- S5 the wiring
def test_s5_wiring_version_and_rollback():
    src = SRC.read_text(encoding="utf-8")
    assert s1.SCRIPT_VERSION == "1.9.8" and "VERSION 1.9.8 (2026-11-27)" in src
    assert "ROLLBACK: repository Variable TFB_S1_SIGNIFICANCE=off (byte-identical to 1.9.7)." in src
    main = src[src.index("\ndef main("):src.index("# SELFTEST ")]
    judged = main.index("gate = evaluate_s1(")
    applied = main.index("gate = apply_significance(gate, _sig, _sig_mode)")
    verdict = main.index('verdict = (f"[S1-GATE v{SCRIPT_VERSION}] {gate[\'verdict\']}')
    written = main.index("write_gate(sh, gate, meta)")
    why_cell = main.index('[gate["why"]],')
    assert judged < applied < verdict < why_cell < written            # the reading is on the gate before any page is built
    assert main.index("gate = evaluate_s1_v2(") < applied             # after criteria v2, so an enforced verdict is read
    assert 'if _sig_mode != "off":' in main and main.count("_sig_line") == 8
    assert 'verdict += f" | {_sig_line}"' in main and '+ (f" | {_sig_line}" if _sig_line else "")' in main
    assert '_rl_details["significance"] = gate.get("significance")' in main
    assert "excluded_days)" in main[main.index("_sig = significance("):applied]     # the gate's own excluded count
    # the reading never reaches a criterion or the verdict rule
    for fn in ("evaluate_s1", "evaluate_s1_v2"):
        body = src[src.index("\ndef %s(" % fn):]
        body = body[:body.index("\ndef ", 5)]
        assert "signif" not in body.lower(), fn
    body = src[src.index("\ndef apply_significance("):src.index("\ndef significance_line(")]
    assert 'g["verdict"] =' not in body and '["status"] =' not in body
    wf = (ROOT / ".github" / "workflows" / "shadow_scorer.yml").read_text(encoding="utf-8")
    assert "TFB_S1_SIGNIFICANCE: ${{ vars.TFB_S1_SIGNIFICANCE || 'observe' }}" in wf and "# VERSION 1.0.4 (2026-11-27)" in wf
    pin = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_shadow_scorer", "1.9.8", "shadow scorer")' in pin


# ----------------------------------------------------------------------------- S6 readers and the end-to-end run
def test_s6_source_has_no_page_reader_of_the_why_text():
    """No other script parses the gate's 'why' text, so a longer line breaks no reader (the pages copy the cell)."""
    hits = []
    for p in list((ROOT / "scripts").glob("*.py")) + list((ROOT / "apps_script").glob("*.gs")) + list((ROOT / "core").rglob("*.py")):
        if p.name == "run_shadow_scorer.py":
            continue
        txt = p.read_text(encoding="utf-8", errors="ignore")
        if "Tranche 1 may be authorized" in txt or "all six criteria met" in txt:
            hits.append(p.name)
    assert hits == []


def test_s6_the_harnesses_run_main_under_the_default():
    """The S-1 harnesses run the REAL main() with the variable unset, i.e. under observe; C8 of the criteria harness
    holds observe against off. Here: they name the delivered version and C8 is in place."""
    for name in ("test_s1_base_policy_p186.py", "test_s1_day_key_p176.py", "test_s1_criteria_v2_p204.py"):
        txt = (ROOT / "tests" / name).read_text(encoding="utf-8")
        assert "v1.9.8]" in txt and "v1.9.7]" not in txt, name
    c8 = (ROOT / "tests" / "test_s1_criteria_v2_p204.py").read_text(encoding="utf-8")
    assert 'print("C8 PASS  significance' in c8 and 'assert strip(go) == gx' in c8 and "TFB_S1_SIGNIFICANCE" in c8
