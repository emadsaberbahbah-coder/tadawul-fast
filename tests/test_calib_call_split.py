"""track_performance v6.59.0 [CALIB-CALL-SPLIT] harness.

The reliability calibration prints, per investability bucket, a directional
win rate (the CALL's outcome) next to a PRICE return averaged over every
matured record, HOLD included. The 7 Nov 2026 clock run (42352240437) read
"BLOCKED win=76.5% roi=-2.14%" and "WARNING: WATCHLIST outperforms
INVESTABLE ... the gate is inverted" over buckets whose call mixes differ
(INVESTABLE decided on 1,246 of 1,249 records, WATCHLIST on 2,410 of
10,978). With TRACK_CALIB_CALL_SPLIT on, the report adds each bucket's buy /
sell / hold split (with distinct-symbol counts), a call-signed ROI and the
INVESTABLE-vs-WATCHLIST note recomputed within buy calls and within sell
calls. Off (default) is v6.53.0.

Runs as a script ("PASS k/k") and under pytest. Zero network.
Controls: TFB_TEST_CALL_SPLIT_TARGET (the tracker under test; the untouched
main copy there is the golden negative), TFB_TEST_TRACK_BASE (a path to the
main a85edfc6 copy instead of `git show`), TFB_TEST_REQUIRE_BASE=1 (a missing
base FAILS the base legs instead of skipping them; set in CI).

  G1  the gate: unset and off words read off, the on words read on
  O1  off: no call_split attribute; the rendered report equals the embedded
      v6.53.0 golden, no log record and no stderr; to_dict, renders and the
      factors string equal the untouched main copy
  O2  on: to_dict, TFB_CALIBRATION_FACTORS and every v6.53.0 line unchanged;
      the block sits right after the discrimination note
  N1  the call-mix artifact: pooled "gate is inverted" while buy calls tie
  N2  the BLOCKED reading (R11): win 77% / price roi -2.16% / call roi +2.16%
  N3  a real inversion inside buy calls stays visible
  N4  edges: thin, breakeven-only, HOLD-only and empty buckets, call words,
      the -0.00 sign, n/a like-for-like lines
  R1  robustness: malformed ROIs, a raising split (one WARNING, one
      'unavailable' line), malformed splits
  W1  ENV parity: both tracker steps map the Variable, default 'off'
  V1  version floor, title, WHY order, verifier pin, the CI step
  V2  against main a85edfc6: ASCII additions, nothing removed, the additions
  X1  the main copy has no split and prints the pooled warning only
"""
from __future__ import annotations

import contextlib
import difflib
import importlib.util
import io
import itertools
import logging
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TRACKER = ROOT / "scripts/track_performance.py"
DELIVERED = Path(os.getenv("TFB_TEST_CALL_SPLIT_TARGET", "").strip() or TRACKER)
BASE_SHA = "a85edfc6099ac3b44ff0a24b36d74f712843a33a"   # main at track_performance 6.53.0
GATE = "TRACK_CALIB_CALL_SPLIT"
MAPPING = "TRACK_CALIB_CALL_SPLIT: ${{ vars.TRACK_CALIB_CALL_SPLIT || 'off' }}"
TAG = "[v6.59.0 CALIB-CALL-SPLIT]"
HELPERS = {"_calib_call_split_enabled", "_calib_call_split", "_fmt_call_family",
           "_calib_call_split_lines"}
OFF_WORDS = (None, "", " ", "0", "off", "OFF", "false", "no", "enable", "enabled",
             "observe", "enforce", "2", "onn", "of", "yes please")
ON_WORDS = ("1", "on", "ON", " true ", "yes", "Yes")
TITLE = "TADAWUL FAST BRIDGE \u2013 ADVANCED PERFORMANCE ANALYTICS ENGINE (v%s)"
UNAVAILABLE = "  Call mix %s: unavailable (the split could not be computed; see the log)" % TAG


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


tp = _load("tp_calib_call_split", DELIVERED)
_BASE = {}


def base():
    """The untouched main copy (a85edfc6, v6.53.0), or skip (or fail when
    TFB_TEST_REQUIRE_BASE=1)."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_TRACK_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/track_performance.py"],
                                      cwd=ROOT, capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "tp_v6530_base.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["path"] = path
        _BASE["mod"] = _load("tp_calib_call_split_base", path) if path else None
    if _BASE["mod"] is None:
        if os.getenv("TFB_TEST_REQUIRE_BASE", "").strip() == "1":
            raise AssertionError("base copy required (TFB_TEST_REQUIRE_BASE=1) but unavailable")
        pytest.skip("base copy unavailable (set TFB_TEST_TRACK_BASE)")
    return _BASE["mod"]


@contextlib.contextmanager
def gate(value):
    old = os.environ.get(GATE)
    try:
        if value is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = value
        yield
    finally:
        if old is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = old


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@contextlib.contextmanager
def captured():
    """Every log record (root, DEBUG) and stderr inside the block."""
    root = logging.getLogger()
    handler, level, err = _Capture(), root.level, io.StringIO()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        with contextlib.redirect_stderr(err):
            yield handler.records, err
    finally:
        root.removeHandler(handler)
        root.setLevel(level)


# ------------------------------------------------------------- fixtures -----
_ids = itertools.count()
NOW = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)


def rec(mod, reco, roi, inv, rel=60.0, symbol=None, day=0):
    R = mod.RecommendationType
    reco = getattr(R, reco) if isinstance(reco, str) and hasattr(R, reco) else reco
    return mod.PerformanceRecord(
        record_id="r", symbol=symbol or "X%d.SR" % next(_ids), horizon=mod.HorizonType("1M"),
        date_recorded=NOW + timedelta(days=day), entry_price=10, entry_recommendation=reco,
        entry_score=50, entry_risk_bucket="LOW", entry_confidence="HIGH",
        origin_tab="Decision_Coverage", target_price=11, target_roi=10,
        target_date=NOW + timedelta(days=30), status=mod.PerformanceStatus.MATURED,
        realized_roi=roi, outcome=None, entry_investability_status=inv,
        entry_forecast_reliability=rel)


def many(mod, n, reco, roi, inv, rel=60.0, syms=None, tag=None):
    """n records; with syms, spread over `syms` symbols recorded on
    consecutive days (distinct keys, as the tracker's daily records)."""
    if not syms:
        return [rec(mod, reco, roi, inv, rel) for _ in range(n)]
    return [rec(mod, reco, roi, inv, rel, symbol="%s%d.SR" % (tag, i % syms), day=i // syms)
            for i in range(n)]


def fixture_artifact(mod):
    """INVESTABLE: 100 buys (20 symbols), half right. WATCHLIST: the same
    buys (20 symbols), 200 sells (28 symbols, 160 right), 300 HOLD.
    BLOCKED: 10 buys (5 right), 90 sells (18 symbols, 72 right), 200 HOLD
    that fell 1%."""
    return (many(mod, 50, "BUY", 2.0, "INVESTABLE", 75.0, 10, "IA") + many(mod, 50, "BUY", -2.0, "INVESTABLE", 75.0, 10, "IB")
            + many(mod, 50, "BUY", 2.0, "WATCHLIST", 60.0, 10, "WA") + many(mod, 50, "BUY", -2.0, "WATCHLIST", 60.0, 10, "WB")
            + many(mod, 160, "SELL", -3.0, "WATCHLIST", 60.0, 20, "WS") + many(mod, 40, "SELL", 3.0, "WATCHLIST", 60.0, 8, "WT")
            + many(mod, 300, "HOLD", 1.0, "WATCHLIST")
            + many(mod, 5, "BUY", 1.0, "BLOCKED", 55.0) + many(mod, 5, "BUY", -1.0, "BLOCKED", 55.0)
            + many(mod, 72, "SELL", -4.0, "BLOCKED", 55.0, 12, "BS") + many(mod, 18, "SELL", 4.0, "BLOCKED", 55.0, 6, "BT")
            + many(mod, 200, "HOLD", -1.0, "BLOCKED", 55.0))


def fixture_inverted(mod):
    """Buy calls only: INVESTABLE 40% right, WATCHLIST 70% right."""
    return (many(mod, 40, "BUY", 1.0, "INVESTABLE") + many(mod, 60, "BUY", -1.0, "INVESTABLE")
            + many(mod, 70, "BUY", 1.0, "WATCHLIST") + many(mod, 30, "BUY", -1.0, "WATCHLIST"))


def calibrate(mod, records):
    return mod.ReliabilityCalibrator().calibrate(records)


def render(mod, rep, verbose=False):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mod.render_calibration_report(rep, verbose=verbose)
    lines = buf.getvalue().splitlines()
    assert lines[1].startswith("RELIABILITY CALIBRATION   ")
    lines[1] = "RELIABILITY CALIBRATION   <ts>"
    return lines


def as_dict(rep):
    d = rep.to_dict()
    d.pop("generated_riyadh")
    return d


def ci(mod, w, n):
    lo, hi = mod.wilson_interval(w, n)
    return "CI[%.0f,%.0f]" % (lo, hi)


# The v6.53.0 report of fixture_artifact (rendered by main a85edfc6).
OFF_GOLDEN = [
    "==================================================================",
    "RELIABILITY CALIBRATION   <ts>",
    "==================================================================",
    "Calibration on 500 decided outcome(s) across 1000 matured record(s).",
    "records: total=1000  matured=1000  decided=500  min_sample=10  shrink_k=20",
    "Brier (if reliability read as P(win)): 0.2396 vs 0.25 baseline -> better than coin-flip",
    "",
    "By investability bucket:",
    "  BLOCKED        n=300 decided=100 win= 77.0% CI[68,84] claim~  55% roi= -1.39% factor*1.33",
    "  INVESTABLE     n=100 decided=100 win= 50.0% CI[40,60] claim~  75% roi= +0.00% factor*0.72",
    "  WATCHLIST      n=600 decided=300 win= 70.0% CI[65,75] claim~  60% roi= -0.10% factor*1.16",
    "  WARNING: WATCHLIST outperforms INVESTABLE in this sample -- the gate is inverted here; "
    "investigate before trusting the split.",
    "",
    "By stated-reliability band:",
    "  0-50           n=0   no decided outcomes yet",
    "  50-70          n=900 decided=400 win= 71.8% CI[67,76] claim~  59% roi= -0.53% factor*1.21",
    "  70-85          n=100 decided=100 win= 50.0% CI[40,60] claim~  75% roi= +0.00% factor*0.72",
    "  85-100         n=0   no decided outcomes yet",
    "  NOT monotonic: stated reliability does not cleanly separate winners here.",
    "",
    "NOTE: Calibration factors are ADVISORY and NOT applied to any recommendation, score, gate, or "
    "stated reliability. They treat mean stated reliability as an implied win-probability (the "
    "engine does not strictly define it that way) and are shrunk toward 1.0 by sample size. Read "
    "them as 'how optimistic was the stated confidence vs outcomes,' not a precise correction. "
    "Applying a haircut is a separate, default-OFF, engine-side phase.",
    "==================================================================",
]


# ----------------------------------------------------------------- G1 -------
@pytest.mark.parametrize("word", OFF_WORDS)
def test_g1_off_words(word):
    with gate(word):
        assert tp._calib_call_split_enabled() is False


@pytest.mark.parametrize("word", ON_WORDS)
def test_g1_on_words(word):
    with gate(word):
        assert tp._calib_call_split_enabled() is True


# ----------------------------------------------------------------- O1 -------
@pytest.mark.parametrize("word", (None, "off", "enable", "observe"))
def test_o1_off_is_v6530(word):
    with gate(word), captured() as (records, err):
        rep = calibrate(tp, fixture_artifact(tp))
        lines = render(tp, rep)
        verbose = render(tp, rep, verbose=True)
    assert not hasattr(rep, "call_split")
    assert lines == OFF_GOLDEN
    assert verbose[:len(OFF_GOLDEN) - 2] == OFF_GOLDEN[:-2]     # verbose adds tiers after the bands
    assert records == [] and err.getvalue() == ""                # no log record, no stderr


def test_o1_off_equals_main_copy():
    b = base()
    for word in (None, "off", "enable"):
        with gate(word):
            with captured() as (rec_new, err_new):
                got = calibrate(tp, fixture_artifact(tp))
                got_lines = [render(tp, got), render(tp, got, verbose=True)]
            with captured() as (rec_old, err_old):
                want = calibrate(b, fixture_artifact(b))
                want_lines = [render(b, want), render(b, want, verbose=True)]
            assert as_dict(got) == as_dict(want)
            assert got_lines == want_lines and got_lines[0] == OFF_GOLDEN
            assert [(r.name, r.levelno, r.getMessage()) for r in rec_new] == \
                   [(r.name, r.levelno, r.getMessage()) for r in rec_old]
            assert err_new.getvalue() == err_old.getvalue()
            got2, want2 = calibrate(tp, fixture_inverted(tp)), calibrate(b, fixture_inverted(b))
            assert as_dict(got2) == as_dict(want2) and render(tp, got2) == render(b, want2)
            assert tp._calibration_env_string(got) == b._calibration_env_string(want)


# ----------------------------------------------------------------- O2 -------
def test_o2_on_adds_lines_only():
    records = fixture_artifact(tp)
    with gate(None):
        off = calibrate(tp, records)
    with gate("on"), captured() as (logs, err):
        on = calibrate(tp, records)
        on_lines = render(tp, on)
    assert logs == [] and err.getvalue() == ""
    assert isinstance(on.call_split, dict) and on.call_split["buckets"]
    assert as_dict(on) == as_dict(off)                                   # JSON export unchanged
    assert "call_split" not in on.to_dict()
    assert tp._calibration_env_string(on) == tp._calibration_env_string(off)
    off_lines = render(tp, off)
    note = off_lines.index("  " + off.discrimination_note)
    block = tp._calib_call_split_lines(on.call_split)
    assert block and on_lines == off_lines[:note + 1] + block + off_lines[note + 1:]
    assert all(ln.isascii() for ln in block)
    assert [b.to_dict() for b in on.by_investability] == [b.to_dict() for b in off.by_investability]


# ----------------------------------------------------------------- N1 -------
def test_n1_pooled_inversion_is_a_call_mix_artifact():
    with gate("on"):
        rep = calibrate(tp, fixture_artifact(tp))
    assert rep.discrimination_note.startswith("WARNING: WATCHLIST outperforms INVESTABLE")
    split = rep.call_split
    rows = {row["name"]: row for row in split["buckets"]}
    assert list(rows) == ["BLOCKED", "INVESTABLE", "WATCHLIST"]
    w = rows["WATCHLIST"]
    assert (w["buy"].n, w["buy"].decided, w["buy"].wins, w["buy"].realized_win_rate) == (100, 100, 50, 50.0)
    assert (w["sell"].n, w["sell"].decided, w["sell"].wins, w["sell"].realized_win_rate) == (200, 200, 160, 80.0)
    assert (w["hold"], w["n"], w["directional_n"], w["buy_symbols"], w["sell_symbols"]) == (300, 600, 300, 20, 28)
    # call roi: buys 0, sells 160 * 3 - 40 * 3 = 360 -> 360 / 300; price roi -360 / 300
    assert (w["call_roi"], w["price_roi"]) == (1.2, -1.2)
    i = rows["INVESTABLE"]
    assert (i["buy"].decided, i["buy"].realized_win_rate, i["sell"].n, i["hold"], i["buy_symbols"]) == (100, 50.0, 0, 0, 20)
    assert (i["call_roi"], i["price_roi"]) == (0.0, 0.0)
    assert split["like_for_like"]["buy"].startswith(
        "INVESTABLE vs WATCHLIST not yet statistically distinguishable")
    assert split["like_for_like"]["sell"] == (
        "n/a: INVESTABLE holds no sell calls in this sample (INVESTABLE n=0, WATCHLIST n=200)")
    lines = tp._calib_call_split_lines(split)
    assert lines[0] == "  Call mix %s: win = the call was right (a buy rose, a sell fell);" % TAG
    assert len(lines) == 6 + 3 + 2
    assert "    WATCHLIST      buy n=100 (20 sym) decided=100 win=50.0%% %s | sell n=200 (28 sym) " \
           "decided=200 win=80.0%% %s | hold n=300 | call roi=+1.20%% price roi=-1.20%%" % (
               ci(tp, 50, 100), ci(tp, 160, 200)) in lines
    assert "    INVESTABLE     buy n=100 (20 sym) decided=100 win=50.0%% %s | sell n=0 | hold n=0 | " \
           "call roi=+0.00%% price roi=+0.00%%" % ci(tp, 50, 100) in lines
    assert lines[-2] == "  Like-for-like, buy calls only: " + split["like_for_like"]["buy"]
    assert lines[-1] == "  Like-for-like, sell calls only: " + split["like_for_like"]["sell"]


# ----------------------------------------------------------------- N2 -------
def test_n2_blocked_reads_one_fact_two_ways():
    with gate("1"):
        rep = calibrate(tp, fixture_artifact(tp))
    pooled = {b.name: b for b in rep.by_investability}["BLOCKED"]
    assert (pooled.realized_win_rate, pooled.mean_realized_roi) == (77.0, -1.39)   # the R11 shape
    row = {r["name"]: r for r in rep.call_split["buckets"]}["BLOCKED"]
    assert (row["buy"].decided, row["buy"].realized_win_rate, row["buy_symbols"]) == (10, 50.0, 10)
    assert (row["sell"].decided, row["sell"].wins, row["sell"].realized_win_rate, row["sell_symbols"]) == (90, 72, 80.0, 18)
    assert row["hold"] == 200
    # sells: 72 * 4 - 18 * 4 = 216 for the calls; the prices fell by the same
    assert (row["call_roi"], row["price_roi"]) == (2.16, -2.16)
    assert ("    BLOCKED        buy n=10 (10 sym) decided=10 win=50.0%% %s | sell n=90 (18 sym) decided=90 "
            "win=80.0%% %s | hold n=200 | call roi=+2.16%% price roi=-2.16%%" % (ci(tp, 5, 10), ci(tp, 72, 90))
            ) in tp._calib_call_split_lines(rep.call_split)


# ----------------------------------------------------------------- N3 -------
def test_n3_a_real_inversion_stays_visible():
    with gate("yes"):
        rep = calibrate(tp, fixture_inverted(tp))
    assert rep.discrimination_note.startswith("WARNING: WATCHLIST outperforms INVESTABLE")
    assert rep.call_split["like_for_like"]["buy"] == rep.discrimination_note
    assert rep.call_split["like_for_like"]["sell"] == (
        "n/a: INVESTABLE and WATCHLIST hold no sell calls in this sample (INVESTABLE n=0, WATCHLIST n=0)")
    rows = {r["name"]: r for r in rep.call_split["buckets"]}
    assert (rows["INVESTABLE"]["buy"].realized_win_rate, rows["WATCHLIST"]["buy"].realized_win_rate) == (40.0, 70.0)
    assert (rows["INVESTABLE"]["call_roi"], rows["WATCHLIST"]["call_roi"]) == (-0.2, 0.4)


# ----------------------------------------------------------------- N4 -------
def test_n4_edges():
    recs = (many(tp, 4, "BUY", 1.0, "INVESTABLE") + many(tp, 3, "HOLD", 2.0, "WATCHLIST")
            + many(tp, 2, "SELL", -1.0, "") + [rec(tp, "STRONG_SELL", 1.0, "WATCHLIST"),
                                                 rec(tp, "ACCUMULATE", -1.0, "WATCHLIST"),
                                                 rec(tp, "AVOID", -2.0, "WATCHLIST")])
    with gate("on"):
        rep = calibrate(tp, recs)
    rows = {r["name"]: r for r in rep.call_split["buckets"]}
    assert list(rows) == ["INVESTABLE", "UNSPECIFIED", "WATCHLIST"]
    w = rows["WATCHLIST"]
    # STRONG_SELL and AVOID are sells, ACCUMULATE a buy, HOLD neutral
    assert (w["buy"].n, w["sell"].n, w["hold"], w["n"]) == (1, 2, 3, 6)
    assert (w["sell"].wins, w["sell"].losses, w["buy"].losses) == (1, 1, 1)
    assert w["call_roi"] == round((-1.0 + 1.0 * -1 + -2.0 * -1) / 3, 2)
    lines = tp._calib_call_split_lines(rep.call_split)
    thin = [ln for ln in lines if ln.startswith("    INVESTABLE")][0]
    assert thin == "    INVESTABLE     buy n=4 (4 sym) decided=4 win=(thin) %s | sell n=0 | hold n=0 | " \
                   "call roi=+1.00%% price roi=+1.00%%" % ci(tp, 4, 4)
    cal = tp.ReliabilityCalibrator()
    # call words the record parser may carry as text: EXIT / TRIM are sells,
    # an unknown word or a blank is hold (as _reco_trade_sign has them)
    words = [rec(tp, "EXIT", -2.0, "WATCHLIST"), rec(tp, "TRIM", 1.0, "WATCHLIST"),
             rec(tp, "NONSENSE", 5.0, "WATCHLIST"), rec(tp, "", 5.0, "WATCHLIST")]
    (row,) = tp._calib_call_split(cal, {"WATCHLIST": words})["buckets"]
    assert (row["buy"].n, row["sell"].n, row["hold"], row["call_roi"]) == (0, 2, 2, 0.5)
    # breakeven-only family: records but nothing decided
    flat = tp._calib_call_split(cal, {"WATCHLIST": many(tp, 5, "BUY", 0.0, "WATCHLIST", syms=2, tag="FL")})
    assert "    WATCHLIST      buy n=5 (2 sym) decided=0 | sell n=0 | hold n=0 | " \
           "call roi=+0.00% price roi=+0.00%" in tp._calib_call_split_lines(flat)
    # a mean that rounds to zero prints +0.00, never -0.00
    tiny = tp._calib_call_split(cal, {"WATCHLIST": [rec(tp, "SELL", 0.004, "WATCHLIST"),
                                                    rec(tp, "SELL", 0.0, "WATCHLIST")]})
    assert tiny["buckets"][0]["call_roi"] == 0.0 and str(tiny["buckets"][0]["call_roi"]) == "0.0"
    assert any(ln.endswith("call roi=+0.00% price roi=+0.00%") for ln in tp._calib_call_split_lines(tiny))
    hold_only = tp._calib_call_split(cal, {"WATCHLIST": many(tp, 3, "HOLD", 1.0, "WATCHLIST")})
    (row,) = hold_only["buckets"]
    assert (row["hold"], row["directional_n"], row["call_roi"], row["price_roi"]) == (3, 0, None, None)
    assert "    WATCHLIST      buy n=0 | sell n=0 | hold n=3 | call roi=n/a" in tp._calib_call_split_lines(hold_only)
    none_both = "n/a: INVESTABLE and WATCHLIST hold no %s calls in this sample (INVESTABLE n=0, WATCHLIST n=0)"
    assert hold_only["like_for_like"] == {"buy": none_both % "buy", "sell": none_both % "sell"}
    with gate("on"):
        empty = calibrate(tp, [])
    assert empty.call_split == {"buckets": [], "like_for_like": hold_only["like_for_like"]}
    assert tp._calib_call_split_lines(empty.call_split) == []
    # both sides present in the family: the calibrator's own note
    both = tp._calib_call_split(cal, {"INVESTABLE": many(tp, 3, "BUY", 1.0, "INVESTABLE"),
                                      "WATCHLIST": many(tp, 3, "BUY", 1.0, "WATCHLIST")})
    assert both["like_for_like"]["buy"] == (
        "INVESTABLE/WATCHLIST discrimination: insufficient sample (INVESTABLE decided=3, WATCHLIST decided=3).")


# ----------------------------------------------------------------- R1 -------
def test_r1_robustness():
    cal = tp.ReliabilityCalibrator()
    odd = [rec(tp, "BUY", None, "WATCHLIST"), rec(tp, "SELL", float("nan"), "WATCHLIST"),
           rec(tp, "SELL", "x", "WATCHLIST"), rec(tp, "BUY", float("inf"), "WATCHLIST"),
           rec(tp, "BUY", 3.0, "WATCHLIST")]
    split = tp._calib_call_split(cal, {"WATCHLIST": odd})
    (row,) = split["buckets"]
    assert (row["buy"].n, row["sell"].n, row["directional_n"], row["call_roi"]) == (3, 2, 1, 3.0)

    class Broken:
        def _bucket(self, *a):
            raise RuntimeError("boom")
    with captured() as (logs, err):
        assert tp._calib_call_split(Broken(), {"WATCHLIST": odd}) == {}
    assert [(r.levelno, r.getMessage()) for r in logs] == [
        (logging.WARNING, "%s call split unavailable: RuntimeError" % TAG)]
    assert tp._calib_call_split(cal, None)["buckets"] == []
    for junk in (None, [], "x", 0):
        assert tp._calib_call_split_lines(junk) == []
    for broken in ({}, {"buckets": None}, {"buckets": [{"name": "X"}]}, {"buckets": "x"}):
        assert tp._calib_call_split_lines(broken) == [UNAVAILABLE]
    assert tp._calib_call_split_lines({"buckets": []}) == []
    # a split that RAISES inside calibrate(): one WARNING, call_split {},
    # the off report plus one 'unavailable' line, never an exception
    old = tp._calib_call_split

    def raising(*a):
        raise ValueError("boom")
    try:
        tp._calib_call_split = raising
        with gate("on"), captured() as (logs, err):
            rep = calibrate(tp, fixture_inverted(tp))
            lines = render(tp, rep)
    finally:
        tp._calib_call_split = old
    assert rep.call_split == {}
    assert [(r.levelno, r.getMessage()) for r in logs] == [
        (logging.WARNING, "%s call split unavailable: ValueError" % TAG)]
    with gate(None):
        off = calibrate(tp, fixture_inverted(tp))
    off_lines = render(tp, off)
    note = off_lines.index("  " + off.discrimination_note)
    assert lines == off_lines[:note + 1] + [UNAVAILABLE] + off_lines[note + 1:]


# ----------------------------------------------------------------- W1 -------
def _env_of(path, pred):
    import yaml
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    hits = []
    for job in (doc.get("jobs") or {}).values():
        for step in job.get("steps") or []:
            if pred(step):
                hits.append(step.get("env") or {})
    return hits


def test_w1_env_parity():
    want = "${{ vars.TRACK_CALIB_CALL_SPLIT || 'off' }}"
    clock = ROOT / ".github/workflows/track_performance.yml"
    daily = ROOT / ".github/workflows/daily_sync.yml"
    for path in (clock, daily):
        text = path.read_text(encoding="utf-8")
        assert text.count(GATE + ":") == 1 and text.count(MAPPING) == 1, path.name
    runs_tracker = lambda s: "scripts/track_performance.py" in str(s.get("run") or "")  # noqa: E731
    (clock_env,) = _env_of(clock, lambda s: "Run performance clock" in str(s.get("name") or ""))
    assert clock_env[GATE] == want
    daily_envs = [e for e in _env_of(daily, runs_tracker) if "TRACK_DEDUP_SIGNAL_HISTORY" in e]
    assert len(daily_envs) == 1 and daily_envs[0][GATE] == want
    clock_text = clock.read_text(encoding="utf-8")       # present and above 1.0.7 (newest first)
    assert clock_text.index("# VERSION 1.0.13 (2026-11-07)") < clock_text.index("# VERSION 1.0.7 (2026-11-02)")
    src = DELIVERED.read_text(encoding="utf-8")
    assert re.search(r'os\.getenv\("TRACK_CALIB_CALL_SPLIT"\)\s*or\s*"off"', src)


# ----------------------------------------------------------------- V1 -------
def test_v1_version_title_why_pin_and_ci():
    assert tuple(int(x) for x in tp.SCRIPT_VERSION.split(".")[:3]) >= (6, 59, 0), tp.SCRIPT_VERSION
    src = DELIVERED.read_text(encoding="utf-8")
    assert "(v%s)" % tp.SCRIPT_VERSION in src.splitlines()[4]
    v = re.search(r'^SCRIPT_VERSION = "%s"' % re.escape(tp.SCRIPT_VERSION), src, re.M)
    assert v and src.index("# v6.53.0 (2026-11-02)") < src.index("# v6.59.0 (2026-11-07)") < v.start()
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % tp.SCRIPT_VERSION in vd
    import yaml
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))["jobs"]
    steps = [s for s in jobs["lean-unit"]["steps"] if "tests/test_calib_call_split.py" in str(s.get("run") or "")]
    assert len(steps) == 1, steps                                 # exactly one blocking step runs it
    step = steps[0]
    assert "if" not in step and "continue-on-error" not in step
    assert "continue-on-error" not in jobs["lean-unit"]
    assert str((step.get("env") or {}).get("TFB_TEST_REQUIRE_BASE")) == "1"
    run = step["run"].strip().splitlines()
    assert any(BASE_SHA in ln for ln in run) and run[-1].strip() == "python -m pytest -q tests/test_calib_call_split.py"
    assert "|| true" not in step["run"]
    assert tp._CALL_SPLIT_TAG == TAG
    assert Path(__file__).read_text(encoding="utf-8").isascii()      # this harness itself


# ----------------------------------------------------------------- V2 -------
def test_v2_against_main_ascii_nothing_removed():
    base()
    import ast
    old_text = Path(_BASE["path"]).read_text(encoding="utf-8")
    new_text = DELIVERED.read_text(encoding="utf-8")
    old, new = old_text.splitlines(), new_text.splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    fresh = [ln for ln in added if ln != TITLE % tp.SCRIPT_VERSION]
    assert fresh and all(ln.isascii() for ln in fresh), [ln for ln in fresh if not ln.isascii()][:3]
    mine = {TITLE % "6.53.0", 'SCRIPT_VERSION = "6.53.0"', "        return CalibrationReport("}
    assert mine <= set(removed), removed
    # every WHY line main carries is still there, verbatim
    assert not [ln for ln in removed if ln.startswith("# v6.")], removed

    def defs(text):
        return {n.name for n in ast.walk(ast.parse(text))
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    gone = defs(old_text) - defs(new_text)
    assert not gone, gone
    others = re.findall(r"^# v6\.(?:(?:5[4-8]|[6-9]\d)\.\d+|59\.[1-9]\d*) \(", new_text, re.M)
    if tp.SCRIPT_VERSION == "6.59.0" and not others:      # exactly this release
        assert set(removed) == mine, removed
        assert defs(new_text) - defs(old_text) == HELPERS
    else:                                                  # merged with, or followed by, other releases
        assert HELPERS <= defs(new_text) - defs(old_text)


# ----------------------------------------------------------------- X1 -------
def test_x1_main_copy_has_no_split():
    b = base()
    assert not hasattr(b, "_calib_call_split")
    with gate("on"):
        rep = calibrate(b, fixture_artifact(b))
    assert not hasattr(rep, "call_split")
    assert render(b, rep) == OFF_GOLDEN          # main prints the pooled warning only


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = skipped = total = 0
    for name, fn in tests:
        params = [m for m in getattr(fn, "pytestmark", []) if m.name == "parametrize"]
        calls = ([((p,), "%s[%r]" % (name, p)) for p in params[0].args[1]] if params else [((), name)])
        for args, label in calls:
            total += 1
            try:
                fn(*args)
                passed += 1
                print("ok  ", label)
            except pytest.skip.Exception as exc:
                skipped += 1
                print("SKIP", label, "::", exc)
    print(f"PASS {passed}/{total}" + (f" (skipped {skipped})" if skipped else ""))
