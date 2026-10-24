"""opportunity_builder 1.27.0 - Top10 data-quality gates (all default OFF).

EVIDENCE: the 2026-10-13 20:37 Riyadh Top_10 export deferred 481 of 500
written rows at Quote Freshness (385 .US prior-session prices while NYSE
traded, 41 European prices older than the venue close, 41 on venues without
a mapped calendar) and rendered six grace-held seats with blank metrics
because none of them was in the capped written audit.

WIRING  W1-W14  the three gates through the real builder entry point
                (build_opportunity_payload) at a pinned clock, 2026-10-13
                17:37 UTC (a Tuesday: NYSE open, LSE and Bursa closed).
NEG     N1-N4   off spellings, never-raise, ASCII, rule not widened.
GOLD    G1-G2   against a base copy of v1.27.2 = main (TFB_TEST_OB_BASE =
                path to `git show df99bcf3:core/analysis/opportunity_builder.py`):
                OFF payload byte-identical; the armed env does nothing on base.

Real exchange_calendars cases SKIP when the package is absent (the lean CI
job installs numpy/pytest/PyYAML only; the whole-suite job runs them).

  pytest -q tests/test_ob_top10_data_quality.py
  or python3 tests/test_ob_top10_data_quality.py      (prints PASS k/k)
"""
import copy
import importlib.util
import json
import os
import re
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.analysis import opportunity_builder as ob  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 10, 13, 17, 37, tzinfo=UTC)
GATES = ("TFB_OPP_VENUE_CAL_EXT", "TFB_OPP_FRESHNESS_ALERT", "TFB_OPP_AUDIT_KEEP")
OB_BASE = os.environ.get("TFB_TEST_OB_BASE", "")
HAS_CAL = (importlib.util.find_spec("exchange_calendars") is not None
           and importlib.util.find_spec("pandas") is not None)
FX = {"USD": 3.75, "GBP": 5.0, "MYR": 0.89, "INR": 0.045, "TWD": 0.117,
      "VND": 0.000148, "CNY": 0.52}


def _clock(now):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is None else now.astimezone(tz)
    return Clock


def _setup(mp, module=ob, now=NOW, arm=()):
    mp.setattr(module, "datetime", _clock(now))
    for name in GATES:
        mp.delenv(name, raising=False)
    mp.setenv("TFB_OPP_ENABLED", "1")
    mp.setenv("TFB_TICKET_FRESHNESS_GATE", "1")
    mp.setenv("TFB_T10_PRICE_XCHECK", "off")
    mp.delenv("TFB_OPP_AUDIT_ROWS_MAX", raising=False)
    for name in arm:
        mp.setenv(name, "1")


def _need_cal():
    if not HAS_CAL:
        pytest.skip("exchange_calendars/pandas not installed (lean job)")


def row(sym, asof, **kw):
    r = {"data_provider": "yahoo", "last_updated_utc": NOW.isoformat(),
         "acquisition_status": "success", "acquisition_provider": "yahoo",
         "acquisition_acquired_at": NOW.isoformat(),
         "acquisition_quote_asof": asof.isoformat(),
         "symbol": sym, "name": "Synthetic " + sym, "sector": "Energy",
         "market": "US", "currency": "USD", "current_price": 100.0,
         "intrinsic_value": 125.0, "forecast_reliability_score": 85.0,
         "data_quality_score": 95.0, "risk_bucket": "Low",
         "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
         "expected_roi_12m": 25.0, "recommendation_detailed": "BUY",
         "investability_status": "INVESTABLE"}
    r.update(kw)
    return r


US_PRIOR = datetime(2026, 10, 12, 20, 0, tzinfo=UTC)       # Mon NYSE close
LSE_MIDDAY = datetime(2026, 10, 13, 12, 20, tzinfo=UTC)    # before 15:30 close
KL_CLOSE = datetime(2026, 10, 13, 9, 0, 30, tzinfo=UTC)    # Bursa 17:00 MYT
NS_CLOSE = datetime(2026, 10, 13, 10, 0, 30, tzinfo=UTC)   # NSE 15:30 IST
SZ_CLOSE = datetime(2026, 10, 13, 7, 0, 30, tzinfo=UTC)    # SZSE/XSHG 15:00 CST
# 2026-10-19 merge with main 1.27.0: v1.26.2 (#796) maps .KL (and 26 more of
# this build's extension suffixes) by DEFAULT, so the pool's "venue without a
# calendar unless TFB_OPP_VENUE_CAL_EXT=1" row is now RELIANCE.NS - one of the
# three suffixes (.AT/.NS/.SZ) that still depend on this build's flag.
# 2026-10-22 merge with main 1.27.2 (#813): .AT is in the default-on map and
# .NS is deliberately fail-closed (XBOM lacks NSE's special sessions), so this
# build keeps only .SZ and the flag's example row is 000001.SZ on XSHG.


def pool():
    return [row("AAA.US", US_PRIOR),
            row("BBB.L", LSE_MIDDAY, currency="GBP", market="LSE"),
            row("000001.SZ", SZ_CLOSE, currency="CNY", market="SZSE"),
            row("CCC.US", NOW - timedelta(minutes=3))]


CRIT = {"board_funding_stage": "research"}


def build(rows=None, criteria=None, module=ob):
    return module.build_opportunity_payload(
        copy.deepcopy(rows if rows is not None else pool()),
        criteria=dict(criteria if criteria is not None else CRIT),
        portfolio={"cash_available_sar": 50000}, fx_rates=dict(FX))


def canon(payload):
    p = copy.deepcopy(payload)
    p.get("meta", {}).pop("generated_at_utc", None)
    text = json.dumps(p, sort_keys=True, default=str)
    return re.sub(r"1\.2[67]\.[0-3]", "<ver>", text)


def by_symbol(payload):
    return {a["symbol"]: a for a in payload["candidates_rows"]}


def assess(sym, asof, **kw):
    cand = ob.normalize_candidate(row(sym, asof, **kw), FX,
                                  ob.make_criteria({"trust_gate_enabled": False}))
    return ob._quote_freshness_assessment(cand)


# ---------------------------------------------------------------- wiring ---
def case_w1_ext_off_resolves_nothing_new(mp, _tmp):
    _setup(mp)
    for sfx in ob._VENUE_CAL_MAP_EXT:
        assert ob._venue_code("X." + sfx) is None, sfx
    base = {s: ob._venue_code("X." + s) for s in ob._VENUE_CAL_MAP}
    mp.setenv("TFB_OPP_VENUE_CAL_EXT", "1")
    for sfx, code in ob._VENUE_CAL_MAP_EXT.items():
        assert ob._venue_code("X." + sfx) == code, sfx
    # the base map always wins; the extension never re-maps a base suffix
    assert {s: ob._venue_code("X." + s) for s in ob._VENUE_CAL_MAP} == base
    assert not set(ob._VENUE_CAL_MAP) & set(ob._VENUE_CAL_MAP_EXT)
    assert ob._venue_code("BRK.B") == "XNYS" and ob._venue_code("AAPL") == "XNYS"


def case_w2_ext_codes_exist_in_pinned_calendars(mp, _tmp):
    _need_cal()
    import exchange_calendars as xc
    names = set(xc.get_calendar_names(include_aliases=True))
    missing = sorted(set(ob._VENUE_CAL_MAP_EXT.values()) - names)
    assert not missing, missing
    req = (ROOT / "requirements.txt").read_text()
    assert "exchange-calendars==4.13.2" in req


def case_w3_ext_certifies_latest_close_only(mp, _tmp):
    _need_cal()
    _setup(mp)
    # .KL is in v1.26.2's default-on map: certified with this build's flag off,
    # and v1.26.2's own kill switch still reverts it.
    ok, cur, det = assess("1295.KL", KL_CLOSE, currency="MYR")
    assert ok and cur.startswith("last close") and det["mode"] == "session_closed"
    mp.setenv("TFB_TICKET_VENUE_CAL_EXT", "0")
    ok, cur, det = assess("1295.KL", KL_CLOSE, currency="MYR")
    assert not ok and "venue calendar unavailable" in cur
    mp.delenv("TFB_TICKET_VENUE_CAL_EXT", raising=False)
    # .SZ is only in this build's extension: unavailable until it is armed
    ok, cur, _ = assess("000001.SZ", SZ_CLOSE, currency="CNY")
    assert not ok and "venue calendar unavailable" in cur
    mp.setenv("TFB_OPP_VENUE_CAL_EXT", "1")
    ok, cur, det = assess("000001.SZ", SZ_CLOSE, currency="CNY")
    assert ok and cur.startswith("last close") and det["mode"] == "session_closed"
    # yesterday's close is still stale once today's close exists
    ok, cur, det = assess("1295.KL", KL_CLOSE - timedelta(days=1), currency="MYR")
    assert not ok and cur.startswith("STALE_PRICE pre-close")


def case_w4_ext_keeps_intraday_rule(mp, _tmp):
    _need_cal()
    taipei_open = datetime(2026, 10, 14, 2, 0, tzinfo=UTC)   # Wed 10:00 CST
    _setup(mp, now=taipei_open, arm=("TFB_OPP_VENUE_CAL_EXT",))
    stamp = {"acquisition_acquired_at": taipei_open.isoformat(),
             "last_updated_utc": taipei_open.isoformat(), "currency": "TWD"}
    ok, cur, det = assess("2330.TW", taipei_open - timedelta(minutes=60), **stamp)
    assert not ok and det["mode"] == "session_open"
    assert cur.startswith("STALE_PRICE intraday")
    ok, cur, _ = assess("2330.TW", taipei_open - timedelta(minutes=5), **stamp)
    assert ok and cur.startswith("live")


def case_w5_alert_off_is_legacy(mp, _tmp):
    _need_cal()
    _setup(mp)
    p = build()
    assert all(a["type"] != "stale_quotes" for a in p["alerts"])
    notes = {n["symbol"]: n["improve_note"] for n in p["near_miss"]}
    assert notes["AAA.US"].startswith("Resolve Quote Freshness (now: STALE_PRICE intraday")


def case_w6_alert_on_explains_timing_display_only(mp, _tmp):
    _need_cal()
    _setup(mp)
    off = build()
    mp.setenv("TFB_OPP_FRESHNESS_ALERT", "1")
    on = build()
    alerts = [a for a in on["alerts"] if a["type"] == "stale_quotes"]
    assert len(alerts) == 1 and alerts[0]["count"] == 3
    text = alerts[0]["required_action"]
    assert "1 prior-session price(s) while the venue trades (.US 1)" in text
    assert ".US closes 20:00 UTC / 23:00 Riyadh" in text
    assert "1 price(s) older than the venue's latest close (.L 1)" in text
    assert "1 on venues without a trading calendar (.SZ 1)" in text
    assert "TFB_OPP_VENUE_CAL_EXT=1" in text and "No gate or ticket changed." in text
    notes = {n["symbol"]: n["improve_note"] for n in on["near_miss"]}
    assert "Re-run after the venue closes (20:00 UTC / 23:00 Riyadh)" in notes["AAA.US"]
    assert "latest close (15:30 UTC / 18:30 Riyadh)" in notes["BBB.L"]
    assert "Arming TFB_OPP_VENUE_CAL_EXT=1" in notes["000001.SZ"]
    # display only: every candidate row, ticket and KPI is unchanged
    assert on["candidates_rows"] == off["candidates_rows"]
    assert on["selected"] == off["selected"] and on["kpis"] == off["kpis"]
    assert [a for a in on["alerts"] if a["type"] != "stale_quotes"] == off["alerts"]
    assert [(n["symbol"], n["failed_gate"], n["current"], n["required"])
            for n in on["near_miss"]] == [
        (n["symbol"], n["failed_gate"], n["current"], n["required"])
        for n in off["near_miss"]]


def case_w7_all_armed_unblocks_only_the_certified_close(mp, _tmp):
    _need_cal()
    _setup(mp, arm=GATES)
    rows = by_symbol(build())
    assert rows["000001.SZ"]["verdict"] == "INVEST"
    assert rows["AAA.US"]["first_fail"]["gate"] == "Quote Freshness"
    assert rows["BBB.L"]["first_fail"]["gate"] == "Quote Freshness"


def case_w8_no_calendar_and_unverified_notes(mp, _tmp):
    _setup(mp, arm=("TFB_OPP_FRESHNESS_ALERT", "TFB_OPP_VENUE_CAL_EXT"))
    mp.setattr(ob, "_venue_state", lambda *_a: None)
    rows = [row("MWG.VN", NOW - timedelta(hours=10), currency="VND", market="HOSE"),
            row("DDD.US", NOW - timedelta(minutes=2), acquisition_status="failed")]
    p = build(rows)
    notes = {n["symbol"]: n["improve_note"] for n in p["near_miss"]}
    assert notes["MWG.VN"].startswith("No trading calendar exists for this venue")
    assert notes["DDD.US"].startswith("The price has no successful acquisition proof")
    text = [a for a in p["alerts"] if a["type"] == "stale_quotes"][0]["required_action"]
    assert "1 without acquisition proof (.US 1)" in text
    assert "TFB_OPP_VENUE_CAL_EXT=1" not in text   # VN is not an extension venue


def _keep_pool():
    rows = [row("KEEP.US", NOW - timedelta(minutes=2), intrinsic_value=101.0,
                expected_roi_12m=1.0)]
    for i in range(6):
        rows.append(row("R%d.US" % i, NOW - timedelta(minutes=2),
                        forecast_reliability_score=40.0 + i))
    return rows


KEEP_CRIT = {"board_funding_stage": "research", "audit_rows_max": 2,
             "near_miss_n": 1, "audit_keep_symbols": ["keep.us", "ABSENT.US"]}


def case_w9_audit_keep_off_drops_the_tail(mp, _tmp):
    _setup(mp)
    mp.setattr(ob, "_venue_state", lambda *_a: None)
    p = build(_keep_pool(), KEEP_CRIT)
    assert len(p["candidates_rows"]) == 2
    assert "KEEP.US" not in by_symbol(p)
    assert "audit_keep" not in p["meta"]


def case_w10_audit_keep_on_writes_the_held_seat(mp, _tmp):
    _setup(mp, arm=("TFB_OPP_AUDIT_KEEP",))
    mp.setattr(ob, "_venue_state", lambda *_a: None)
    p = build(_keep_pool(), KEEP_CRIT)
    rows = by_symbol(p)
    assert "KEEP.US" in rows and rows["KEEP.US"]["reliability"] is not None
    # the cap still holds: the kept seat takes the free slot, not an extra one
    assert len(p["candidates_rows"]) == 2 and "R4.US" not in rows
    assert p["meta"]["audit_keep"] == {"requested": 2, "written": 1,
                                       "missing": ["ABSENT.US"]}
    mp.delenv("TFB_OPP_AUDIT_KEEP")
    off = build(_keep_pool(), KEEP_CRIT)
    # selection, near-miss, KPIs untouched; only the written audit grows
    assert p["selected"] == off["selected"] and p["kpis"] == off["kpis"]
    assert p["near_miss"] == off["near_miss"]


def case_w10b_audit_keep_never_grows_the_cap(mp, _tmp):
    """Review (Codex P2): held seats only fill the room the cap leaves."""
    _setup(mp, arm=("TFB_OPP_AUDIT_KEEP",))
    mp.setattr(ob, "_venue_state", lambda *_a: None)
    rows = _keep_pool() + [row("KEEP%d.US" % i, NOW - timedelta(minutes=2),
                               intrinsic_value=101.0, expected_roi_12m=1.0)
                           for i in (2, 3)]
    crit = dict(KEEP_CRIT, audit_keep_symbols=["KEEP.US", "KEEP2.US", "KEEP3.US"])
    p = build(rows, crit)                       # cap 2, one near-miss row
    assert len(p["candidates_rows"]) == 2
    meta = p["meta"]["audit_keep"]
    assert meta["requested"] == 3 and meta["written"] == 1 and len(meta["missing"]) == 2
    full = build(rows, dict(crit, near_miss_n=2))   # cap already full
    assert len(full["candidates_rows"]) == 2
    assert full["meta"]["audit_keep"]["written"] == 0
    assert sorted(full["meta"]["audit_keep"]["missing"]) == ["KEEP.US", "KEEP2.US", "KEEP3.US"]


def case_w11_keep_list_parsing(mp, _tmp):
    f = ob._audit_keep_symbols
    assert f(None) == [] and f({}) == [] and f({"audit_keep_symbols": 5}) == []
    assert f({"audit_keep_symbols": " aed.br, BBOX.L  indv.us,AED.BR"}) == [
        "AED.BR", "BBOX.L", "INDV.US"]
    assert f({"audit_keep_symbols": ["a", True, None, {"x": 1}, "", "b"]}) == ["A", "B"]
    assert f({"audit_keep_symbols": ["X" * 33, "ok"]}) == ["OK"]
    many = ["S%d" % i for i in range(100)]
    assert f({"audit_keep_symbols": many}) == many[:ob._AUDIT_KEEP_MAX]


def case_w12_criteria_snapshot_unchanged(mp, _tmp):
    _setup(mp, arm=("TFB_OPP_AUDIT_KEEP",))
    mp.setattr(ob, "_venue_state", lambda *_a: None)
    with_key = build(_keep_pool(), KEEP_CRIT)
    crit = dict(KEEP_CRIT)
    crit.pop("audit_keep_symbols")
    without = build(_keep_pool(), crit)
    assert with_key["meta"]["criteria_snapshot"] == without["meta"]["criteria_snapshot"]
    assert "audit_keep_symbols" not in with_key["meta"]["criteria_snapshot"]


def case_w13_route_passes_the_key_through(mp, _tmp):
    try:
        from routes import advanced_analysis as aa
    except Exception as exc:  # noqa: BLE001
        pytest.skip("routes.advanced_analysis not importable here: %s" % exc)
    out = aa._opp_criteria_from_body({"audit_keep_symbols": ["AED.BR"],
                                      "T10: Max Selected": 10})
    assert out.get("audit_keep_symbols") == ["AED.BR"]


def case_w14_verifier_registers_flags_and_pin(mp, _tmp):
    text = (ROOT / "scripts" / "verify_deployment.py").read_text()
    for name in GATES:
        assert "('" + name + "', '0'," in text, name
    assert '"%s", "opportunity builder"' % ob.OPPORTUNITY_BUILDER_VERSION in text
    assert tuple(int(x) for x in ob.OPPORTUNITY_BUILDER_VERSION.split(".")[:3]) >= (1, 27, 1)


# --------------------------------------------------------------- negative ---
def case_n1_off_spellings_are_off(mp, _tmp):
    _setup(mp)
    mp.setattr(ob, "_venue_state", lambda *_a: None)
    ref = canon(build())
    for value in ("0", "false", "", "off", "no", "disabled", " 2 "):
        for name in GATES:
            mp.setenv(name, value)
        assert canon(build()) == ref, value


def case_n2_never_raises(mp, _tmp):
    _setup(mp)
    assert ob._venue_next_close("ZZZ.QQ", NOW) is None

    def boom(_s):
        raise RuntimeError("calendar layer down")
    mp.setattr(ob, "_venue_code", boom)
    assert ob._venue_next_close("AAA.US", NOW) is None
    assert ob._freshness_improve_note("AAA.US", {"gate": "Quote Freshness", "freshness_detail":
                                                 {"mode": "session_open"}}).startswith(
        "Re-qualifies on a fresh quote")
    mp.undo()
    _setup(mp)
    assert ob._utc_riyadh_hm(None) == ""
    assert ob._freshness_alert([]) is None
    assert ob._freshness_alert([{}, {"first_fail": None}]) is None
    a = ob._freshness_alert([{"symbol": None, "first_fail": {"gate": "Quote Freshness"}}])
    assert a["count"] == 1 and "1 without acquisition proof" in a["required_action"]
    assert ob._freshness_improve_note("X.US", None).startswith("The price has no")
    assert ob._freshness_class({"freshness_detail": {"mode": "quote_asof_future"}}) == "unverified"


def case_n3_ascii_text(mp, _tmp):
    _need_cal()
    _setup(mp, arm=GATES)
    p = build()
    blobs = [a["required_action"] for a in p["alerts"] if a["type"] == "stale_quotes"]
    blobs += [n["improve_note"] for n in p["near_miss"]]
    assert blobs and all(b.isascii() for b in blobs)
    src = (ROOT / "core" / "analysis" / "opportunity_builder.py").read_text(encoding="utf-8")
    added = src[src.index("def _venue_next_close("):src.index("def _data_trust_assessment(")]
    assert added.isascii() and src[src.index("def _venue_suffix("):
                                   src.index("def _venue_state(")].isascii()
    block = src[src.index("# v1.27.3 (2026-10-13, renumbered 2026-10-19 and 2026-10-22) [TOP10 DATA-QUALITY]"):
                src.index('OPPORTUNITY_BUILDER_VERSION = "')]
    assert block.isascii()


def case_n4_extension_never_widens_thresholds(mp, _tmp):
    _setup(mp, arm=GATES)
    # same max age, same close tolerance - the gate text is unchanged
    assert ob._env_quote_max_age_min() == 15
    mp.setattr(ob, "_venue_state", lambda *_a: (True, NOW - timedelta(hours=20)))
    ok, cur, _ = assess("2330.TW", NOW - timedelta(minutes=16), currency="TWD")
    assert not ok and cur.startswith("STALE_PRICE intraday")


# ------------------------------------------------------------------- gold ---
def _base():
    if not OB_BASE or not os.path.exists(OB_BASE):
        pytest.skip("TFB_TEST_OB_BASE unset")
    spec = importlib.util.spec_from_file_location("ob_base_1270", OB_BASE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # 2026-10-19 merge: this build now sits on main's v1.27.0 (#797), so the
    # golden base is `git show origin/main:core/analysis/opportunity_builder.py`
    # at 1.27.0 (it was the v1.26.1 copy before main moved).
    # 2026-10-22 merge: main is now v1.27.2 (#813), the base copy is
    # `git show df99bcf3:core/analysis/opportunity_builder.py`.
    assert mod.OPPORTUNITY_BUILDER_VERSION == "1.27.2", "TFB_TEST_OB_BASE is not v1.27.2"
    return mod


def case_g1_off_payload_byte_identical_to_base(mp, _tmp):
    base = _base()
    _setup(mp)
    _setup(mp, module=base)
    for rows, crit in ((pool(), CRIT), (_keep_pool(), KEEP_CRIT)):
        assert canon(build(rows, crit)) == canon(build(rows, crit, module=base))


def case_g2_armed_env_is_inert_on_base(mp, _tmp):
    _need_cal()
    base = _base()
    _setup(mp, module=base, arm=GATES)
    p = build(module=base)
    assert all(a["type"] != "stale_quotes" for a in p["alerts"])
    assert by_symbol(p)["000001.SZ"]["verdict"] != "INVEST"
    assert "audit_keep" not in build(_keep_pool(), KEEP_CRIT, module=base)["meta"]


CASES = [value for _name, value in sorted(
    ((name, value) for name, value in globals().items()
     if name.startswith("case_") and callable(value)),
    key=lambda kv: ({"w": 0, "n": 1, "g": 2}[kv[0][5]],
                    int(re.sub(r"\D", "", kv[0].split("_")[1]) or 0), kv[0]))]


@pytest.mark.parametrize("case", CASES, ids=[c.__name__[5:] for c in CASES])
def test_top10_data_quality(case, monkeypatch, tmp_path):
    case(monkeypatch, tmp_path)


def _main():
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
