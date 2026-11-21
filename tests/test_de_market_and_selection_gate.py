"""v5.161.0 [MARKET-HOLD] + [SELECTION]: two DEMOTE-only tiers of the real investability gate.

Owner, 21 Nov 2026 19:10 Riyadh: "MODEL EVIDENCE 47.60% INVESTABLE hit rate ...
since 2026-11-10: 11.1% (n 45) ... need to improve". On the matured INVESTABLE
records: market calm 52.5% (1,150); analyst upside >= 15% and risk < 30 60.8%
(729); both 69.8% (374) against 45.3% for all records of the same Sep - Nov
stretch. TFB_MARKET_GATE / TFB_SELECTION_GATE: off (default, byte-identical) |
observe (one substring-safe tag) | enforce (WATCHLIST / WATCH).

G1  off / unset / junk modes and the knobs alone are byte-identical; off never
    asks core.market_condition for anything.
G2  market observe: one tag market_hold_watch:<market>_<label>, verdict,
    reliability and reason unchanged, a repeat pass byte-identical.
G3  market enforce: WATCHLIST with "Market on hold: <text>"; calm never holds;
    TFB_MARKET_GATE_HOLD and TFB_MARKET_GATE_MARKETS narrow it.
G4  market scope: no label (not read, stale, other market) never holds;
    commodities / currencies / indices / bonds / sukuk / crypto / futures are
    never held; a fund on a held market is.
G5  selection: upside and risk limits at their boundaries, no consensus, the
    target_price fallback, the knobs, the tag codes, the reasons.
G6  selection scope: ETFs, funds, fund-vehicle industries, crypto, commodities
    are out; a blank asset class counts as a share.
G7  DEMOTE-only and order: an earlier tier's demotion and a BLOCKED row keep
    their verdict and reason; the market reason wins over the selection one.
G8  tag lifecycle while armed: replaced in place, removed when the market is
    calm again or the row stops failing; untouched while off.
G9  surface_gate_states reports both modes, the knobs and the labels known.
G10 version, manifest pin and the WHY block.
"""
import copy
import datetime as dt
import pathlib

import pytest

from core import market_condition as mc

_REPO = pathlib.Path(__file__).resolve().parents[1]
FLAGS = ("TFB_MARKET_GATE", "TFB_MARKET_GATE_HOLD", "TFB_MARKET_GATE_MARKETS", "TFB_SELECTION_GATE",
         "TFB_SELECTION_MIN_UPSIDE", "TFB_SELECTION_MAX_RISK", "TFB_MARKET_CONDITION_TTL_SEC",
         "TFB_EVIDENCE_GATE", "TFB_EVIDENCE_DEMOTE_COHORTS", "TFB_CONSERVATIVE_GATE",
         "TFB_CONSERVATIVE_GATE_SCORE_FLOOR", "TFB_MARKET_REGIME", "TFB_STRICT_INVEST_GATE",
         "TFB_CALIBRATION_ADJUST", "TFB_RELIABILITY_RECALIBRATION", "TFB_THIN_HISTORY_GUARD")
BANNED = ("cap", "forecast", "target", "roi", "drop", "reject")
DAY = dt.date(2026, 11, 20)


@pytest.fixture(autouse=True)
def gate_environment(monkeypatch):
    for name in FLAGS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_INVESTABILITY_GATE", "1")
    monkeypatch.setattr(mc, "_spawn", lambda target, args: None)    # never a network read in this suite
    mc.reset()
    yield
    mc.reset()


def label(mid, word):
    return {"label": word, "day": DAY, "text": "%s %s as of %s" % (mc.MARKET_NAMES[mid], word, DAY.isoformat())}


def markets(**words):
    mc.set_labels({mid: label(mid, w) for mid, w in words.items()})


def investable_row(**changes):
    row = {"symbol": "EFC.US", "name": "Synthetic Lender", "asset_class": "Equity", "current_price": 100,
           "forecast_price_3m": 110, "forecast_price_12m": 120, "expected_roi_3m": .10,
           "expected_roi_12m": .20, "forecast_source": "provider_target", "forecast_confidence": .6641,
           "week_52_low": 80, "week_52_high": 130, "avg_volume_30d": 10000, "pe_ttm": 12,
           "eps_ttm": 8, "market_cap": 1e9, "volatility_30d": .1, "overall_score": 74.26,
           "debt_to_equity": .2, "free_cash_flow_ttm": 1e6, "recommendation": "BUY", "risk_bucket": "LOW",
           "risk_score": 20.0, "target_mean_price": 120.0}
    row.update(changes)
    return row


def apply(row):
    from core import data_engine_v2 as de
    out = copy.deepcopy(row)
    de._apply_investability_gate(out)
    return out


def verdict(out):
    return out["investability_status"], out["final_action"]


def warnings_of(out):
    return str(out.get("warnings") or "")


def tags_of(out, family):
    return [p.strip() for p in warnings_of(out).split(";") if p.strip().startswith(family + ":")]


# ---------------------------------------------------------------- G1
def test_g1_off_is_byte_identical_and_reads_nothing(monkeypatch):
    markets(us="weak")
    base = apply(investable_row(target_mean_price=101.0, risk_score=40.0))
    assert verdict(base) == ("INVESTABLE", "INVEST")
    assert "market_hold_watch" not in warnings_of(base) and "selection_watch" not in warnings_of(base)

    def no_read(*a, **k):
        raise AssertionError("the gate is off: nothing may be read")

    monkeypatch.setattr(mc, "labels", no_read)
    monkeypatch.setattr(mc, "market_of", no_read)
    for word in ("off", "", "0", "false", "maybe", "enforced", "observed"):
        monkeypatch.setenv("TFB_MARKET_GATE", word)
        monkeypatch.setenv("TFB_SELECTION_GATE", word)
        assert apply(investable_row(target_mean_price=101.0, risk_score=40.0)) == base, word
    monkeypatch.delenv("TFB_MARKET_GATE")
    monkeypatch.delenv("TFB_SELECTION_GATE")
    for knob, value in (("TFB_MARKET_GATE_HOLD", "calm,weak"), ("TFB_MARKET_GATE_MARKETS", "us"),
                        ("TFB_SELECTION_MIN_UPSIDE", "50"), ("TFB_SELECTION_MAX_RISK", "5")):
        monkeypatch.setenv(knob, value)
    assert apply(investable_row(target_mean_price=101.0, risk_score=40.0)) == base, "the knobs alone arm nothing"
    # an old observe tag on a cached row is left alone while the gate is off
    tagged = investable_row(warnings="market_hold_watch:us_weak; selection_watch:no_analyst_px")
    assert warnings_of(apply(tagged)).startswith("market_hold_watch:us_weak; selection_watch:no_analyst_px")


# ---------------------------------------------------------------- G2 / G3
def test_g2_market_observe_tags_once(monkeypatch):
    markets(us="weak")
    base = apply(investable_row())
    monkeypatch.setenv("TFB_MARKET_GATE", "observe")
    out = apply(investable_row())
    assert verdict(out) == ("INVESTABLE", "INVEST")
    assert tags_of(out, "market_hold_watch") == ["market_hold_watch:us_weak"]
    for key in ("block_reason", "forecast_reliability_score", "data_quality_score", "overall_score"):
        assert out[key] == base[key], key
    again = apply(out)
    assert again == out, "a repeat pass is byte-identical"
    for word in ("weak", "stress", "drawdown", "crash_day"):
        for mid in ("us", "sa", "eu", "jp", "cn"):
            tag = "market_hold_watch:%s_%s" % (mid, word)
            assert not any(bad in tag for bad in BANNED), tag


def test_g3_market_enforce_demotes(monkeypatch):
    markets(us="weak", sa="drawdown", eu="calm", jp="crash_day", cn="stress")
    monkeypatch.setenv("TFB_MARKET_GATE", "enforce")
    out = apply(investable_row())
    assert verdict(out) == ("WATCHLIST", "WATCH")
    assert out["block_reason"] == "Market on hold: the US market weak as of 2026-11-20"
    assert "market_hold_watch" not in warnings_of(out), "enforce writes the reason, not the tag"
    for symbol, held in (("2222.SR", True), ("1120", True), ("BBOX.L", False), ("7203.T", True),
                         ("0700.HK", True), ("GOOGL", True), ("PEO.WA", False)):
        got = verdict(apply(investable_row(symbol=symbol)))
        assert (got == ("WATCHLIST", "WATCH")) is held, symbol
    for word in ("ENFORCE", " on ", "1", "true", "yes"):
        monkeypatch.setenv("TFB_MARKET_GATE", word)
        assert verdict(apply(investable_row())) == ("WATCHLIST", "WATCH"), word
    monkeypatch.setenv("TFB_MARKET_GATE", "enforce")
    monkeypatch.setenv("TFB_MARKET_GATE_HOLD", "crash_day, DRAWDOWN ,calm,junk")
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST"), "weak no longer holds"
    assert verdict(apply(investable_row(symbol="2222.SR"))) == ("WATCHLIST", "WATCH")
    assert verdict(apply(investable_row(symbol="BBOX.L"))) == ("INVESTABLE", "INVEST"), "calm can never hold"
    monkeypatch.setenv("TFB_MARKET_GATE_HOLD", "")
    assert verdict(apply(investable_row(symbol="7203.T"))) == ("INVESTABLE", "INVEST"), "blank list: nothing holds"
    monkeypatch.delenv("TFB_MARKET_GATE_HOLD")
    monkeypatch.setenv("TFB_MARKET_GATE_MARKETS", " US ,xx")
    assert verdict(apply(investable_row())) == ("WATCHLIST", "WATCH")
    assert verdict(apply(investable_row(symbol="2222.SR"))) == ("INVESTABLE", "INVEST")
    monkeypatch.setenv("TFB_MARKET_GATE_MARKETS", "")
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST")


# ---------------------------------------------------------------- G4
@pytest.mark.parametrize("changes,held", [
    ({"asset_class": "Commodity", "symbol": "GLDX.US"}, False),
    ({"asset_class": "FX"}, False),
    ({"asset_class": "Currency"}, False),
    ({"asset_class": "Index"}, False),
    ({"asset_class": "Fixed Income"}, False),
    ({"asset_class": "Sukuk"}, False),
    ({"asset_class": "Crypto", "symbol": "BTC-USD"}, False),
    ({"asset_class": "Future"}, False),
    ({"asset_class": "ETF", "symbol": "SPY.US"}, True),          # a fund on a held market is held
    ({"asset_class": "Fund"}, True),
    ({"asset_class": ""}, True),
    ({"symbol": "RY.TO"}, False),                                # no read market
    ({"symbol": ""}, None),                                      # not investable for another reason, or no market
])
def test_g4_market_scope(monkeypatch, changes, held):
    markets(us="weak")
    monkeypatch.setenv("TFB_MARKET_GATE", "off")
    off = apply(investable_row(**changes))
    if off["investability_status"] != "INVESTABLE":
        pytest.skip("not investable for an unrelated reason: %s" % off["block_reason"])
    monkeypatch.setenv("TFB_MARKET_GATE", "enforce")
    out = apply(investable_row(**changes))
    if held is None:
        assert verdict(out) == ("INVESTABLE", "INVEST")
        return
    assert (verdict(out) == ("WATCHLIST", "WATCH")) is held, changes
    if not held:
        assert out == off


def test_g4_no_label_never_holds(monkeypatch):
    monkeypatch.setenv("TFB_MARKET_GATE", "enforce")
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST"), "nothing read yet"
    markets(sa="drawdown")
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST"), "the US was not read"
    mc.set_labels({"us": label("us", "weak")}, at=mc.time.time() - mc.STALE_MAX_SEC - 60)
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST"), "a label older than 12 hours"

    def broken(*a, **k):
        raise RuntimeError("module fault")

    markets(us="weak")
    monkeypatch.setattr(mc, "labels", broken)
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST"), "a fault never holds"


# ---------------------------------------------------------------- G5
@pytest.mark.parametrize("changes,reason,codes", [
    ({}, None, None),
    ({"target_mean_price": 115.0}, None, None),                                   # exactly 15% passes
    ({"target_mean_price": 114.99}, "analyst upside 15.0% < 15%", "upside_lt15"),
    ({"target_mean_price": 110.0}, "analyst upside 10.0% < 15%", "upside_lt15"),
    ({"target_mean_price": 90.0}, "analyst upside -10.0% < 15%", "upside_lt15"),
    ({"target_mean_price": None}, "no analyst consensus price", "no_analyst_px"),
    ({"target_mean_price": 0}, "no analyst consensus price", "no_analyst_px"),
    ({"target_mean_price": "n/a"}, "no analyst consensus price", "no_analyst_px"),
    ({"target_mean_price": None, "target_price": 125.0}, None, None),             # the projected column
    ({"target_mean_price": None, "target_price": 105.0}, "analyst upside 5.0% < 15%", "upside_lt15"),
    ({"risk_score": 29.99}, None, None),
    ({"risk_score": 30.0}, "risk 30 >= 30", "risk_ge30"),
    ({"risk_score": None}, None, None),                                           # no risk score: that test passes
    ({"risk_score": 33.0, "target_mean_price": 101.0}, "analyst upside 1.0% < 15%; risk 33 >= 30",
     "upside_lt15+risk_ge30"),
])
def test_g5_selection_rule(monkeypatch, changes, reason, codes):
    row = investable_row(**changes)
    off = apply(row)
    assert verdict(off) == ("INVESTABLE", "INVEST"), off["block_reason"]
    monkeypatch.setenv("TFB_SELECTION_GATE", "enforce")
    out = apply(row)
    if reason is None:
        assert out == off
    else:
        assert verdict(out) == ("WATCHLIST", "WATCH")
        assert out["block_reason"] == "Selection gate: " + reason
    monkeypatch.setenv("TFB_SELECTION_GATE", "observe")
    obs = apply(row)
    assert verdict(obs) == ("INVESTABLE", "INVEST")
    if codes is None:
        assert obs == off
    else:
        tag = "selection_watch:" + codes
        assert tags_of(obs, "selection_watch") == [tag]
        assert not any(bad in tag for bad in BANNED), tag
        assert obs["forecast_reliability_score"] == off["forecast_reliability_score"]
        assert apply(obs) == obs, "a repeat pass is byte-identical"


def test_g5_selection_knobs(monkeypatch):
    from core import data_engine_v2 as de
    assert (de._selection_min_upside_pp(), de._selection_max_risk()) == (15.0, 30.0)
    for raw, want in (("0.2", 20.0), ("20", 20.0), ("0.155", 15.5), ("-3", 15.0), ("0", 15.0), ("x", 15.0),
                      ("nan", 15.0), ("inf", 15.0)):
        monkeypatch.setenv("TFB_SELECTION_MIN_UPSIDE", raw)
        assert de._selection_min_upside_pp() == want, raw
    for raw, want in (("25", 25.0), ("100", 100.0), ("0", 30.0), ("101", 30.0), ("x", 30.0), ("nan", 30.0)):
        monkeypatch.setenv("TFB_SELECTION_MAX_RISK", raw)
        assert de._selection_max_risk() == want, raw
    monkeypatch.setenv("TFB_SELECTION_GATE", "enforce")
    monkeypatch.setenv("TFB_SELECTION_MIN_UPSIDE", "0.25")
    monkeypatch.setenv("TFB_SELECTION_MAX_RISK", "15")
    out = apply(investable_row())
    assert out["block_reason"] == "Selection gate: analyst upside 20.0% < 25%; risk 20 >= 15"
    monkeypatch.setenv("TFB_SELECTION_GATE", "observe")
    assert tags_of(apply(investable_row()), "selection_watch") == ["selection_watch:upside_lt25+risk_ge15"]


# ---------------------------------------------------------------- G6
@pytest.mark.parametrize("changes,applies", [
    ({"asset_class": "ETF"}, False),
    ({"asset_class": "Fund"}, False),
    ({"asset_class": "Mutual Fund"}, False),
    ({"asset_class": "Equity", "industry": "Exchange Traded Fund"}, False),
    ({"asset_class": "Equity", "industry": "ETF"}, False),
    ({"asset_class": "Commodity"}, False),
    ({"asset_class": "Crypto"}, False),
    ({"asset_class": "Future"}, False),
    ({"asset_class": "Sukuk"}, False),
    ({"asset_class": "EQUITY"}, True),
    ({"asset_class": ""}, True),
    ({"asset_class": "Equity", "industry": "Asset Management"}, True),
])
def test_g6_selection_scope(monkeypatch, changes, applies):
    row = investable_row(target_mean_price=None, **changes)
    off = apply(row)
    if off["investability_status"] != "INVESTABLE":
        pytest.skip("not investable for an unrelated reason: %s" % off["block_reason"])
    monkeypatch.setenv("TFB_SELECTION_GATE", "enforce")
    out = apply(row)
    assert (verdict(out) == ("WATCHLIST", "WATCH")) is applies, changes
    if not applies:
        assert out == off


# ---------------------------------------------------------------- G7
def test_g7_demote_only_and_order(monkeypatch):
    markets(us="weak", sa="drawdown")
    monkeypatch.setenv("TFB_MARKET_GATE", "enforce")
    monkeypatch.setenv("TFB_SELECTION_GATE", "enforce")
    cons = apply(investable_row(overall_score=66.0, target_mean_price=None))
    assert verdict(cons) == ("WATCHLIST", "WATCH")
    assert cons["block_reason"].startswith("Conservative gate: overall 66 < 68")
    blocked = apply(investable_row(current_price=None))
    assert verdict(blocked) == ("BLOCKED", "DO_NOT_INVEST") and blocked["block_reason"] == "Missing current price"
    both = apply(investable_row(target_mean_price=None))
    assert both["block_reason"].startswith("Market on hold: the US market weak"), "the market tier runs first"
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    sr = apply(investable_row(symbol="1050.SR", forecast_confidence=.8243))
    assert sr["block_reason"].startswith("Evidence gate: SR:HIGH cohort"), "the evidence tier runs before both"
    # observe never tags a row an earlier tier already demoted
    monkeypatch.setenv("TFB_MARKET_GATE", "observe")
    monkeypatch.setenv("TFB_SELECTION_GATE", "observe")
    cons = apply(investable_row(overall_score=66.0, target_mean_price=None))
    assert not tags_of(cons, "market_hold_watch") and not tags_of(cons, "selection_watch")
    # both observe tags on one row, each once
    out = apply(investable_row(target_mean_price=None))
    assert tags_of(out, "market_hold_watch") == ["market_hold_watch:us_weak"]
    assert tags_of(out, "selection_watch") == ["selection_watch:no_analyst_px"]


# ---------------------------------------------------------------- G8
def test_g8_tag_lifecycle(monkeypatch):
    monkeypatch.setenv("TFB_MARKET_GATE", "observe")
    monkeypatch.setenv("TFB_SELECTION_GATE", "observe")
    markets(us="weak")
    row = investable_row(target_mean_price=110.0, warnings="analyst_lkg:2h")
    first = apply(row)
    assert warnings_of(first).startswith("analyst_lkg:2h; market_hold_watch:us_weak; selection_watch:upside_lt15")
    markets(us="stress")
    second = apply(first)
    parts = [p.strip() for p in warnings_of(second).split(";")]
    assert parts[:3] == ["analyst_lkg:2h", "market_hold_watch:us_stress", "selection_watch:upside_lt15"], \
        "replaced in place"
    markets(us="calm")
    second["target_mean_price"] = 130.0
    third = apply(second)
    assert not tags_of(third, "market_hold_watch") and not tags_of(third, "selection_watch")
    assert warnings_of(third).startswith("analyst_lkg:2h")
    # an enforce pass also clears an observe tag left on a cached row
    monkeypatch.setenv("TFB_MARKET_GATE", "enforce")
    markets(us="weak")
    enf = apply(first)
    assert not tags_of(enf, "market_hold_watch") and enf["block_reason"].startswith("Market on hold:")
    # duplicate stale tags collapse to one
    monkeypatch.setenv("TFB_MARKET_GATE", "observe")
    messy = investable_row(warnings="market_hold_watch:sa_drawdown; x; market_hold_watch:us_stress")
    assert warnings_of(apply(messy)).startswith("market_hold_watch:us_weak; x")


# ---------------------------------------------------------------- G9 / G10
def test_g9_health_surface(monkeypatch):
    from core import data_engine_v2 as de
    s = de.surface_gate_states()
    assert (s["market_gate"], s["selection_gate"]) == ("off", "off")
    assert s["market_gate_hold"] == "crash_day,drawdown,stress,weak"
    assert s["market_gate_markets"] == "us,sa,eu,jp,cn"
    assert (s["selection_min_upside_pp"], s["selection_max_risk"]) == (15.0, 30.0)
    assert s["market_labels"] == {}
    monkeypatch.setenv("TFB_MARKET_GATE", "observe")
    monkeypatch.setenv("TFB_SELECTION_GATE", "enforce")
    monkeypatch.setenv("TFB_MARKET_GATE_HOLD", "drawdown,crash_day")
    monkeypatch.setenv("TFB_MARKET_GATE_MARKETS", "sa,us")
    markets(us="weak")
    s = de.surface_gate_states()
    assert (s["market_gate"], s["selection_gate"]) == ("observe", "enforce")
    assert (s["market_gate_hold"], s["market_gate_markets"]) == ("crash_day,drawdown", "us,sa")
    assert s["market_labels"]["us"]["label"] == "weak" and s["market_labels"]["us"]["day"] == "2026-11-20"


def test_g10_version_manifest_and_why():
    from core import data_engine_v2 as de
    assert de.__version__ == "5.161.0"
    assert (de._MARKET_HOLD_TAG, de._SELECTION_TAG) == ("market_hold_watch", "selection_watch")
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("core.data_engine_v2", "__version__", "5.161.0",' in vd
    src = (_REPO / "core" / "data_engine_v2.py").read_text(encoding="utf-8")
    assert src.count("WHY v5.161.0 - NO NEW BUY IN A MARKET THAT IS NOT CALM;") == 1
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v5.161.0" in src
    assert de._MARKET_GATE_KNOWN_MARKETS == tuple(mc.MARKETS)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
