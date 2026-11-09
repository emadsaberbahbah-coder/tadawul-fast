"""v5.160.0 [EVIDENCE-COHORT]: the real investability gate, a measured-loser cohort.

Owner, 9 Nov 2026: "Still our system is less than 49% ... check what is
preventing that and let's correct it". Matured Performance_Log, 28 Jun - 9 Nov:
INVESTABLE 617 of 1,268 = 48.7%; Tadawul INVESTABLE with forecast confidence
>= 0.75 won 82 of 288 (28.5%; 37.4% Jun-Aug, 15.4% Sep-Nov). The tier demotes
that cohort under TFB_EVIDENCE_GATE=enforce, tags it under observe, and is
byte-identical while off (the default).

E1  off / unset / junk mode -> the gate output equals the unset run exactly.
E2  observe -> verdict unchanged, one substring-safe tag, idempotent pass.
E3  enforce -> SR:HIGH demoted to WATCHLIST / WATCH with the evidence reason;
    scores and reliability untouched.
E4  cohort keys: SR:MEDIUM and US:HIGH stay; a bare four-digit code is
    Tadawul; percent-scale confidence reads as points; no confidence or no
    suffix -> no cohort.
E5  TFB_EVIDENCE_DEMOTE_COHORTS parsing (unset default, list, blank, junk).
E6  DEMOTE-only: a conservative-gate demotion and a BLOCKED row keep their
    verdict and reason; the conservative floor knob (70) is honoured first.
E7  surface_gate_states reports the mode, the cohort list and the floor.
E8  version and manifest pin.
"""
import copy
import pathlib

import pytest

_REPO = pathlib.Path(__file__).resolve().parents[1]
FLAGS = ("TFB_EVIDENCE_GATE", "TFB_EVIDENCE_DEMOTE_COHORTS", "TFB_CONSERVATIVE_GATE",
         "TFB_CONSERVATIVE_GATE_SCORE_FLOOR", "TFB_MARKET_REGIME", "TFB_STRICT_INVEST_GATE",
         "TFB_CALIBRATION_ADJUST", "TFB_RELIABILITY_RECALIBRATION", "TFB_THIN_HISTORY_GUARD")
BANNED = ("cap", "forecast", "target", "roi", "drop", "reject")


@pytest.fixture(autouse=True)
def gate_environment(monkeypatch):
    for name in FLAGS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_INVESTABILITY_GATE", "1")


def investable_row(**changes):
    row = {"symbol": "1050.SR", "name": "Synthetic Bank", "asset_class": "Equity", "current_price": 100,
           "forecast_price_3m": 110, "forecast_price_12m": 120, "expected_roi_3m": .10,
           "expected_roi_12m": .20, "forecast_source": "provider_target", "forecast_confidence": .8243,
           "week_52_low": 80, "week_52_high": 130, "avg_volume_30d": 10000, "pe_ttm": 12,
           "eps_ttm": 8, "market_cap": 1e9, "volatility_30d": .1, "overall_score": 74.26,
           "debt_to_equity": .2, "free_cash_flow_ttm": 1e6, "recommendation": "BUY", "risk_bucket": "LOW"}
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


def test_e1_off_unset_and_junk_are_byte_identical(monkeypatch):
    base = apply(investable_row())
    assert verdict(base) == ("INVESTABLE", "INVEST")
    assert "evidence_cohort_watch" not in warnings_of(base)
    for word in ("off", "", "0", "false", "maybe", "enforced", "observed"):
        monkeypatch.setenv("TFB_EVIDENCE_GATE", word)
        assert apply(investable_row()) == base, word
    monkeypatch.delenv("TFB_EVIDENCE_GATE")
    monkeypatch.setenv("TFB_EVIDENCE_DEMOTE_COHORTS", "SR:HIGH,US:MEDIUM")
    assert apply(investable_row()) == base, "the cohort list alone arms nothing"


def test_e2_observe_tags_once_and_keeps_the_verdict(monkeypatch):
    base = apply(investable_row())
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "observe")
    out = apply(investable_row())
    assert verdict(out) == ("INVESTABLE", "INVEST")
    assert "evidence_cohort_watch:sr:high" in warnings_of(out)
    assert out["block_reason"] == base["block_reason"]
    assert out["forecast_reliability_score"] == base["forecast_reliability_score"]
    assert out["data_quality_score"] == base["data_quality_score"]
    again = apply(out)
    assert warnings_of(again).count("evidence_cohort_watch") == 1, "a second pass adds no duplicate"
    assert verdict(again) == verdict(out)
    assert again["forecast_reliability_score"] == out["forecast_reliability_score"], "tag is reliability-scan safe"
    tag = "evidence_cohort_watch:sr:high"
    for bad in BANNED:
        assert bad not in tag


def test_e3_enforce_demotes_the_cohort(monkeypatch):
    base = apply(investable_row())
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    out = apply(investable_row())
    assert verdict(out) == ("WATCHLIST", "WATCH")
    assert out["block_reason"].startswith("Evidence gate: SR:HIGH cohort")
    assert out["overall_score"] == base["overall_score"]
    assert out["forecast_reliability_score"] == base["forecast_reliability_score"]
    assert out["data_quality_score"] == base["data_quality_score"]
    for word in ("ENFORCE", " on ", "1", "true"):
        monkeypatch.setenv("TFB_EVIDENCE_GATE", word)
        assert verdict(apply(investable_row())) == ("WATCHLIST", "WATCH"), word


@pytest.mark.parametrize("changes,demoted", [
    ({"forecast_confidence": .70}, False),                       # SR:MEDIUM won 56.9%
    ({"symbol": "EFC.US"}, False),                               # US:HIGH won 58.9%
    ({"symbol": "1050"}, True),                                  # bare Tadawul code
    ({"symbol": "1050.sr"}, True),                               # case
    ({"forecast_confidence": 79.3}, True),                       # percent points -> 0.793
    ({"forecast_confidence": 66.41}, False),                     # percent points -> MEDIUM
    ({"forecast_confidence": .75}, True),                        # tracker boundary: >= 0.75 HIGH
    ({"forecast_confidence": .7499}, False),
    ({"forecast_confidence": None}, False),                      # no confidence -> no cohort
    ({"forecast_confidence": "n/a"}, False),
    ({"symbol": "BRKB"}, False),                                 # no suffix -> no cohort
])
def test_e4_cohort_keys(monkeypatch, changes, demoted):
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    row = investable_row(**changes)
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "off")
    off = apply(row)
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    out = apply(row)
    if off["investability_status"] != "INVESTABLE":
        pytest.skip("row not investable for an unrelated reason: %s" % off["block_reason"])
    assert (verdict(out) == ("WATCHLIST", "WATCH")) is demoted, changes
    if not demoted:
        assert out == off


def test_e5_cohort_list_parsing(monkeypatch):
    from core import data_engine_v2 as de
    assert de._evidence_demote_cohorts() == frozenset({"SR:HIGH"})
    monkeypatch.setenv("TFB_EVIDENCE_DEMOTE_COHORTS", " us:high , Sr : Medium,SR:HIGH ")
    assert de._evidence_demote_cohorts() == frozenset({"US:HIGH", "SR:MEDIUM", "SR:HIGH"})
    monkeypatch.setenv("TFB_EVIDENCE_DEMOTE_COHORTS", "")
    assert de._evidence_demote_cohorts() == frozenset()
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST"), "an empty list never fires"
    monkeypatch.setenv("TFB_EVIDENCE_DEMOTE_COHORTS", "SR-HIGH,XX:SUPER,:HIGH,SR:HIGH:X,  ")
    assert de._evidence_demote_cohorts() == frozenset()
    monkeypatch.setenv("TFB_EVIDENCE_DEMOTE_COHORTS", "US:HIGH")
    assert verdict(apply(investable_row())) == ("INVESTABLE", "INVEST")
    assert verdict(apply(investable_row(symbol="EFC.US"))) == ("WATCHLIST", "WATCH")
    assert de._evidence_market_of("BBOX.L") == "L" and de._evidence_market_of("ACA.PA") == "PA"
    assert de._evidence_market_of("GC=F") == "" and de._evidence_market_of("^N225") == ""
    assert de._evidence_market_of("BRK-B.US") == "US" and de._evidence_market_of(None) == ""


def test_e6_demote_only_and_floor_first(monkeypatch):
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    cons = apply(investable_row(overall_score=66.0))
    assert verdict(cons) == ("WATCHLIST", "WATCH")
    assert cons["block_reason"].startswith("Conservative gate: overall 66 < 68")
    blocked = apply(investable_row(current_price=None))
    assert verdict(blocked) == ("BLOCKED", "DO_NOT_INVEST")
    assert blocked["block_reason"] == "Missing current price"
    # the v5.81.0 floor knob is the second lever: 69.5 is INVESTABLE at 68 and WATCHLIST at 70
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "off")
    us = investable_row(symbol="MBIN.US", forecast_confidence=.6641, overall_score=69.75)
    assert verdict(apply(us)) == ("INVESTABLE", "INVEST")
    monkeypatch.setenv("TFB_CONSERVATIVE_GATE_SCORE_FLOOR", "70")
    floor70 = apply(us)
    assert verdict(floor70) == ("WATCHLIST", "WATCH")
    assert floor70["block_reason"].startswith("Conservative gate: overall 70 < 70")
    # a floor-demoted Tadawul HIGH row keeps the conservative reason under enforce
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    sr = apply(investable_row(overall_score=69.37))
    assert sr["block_reason"].startswith("Conservative gate:")


def test_e7_health_surface(monkeypatch):
    from core import data_engine_v2 as de
    s = de.surface_gate_states()
    assert s["evidence_gate"] == "off" and s["evidence_cohorts"] == "SR:HIGH"
    assert s["conservative_floor"] == 68.0
    monkeypatch.setenv("TFB_EVIDENCE_GATE", "enforce")
    monkeypatch.setenv("TFB_EVIDENCE_DEMOTE_COHORTS", "us:high,sr:high")
    monkeypatch.setenv("TFB_CONSERVATIVE_GATE_SCORE_FLOOR", "70")
    s = de.surface_gate_states()
    assert (s["evidence_gate"], s["evidence_cohorts"], s["conservative_floor"]) == ("enforce", "SR:HIGH,US:HIGH", 70.0)


def test_e8_version_and_manifest_pin():
    from core import data_engine_v2 as de
    assert de.__version__ == "5.160.0"
    assert de._EVIDENCE_OBSERVE_TAG == "evidence_cohort_watch"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("core.data_engine_v2", "__version__", "5.160.0",' in vd
    src = (_REPO / "core" / "data_engine_v2.py").read_text(encoding="utf-8")
    assert src.count("WHY v5.160.0 - A COHORT THAT LOSES ON MATURED RECORDS IS NOT INVESTABLE") == 1
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v5.160.0" in src


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
