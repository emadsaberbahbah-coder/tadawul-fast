"""v5.162.0 [GATE-EVIDENCE]: the investability gate admits only affirmative evidence, and the reliability
penalties read one warning tag at a time (independent audit of 23 Nov 2026, P0 + P1, reproduced that day).

E1  off is byte-identical to v5.161.0 on every fixture: the holes stay open and no tag is written.
E2  unset (= observe): every verdict, reason and reliability unchanged; one substring-safe tag per family;
    a repeat pass is byte-identical.
E3  enforce: blank reco, no reco key, zero forecast prices, no risk reading, and all three, each demote to
    WATCHLIST / WATCH with the missing evidence named; the full-evidence control stays INVESTABLE / INVEST.
E4  valid evidence that must still pass under enforce: an explicit 0 expected ROI, only a 12M forecast price,
    only a risk bucket, only a risk score, a STRONG_BUY / ACCUMULATE reco.
E5  DEMOTE-only: an earlier BLOCKED / WATCHLIST verdict and its reason are never touched.
E6  reliability: an unrelated market-cap note beside a forecast note is tagged under observe AND enforce,
    the legacy number kept; only TFB_REL_TAGS_EXACT=1 applies the exact reading (it costs nothing then); a
    real forecast / target cap tag still costs 20; a provider-target drop tag still costs 15; a market-cap
    tag with a target word never counts.
E6b demote-only with the strict tier (Codex P2 on #1067): TFB_GATE_EVIDENCE enforce never promotes a row the
    strict reliability floor held; TFB_REL_TAGS_EXACT is the separate switch that can.
E7  tag lifecycle: the tag goes when the row is repaired; switching to off leaves rows untouched.
E8  version, manifest pin, the WHY block and surface_gate_states.
"""
import copy
import pathlib

import pytest

_REPO = pathlib.Path(__file__).resolve().parents[1]
FLAGS = ("TFB_GATE_EVIDENCE", "TFB_REL_TAGS_EXACT", "TFB_MARKET_GATE", "TFB_SELECTION_GATE", "TFB_EVIDENCE_GATE", "TFB_CONSERVATIVE_GATE",
         "TFB_STRICT_INVEST_GATE", "TFB_CALIBRATION_ADJUST", "TFB_RELIABILITY_RECALIBRATION",
         "TFB_THIN_HISTORY_GUARD", "TFB_MARKET_REGIME")
BANNED = ("cap", "forecast", "target", "roi", "drop", "reject")


@pytest.fixture(autouse=True)
def gate_environment(monkeypatch):
    for name in FLAGS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_INVESTABILITY_GATE", "1")
    yield


def full_row(**changes):
    row = {"symbol": "TEST.US", "name": "Synthetic Industrial", "asset_class": "Equity", "sector": "Industrials",
           "industry": "Machinery", "current_price": 100.0, "week_52_high": 120.0, "week_52_low": 80.0,
           "volume": 1e6, "avg_volume_30d": 1e6, "pe_ttm": 15.0, "eps_ttm": 6.6, "market_cap": 5e10,
           "volatility_30d": 0.25, "max_drawdown_1y": -0.2, "forecast_price_3m": 108.0,
           "forecast_price_12m": 125.0, "expected_roi_3m": 0.08, "expected_roi_12m": 0.25, "overall_score": 82.0,
           "debt_to_equity": 0.4, "free_cash_flow_ttm": 3e9, "forecast_confidence": 0.9, "risk_score": 20.0,
           "risk_bucket": "LOW", "recommendation": "BUY"}
    for k, v in changes.items():
        if v is _DROP:
            row.pop(k, None)
        else:
            row[k] = v
    return row


_DROP = object()
HOLES = {
    "blank reco": dict(recommendation=""),
    "no reco key": dict(recommendation=_DROP),
    "zero forecast": dict(forecast_price_3m=0.0, forecast_price_12m=0.0, expected_roi_3m=_DROP, expected_roi_12m=_DROP),
    "no risk": dict(risk_score=_DROP, risk_bucket=_DROP),
    "all three": dict(recommendation="", forecast_price_3m=0.0, forecast_price_12m=_DROP, expected_roi_3m=_DROP,
                      expected_roi_12m=_DROP, risk_score=_DROP, risk_bucket=_DROP),
}
CODES = {"blank reco": "nr", "no reco key": "nr", "zero forecast": "np", "no risk": "nk", "all three": "nr+np+nk"}


def gate(row):
    from core import data_engine_v2 as de
    de._apply_investability_gate(row)
    return row


def tags(row, family):
    from core import data_engine_v2 as de
    return [p for p in de._mpc_warning_parts(row) if p.startswith(family + ":")]


def verdict(row):
    return (row.get("investability_status"), row.get("final_action"), row.get("block_reason"),
            row.get("forecast_reliability_score"))


def test_e1_off_is_byte_identical(monkeypatch):
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "off")
    for name, change in HOLES.items():
        row = gate(full_row(**change))
        assert verdict(row)[:2] == ("INVESTABLE", "INVEST"), name          # the hole is left exactly as it was
        assert not tags(row, "gate_evidence") and not tags(row, "rel_tags_exact"), name
    row = gate(full_row(warnings="market_cap_subunit_normalized_to_major; forecast_source_cleared"))
    assert row["forecast_reliability_score"] == 70.0                        # the legacy substring penalty, kept


def test_e2_observe_is_the_default_and_changes_no_verdict(monkeypatch):
    from core import data_engine_v2 as de
    assert de._gate_evidence_mode() == "observe"
    for name, change in HOLES.items():
        monkeypatch.setenv("TFB_GATE_EVIDENCE", "off")
        before = verdict(gate(full_row(**change)))
        monkeypatch.delenv("TFB_GATE_EVIDENCE")
        row = gate(full_row(**change))
        assert verdict(row) == before, name
        assert tags(row, "gate_evidence") == ["gate_evidence:" + CODES[name]], name
        again = gate(copy.deepcopy(row))
        assert again == row, name                                           # a repeat pass changes nothing
        for t in tags(row, "gate_evidence"):
            assert not any(b in t.lower() for b in BANNED), t                # the tag cannot trip a penalty
    assert not tags(gate(full_row()), "gate_evidence")


def test_e3_enforce_demotes_each_hole(monkeypatch):
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "enforce")
    words = {"nr": "no BUY-family recommendation", "np": "no positive forecast price or valid ROI",
             "nk": "no risk reading"}
    for name, change in HOLES.items():
        row = gate(full_row(**change))
        assert verdict(row)[:2] == ("WATCHLIST", "WATCH"), (name, verdict(row))
        assert row["block_reason"] == "Evidence gate: " + "; ".join(words[c] for c in CODES[name].split("+")), name
        assert not tags(row, "gate_evidence"), name
    assert verdict(gate(full_row()))[:3] == ("INVESTABLE", "INVEST", "")


@pytest.mark.parametrize("change", [
    dict(forecast_price_3m=0.0, forecast_price_12m=0.0, expected_roi_3m=0.0, expected_roi_12m=_DROP),
    dict(forecast_price_3m=_DROP, expected_roi_3m=_DROP, expected_roi_12m=_DROP),
    dict(risk_score=_DROP),
    dict(risk_bucket=_DROP),
    dict(recommendation="STRONG_BUY"),
    dict(recommendation="ACCUMULATE"),
])
def test_e4_valid_evidence_still_passes(monkeypatch, change):
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "enforce")
    assert verdict(gate(full_row(**change)))[:2] == ("INVESTABLE", "INVEST")


@pytest.mark.parametrize("change,status", [
    (dict(current_price=_DROP, price=_DROP), "BLOCKED"),
    (dict(recommendation="HOLD"), "WATCHLIST"),
    (dict(recommendation="SELL"), "WATCHLIST"),
])
def test_e5_demote_only(monkeypatch, change, status):
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "off")
    before = verdict(gate(full_row(**change)))
    for mode in ("observe", "enforce"):
        monkeypatch.setenv("TFB_GATE_EVIDENCE", mode)
        row = gate(full_row(**change))
        assert verdict(row) == before and before[0] == status
        assert not tags(row, "gate_evidence")


def test_e6_reliability_reads_one_tag_at_a_time(monkeypatch):
    unrelated = "market_cap_subunit_normalized_to_major; forecast_source_cleared"
    for mode in (None, "enforce"):                                          # observe and enforce: legacy number, tagged
        if mode:
            monkeypatch.setenv("TFB_GATE_EVIDENCE", mode)
        row = gate(full_row(warnings=unrelated))
        assert row["forecast_reliability_score"] == 70.0 and tags(row, "rel_tags_exact") == ["rel_tags_exact:+20"]
    monkeypatch.setenv("TFB_REL_TAGS_EXACT", "1")
    row = gate(full_row(warnings=unrelated))
    assert row["forecast_reliability_score"] == 90.0 and not tags(row, "rel_tags_exact")
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "off")                          # off wins: nothing exact at all
    row = gate(full_row(warnings=unrelated))
    assert row["forecast_reliability_score"] == 70.0 and not tags(row, "rel_tags_exact")
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "enforce")
    for real, rel in (("forecast_capped_at_ceiling", 70.0),
                      ("provider_target_capped_for_short_horizon_derivation", 70.0),
                      ("provider_target_12m_capped_to_phase_ii_ceiling", 70.0),
                      ("provider_target_dropped:implausible", 75.0),
                      ("market_cap_currency_suspect; target_mean_present", 90.0),
                      ("dq_capped; roi_note", 90.0)):
        assert gate(full_row(warnings=real))["forecast_reliability_score"] == rel, real


def test_e6b_evidence_enforce_never_promotes_through_the_strict_tier(monkeypatch):
    monkeypatch.setenv("TFB_STRICT_INVEST_GATE", "1")
    case = dict(forecast_confidence=0.8, warnings="market_cap_subunit_normalized_to_major; forecast_source_cleared")
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "off")
    before = verdict(gate(full_row(**case)))
    assert before[:2] == ("WATCHLIST", "WATCH") and before[3] == 60.0
    for mode in ("observe", "enforce"):
        monkeypatch.setenv("TFB_GATE_EVIDENCE", mode)
        assert verdict(gate(full_row(**case))) == before, mode             # demote-only: the strict hold stands
    monkeypatch.setenv("TFB_REL_TAGS_EXACT", "1")                          # the separate, promote-capable switch
    row = gate(full_row(**case))
    assert row["forecast_reliability_score"] == 80.0 and verdict(row)[:2] == ("INVESTABLE", "INVEST")


def test_e7_tag_lifecycle(monkeypatch):
    row = gate(full_row(recommendation=""))
    assert tags(row, "gate_evidence") == ["gate_evidence:nr"]
    row["recommendation"] = "BUY"
    gate(row)
    assert not tags(row, "gate_evidence")
    row = gate(full_row(recommendation=""))
    monkeypatch.setenv("TFB_GATE_EVIDENCE", "off")
    kept = copy.deepcopy(row)
    gate(row)
    assert tags(row, "gate_evidence") == tags(kept, "gate_evidence")       # off never edits a row


def test_e8_version_manifest_why_and_surface(monkeypatch):
    from core import data_engine_v2 as de
    assert de.__version__ == "5.165.0"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("core.data_engine_v2", "__version__", "5.165.0",' in vd
    src = (_REPO / "core" / "data_engine_v2.py").read_text(encoding="utf-8")
    assert src.count("WHY v5.162.0 - INVEST ONLY ON AFFIRMATIVE EVIDENCE;") == 1
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v5.165.0" in src
    for raw, mode in (("", "observe"), ("off", "off"), ("0", "off"), ("enforce", "enforce"), ("junk", "observe")):
        monkeypatch.setenv("TFB_GATE_EVIDENCE", raw)
        assert de._gate_evidence_mode() == mode and de.surface_gate_states()["gate_evidence"] == mode
    assert de.surface_gate_states()["rel_tags_exact"] is False
    monkeypatch.setenv("TFB_REL_TAGS_EXACT", "1")
    assert de.surface_gate_states()["rel_tags_exact"] is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
