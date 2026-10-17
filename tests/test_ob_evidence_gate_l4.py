"""L4 (2026-10-17): opportunity_builder v1.30.0 — evidence gate on qualification,
observe first. TFB_T10_EVIDENCE_GATE off|observe|enforce reads the cockpit's
copy of the Forecast Scorecard (upstream_meta["evidence_scorecard"]): a
candidate's Investability x Recommendation cohort at the plan horizon must show
hit >= TFB_T10_EVIDENCE_MIN_HIT (55) with n >= TFB_T10_EVIDENCE_MIN_N (100).
Off is byte-identical to v1.29.0 save the version stamp.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

from core.analysis import opportunity_builder as ob

sys.path.insert(0, str(Path(__file__).resolve().parent))
from decision_evidence_fixtures import observed_price_fields, observed_portfolio  # noqa: E402

CRIT = {"investability_gate_enabled": True, "blocked_identity_gate_enabled": True}
ENVS = ("TFB_T10_EVIDENCE_GATE", "TFB_T10_EVIDENCE_MIN_HIT", "TFB_T10_EVIDENCE_MIN_N",
        "TFB_T10_INVEST_REASONS_LEGACY", "TFB_T10_CLUSTER_LEGACY")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for k in ENVS:
        monkeypatch.delenv(k, raising=False)


def _row(symbol, **over):
    row = {
        **observed_price_fields(),
        "symbol": symbol, "name": symbol + " Co", "sector": "Financials", "market": "Tadawul",
        "currency": "SAR", "current_price": 100.0, "intrinsic_value": 130.0,
        "forecast_reliability_score": 82.0, "data_quality_score": 91.0, "risk_bucket": "Moderate",
        "provider_engine_conflict": "No", "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": 24.0, "recommendation_detailed": "BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
    }
    row.update(over)
    return row


ROWS = [_row("GOOD.SR", sector="Energy"),                                     # INVESTABLE|BUY: 63.3% n=671 -> ok
        _row("WEAK.SR", sector="Utilities", recommendation_detailed="ACCUMULATE"),  # INVESTABLE|ACCUMULATE: 48% n=400 -> hit below
        _row("THIN.SR", sector="Consumer", recommendation_detailed="STRONG BUY"),   # INVESTABLE|STRONG_BUY: 70% n=40 -> n below
        _row("NEW.SR", sector="Industrials", recommendation_detailed="HOLD"),       # no cohort
        _row("WL.SR", sector="Materials", investability_status="WATCHLIST",
             block_reason="Engine recommends HOLD", recommendation_detailed="HOLD")]  # WATCHLIST|HOLD 47% n=5000

SCORECARD = [
    {"horizon": "1Y", "dimension": "Investability x Recommendation", "cohort": "INVESTABLE|BUY", "n": 671, "hit": 63.3},
    {"horizon": "1M", "dimension": "Investability x Recommendation", "cohort": "INVESTABLE|ACCUMULATE", "n": 400, "hit": 48.0},
    {"horizon": "3M", "dimension": "Investability x Recommendation", "cohort": "INVESTABLE|STRONG_BUY", "n": 40, "hit": 70.0},
    {"horizon": "1M", "dimension": "Investability x Recommendation", "cohort": "WATCHLIST|HOLD", "n": 5000, "hit": 47.0},
    {"horizon": "1M", "dimension": "Investability", "cohort": "INVESTABLE", "n": 963, "hit": 47.1},   # other dimension: ignored
    {"horizon": "1M", "cohort": "", "n": 1, "hit": 1}, "junk", None,
]


def _build(scorecard=SCORECARD, portfolio=True, crit=None):
    meta = {"rows_supplied_by": "harness"}
    if scorecard is not None:
        meta["evidence_scorecard"] = scorecard
    return ob.build_opportunity_payload(
        [dict(r) for r in ROWS], criteria={**CRIT, **(crit or {})},
        portfolio=observed_portfolio({"cash_available_sar": 100_000}) if portfolio else {},
        upstream_meta=meta)


def _gate(rec, name="Evidence (Scorecard)"):
    return next((g for g in rec["gates"] if g["gate"] == name), None)


# ---------------------------------------------------------------------------
# readers and pure helpers
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value,expected", ((None, "off"), ("", "off"), ("observe", "observe"), (" ENFORCE", "enforce"),
                                            ("on", "off"), ("1", "off")))
def test_mode_reader(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("TFB_T10_EVIDENCE_GATE", raising=False)
    else:
        monkeypatch.setenv("TFB_T10_EVIDENCE_GATE", value)
    assert ob._env_evidence_gate_mode() == expected


def test_floor_readers(monkeypatch):
    assert ob._env_evidence_min_hit() == 55.0 and ob._env_evidence_min_n() == 100
    monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_HIT", "60.5"); monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_N", "250")
    assert ob._env_evidence_min_hit() == 60.5 and ob._env_evidence_min_n() == 250
    monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_HIT", "150"); monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_N", "0")
    assert ob._env_evidence_min_hit() == 55.0 and ob._env_evidence_min_n() == 100
    monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_HIT", "junk"); monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_N", "junk")
    assert ob._env_evidence_min_hit() == 55.0 and ob._env_evidence_min_n() == 100


@pytest.mark.parametrize("months,hz", ((1, "1M"), (0.5, "1M"), (3, "3M"), (6, "6M"), (12, "1Y"), (24, "1Y"), (None, "1Y"), ("x", "1Y")))
def test_plan_horizon(months, hz):
    assert ob._evidence_horizon(months) == hz


def test_index_and_lookup():
    index, hzs = ob._evidence_index(SCORECARD)
    assert hzs == ["1M", "1Y", "3M"] and len(index) == 4
    assert index[("1Y", "INVESTABLE|BUY")] == (671, 63.3)
    assert ob._evidence_lookup(index, "INVESTABLE|BUY", "1Y") == (671, 63.3, "1Y")
    assert ob._evidence_lookup(index, "INVESTABLE|ACCUMULATE", "1Y") == (400, 48.0, "1M"), "fallback to the published horizon"
    assert ob._evidence_lookup(index, "INVESTABLE|SELL", "1Y") is None
    assert ob._evidence_lookup({}, "X", "1Y") is None
    assert ob._evidence_index(None) == ({}, []) and ob._evidence_index("junk") == ({}, [])
    alt, _ = ob._evidence_index([{"Horizon": "ALL", "Cohort": "investable|buy", "N": "10", "Hit Rate %": "50"}])
    assert alt == {("ALL", "INVESTABLE|BUY"): (10, 50.0)}
    assert ob.GATE_ORDER.index("Evidence (Scorecard)") == ob.GATE_ORDER.index("Earnings Proximity") + 1


# ---------------------------------------------------------------------------
# end to end
# ---------------------------------------------------------------------------
def test_off_is_byte_identical_and_has_no_gate():
    out = _build()
    assert out["version"] == ob.OPPORTUNITY_BUILDER_VERSION
    assert all(_gate(c) is None for c in out["candidates_rows"])
    assert "evidence_gate" not in out["meta"]
    assert not any("evidence-" in str(c.get("failure_reason")) for c in out["candidates_rows"])


def test_observe_tags_below_floor_rows_and_keeps_verdicts(monkeypatch):
    base = _build()
    monkeypatch.setenv("TFB_T10_EVIDENCE_GATE", "observe")
    out = _build()
    by = {c["symbol"]: c for c in out["candidates_rows"]}
    base_by = {c["symbol"]: c for c in base["candidates_rows"]}
    for sym, c in by.items():
        assert c["verdict"] == base_by[sym]["verdict"] and _gate(c)["passed"] is True
    assert _gate(by["GOOD.SR"])["note"] == "[evidence-observe] cohort=INVESTABLE|BUY@1Y hit=63.3% n=671"
    assert "evidence-" not in str(by["GOOD.SR"]["failure_reason"]), "an ok cohort is not tagged"
    assert by["WEAK.SR"]["failure_reason"] == "[evidence-observe] cohort=INVESTABLE|ACCUMULATE@1M hit=48.0% n=400 BELOW"
    assert by["THIN.SR"]["failure_reason"] == "[evidence-observe] cohort=INVESTABLE|STRONG_BUY@3M hit=70.0% n=40 BELOW"
    assert by["NEW.SR"]["failure_reason"] == "[evidence-observe] cohort=INVESTABLE|HOLD no outcomes yet"
    assert by["WL.SR"]["failure_reason"].startswith("Investability: WATCHLIST")
    assert by["WL.SR"]["failure_reason"].endswith(" | [evidence-observe] cohort=WATCHLIST|HOLD@1M hit=47.0% n=5000 BELOW")
    assert _gate(by["THIN.SR"])["evidence_detail"] == {"cohort": "INVESTABLE|STRONG_BUY", "horizon": "3M", "n": 40,
                                                        "hit": 70.0, "ok": False, "reason": "n_below"}
    assert [t["symbol"] for t in out["selected"]] == [t["symbol"] for t in base["selected"]], "observe never moves a seat"
    meta = out["meta"]["evidence_gate"]
    assert meta == {"mode": "observe", "min_hit": 55.0, "min_n": 100, "horizon": "1Y", "scorecard_rows": 4,
                    "evaluated": 5, "no_scorecard": 0, "no_cohort": 1, "would_fail": 4, "failed": 0}


def test_enforce_fails_below_floor_rows_and_removes_seats(monkeypatch):
    base = _build()
    assert sorted(t["symbol"] for t in base["selected"]) == ["GOOD.SR", "NEW.SR", "THIN.SR", "WEAK.SR"]
    monkeypatch.setenv("TFB_T10_EVIDENCE_GATE", "enforce")
    out = _build()
    by = {c["symbol"]: c for c in out["candidates_rows"]}
    assert _gate(by["GOOD.SR"])["passed"] is True and by["GOOD.SR"]["verdict"] == "INVEST"
    for sym in ("WEAK.SR", "THIN.SR", "NEW.SR"):
        assert _gate(by[sym])["passed"] is False and _gate(by[sym])["fail_class"] == "NON_CRITICAL"
        assert by[sym]["verdict"] != "INVEST"
    assert [t["symbol"] for t in out["selected"]] == ["GOOD.SR"]
    assert by["WEAK.SR"]["first_fail"]["gate"] == "Evidence (Scorecard)"
    assert by["WEAK.SR"]["failure_reason"].startswith("Evidence (Scorecard): hit=48.0% n=400 vs hit >= 55% and n >= 100")
    assert by["WEAK.SR"]["failure_reason"].endswith(" | [evidence-enforce] cohort=INVESTABLE|ACCUMULATE@1M hit=48.0% n=400 BELOW")
    meta = out["meta"]["evidence_gate"]
    assert meta["failed"] == 4 and meta["would_fail"] == 4 and meta["mode"] == "enforce"
    assert "Evidence (Scorecard)" in out["meta"]["gate_trace_counts"]


def test_no_scorecard_passes_and_is_counted(monkeypatch):
    monkeypatch.setenv("TFB_T10_EVIDENCE_GATE", "enforce")
    out = _build(scorecard=None)
    by = {c["symbol"]: c for c in out["candidates_rows"]}
    assert all(_gate(c)["passed"] is True for c in by.values())
    assert _gate(by["GOOD.SR"])["note"] == "[evidence-enforce] cohort=INVESTABLE|BUY no scorecard"
    assert not any("evidence-" in str(c["failure_reason"]) for c in by.values()), "absent evidence is not tagged"
    assert out["meta"]["evidence_gate"]["no_scorecard"] == 5 and out["meta"]["evidence_gate"]["scorecard_rows"] == 0
    assert sorted(t["symbol"] for t in out["selected"]) == ["GOOD.SR", "NEW.SR", "THIN.SR", "WEAK.SR"]


def test_floors_and_horizon_follow_env_and_criteria(monkeypatch):
    monkeypatch.setenv("TFB_T10_EVIDENCE_GATE", "observe")
    monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_HIT", "45"); monkeypatch.setenv("TFB_T10_EVIDENCE_MIN_N", "30")
    out = _build(crit={"period_months": 3})
    by = {c["symbol"]: c for c in out["candidates_rows"]}
    assert out["meta"]["evidence_gate"]["horizon"] == "3M"
    assert _gate(by["THIN.SR"])["evidence_detail"]["ok"] is True, "n=40 >= 30 and hit 70 >= 45"
    assert _gate(by["WEAK.SR"])["evidence_detail"]["ok"] is True
    assert _gate(by["GOOD.SR"])["evidence_detail"]["horizon"] == "1Y", "3M absent for BUY -> longest published"
    assert out["meta"]["evidence_gate"]["would_fail"] == 1, "only the no-cohort row"


def test_withheld_path_carries_tags_and_read_back(monkeypatch):
    monkeypatch.setenv("TFB_T10_EVIDENCE_GATE", "observe")
    out = _build(portfolio=False)
    assert out["meta"]["execution_ready"] is False
    by = {c["symbol"]: c for c in out["candidates_rows"]}
    assert by["WEAK.SR"]["failure_reason"].endswith("hit=48.0% n=400 BELOW")
    assert out["meta"]["evidence_gate"]["mode"] == "observe" and out["meta"]["evidence_gate"]["would_fail"] == 4


def test_route_forwards_the_scorecard_copy():
    src = Path("routes/advanced_analysis.py").read_text(encoding="utf-8")
    assert '"evidence_scorecard": merged_body.get("evidence_scorecard"),' in src
