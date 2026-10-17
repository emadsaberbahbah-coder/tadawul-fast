"""T1 (2026-10-17): opportunity_builder v1.27.0 — WHY the Investability gate
fails. meta["investability_reasons"] is a pure breakdown of the written audit
rows whose FIRST failed gate is Investability (the rows the GAS DATA GAPS
'Investability' row counts), top 6 primary reasons + Other, counts summing
to that row; present on the certified and the withheld path; removed by
TFB_T10_INVEST_REASONS_LEGACY=1. No gate, verdict, ticket or score change.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

from core.analysis import opportunity_builder as ob

sys.path.insert(0, str(Path(__file__).resolve().parent))
from decision_evidence_fixtures import observed_price_fields, observed_portfolio  # noqa: E402

CRIT = {"investability_gate_enabled": True, "blocked_identity_gate_enabled": True}


def _row(symbol, investability="WATCHLIST", reason="", **over):
    row = {
        **observed_price_fields(),
        "symbol": symbol, "name": symbol + " Co", "sector": "Financials", "market": "US",
        "currency": "USD", "current_price": 100.0, "intrinsic_value": 130.0,
        "forecast_reliability_score": 82.0, "data_quality_score": 91.0, "risk_bucket": "Moderate",
        "provider_engine_conflict": "No", "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": 24.0, "recommendation_detailed": "STRONG BUY",
        "investability_status": investability, "block_reason": reason,
    }
    row.update(over)
    return row


# ---------------------------------------------------------------------------
# pure: reason normalisation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("text,expected", (
    ("Engine recommends SELL", "Engine recommends SELL"),
    ("Engine recommends STRONG_SELL", "Engine recommends STRONG_SELL"),
    ("Engine neutral (HOLD)", "Engine neutral"),
    ("Conservative gate: overall 61 < 68; risk 52 > 45", "Conservative gate"),
    ("Conservative gate: risk 52 > 45", "Conservative gate"),
    ("Data quality below floor (35)", "Data quality below floor"),
    ("Moderate data quality (64)", "Moderate data quality"),
    ("Incomplete fundamentals (D/E, FCF)", "Incomplete fundamentals"),
    ("Negative forecast (expected ROI -3.1%)", "Negative forecast"),
    ("Stale price bar (provider data 3 session(s) old)", "Stale price bar"),
    ("Strict gate: data quality 61 < 70; risk bucket HIGH", "Strict gate"),
    ("Scoring settlement failed; clean rebuild required | Engine recommends HOLD",
     "Scoring settlement failed; clean rebuild required"),
    ("Engine recommends REDUCE | Scoring settlement failed; clean rebuild required",
     "Engine recommends REDUCE"),
    ("Thin trading history (no 52W range) -- treat as new/IPO listing", "Thin trading history"),
    ("", "(no reason on row)"), (None, "(no reason on row)"), ("   ", "(no reason on row)"),
    (":", "(no reason on row)"), (12, "12"),
))
def test_primary_reason_normalisation(text, expected):
    assert ob._primary_block_reason(text) == expected


# ---------------------------------------------------------------------------
# pure: breakdown
# ---------------------------------------------------------------------------
def _audit(symbol, gate, reason):
    return {"symbol": symbol, "first_fail": {"gate": gate} if gate else None,
            "engine_gate": {"investability": "WATCHLIST", "reasons": reason}}


def test_breakdown_counts_only_first_fail_investability_and_sums_to_it():
    rows = ([_audit("S%d" % i, "Investability", "Engine recommends SELL") for i in range(5)]
            + [_audit("R%d" % i, "Investability", "Engine recommends REDUCE") for i in range(4)]
            + [_audit("H%d" % i, "Investability", "Engine neutral (HOLD)") for i in range(3)]
            + [_audit("C%d" % i, "Investability", "Conservative gate: overall %d < 68" % (60 + i)) for i in range(3)]
            + [_audit("A1", "Investability", "Engine recommends AVOID"),
               _audit("F1", "Investability", "Incomplete fundamentals (FCF)"),
               _audit("X1", "Investability", "Scoring settlement failed; clean rebuild required | Engine recommends HOLD"),
               _audit("N1", "Investability", ""),
               _audit("Q1", "Quote Freshness", "Engine recommends SELL"),   # a different first gate
               _audit("I1", None, "")]                                      # INVEST, no first_fail
            )
    out = ob.investability_reason_breakdown(rows)
    assert out["total"] == 19 and out["distinct_reasons"] == 8 and out["other_reasons"] == 2
    assert [r["reason"] for r in out["rows"]] == [
        "Engine recommends SELL", "Engine recommends REDUCE", "Conservative gate", "Engine neutral",
        "(no reason on row)", "Engine recommends AVOID", "Other (2 reasons)"]
    assert [r["count"] for r in out["rows"]] == [5, 4, 3, 3, 1, 1, 2]
    assert sum(r["count"] for r in out["rows"]) == out["total"] == 19
    assert out["rows"][0]["examples"] == ["S0", "S1", "S2"] and out["rows"][0]["share_pct"] == 26.3
    assert out["rows"][-1]["examples"] == [] and out["rows"][-1]["share_pct"] == 10.5
    assert abs(sum(r["share_pct"] for r in out["rows"]) - 100.0) < 0.5


def test_breakdown_is_empty_and_safe_on_no_rows():
    assert ob.investability_reason_breakdown([]) == {"total": 0, "rows": [], "other_reasons": 0, "distinct_reasons": 0}
    assert ob.investability_reason_breakdown(None)["total"] == 0
    assert ob.investability_reason_breakdown([{"symbol": "X"}, {"first_fail": "junk"}])["total"] == 0


def test_breakdown_top_and_examples_are_configurable():
    rows = [_audit("A%d" % i, "Investability", "R%d" % (i % 4)) for i in range(12)]
    out = ob.investability_reason_breakdown(rows, top=2, examples=1)
    assert len(out["rows"]) == 3 and out["rows"][-1]["reason"] == "Other (2 reasons)"
    assert all(len(r["examples"]) <= 1 for r in out["rows"])
    assert sum(r["count"] for r in out["rows"]) == 12


# ---------------------------------------------------------------------------
# end to end through build_opportunity_payload
# ---------------------------------------------------------------------------
ROWS = [
    _row("OK.US", "INVESTABLE", ""),
    _row("S1.US", reason="Engine recommends SELL"), _row("S2.US", reason="Engine recommends SELL"),
    _row("R1.US", reason="Engine recommends REDUCE"),
    _row("C1.US", reason="Conservative gate: overall 61 < 68"),
    _row("C2.US", reason="Conservative gate: risk 52 > 45"),
    _row("H1.US", reason="Engine neutral (HOLD)"),
    _row("N1.US", reason=""),
    # BLOCKED fails the Investability gate too, and Investability sits before
    # Blocked Identity in GATE_ORDER, so it is counted here under its reason.
    _row("BAD.US", "BLOCKED", reason="identity mismatch"),
]


def _gap_investability_count(payload):
    return sum(1 for c in payload["candidates_rows"]
               if (c.get("first_fail") or {}).get("gate") == "Investability")


def test_certified_payload_carries_the_breakdown_summing_to_the_gaps_row(monkeypatch):
    monkeypatch.delenv("TFB_T10_INVEST_REASONS_LEGACY", raising=False)
    payload = ob.build_opportunity_payload(ROWS, criteria=CRIT,
                                           portfolio=observed_portfolio({"cash_available_sar": 50_000}))
    block = payload["meta"]["investability_reasons"]
    assert block["total"] == _gap_investability_count(payload) == 8
    # ties sort by name: "Conservative gate" < "Engine recommends SELL"
    assert [(r["reason"], r["count"]) for r in block["rows"]] == [
        ("Conservative gate", 2), ("Engine recommends SELL", 2), ("(no reason on row)", 1),
        ("Engine neutral", 1), ("Engine recommends REDUCE", 1), ("identity mismatch", 1)]
    assert block["rows"][1]["examples"] == ["S1.US", "S2.US"]
    assert block["rows"][0]["examples"] == ["C1.US", "C2.US"]
    assert block["other_reasons"] == 0 and sum(r["count"] for r in block["rows"]) == 8
    # INVESTABLE rows never appear
    assert all("OK.US" not in r["examples"] for r in block["rows"])
    assert payload["version"] == "1.27.0"


def test_withheld_payload_carries_the_breakdown_too(monkeypatch):
    monkeypatch.delenv("TFB_T10_INVEST_REASONS_LEGACY", raising=False)
    payload = ob.build_opportunity_payload(ROWS, criteria=CRIT, portfolio={})
    assert payload["meta"]["execution_ready"] is False
    assert any(a["type"] == "portfolio_inputs_unverified" for a in payload["alerts"])
    block = payload["meta"]["investability_reasons"]
    assert block["total"] == _gap_investability_count(payload) == 8


def test_kill_switch_removes_the_key_and_changes_nothing_else(monkeypatch):
    monkeypatch.delenv("TFB_T10_INVEST_REASONS_LEGACY", raising=False)
    on = ob.build_opportunity_payload(ROWS, criteria=CRIT,
                                      portfolio=observed_portfolio({"cash_available_sar": 50_000}))
    monkeypatch.setenv("TFB_T10_INVEST_REASONS_LEGACY", "1")
    off = ob.build_opportunity_payload(ROWS, criteria=CRIT,
                                       portfolio=observed_portfolio({"cash_available_sar": 50_000}))
    assert "investability_reasons" not in off["meta"]
    strip = lambda p: {k: v for k, v in p.items() if k != "meta"}  # noqa: E731
    assert strip(on) == strip(off)
    on_meta = {k: v for k, v in on["meta"].items() if k not in ("investability_reasons", "generated_at_utc")}
    off_meta = {k: v for k, v in off["meta"].items() if k != "generated_at_utc"}
    assert on_meta == off_meta
    off_w = ob.build_opportunity_payload(ROWS, criteria=CRIT, portfolio={})
    assert "investability_reasons" not in off_w["meta"]
