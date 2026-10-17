"""T2 (2026-10-17): opportunity_builder v1.29.0 — concentration: disclose the
cluster. Every audit row gains "cluster" ("<Industry or Sector> ×N" on
qualified rows, "" elsewhere), meta["concentration"] ranks the clusters and
ONE {"type": "concentration"} alert fires when the top cluster holds > 50% of
a qualified set of >= 4 names. Present on the certified and the withheld
path; TFB_T10_CLUSTER_LEGACY=1 removes all three (v1.28.0 payload save the
version stamp). No gate, verdict, selection, sizing or score change.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

from core.analysis import opportunity_builder as ob

sys.path.insert(0, str(Path(__file__).resolve().parent))
from decision_evidence_fixtures import observed_price_fields, observed_portfolio  # noqa: E402

CRIT = {"investability_gate_enabled": True, "blocked_identity_gate_enabled": True}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("TFB_T10_CLUSTER_LEGACY", raising=False)
    monkeypatch.delenv("TFB_T10_INVEST_REASONS_LEGACY", raising=False)


def _row(symbol, **over):
    row = {
        **observed_price_fields(),
        "symbol": symbol, "name": symbol + " Co", "sector": "Financials", "market": "Tadawul",
        "currency": "SAR", "current_price": 100.0, "intrinsic_value": 130.0,
        "forecast_reliability_score": 82.0, "data_quality_score": 91.0, "risk_bucket": "Moderate",
        "provider_engine_conflict": "No", "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": 24.0, "recommendation_detailed": "STRONG BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
    }
    row.update(over)
    return row


# ---------------------------------------------------------------------------
# pure core
# ---------------------------------------------------------------------------
def _rec(symbol, verdict="INVEST", sector="Financials", industry="Banks - Regional"):
    return {"symbol": symbol, "verdict": verdict, "sector": sector, "_cand": {"industry": industry}}


@pytest.mark.parametrize("rec,expected", (
    (_rec("A"), ("Banks - Regional", "industry")),
    (_rec("A", industry="  Banks -   Regional "), ("Banks - Regional", "industry")),
    (_rec("A", industry=""), ("Financials", "sector")),
    (_rec("A", industry="Unknown"), ("Financials", "sector")),
    (_rec("A", industry=None, sector="unknown"), ("", "")),
    (_rec("A", industry="", sector=""), ("", "")),
    ({"symbol": "A", "verdict": "INVEST", "sector": "Energy", "industry": "Oil & Gas"}, ("Oil & Gas", "industry")),
    ({"symbol": "A", "verdict": "INVEST", "sector": "Energy", "_cand": "junk"}, ("Energy", "sector")),
))
def test_cluster_key(rec, expected):
    assert ob._cluster_key(rec) == expected


def test_breakdown_stamps_every_row_and_ranks_clusters_with_examples():
    rows = ([_rec("B%d" % i) for i in range(19)]
            + [_rec("R%d" % i, sector="Real Estate", industry="REIT - Diversified") for i in range(3)]
            + [_rec("E1", sector="Energy", industry=""), _rec("U1", sector="Unknown", industry="")]
            + [_rec("W1", verdict="WATCH"), _rec("D1", verdict="DO_NOT_INVEST", industry="")]
            + ["junk", None])
    out = ob.concentration_breakdown(rows)
    assert out["qualified"] == 24
    assert [(c["cluster"], c["basis"], c["count"], c["share_pct"]) for c in out["clusters"]] == [
        ("Banks - Regional", "industry", 19, 79.2), ("REIT - Diversified", "industry", 3, 12.5), ("Energy", "sector", 1, 4.2)]
    assert out["clusters"][0]["examples"] == ["B0", "B1", "B2"]
    assert out["top"]["cluster"] == "Banks - Regional" and out["alert"] is True
    assert out["threshold_pct"] == 50.0 and out["min_qualified"] == 4
    dict_rows = [r for r in rows if isinstance(r, dict)]
    assert all("cluster" in r for r in dict_rows), "every dict row is stamped"
    assert {r["cluster"] for r in dict_rows[:19]} == {"Banks - Regional ×19"}
    assert dict_rows[19]["cluster"] == "REIT - Diversified ×3"
    assert dict_rows[22]["cluster"] == "Energy ×1"
    assert dict_rows[23]["cluster"] == "", "Unknown never clusters"
    assert dict_rows[24]["cluster"] == "" and dict_rows[25]["cluster"] == "", "non-qualified rows blank"
    assert sum(c["count"] for c in out["clusters"]) == 23 == out["qualified"] - 1


@pytest.mark.parametrize("n_bank,n_other,alert", (
    (3, 0, False),   # 100% of 3: below the 4-name floor
    (3, 1, True),    # 75% of 4
    (2, 2, False),   # exactly 50% is not > 50%
    (19, 5, True),   # the 17-Oct board
    (0, 0, False),
))
def test_alert_rule(n_bank, n_other, alert):
    rows = [_rec("B%d" % i) for i in range(n_bank)] + [
        _rec("O%d" % i, sector="S%d" % i, industry="I%d" % i) for i in range(n_other)]
    out = ob.concentration_breakdown(rows)
    assert out["alert"] is alert
    if alert:
        a = ob._concentration_alert(out)
        assert a["type"] == "concentration" and a["count"] == n_bank
        assert ("= %d of %d qualified names" % (n_bank, n_bank + n_other)) in a["required_action"]
        assert "changes no selection" in a["required_action"]


def test_empty_and_odd_inputs_never_raise():
    assert ob.concentration_breakdown([]) == {"qualified": 0, "clusters": [], "top": None, "alert": False,
                                             "threshold_pct": 50.0, "min_qualified": 4}
    assert ob.concentration_breakdown(None)["qualified"] == 0
    out = ob.concentration_breakdown([_rec("B%d" % i, industry="I%d" % i) for i in range(12)])
    assert len(out["clusters"]) == 8 and out["alert"] is False


# ---------------------------------------------------------------------------
# end to end — the 17-Oct shape: 19 regional banks + 5 others qualified
# ---------------------------------------------------------------------------
BANKS = ["AX", "VLY", "FRST", "PGC", "AUB", "BLX", "CNOB", "RDN", "WTFC", "FBK",
         "UNTY", "PB", "UCB", "COLB", "EBC", "JMSB", "SPFI", "AMAL", "AVBC"]
ROWS = ([_row(s + ".SR", industry="Banks - Regional") for s in BANKS]
        + [_row("VICI.SR", sector="Real Estate", industry="REIT - Diversified"),
           _row("GLPI.SR", sector="Real Estate", industry="REIT - Diversified"),
           _row("CRC.SR", sector="Energy", industry="Oil & Gas E&P"),
           _row("ATAT.SR", sector="Consumer", industry=""),
           _row("0883.SR", sector="Energy", industry="Oil & Gas E&P"),
           _row("WL.SR", sector="Utilities", investability_status="WATCHLIST",
                block_reason="Engine recommends HOLD")])


def _build(portfolio=True):
    return ob.build_opportunity_payload(
        ROWS, criteria=CRIT,
        portfolio=observed_portfolio({"cash_available_sar": 100_000}) if portfolio else {})


def test_certified_path_carries_cluster_field_meta_and_one_alert():
    payload = _build()
    assert payload["version"] == ob.OPPORTUNITY_BUILDER_VERSION
    by = {c["symbol"]: c for c in payload["candidates_rows"]}
    assert sum(1 for c in by.values() if c["verdict"] == "INVEST") == 24
    assert by["AX.SR"]["cluster"] == "Banks - Regional ×19"
    assert by["VICI.SR"]["cluster"] == "REIT - Diversified ×2"
    assert by["ATAT.SR"]["cluster"] == "Consumer ×1", "no industry -> sector"
    assert by["WL.SR"]["cluster"] == "" and by["WL.SR"]["verdict"] != "INVEST"
    assert all("_cand" not in c for c in by.values())
    conc = payload["meta"]["concentration"]
    assert conc["qualified"] == 24 and conc["alert"] is True
    assert conc["top"] == {"cluster": "Banks - Regional", "basis": "industry", "count": 19, "share_pct": 79.2,
                           "examples": ["AX.SR", "VLY.SR", "FRST.SR"]}
    alerts = [a for a in payload["alerts"] if a["type"] == "concentration"]
    assert len(alerts) == 1 and alerts[0]["count"] == 19
    assert "Banks - Regional (industry) = 19 of 24 qualified names (79.2% > 50%)" in alerts[0]["required_action"]


def test_legacy_switch_is_byte_identical_save_the_additions(monkeypatch):
    on = _build()
    monkeypatch.setenv("TFB_T10_CLUSTER_LEGACY", "1")
    off = _build()
    assert "concentration" not in off["meta"]
    assert not any("cluster" in c for c in off["candidates_rows"])
    assert not any(a["type"] == "concentration" for a in off["alerts"])
    # nothing else moved: same tickets, verdicts, scores, deferrals, other alerts
    assert [t["symbol"] for t in on["selected"]] == [t["symbol"] for t in off["selected"]]
    assert [(c["symbol"], c["verdict"], c["opportunity_score"], c.get("deferral")) for c in on["candidates_rows"]] == \
           [(c["symbol"], c["verdict"], c["opportunity_score"], c.get("deferral")) for c in off["candidates_rows"]]
    assert [a for a in on["alerts"] if a["type"] != "concentration"] == off["alerts"]
    strip = lambda p: {k: v for k, v in p["meta"].items() if k not in ("concentration", "generated_at_utc")}  # noqa: E731
    assert strip(on) == strip(off)


def test_withheld_path_carries_the_same_read_back():
    payload = _build(portfolio=False)
    assert payload["meta"]["execution_ready"] is False
    by = {c["symbol"]: c for c in payload["candidates_rows"]}
    assert by["AX.SR"]["cluster"] == "Banks - Regional ×19" and by["WL.SR"]["cluster"] == ""
    assert payload["meta"]["concentration"]["top"]["count"] == 19
    types = [a["type"] for a in payload["alerts"]]
    assert types == ["portfolio_inputs_unverified", "concentration"]


def test_selection_is_untouched_by_the_disclosure():
    """The cap at selection still defers; the cluster field never changes it."""
    payload = _build()
    deferred = [c for c in payload["candidates_rows"] if c.get("deferral")]
    assert any("sector cap" in c["deferral"] for c in deferred)
    assert all(c["cluster"] for c in deferred), "deferred qualified rows still disclose their cluster"
