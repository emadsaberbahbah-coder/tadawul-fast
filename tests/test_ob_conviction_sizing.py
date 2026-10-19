#!/usr/bin/env python3
"""tests/test_ob_conviction_sizing.py - opportunity_builder v1.27.0:
TFB_OPP_SIZING_MODE=conviction spreads the deployable budget by conviction.

WHY (2026-10-19)
----------------
_size_one funds every seat at min(MaxWeight% x budget, remaining), so the
live board funds the first seats at the per-seat cap and the rest with the
scraps. The owner wants the cash distributed by expected ROI with its risk,
an absolute cash buffer (existing TFB_OPP_CASH_FLOOR_SAR) and otherwise
everything invested.

  S1  default (unset / cap): payload identical to v1.26.2 and meta.sizing
      is exactly {"mode": "cap"}
  S2  conviction: every considered seat is funded, sizes follow conviction
      (higher plan ROI / lower risk / higher reliability first), each seat
      stays within the per-seat cap, the total never exceeds the budget
  S3  the absolute cash floor is honoured under conviction (enforce)
  S4  pure water-filling: caps re-spread, floor releases the weakest seat,
      all-zero weights fall back to equal, empty budget funds nothing
  S5  pure weight: ordering by ROI, risk and reliability; tilt 0 = equal;
      a missing ROI counts as the required minimum, never zero
  S6  cohort mirrors the loop: rank order, max_selected, sector cap,
      replay eligibility
  S7  top-up: the lot-rounding remainder goes to the highest-conviction seat
      within the cap; meta.sizing.topup_sar records it
  S8  the research stage is never sized under either mode
  S9  the new env reads live in the fingerprinted TFB_OPP_ family

Run: pytest -q tests/test_ob_conviction_sizing.py
"""
from __future__ import annotations

import ast
import copy
import inspect
import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "tests"))

from core.analysis import opportunity_builder as ob  # noqa: E402
from decision_evidence_fixtures import build_with_observed_inputs, observed_price_fields  # noqa: E402

CRITERIA = {
    "max_selected": 10,
    "max_per_sector": 10,
    "max_per_market": 10,
    "max_weight_pct": 100.0,
    "pf_max_sector_pct": 100.0,
    "min_ticket_sar": 1_000.0,
    "trust_gate_enabled": False,
}
FX = {"SAR": 1.0}
SECTORS = ("Energy", "Financials", "Utilities", "Materials", "Industrials")


def _row(symbol, upside_pct, *, risk="Low", rel=90.0, sector=None, price=100.0):
    row = {
        "symbol": symbol, "name": "Synthetic " + symbol,
        "sector": sector or SECTORS[hash(symbol) % len(SECTORS)], "market": "Tadawul",
        "currency": "SAR", "current_price": price,
        "intrinsic_value": round(price * (1.0 + upside_pct / 100.0), 4),
        "forecast_reliability_score": rel, "data_quality_score": 95.0,
        "risk_bucket": risk, "provider_engine_conflict": "No",
        "volatility_30d": 4.0, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": upside_pct, "recommendation_detailed": "STRONG BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
    }
    row.update(observed_price_fields())
    return row


ROWS = [
    _row("ALPHA.SR", 60.0, risk="Low", rel=95.0, sector="Energy"),
    _row("BRAVO.SR", 30.0, risk="Low", rel=80.0, sector="Financials"),
    _row("CHARLIE.SR", 20.0, risk="Moderate", rel=72.0, sector="Utilities"),
]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for key in list(os.environ):
        if key.startswith(("TFB_OPP_", "TFB_T10_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("APP_TOKEN", "synthetic-sizing-test")


def _build(rows=ROWS, cash=30_000.0, **criteria):
    return build_with_observed_inputs(
        ob, copy.deepcopy(rows), criteria={**CRITERIA, **criteria},
        portfolio={"cash_available_sar": cash}, fx_rates=dict(FX))


def _sizes(payload):
    return {t["symbol"]: float(t["suggested_sar"]) for t in payload["selected"]}


def test_s1_default_is_cap_and_byte_identical(monkeypatch):
    base = _build()
    assert base["status"] == "ok", base.get("message")
    assert base["meta"]["sizing"] == {"mode": "cap"}
    sizes = _sizes(base)
    # the legacy greedy cap: the first seat takes everything it may
    assert sizes and max(sizes.values()) == sizes["ALPHA.SR"]
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "cap")
    again = _build()
    assert _sizes(again) == sizes and again["meta"]["sizing"] == {"mode": "cap"}
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "nonsense")
    assert _sizes(_build()) == sizes


def test_s2_conviction_spreads_by_conviction(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    payload = _build()
    assert payload["status"] == "ok", payload.get("message")
    sizes = _sizes(payload)
    assert set(sizes) == {"ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"}, payload["near_miss"]
    assert sizes["ALPHA.SR"] > sizes["BRAVO.SR"] > sizes["CHARLIE.SR"] >= CRITERIA["min_ticket_sar"]
    assert sum(sizes.values()) <= 30_000.0 + 1e-6
    meta = payload["meta"]["sizing"]
    assert meta["mode"] == "conviction" and meta["tilt"] == 1.0
    assert meta["cohort"] == ["ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"]
    assert meta["weights"]["ALPHA.SR"] > meta["weights"]["BRAVO.SR"] > meta["weights"]["CHARLIE.SR"]
    assert set(meta["targets"]) == set(sizes)
    # the per-seat cap still binds: with a 40% cap no seat exceeds 12,000
    capped = _build(max_weight_pct=40.0)
    assert max(_sizes(capped).values()) <= 0.4 * (30_000.0) + 1e-6


def test_s2b_tilt_zero_is_an_equal_split(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    monkeypatch.setenv("TFB_OPP_SIZING_TILT", "0")
    sizes = _sizes(_build())
    assert len(sizes) == 3
    assert max(sizes.values()) - min(sizes.values()) <= 100.0 * 1  # one share of lot rounding


def test_s3_absolute_cash_floor_is_kept_under_conviction(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    monkeypatch.setenv("TFB_OPP_CASH_FLOOR_SAR", "3750")
    monkeypatch.setenv("TFB_OPP_CASH_FLOOR_MODE", "enforce")
    payload = _build()
    sizes = _sizes(payload)
    assert sum(sizes.values()) <= 30_000.0 - 3_750.0 + 1e-6
    assert payload["meta"]["sizing"]["budget_sar"] <= 30_000.0 - 3_750.0


def test_s3b_absolute_floor_without_enforce_is_unchanged(monkeypatch):
    # v1.16.0 contract kept: SAR set, mode unset -> the reserve only labels
    # funding; the sizing budget, meta and alerts are byte-identical to no floor
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    plain = _build()
    monkeypatch.setenv("TFB_OPP_CASH_FLOOR_SAR", "3750")
    floored = _build()
    assert _sizes(floored) == _sizes(plain)
    assert "cash_floor" not in floored["meta"]
    assert not [a for a in floored["alerts"] if a["type"] == "cash_floor"]
    monkeypatch.setenv("TFB_OPP_CASH_FLOOR_MODE", "enforce")
    enforced = _build()
    assert sum(_sizes(enforced).values()) <= 30_000.0 - 3_750.0 + 1e-6
    assert enforced["meta"]["cash_floor"]["mode"] == "enforce"
    assert enforced["meta"]["cash_floor"]["pct"] is None
    alert = [a for a in enforced["alerts"] if a["type"] == "cash_floor"]
    assert alert and "absolute reserve TFB_OPP_CASH_FLOOR_SAR" in alert[0]["required_action"] \
        and "ENFORCED" in alert[0]["required_action"] and "None" not in alert[0]["required_action"]
    # the cap path honours the same enforced reserve
    monkeypatch.delenv("TFB_OPP_SIZING_MODE", raising=False)
    assert sum(_sizes(_build()).values()) <= 30_000.0 - 3_750.0 + 1e-6


def test_s4_water_filling_rules():
    w = {"A": 3.0, "B": 2.0, "C": 1.0}
    t = ob._conviction_targets(w, 6000.0, 10_000.0, 0.0)
    assert t == {"A": 3000.0, "B": 2000.0, "C": 1000.0}
    # cap binds on A; its surplus is re-spread over B and C in proportion
    t = ob._conviction_targets(w, 6000.0, 2400.0, 0.0)
    assert t["A"] == 2400.0 and abs(t["B"] - 2400.0) < 0.01 and abs(t["C"] - 1200.0) < 0.01
    # floor releases the weakest seat and re-spreads
    t = ob._conviction_targets(w, 6000.0, 10_000.0, 1500.0)
    assert set(t) == {"A", "B"} and abs(t["A"] - 3600.0) < 0.01 and abs(t["B"] - 2400.0) < 0.01
    # nothing can be funded above the floor
    assert ob._conviction_targets(w, 1000.0, 10_000.0, 900.0) == {"A": 1000.0}
    assert ob._conviction_targets(w, 1000.0, 10_000.0, 1500.0) == {}
    # all-zero weights fall back to an equal split; empty budget funds nothing
    assert ob._conviction_targets({"A": 0, "B": 0}, 1000.0, 10_000.0) == {"A": 500.0, "B": 500.0}
    assert ob._conviction_targets(w, 0.0, 10_000.0) == {}
    assert ob._conviction_targets({}, 1000.0, 10_000.0) == {}
    assert ob._conviction_targets(w, "bad", 1.0) == {}


def test_s5_weight_ordering_and_fallbacks():
    crit = {"required_roi_pct": 12.0}
    low = ob._conviction_weight({"roi_pct": 20.0, "risk_level": "Low", "reliability": 90.0}, crit, 1.0)
    med = ob._conviction_weight({"roi_pct": 20.0, "risk_level": "Medium", "reliability": 90.0}, crit, 1.0)
    high = ob._conviction_weight({"roi_pct": 20.0, "risk_level": "High", "reliability": 90.0}, crit, 1.0)
    assert low > med > high > 0
    assert ob._conviction_weight({"roi_pct": 40.0, "risk_level": "Low", "reliability": 90.0}, crit, 1.0) == 2 * low
    weaker_rel = ob._conviction_weight({"roi_pct": 20.0, "risk_level": "Low", "reliability": 70.0}, crit, 1.0)
    assert weaker_rel < low
    # missing / non-positive ROI counts as the required minimum, never zero
    assert ob._conviction_weight({"roi_pct": None, "risk_level": "Low", "reliability": 90.0}, crit, 1.0) == \
        ob._conviction_weight({"roi_pct": 12.0, "risk_level": "Low", "reliability": 90.0}, crit, 1.0)
    assert ob._conviction_weight({"roi_pct": -5.0, "risk_level": "Low", "reliability": None}, {}, 1.0) > 0
    # tilt 0 => every positive weight is 1 (equal split); tilt 2 widens the spread
    assert ob._conviction_weight({"roi_pct": 20.0, "risk_level": "Low", "reliability": 90.0}, crit, 0.0) == 1.0
    assert ob._conviction_weight({"roi_pct": 40.0, "risk_level": "Low", "reliability": 90.0}, crit, 2.0) == 4 * \
        ob._conviction_weight({"roi_pct": 20.0, "risk_level": "Low", "reliability": 90.0}, crit, 2.0)


def test_s5b_tilt_env_parsing(monkeypatch):
    for raw, want in (("", 1.0), ("1", 1.0), ("0", 0.0), ("2.5", 2.5), ("3", 3.0),
                      ("4", 1.0), ("-1", 1.0), ("abc", 1.0), ("nan", 1.0)):
        monkeypatch.setenv("TFB_OPP_SIZING_TILT", raw)
        assert ob._env_sizing_tilt() == want, raw


def test_s6_cohort_mirrors_the_loop():
    def c(sym, sector, market="Tadawul"):
        return {"symbol": sym, "sector": sector, "market": market}
    cands = [c("A", "Energy"), c("B", "Energy"), c("C", "Energy"), c("D", "Utilities"),
             c("E", "Materials", "NYSE/NASDAQ"), c("F", "Industrials", "NASDAQ/NYSE")]
    crit = {"max_selected": 4, "max_per_sector": 2, "max_per_market": 1}
    got = [x["symbol"] for x in ob._sizing_cohort(cands, crit, False, set(), True)]
    # B, C and D are market-capped (Tadawul already holds A; C also sector-
    # capped), E takes the one US slot, F (same canonical market) is capped
    assert got == ["A", "E"]
    crit = {"max_selected": 4, "max_per_sector": 2, "max_per_market": 10}
    got = [x["symbol"] for x in ob._sizing_cohort(cands, crit, False, set(), True)]
    assert got == ["A", "B", "D", "E"]       # stops at max_selected
    got = [x["symbol"] for x in ob._sizing_cohort(cands, crit, True, {"B", "E"}, True)]
    assert got == ["B", "E"]                 # replay: eligible seats only


def test_s7_topup_invests_the_rounding_remainder(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    # prices that cannot divide the targets evenly leave a remainder
    rows = [_row("ALPHA.SR", 60.0, price=99.7, sector="Energy"),
            _row("BRAVO.SR", 30.0, price=99.1, rel=80.0, sector="Financials"),
            _row("CHARLIE.SR", 20.0, price=98.3, rel=72.0, risk="Moderate", sector="Utilities")]
    payload = _build(rows, cash=30_000.0)
    sizes = _sizes(payload)
    assert set(sizes) == {"ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"}
    total = sum(sizes.values())
    assert total <= 30_000.0 + 1e-6
    # what is left is less than one share of the cheapest funded seat
    assert 30_000.0 - total < 99.7
    assert payload["meta"]["sizing"]["topup_sar"] >= 0.0
    for t in payload["selected"]:
        assert float(t["suggested_sar"]) <= 30_000.0 * 1.0 + 1e-6
    # with the per-seat cap at 34%, the top-up never breaches it
    capped = _build(rows, cash=30_000.0, max_weight_pct=34.0)
    assert max(_sizes(capped).values()) <= 0.34 * 30_000.0 + 1e-6


def test_s8_research_stage_is_never_sized(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    payload = _build(board_funding_stage="research")
    assert payload["status"] == "ok", payload.get("message")
    assert payload["meta"]["sizing"]["mode"] == "conviction"
    assert "targets" not in payload["meta"]["sizing"]


def test_s10_frozen_snapshot_replay_sizes_only_the_eligible_seats(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    research = _build(board_funding_stage="research")
    assert research["status"] == "ok", research.get("message")
    frozen = copy.deepcopy(research["meta"]["board_funding"]["snapshot"])
    allocated = _build(frozen["rows"], board_funding_stage="allocate",
                       board_funding_symbols=["ALPHA.SR", "CHARLIE.SR"],
                       board_funding_snapshot=frozen)
    assert allocated["status"] == "ok", allocated.get("message")
    sizes = _sizes(allocated)
    assert set(sizes) == {"ALPHA.SR", "CHARLIE.SR"}
    assert sizes["ALPHA.SR"] > sizes["CHARLIE.SR"] >= CRITERIA["min_ticket_sar"]
    assert sum(sizes.values()) <= 30_000.0 + 1e-6
    meta = allocated["meta"]["sizing"]
    assert meta["cohort"] == ["ALPHA.SR", "CHARLIE.SR"] and "BRAVO.SR" not in meta["targets"]


def test_s9_env_reads_are_in_the_fingerprinted_family():
    names = set()
    for node in ast.walk(ast.parse(inspect.getsource(ob))):
        if isinstance(node, ast.Call) and node.args:
            fn = (node.func.attr if isinstance(node.func, ast.Attribute)
                  else node.func.id if isinstance(node.func, ast.Name) else "")
            if (fn == "getenv" or fn.startswith("_env_")) and isinstance(node.args[0], ast.Constant):
                names.add(str(node.args[0].value))
    assert {"TFB_OPP_SIZING_MODE", "TFB_OPP_SIZING_TILT"} <= names
    assert all(n.startswith(ob._BOARD_FUNDING_POLICY_PREFIXES) for n in ("TFB_OPP_SIZING_MODE", "TFB_OPP_SIZING_TILT"))
    # 2026-10-19 merge of #778: version floor instead of an exact pin.
    assert tuple(int(x) for x in ob.OPPORTUNITY_BUILDER_VERSION.split(".")[:3]) >= (1, 27, 0)


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
