# -*- coding: utf-8 -*-
"""tests/test_pf_total_return.py

portfolio_actions v1.20.0 [TOTAL-RETURN]: a holding row whose ledger "Dividends Recv" is counted also carries
dividends_sar, total_pnl_sar and total_pnl_pct, and the KPIs carry total_pnl_sar / total_pnl_pct. Owner, 19 Nov
2026: Portfolio_Decision still printed a holding's price-only loss after a special dividend he had been paid.

Executes the REAL core.analysis.portfolio_actions. Every figure below is synthetic.
"""
import json
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import core.analysis.portfolio_actions as pa  # noqa: E402

_KEYS = ("TFB_PF_TOTAL_RETURN", "TFB_PF_DD_DIV_BASIS", "TFB_PF_DD_EXIT", "TFB_PF_ADD_LOSER_VETO", "TFB_PF_RESEARCH_VIEW")

# 100 shares bought at 35.00 (cost 3,500 native = 13,125 SAR at 3.75); the price is now 28.70 after a special
# dividend of 8.00 a share, of which 5.60 was kept after tax: 560 native = 2,100 SAR.
COST, MV, DIV = 13125.0, 10762.5, 2100.0
CAND = {"symbol": "PAID.US", "pnl_sar": MV - COST, "cost_sar": COST, "market_value_sar": MV, "dd_div_sar": DIV}
ROW = {"Symbol": "PAID.US", "Name": "Paid Out Inc.", "Current Price": 28.70, "Currency": "USD", "Quantity": 100,
       "Buy Price": 35.00, "Buy Date": "2026-11-02"}
OTHER = {"Symbol": "OTHER.US", "Name": "Other Co", "Current Price": 50.0, "Currency": "USD", "Quantity": 40,
         "Buy Price": 48.0, "Buy Date": "2026-10-01"}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)


def _build(rows):
    return pa.build_portfolio_actions([dict(r) for r in rows], fx_rates={"USD": 3.75})


def _acts(payload):
    return {a.get("symbol"): a for a in payload.get("actions", [])}


def test_version_and_manifest_pin():
    assert pa.PORTFOLIO_ACTIONS_VERSION == "1.20.1"
    manifest = open(os.path.join(_ROOT, "scripts", "verify_deployment.py"), encoding="utf-8").read()
    assert '"1.20.1", "portfolio actions")' in manifest
    ci = open(os.path.join(_ROOT, ".github", "workflows", "ci.yml"), encoding="utf-8").read()
    assert "tests/test_pf_total_return.py" in ci


def test_the_fields_of_one_row():
    f = pa._total_return_fields(CAND)
    assert f == {"dividends_sar": 2100.0, "total_pnl_sar": -262.0, "total_pnl_pct": -2.0}
    assert round((MV - COST) / COST * 100, 1) == -18.0          # the price-only figure the row keeps in pnl_pct


@pytest.mark.parametrize("cand", [
    dict(CAND, dd_div_sar=None), dict(CAND, dd_div_sar=0.0), dict(CAND, dd_div_sar=-5.0),
    dict(CAND, dd_div_sar=COST + 1.0), dict(CAND, pnl_sar=None), dict(CAND, cost_sar=None), dict(CAND, cost_sar=0.0),
    None, {}])
def test_no_counted_dividend_no_fields(cand):
    assert pa._total_return_fields(cand) == {}


@pytest.mark.parametrize("env", [("TFB_PF_TOTAL_RETURN", "off"), ("TFB_PF_TOTAL_RETURN", "0"),
                                 ("TFB_PF_TOTAL_RETURN", "FALSE"), ("TFB_PF_TOTAL_RETURN", " no "),
                                 ("TFB_PF_DD_DIV_BASIS", "off")])
def test_the_gates(monkeypatch, env):
    monkeypatch.setenv(*env)
    assert pa._total_return_fields(CAND) == {}
    assert pa._total_return_kpis([CAND]) == {}


def test_kpis_over_the_rows_with_cost_and_pnl():
    plain = {"symbol": "X.US", "pnl_sar": 300.0, "cost_sar": 6875.0}
    blank = {"symbol": "Y.US", "pnl_sar": None, "cost_sar": 4000.0, "dd_div_sar": 500.0}
    k = pa._total_return_kpis([CAND, plain, blank])
    assert k == {"total_pnl_sar": round(MV - COST + DIV + 300.0, 0),
                 "total_pnl_pct": round((MV - COST + DIV + 300.0) / (COST + 6875.0) * 100, 1)}
    assert pa._total_return_kpis([plain]) == {}                  # nobody received a dividend
    assert pa._total_return_kpis([]) == {} and pa._total_return_kpis(None) == {}


def test_end_to_end_the_row_and_the_kpis(monkeypatch):
    # the live page runs the research view (TFB_PF_RESEARCH_VIEW=enforce): rows carry cost and P&L
    monkeypatch.setenv("TFB_PF_RESEARCH_VIEW", "enforce")
    base = _build([ROW, OTHER])
    paid = _build([dict(ROW, **{"Dividends Recv": 560.0}), OTHER])
    a, b = _acts(paid), _acts(base)
    assert a["OTHER.US"] == b["OTHER.US"]                        # a row without a dividend is untouched
    for key in ("pnl_sar", "pnl_pct", "cost_sar", "market_value_sar"):
        assert a["PAID.US"].get(key) == b["PAID.US"].get(key), key   # the price-only fields stay
    assert "total_pnl_pct" not in b["PAID.US"] and "dividends_sar" not in b["PAID.US"]
    row = a["PAID.US"]
    assert row["pnl_sar"] == -2362.0 and row["pnl_pct"] == -18.0
    assert row["dividends_sar"] == 2100.0
    assert row["total_pnl_sar"] == round(row["pnl_sar"] + 2100.0, 0)
    assert row["total_pnl_pct"] == round((row["pnl_sar"] + 2100.0) / row["cost_sar"] * 100, 1)
    kb, kp = base["kpis"], paid["kpis"]
    assert "total_pnl_sar" not in kb and "total_pnl_pct" not in kb
    assert kp["pnl_sar"] == -2062.0 and kp["total_pnl_sar"] == 38.0 and kp["total_pnl_pct"] == 0.2
    for key in kb:
        assert kp.get(key) == kb.get(key), key                   # every 1.19.0 KPI is unchanged


def test_gate_off_restores_1_19_0_byte_for_byte(monkeypatch):
    monkeypatch.setenv("TFB_PF_RESEARCH_VIEW", "enforce")
    monkeypatch.setenv("TFB_PF_TOTAL_RETURN", "off")
    off = _build([dict(ROW, **{"Dividends Recv": 560.0}), OTHER])
    monkeypatch.delenv("TFB_PF_TOTAL_RETURN")
    on = _build([dict(ROW, **{"Dividends Recv": 560.0}), OTHER])
    strip = lambda p: json.dumps({"a": [{k: v for k, v in r.items() if k not in ("dividends_sar", "total_pnl_sar", "total_pnl_pct")}
                                         for r in p["actions"]],
                                   "k": {k: v for k, v in p["kpis"].items() if k not in ("total_pnl_sar", "total_pnl_pct")}},
                                  default=str, sort_keys=True)
    assert strip(off) == strip(on)
    assert "total_pnl" not in json.dumps(off["actions"]) + json.dumps(off["kpis"])


def test_never_raises_on_junk():
    assert pa._total_return_fields({"pnl_sar": "x", "cost_sar": "y", "dd_div_sar": "z"}) == {}
    assert pa._total_return_kpis([{"pnl_sar": "x", "cost_sar": object()}]) == {}


@pytest.mark.parametrize("mode", ["", "observe"])
def test_a_row_whose_pnl_is_withheld_gets_no_total(monkeypatch, mode):
    if mode:
        monkeypatch.setenv("TFB_PF_RESEARCH_VIEW", mode)
    row = _acts(_build([dict(ROW, **{"Dividends Recv": 560.0})]))["PAID.US"]
    assert row.get("pnl_sar") is None
    assert "total_pnl_pct" not in row and "dividends_sar" not in row

