"""C1 / C3 (2026-10-17): portfolio_actions v1.17.0 — HOLD-uncertified, not
BLOCK-with-zeros, and the ledger cash fallback. Gate TFB_PF_UNCERTIFIED_VIEW
= off (default) | on.

Synthetic holdings and invented packets only. The 17-Oct shape: three US
rows, no fresh capture (age 1497 m), panel cash 0, _Cash_Snapshot 53,769.26.
The 16-Oct shape: a fresh packet -> the certified build, byte-identical with
the gate on or off.
"""
from __future__ import annotations

import copy
import datetime as dt
import inspect
import json

import pytest

from core.analysis import portfolio_actions as pa

ACCOUNT = "acct-synthetic-sentinel"
INSTRUMENT = "instr-synthetic-%d"
FX = {"USD": 3.75, "SAR": 1}
CTL = {"cash_available_sar": 0, "max_position_pct": 15, "max_sector_pct": 30,
       "target_cash_pct": 10, "min_reliability_add": 60, "min_dq_add": 60, "review_days": 30}
CLEARED = ("TFB_PF_UNCERTIFIED_VIEW", "TFB_PF_RESEARCH_VIEW", "TFB_PF_DD_EXIT", "TFB_PF_SWITCH_SCAN",
           "TFB_PF_ADD_LOSER_VETO", "TFB_PF_CONFIRM_SESSION", "TFB_PF_F1_OBSERVE", "TFB_PF_ENABLED")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in CLEARED:
        monkeypatch.delenv(key, raising=False)


def _stamp(age_s=0):
    when = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=age_s)
    return when.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def holding(symbol, qty, buy, price, **updates):
    stamp = _stamp()
    row = {"Symbol": symbol, "Currency": "USD", "Position Qty": qty, "Buy Price": buy,
           "Current Price": price, "Intrinsic Value": round(price * 1.04, 2), "Sector": "Financials",
           "Market": "NYSE/NASDAQ", "Forecast Reliability Score": 85, "Data Quality Score": 90,
           "Recommendation": "HOLD", "Investability Status": "INVESTABLE", "Risk Bucket": "Moderate",
           "Data Provider": "EODHD", "Last Updated": stamp,
           "Warnings": ("acquisition_status:success; acquisition_provider:EODHD; "
                        "acquisition_acquired_at:" + stamp + "; acquisition_quote_asof:" + stamp)}
    row.update(updates)
    return row


def rows_17oct():
    # ledger (cost basis) 11,860 / 9,664 / 7,943 SAR at the 3.75 peg
    return [holding("DDI.US", 200, 15.50, 15.81),            # MV 200*15.81*3.75 = 11,857.5
            holding("CWBC.US", 100, 25.00, 25.77, Sector="Banks"),   # 9,663.75
            holding("KRP.US", 150, 13.90, 14.12, Sector="Energy")]   # 7,942.5


def packet(rows, age_s=0, cash=53769.26, qty=None):
    stamp = _stamp(age_s)
    qty = qty or {}
    return {"schema_version": 1, "source_ref": "synthetic://sentinel", "captured_at": stamp,
            "accounts": [{"account_id": ACCOUNT, "positions_complete": True, "positions_asof": stamp,
                          "positions": [{"instrument_id": INSTRUMENT % i, "symbol": r["Symbol"],
                                         "currency": "USD", "quantity": qty.get(i, r["Position Qty"])}
                                        for i, r in enumerate(rows)],
                          "cash_complete": True, "cash_asof": stamp,
                          "cash": [{"currency": "SAR", "settled_cash": cash, "reserved_cash": 0,
                                    "reservations_complete": True}]}],
            "holding_links": [{"row_index": i, "account_id": ACCOUNT, "instrument_id": INSTRUMENT % i,
                               "position_symbol": r["Symbol"], "symbol": r["Symbol"], "currency": "USD"}
                              for i, r in enumerate(rows)],
            "funding_accounts": [ACCOUNT],
            "fx": [{"currency": "USD", "rate_to_sar": 3.75, "asof": stamp, "source_ref": "peg"}]}


CONTEXT = {"captured_at": "2026-10-16T05:41:07Z", "age_min": 1497, "limit_min": 14,
           "reason": "capture 16-10 08:41 is 24h 57m old (limit 14m)"}
SNAPSHOT = {"balance_sar": 53769.26, "as_of": "2026-10-16"}


def build(rows=None, evidence=None, controls=None, context=None, snapshot=None):
    meta = {"rows_supplied_by": "harness"}
    if evidence is not None:
        meta["reconciliation_evidence"] = copy.deepcopy(evidence)
    if context is not None:
        meta["capture_context"] = copy.deepcopy(context)
    if snapshot is not None:
        meta["cash_snapshot"] = copy.deepcopy(snapshot)
    ctl = dict(CTL); ctl.update(controls or {})
    out = pa.build_portfolio_actions(copy.deepcopy(rows if rows is not None else rows_17oct()), ctl, dict(FX), meta)
    out["meta"].pop("generated_utc", None)
    return out


def norm(payload):
    return json.dumps(payload, sort_keys=True, default=str)


# ---------------------------------------------------------------------------
# readers
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value,expected", (
    (None, False), ("", False), ("on", True), (" ON ", True), ("off", False), ("observe", False),
    ("1", False), ("true", False), ("enforce", False),
))
def test_gate_reader(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("TFB_PF_UNCERTIFIED_VIEW", raising=False)
    else:
        monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", value)
    assert pa._env_uncertified_view() is expected


def test_cash_fallback_order():
    assert pa._uncertified_cash({"cash_available_sar": 100.5}, SNAPSHOT) == (100.5, "panel (uncertified)")
    assert pa._uncertified_cash({"cash_available_sar": 0}, SNAPSHOT) == (53769.26, "_Cash_Snapshot 2026-10-16 (uncertified)")
    assert pa._uncertified_cash({"cash_available_sar": None}, {"balance_sar": "1200"}) == (1200.0, "_Cash_Snapshot (uncertified)")
    assert pa._uncertified_cash({"cash_available_sar": 0}, None) == (0.0, "none")
    assert pa._uncertified_cash({}, {"balance_sar": -5}) == (0.0, "none")
    assert pa._uncertified_cash({}, "junk") == (0.0, "none")


# ---------------------------------------------------------------------------
# the 17-Oct shape
# ---------------------------------------------------------------------------
def test_off_is_the_v1160_dispatch_and_never_enters_the_view(monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("uncertified view entered with the gate off")
    monkeypatch.setattr(pa, "_uncertified_view_portfolio", boom)
    out = build(context=CONTEXT, snapshot=SNAPSHOT)
    assert [a["action"] for a in out["actions"]] == ["BLOCK"] * 3
    assert all(a["market_value_sar"] is None and a["weight_pct"] is None for a in out["actions"])
    assert out["kpis"]["portfolio_value_sar"] is None and out["kpis"]["cash_sar"] is None
    assert "uncertified_view" not in out["meta"]
    assert "capture_context" not in out["meta"]["upstream"] and "cash_snapshot" not in out["meta"]["upstream"]


def test_17oct_rows_read_hold_uncertified_with_mv_weights_and_zero_deltas(monkeypatch):
    monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", "on")
    out = build(context=CONTEXT, snapshot=SNAPSHOT)
    assert out["version"] == pa.PORTFOLIO_ACTIONS_VERSION and out["status"] == "ok"
    acts = out["actions"]
    assert [a["action"] for a in acts] == ["HOLD"] * 3
    by = {a["symbol"]: a for a in acts}
    assert by["DDI.US"]["market_value_sar"] == 11858 and by["CWBC.US"]["market_value_sar"] == 9664
    assert by["KRP.US"]["market_value_sar"] == 7942
    holdings = 11857.5 + 9663.75 + 7942.5
    total = holdings + 53769.26
    assert by["DDI.US"]["weight_pct"] == round(11857.5 / total * 100, 1)
    assert abs(sum(a["weight_pct"] for a in acts) - round(holdings / total * 100, 1)) < 0.2
    for a in acts:
        assert a["suggested_delta_shares"] == 0 and a["suggested_delta_sar"] == 0 and a["proceeds_sar"] == 0
        assert a["funds_from"] is None
        assert a["action_reason"].startswith("HOLD (uncertified): ")
        assert ("| [uncertified] [custody-unverified] custody missing_evidence; capture 1497m old, limit 14m; "
                "sizing withheld") in a["action_reason"]
        assert a["detail"]["uncertified"] is True and a["detail"]["execution_ready"] is False
        assert a["detail"]["reconciliation_status"] == "missing_evidence"
        assert a["detail"]["position_evidence_matched"] is False
        assert a["detail"]["uncertified_view"] == {"tier": "unverified", "verdict": "HOLD",
                                                   "fx_rejected": False, "capture_age_min": 1497}
        assert a["cost_sar"] is not None and a["pnl_sar"] is not None and a["stop_sar"] is not None
    k = out["kpis"]
    assert k["cash_sar"] == 53769 and k["holdings_value_sar"] == round(holdings) and k["portfolio_value_sar"] == round(total)
    assert k["cash_pct"] == round(53769.26 / total * 100, 1)
    assert k["deployable_sar"] == 0.0 and k["adds_funded_sar"] == 0.0 and k["capital_unallocated_sar"] == 0.0
    assert k["action_counts"] == {"ADD": 0, "HOLD": 3, "TRIM": 0, "EXIT": 0, "BLOCK": 0} and k["positions"] == 3
    assert k["pnl_sar"] is not None and k["cost_basis_sar"] is not None
    assert [s["sector"] for s in out["sector_summary"]] == ["Banks", "Energy", "Financials"]
    assert all(s["weight_pct"] and s["value_sar"] and s["positions"] == 1 and s["cap_pct"] == 30 for s in out["sector_summary"])
    assert [a["type"] for a in out["alerts"]] == ["portfolio_inputs_unverified"]
    assert out["meta"]["execution_ready"] is False
    assert out["meta"]["input_certification"]["funding_eligible"] is False
    view = out["meta"]["uncertified_view"]
    assert view == {"mode": "on", "status": "ok", "tiers": {"unverified": 3}, "verdicts": {"HOLD": 3},
                    "cash_basis": "_Cash_Snapshot 2026-10-16 (uncertified)", "capture_age_min": 1497,
                    "capture_limit_min": 14, "kpi_basis": "sheet_quantities_custody_unverified",
                    "kpi_withheld": [], "fx_rejected_rows": 0}
    assert "capture_context" not in out["meta"]["upstream"] and "cash_snapshot" not in out["meta"]["upstream"]


def test_panel_cash_wins_and_missing_context_reads_unknown(monkeypatch):
    monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", "on")
    out = build(controls={"cash_available_sar": 1000}, snapshot=SNAPSHOT)
    assert out["meta"]["uncertified_view"]["cash_basis"] == "panel (uncertified)"
    assert out["kpis"]["cash_sar"] == 1000 and out["meta"]["uncertified_view"]["capture_age_min"] is None
    assert "capture age unknown; sizing withheld" in out["actions"][0]["action_reason"]
    none = build()
    assert none["meta"]["uncertified_view"]["cash_basis"] == "none" and none["kpis"]["cash_sar"] is None
    assert none["kpis"]["cash_pct"] == 0.0 and none["kpis"]["portfolio_value_sar"] == none["kpis"]["holdings_value_sar"]


def test_verdicts_add_becomes_hold_sell_becomes_exit_with_zero_sizing(monkeypatch):
    monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", "on")
    rows = [holding("ADDME.US", 10, 9, 10, **{"Intrinsic Value": 13, "Recommendation": "BUY"}),
            holding("SELLME.US", 10, 9, 10, **{"Recommendation": "STRONG_SELL", "Intrinsic Value": 10.4})]
    out = build(rows=rows, context=CONTEXT, snapshot=SNAPSHOT)
    by = {a["symbol"]: a for a in out["actions"]}
    assert by["ADDME.US"]["action"] == "HOLD" and by["ADDME.US"]["detail"]["capped_from"] == "ADD"
    assert by["ADDME.US"]["action_reason"].startswith("HOLD (uncertified): new-money verdict withheld")
    assert by["ADDME.US"]["detail"]["uncertified_view"]["verdict"] == "ADD"
    assert by["SELLME.US"]["action"] == "EXIT" and by["SELLME.US"]["proceeds_sar"] == 0
    assert by["SELLME.US"]["suggested_delta_shares"] == 0
    assert out["kpis"]["action_counts"]["HOLD"] == 1 and out["kpis"]["action_counts"]["EXIT"] == 1
    assert out["meta"]["uncertified_view"]["verdicts"] == {"EXIT": 1, "HOLD": 1}


def test_contradicted_row_keeps_the_legacy_block_row_and_withholds_totals(monkeypatch):
    monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", "on")
    rows = rows_17oct()
    out = build(rows=rows, evidence=packet(rows, qty={1: 7}), context=CONTEXT, snapshot=SNAPSHOT,
                controls={"cash_available_sar": 53769.26})
    by = {a["symbol"]: a for a in out["actions"]}
    assert by["CWBC.US"]["action"] == "BLOCK" and by["CWBC.US"]["market_value_sar"] is None
    assert "uncertified" not in by["CWBC.US"]["detail"]
    assert by["DDI.US"]["action"] == "HOLD" and by["DDI.US"]["market_value_sar"] == 11858
    assert "[custody-matched] custody matched" in by["DDI.US"]["action_reason"]
    view = out["meta"]["uncertified_view"]
    assert view["tiers"] == {"contradicted": 1, "matched": 2} and "contradicting_evidence" in view["kpi_withheld"]
    assert out["kpis"]["holdings_value_sar"] is None and out["sector_summary"] == []
    assert out["kpis"]["action_counts"]["BLOCK"] == 1 and out["kpis"]["action_counts"]["HOLD"] == 2


def rows_sar():
    """The 16-Oct shape on SAR rows (the certified path needs no FX proof)."""
    return [holding(sym, qty, buy, price, Currency="SAR", Market="Tadawul")
            for sym, qty, buy, price in (("1120.SR", 200, 15.5, 15.81), ("2222.SR", 100, 25.0, 25.77),
                                         ("1180.SR", 150, 13.9, 14.12))]


def packet_sar(rows, cash=53769.26):
    ev = packet(rows, cash=cash)
    ev.pop("fx", None)
    for pos in ev["accounts"][0]["positions"]:
        pos["currency"] = "SAR"
    for link in ev["holding_links"]:
        link["currency"] = "SAR"
    return ev


def test_16oct_fresh_packet_is_the_certified_build_regardless_of_the_gate(monkeypatch):
    rows = rows_sar()
    off = build(rows=rows, evidence=packet_sar(rows), controls={"cash_available_sar": 53769.26})
    monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", "on")
    on = build(rows=rows, evidence=packet_sar(rows), controls={"cash_available_sar": 53769.26},
               context=CONTEXT, snapshot=SNAPSHOT)
    assert off["meta"]["execution_ready"] is True, off["meta"]["input_certification"]
    assert norm(on) == norm(off)
    assert "uncertified_view" not in on["meta"]


def test_gate_supersedes_the_research_view_and_fails_closed(monkeypatch):
    monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", "on")
    monkeypatch.setenv("TFB_PF_RESEARCH_VIEW", "enforce")
    out = build(context=CONTEXT, snapshot=SNAPSHOT)
    assert "uncertified_view" in out["meta"] and "research_view" not in out["meta"]
    assert [a["action"] for a in out["actions"]] == ["HOLD"] * 3

    def boom(*_a, **_k):
        raise RuntimeError("synthetic")
    monkeypatch.setattr(pa, "_research_verdict", boom)
    fallback = build(context=CONTEXT, snapshot=SNAPSHOT)
    assert [a["action"] for a in fallback["actions"]] == ["BLOCK"] * 3
    assert fallback["meta"]["uncertified_view"] == {"mode": "on", "status": "error", "error_class": "RuntimeError"}
    assert fallback["kpis"]["portfolio_value_sar"] is None


def test_route_forwards_the_caller_context():
    from routes import advanced_analysis as ra
    src = inspect.getsource(ra.portfolio_actions_post)
    assert '"capture_context": merged_body.get("capture_context")' in src
    assert '"cash_snapshot": merged_body.get("cash_snapshot")' in src
