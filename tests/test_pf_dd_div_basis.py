# -*- coding: utf-8 -*-
"""tests/test_pf_dd_div_basis.py

portfolio_actions v1.19.0 [DIV-BASIS]: the two loss rules (the F-2 drawdown / time guard and the P-183
add-on-loser veto) count the dividends a position has received since its purchase, so a paid-out dividend is
not read as a loss. Owner, 18 Nov 2026: Portfolio_Decision printed "[dd-observe] drawdown -18.1% ... would EXIT"
on a holding whose price had fallen by a special dividend he had been paid; his result was about -2.6%.

Executes the REAL core.analysis.portfolio_actions. Every figure below is synthetic.
"""
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import core.analysis.portfolio_actions as pa  # noqa: E402

_KEYS = ("TFB_PF_DD_EXIT", "TFB_PF_DD_DIV_BASIS", "TFB_PF_DD_EXIT_PCT", "TFB_PF_DD_TIME_D", "TFB_PA_PROTECT_SUKUK",
         "TFB_PF_ADD_LOSER_VETO", "TFB_PF_ADD_LOSER_PCT", "TFB_PF_ADD_STOP_PROX_PCT")

# 100 shares bought at 35.00 (cost 3,500 native = 13,125 SAR at 3.75); the price is now 28.70 after a special
# dividend of 8.00 a share, of which 5.60 was kept after tax: 560 native = 2,100 SAR.
COST, MV, DIV = 13125.0, 10762.5, 2100.0
PAID = {"symbol": "PAID.US", "name": "Paid Out Inc.", "pnl_sar": MV - COST, "cost_sar": COST,
        "market_value_sar": MV, "buy_date": "2026-11-02", "dd_div_sar": DIV}
NO_DIV = dict(PAID, dd_div_sar=None)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)


def pa_days(buy_date):
    from datetime import datetime, timezone
    return (datetime.now(timezone.utc).date() - datetime.strptime(buy_date, "%Y-%m-%d").date()).days


def _g(cand, action="HOLD"):
    return pa._apply_drawdown_guard(dict(cand), action, "base reason", 0.0)


def test_version_and_manifest_pin():
    assert pa.PORTFOLIO_ACTIONS_VERSION == "1.19.0"
    manifest = open(os.path.join(_ROOT, "scripts", "verify_deployment.py"), encoding="utf-8").read()
    assert '"1.19.0", "portfolio actions")' in manifest


def test_the_return_the_rules_judge_counts_the_dividends():
    ret, price_only, div = pa._loss_return_pct(PAID)
    assert round(price_only, 1) == -18.0 and round(ret, 1) == -2.0 and div == DIV
    assert pa._loss_return_pct(NO_DIV) == (price_only, price_only, 0.0)
    assert pa._loss_return_pct({"pnl_sar": -1.0, "cost_sar": None}) == (None, None, 0.0)
    assert pa._loss_return_pct({"pnl_sar": -1.0, "cost_sar": 0.0}) == (None, None, 0.0)
    assert pa._loss_return_pct(None) == (None, None, 0.0)


@pytest.mark.parametrize("bad", [0.0, -50.0, COST + 1.0, "n/a", "", None, float("nan")])
def test_a_dividend_can_lift_a_loss_figure_never_invent_one(bad):
    ret, price_only, div = pa._loss_return_pct(dict(PAID, dd_div_sar=bad))
    assert ret == price_only and div == 0.0                      # not counted: the 1.18.1 basis
    assert pa._loss_return_pct(dict(PAID, dd_div_sar=COST))[2] == COST   # the whole cost paid back is the bound


def test_owner_case_observe_no_false_alarm_and_the_read_back_says_why(monkeypatch):
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    a, r, p = _g(PAID)
    assert (a, p) == ("HOLD", 0.0)
    assert r == "base reason | [dd-observe] ok after dividends received: -2.0% (price only -18.0%)"
    assert "would EXIT" not in r
    # the same holding without the field: the 1.18.1 text, to the character
    a, r, p = _g(NO_DIV)
    assert r == "base reason | [dd-observe] drawdown -18.0% breaches -8.0% budget - would EXIT under enforce"


def test_owner_case_enforce_no_exit(monkeypatch):
    monkeypatch.setenv("TFB_PF_DD_EXIT", "enforce")
    assert _g(PAID) == ("HOLD", "base reason", 0.0)             # a roughly flat holding is not sold
    a, r, p = _g(NO_DIV)
    assert (a, p) == ("EXIT", MV) and r == "Drawdown/time guard: drawdown -18.0% breaches -8.0% budget - full exit (was HOLD)"


def test_a_real_loss_still_fires_and_names_both_figures(monkeypatch):
    deep = dict(PAID, pnl_sar=-4000.0, market_value_sar=COST - 4000.0)   # -30.5% on price, -14.5% with the dividend
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    a, r, p = _g(deep)
    assert r == ("base reason | [dd-observe] drawdown -14.5% after dividends received (price only -30.5%) "
                 "breaches -8.0% budget - would EXIT under enforce")
    monkeypatch.setenv("TFB_PF_DD_EXIT", "enforce")
    a, r, p = _g(deep)
    assert (a, p) == ("EXIT", COST - 4000.0) and "after dividends received (price only -30.5%)" in r


def test_the_time_leg_uses_the_same_basis(monkeypatch):
    old = dict(PAID, buy_date="2026-01-05", pnl_sar=-1000.0, market_value_sar=COST - 1000.0)   # -7.6% price, +8.4% total
    monkeypatch.setenv("TFB_PF_DD_EXIT", "enforce")
    assert _g(old) == ("HOLD", "base reason", 0.0)              # positive on what the owner received: no time exit
    assert _g(dict(old, dd_div_sar=None))[0] == "EXIT"          # price only: still negative past the time budget
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    assert _g(old)[1] == "base reason | [dd-observe] ok after dividends received: 8.4% (price only -7.6%)"
    still = dict(old, dd_div_sar=500.0)                          # -3.8% with the dividend: the time leg stands
    assert "still negative (-3.8% after dividends received (price only -7.6%)) after" in _g(still)[1]


def test_gate_off_restores_the_price_only_basis(monkeypatch):
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    for word in ("off", "0", "false", "NO"):
        monkeypatch.setenv("TFB_PF_DD_DIV_BASIS", word)
        assert _g(PAID) == _g(NO_DIV)
        assert "would EXIT" in _g(PAID)[1] and "dividends" not in _g(PAID)[1]
    for word in ("", "on", "1", "anything"):
        monkeypatch.setenv("TFB_PF_DD_DIV_BASIS", word)
        assert "ok after dividends received" in _g(PAID)[1]


def test_no_tag_when_nothing_would_have_fired_and_guard_off_is_a_pass_through(monkeypatch):
    fine = dict(PAID, pnl_sar=-300.0, market_value_sar=COST - 300.0)
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    assert _g(fine) == ("HOLD", "base reason", 0.0)             # -2.3% on price: neither figure fires, no tag
    monkeypatch.delenv("TFB_PF_DD_EXIT")
    assert _g(PAID) == ("HOLD", "base reason", 0.0)             # guard off
    monkeypatch.setenv("TFB_PF_DD_EXIT", "enforce")
    assert pa._apply_drawdown_guard(dict(NO_DIV), "EXIT", "r", 5.0) == ("EXIT", "r", 5.0)   # never over EXIT / BLOCK
    assert pa._apply_drawdown_guard(dict(NO_DIV), "BLOCK", "r", 0.0) == ("BLOCK", "r", 0.0)


def test_a_sukuk_keeps_its_exemption_and_gets_no_ok_tag(monkeypatch):
    sukuk = {"symbol": "5023.SR", "name": "", "pnl_sar": -900.0, "cost_sar": 10000.0, "market_value_sar": 9100.0,
             "buy_date": "2025-11-23", "dd_div_sar": 850.0}
    assert pa._is_sukuk_holding(sukuk) is True
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    a, r, p = _g(sukuk)                                          # -0.5% with its income, past the time budget
    assert (a, p) == ("HOLD", 0.0) and "would EXIT" not in r
    assert r == ("base reason | [dd-exempt] sukuk / fixed income (D-9): still negative (-0.5% after dividends received "
                 "(price only -9.0%)) after " + str(pa_days("2025-11-23")) + " d > 45 d time budget on price and "
                 "recorded income - equity loss budget not applied")
    assert _g(dict(sukuk, dd_div_sar=1000.0)) == ("HOLD", "base reason", 0.0)   # +1.0% with its income: no rule, no tag
    a, r, p = _g(dict(sukuk, dd_div_sar=None))
    assert "[dd-exempt]" in r and r.endswith("on a price-only basis - equity loss budget not applied")   # v1.12.1 text


def test_the_add_on_loser_veto_uses_the_same_basis(monkeypatch):
    ev = pa._add_loser_eval(dict(PAID, price=28.7, stop=24.0), 2.0, 2.0)
    assert round(ev["ret_pct"], 1) == -2.0 and ev["loser"] is True
    assert ev["trig"] == "position -2.0% vs cost after dividends received (price only -18.0%) breaches the -2.0% loser floor"
    ok = pa._add_loser_eval(dict(PAID, dd_div_sar=2300.0, price=28.7, stop=24.0), 2.0, 2.0)
    assert ok["loser"] is False and ok["trig"] == ""            # -0.5% on what was received: not a loser
    plain = pa._add_loser_eval(dict(NO_DIV, price=28.7, stop=24.0), 2.0, 2.0)
    assert plain["trig"] == "position -18.0% vs cost breaches the -2.0% loser floor"       # the 1.18.1 text
    monkeypatch.setenv("TFB_PF_DD_DIV_BASIS", "off")
    assert pa._add_loser_eval(dict(PAID, price=28.7, stop=24.0), 2.0, 2.0) == plain


ROW = {"Symbol": "PAID.US", "Name": "Paid Out Inc.", "Current Price": 28.70, "Currency": "USD", "Quantity": 100,
       "Buy Price": 35.00, "Buy Date": "2026-11-02"}


@pytest.mark.parametrize("header", ["Dividends Recv", "dividends_recv", "DIVIDENDS RECV", "Dividends Received"])
def test_normalize_holding_reads_the_ledger_field_in_any_header_form(header):
    cand = pa.normalize_holding(dict(ROW, **{header: 560.0}), {"USD": 3.75}, pa.make_controls())
    assert cand["dd_div_sar"] == pytest.approx(DIV) and cand["cost_sar"] == pytest.approx(COST)
    assert round(cand["pnl_pct"], 1) == -18.0                    # the page's P&L column stays price only
    ret, price_only, div = pa._loss_return_pct(cand)
    assert round(ret, 1) == -2.0 and round(price_only, 1) == -18.0


@pytest.mark.parametrize("value", [None, "", "n/a", 0, -5])
def test_normalize_holding_without_a_usable_field(value):
    row = dict(ROW) if value is None else dict(ROW, **{"Dividends Recv": value})
    assert pa.normalize_holding(row, {"USD": 3.75}, pa.make_controls())["dd_div_sar"] is None
    # no cost basis: nothing to compare a dividend with
    bare = {k: v for k, v in ROW.items() if k != "Buy Price"}
    assert pa.normalize_holding(dict(bare, **{"Dividends Recv": 560.0}), {"USD": 3.75}, pa.make_controls())["dd_div_sar"] is None


def test_the_field_is_internal_and_a_row_without_it_builds_the_same_output(monkeypatch):
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    rows = [dict(ROW), {"Symbol": "OTHER.US", "Name": "Other Co", "Current Price": 50.0, "Currency": "USD",
                        "Quantity": 40, "Buy Price": 48.0, "Buy Date": "2026-10-01"}]
    base = pa.build_portfolio_actions([dict(r) for r in rows], fx_rates={"USD": 3.75})
    with_div = pa.build_portfolio_actions([dict(rows[0], **{"Dividends Recv": 560.0}), dict(rows[1])],
                                          fx_rates={"USD": 3.75})
    import json
    assert "dd_div" not in json.dumps(with_div, default=str)     # never emitted, under any key
    acts = {a.get("symbol"): a for a in with_div.get("actions", [])}
    base_acts = {a.get("symbol"): a for a in base.get("actions", [])}
    assert set(acts) == set(base_acts) and "PAID.US" in acts
    assert acts["OTHER.US"] == base_acts["OTHER.US"]             # a row without the field is untouched
    for key in ("pnl_sar", "pnl_pct", "cost_sar", "market_value_sar", "action"):
        assert acts["PAID.US"].get(key) == base_acts["PAID.US"].get(key), key
    # the only difference is the guard's tag, and only when the guard reached the row
    r_base, r_div = str(base_acts["PAID.US"].get("action_reason")), str(acts["PAID.US"].get("action_reason"))
    if "[dd-observe]" in r_base:
        assert "would EXIT under enforce" in r_base and "[dd-observe] ok after dividends received" in r_div
        assert "would EXIT" not in r_div
    else:
        assert r_div == r_base
    monkeypatch.setenv("TFB_PF_DD_DIV_BASIS", "off")
    off = pa.build_portfolio_actions([dict(rows[0], **{"Dividends Recv": 560.0}), dict(rows[1])], fx_rates={"USD": 3.75})
    assert json.dumps(off.get("actions"), default=str, sort_keys=True) == json.dumps(base.get("actions"), default=str, sort_keys=True)
