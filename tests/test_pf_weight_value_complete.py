"""data_engine_v2 v5.156.0 + portfolio_actions v1.16.1: portfolio weight fail-closed fixes.

  PF-VALUE-COMPLETE   a held row without a price used to drop out of the weight
                      denominator: 2222.SR 100@25 + 1120.SR 100@None read
                      100% / gap -50 / REDUCE under "OK:SAR". The whole book is
                      now blanked with PORTFOLIO_VALUE_INCOMPLETE.
  PF-FX-SUBUNIT-KEY   TFB_PF_FX_RATES="GBP:4.8,GBp:0.048" overwrote the pounds
                      rate with the pence rate.
  FRAC-QTY            EXIT of 10.6 shares suggested selling 11.
"""
from __future__ import annotations

import pytest

from core import data_engine_v2 as E
from core.analysis import portfolio_actions as pa


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in ("TFB_PF_FX_RATES", "TFB_PF_BASE_CCY", "TFB_PF_FX_AS_OF"):
        monkeypatch.delenv(k, raising=False)


def _row(sym, qty, price, ccy="SAR", tw=None):
    r = {"symbol": sym, "currency": ccy, "position_qty": qty, "avg_cost": price or 10.0,
         "current_price": price}
    if tw is not None:
        r["target_weight"] = tw
    return r


# ------------------------------------------------------ PF-VALUE-COMPLETE --- #
def test_unpriced_holding_blanks_the_whole_book_instead_of_inflating_weights():
    rows = [_row("2222.SR", 100, 25.0, tw=50), _row("1120.SR", 100, None, tw=50)]
    E._compute_portfolio_fields(rows)
    for r in rows:
        assert r["actual_weight"] is None and r["weight_gap"] is None
        assert r["portfolio_fx_status"] == "PORTFOLIO_VALUE_INCOMPLETE: 1120.SR"
        assert r["action_flag"] != "REDUCE" and r["action_flag"] != "ADD"
    assert rows[0]["target_weight"] == 50


def test_fully_priced_book_keeps_its_weights():
    rows = [_row("2222.SR", 100, 25.0, tw=50), _row("1120.SR", 100, 75.0, tw=50)]
    E._compute_portfolio_fields(rows)
    assert [r["actual_weight"] for r in rows] == [25.0, 75.0]
    assert [r["weight_gap"] for r in rows] == [25.0, -25.0]
    assert rows[0]["portfolio_fx_status"] == "OK:SAR"


def test_zero_quantity_watch_row_without_price_does_not_block_the_book():
    rows = [_row("2222.SR", 100, 25.0), _row("1120.SR", 0, None)]
    E._compute_portfolio_fields(rows)
    assert rows[0]["actual_weight"] == 100.0
    assert rows[0]["portfolio_fx_status"] == "OK:SAR"


# ------------------------------------------------------ PF-FX-SUBUNIT-KEY --- #
def test_pence_key_does_not_overwrite_the_pounds_rate(monkeypatch):
    monkeypatch.setenv("TFB_PF_FX_RATES", "GBP:4.8,GBp:0.048")
    table = E._pf_fx_table()
    assert table["GBP"] == 4.8 and table["GBX"] == 0.048
    rows = [_row("VOD.L", 1000, 70.0, "GBp"), _row("BP.L", 100, 4.0, "GBP"),
            _row("2222.SR", 1000, 25.0)]
    E._compute_portfolio_fields(rows)
    # SAR values: 70,000p * 0.048 = 3,360; 400 GBP * 4.8 = 1,920; 25,000 SAR
    total = 3360.0 + 1920.0 + 25000.0
    got = {r["symbol"]: r["actual_weight"] for r in rows}
    assert got["VOD.L"] == pytest.approx(3360.0 / total * 100.0, abs=1e-3)
    assert got["BP.L"] == pytest.approx(1920.0 / total * 100.0, abs=1e-3)


def test_pounds_only_rate_still_converts_pence_by_one_hundred(monkeypatch):
    monkeypatch.setenv("TFB_PF_FX_RATES", "GBP:4.8")
    rows = [_row("VOD.L", 1000, 70.0, "GBp"), _row("2222.SR", 1000, 25.0)]
    E._compute_portfolio_fields(rows)
    total = 70000.0 / 100.0 * 4.8 + 25000.0
    assert rows[0]["actual_weight"] == pytest.approx(3360.0 / total * 100.0, abs=1e-3)


@pytest.mark.parametrize("raw,key", [("GBp", "GBX"), ("ZAc", "ZAC"), ("ILa", "ILA"),
                                     ("GBP", "GBP"), ("gbx", "GBX"), (" usd ", "USD")])
def test_fx_key_canonicalisation(raw, key):
    assert E._pf_fx_key(raw) == key


# --------------------------------------------------------------- FRAC-QTY --- #
def _exit_entry(qty, mv):
    return {"action": pa.ACTION_EXIT,
            "cand": {"quantity": qty, "market_value_sar": mv}}


def test_exit_never_sells_more_whole_shares_than_held():
    (e,) = pa._sell_side_deltas([_exit_entry(10.6, 1060.0)], {}, 10600.0)
    assert e["suggested_delta_shares"] == -10                       # was -11
    assert e["post_trade_weight_pct"] == pytest.approx(0.6)         # 0.6 sh x 100 SAR


def test_exit_of_whole_holding_is_unchanged():
    (e,) = pa._sell_side_deltas([_exit_entry(10, 1000.0)], {}, 10000.0)
    assert e["suggested_delta_shares"] == -10
    assert e["post_trade_weight_pct"] == 0.0


def test_sub_one_share_holding_stays_blank():
    (e,) = pa._sell_side_deltas([_exit_entry(0.4, 40.0)], {}, 1000.0)
    assert e.get("suggested_delta_shares") is None
