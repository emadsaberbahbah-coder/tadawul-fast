"""data_engine_v2 v5.159.0: My_Portfolio weight fixes found by the 25 Oct audit.

  PF-VALUE-ZERO       a held row quoted at 0 got position_value 0, passed the
                      v5.156.0 `is None` check and dropped out of total_mv:
                      2222.SR 100@0 + 1120.SR 100@90 read 1120.SR 100% / gap
                      -50 / REDUCE under "OK:SAR".
  PF-SHORT            a negative quantity published a negative weight and a
                      profitable short read a negative P&L percent.
  PF-FX-OPERATOR-KEY  an operator "GBp" rate counted as an operator GBP rate,
                      hiding the INDICATIVE:GBP disclosure.
"""
from __future__ import annotations

import pytest

from core import data_engine_v2 as E


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in ("TFB_PF_FX_RATES", "TFB_PF_BASE_CCY", "TFB_PF_FX_AS_OF"):
        monkeypatch.delenv(k, raising=False)


def _row(sym, qty, price, ccy="SAR", tw=None, cost=None):
    r = {"symbol": sym, "currency": ccy, "position_qty": qty,
         "avg_cost": cost if cost is not None else 10.0, "current_price": price}
    if tw is not None:
        r["target_weight"] = tw
    return r


# ---------------------------------------------------------- PF-VALUE-ZERO --- #
@pytest.mark.parametrize("bad", [0.0, 0, "0", -1.0])
def test_zero_or_negative_price_blanks_the_book_like_a_missing_price(bad):
    rows = [_row("2222.SR", 100, bad, tw=50, cost=25.0), _row("1120.SR", 100, 90.0, tw=50)]
    E._compute_portfolio_fields(rows)
    for r in rows:
        assert r["actual_weight"] is None and r["weight_gap"] is None
        assert r["portfolio_fx_status"] == "PORTFOLIO_VALUE_INCOMPLETE: 2222.SR"
        assert r["action_flag"] not in ("ADD", "REDUCE")
        assert r["decision"] not in ("ADD", "REDUCE")
    # no fabricated -100% loss from a failed quote
    assert rows[0].get("position_value") is None
    assert rows[0].get("unrealized_pl") is None and rows[0].get("unrealized_pl_pct") is None
    assert rows[0]["position_cost"] == 2500.0


def test_zero_price_matches_the_missing_price_contract():
    a = [_row("2222.SR", 100, 0.0, tw=50, cost=25.0), _row("1120.SR", 100, 90.0, tw=50)]
    b = [_row("2222.SR", 100, None, tw=50, cost=25.0), _row("1120.SR", 100, 90.0, tw=50)]
    E._compute_portfolio_fields(a)
    E._compute_portfolio_fields(b)
    keys = ("position_value", "unrealized_pl", "actual_weight", "weight_gap",
            "portfolio_fx_status", "action_flag", "decision")
    for ra, rb in zip(a, b):
        assert {k: ra.get(k) for k in keys} == {k: rb.get(k) for k in keys}


@pytest.mark.parametrize('bad', [0, '0', -1.0])
def test_explicit_invalid_primary_cannot_borrow_an_unverified_price_alias(bad):
    rows = [_row('2222.SR', 100, bad, tw=50, cost=25),
            _row('1120.SR', 100, 90, tw=50)]
    rows[0]['price'] = 30.0
    E._compute_portfolio_fields(rows)
    for row in rows:
        assert row['actual_weight'] is None and row['weight_gap'] is None
        assert row['portfolio_fx_status'] == 'PORTFOLIO_VALUE_INCOMPLETE: 2222.SR'
        assert row['action_flag'] == 'HOLD'
    assert rows[0].get('position_value') is None
    assert rows[0].get('unrealized_pl') is None


def test_absent_primary_preserves_existing_positive_alias_fallback():
    row = _row('2222.SR', 100, None, cost=25)
    row['price'] = 30.0
    E._compute_position_math(row)
    assert row['position_value'] == 3000.0


def test_failed_primary_clears_derived_values_from_an_earlier_successful_pass():
    rows = [_row('2222.SR', 100, 30, tw=50, cost=25),
            _row('1120.SR', 100, 90, tw=50)]
    E._compute_portfolio_fields(rows)
    assert rows[0]['position_value'] == 3000.0
    rows[0]['current_price'] = 0
    rows[0]['price'] = 30.0
    E._compute_portfolio_fields(rows)
    assert rows[0]['position_value'] is None
    assert rows[0]['unrealized_pl'] is None
    assert rows[0]['unrealized_pl_pct'] is None
    assert all(row['actual_weight'] is None for row in rows)
    assert rows[0]['position_cost'] == 2500.0


def test_zero_quantity_watch_row_at_price_zero_does_not_block_the_book():
    rows = [_row("2222.SR", 100, 25.0), _row("1120.SR", 0, 0.0)]
    E._compute_portfolio_fields(rows)
    assert rows[0]["actual_weight"] == 100.0
    assert rows[0]["portfolio_fx_status"] == "OK:SAR"


def test_positive_long_book_is_unchanged():
    rows = [_row("2222.SR", 100, 25.0, tw=50), _row("1120.SR", 100, 75.0, tw=50)]
    E._compute_portfolio_fields(rows)
    assert [r["actual_weight"] for r in rows] == [25.0, 75.0]
    assert [r["weight_gap"] for r in rows] == [25.0, -25.0]
    assert rows[0]["portfolio_fx_status"] == "OK:SAR"
    assert rows[0]["unrealized_pl_pct"] == 150.0


# --------------------------------------------------------------- PF-SHORT --- #
def test_short_position_blanks_the_book_and_keeps_the_pl_sign():
    rows = [_row("AAPL", -10, 90.0, ccy="USD", tw=10, cost=100.0),
            _row("1120.SR", 100, 90.0, tw=50, cost=80.0)]
    E._compute_portfolio_fields(rows)
    for r in rows:
        assert r["actual_weight"] is None and r["weight_gap"] is None
        assert r["portfolio_fx_status"] == "PORTFOLIO_SHORT_UNSUPPORTED: AAPL"
        assert r["action_flag"] not in ("ADD", "REDUCE")
    assert rows[0]["unrealized_pl"] == 100.0
    assert rows[0]["unrealized_pl_pct"] == 10.0   # was -10.0


def test_short_and_unvalued_are_both_named():
    rows = [_row("AAPL", -10, 90.0, ccy="USD"), _row("2222.SR", 100, None),
            _row("1120.SR", 100, 90.0)]
    E._compute_portfolio_fields(rows)
    assert rows[2]["portfolio_fx_status"] == (
        "PORTFOLIO_VALUE_INCOMPLETE: 2222.SR | PORTFOLIO_SHORT_UNSUPPORTED: AAPL")


@pytest.mark.parametrize('signal', ['SELL', 'STRONG_SELL'])
def test_unsupported_short_has_no_sell_instruction(signal):
    rows = [_row('AAPL', -10, 90, ccy='USD', cost=100),
            _row('1120.SR', 100, 90)]
    rows[0]['recommendation'] = signal
    E._compute_portfolio_fields(rows)
    assert rows[0]['decision'] == 'HOLD'
    assert rows[0]['action_flag'] == 'HOLD'
    assert rows[0]['actual_weight'] is None
    assert rows[0]['portfolio_fx_status'] == 'PORTFOLIO_SHORT_UNSUPPORTED: AAPL'


def test_long_signal_verdict_is_preserved_when_another_holding_is_unpriced():
    rows = [_row('AAPL', 10, 90, ccy='USD', cost=100),
            _row('1120.SR', 100, None)]
    rows[0]['recommendation'] = 'SELL'
    E._compute_portfolio_fields(rows)
    assert rows[0]['decision'] == 'SELL'
    assert rows[0]['actual_weight'] is None


# ----------------------------------------------------- PF-FX-OPERATOR-KEY --- #
def test_operator_pence_key_keeps_the_indicative_pounds_disclosure():
    rows = [_row("VOD.L", 1000, 72.0, ccy="GBp", cost=70.0),
            _row("BP", 10, 5.0, ccy="GBP", cost=4.0)]
    E._compute_portfolio_fields(rows, {"GBp": 0.048})
    assert rows[0]["portfolio_fx_status"] == "OK:SAR INDICATIVE:GBP"
    # weights unchanged: 1000*72*0.048 = 3456 SAR vs 10*5*4.8 = 240 SAR
    assert rows[0]["actual_weight"] == pytest.approx(93.5065, abs=1e-4)


def test_operator_pounds_key_still_suppresses_the_disclosure(monkeypatch):
    rows = [_row("BP", 10, 5.0, ccy="GBP", cost=4.0)]
    E._compute_portfolio_fields(rows, {"GBP": 4.9})
    assert rows[0]["portfolio_fx_status"] == "OK:SAR"
    monkeypatch.setenv("TFB_PF_FX_RATES", "GBP:4.9")
    rows = [_row("BP", 10, 5.0, ccy="GBP", cost=4.0)]
    E._compute_portfolio_fields(rows)
    assert rows[0]["portfolio_fx_status"] == "OK:SAR"


def test_env_operator_pence_key_keeps_the_indicative_pounds_disclosure(monkeypatch):
    monkeypatch.setenv("TFB_PF_FX_RATES", "GBp:0.048")
    rows = [_row("BP", 10, 5.0, ccy="GBP", cost=4.0)]
    E._compute_portfolio_fields(rows)
    assert rows[0]["portfolio_fx_status"] == "OK:SAR INDICATIVE:GBP"
