"""Missing research position inputs cannot become zero balances or losses."""
import asyncio
from copy import deepcopy

import pytest

from core import data_engine_v2 as engine
from core.analysis import insights_builder as builder
from core.analysis import research_money


def position(**overrides):
    return {"symbol": "SYNTH.US", "position_qty": 2, "currency": "USD",
            "position_value": 200, "position_cost": 180,
            "avg_cost": 90, "current_price": 100, "risk_score": 40,
            **overrides}


@pytest.mark.parametrize("key", ["position_qty", "currency", "position_value", "position_cost"])
def test_one_missing_input_withholds_all_base_totals(key):
    row = position()
    del row[key]
    totals = engine._insights_base_totals([position(symbol="OTHER.US"), row], {"USD": 3.75})
    assert totals["complete"] is False
    assert totals["tot_mv"] is totals["tot_cost"] is None
    assert any(key in issue for issue in totals["issues"])


@pytest.mark.parametrize("key,value", [("position_qty", -1), ("position_qty", True),
                                       ("position_value", float("inf")), ("position_cost", float("nan")),
                                       ("position_value", -1), ("position_cost", -1)])
def test_invalid_position_numbers_do_not_pass_completeness(key, value):
    totals = engine._insights_base_totals([position(**{key: value})], {"USD": 3.75})
    assert totals["complete"] is False
    assert totals["tot_mv"] is totals["tot_cost"] is None


def test_valid_mixed_currency_conversion_and_measured_zero_are_preserved():
    rows = [position(), position(symbol="SYNTH.SR", currency="SAR", position_value=80, position_cost=0)]
    original = deepcopy(rows)
    totals = engine._insights_base_totals(rows, {"USD": 3.75, "SAR": 1})
    assert totals == {"complete": True, "tot_mv": 830, "tot_cost": 675, "missing": [], "issues": []}
    assert rows == original


@pytest.mark.parametrize("rate", [None, 0, -1, float("inf"), float("nan"), True])
def test_unknown_or_invalid_fx_withholds_all_totals(rate):
    totals = engine._insights_base_totals([position()], {"USD": rate})
    assert totals["complete"] is False
    assert totals["missing"] == ["USD"]


def test_empty_rows_are_unknown_but_explicit_zero_quantity_is_not_a_holding():
    assert engine._insights_base_totals([], {})["complete"] is False
    totals = engine._insights_base_totals([{"position_qty": 0}], {})
    assert totals["complete"] is True
    assert totals["tot_mv"] == totals["tot_cost"] == 0


def test_base_totals_reject_overflow():
    totals = engine._insights_base_totals([position(position_value=1e308)], {"USD": 3.75})
    assert totals["complete"] is False
    assert any("position_value" in issue or "nonfinite" in issue for issue in totals["issues"])


def test_engine_insights_suppress_loss_and_label_configured_base(monkeypatch):
    instance = object.__new__(engine.DataEngineV5)

    async def page_rows(page, **kwargs):
        return [position(position_value=None, current_price=None)] if page == "My_Portfolio" else []

    instance.get_page_rows = page_rows
    monkeypatch.setenv("TFB_PF_BASE_CCY", "USD")
    rows = asyncio.run(instance._build_insights_rows())
    money = [row for row in rows if row["section"] == "Portfolio Summary" and row["metric"] == "USD"]
    assert len(money) == 3
    assert all(row["value"] == "" and "position_value" in row["notes"] for row in money)


def ctx():
    return builder.BuildContext(keys=list(builder._FALLBACK_KEYS), ts="2026-10-14T12:00:00+03:00", norm_criteria={})


@pytest.mark.parametrize("key", ["position_qty", "avg_cost", "current_price", "currency"])
def test_active_insights_cannot_publish_partial_money_or_health(key):
    bad = position(symbol="OTHER.US")
    del bad[key]
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US", "OTHER.US"],
                                             {"SYNTH.US": position(), "OTHER.US": bad})
    assert [row["metric"] for row in rows] == ["status"]
    assert key in rows[0]["notes"]


def test_active_insights_do_not_sum_mixed_native_currencies():
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US", "SYNTH.SR"],
        {"SYNTH.US": position(), "SYNTH.SR": position(symbol="SYNTH.SR", currency="SAR")})
    assert [row["metric"] for row in rows] == ["status"]
    assert "mixed_currency" in rows[0]["notes"]


def test_complete_single_currency_research_math_and_zero_daily_pl_remain_measured():
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US"], {"SYNTH.US": position(day_pl=0)})
    by_metric = {row["metric"]: row for row in rows}
    assert by_metric["total_value"]["value"] == 200
    assert by_metric["unrealized_pl"]["value"] == 20
    assert by_metric["day_pl"]["value"] == 0
    assert "USD" in by_metric["total_value"]["notes"]
    assert "custody not certified" in by_metric["total_value"]["notes"]


def test_missing_risk_or_daily_pl_never_becomes_complete_health_or_partial_day_pl():
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US", "OTHER.US"],
        {"SYNTH.US": position(day_pl=10), "OTHER.US": position(symbol="OTHER.US", risk_score=None)})
    by_metric = {row["metric"]: row for row in rows}
    assert by_metric["total_value"]["value"] == 400
    assert "Health unavailable" in by_metric["position_count"]["notes"]
    assert "1 measured of 2" in by_metric["position_count"]["notes"]
    assert "day_pl" not in by_metric


@pytest.mark.parametrize("key", ["position_qty", "position_cost", "position_value", "current_price"])
@pytest.mark.parametrize("value", ["100%", "$100", "100 USD", True])
def test_percent_or_formatted_money_never_supplies_engine_totals(key, value):
    totals = engine._insights_base_totals([position(**{key: value})], {"USD": 3.75})
    assert totals["complete"] is False
    assert totals["tot_mv"] is totals["tot_cost"] is None
    assert any(key in issue for issue in totals["issues"])


@pytest.mark.parametrize("key", ["position_qty", "avg_cost", "current_price"])
@pytest.mark.parametrize("value", ["100%", "$100", "100 USD", True])
def test_percent_or_formatted_money_never_supplies_active_kpis(key, value):
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US"], {"SYNTH.US": position(**{key: value})})
    assert [row["metric"] for row in rows] == ["status"]


@pytest.mark.parametrize("currency", ["GBp", "GBX", "GBx", "ZAC", "ZAc", "ILA", "ILa", "USX", "usd"])
def test_minor_or_ambiguous_currency_never_becomes_major_money(currency):
    row = position(currency=currency)
    totals = engine._insights_base_totals([row], {"GBP": 4.8, "USD": 3.75, currency: 1})
    assert totals["complete"] is False
    assert totals["tot_mv"] is None
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US"], {"SYNTH.US": row})
    assert [item["metric"] for item in rows] == ["status"]
    assert "ambiguous" in rows[0]["notes"]


@pytest.mark.parametrize("price", [0, "0", "0.00", -1])
def test_unpriced_holding_never_turns_into_zero_nav_and_a_total_loss(price):
    row = position(current_price=price, position_value=0)
    totals = engine._insights_base_totals([row], {"USD": 3.75})
    assert totals["complete"] is False
    assert totals["tot_mv"] is totals["tot_cost"] is None
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US"], {"SYNTH.US": row})
    assert [item["metric"] for item in rows] == ["status"]


def test_missing_current_price_withholds_even_when_stale_position_value_survives():
    totals = engine._insights_base_totals([position(current_price=None)], {"USD": 3.75})
    assert totals["complete"] is False
    assert totals["tot_mv"] is None


def test_explicit_major_currency_and_signed_daily_money_remain_valid():
    row = position(currency="GBP", day_pl="-2.50")
    totals = engine._insights_base_totals([row], {"GBP": 4.8})
    assert totals["complete"] is True
    assert totals["tot_mv"] == 960
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US"], {"SYNTH.US": row})
    assert next(item for item in rows if item["metric"] == "day_pl")["value"] == -2.5


def test_percent_daily_pl_is_unavailable_while_valid_research_totals_remain():
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US"], {"SYNTH.US": position(day_pl="100%")})
    assert any(item["metric"] == "total_value" for item in rows)
    assert not any(item["metric"] == "day_pl" for item in rows)


def test_known_derived_return_ratio_above_one_and_a_half_remains_fraction():
    row = position(position_qty=2, avg_cost=10, current_price=30, position_cost=20, position_value=60)
    rows = builder._build_portfolio_kpis_rows(ctx(), ["SYNTH.US"], {"SYNTH.US": row})
    by_metric = {item["metric"]: item for item in rows}
    assert by_metric["unrealized_pl"]["value"] == 40
    assert "200.00%" in by_metric["unrealized_pl"]["notes"]
    assert "Research health=90.0/100" in by_metric["position_count"]["notes"]


@pytest.mark.parametrize("value", ["1e999", "-1e999", "1e-999"])
def test_oversized_decimal_is_unavailable(value):
    assert research_money.research_amount(value) is None
    assert research_money.research_amount(value, signed=True) is None


def test_float_conversion_of_finite_but_oversized_decimal_is_never_infinite(monkeypatch):
    from decimal import Decimal
    monkeypatch.setattr(research_money, "_number", lambda *args, **kwargs: Decimal("1e999"))
    assert research_money.research_amount("1") is None


def test_minor_unit_base_configuration_never_gets_capitalized_into_major_totals(monkeypatch):
    instance = object.__new__(engine.DataEngineV5)

    async def page_rows(page, **kwargs):
        return [position(currency="GBP")] if page == "My_Portfolio" else []

    instance.get_page_rows = page_rows
    monkeypatch.setenv("TFB_PF_BASE_CCY", "GBp")
    rows = asyncio.run(instance._build_insights_rows())
    money = [row for row in rows if row["section"] == "Portfolio Summary" and row["item"] != "Holdings"]
    assert len(money) == 3
    assert all(row["value"] == "" and "base_currency_unknown_or_ambiguous" in row["notes"] for row in money)
