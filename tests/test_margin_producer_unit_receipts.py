"""Actual adapter margin contracts, with only external wire calls replaced."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from core.providers import eodhd_provider as ep
from core.providers import yahoo_fundamentals_provider as yp


SYMBOL = "SYNTH.US"
UNIT_KEY = "_margin_unit_basis"
MARGINS = {
    "gross_margin": "GrossMargin",
    "operating_margin": "OperatingMargin",
    "profit_margin": "ProfitMargin",
}
YAHOO_MARGINS = {
    "gross_margin": "grossMargins",
    "operating_margin": "operatingMargins",
    "profit_margin": "profitMargins",
}


@pytest.fixture(autouse=True)
def offline_configuration(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "1")
    monkeypatch.setenv("YF_ENABLE_REDIS", "0")
    monkeypatch.setenv("YF_RATE_LIMIT_PER_SEC", "0")
    monkeypatch.setenv("YF_RETRY_ATTEMPTS", "1")
    # Unit proof belongs to the producer, including when display mode is Off.
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")


def eodhd_wire(monkeypatch, *, highlights=None, financials=None):
    monkeypatch.setattr(ep.httpx, "AsyncClient", lambda **kwargs: object())
    client = ep.EODHDClient()
    requests = []

    async def request_json(path, params):
        requests.append(path)
        return {
            "General": {"Code": "SYNTH", "Exchange": "US", "CurrencyCode": "USD"},
            "Highlights": highlights or {},
            "Financials": financials or {},
        }, None

    monkeypatch.setattr(client, "_request_json", request_json)
    return client, requests


def yahoo_wire(monkeypatch, values):
    constructions = []
    ticker = SimpleNamespace(
        info={"symbol": "SYNTH", "currency": "USD", "currentPrice": 20.0, **values},
        fast_info=None,
    )

    def construct(symbol, session):
        constructions.append(symbol)
        return ticker

    monkeypatch.setattr(yp, "_HAS_YFINANCE", True)
    monkeypatch.setattr(yp, "yf", object())
    monkeypatch.setattr(yp, "_configured", lambda: True)
    monkeypatch.setattr(yp, "_create_yf_session", lambda rotate: object())
    monkeypatch.setattr(yp, "_construct_ticker", construct)
    monkeypatch.setattr(yp.YahooFundamentalsProvider, "_history_rows", lambda *args, **kwargs: [])
    return yp.YahooFundamentalsProvider(), constructions


def assert_receipt(row, field, *, provider, raw_unit, raw_value, source_field, unit_basis):
    receipt = row[UNIT_KEY][field]
    assert receipt["unit"] == "fraction"
    assert receipt["value"] == row[field]
    assert receipt["provider"] == provider
    assert receipt["raw_unit"] == raw_unit
    assert receipt["raw_value"] == pytest.approx(raw_value)
    assert receipt["unit_basis"] == unit_basis
    assert receipt["source_field"] == source_field
    assert receipt["transform_version"].endswith("_margin_fraction_v1")


@pytest.mark.parametrize("raw_points", ["0%", "0.9%", "-0.4%", "180%", "-250%"])
def test_eodhd_explicit_percent_margins_convert_once_with_source_receipts(monkeypatch, raw_points):
    client, requests = eodhd_wire(
        monkeypatch, highlights={source: raw_points for source in MARGINS.values()},
    )
    row, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    expected_points = float(str(raw_points).removesuffix("%"))
    for field, source in MARGINS.items():
        assert row[field] == pytest.approx(expected_points / 100.0)
        assert_receipt(row, field, provider="eodhd", raw_unit="percent_points",
                       raw_value=expected_points, source_field=f"Highlights.{source}",
                       unit_basis="explicit_percent")
    assert row["net_margin"] == row["profit_margin"]
    assert requests == ["fundamentals/SYNTH.US"]


@pytest.mark.parametrize("fraction", [0.0, 0.009, -0.004, 0.2762, 1.5, 1.8, -2.5])
def test_eodhd_native_margin_fractions_keep_their_field_units(monkeypatch, fraction):
    client, requests = eodhd_wire(monkeypatch, highlights={
        "ProfitMargin": fraction, "OperatingMarginTTM": fraction,
        "OperatingMargin": 999.0,
    })
    row, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    for field, source in (("profit_margin", "ProfitMargin"), ("operating_margin", "OperatingMarginTTM")):
        assert row[field] == fraction
        assert_receipt(row, field, provider="eodhd", raw_unit="fraction", raw_value=fraction,
                       source_field=f"Highlights.{source}", unit_basis="supplier_field_contract")
    assert requests == ["fundamentals/SYNTH.US"]


@pytest.mark.parametrize("raw_value", [0.0, 0.9, -0.4, 180.0, -250.0])
def test_eodhd_bare_nonstandard_alias_units_remain_unproven(monkeypatch, raw_value):
    client, _ = eodhd_wire(monkeypatch, highlights={"GrossMargin": raw_value, "OperatingMargin": raw_value})
    row, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    for field, source in (("gross_margin", "GrossMargin"), ("operating_margin", "OperatingMargin")):
        # Retain the configured transform for analysis without certifying its
        # raw unit or admitting the value as a canonical scoring quantity.
        assert row[field] == pytest.approx(raw_value / 100.0)
        receipt = row[UNIT_KEY][field]
        assert receipt["unit"] == receipt["raw_unit"] == "unknown"
        assert receipt["raw_value"] == raw_value
        assert receipt["value"] == row[field]
        assert receipt["unit_basis"] == "configured_adapter_contract"
        assert receipt["source_field"] == f"Highlights.{source}"


@pytest.mark.parametrize("invalid", [None, True, float("nan"), float("inf"), "invalid"])
def test_eodhd_invalid_native_operating_margin_cannot_certify_alias(monkeypatch, invalid):
    client, _ = eodhd_wire(monkeypatch, highlights={"OperatingMarginTTM": invalid, "OperatingMargin": 0.9})
    row, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    assert row["operating_margin"] == pytest.approx(0.009)
    receipt = row[UNIT_KEY]["operating_margin"]
    assert receipt["unit"] == receipt["raw_unit"] == "unknown"
    assert receipt["source_field"] == "Highlights.OperatingMargin"


def test_eodhd_legacy_mode_preserves_prior_operating_source_selection(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "0")
    client, _ = eodhd_wire(monkeypatch, highlights={"OperatingMarginTTM": 0.3262, "OperatingMargin": 0.9})
    row, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    assert row["operating_margin"] == 0.9
    receipt = row[UNIT_KEY]["operating_margin"]
    assert receipt["unit"] == receipt["raw_unit"] == "unknown"
    assert receipt["source_field"] == "Highlights.OperatingMargin"


def test_eodhd_derived_margins_are_known_fractions(monkeypatch):
    financials = {
        "Income_Statement": {"quarterly": {
            f"2026-{month:02d}-01": {"date": f"2026-{month:02d}-01", "totalRevenue": 100.0,
                                    "grossProfit": 180.0, "operatingIncome": -250.0,
                                    "netIncome": 0.9}
            for month in (1, 4, 7, 10)
        }},
    }
    client, _ = eodhd_wire(monkeypatch, financials=financials)
    row, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    for field, numerator, expected in (
        ("gross_margin", "grossProfit", 1.8),
        ("operating_margin", "operatingIncome", -2.5),
        ("profit_margin", "netIncome", 0.009),
    ):
        assert row[field] == pytest.approx(expected)
        assert_receipt(row, field, provider="eodhd", raw_unit="fraction", raw_value=expected,
                       source_field=f"Financials.Income_Statement.quarterly:{numerator}_ttm/revenue_ttm",
                       unit_basis="computed_ratio")


def test_eodhd_normalization_switch_cannot_relabel_cached_legacy_margin(monkeypatch):
    client, requests = eodhd_wire(monkeypatch, highlights={"ProfitMargin": 1.8})

    async def drive():
        monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "0")
        legacy, _ = await client.fetch_fundamentals(SYMBOL)
        monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "1")
        canonical, _ = await client.fetch_fundamentals(SYMBOL)
        repeated, _ = await client.fetch_fundamentals(SYMBOL)
        monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "0")
        legacy_again, _ = await client.fetch_fundamentals(SYMBOL)
        return legacy, canonical, repeated, legacy_again

    legacy, canonical, repeated, legacy_again = asyncio.run(drive())
    assert canonical["profit_margin"] == 1.8
    assert repeated == canonical
    assert legacy["profit_margin"] == legacy_again["profit_margin"] == pytest.approx(0.018)
    assert legacy[UNIT_KEY]["profit_margin"]["unit"] == "unknown"
    assert legacy[UNIT_KEY]["profit_margin"]["transform_version"] == "eodhd_margin_legacy_v1"
    assert legacy[UNIT_KEY]["profit_margin"]["unit_basis"] == "supplier_field_contract"
    assert canonical[UNIT_KEY]["profit_margin"]["unit"] == "fraction"
    assert canonical[UNIT_KEY]["profit_margin"]["unit_basis"] == "supplier_field_contract"
    assert requests == ["fundamentals/SYNTH.US", "fundamentals/SYNTH.US"]


@pytest.mark.parametrize("origin", ["explicit_percent", "computed_ratio"])
def test_eodhd_legacy_receipts_keep_origin_without_certifying_units(monkeypatch, origin):
    monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "0")
    if origin == "explicit_percent":
        client, _ = eodhd_wire(monkeypatch, highlights={"GrossMargin": "0.9%"})
        raw_value, raw_unit = 0.9, "percent_points"
    else:
        client, _ = eodhd_wire(monkeypatch, financials={
            "Income_Statement": {"quarterly": {
                "2026-10-01": {"date": "2026-10-01", "totalRevenue": 100.0, "grossProfit": 180.0},
            }},
        })
        raw_value, raw_unit = 1.8, "fraction"
    row, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    receipt = row[UNIT_KEY]["gross_margin"]
    assert receipt["unit"] == "unknown"
    assert receipt["unit_basis"] == origin
    assert receipt["raw_unit"] == raw_unit
    assert receipt["raw_value"] == raw_value
    assert receipt["value"] == row["gross_margin"]


def test_eodhd_unversioned_cached_values_are_not_reissued_as_new_proof(monkeypatch):
    client, requests = eodhd_wire(monkeypatch, highlights={"ProfitMargin": 0.2762})

    async def drive():
        await client.fund_cache.set(f"f:{SYMBOL}", {"profit_margin": 0.002762})
        return await client.fetch_fundamentals(SYMBOL)

    row, error = asyncio.run(drive())
    assert error is None
    assert row["profit_margin"] == 0.2762
    assert row[UNIT_KEY]["profit_margin"]["raw_value"] == 0.2762
    assert requests == ["fundamentals/SYNTH.US"]


@pytest.mark.parametrize("fraction", [0.0, 0.009, -0.004, 1.5, 1.8, -2.5, "0.9%", "180%", "-250%"])
def test_yahoo_supplier_fraction_margins_keep_their_units(monkeypatch, fraction):
    client, constructions = yahoo_wire(
        monkeypatch, {source: fraction for source in YAHOO_MARGINS.values()},
    )
    row = client._blocking_fetch("SYNTH")
    explicit_percent = isinstance(fraction, str) and fraction.endswith("%")
    raw_value = float(str(fraction).removesuffix("%"))
    expected = raw_value / 100.0 if explicit_percent else raw_value
    for field, source in YAHOO_MARGINS.items():
        assert row[field] == expected
        assert_receipt(row, field, provider="yahoo_fundamentals",
                       raw_unit="percent_points" if explicit_percent else "fraction",
                       raw_value=raw_value, source_field=source,
                       unit_basis="explicit_percent" if explicit_percent else "supplier_field_contract")
    assert constructions == ["SYNTH"]


def test_yahoo_derived_margin_receipts_identify_the_computation(monkeypatch):
    client, _ = yahoo_wire(
        monkeypatch, {"grossProfits": 180.0, "ebitda": -250.0, "totalRevenue": 100.0,
                      "netMargins": -0.004},
    )
    row = client._blocking_fetch("SYNTH")
    for field, source, expected in (
        ("gross_margin", "grossProfits/revenue_ttm", 1.8),
        ("operating_margin", "ebitda/revenue_ttm", -2.5),
        ("profit_margin", "netMargins", -0.004),
    ):
        assert row[field] == expected
        assert_receipt(row, field, provider="yahoo_fundamentals", raw_unit="fraction",
                       raw_value=expected, source_field=source,
                       unit_basis="supplier_field_contract" if field == "profit_margin" else "computed_ratio")


def test_yahoo_cached_rows_preserve_receipts_and_ignore_unversioned_cache(monkeypatch):
    client, constructions = yahoo_wire(monkeypatch, {"profitMargins": 1.8})

    async def drive():
        await client.fund_cache.set("SYNTH", {"current_price": 20.0, "profit_margin": 0.018})
        fresh = await client.fetch_fundamentals("SYNTH")
        warm = await client.fetch_fundamentals("SYNTH")
        return fresh, warm

    fresh, warm = asyncio.run(drive())
    assert fresh["profit_margin"] == warm["profit_margin"] == 1.8
    assert fresh[UNIT_KEY] == warm[UNIT_KEY]
    assert constructions == ["SYNTH"]


def test_margin_changes_preserve_nonmargin_adapter_numbers(monkeypatch):
    eodhd, _ = eodhd_wire(monkeypatch, highlights={
        "ProfitMargin": 0.9, "ROE": 0.9, "RevenueGrowth": 180.0,
        "DividendYield": 0.9, "PayoutRatio": 0.9,
    })
    eodhd_row, error = asyncio.run(eodhd.fetch_fundamentals(SYMBOL))
    assert error is None
    for field, expected in (("roe", 0.009), ("revenue_growth_yoy", 1.8),
                            ("dividend_yield", 0.009), ("payout_ratio", 0.009)):
        assert eodhd_row[field] == pytest.approx(expected)
    yahoo, _ = yahoo_wire(monkeypatch, {
        "profitMargins": 1.8, "returnOnEquity": 1.8,
        "revenueGrowth": 1.8, "dividendYield": 0.009,
    })
    yahoo_row = yahoo._blocking_fetch("SYNTH")
    assert yahoo_row["profit_margin"] == 1.8
    assert yahoo_row["roe"] == yahoo_row["revenue_growth_yoy"] == pytest.approx(0.018)
    assert yahoo_row["dividend_yield"] == 0.009


@pytest.mark.parametrize("invalid", [None, True, float("nan"), float("inf"), "invalid"])
def test_invalid_supplier_margins_never_receive_numeric_proof(monkeypatch, invalid):
    eodhd, _ = eodhd_wire(monkeypatch, highlights={"ProfitMargin": invalid})
    eodhd_row, error = asyncio.run(eodhd.fetch_fundamentals(SYMBOL))
    assert error is None
    yahoo, _ = yahoo_wire(monkeypatch, {"profitMargins": invalid})
    yahoo_row = yahoo._blocking_fetch("SYNTH")
    for row in (eodhd_row, yahoo_row):
        assert "profit_margin" not in row
        assert "profit_margin" not in row.get(UNIT_KEY, {})
