"""Offline provider-spy regressions for instrument routing and provenance."""
from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace

import pytest

from core import data_engine_v2 as de
from core.provider_capabilities import providers_for_instrument
from core.data_validity import row_acquisition

ACQUIRED = "2026-10-07T08:10:00+00:00"
QUOTE_ASOF = "2026-10-07T08:09:00+00:00"


def quote(symbol, **changes):
    index_currency = {"^GSPC": "USD", "^GDAXI": "EUR", "^BSESN": "INR", "^TASI.SR": "SAR"}
    currency = index_currency.get(symbol) or ("EUR" if symbol.endswith(".MI") else "NZD" if symbol.endswith(".NZ") else "USD")
    result = {
        "symbol": symbol, "name": "Test instrument", "current_price": 100.0,
        "previous_close": 99.0, "currency": currency, "exchange": "Test venue",
        "timestamp": QUOTE_ASOF,
    }
    result.update(changes)
    return result


def engine(monkeypatch, *, providers=None, quote_result=quote, history_result=()):
    instance = de.DataEngineV5(settings=SimpleNamespace(), providers=providers)
    calls = []
    modules = {}
    for provider in ("eodhd", "yahoo_chart", "finnhub", "tadawul", "custom"):
        async def fetch(symbol, provider=provider):
            calls.append(("quote", provider, symbol))
            return quote_result(symbol) if callable(quote_result) else quote_result

        async def history(symbol, provider=provider):
            calls.append(("history", provider, symbol))
            return list(history_result)

        modules[provider] = SimpleNamespace(get_quote=fetch, get_history=history)
    instance._provider_registry._modules = modules

    async def no_sideband(*args, **kwargs):
        return {}

    async def paid_sideband(*args, **kwargs):
        calls.append(("fundamentals", "eodhd", args[0]))
        return {}

    monkeypatch.setattr(instance, "_fetch_yahoo_fundamentals_patch", no_sideband)
    monkeypatch.setattr(instance, "_fetch_yahoo_chart_patch", no_sideband)
    monkeypatch.setattr(instance, "_fetch_eodhd_fundamentals_patch", paid_sideband)
    monkeypatch.setattr(de, "_now_utc_iso", lambda: ACQUIRED)
    monkeypatch.setenv("TFB_EODHD_FUNDAMENTALS_FALLBACK", "1")
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "off")
    return instance, calls


@pytest.mark.parametrize("symbol", ["GC=F", "CL=F", "^TASI.SR", "ENI.MI", "AIR.NZ"])
@pytest.mark.parametrize("page", ["", "Commodities_FX", "Global_Markets", "My_Portfolio"])
def test_quote_history_and_enrichment_share_capability_route(monkeypatch, symbol, page):
    instance, calls = engine(monkeypatch)

    async def run():
        row = await instance._get_enriched_quote_impl(symbol, page)
        await instance._get_history_patch_best_effort(symbol, page)
        return row

    row = asyncio.run(run())
    assert calls and all(provider == "yahoo_chart" for _, provider, _ in calls)
    assert all(requested == symbol for _, _, requested in calls)
    assert row["current_price"] == 100.0
    assert row["currency"] == quote(symbol)["currency"]
    assert row["data_provider"] == "yahoo_chart"
    assert "acquisition_status:success" in row["warnings"]
    assert "acquisition_quote_asof:" + QUOTE_ASOF in row["warnings"]
    assert "acquisition_acquired_at:" + ACQUIRED in row["warnings"]
    assert instance._resolve_quote_page_context(symbol, page)[1] == "yahoo_chart"


@pytest.mark.parametrize("symbol", ["AAPL.US", "2222.SR", "SPUS.US", "ALV.DE", "USDSAR=X", "^GSPC", "^GDAXI", "^BSESN"])
def test_other_instruments_keep_existing_default_and_explicit_order(monkeypatch, symbol):
    instance, _ = engine(monkeypatch)
    assert instance._providers_for_instrument("Global_Markets", symbol) == instance._providers_for("Global_Markets")
    configured = ["custom", "yahoo_chart", "eodhd", "finnhub"]
    instance, _ = engine(monkeypatch, providers=configured)
    assert instance._providers_for_instrument("My_Portfolio", symbol) == configured


def history_bars():
    return [{"timestamp": "2026-10-07", "close": 90.0 + index}
            for index in range(30)]


@pytest.mark.parametrize("symbol", ["^GSPC", "^GDAXI", "^BSESN"])
@pytest.mark.parametrize("page", ["", "Commodities_FX", "Global_Markets", "My_Portfolio"])
def test_other_indices_keep_eodhd_quote_and_history_when_yahoo_is_unavailable(monkeypatch, symbol, page):
    instance, calls = engine(monkeypatch, history_result=history_bars())

    async def yahoo_outage(requested):
        calls.append(("quote", "yahoo_chart", requested))
        return {"symbol": requested, "error": "fetch_failed:HTTP429", "warnings": "fetch_failed:HTTP429"}

    instance._provider_registry._modules["yahoo_chart"].get_quote = yahoo_outage

    async def run():
        row = await instance._get_enriched_quote_impl(symbol, page)
        history = await instance._get_history_patch_best_effort(symbol, page)
        return row, history

    row, history = asyncio.run(run())
    assert row["current_price"] == 100.0 and row["data_provider"] == "eodhd"
    assert row["symbol"] == symbol and row["currency"] == quote(symbol)["currency"]
    assert instance._resolve_quote_page_context(symbol, page)[1] == "eodhd"
    price_history_calls = [(kind, provider, requested) for kind, provider, requested in calls
                           if kind in {"quote", "history"}]
    assert price_history_calls and all(provider == "eodhd" and requested == symbol
                                       for _, provider, requested in price_history_calls)
    assert history["volatility_30d"] is not None
    assert row_acquisition(row, datetime.fromisoformat(ACQUIRED), 3600).successful


@pytest.mark.parametrize("symbol", ["^GSPC", "^GDAXI", "^BSESN"])
def test_other_indices_fall_back_to_yahoo_for_both_quote_and_history(monkeypatch, symbol):
    instance, calls = engine(monkeypatch, providers=["eodhd", "yahoo_chart"], history_result=history_bars())

    async def failed_quote(requested):
        calls.append(("quote", "eodhd", requested))
        return {"symbol": requested, "error": "fetch_failed:HTTP422",
                "warnings": "fetch_failed:HTTP422", "currency": "WRONG"}

    async def failed_history(requested):
        calls.append(("history", "eodhd", requested))
        return []

    instance._provider_registry._modules["eodhd"].get_quote = failed_quote
    instance._provider_registry._modules["eodhd"].get_history = failed_history

    async def run():
        row = await instance._get_enriched_quote_impl(symbol, "Commodities_FX")
        history = await instance._get_history_patch_best_effort(symbol, "Commodities_FX")
        return row, history

    row, history = asyncio.run(run())
    assert [provider for kind, provider, _ in calls if kind == "quote"] == ["eodhd", "yahoo_chart"]
    assert {provider for kind, provider, _ in calls if kind == "history"} == {"eodhd", "yahoo_chart"}
    assert row["current_price"] == 100.0 and row["data_provider"] == "yahoo_chart"
    assert row["symbol"] == symbol and row["currency"] == quote(symbol)["currency"]
    assert "quote_attempt:eodhd:unpriced" in row["warnings"] and "fetch_failed" not in row["warnings"]
    assert history["volatility_30d"] is not None
    assert row_acquisition(row, datetime.fromisoformat(ACQUIRED), 3600).successful


@pytest.mark.parametrize("symbol", ["^GSPC", "^GDAXI", "^BSESN"])
def test_other_indices_remain_unavailable_when_both_quote_sources_fail(monkeypatch, symbol):
    instance, calls = engine(monkeypatch, providers=["eodhd", "yahoo_chart"],
                             quote_result={"error": "fetch_failed:timeout", "warnings": "fetch_failed:timeout"})
    row = asyncio.run(instance._get_enriched_quote_impl(symbol, "Commodities_FX"))
    assert [provider for kind, provider, _ in calls if kind == "quote"] == ["eodhd", "yahoo_chart"]
    assert row.get("current_price") is None and "acquisition_status:unavailable" in row["warnings"]
    assert not row_acquisition(row, datetime.fromisoformat(ACQUIRED), 3600).successful


def test_configured_eodhd_only_is_retained_for_other_caret_indices(monkeypatch):
    instance, calls = engine(monkeypatch, providers=["eodhd"])
    row = asyncio.run(instance._get_enriched_quote_impl("^GDAXI", "Commodities_FX"))
    assert instance._providers_for_instrument("Commodities_FX", "^GDAXI") == ["eodhd"]
    assert row["data_provider"] == "eodhd" and row["currency"] == "EUR"
    assert [provider for kind, provider, _ in calls if kind == "quote"] == ["eodhd"]
    assert row_acquisition(row, datetime.fromisoformat(ACQUIRED), 3600).successful


@pytest.mark.parametrize("symbol", ["GC=F", "^TASI.SR", "ENI.MI", "AIR.NZ"])
def test_explicit_configuration_does_not_enable_omitted_yahoo(monkeypatch, symbol):
    instance, calls = engine(monkeypatch, providers=["eodhd", "finnhub"])
    assert instance._providers_for_instrument("Global_Markets", symbol) == []

    async def run():
        row = await instance._get_enriched_quote_impl(symbol, "Global_Markets")
        await instance._get_history_patch_best_effort(symbol, "Global_Markets")
        return row

    row = asyncio.run(run())
    assert calls == []
    assert "acquisition_status:unavailable" in row["warnings"]


@pytest.mark.parametrize("provider", ["yahoo", "yfinance", "yahoo_chart"])
def test_existing_yahoo_alias_configuration_is_retained(provider):
    assert providers_for_instrument("GC=F", ["eodhd", provider, "finnhub"]) == [provider]


def test_finnhub_price_entrypoint_is_not_enabled():
    pytest.importorskip("httpx", reason="Finnhub's unchanged module requires optional HTTP client")
    from core.providers import finnhub_provider

    assert de._pick_provider_callable(
        finnhub_provider, "get_quote_async", "fetch_quote_async", "get_quote",
        "fetch_quote", "quote_async", "quote", "get_unified_quote", "fetch",
    ) is None


def test_direct_quote_history_retry_cannot_bypass_capabilities(monkeypatch):
    instance, calls = engine(monkeypatch)

    async def run():
        assert await instance._fetch_patch("eodhd", "GC=F") == {}
        assert await instance._fetch_history_patch("eodhd", "^TASI.SR") == {}

    asyncio.run(run())
    assert calls == []


def test_wrong_symbol_echo_is_rejected_before_success_provenance(monkeypatch):
    monkeypatch.setenv("TFB_ENGINE_IDENTITY_GUARD", "1")
    instance, calls = engine(monkeypatch, quote_result=lambda symbol: quote(symbol, symbol="WRONG.US"))
    row = asyncio.run(instance._get_enriched_quote_impl("ENI.MI", "Global_Markets"))
    assert row.get("current_price") is None
    assert "acquisition_status:success" not in row["warnings"]
    assert all(provider == "yahoo_chart" for _, provider, _ in calls)


def test_failed_primary_does_not_poison_successful_secondary_quote(monkeypatch):
    instance, calls = engine(monkeypatch)

    async def failed_primary(symbol):
        calls.append(("quote", "eodhd", symbol))
        return {"symbol": symbol, "error": "HTTP422", "warnings": "fetch_failed:HTTP422",
                "currency": "WRONG", "timestamp": "2026-01-01T00:00:00+00:00"}

    instance._provider_registry._modules["eodhd"].get_quote = failed_primary
    row = asyncio.run(instance._get_enriched_quote_impl("AAPL.US", "Global_Markets"))
    assert [provider for kind, provider, _ in calls if kind == "quote"] == ["eodhd", "yahoo_chart"]
    assert row["current_price"] == 100.0
    assert row["currency"] == "USD"
    assert row["data_provider"] == "yahoo_chart"
    assert row["price_bar_ts"] == QUOTE_ASOF
    assert "acquisition_provider:yahoo_chart" in row["warnings"]
    assert "quote_attempt:eodhd:unpriced" in row["warnings"]
    assert "fetch_failed" not in row["warnings"]


def test_all_failed_sources_keep_terminal_failure_and_unavailable_acquisition(monkeypatch):
    instance, calls = engine(monkeypatch, providers=["eodhd", "yahoo_chart"], quote_result={
        "error": "HTTP422", "warnings": "fetch_failed:HTTP422",
    })
    row = asyncio.run(instance._get_enriched_quote_impl("AAPL.US", "Global_Markets"))
    assert row.get("current_price") is None
    assert "fetch_failed:HTTP422" in row["warnings"]
    assert "acquisition_status:unavailable" in row["warnings"]
    assert "quote_attempt:eodhd:unpriced" in row["warnings"]
    assert "quote_attempt:yahoo_chart:unpriced" in row["warnings"]


@pytest.mark.parametrize("failure", [
    {"warnings": "fetch_failed:HTTP402"},
    {"warnings": ["fetch_failed:HTTP402"]},
    {"error": "fetch_failed:HTTP402"},
])
def test_later_unpriced_shell_cannot_erase_earlier_terminal_fetch_failure(monkeypatch, failure):
    instance, calls = engine(monkeypatch, providers=["eodhd", "yahoo_chart"],
                             quote_result=lambda symbol: {"symbol": symbol, "name": "Empty quote shell"})

    async def failed_primary(symbol):
        calls.append(("quote", "eodhd", symbol))
        return {"symbol": symbol, **failure}

    instance._provider_registry._modules["eodhd"].get_quote = failed_primary
    row = asyncio.run(instance._get_enriched_quote_impl("AAPL.US", "Global_Markets"))
    assert [provider for kind, provider, _ in calls if kind == "quote"] == ["eodhd", "yahoo_chart"]
    assert row.get("current_price") is None
    assert "fetch_failed" in row["warnings"]
    assert "acquisition_status:unavailable" in row["warnings"]
    assert not row_acquisition(row, datetime.fromisoformat(ACQUIRED), 3600).successful


def test_actual_success_after_multiple_unpriced_attempts_keeps_failure_scoped(monkeypatch):
    instance, calls = engine(monkeypatch, providers=["eodhd", "custom", "yahoo_chart"])

    async def failed_primary(symbol):
        calls.append(("quote", "eodhd", symbol))
        return {"symbol": symbol, "warnings": "fetch_failed:HTTP402"}

    async def unpriced_secondary(symbol):
        calls.append(("quote", "custom", symbol))
        return {"symbol": symbol, "name": "Empty quote shell"}

    instance._provider_registry._modules["eodhd"].get_quote = failed_primary
    instance._provider_registry._modules["custom"].get_quote = unpriced_secondary
    row = asyncio.run(instance._get_enriched_quote_impl("AAPL.US", "Global_Markets"))
    assert [provider for kind, provider, _ in calls if kind == "quote"] == ["eodhd", "custom", "yahoo_chart"]
    assert row["current_price"] == 100.0 and row["data_provider"] == "yahoo_chart"
    assert "quote_attempt:eodhd:unpriced" in row["warnings"]
    assert "quote_attempt:custom:unpriced" in row["warnings"]
    assert "fetch_failed" not in row["warnings"]
    assert row_acquisition(row, datetime.fromisoformat(ACQUIRED), 3600).successful


def test_priced_provider_failure_is_not_sanitized_into_success(monkeypatch):
    instance, _ = engine(monkeypatch, quote_result=lambda symbol: quote(symbol, warnings="fetch_failed:served_last_good"))
    row = asyncio.run(instance._get_enriched_quote_impl("GC=F", "Commodities_FX"))
    assert "fetch_failed:served_last_good" in row["warnings"]
    assert "quote_attempt:yahoo_chart:unpriced" not in row["warnings"]
    assert "acquisition_status:failed" in row["warnings"]
    assert "acquisition_status:success" not in row["warnings"]


def test_raw_priced_fetch_failure_survives_canonical_projection(monkeypatch):
    from datetime import datetime
    from core.data_validity import row_acquisition

    instance, _ = engine(monkeypatch, quote_result=lambda symbol: quote(symbol, error="fetch_failed"))
    row = asyncio.run(instance._get_enriched_quote_impl("GC=F", "Commodities_FX"))
    assert row["current_price"] == 100.0
    assert "fetch_failed:quote_provider_error" in row["warnings"]
    assert "acquisition_status:failed" in row["warnings"]
    assert "acquisition_status:success" not in row["warnings"]
    assert not row_acquisition(row, datetime.fromisoformat(ACQUIRED), 3600).successful


def test_missing_source_quote_time_stays_unknown_without_fabricating_it(monkeypatch):
    instance, _ = engine(monkeypatch, quote_result=lambda symbol: quote(symbol, timestamp=None))
    row = asyncio.run(instance._get_enriched_quote_impl("GC=F", "Commodities_FX"))
    assert "acquisition_status:success" in row["warnings"]
    assert "acquisition_quote_asof:" not in row["warnings"]
    assert "acquisition_acquired_at:" + ACQUIRED in row["warnings"]


@pytest.mark.parametrize("source_time", ["2026-10-07", "2026-10-07T08:09:00", None, True, "bad"])
def test_source_quote_instant_requires_precise_timezone_evidence(source_time):
    assert de._quote_acquisition_asof(source_time) == ""


def test_cache_read_preserves_original_retrieval_and_quote_times(monkeypatch):
    instance, calls = engine(monkeypatch)

    async def run():
        first = await instance._get_enriched_quote_impl("GC=F", "Commodities_FX")
        initial_calls = list(calls)
        monkeypatch.setattr(de, "_now_utc_iso", lambda: "2026-10-07T09:00:00+00:00")
        second = await instance._get_enriched_quote_impl("GC=F", "Commodities_FX")
        assert calls == initial_calls
        assert second["warnings"] == first["warnings"]
        assert "acquisition_acquired_at:" + ACQUIRED in second["warnings"]

    asyncio.run(run())


@pytest.mark.parametrize("quote_result", [None, {}, {"error": "fetch_failed", "warnings": "fetch_failed:timeout"}, {"current_price": 0.0}])
def test_outage_and_invalid_price_cannot_claim_success(monkeypatch, quote_result):
    instance, calls = engine(monkeypatch, quote_result=quote_result)
    row = asyncio.run(instance._get_enriched_quote_impl("GC=F", "Commodities_FX"))
    assert "acquisition_status:success" not in row["warnings"]
    assert "acquisition_status:unavailable" in row["warnings"]
    assert all(provider == "yahoo_chart" for _, provider, _ in calls)


def test_snapshot_retains_old_stamp_and_cannot_inherit_success(monkeypatch):
    instance, _ = engine(monkeypatch, quote_result={})

    async def run():
        snapshot = quote("GC=F", last_updated_utc="2026-10-05T08:00:00+00:00",
                         data_provider="yahoo_chart", warnings="acquisition_status:success; acquisition_acquired_at:2026-10-05T08:00:00+00:00")
        await instance._store_sheet_snapshot("Commodities_FX", [snapshot])
        return await instance._get_enriched_quote_impl("GC=F", "Commodities_FX")

    row = asyncio.run(run())
    assert row["current_price"] == 100.0
    assert row["last_updated_utc"] == "2026-10-05T08:00:00+00:00"
    assert "acquisition_status:preserved" in row["warnings"]
    assert "acquisition_status:success" not in row["warnings"]
    assert "acquisition_acquired_at:" not in row["warnings"]


def test_history_technicals_do_not_mint_a_price_acquisition_with_gate_off(monkeypatch):
    monkeypatch.setenv("TFB_GATE_PRICE_STALENESS", "0")
    instance, _ = engine(monkeypatch, quote_result={}, history_result=[
        {"timestamp": "2026-10-07", "close": 90.0 + index}
        for index in range(30)
    ])
    row = asyncio.run(instance._get_enriched_quote_impl("GC=F", "Commodities_FX"))
    assert row.get("current_price") is None
    assert row["volatility_30d"] is not None
    assert "acquisition_status:unavailable" in row["warnings"]
    assert "acquisition_status:success" not in row["warnings"]
