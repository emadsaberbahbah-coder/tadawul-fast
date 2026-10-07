"""Offline acquisitions prove symbol misses cannot disable healthy Yahoo quotes.

The real chart parser, two-host ladder, executor, cache, single-flight and
circuit breaker run. Only the HTTP seam and blocking yfinance seam are faked;
neither fixture can make a network request.
"""
from __future__ import annotations

import asyncio
from collections import Counter
import math
import threading
from datetime import datetime
from types import SimpleNamespace

import pytest

from core.providers import yahoo_chart_provider as yc


REAL_RAW_FETCH = yc._raw_chart_fetch_triple
REAL_SYNC_FETCH = yc._fetch_ticker_sync
QUOTE_TIME = "2026-10-07T12:00:00+00:00"
QUOTE_EPOCH = int(datetime.fromisoformat(QUOTE_TIME).timestamp())


def chart(symbol, *, price=123.0, currency="USD", error=None):
    return {"chart": {"error": error, "result": [{
        "meta": {"symbol": symbol, "regularMarketPrice": price,
                 "regularMarketTime": QUOTE_EPOCH, "currency": currency},
        "timestamp": [], "indicators": {"quote": [{}]},
    }]}}


def run(provider, symbol):
    # Separate asyncio.run calls also exercise the process-lived state across
    # cockpit loops, rather than merely checking a classifier in isolation.
    return asyncio.run(provider.get_enriched_quote(symbol))


@pytest.fixture
def provider(monkeypatch):
    async def unconfigured_http(*args, **kwargs):
        raise AssertionError("HTTP fake must be configured by this test")

    monkeypatch.setattr(yc, "_HAS_HTTPX", True)
    monkeypatch.setattr(yc, "_HAS_YFINANCE", True)
    monkeypatch.setattr(yc, "_raw_chart_enabled", lambda: True)
    monkeypatch.setattr(yc, "_raw_chart_fetch_triple", REAL_RAW_FETCH)
    monkeypatch.setattr(yc, "_raw_http_get_json", unconfigured_http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", lambda *args: ({}, {}, []))
    monkeypatch.setenv("TFB_CHART_IDENTITY_GUARD", "1")
    monkeypatch.delenv("TFB_YC_SYMBOL_SKIP", raising=False)
    instance = yc.YahooChartProvider(yc.YahooConfig(
        enabled=True, rate_limit_per_sec=0, cb_fail_threshold=2,
        cb_cooldown_sec=60, cb_success_threshold=1, quote_ttl_sec=120,
        threadpool_workers=2,
    ))
    yield instance
    instance._executor.shutdown(wait=True)


@pytest.mark.parametrize("miss", ["http_404", "not_found", "empty_result", "null_result"])
def test_symbol_misses_above_threshold_leave_other_instruments_available(
    provider, monkeypatch, miss,
):
    calls = []

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        calls.append(symbol)
        if symbol.startswith("MISSING"):
            if miss == "http_404":
                raise yc.YahooFetchError("http_404")
            if miss == "not_found":
                return {"chart": {"result": None, "error": {"code": "Not Found"}}}
            return {"chart": {"result": None if miss == "null_result" else [],
                              "error": None}}
        return chart(symbol)

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    for symbol in ("MISSING1", "MISSING2", "MISSING3"):
        assert run(provider, symbol) is None
    assert provider._circuit_breaker.failures == 0
    assert provider._circuit_breaker.state == "closed"
    quote = run(provider, "VALID")
    assert quote["current_price"] == 123.0
    assert quote["symbol"] == "VALID" and quote["currency"] == "USD"
    assert calls == ["MISSING1", "MISSING1", "MISSING2", "MISSING2",
                     "MISSING3", "MISSING3", "VALID"]


def test_local_miss_does_not_erase_real_provider_failures(provider, monkeypatch):
    provider._circuit_breaker.failures = 1

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    assert run(provider, "MISSING") is None
    assert provider._circuit_breaker.failures == 1
    assert provider._circuit_breaker.state == "closed"


def test_yfinance_only_empty_symbols_do_not_disable_priced_fallback(
    provider, monkeypatch,
):
    monkeypatch.setattr(yc, "_raw_chart_enabled", lambda: False)
    monkeypatch.setattr(yc, "_HAS_YFINANCE", True)

    def sync(symbol, *args):
        if symbol.startswith("MISSING"):
            return {}, {}, []
        return {"symbol": symbol, "currentPrice": 123, "currency": "USD"}, {}, []

    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    for symbol in ("MISSING1", "MISSING2", "MISSING3"):
        assert run(provider, symbol) is None
    assert provider._circuit_breaker.failures == 0
    assert run(provider, "VALID")["current_price"] == 123


@pytest.mark.parametrize("price", [None, 0, -1, float("nan"), float("inf")])
def test_raw_metadata_without_usable_price_is_a_local_miss(
    provider, monkeypatch, price,
):
    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        return chart(symbol, price=price if symbol.startswith("UNPRICED") else 123)

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    for symbol in ("UNPRICED1", "UNPRICED2", "UNPRICED3"):
        row = run(provider, symbol)
        returned_price = row.get("current_price") if row else None
        assert returned_price is None or not (
            math.isfinite(float(returned_price)) and float(returned_price) > 0
        )
        assert asyncio.run(provider._cache.get(symbol, kind="enriched")) is None
    assert provider._circuit_breaker.failures == 0
    assert run(provider, "VALID")["current_price"] == 123


def failure(name):
    if name == "timeout":
        return TimeoutError("Yahoo transport timed out")
    if name == "network":
        return ConnectionError("Yahoo transport connection failed")
    if name == "unknown":
        return RuntimeError("unclassified fetch failure")
    return yc.YahooFetchError(name)


@pytest.mark.parametrize("reason", [
    "http_401", "http_403", "http_429", "http_500", "http_503",
    "timeout", "network", "unknown", "malformed", "chart_unknown_error",
    "possibly delisted; no price data found (HTTP Error 429)",
])
def test_provider_outages_still_open_global_breaker(provider, monkeypatch, reason):
    calls = []

    async def http(url, *args):
        calls.append(url)
        if reason == "malformed":
            return {}
        if reason == "chart_unknown_error":
            return {"chart": {"result": None, "error": {"code": "Unauthorized"}}}
        raise failure(reason)

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    assert run(provider, "FAIL1") is None
    assert provider._circuit_breaker.failures == 1
    assert run(provider, "FAIL2") is None
    assert provider._circuit_breaker.failures == 2
    assert provider._circuit_breaker.state == "open"
    assert run(provider, "VALID") is None
    assert len(calls) == 4  # Third symbol cannot retry the protected raw leg.


@pytest.mark.parametrize("host_reasons", [
    ("http_429", "http_404"), ("http_404", "http_429"),
    ("http_401", "http_404"), ("http_404", "http_503"),
    ("timeout", "http_404"),
])
def test_host_ladder_retains_provider_outage_even_if_other_host_is_local(
    provider, monkeypatch, host_reasons,
):
    async def http(url, *args):
        host_index = 0 if url.startswith(yc._RAW_CHART_HOSTS[0]) else 1
        raise failure(host_reasons[host_index])

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    assert run(provider, "FAIL1") is None
    assert provider._circuit_breaker.failures == 1
    assert run(provider, "FAIL2") is None
    assert provider._circuit_breaker.state == "open"


@pytest.mark.parametrize("fallback_shape,reason", [
    ("metadata", None), ("exception", "http_401"),
    ("exception", "http_429"), ("exception", "timeout"),
])
@pytest.mark.parametrize("healthy,price,currency", [
    ("KE=F", 7.25, "USD"), ("USDSAR=X", 3.75, "SAR"),
])
def test_fallback_outage_cannot_disable_healthy_raw_quotes(
    provider, monkeypatch, fallback_shape, reason, healthy, price, currency,
):
    http_calls = []
    fallback_calls = []

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        http_calls.append(symbol)
        if symbol == healthy:
            return chart(symbol, price=price, currency=currency)
        raise yc.YahooFetchError("http_404")

    def sync(symbol, *args):
        fallback_calls.append(symbol)
        if fallback_shape == "exception":
            raise failure(reason)
        return {"_yc_provider_outage": True}, {}, []

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    assert run(provider, "FAIL1") is None
    assert provider._circuit_breaker.failures == 0
    assert provider._fallback_circuit_breaker.failures == 1
    assert run(provider, "FAIL2") is None
    assert provider._circuit_breaker.state == "closed"
    assert provider._fallback_circuit_breaker.state == "open"
    # A local miss is still backed off even when its fallback auth failed.
    before_repeat = list(http_calls)
    assert run(provider, "FAIL1") is None
    assert http_calls == before_repeat
    assert run(provider, "FAIL3") is None
    assert fallback_calls == ["FAIL1", "FAIL2"]
    quote = run(provider, healthy)
    assert quote["current_price"] == price and quote["currency"] == currency
    assert quote["symbol"] == healthy and quote["timestamp"] == QUOTE_TIME
    assert quote["provider"] == yc.PROVIDER_NAME
    assert http_calls[-1] == healthy
    assert provider._circuit_breaker.failures == 0
    # A successful raw request cannot report recovery of the crumb transport.
    assert provider._fallback_circuit_breaker.state == "open"
    assert provider._fallback_circuit_breaker.failures == 2


def test_local_fallback_exception_does_not_erase_prior_raw_outage(
    provider, monkeypatch,
):
    async def http(url, *args):
        raise yc.YahooFetchError("http_429")

    def sync(*args):
        raise yc.YahooFetchError("http_404", provider_outage=False)

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    assert run(provider, "FAIL1") is None
    assert provider._circuit_breaker.failures == 1
    assert run(provider, "FAIL2") is None
    assert provider._circuit_breaker.state == "open"


@pytest.mark.parametrize("raw_enabled", [True, False])
@pytest.mark.parametrize("auth_error", [
    "HTTP Error 401: Unauthorized", "Invalid Crumb", "HTTP Error 429",
])
def test_sync_swallowed_auth_is_protected_on_its_active_transport(
    provider, monkeypatch, raw_enabled, auth_error,
):
    class Ticker:
        fast_info = {}
        history_metadata = {}

        @property
        def info(self):
            raise RuntimeError(auth_error)

        def history(self, **kwargs):
            return None

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_raw_chart_enabled", lambda: raw_enabled)
    monkeypatch.setattr(yc, "_HAS_YFINANCE", True)
    monkeypatch.setattr(yc, "yf", SimpleNamespace(Ticker=lambda symbol: Ticker()))
    monkeypatch.setattr(yc, "_fetch_ticker_sync", REAL_SYNC_FETCH)
    assert run(provider, "FAIL1") is None
    assert provider._circuit_breaker.failures == 0
    assert provider._fallback_circuit_breaker.failures == 1
    assert run(provider, "FAIL2") is None
    assert provider._circuit_breaker.state == "closed"
    assert provider._fallback_circuit_breaker.state == "open"


@pytest.mark.parametrize("status", [401, 403, 429, 503])
def test_wrapped_yfinance_missing_price_error_retains_explicit_outage_status(
    provider, monkeypatch, status,
):
    class YFPricesMissingError(Exception):
        pass

    class Ticker:
        fast_info = {}
        info = {}
        history_metadata = {}

        def history(self, **kwargs):
            raise YFPricesMissingError(
                "possibly delisted; no price data found "
                f"(Yahoo status_code = {status})"
            )

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "yf", SimpleNamespace(Ticker=lambda symbol: Ticker()))
    monkeypatch.setattr(yc, "_fetch_ticker_sync", REAL_SYNC_FETCH)
    assert run(provider, "FAIL1") is None
    assert provider._fallback_circuit_breaker.failures == 1
    assert run(provider, "FAIL2") is None
    assert provider._fallback_circuit_breaker.state == "open"
    assert provider._circuit_breaker.failures == 0


@pytest.mark.parametrize("description,is_outage", [
    ("Internal Server Error", True), ("Service Unavailable", True),
    ("User is unable to access this feature", True), ("Not Found", False),
])
def test_wrapped_yahoo_error_description_outweighs_missing_price_class(
    provider, monkeypatch, description, is_outage,
):
    class YFPricesMissingError(Exception):
        pass

    class Ticker:
        fast_info = {}
        info = {}
        history_metadata = {}

        def history(self, **kwargs):
            raise YFPricesMissingError(
                "possibly delisted; no price data found "
                f'(Yahoo error = "{description}")'
            )

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        if symbol == "VALID":
            return chart(symbol)
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "yf", SimpleNamespace(Ticker=lambda symbol: Ticker()))
    monkeypatch.setattr(yc, "_fetch_ticker_sync", REAL_SYNC_FETCH)
    assert run(provider, "FAIL1") is None
    assert provider._fallback_circuit_breaker.failures == int(is_outage)
    assert run(provider, "FAIL2") is None
    assert provider._fallback_circuit_breaker.state == ("open" if is_outage else "closed")
    assert provider._circuit_breaker.failures == 0
    assert run(provider, "VALID")["current_price"] == 123


def test_sync_history_requests_errors_without_changing_existing_history_options(
    provider, monkeypatch,
):
    calls = []

    class Ticker:
        fast_info = {}
        info = {}
        history_metadata = {}

        def history(self, **kwargs):
            calls.append(kwargs)
            assert kwargs == {
                "period": provider.config.history_period,
                "interval": provider.config.history_interval,
                "auto_adjust": True,
                "raise_errors": True,
            }
            raise TimeoutError("history transport timeout")

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "yf", SimpleNamespace(Ticker=lambda symbol: Ticker()))
    monkeypatch.setattr(yc, "_fetch_ticker_sync", REAL_SYNC_FETCH)
    assert run(provider, "FAIL1") is None
    assert run(provider, "FAIL2") is None
    assert len(calls) == 2
    assert provider._fallback_circuit_breaker.state == "open"
    assert provider._circuit_breaker.failures == 0


@pytest.mark.parametrize("reason", ["timeout", "status_503"])
def test_pinned_yfinance_history_exposes_outage_to_actual_fallback_accounting(
    provider, monkeypatch, reason,
):
    library = pytest.importorskip("yfinance")
    assert library.__version__ == "0.2.66"
    from yfinance.scrapers.history import PriceHistory

    calls = []

    class Data:
        def get(self, **kwargs):
            calls.append(kwargs)
            if reason == "timeout":
                raise TimeoutError("history transport timeout")
            return SimpleNamespace(text="", json=lambda: {"status_code": 503})

        cache_get = get

    # The pinned library's default swallows these failures into an empty
    # DataFrame. A truthy inert session prevents creation of a real client.
    baseline = PriceHistory(Data(), "BASELINE", "UTC", session=object())
    empty = baseline.history(period="2y", interval="1d", auto_adjust=True)
    assert empty.empty

    class Ticker:
        fast_info = {}
        info = {}
        history_metadata = {}

        def __init__(self, symbol):
            self.history_bridge = PriceHistory(Data(), symbol, "UTC", session=object())

        def history(self, **kwargs):
            return self.history_bridge.history(**kwargs)

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "yf", SimpleNamespace(Ticker=Ticker))
    monkeypatch.setattr(yc, "_fetch_ticker_sync", REAL_SYNC_FETCH)
    assert run(provider, "FAIL1") is None
    assert provider._fallback_circuit_breaker.failures == 1
    assert run(provider, "FAIL2") is None
    assert provider._fallback_circuit_breaker.state == "open"
    assert provider._circuit_breaker.failures == 0
    assert len(calls) == 3  # One library baseline plus one fetch per acquisition.


@pytest.mark.parametrize("priced", [False, True])
@pytest.mark.parametrize("getter", ["property", "method"])
def test_metadata_outage_is_recorded_only_when_fallback_has_no_usable_quote(
    provider, monkeypatch, priced, getter,
):
    class Ticker:
        info = {}
        fast_info = ({"symbol": "ENI.MI", "currentPrice": 15.5, "currency": "EUR",
                      "regularMarketTime": QUOTE_EPOCH} if priced else {})

        @property
        def history_metadata(self):
            if getter == "property":
                raise RuntimeError("HTTP Error 503")
            return {}

        def get_history_metadata(self):
            if getter == "method":
                raise RuntimeError("HTTP Error 401: Unauthorized")
            return {}

        def history(self, **kwargs):
            return None

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "yf", SimpleNamespace(Ticker=lambda symbol: Ticker()))
    monkeypatch.setattr(yc, "_fetch_ticker_sync", REAL_SYNC_FETCH)
    quote = run(provider, "ENI.MI")
    if priced:
        assert quote["current_price"] == 15.5 and quote["currency"] == "EUR"
        assert quote["symbol"] == "ENI.MI" and quote["timestamp"] == QUOTE_TIME
        assert provider._fallback_circuit_breaker.failures == 0
    else:
        assert quote is None
        assert provider._fallback_circuit_breaker.failures == 1
    assert provider._circuit_breaker.failures == 0


def test_usable_fallback_quote_keeps_currency_identity_and_quote_time(
    provider, monkeypatch,
):
    async def http(url, *args):
        raise yc.YahooFetchError("http_503")

    def sync(*args):
        return ({"symbol": "ENI.MI", "currentPrice": 15.5, "currency": "EUR",
                 "regularMarketTime": QUOTE_EPOCH, "_yc_provider_outage": True},
                {"symbol": "ENI.MI"}, [])

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    quote = run(provider, "ENI.MI")
    assert quote["symbol"] == "ENI.MI" and quote["currency"] == "EUR"
    assert quote["current_price"] == 15.5 and quote["timestamp"] == QUOTE_TIME
    assert quote["provider"] == yc.PROVIDER_NAME
    assert "_yc_provider_outage" not in quote
    assert provider._circuit_breaker.failures == 1  # Actual raw503 remains unhealthy.
    assert provider._fallback_circuit_breaker.failures == 0


def test_open_raw_transport_still_serves_healthy_fallback_and_is_not_healed_by_it(
    provider, monkeypatch,
):
    clock = {"now": 100.0}
    monkeypatch.setattr(yc, "time", SimpleNamespace(monotonic=lambda: clock["now"]))
    raw_calls = []
    fallback_calls = []

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        raw_calls.append(symbol)
        if symbol.startswith("FAIL"):
            raise yc.YahooFetchError("http_429")
        return chart(symbol, price=9.25, currency="EUR")

    def sync(symbol, *args):
        fallback_calls.append(symbol)
        return ({"symbol": symbol, "currentPrice": 15.5, "currency": "EUR",
                 "regularMarketTime": QUOTE_EPOCH}, {}, [])

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    for symbol in ("FAIL1", "FAIL2"):
        assert run(provider, symbol)["current_price"] == 15.5
    assert provider._circuit_breaker.state == "open"
    assert provider._circuit_breaker.failures == 2
    quote = run(provider, "ENI.MI")
    assert quote["current_price"] == 15.5 and quote["currency"] == "EUR"
    assert quote["symbol"] == "ENI.MI" and quote["timestamp"] == QUOTE_TIME
    assert raw_calls == ["FAIL1", "FAIL1", "FAIL2", "FAIL2"]
    assert fallback_calls == ["FAIL1", "FAIL2", "ENI.MI"]
    assert provider._circuit_breaker.state == "open"
    assert provider._circuit_breaker.failures == 2
    assert provider._fallback_circuit_breaker.failures == 0
    clock["now"] += 61
    quote = run(provider, "NEW.MI")
    assert quote["current_price"] == 9.25 and quote["currency"] == "EUR"
    assert raw_calls[-1] == "NEW.MI" and "NEW.MI" not in fallback_calls
    assert provider._circuit_breaker.state == "closed"
    assert provider._circuit_breaker.failures == 0


def test_usable_sync_quote_ignores_optional_info_auth_failure(provider, monkeypatch):
    class Ticker:
        fast_info = {"symbol": "ENI.MI", "currentPrice": 15.5, "currency": "EUR",
                     "regularMarketTime": QUOTE_EPOCH}
        history_metadata = {"symbol": "ENI.MI"}

        @property
        def info(self):
            raise RuntimeError("Invalid Crumb")

        def history(self, **kwargs):
            return None

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "yf", SimpleNamespace(Ticker=lambda symbol: Ticker()))
    monkeypatch.setattr(yc, "_fetch_ticker_sync", REAL_SYNC_FETCH)
    quote = run(provider, "ENI.MI")
    assert quote["current_price"] == 15.5 and quote["currency"] == "EUR"
    assert quote["symbol"] == "ENI.MI" and quote["timestamp"] == QUOTE_TIME
    assert "_yc_provider_outage" not in quote
    assert provider._fallback_circuit_breaker.failures == 0
    assert provider._circuit_breaker.failures == 0


def test_fallback_cooldown_probe_can_recover_without_raw_breaker_interference(
    provider, monkeypatch,
):
    clock = {"now": 100.0}
    monkeypatch.setattr(yc, "time", SimpleNamespace(monotonic=lambda: clock["now"]))

    async def http(url, *args):
        raise yc.YahooFetchError("http_404")

    def sync(symbol, *args):
        if symbol.startswith("FAIL"):
            raise yc.YahooFetchError("http_401")
        return ({"symbol": symbol, "currentPrice": 15.5, "currency": "EUR",
                 "regularMarketTime": QUOTE_EPOCH}, {}, [])

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    assert run(provider, "FAIL1") is None
    assert run(provider, "FAIL2") is None
    assert provider._fallback_circuit_breaker.state == "open"
    assert provider._circuit_breaker.state == "closed"
    clock["now"] += 61
    quote = run(provider, "ENI.MI")
    assert quote["current_price"] == 15.5 and quote["currency"] == "EUR"
    assert provider._fallback_circuit_breaker.state == "closed"
    assert provider._fallback_circuit_breaker.failures == 0
    assert provider._circuit_breaker.state == "closed"


def test_negative_cache_is_symbol_scoped_bounded_and_expires(provider, monkeypatch):
    clock = {"now": 100.0}
    monkeypatch.setattr(yc, "time", SimpleNamespace(monotonic=lambda: clock["now"]))
    calls = []

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        calls.append(symbol)
        if symbol == "MISSING":
            raise yc.YahooFetchError("http_404")
        return chart(symbol)

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    assert run(provider, "MISSING") is None
    assert run(provider, "MISSING") is None
    assert calls == ["MISSING", "MISSING"]
    assert run(provider, "VALID")["current_price"] == 123
    clock["now"] += 61  # min(quote TTL=120s, breaker cooldown=60s).
    assert run(provider, "MISSING") is None
    assert calls == ["MISSING", "MISSING", "VALID", "MISSING", "MISSING"]


def test_positive_cache_preserves_original_quote_time_across_loops(provider, monkeypatch):
    calls = []

    async def http(url, *args):
        calls.append(url)
        return chart("ENI.MI", price=15.5, currency="EUR")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    first = run(provider, "ENI.MI")
    second = run(provider, "ENI.MI")
    assert first == second
    assert len(calls) == 1
    assert second["current_price"] == 15.5 and second["currency"] == "EUR"
    assert second["timestamp"] == QUOTE_TIME
    assert first["last_updated_utc"] == second["last_updated_utc"]


def test_identity_crossing_remains_rejected_without_cached_foreign_quote(
    provider, monkeypatch,
):
    async def http(url, *args):
        return chart("OTHER.US", price=999, currency="USD")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    assert run(provider, "ENI.MI") is None
    assert asyncio.run(provider._cache.get("ENI.MI", kind="enriched")) is None
    assert provider._circuit_breaker.failures == 0


def test_real_outage_cooldown_probe_recovers_healthy_quotes(provider, monkeypatch):
    clock = {"now": 100.0}
    monkeypatch.setattr(yc, "time", SimpleNamespace(monotonic=lambda: clock["now"]))

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        if symbol.startswith("FAIL"):
            raise yc.YahooFetchError("http_429")
        return chart(symbol)

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    assert run(provider, "FAIL1") is None
    assert run(provider, "FAIL2") is None
    assert provider._circuit_breaker.state == "open"
    assert run(provider, "VALID") is None
    clock["now"] += 61
    assert run(provider, "VALID")["current_price"] == 123
    assert provider._circuit_breaker.state == "closed"
    assert provider._circuit_breaker.failures == 0


@pytest.mark.parametrize("stage", ["raw", "fallback"])
def test_owner_cancellation_keeps_breakers_cache_and_flight_bookkeeping_clean(
    provider, monkeypatch, stage,
):
    """Cancel a single owner; this makes no claim about preexisting waiters."""
    release_worker = threading.Event()

    async def scenario():
        started = asyncio.Event()
        never = asyncio.Event()
        loop = asyncio.get_running_loop()

        async def http(url, *args):
            if stage == "raw":
                started.set()
                await never.wait()
            raise yc.YahooFetchError("http_404")

        def sync(*args):
            loop.call_soon_threadsafe(started.set)
            if not release_worker.wait(timeout=5):
                raise AssertionError("cancelled executor worker was not released")
            return {}, {}, []

        monkeypatch.setattr(yc, "_raw_http_get_json", http)
        monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
        owner = asyncio.create_task(provider.get_enriched_quote("CANCELLED"))
        try:
            await asyncio.wait_for(started.wait(), timeout=2)
            owner.cancel()
            with pytest.raises(asyncio.CancelledError):
                await owner
            assert provider._circuit_breaker.failures == 0
            assert provider._fallback_circuit_breaker.failures == 0
            assert provider._circuit_breaker.state == "closed"
            assert provider._fallback_circuit_breaker.state == "closed"
            assert await provider._cache.size() == 0
            assert provider._single_flight.inflight() == 0
        finally:
            release_worker.set()
            if not owner.done():
                owner.cancel()

    asyncio.run(scenario())


def test_concurrent_local_misses_and_one_raw_outage_count_each_flight_once(
    provider, monkeypatch,
):
    raw_calls = []
    fallback_calls = []

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        raw_calls.append(symbol)
        await asyncio.sleep(0)  # Overlap real single-flight owners and waiters.
        raise yc.YahooFetchError("http_429" if symbol == "OUTAGE" else "http_404")

    def sync(symbol, *args):
        fallback_calls.append(symbol)
        return {}, {}, []

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    rows = asyncio.run(provider.get_enriched_quotes_batch(
        ["MISSING1", "OUTAGE", "MISSING1", "MISSING2", "OUTAGE"],
    ))
    assert rows == {}
    assert Counter(raw_calls) == {"MISSING1": 2, "MISSING2": 2, "OUTAGE": 2}
    assert Counter(fallback_calls) == {"MISSING1": 1, "MISSING2": 1, "OUTAGE": 1}
    assert provider._circuit_breaker.failures == 1
    assert provider._circuit_breaker.state == "closed"
    assert provider._fallback_circuit_breaker.failures == 0
    assert provider._single_flight.inflight() == 0


def test_denied_transports_do_not_extend_outage_with_symbol_negative_cache(
    provider, monkeypatch,
):
    clock = {"now": 100.0}
    monkeypatch.setattr(yc, "time", SimpleNamespace(monotonic=lambda: clock["now"]))
    calls = []

    async def http(url, *args):
        symbol = url.rsplit("/", 1)[-1]
        calls.append(symbol)
        if symbol.startswith("FAIL"):
            raise yc.YahooFetchError("http_429")
        return chart(symbol)

    def sync(*args):
        raise yc.YahooFetchError("http_401")

    monkeypatch.setattr(yc, "_raw_http_get_json", http)
    monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
    assert run(provider, "FAIL1") is None
    assert run(provider, "FAIL2") is None
    assert provider._circuit_breaker.state == "open"
    assert provider._fallback_circuit_breaker.state == "open"
    clock["now"] += 50
    assert run(provider, "NEW") is None
    assert asyncio.run(provider._cache.get("NEW", kind="quote_miss")) is None
    clock["now"] += 11
    assert run(provider, "NEW")["current_price"] == 123
    assert calls[-1] == "NEW"
