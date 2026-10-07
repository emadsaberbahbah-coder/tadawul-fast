"""Offline single-flight cancellation lifecycle and provider integration.

An owner's cancellation terminates its subscribers. Cancelling a subscriber
leaves the shared acquisition alive for its owner and remaining subscribers.
"""
from __future__ import annotations

import asyncio

import pytest

from core.providers import yahoo_chart_provider as yc


REAL_RAW_FETCH = yc._raw_chart_fetch_triple


async def clean_tasks(tasks):
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def test_owner_cancellation_wakes_all_waiters_and_allows_new_same_key_flight():
    async def scenario():
        flight = yc.SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def acquire():
            calls.append("owner")
            started.set()
            await release.wait()
            return "unreachable"

        owner = asyncio.create_task(flight.run("quote", acquire))
        await asyncio.wait_for(started.wait(), timeout=2)
        waiters = [asyncio.create_task(flight.run("quote", acquire)) for _ in range(2)]
        tasks = [owner, *waiters]
        try:
            await asyncio.sleep(0)  # Ensure both subscribers await the shared flight.
            assert calls == ["owner"]
            owner.cancel()
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=1,
            )
            assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
            assert flight.inflight() == 0

            async def reacquire():
                return "new acquisition"

            assert await flight.run("quote", reacquire) == "new acquisition"
            assert flight.inflight() == 0
        finally:
            release.set()
            await clean_tasks(tasks)

    asyncio.run(scenario())


@pytest.mark.parametrize("owner_fails", [False, True])
def test_waiter_cancellation_leaves_owner_and_other_waiter_alive(owner_fails):
    async def scenario():
        flight = yc.SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []
        error = RuntimeError("owner acquisition failed")

        async def acquire():
            calls.append("owner")
            started.set()
            await release.wait()
            if owner_fails:
                raise error
            return "verified quote"

        owner = asyncio.create_task(flight.run("quote", acquire))
        await asyncio.wait_for(started.wait(), timeout=2)
        cancelled = asyncio.create_task(flight.run("quote", acquire))
        survivor = asyncio.create_task(flight.run("quote", acquire))
        tasks = [owner, cancelled, survivor]
        try:
            await asyncio.sleep(0)
            cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):
                await cancelled
            assert not owner.done()
            assert not survivor.done()
            assert not flight._futures["quote"].cancelled()
            assert calls == ["owner"]
            release.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(owner, survivor, return_exceptions=True), timeout=1,
            )
            if owner_fails:
                assert outcomes == [error, error]
            else:
                assert outcomes == ["verified quote", "verified quote"]
            assert flight.inflight() == 0
        finally:
            release.set()
            await clean_tasks(tasks)

    asyncio.run(scenario())


def test_owner_failure_reaches_all_waiters_and_clears_flight():
    async def scenario():
        flight = yc.SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []
        error = RuntimeError("quote transport failed")

        async def acquire():
            calls.append("owner")
            started.set()
            await release.wait()
            raise error

        owner = asyncio.create_task(flight.run("quote", acquire))
        await asyncio.wait_for(started.wait(), timeout=2)
        waiters = [asyncio.create_task(flight.run("quote", acquire)) for _ in range(2)]
        tasks = [owner, *waiters]
        try:
            await asyncio.sleep(0)
            release.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=1,
            )
            assert outcomes == [error, error, error]
            assert calls == ["owner"]
            assert flight.inflight() == 0
        finally:
            release.set()
            await clean_tasks(tasks)

    asyncio.run(scenario())


def test_cancelled_quote_owner_terminates_subscribers_without_cache_or_breaker_changes(
    monkeypatch,
):
    monkeypatch.setattr(yc, "_HAS_HTTPX", True)
    monkeypatch.setattr(yc, "_raw_chart_enabled", lambda: True)
    monkeypatch.setattr(yc, "_raw_chart_fetch_triple", REAL_RAW_FETCH)
    monkeypatch.delenv("TFB_YC_SYMBOL_SKIP", raising=False)
    provider = yc.YahooChartProvider(yc.YahooConfig(
        enabled=True, rate_limit_per_sec=0, threadpool_workers=2,
    ))

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def http(*args):
            calls.append("raw acquisition")
            started.set()
            await release.wait()
            raise AssertionError("cancelled owner must not continue to a payload")

        def sync(*args):
            raise AssertionError("cancelled owner must not invoke fallback")

        monkeypatch.setattr(yc, "_raw_http_get_json", http)
        monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
        owner = asyncio.create_task(provider.get_enriched_quote("KE=F"))
        await asyncio.wait_for(started.wait(), timeout=2)
        waiters = [asyncio.create_task(provider.get_enriched_quote("KE=F")) for _ in range(2)]
        tasks = [owner, *waiters]
        try:
            await asyncio.sleep(0)
            owner.cancel()
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=1,
            )
            assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
            assert calls == ["raw acquisition"]
            assert await provider._cache.size() == 0
            assert provider._circuit_breaker.failures == 0
            assert provider._fallback_circuit_breaker.failures == 0
            assert provider._single_flight.inflight() == 0
        finally:
            release.set()
            await clean_tasks(tasks)

    try:
        asyncio.run(scenario())
    finally:
        provider._executor.shutdown(wait=True)


def test_batch_propagates_shared_owner_cancellation_after_other_quote_completes(monkeypatch):
    monkeypatch.setattr(yc, "_HAS_HTTPX", True)
    monkeypatch.setattr(yc, "_raw_chart_enabled", lambda: True)
    monkeypatch.setattr(yc, "_raw_chart_fetch_triple", REAL_RAW_FETCH)
    monkeypatch.delenv("TFB_YC_SYMBOL_SKIP", raising=False)
    provider = yc.YahooChartProvider(yc.YahooConfig(
        enabled=True, rate_limit_per_sec=0, threadpool_workers=2,
    ))

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def http(url, *args):
            symbol = url.rsplit("/", 1)[-1]
            calls.append(symbol)
            if symbol == "KE=F":
                started.set()
                await release.wait()
                raise AssertionError("cancelled owner must not publish a quote")
            return {"chart": {"error": None, "result": [{
                "meta": {"symbol": symbol, "regularMarketPrice": 123,
                         "currency": "USD", "regularMarketTime": 1791374400},
                "timestamp": [], "indicators": {"quote": [{}]},
            }]}}

        def sync(*args):
            raise AssertionError("batch cancellation must not retry through fallback")

        monkeypatch.setattr(yc, "_raw_http_get_json", http)
        monkeypatch.setattr(yc, "_fetch_ticker_sync", sync)
        owner = asyncio.create_task(provider.get_enriched_quote("KE=F"))
        await asyncio.wait_for(started.wait(), timeout=2)
        batch = asyncio.create_task(provider.get_enriched_quotes_batch(["KE=F", "VALID=F"]))
        tasks = [owner, batch]
        try:
            await asyncio.sleep(0)
            await asyncio.sleep(0)  # Batch children subscribe before owner cancellation.
            owner.cancel()
            with pytest.raises(asyncio.CancelledError):
                await owner
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(batch, timeout=1)
            healthy = await provider._cache.get("VALID=F", kind="enriched")
            assert healthy["current_price"] == 123
            assert healthy["symbol"] == "VALID=F" and healthy["currency"] == "USD"
            assert calls.count("KE=F") == 1 and calls.count("VALID=F") == 1
            assert provider._circuit_breaker.failures == 0
            assert provider._fallback_circuit_breaker.failures == 0
            assert await provider._cache.get("KE=F", kind="enriched") is None
            assert await provider._cache.get("KE=F", kind="quote_miss") is None
            assert provider._single_flight.inflight() == 0
        finally:
            release.set()
            await clean_tasks(tasks)

    try:
        asyncio.run(scenario())
    finally:
        provider._executor.shutdown(wait=True)
