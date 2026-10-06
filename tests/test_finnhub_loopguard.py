"""Zero-network loop, cancellation and engine-callable regressions for Finnhub."""

from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Tuple

import pytest

from core.providers import finnhub_provider as finnhub


class _FakeResponse:
    status_code = 200
    headers: Dict[str, str] = {}
    content = json.dumps(
        {"c": 101.0, "pc": 100.0, "d": 1.0, "dp": 1.0}
    ).encode("utf-8")


class _FakeAsyncClient:
    """Loop-neutral test double; each instance records its own concurrency."""

    instances: List["_FakeAsyncClient"] = []
    instances_lock = threading.Lock()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        self.active = 0
        self.max_active = 0
        self.calls = 0
        self.closed = False
        self._lock = threading.Lock()
        with self.instances_lock:
            self.instances.append(self)

    async def get(self, url: str, params: Dict[str, Any]) -> _FakeResponse:
        del url, params
        with self._lock:
            if self.closed:
                raise AssertionError("request used a closed loop transport")
            self.active += 1
            self.calls += 1
            self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            return _FakeResponse()
        finally:
            with self._lock:
                self.active -= 1

    async def aclose(self) -> None:
        with self._lock:
            if self.active:
                raise AssertionError("transport closed with a request in flight")
            self.closed = True


def _install_fake_http(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeAsyncClient.instances = []
    finnhub._HTTP_GRAVEYARD.clear()
    monkeypatch.setattr(finnhub.httpx, "AsyncClient", _FakeAsyncClient)


def _test_config(**overrides: Any) -> finnhub.FinnhubConfig:
    values: Dict[str, Any] = {
        "api_key": "test-key",
        "retry_attempts": 0,
        "max_concurrency": 1,
        "rate_limit_rps": 0.0,
        "enable_profile": False,
        "enable_metric": False,
        "enable_history": False,
    }
    values.update(overrides)
    return finnhub.FinnhubConfig(**values)


def test_direct_client_keeps_transport_and_cap_per_simultaneous_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_http(monkeypatch)
    client = finnhub.FinnhubClient(_test_config())
    loops_ready = threading.Barrier(2)

    def drive(label: str) -> Tuple[List[Any], int, _FakeAsyncClient]:
        async def run() -> Tuple[List[Any], int, _FakeAsyncClient]:
            loops_ready.wait(timeout=2.0)
            results = await asyncio.gather(
                client._request_json("quote", {"symbol": f"{label}1"}),
                client._request_json("quote", {"symbol": f"{label}2"}),
            )
            semaphore, transport = client._get_loop_resources()
            return results, id(semaphore), transport

        return asyncio.run(run())

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(drive, label) for label in ("A", "B")]
        outcomes = [future.result(timeout=5.0) for future in futures]

    transports = [outcome[2] for outcome in outcomes]
    assert transports[0] is not transports[1]
    assert outcomes[0][1] != outcomes[1][1]
    assert all(err is None for outcome in outcomes for _data, err in outcome[0])
    assert all(transport.calls == 2 for transport in transports)
    assert all(transport.max_active == 1 for transport in transports)
    assert all(not transport.closed for transport in transports)

    # A later loop prunes only the now-closed owners, gets a fresh transport,
    # and can close its own resource without touching the earlier two.
    async def third_loop() -> _FakeAsyncClient:
        data, err = await client._request_json("quote", {"symbol": "C1"})
        assert data and err is None
        _semaphore, transport = client._get_loop_resources()
        await client.close()
        return transport

    third_transport = asyncio.run(third_loop())
    assert third_transport not in transports
    assert third_transport.closed
    assert all(not transport.closed for transport in transports)
    assert len(finnhub._HTTP_GRAVEYARD) == 2
    finnhub._HTTP_GRAVEYARD.clear()


def test_module_factory_is_per_live_loop_and_never_retires_active_peer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_http(monkeypatch)
    monkeypatch.setattr(finnhub, "_CLIENTS", {})
    monkeypatch.setattr(finnhub, "_CLIENT_INSTANCE", None)
    monkeypatch.setattr(finnhub, "_CLIENT_LOCK", threading.Lock())
    loops_ready = threading.Barrier(2)

    def drive() -> Tuple[finnhub.FinnhubClient, _FakeAsyncClient]:
        async def run() -> Tuple[finnhub.FinnhubClient, _FakeAsyncClient]:
            loops_ready.wait(timeout=2.0)
            client = await finnhub.get_client()
            same_client = await finnhub.get_client()
            assert same_client is client
            _semaphore, transport = client._get_loop_resources()
            await asyncio.sleep(0.01)
            return client, transport

        return asyncio.run(run())

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(drive) for _ in range(2)]
        outcomes = [future.result(timeout=5.0) for future in futures]

    assert outcomes[0][0] is not outcomes[1][0]
    assert outcomes[0][1] is not outcomes[1][1]
    assert all(not transport.closed for _client, transport in outcomes)

    async def prune_and_close_current() -> None:
        current = await finnhub.get_client()
        current._get_loop_resources()
        await finnhub.close_client()

    asyncio.run(prune_and_close_current())
    assert finnhub._CLIENTS == {}
    assert all(not transport.closed for _client, transport in outcomes)
    assert len(finnhub._HTTP_GRAVEYARD) == 2
    finnhub._HTTP_GRAVEYARD.clear()


def test_singleflight_waiter_and_owner_cancellation_are_bounded() -> None:
    async def scenario() -> None:
        singleflight = finnhub.SingleFlight()
        owner_started = asyncio.Event()
        release_owner = asyncio.Event()
        calls = 0

        async def work() -> str:
            nonlocal calls
            calls += 1
            owner_started.set()
            await release_owner.wait()
            return "shared"

        async def must_not_run() -> str:
            raise AssertionError("follower became an owner")

        owner = asyncio.create_task(singleflight.do("waiter", work))
        await asyncio.wait_for(owner_started.wait(), timeout=1.0)
        cancelled_follower = asyncio.create_task(
            singleflight.do("waiter", must_not_run)
        )
        surviving_follower = asyncio.create_task(
            singleflight.do("waiter", must_not_run)
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        cancelled_follower.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_follower
        release_owner.set()
        assert await asyncio.wait_for(owner, timeout=1.0) == "shared"
        assert await asyncio.wait_for(surviving_follower, timeout=1.0) == "shared"
        assert calls == 1

        owner_started.clear()
        release_owner.clear()
        cancelled_owner = asyncio.create_task(singleflight.do("owner", work))
        await asyncio.wait_for(owner_started.wait(), timeout=1.0)
        follower = asyncio.create_task(singleflight.do("owner", must_not_run))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        cancelled_owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_owner
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(follower, timeout=1.0)
        assert singleflight.inflight() == 0

        async def replacement() -> str:
            return "replacement"

        assert await singleflight.do("owner", replacement) == "replacement"

    asyncio.run(scenario())


def test_singleflight_same_key_is_independent_across_simultaneous_loops() -> None:
    singleflight = finnhub.SingleFlight()
    owners_ready = threading.Barrier(2)
    calls = 0
    calls_lock = threading.Lock()

    def drive(label: str) -> str:
        async def work() -> str:
            nonlocal calls
            with calls_lock:
                calls += 1
            # Both loops must become owners. A foreign-loop shared Future
            # would leave only one owner here and break the barrier.
            owners_ready.wait(timeout=2.0)
            await asyncio.sleep(0)
            return label

        return asyncio.run(singleflight.do("same-key", work))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(drive, label) for label in ("A", "B")]
        results = [future.result(timeout=5.0) for future in futures]

    assert sorted(results) == ["A", "B"]
    assert calls == 2
    assert singleflight.inflight() == 0


def test_engine_callable_aliases_delegate_to_enriched_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.data_engine_v2 import _pick_provider_callable

    selected = _pick_provider_callable(
        finnhub,
        "get_quote_async",
        "fetch_quote_async",
        "get_quote",
        "fetch_quote",
        "quote_async",
        "quote",
        "get_unified_quote",
        "fetch",
    )
    assert selected is finnhub.get_quote

    calls: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []

    async def enriched(
        symbol: str,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        calls.append((symbol, args, kwargs))
        return {"symbol": symbol, "data_quality": "OK", "sentinel": True}

    monkeypatch.setattr(finnhub, "fetch_enriched_quote_patch", enriched)

    async def scenario() -> List[Dict[str, Any]]:
        return [
            await finnhub.get_quote("AAPL", "ignored", mode="x"),
            await finnhub.fetch_quote("MSFT", source="engine"),
        ]

    outputs = asyncio.run(scenario())
    assert [output["symbol"] for output in outputs] == ["AAPL", "MSFT"]
    assert all(output["sentinel"] for output in outputs)
    assert calls == [
        ("AAPL", ("ignored",), {"mode": "x"}),
        ("MSFT", (), {"source": "engine"}),
    ]
    assert "get_quote" in finnhub.__all__
    assert "fetch_quote" in finnhub.__all__
    assert finnhub.PROVIDER_VERSION == "6.2.0"
    assert finnhub.VERSION == "6.2.0"


def test_default_ksa_guard_and_disabled_error_shape_are_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_http(monkeypatch)

    async def scenario() -> Tuple[Dict[str, Any], Dict[str, Any]]:
        enabled = finnhub.FinnhubClient(_test_config())
        disabled = finnhub.FinnhubClient(_test_config(api_key=""))
        try:
            ksa = await enabled.fetch_enriched_quote_patch("2222.SR")
            unavailable = await disabled.fetch_enriched_quote_patch("AAPL")
            return ksa, unavailable
        finally:
            await enabled.close()
            await disabled.close()

    ksa, unavailable = asyncio.run(scenario())
    assert ksa["provider"] == "finnhub"
    assert ksa["data_quality"] == "BLOCKED"
    assert ksa["error"] == "ksa_blocked"
    assert unavailable["provider"] == "finnhub"
    assert unavailable["data_quality"] == "DISABLED"
    assert unavailable["error"] == "provider_disabled_or_missing_key"
