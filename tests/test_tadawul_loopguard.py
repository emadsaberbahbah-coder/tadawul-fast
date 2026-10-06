"""Actual httpx client lifecycle tests using offline, loop-owned transports."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from core.providers import tadawul_provider as tadawul


class _Transport(httpx.AsyncBaseTransport):
    def __init__(self, handler=None):
        self.owner = None
        self.active = 0
        self.max_active = 0
        self.calls = 0
        self.closed = False
        self.handler = handler

    async def handle_async_request(self, request):
        loop = asyncio.get_running_loop()
        if self.owner is None:
            self.owner = loop
        assert self.owner is loop, "HTTP transport reused by a foreign loop"
        assert not self.closed
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.calls += 1
        try:
            if self.handler is not None:
                await self.handler(request)
            else:
                await asyncio.sleep(0)
            return httpx.Response(200, json={"price": 10.0, "previous_close": 9.0})
        finally:
            self.active -= 1

    async def aclose(self):
        assert self.active == 0, "transport closed with a live request"
        if self.owner is not None:
            assert self.owner is asyncio.get_running_loop(), "closed on foreign loop"
        self.closed = True


def _install_http(monkeypatch, handler=None):
    clients = []
    transports = []
    lock = threading.Lock()

    def build(client):
        transport = _Transport(handler)
        http_client = httpx.AsyncClient(
            transport=transport, headers=client._headers,
            timeout=client.config.timeout_sec, follow_redirects=True,
        )
        with lock:
            clients.append(http_client)
            transports.append(transport)
        return http_client

    monkeypatch.setattr(tadawul.TadawulClient, "_new_http_client", build)
    monkeypatch.setattr(tadawul, "_HTTP_GRAVEYARD", [])
    return clients, transports


def _config():
    return tadawul.TadawulConfig(
        quote_url="https://offline.invalid/{code}", rate_limit=0,
        max_concurrency=1, retry_attempts=1,
        enable_profile=False, enable_fundamentals=False, enable_history=False,
    )


def test_direct_client_rebuilds_http_and_semaphore_after_closed_loop(monkeypatch):
    http_clients, transports = _install_http(monkeypatch)
    client = tadawul.TadawulClient(_config())

    async def run(close=False):
        results = await asyncio.gather(
            client._request("https://offline.invalid/one"),
            client._request("https://offline.invalid/two"),
        )
        assert all(error is None and data["price"] == 10 for data, error in results)
        semaphore, http_client = client._get_loop_resources()
        if close:
            await client.close()
        return semaphore, http_client

    first = asyncio.run(run())
    second = asyncio.run(run(close=True))
    assert first[0] is not second[0]
    assert first[1] is not second[1]
    assert len(http_clients) == 2
    assert all(t.calls == 2 and t.max_active == 1 for t in transports)
    assert not first[1].is_closed and second[1].is_closed
    assert tadawul._HTTP_GRAVEYARD == [first[1]]


def test_simultaneous_loops_and_close_preserve_active_peer(monkeypatch):
    requests_active = threading.Barrier(2)
    peer_closed = threading.Event()

    async def handle(request):
        if request.url.path == "/one":
            requests_active.wait(timeout=2)
            while not peer_closed.is_set():
                await asyncio.sleep(0)
        else:
            requests_active.wait(timeout=2)

    http_clients, transports = _install_http(monkeypatch, handle)
    client = tadawul.TadawulClient(_config())

    def drive(path):
        async def run():
            result = await asyncio.wait_for(
                client._request("https://offline.invalid/" + path), 3,
            )
            _, http_client = client._get_loop_resources()
            assert result[1] is None
            await client.close()
            if path == "two":
                peer_closed.set()
            return http_client
        return asyncio.run(run())

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(drive, path) for path in ("one", "two")]
        results = [future.result(timeout=5) for future in futures]
    assert len(http_clients) == 2 and results[0] is not results[1]
    assert all(t.closed and t.calls == 1 for t in transports)
    assert client._loop_resources == {}


def test_singleton_keeps_shared_cache_and_quota_across_loops(monkeypatch):
    _install_http(monkeypatch)
    monkeypatch.setattr(tadawul, "_CLIENT_INSTANCE", None)
    monkeypatch.setattr(tadawul.TadawulConfig, "from_env", classmethod(lambda cls: _config()))
    instances = []

    async def first():
        client = await tadawul.get_client()
        instances.append(client)
        await client._quote_cache.set("saved", {"price": 7.0})
        await client._request("https://offline.invalid/first")

    async def second():
        client = await tadawul.get_client()
        assert client is instances[0]
        assert await client._quote_cache.get("saved") == {"price": 7.0}
        assert client._rate_limiter is instances[0]._rate_limiter
        await client._request("https://offline.invalid/second")
        await tadawul.close_client()
        assert tadawul._CLIENT_INSTANCE is None

    asyncio.run(first())
    asyncio.run(second())


def test_singleflight_follower_and_owner_cancellation_cleanup():
    async def run():
        flight = tadawul.SingleFlight()
        started, release = asyncio.Event(), asyncio.Event()

        async def work():
            started.set()
            await release.wait()
            return "shared"

        owner = asyncio.create_task(flight.run("key", work))
        await asyncio.wait_for(started.wait(), 1)
        cancelled = asyncio.create_task(flight.run("key", work))
        survivor = asyncio.create_task(flight.run("key", work))
        await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        release.set()
        assert await asyncio.wait_for(owner, 1) == "shared"
        assert await asyncio.wait_for(survivor, 1) == "shared"
        assert flight._futures == {}

        started.clear()
        release.clear()
        owner = asyncio.create_task(flight.run("key", work))
        await asyncio.wait_for(started.wait(), 1)
        follower = asyncio.create_task(flight.run("key", work))
        await asyncio.sleep(0)
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(follower, 1)
        assert flight._futures == {}

    asyncio.run(run())


def test_singleflight_same_key_gets_independent_owners_per_live_loop():
    flight = tadawul.SingleFlight()
    owners_ready = threading.Barrier(2)

    def drive(label):
        async def work():
            owners_ready.wait(timeout=2)
            await asyncio.sleep(0)
            return label
        return asyncio.run(flight.run("same", work))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(drive, label) for label in ("A", "B")]
        assert sorted(f.result(timeout=5) for f in futures) == ["A", "B"]
    assert flight._futures == {}


def test_owner_only_failure_is_observed_and_key_can_retry():
    async def run():
        flight = tadawul.SingleFlight()
        errors = []
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: errors.append(context))

        async def broken():
            raise RuntimeError("offline failure")

        with pytest.raises(RuntimeError, match="offline failure"):
            await flight.run("key", broken)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert flight._futures == {} and errors == []

        async def recovered():
            return "recovered"

        assert await flight.run("key", recovered) == "recovered"

    asyncio.run(run())


def test_cancelled_request_releases_semaphore_for_next_request(monkeypatch):
    started, release = None, None

    async def handler(request):
        if request.url.path == "/cancel":
            started.set()
            await release.wait()

    _install_http(monkeypatch, handler)
    client = tadawul.TadawulClient(_config())

    async def run():
        nonlocal started, release
        started, release = asyncio.Event(), asyncio.Event()
        cancelled = asyncio.create_task(client._request("https://offline.invalid/cancel"))
        await asyncio.wait_for(started.wait(), 1)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        result = await asyncio.wait_for(client._request("https://offline.invalid/next"), 1)
        assert result[1] is None
        await client.close()

    asyncio.run(run())
