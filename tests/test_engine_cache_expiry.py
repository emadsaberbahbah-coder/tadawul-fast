"""Quote-cache lifetime and retention tests without provider/network access."""
import asyncio
from unittest.mock import patch

import core.data_engine_v2 as de


def test_new_quote_releases_expired_payloads_that_are_never_read_again():
    async def scenario():
        with patch.object(de.time, "time", return_value=100) as clock:
            cache = de.MultiLevelCache(ttl_seconds=30)
            for i in range(1500):
                await cache.set(str(i), {"symbol": str(i), "history": [1] * 100})
            assert cache.size() == 1500
            clock.return_value = 131
            await cache.set("new", {"price": 20})
            assert cache.size() == 1
            assert await cache.get("new") == {"price": 20}
    asyncio.run(scenario())


def test_live_quote_and_exact_ttl_boundary_remain_available():
    async def scenario():
        with patch.object(de.time, "time", return_value=100) as clock:
            cache = de.MultiLevelCache(ttl_seconds=30)
            await cache.set("old", 10)
            clock.return_value = 120
            await cache.set("live", 20)
            clock.return_value = 130
            assert await cache.get("old") == 10
            assert await cache.get("live") == 20
            clock.return_value = 131
            assert await cache.get("old") is None
            assert await cache.get("live") == 20
    asyncio.run(scenario())


def test_expiration_does_not_assume_lru_order_is_timestamp_order():
    async def scenario():
        with patch.object(de.time, "time", return_value=100) as clock:
            cache = de.MultiLevelCache(ttl_seconds=30)
            await cache.set("old", 10)
            clock.return_value = 120
            await cache.set("live", 20)
            clock.return_value = 129
            assert await cache.get("old") == 10  # moves old behind live
            clock.return_value = 131
            await cache.set("new", 30)
            assert cache.size() == 2
            assert await cache.get("old") is None
            assert await cache.get("live") == 20
    asyncio.run(scenario())


def test_replacing_quote_refreshes_only_its_own_ttl():
    async def scenario():
        with patch.object(de.time, "time", return_value=100) as clock:
            cache = de.MultiLevelCache(ttl_seconds=30)
            await cache.set("replaced", 10)
            await cache.set("old", 11)
            clock.return_value = 120
            await cache.set("replaced", 20)
            clock.return_value = 131
            await cache.set("trigger", 30)
            assert cache.size() == 2
            assert await cache.get("replaced") == 20
            assert await cache.get("old") is None
    asyncio.run(scenario())


def test_concurrent_expiry_and_writes_preserve_fresh_payloads():
    async def scenario():
        with patch.object(de.time, "time", return_value=100) as clock:
            cache = de.MultiLevelCache(ttl_seconds=30)
            await asyncio.gather(*(cache.set(f"old{i}", i) for i in range(1500)))
            clock.return_value = 131
            await asyncio.gather(*(cache.set(f"new{i}", i) for i in range(1500)))
            values = await asyncio.gather(*(cache.get(f"new{i}") for i in range(1500)))
            assert values == list(range(1500))
            assert cache.size() == 1500
            assert await cache.get("old0") is None
    asyncio.run(scenario())
