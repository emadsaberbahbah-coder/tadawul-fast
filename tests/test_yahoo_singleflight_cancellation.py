"""Zero-network cancellation regressions for the Yahoo provider single flights."""

from __future__ import annotations

import asyncio
from typing import Any, Type

import pytest

from core.providers import yahoo_chart_provider
from core.providers import yahoo_fundamentals_provider


_IMPLEMENTATIONS = [
    pytest.param(yahoo_chart_provider.SingleFlight, id="chart"),
    pytest.param(yahoo_fundamentals_provider.SingleFlight, id="fundamentals"),
]


@pytest.mark.parametrize("singleflight_cls", _IMPLEMENTATIONS)
def test_cancelled_follower_does_not_cancel_shared_flight(
    singleflight_cls: Type[Any],
) -> None:
    async def scenario() -> None:
        singleflight = singleflight_cls()
        owner_started = asyncio.Event()
        release_owner = asyncio.Event()
        calls = 0

        async def owner_work() -> str:
            nonlocal calls
            calls += 1
            owner_started.set()
            await release_owner.wait()
            return "shared-result"

        async def follower_must_not_run() -> str:
            raise AssertionError("a follower became a second owner")

        owner = asyncio.create_task(singleflight.run("same-key", owner_work))
        await asyncio.wait_for(owner_started.wait(), timeout=1.0)

        cancelled_follower = asyncio.create_task(
            singleflight.run("same-key", follower_must_not_run)
        )
        surviving_follower = asyncio.create_task(
            singleflight.run("same-key", follower_must_not_run)
        )
        # Both tasks now run until they are waiting on the existing flight.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        cancelled_follower.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_follower

        release_owner.set()
        assert await asyncio.wait_for(owner, timeout=1.0) == "shared-result"
        assert (
            await asyncio.wait_for(surviving_follower, timeout=1.0)
            == "shared-result"
        )
        assert calls == 1
        assert singleflight.inflight() == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("singleflight_cls", _IMPLEMENTATIONS)
def test_cancelled_owner_wakes_followers_and_cleans_flight(
    singleflight_cls: Type[Any],
) -> None:
    async def scenario() -> None:
        singleflight = singleflight_cls()
        owner_started = asyncio.Event()
        hold_owner = asyncio.Event()

        async def owner_work() -> str:
            owner_started.set()
            await hold_owner.wait()
            return "unreachable"

        async def follower_must_not_run() -> str:
            raise AssertionError("a follower became a second owner")

        owner = asyncio.create_task(singleflight.run("same-key", owner_work))
        await asyncio.wait_for(owner_started.wait(), timeout=1.0)
        follower = asyncio.create_task(
            singleflight.run("same-key", follower_must_not_run)
        )
        # Ensure the follower has joined before cancelling the owner.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(follower, timeout=1.0)

        assert singleflight.inflight() == 0
        assert (
            await asyncio.wait_for(
                singleflight.run("same-key", lambda: _return("replacement")),
                timeout=1.0,
            )
            == "replacement"
        )
        assert singleflight.inflight() == 0

    asyncio.run(scenario())


async def _return(value: str) -> str:
    return value


def test_provider_patch_versions() -> None:
    assert yahoo_chart_provider.PROVIDER_VERSION == "8.15.2"
    assert yahoo_chart_provider.VERSION == "8.15.2"
    assert yahoo_fundamentals_provider.PROVIDER_VERSION == "6.9.1"
    assert yahoo_fundamentals_provider.VERSION == "6.9.1"
