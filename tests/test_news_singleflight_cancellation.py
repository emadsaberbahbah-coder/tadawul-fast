"""Offline cancellation and lifecycle regressions for the news shared task."""
from __future__ import annotations

import asyncio
import gc
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from core.news_intelligence import SingleFlight


async def finish(tasks):
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def test_first_caller_cancellation_leaves_followers_and_acquisition_alive():
    async def scenario():
        flight = SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def acquire():
            calls.append("news acquisition")
            started.set()
            await release.wait()
            return "shared news"

        first = asyncio.create_task(flight.execute("ACME", acquire))
        await asyncio.wait_for(started.wait(), timeout=1)
        followers = [asyncio.create_task(flight.execute("ACME", acquire)) for _ in range(2)]
        tasks = [first, *followers]
        try:
            await asyncio.sleep(0)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            release.set()
            outcomes = await asyncio.wait_for(asyncio.gather(*followers), timeout=0.2)
            assert outcomes == ["shared news", "shared news"]
            assert calls == ["news acquisition"]

            async def retry():
                return "fresh news"

            assert await flight.execute("ACME", retry) == "fresh news"
        finally:
            release.set()
            await finish(tasks)

    asyncio.run(scenario())


def test_follower_cancellation_leaves_first_caller_and_other_follower_alive():
    async def scenario():
        flight = SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def acquire():
            calls.append("news acquisition")
            started.set()
            await release.wait()
            return "shared news"

        first = asyncio.create_task(flight.execute("ACME", acquire))
        await asyncio.wait_for(started.wait(), timeout=1)
        cancelled = asyncio.create_task(flight.execute("ACME", acquire))
        survivor = asyncio.create_task(flight.execute("ACME", acquire))
        tasks = [first, cancelled, survivor]
        try:
            await asyncio.sleep(0)
            cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):
                await cancelled
            release.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(first, survivor, return_exceptions=True), timeout=0.2,
            )
            assert outcomes == ["shared news", "shared news"]
            assert calls == ["news acquisition"]
        finally:
            release.set()
            await finish(tasks)

    asyncio.run(scenario())


def test_acquisition_exception_reaches_every_caller_and_same_key_retries():
    async def scenario():
        flight = SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()
        error = RuntimeError("news acquisition failed")
        calls = []

        async def acquire():
            calls.append("attempt")
            started.set()
            await release.wait()
            raise error

        first = asyncio.create_task(flight.execute("ACME", acquire))
        await asyncio.wait_for(started.wait(), timeout=1)
        followers = [asyncio.create_task(flight.execute("ACME", acquire)) for _ in range(2)]
        tasks = [first, *followers]
        try:
            await asyncio.sleep(0)
            release.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=1,
            )
            assert outcomes == [error, error, error]
            assert calls == ["attempt"]
            assert flight.inflight() == 0

            async def retry():
                return "recovered news"

            assert await flight.execute("ACME", retry) == "recovered news"
        finally:
            release.set()
            await finish(tasks)

    asyncio.run(scenario())


@pytest.mark.parametrize("timed_out", [False, True])
def test_failure_is_observed_after_every_subscriber_cancels(timed_out):
    async def scenario():
        flight = SingleFlight(timeout_seconds=0.03 if timed_out else 1)
        started = asyncio.Event()
        release = asyncio.Event()
        finalized = asyncio.Event()
        contexts = []
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: contexts.append(context))

        async def acquire():
            started.set()
            try:
                await release.wait()
                raise RuntimeError("failure after callers left")
            finally:
                finalized.set()

        first = asyncio.create_task(flight.execute("ACME", acquire))
        await asyncio.wait_for(started.wait(), timeout=1)
        follower = asyncio.create_task(flight.execute("ACME", acquire))
        tasks = [first, follower]
        try:
            await asyncio.sleep(0)
            await finish(tasks)
            assert flight.inflight() == 1
            if not timed_out:
                release.set()
            await asyncio.wait_for(finalized.wait(), timeout=1)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            gc.collect()
            assert flight.inflight() == 0
            assert contexts == []
        finally:
            release.set()
            await flight.aclose()
            loop.set_exception_handler(None)

    asyncio.run(scenario())


def test_shared_timeout_wakes_all_callers_and_retry_succeeds():
    async def scenario():
        flight = SingleFlight(timeout_seconds=0.03)
        started = asyncio.Event()
        finalized = asyncio.Event()
        calls = []

        async def acquire():
            calls.append("attempt")
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finalized.set()

        first = asyncio.create_task(flight.execute("ACME", acquire))
        await asyncio.wait_for(started.wait(), timeout=1)
        followers = [asyncio.create_task(flight.execute("ACME", acquire)) for _ in range(2)]
        tasks = [first, *followers]
        try:
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=1,
            )
            assert all(isinstance(outcome, TimeoutError) for outcome in outcomes)
            assert calls == ["attempt"]
            assert finalized.is_set()
            assert flight.inflight() == 0

            async def retry():
                return "fresh news"

            assert await flight.execute("ACME", retry) == "fresh news"
        finally:
            await finish(tasks)
            await flight.aclose()

    asyncio.run(scenario())


def test_factory_exception_is_owned_and_does_not_leave_a_stale_flight():
    async def scenario():
        flight = SingleFlight()
        contexts = []
        asyncio.get_running_loop().set_exception_handler(
            lambda _loop, context: contexts.append(context),
        )

        def broken_factory():
            raise RuntimeError("factory rejected acquisition")

        with pytest.raises(RuntimeError, match="factory rejected acquisition"):
            await flight.execute("ACME", broken_factory)
        assert flight.inflight() == 0
        gc.collect()
        assert contexts == []

        async def retry():
            return "healthy"

        assert await flight.execute("ACME", retry) == "healthy"

    asyncio.run(scenario())


def test_producer_cancellation_wakes_every_caller_and_allows_retry():
    async def scenario():
        flight = SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()

        async def acquire():
            started.set()
            await release.wait()
            raise asyncio.CancelledError

        first = asyncio.create_task(flight.execute("ACME", acquire))
        await asyncio.wait_for(started.wait(), timeout=1)
        follower = asyncio.create_task(flight.execute("ACME", acquire))
        tasks = [first, follower]
        try:
            await asyncio.sleep(0)
            release.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=1,
            )
            assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
            assert flight.inflight() == 0

            async def retry():
                return "healthy"

            assert await flight.execute("ACME", retry) == "healthy"
        finally:
            release.set()
            await finish(tasks)

    asyncio.run(scenario())


def test_different_keys_run_concurrently_and_one_caller_timeout_is_isolated():
    async def scenario():
        flight = SingleFlight(timeout_seconds=1)
        started = {key: asyncio.Event() for key in ("ACME", "OTHER")}
        release = asyncio.Event()
        calls = []

        async def acquire(key):
            calls.append(key)
            started[key].set()
            await release.wait()
            return key

        first = asyncio.create_task(flight.execute("ACME", lambda: acquire("ACME")))
        await asyncio.wait_for(started["ACME"].wait(), timeout=1)
        follower = asyncio.create_task(flight.execute("ACME", lambda: acquire("ACME")))
        other = asyncio.create_task(flight.execute("OTHER", lambda: acquire("OTHER")))
        tasks = [first, follower, other]
        try:
            await asyncio.wait_for(started["OTHER"].wait(), timeout=0.2)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(follower, timeout=0.01)
            assert not first.done() and not other.done()
            assert flight.inflight() == 2
            release.set()
            assert await asyncio.wait_for(asyncio.gather(first, other), timeout=1) == ["ACME", "OTHER"]
            assert sorted(calls) == ["ACME", "OTHER"]
            assert flight.inflight() == 0
        finally:
            release.set()
            await finish(tasks)
            await flight.aclose()

    asyncio.run(scenario())


def test_shutdown_cancels_and_drains_owned_work_and_its_callers():
    async def scenario():
        flight = SingleFlight()
        started = {key: asyncio.Event() for key in ("ACME", "OTHER")}
        finalized = []

        async def acquire(key):
            started[key].set()
            try:
                await asyncio.Event().wait()
            finally:
                finalized.append(key)

        tasks = [
            asyncio.create_task(flight.execute(key, lambda key=key: acquire(key)))
            for key in ("ACME", "ACME", "OTHER")
        ]
        try:
            await asyncio.wait_for(
                asyncio.gather(*(event.wait() for event in started.values())), timeout=1,
            )
            await asyncio.wait_for(flight.aclose(), timeout=1)
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=1,
            )
            assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
            assert sorted(finalized) == ["ACME", "OTHER"]
            assert flight.inflight() == 0
            with pytest.raises(RuntimeError, match="closed for this event loop"):
                await flight.execute("ACME", lambda: acquire("ACME"))
            await flight.aclose()  # Repeated shutdown is safe.
        finally:
            await finish(tasks)

    asyncio.run(scenario())


def test_event_loop_shutdown_cleans_abandoned_task_and_next_loop_can_reuse_key():
    flight = SingleFlight()
    finalized = []

    async def old_loop():
        started = asyncio.Event()

        async def acquire():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finalized.append("old loop")

        first = asyncio.create_task(flight.execute("ACME", acquire))
        await asyncio.wait_for(started.wait(), timeout=1)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert flight.inflight() == 1

    asyncio.run(old_loop())
    assert finalized == ["old loop"]

    async def new_loop():
        async def acquire():
            return "new loop news"

        assert flight.inflight() == 0
        assert await flight.execute("ACME", acquire) == "new loop news"
        await flight.aclose()

    asyncio.run(new_loop())
    asyncio.run(new_loop())  # Explicitly closing one loop does not close another.


def test_simultaneous_event_loops_do_not_share_foreign_tasks():
    flight = SingleFlight()
    barrier = threading.Barrier(2)

    def worker(label):
        async def scenario():
            async def acquire():
                # Both independent loop threads must reach acquisition before
                # either completes; no cross-thread asyncio wakeup is needed.
                barrier.wait(timeout=1)
                return label

            try:
                return await flight.execute("ACME", acquire)
            finally:
                await flight.aclose()

        return asyncio.run(scenario())

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker, label) for label in ("loop one", "loop two")]
        assert [future.result(timeout=3) for future in futures] == ["loop one", "loop two"]


@pytest.mark.parametrize("timeout", [0.0, -1.0, float("nan"), float("inf"), -float("inf")])
def test_timeout_must_be_positive_and_finite(timeout):
    with pytest.raises(ValueError, match="positive and finite"):
        SingleFlight(timeout_seconds=timeout)
