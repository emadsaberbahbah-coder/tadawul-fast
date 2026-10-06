"""Quota regressions for concurrent Yahoo callers, with no real time or I/O."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from core.providers import yahoo_chart_provider as yahoo


class _Clock:
    def __init__(self):
        self.now = 100.0
        self.sleepers = []
        self.delays = []

    def monotonic(self):
        return self.now

    async def sleep(self, delay):
        self.delays.append(delay)
        future = asyncio.get_running_loop().create_future()
        self.sleepers.append((self.now + delay, future))
        await future

    def advance(self):
        pending = [(deadline, f) for deadline, f in self.sleepers if not f.done()]
        if not pending:
            return False
        self.now = min(deadline for deadline, _ in pending)
        for deadline, future in pending:
            if deadline <= self.now:
                future.set_result(None)
        return True


def _install_clock(monkeypatch):
    clock = _Clock()
    # Replace the provider references only; keep asyncio's own clock intact.
    monkeypatch.setattr(yahoo, "time", SimpleNamespace(monotonic=clock.monotonic))
    monkeypatch.setattr(yahoo, "asyncio", SimpleNamespace(sleep=clock.sleep))
    return clock


def _acquisition_times(bucket, clock, count):
    async def run():
        times = []

        async def acquire():
            await bucket.acquire()
            times.append(clock.now)

        tasks = [asyncio.create_task(acquire()) for _ in range(count)]
        for _ in range(1000):
            await asyncio.sleep(0)
            if all(task.done() for task in tasks):
                await asyncio.gather(*tasks)
                return sorted(times)
            clock.advance()
        raise AssertionError("token acquisition failed to complete")

    return asyncio.run(run())


def test_concurrent_waiters_consume_individual_refills(monkeypatch):
    clock = _install_clock(monkeypatch)
    bucket = yahoo.TokenBucket(rate_per_sec=2.0, burst=2.0)
    times = _acquisition_times(bucket, clock, 7)
    assert times == pytest.approx([100, 100, 100.5, 101, 101.5, 102, 102.5])


def test_capped_sleep_rechecks_until_low_rate_token_is_available(monkeypatch):
    clock = _install_clock(monkeypatch)
    bucket = yahoo.TokenBucket(rate_per_sec=0.05, burst=1.0)
    assert _acquisition_times(bucket, clock, 3) == pytest.approx([100, 120, 140])
    assert all(0 < delay <= 5 for delay in clock.delays)


def test_cancelled_waiter_preserves_available_fractional_tokens(monkeypatch):
    clock = _install_clock(monkeypatch)
    bucket = yahoo.TokenBucket(rate_per_sec=1.0, burst=1.0, tokens=0.75, last=100.0)

    async def run():
        task = asyncio.create_task(bucket.acquire())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert bucket.tokens == pytest.approx(0.75)
        clock.now += 0.25
        await bucket.acquire()
        assert bucket.tokens == pytest.approx(0.0)

    asyncio.run(run())


def test_impossible_request_fails_without_waiting(monkeypatch):
    clock = _install_clock(monkeypatch)
    bucket = yahoo.TokenBucket(rate_per_sec=1.0, burst=1.0)

    async def run():
        task = asyncio.create_task(bucket.acquire(2.0))
        await asyncio.sleep(0)
        clock.advance()
        await task

    with pytest.raises(ValueError, match="burst"):
        asyncio.run(run())
    assert clock.delays == []


def test_disabled_limiter_does_not_wait(monkeypatch):
    clock = _install_clock(monkeypatch)
    bucket = yahoo.TokenBucket(rate_per_sec=0.0, burst=1.0)
    assert _acquisition_times(bucket, clock, 7) == [100.0] * 7
    assert clock.delays == []
