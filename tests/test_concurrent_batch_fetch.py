from __future__ import annotations

import asyncio
import os
import time
import unittest
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import patch

from scripts.concurrent_batch_fetch import build, get_metrics, install


@dataclass
class Result:
    request_id: str = "req"
    warnings: list[str] = field(default_factory=list)
    batch_metrics: dict = field(default_factory=dict)


class Backend:
    def __init__(self, fail_once=(), fail_always=(), delay=.02, mismatch=()):
        self.fail_once = set(fail_once)
        self.fail_always = set(fail_always)
        self.mismatch = set(mismatch)
        self.failed = set()
        self.delay = delay
        self.active = 0
        self.max_active = 0

    async def post_json(self, endpoint, payload):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delay)
            symbols = list(payload["symbols"])
            key = symbols[0]
            if key in self.fail_always:
                return None, "permanent", 500
            if key in self.fail_once and key not in self.failed:
                self.failed.add(key)
                return None, "temporary", 500
            headers = ["Value", "Symbol"] if key in self.mismatch else ["Symbol", "Value"]
            rows = (
                [[s.lower(), s] for s in reversed(symbols)]
                if key in self.mismatch
                else [[s, s.lower()] for s in reversed(symbols)]
            )
            return {"headers": headers, "rows": rows}, None, 200
        finally:
            self.active -= 1


def fake(size=1):
    return SimpleNamespace(
        _request_limit_ceiling=lambda: 1000,
        _time_budget_exceeded=lambda: False,
        _symbol_batch_size=lambda: size,
        _batch_delay_ms=lambda: 0,
        build_isolated_batches=lambda syms, n: [
            syms[i:i + n] for i in range(0, len(syms), n)
        ],
        _endpoint_candidates_for_gateway=lambda g: ["/e"],
        _extract_table_payload=lambda d: (d.get("headers", []), d.get("rows", [])),
        _rectify_matrix=lambda h, r: r,
        _batch_identity_enabled=lambda: True,
        _guard_find_col=lambda h, a: 0,
        _GUARD_SYMBOL_ALIASES={"symbol"},
        _guard_is_blank=lambda v: v is None or str(v).strip() == "",
        canonicalize_symbol=lambda v: str(v).strip().upper(),
        _BATCH_IDENTITY_TAG="[ID]",
        logger=SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )


class Tests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_concurrency_and_order(self):
        fn = build(fake())
        backend = Backend(delay=.03)
        result = Result("bounded")
        with patch.dict(
            os.environ,
            {"TFB_SYNC_BATCH_CONCURRENCY": "3", "TFB_SYNC_BATCH_OUTER_RETRIES": "0"},
        ):
            _, rows, _, _ = await fn(
                backend,
                SimpleNamespace(sheet_name="Global_Markets"),
                list("ABCDEFG"),
                {},
                "analysis",
                result,
            )
        self.assertEqual(backend.max_active, 3)
        self.assertEqual([row[0] for row in rows], list("ABCDEFG"))
        self.assertEqual(result.batch_metrics["symbols_fresh"], 7)

    async def test_failed_first_batch_does_not_block_endpoint_resolution(self):
        fn = build(fake())
        result = Result("resolve")
        with patch.dict(
            os.environ,
            {"TFB_SYNC_BATCH_CONCURRENCY": "3", "TFB_SYNC_BATCH_OUTER_RETRIES": "1"},
        ):
            _, rows, endpoint, _ = await fn(
                Backend(fail_once={"A"}),
                SimpleNamespace(sheet_name="P"),
                list("ABCD"),
                {},
                "analysis",
                result,
            )
        self.assertEqual(endpoint, "/e")
        self.assertEqual([row[0] for row in rows], list("ABCD"))
        self.assertEqual(result.batch_metrics["endpoint_resolve_batches"], 2)

    async def test_retry_recovers_failed_batch(self):
        fn = build(fake())
        result = Result("retry")
        with patch.dict(
            os.environ,
            {"TFB_SYNC_BATCH_CONCURRENCY": "3", "TFB_SYNC_BATCH_OUTER_RETRIES": "1"},
        ):
            _, rows, _, _ = await fn(
                Backend({"C"}),
                SimpleNamespace(sheet_name="P"),
                list("ABCD"),
                {},
                "analysis",
                result,
            )
        self.assertEqual([row[0] for row in rows], list("ABCD"))
        self.assertEqual(get_metrics("retry")["symbols_failed"], 0)

    async def test_header_mismatch_is_not_merged(self):
        fn = build(fake())
        result = Result("header")
        with patch.dict(
            os.environ,
            {"TFB_SYNC_BATCH_CONCURRENCY": "3", "TFB_SYNC_BATCH_OUTER_RETRIES": "0"},
        ):
            _, rows, _, _ = await fn(
                Backend(mismatch={"C"}),
                SimpleNamespace(sheet_name="P"),
                list("ABCD"),
                {},
                "analysis",
                result,
            )
        self.assertEqual([row[0] for row in rows], ["A", "B", "D"])
        self.assertEqual(result.batch_metrics["symbols_failed"], 1)

    async def test_install_concurrency_one_uses_exact_original(self):
        calls = []

        async def original(*args, **kwargs):
            calls.append("original")
            return ["H"], [["A"]], "/old", None

        sync = fake()
        sync._fetch_market_rows_batched = original
        install(sync)
        with patch.dict(os.environ, {"TFB_SYNC_BATCH_CONCURRENCY": "1"}):
            result = await sync._fetch_market_rows_batched(
                None, None, [], {}, "g", Result()
            )
        self.assertEqual(calls, ["original"])
        self.assertEqual(result[2], "/old")

    async def test_adapter_exception_falls_back_to_original(self):
        calls = []

        async def original(*args, **kwargs):
            calls.append("original")
            return ["H"], [["A"]], "/old", None

        sync = fake()
        sync._fetch_market_rows_batched = original
        sync.build_isolated_batches = lambda *_: (_ for _ in ()).throw(
            RuntimeError("boom")
        )
        install(sync)
        with patch.dict(os.environ, {"TFB_SYNC_BATCH_CONCURRENCY": "3"}):
            result = await sync._fetch_market_rows_batched(
                None, None, ["A"], {}, "g", Result()
            )
        self.assertEqual(calls, ["original"])
        self.assertEqual(result[2], "/old")

    async def test_parallel_is_materially_faster(self):
        fn = build(fake())
        symbols = list("ABCDEFG")
        with patch.dict(
            os.environ,
            {"TFB_SYNC_BATCH_CONCURRENCY": "1", "TFB_SYNC_BATCH_OUTER_RETRIES": "0"},
        ):
            start = time.perf_counter()
            await fn(
                Backend(delay=.03),
                SimpleNamespace(sheet_name="P"),
                symbols,
                {},
                "analysis",
                Result("sequential"),
            )
            sequential = time.perf_counter() - start
        with patch.dict(
            os.environ,
            {"TFB_SYNC_BATCH_CONCURRENCY": "3", "TFB_SYNC_BATCH_OUTER_RETRIES": "0"},
        ):
            start = time.perf_counter()
            await fn(
                Backend(delay=.03),
                SimpleNamespace(sheet_name="P"),
                symbols,
                {},
                "analysis",
                Result("parallel"),
            )
            parallel = time.perf_counter() - start
        self.assertLess(parallel, sequential * .65)


if __name__ == "__main__":
    unittest.main()
