from __future__ import annotations

import asyncio
import os
import time
import unittest
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import patch

from scripts.concurrent_batch_fetch import build, get_metrics, install
from scripts import run_dashboard_sync as production_sync


@dataclass
class Result:
    request_id: str = "req"
    warnings: list[str] = field(default_factory=list)
    batch_metrics: dict = field(default_factory=dict)


class Backend:
    def __init__(
        self,
        fail_once=(),
        fail_always=(),
        delay=.02,
        mismatch=(),
        omit_once=(),
        stub_once=(),
        response_aliases=None,
    ):
        self.fail_once = set(fail_once)
        self.fail_always = set(fail_always)
        self.mismatch = set(mismatch)
        self.omit_once = set(omit_once)
        self.stub_once = set(stub_once)
        self.response_aliases = dict(response_aliases or {})
        self.failed = set()
        self.omitted = set()
        self.stubbed = set()
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

            headers = ["Symbol", "Name", "Current Price", "Data Provider"]
            rows = [
                [self.response_aliases.get(symbol, symbol), symbol.lower(), 100.0, "mock"]
                for symbol in reversed(symbols)
            ]
            if key in self.omit_once and key not in self.omitted:
                self.omitted.add(key)
                rows = [row for row in rows if row[0] != key]
            if key in self.stub_once and key not in self.stubbed:
                self.stubbed.add(key)
                rows = [
                    [row[0], "", None, "fallback_error"] if row[0] == key else row
                    for row in rows
                ]
            if key in self.mismatch:
                headers = ["Name", "Symbol", "Current Price", "Data Provider"]
                rows = [[row[1], row[0], row[2], row[3]] for row in rows]
            return {"headers": headers, "rows": rows}, None, 200
        finally:
            self.active -= 1


def fake(size=1):
    symbol_aliases = {"symbol", "ticker"}
    name_aliases = {"name", "companyname"}
    price_aliases = {"currentprice", "price", "lastprice"}
    provider_aliases = {"dataprovider", "provider", "datasource"}

    def normalize(value):
        return "".join(character for character in str(value or "").lower() if character.isalnum())

    def find_column(headers, aliases):
        wanted = {normalize(alias) for alias in aliases}
        for index, header in enumerate(headers):
            if normalize(header) in wanted:
                return index
        return -1

    return SimpleNamespace(
        _request_limit_ceiling=lambda: 1000,
        _time_budget_exceeded=lambda: False,
        _symbol_batch_size=lambda: size,
        _batch_delay_ms=lambda: 0,
        build_isolated_batches=lambda symbols, batch_size: [
            symbols[index:index + batch_size]
            for index in range(0, len(symbols), batch_size)
        ],
        _endpoint_candidates_for_gateway=lambda gateway: ["/e"],
        _extract_table_payload=lambda data: (data.get("headers", []), data.get("rows", [])),
        _rectify_matrix=lambda headers, rows: rows,
        _batch_identity_enabled=lambda: True,
        _guard_find_col=find_column,
        _GUARD_SYMBOL_ALIASES=symbol_aliases,
        _GUARD_NAME_ALIASES=name_aliases,
        _XPAGE_PRICE_ALIASES=price_aliases,
        _KLG_PROVIDER_ALIASES=provider_aliases,
        _guard_is_blank=lambda value: value is None or str(value).strip() == "",
        _klg_provider_is_error=lambda value: normalize(value) in {"fallbackerror", "error"},
        canonicalize_symbol=lambda value: str(value).strip().upper(),
        _build_request_symbol_index=production_sync._build_request_symbol_index,
        _resolve_requested_symbol=production_sync._resolve_requested_symbol,
        _BATCH_IDENTITY_TAG="[ID]",
        logger=SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
            exception=lambda *args, **kwargs: None,
        ),
    )


class Tests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_concurrency_and_order(self):
        fn = build(fake())
        backend = Backend(delay=.03)
        result = Result("bounded")
        with patch.dict(
            os.environ,
            {
                "TFB_SYNC_BATCH_CONCURRENCY": "3",
                "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
                "TFB_SYNC_TARGET_RECOVERY": "1",
            },
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
                Backend(fail_once={"C"}),
                SimpleNamespace(sheet_name="P"),
                list("ABCD"),
                {},
                "analysis",
                result,
            )
        self.assertEqual([row[0] for row in rows], list("ABCD"))
        self.assertEqual(get_metrics("retry")["symbols_failed"], 0)

    async def test_targeted_recovery_heals_missing_symbol(self):
        fn = build(fake())
        result = Result("missing")
        with patch.dict(
            os.environ,
            {
                "TFB_SYNC_BATCH_CONCURRENCY": "3",
                "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
                "TFB_SYNC_TARGET_RECOVERY": "1",
                "TFB_SYNC_TARGET_RECOVERY_ROUNDS": "1",
            },
        ):
            _, rows, _, _ = await fn(
                Backend(omit_once={"C"}),
                SimpleNamespace(sheet_name="P"),
                list("ABCD"),
                {},
                "analysis",
                result,
            )
        self.assertEqual([row[0] for row in rows], list("ABCD"))
        self.assertEqual(result.batch_metrics["symbols_missing_initial"], 1)
        self.assertEqual(result.batch_metrics["targeted_recovery_healed"], 1)
        self.assertEqual(result.batch_metrics["symbols_missing"], 0)

    async def test_targeted_recovery_heals_data_free_stub(self):
        fn = build(fake())
        result = Result("stub")
        with patch.dict(
            os.environ,
            {
                "TFB_SYNC_BATCH_CONCURRENCY": "3",
                "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
                "TFB_SYNC_TARGET_RECOVERY": "1",
                "TFB_SYNC_TARGET_RECOVERY_ROUNDS": "1",
            },
        ):
            _, rows, _, _ = await fn(
                Backend(stub_once={"C"}),
                SimpleNamespace(sheet_name="P"),
                list("ABCD"),
                {},
                "analysis",
                result,
            )
        self.assertEqual([row[0] for row in rows], list("ABCD"))
        self.assertEqual(result.batch_metrics["symbols_data_free_initial"], 1)
        self.assertEqual(result.batch_metrics["targeted_recovery_healed"], 1)
        self.assertEqual(result.batch_metrics["symbols_data_free"], 0)
        self.assertEqual(result.batch_metrics["fresh_coverage_pct"], 100.0)


    async def test_request_scoped_us_suffix_echoes_map_back_to_requested_order(self):
        fn = build(fake(size=2))
        requested = ["HNGE", "TT.US", "ADP", "ITW.US"]
        aliases = {
            "HNGE": "HNGE.US",
            "TT.US": "TT",
            "ADP": "ADP.US",
            "ITW.US": "ITW",
        }
        result = Result("us-suffix-echo")
        with patch.dict(
            os.environ,
            {
                "TFB_SYNC_BATCH_CONCURRENCY": "3",
                "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
                "TFB_SYNC_TARGET_RECOVERY": "0",
            },
        ):
            _, rows, _, _ = await fn(
                Backend(response_aliases=aliases),
                SimpleNamespace(sheet_name="Market_Leaders"),
                requested,
                {},
                "analysis",
                result,
            )
        self.assertEqual([row[0] for row in rows], requested)
        self.assertEqual(result.batch_metrics["symbols_fresh"], 4)
        self.assertEqual(result.batch_metrics["symbols_missing"], 0)
        self.assertFalse(any("cross_batch=" in warning for warning in result.warnings))

    async def test_header_mismatch_is_not_merged(self):
        fn = build(fake())
        result = Result("header")
        with patch.dict(
            os.environ,
            {
                "TFB_SYNC_BATCH_CONCURRENCY": "3",
                "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
                "TFB_SYNC_TARGET_RECOVERY_ROUNDS": "1",
            },
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
        self.assertEqual(result.batch_metrics["symbols_missing"], 1)

    async def test_install_concurrency_one_uses_exact_original(self):
        calls = []

        async def original(*args, **kwargs):
            calls.append("original")
            return ["H"], [["A"]], "/old", None

        sync = fake()
        sync._fetch_market_rows_batched = original
        install(sync)
        with patch.dict(os.environ, {"TFB_SYNC_BATCH_CONCURRENCY": "1"}):
            output = await sync._fetch_market_rows_batched(
                None, None, [], {}, "g", Result()
            )
        self.assertEqual(calls, ["original"])
        self.assertEqual(output[2], "/old")

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
            output = await sync._fetch_market_rows_batched(
                None, None, ["A"], {}, "g", Result()
            )
        self.assertEqual(calls, ["original"])
        self.assertEqual(output[2], "/old")

    async def test_parallel_is_materially_faster(self):
        fn = build(fake())
        symbols = list("ABCDEFG")
        base_env = {
            "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
            "TFB_SYNC_TARGET_RECOVERY": "0",
        }
        with patch.dict(os.environ, base_env | {"TFB_SYNC_BATCH_CONCURRENCY": "1"}):
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
        with patch.dict(os.environ, base_env | {"TFB_SYNC_BATCH_CONCURRENCY": "3"}):
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

    async def test_production_runner_dispatches_concurrent_adapter(self):
        called = {}

        async def fake_fetch(backend, task, symbols, payload, gateway, result):
            called["symbols"] = list(symbols)
            return ["Symbol"], [["A"]], "/fast", None

        task = production_sync.TaskSpec(
            key="GLOBAL_MARKETS",
            sheet_name="Global_Markets",
            gateway="analysis",
        )
        result = production_sync.TaskResult(
            key=task.key,
            sheet_name=task.sheet_name,
            status="pending",
            start_utc="2026-07-30T00:00:00+00:00",
        )
        with patch.dict(os.environ, {"TFB_SYNC_BATCH_CONCURRENCY": "3"}), patch(
            "scripts.concurrent_batch_fetch.build", return_value=fake_fetch
        ):
            output = await production_sync._fetch_market_rows_batched(
                object(), task, ["A", "B"], {}, "analysis", result
            )
        self.assertEqual(called["symbols"], ["A", "B"])
        self.assertEqual(output[2], "/fast")

    async def test_production_runner_concurrency_one_avoids_adapter(self):
        task = production_sync.TaskSpec(
            key="GLOBAL_MARKETS",
            sheet_name="Global_Markets",
            gateway="analysis",
        )
        result = production_sync.TaskResult(
            key=task.key,
            sheet_name=task.sheet_name,
            status="pending",
            start_utc="2026-07-30T00:00:00+00:00",
        )
        with patch.dict(os.environ, {"TFB_SYNC_BATCH_CONCURRENCY": "1"}), patch(
            "scripts.concurrent_batch_fetch.build",
            side_effect=AssertionError("adapter must not load"),
        ), patch.object(
            production_sync, "_time_budget_exceeded", return_value=True
        ), patch.object(
            production_sync, "_time_budget_sec", return_value=3600.0
        ):
            output = await production_sync._fetch_market_rows_batched(
                object(), task, ["A"], {}, "analysis", result
            )
        self.assertEqual(output[3], "time budget exhausted before fetch")


if __name__ == "__main__":
    unittest.main()
