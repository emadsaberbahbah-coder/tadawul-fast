from __future__ import annotations

import argparse
import unittest
from unittest.mock import AsyncMock, patch

from scripts import benchmark_dashboard_fetch as benchmark
from scripts import run_dashboard_sync as sync


class BenchmarkTests(unittest.IsolatedAsyncioTestCase):
    def test_no_write_sink_records_without_google_write_call(self):
        sheets = benchmark.NoWriteSheets()
        sheets._service = object()
        rows = [["A", 1], ["B", 2]]
        written = sheets.write_table("sid", "Market_Leaders", "A1", ["Symbol", "Value"], rows)
        sheets.clear_from("sid", "Market_Leaders", "A1")
        self.assertEqual(written, 2)
        self.assertEqual(sheets.planned_writes[0]["rows"], 2)
        self.assertEqual(sheets.clear_requests[0]["sheet_name"], "Market_Leaders")

    def test_task_resolution_accepts_key_and_page(self):
        self.assertEqual(benchmark._task_for("MARKET_LEADERS").sheet_name, "Market_Leaders")
        self.assertEqual(benchmark._task_for("Global_Markets").key, "GLOBAL_MARKETS")
        with self.assertRaises(ValueError):
            benchmark._task_for("Not_A_Page")

    def test_parser_defaults_to_sequential_concurrency(self):
        args = benchmark.create_parser().parse_args([])
        self.assertEqual(
            args.concurrency,
            1,
            "concurrency greater than 1 must require an explicit benchmark argument",
        )

    async def test_run_benchmark_reports_no_write_acceptance(self):
        args = argparse.Namespace(
            page="Market_Leaders",
            sheet_id="sheet-id",
            backend="https://example.invalid",
            max_symbols=1000,
            batch_size=25,
            concurrency=3,
            outer_retries=1,
            timeout=120.0,
            time_budget=2100,
            json_out="",
        )
        result = sync.TaskResult(
            key="MARKET_LEADERS",
            sheet_name="Market_Leaders",
            status="success",
            start_utc="2026-07-30T00:00:00+00:00",
            symbols_requested=1000,
            rows_written=1000,
            batch_metrics={
                "symbols_requested": 1000,
                "symbols_returned": 1000,
                "symbols_fresh": 1000,
                "symbols_data_free": 0,
                "symbols_missing": 0,
                "symbols_failed": 0,
                "symbols_unattempted": 0,
                "targeted_recovery_requested": 0,
                "targeted_recovery_healed": 0,
                "fresh_coverage_pct": 100.0,
                "http_429": 0,
                "http_5xx": 0,
            },
        )

        class FakeBackend:
            def __init__(self, *args, **kwargs):
                pass

            async def close(self):
                return None

        async def fake_run_one_task(**kwargs):
            # The production runner writes into NoWriteSheets, which records the
            # planned matrix without calling Google write APIs. Simulate that
            # contract so the acceptance test proves both complete metrics and
            # preservation of the requested 1,000-row universe.
            kwargs["sheets"].write_table(
                "sheet-id",
                "Market_Leaders",
                "A1",
                ["Symbol"],
                [[f"S{i}"] for i in range(1000)],
            )
            return result

        with patch.object(sync, "BackendClient", FakeBackend), patch.object(
            sync, "_run_one_task", AsyncMock(side_effect=fake_run_one_task)
        ), patch.object(sync, "_idfw_selftest_", return_value=True):
            code, payload = await benchmark.run_benchmark(args)

        self.assertEqual(code, 0)
        self.assertTrue(payload["no_workbook_writes"])
        self.assertTrue(payload["acceptance"]["complete_fresh_fetch"])
        self.assertTrue(payload["acceptance"]["universe_preserved"])
        self.assertEqual(payload["batch_metrics"]["symbols_fresh"], 1000)

    async def test_sequential_empty_runner_metrics_are_observed_and_fail_closed(self):
        args = argparse.Namespace(
            page="Market_Leaders",
            sheet_id="sheet-id",
            backend="https://example.invalid",
            max_symbols=3,
            batch_size=2,
            concurrency=1,
            outer_retries=1,
            timeout=120.0,
            time_budget=2100,
            json_out="",
        )
        headers = ["Symbol", "Name", "Current Price", "Data Provider"]
        raw_rows = [
            ["AAA", "Alpha", 10.0, "eodhd"],
            ["BBB", "", 20.0, "eodhd"],
        ]

        class FakeBackend:
            def __init__(self, *args, **kwargs):
                self.calls = 0

            async def post_json(self, endpoint, payload):
                self.calls += 1
                if self.calls == 1:
                    return {}, "rate limited", 429
                if self.calls == 2:
                    return {}, "server error", 503
                return {}, None, 200

            async def close(self):
                return None

        async def fake_fetch(backend, task, symbols, base_payload, eff_gw, res):
            payload = {"symbols": list(symbols), "request_id": "sequential-b1"}
            await backend.post_json("/candidate-1", payload)
            await backend.post_json("/candidate-2", payload)
            await backend.post_json("/v1/analysis/sheet-rows", payload)
            return list(headers), [list(row) for row in raw_rows], "/v1/analysis/sheet-rows", None

        async def fake_run_one_task(**kwargs):
            result = sync.TaskResult(
                key="MARKET_LEADERS",
                sheet_name="Market_Leaders",
                status="success",
                start_utc="2026-07-30T00:00:00+00:00",
                symbols_requested=3,
                symbols_processed=3,
                rows_written=3,
                batch_metrics={},
            )
            await sync._fetch_market_rows_batched(
                kwargs["backend"],
                kwargs["task"],
                ["AAA", "BBB", "CCC"],
                {},
                "analysis",
                result,
            )
            kwargs["sheets"].write_table(
                "sheet-id",
                "Market_Leaders",
                "A1",
                list(headers),
                [
                    ["AAA", "Alpha", 10.0, "eodhd"],
                    ["BBB", "Last good", 20.0, "eodhd"],
                    ["CCC", "Last good", 30.0, "eodhd"],
                ],
            )
            return result

        with patch.object(sync, "BackendClient", FakeBackend), patch.object(
            sync, "_fetch_market_rows_batched", fake_fetch
        ), patch.object(
            sync, "_run_one_task", AsyncMock(side_effect=fake_run_one_task)
        ), patch.object(sync, "_idfw_selftest_", return_value=True):
            code, payload = await benchmark.run_benchmark(args)

        metrics = payload["batch_metrics"]
        self.assertEqual(code, 1)
        self.assertEqual(metrics["mode"], "benchmark_observed_sequential")
        self.assertEqual(metrics["symbols_requested"], 3)
        self.assertEqual(metrics["symbols_attempted"], 3)
        self.assertEqual(metrics["symbols_returned"], 2)
        self.assertEqual(metrics["symbols_fresh"], 1)
        self.assertEqual(metrics["symbols_data_free"], 1)
        self.assertEqual(metrics["symbols_missing"], 1)
        self.assertEqual(metrics["symbols_unattempted"], 0)
        self.assertEqual(metrics["targeted_recovery_requested"], 0)
        self.assertEqual(metrics["targeted_recovery_healed"], 0)
        self.assertEqual(metrics["http_429"], 1)
        self.assertEqual(metrics["http_5xx"], 1)
        self.assertTrue(payload["acceptance"]["acceptance_metrics_complete"])
        self.assertTrue(payload["acceptance"]["universe_preserved"])
        self.assertFalse(payload["acceptance"]["complete_fresh_fetch"])


if __name__ == "__main__":
    unittest.main()
