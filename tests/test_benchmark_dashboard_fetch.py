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


if __name__ == "__main__":
    unittest.main()
