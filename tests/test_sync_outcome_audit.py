from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.audit_sync_outcome import CRITICAL_MARKET_PAGES, audit_artifacts


class SyncOutcomeAuditTests(unittest.TestCase):
    def _audit(self, text: str):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / "artifact"
            artifact.mkdir()
            (artifact / "sync_execution.log").write_text(text, encoding="utf-8")
            return audit_artifacts(root)

    @staticmethod
    def _line(
        page: str,
        status: str = "success",
        rows: int = 10,
        *,
        fresh: int | None = None,
        requested: int | None = None,
    ) -> str:
        coverage = ""
        if fresh is not None and requested is not None:
            pct = 100.0 * fresh / requested if requested else 0.0
            coverage = (
                f"fresh_rows={fresh} requested_rows={requested} "
                f"fresh_pct={pct:.4f} "
            )
        return (
            "2026-07-29 01:00:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v6.26.0] page={page} status={status} "
            f"rows_written={rows} {coverage}newest_stamp_age_h=1 reason=test\n"
        )

    def test_all_required_pages_with_rows_pass(self):
        result = self._audit("".join(self._line(page) for page in CRITICAL_MARKET_PAGES))
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.missing_pages)
        self.assertFalse(result.failed_pages)

    def test_skipped_required_page_blocks(self):
        text = "".join(
            self._line(page, "skipped" if page == "Global_Markets" else "success", 0 if page == "Global_Markets" else 10)
            for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertEqual(result.status, "blocked")
        self.assertIn("Global_Markets", result.failed_pages)
        self.assertEqual(result.exit_code, 2)

    def test_success_with_zero_rows_blocks(self):
        text = "".join(
            self._line(page, "success", 0 if page == "Market_Leaders" else 10)
            for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertIn("Market_Leaders", result.failed_pages)

    def test_missing_required_page_blocks(self):
        text = "".join(self._line(page) for page in CRITICAL_MARKET_PAGES[:-1])
        result = self._audit(text)
        self.assertEqual(result.missing_pages, ("Mutual_Funds",))

    def test_non_market_verdict_does_not_replace_required_pages(self):
        text = self._line("Data_Dictionary") + "".join(
            self._line(page) for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertEqual(result.status, "ok")
        self.assertNotIn("Data_Dictionary", result.observed_pages)

    def test_force_refetch_evidence_is_counted(self):
        text = "[FORCE-REFETCH] symbol=BK provider=eodhd\n" + "".join(
            self._line(page) for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertEqual(result.force_refetch_evidence_lines, 1)

    def _incomplete_fetch_log(self) -> str:
        return (
            "[v6.64.0 TIME-BUDGET] Global_Markets: budget 3600s exhausted "
            "after 73/239 batches\n"
            "[v6.64.0 FLOOR-MERGE] Partial fetch on 'Global_Markets': "
            "73 fresh row(s) for 239 requested (30% coverage; last-good rows retained)\n"
            + "".join(self._line(page, rows=6609 if page == "Global_Markets" else 10) for page in CRITICAL_MARKET_PAGES)
        )

    def test_incomplete_fetch_is_diagnostic_while_gate_is_off(self):
        with mock.patch.dict("os.environ", {"TFB_AUDIT_REQUIRE_FULL_FETCH": "0"}):
            result = self._audit(self._incomplete_fetch_log())
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.incomplete_pages, ("Global_Markets",))
        self.assertFalse(result.full_fetch_gate)

    def test_incomplete_fetch_blocks_when_workflow_gate_is_on(self):
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit(self._incomplete_fetch_log())
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Global_Markets", result.failed_pages)
        self.assertTrue(result.full_fetch_gate)

    def test_exact_page_coverage_blocks_between_runner_and_audit_floors(self):
        text = "".join(
            self._line(
                page,
                rows=100,
                fresh=90 if page == "Global_Markets" else 100,
                requested=100,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit(text)
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.incomplete_pages, ("Global_Markets",))
        evidence = next(
            item for item in result.fetch_evidence
            if item["page"] == "Global_Markets"
        )
        self.assertEqual(evidence["fresh_rows"], 90)
        self.assertEqual(evidence["requested"], 100)
        self.assertEqual(evidence["coverage_source"], "page_verdict")

    def test_exact_page_coverage_uses_ratio_not_rounded_percent(self):
        # 94/99 = 94.949...%; a one-decimal display would round to 94.9 and a
        # whole-percent display to 95. Integer comparison keeps it below 95.
        text = "".join(
            self._line(
                page,
                rows=99,
                fresh=94 if page == "Global_Markets" else 99,
                requested=99,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit(text)
        self.assertEqual(result.incomplete_pages, ("Global_Markets",))

    def test_exact_page_coverage_passes_at_threshold(self):
        text = "".join(
            self._line(
                page,
                rows=100,
                fresh=95 if page == "Global_Markets" else 100,
                requested=100,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit(text)
        self.assertEqual(result.status, "ok")
        self.assertFalse(result.incomplete_pages)

    def test_explicit_unknown_coverage_blocks_only_when_gate_is_armed(self):
        unknown = (
            "[PAGE-VERDICT v6.64.2] page=Global_Markets status=success "
            "rows_written=100 fresh_rows=NA requested_rows=100 fresh_pct=NA "
            "newest_stamp_age_h=1 reason=test\n"
        )
        text = unknown + "".join(
            self._line(page, fresh=100, requested=100)
            for page in CRITICAL_MARKET_PAGES
            if page != "Global_Markets"
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            armed = self._audit(text)
        self.assertEqual(armed.status, "blocked")
        self.assertEqual(armed.incomplete_pages, ("Global_Markets",))
        evidence = next(
            item for item in armed.fetch_evidence
            if item["page"] == "Global_Markets"
        )
        self.assertTrue(evidence["coverage_unknown"])

        with mock.patch.dict(
            "os.environ", {"TFB_AUDIT_REQUIRE_FULL_FETCH": "0"}
        ):
            diagnostic = self._audit(text)
        self.assertEqual(diagnostic.status, "ok")
        self.assertEqual(diagnostic.incomplete_pages, ("Global_Markets",))

    def test_armed_gate_keeps_legacy_no_coverage_verdicts_compatible(self):
        text = "".join(self._line(page) for page in CRITICAL_MARKET_PAGES)
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit(text)
        self.assertEqual(result.status, "ok")
        self.assertFalse(result.incomplete_pages)

    def test_partial_with_rows_blocks_only_when_full_fetch_gate_is_armed(self):
        text = "".join(
            self._line(
                page,
                status="partial" if page == "Global_Markets" else "success",
                rows=100,
                fresh=100,
                requested=100,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        with mock.patch.dict(
            "os.environ", {"TFB_AUDIT_REQUIRE_FULL_FETCH": "0"}
        ):
            legacy = self._audit(text)
        self.assertEqual(legacy.status, "ok")

        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            armed = self._audit(text)
        self.assertEqual(armed.status, "blocked")
        self.assertIn("Global_Markets", armed.failed_pages)
        verdict = next(
            item for item in armed.to_dict()["verdicts"]
            if item["page"] == "Global_Markets"
        )
        self.assertFalse(verdict["passed"])

    def test_runner_page_verdict_emits_exact_fresh_coverage(self):
        from scripts import run_dashboard_sync as sync

        result = sync.TaskResult(
            key="global_markets",
            sheet_name="Global_Markets",
            status="success",
            start_utc="2026-10-06T00:00:00+00:00",
            rows_written=100,
            symbols_requested=100,
            _stamp_meta={
                "requested": 100,
                "pre_persist_rows": 90,
                "klg_kept": 0,
            },
        )
        with mock.patch.object(
            sync, "_page_newest_stamp_age_h", return_value=1.0
        ), mock.patch.object(
            sync, "_stale_skip_red_enabled", return_value=True
        ), mock.patch.object(sync.logger, "info") as log_info:
            sync._apply_stale_skip_escalation([result], None, "sheet")
        verdict_call = next(
            item for item in log_info.call_args_list
            if len(item.args) > 1 and item.args[1] == sync._PAGE_VERDICT_TAG
        )
        line = verdict_call.args[0] % verdict_call.args[1:]
        self.assertIn("fresh_rows=90", line)
        self.assertIn("requested_rows=100", line)
        self.assertIn("fresh_pct=90.0000", line)

    def test_runner_fresh_lineage_deduplicates_prior_row_substitutions(self):
        from scripts import run_dashboard_sync as sync

        fetched, known = sync._fresh_symbol_lineage(
            ["Symbol", "Price"],
            [
                ["A.US", 1],
                ["B.US", 2],
                ["C.US", 3],
                ["D.US", 4],
                ["FOREIGN.US", 5],
            ],
            ["A.US", "B.US", "C.US", "D.US", "MISSING.US"],
        )
        self.assertTrue(known)
        # Foreign provider rows cannot inflate the numerator when strict
        # membership is disabled or fails open.
        self.assertEqual(fetched, {"A.US", "B.US", "C.US", "D.US"})
        noncurrent = sync._merge_noncurrent_fetched_lineage(
            fetched, set(), ["A.US", "MISSING.US"]
        )
        # FW-KEEP overlaps KLG on A; the union must count A only once.
        noncurrent = sync._merge_noncurrent_fetched_lineage(
            fetched, noncurrent, ["A.US", "B.US"]
        )
        # PV-2 restores one originally fetched symbol and one symbol that was
        # absent from the fetch. Only the former reduces the numerator.
        noncurrent = sync._merge_noncurrent_fetched_lineage(
            fetched, noncurrent, ["C.US", "MISSING.US"]
        )
        self.assertEqual(noncurrent, {"A.US", "B.US", "C.US"})

        result = sync.TaskResult(
            key="global_markets",
            sheet_name="Global_Markets",
            status="success",
            start_utc="2026-10-06T00:00:00+00:00",
            rows_written=5,
            symbols_requested=5,
            _stamp_meta={
                "requested": 5,
                "pre_persist_rows": 4,
                "klg_kept": 2,
                "fresh_lineage_known": True,
                "fetched_origin": len(fetched),
                "noncurrent_fetched": len(noncurrent),
                "fetchfail_lineage_known": True,
                "ff_new_fetched": 0,
            },
        )
        self.assertEqual(
            sync._page_fresh_fetch_metrics(result),
            (1, 5, 20.0),
        )

    def test_runner_missing_modern_symbol_lineage_stays_unknown(self):
        from scripts import run_dashboard_sync as sync

        fetched, known = sync._fresh_symbol_lineage(
            ["Name", "Price"], [["Alpha", 1]]
        )
        self.assertEqual((fetched, known), (set(), False))
        result = sync.TaskResult(
            key="global_markets",
            sheet_name="Global_Markets",
            status="success",
            start_utc="2026-10-06T00:00:00+00:00",
            rows_written=10,
            symbols_requested=10,
            _stamp_meta={
                "requested": 10,
                "pre_persist_rows": 10,
                "klg_kept": 2,
                "fresh_lineage_known": False,
                "fetched_origin": 0,
                "noncurrent_fetched": 0,
            },
        )
        self.assertEqual(
            sync._page_fresh_fetch_metrics(result),
            (None, 10, None),
        )

    def test_runner_klg_stub_candidate_is_noncurrent_without_prior(self):
        from scripts import run_dashboard_sync as sync

        headers = [
            "Symbol",
            "Name",
            "Current Price",
            "EPS (TTM)",
            "P/E (TTM)",
            "Data Provider",
            "Warnings",
            "Last Updated (UTC)",
        ]
        stub = [
            "A.US", "", "", "", "", "history", "no_data_stub", ""
        ]

        class Sheets:
            def __init__(self, grid):
                self.grid = grid

            def read_values(self, *_args, **_kwargs):
                return [list(row) for row in self.grid]

        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_KLG_FETCHFAIL": "off"}
        ):
            unchanged, no_swap = sync._keep_last_good_rows(
                Sheets([headers]),
                "sheet",
                "Global_Markets",
                headers,
                [list(stub)],
            )
        self.assertEqual(unchanged, [stub])
        self.assertEqual(no_swap, [])
        self.assertEqual(sync._LAST_KLG_STUB_CANDIDATES, ["A.US"])

        fetched = {"A.US"}
        noncurrent = sync._merge_noncurrent_fetched_lineage(
            fetched, set(), sync._LAST_KLG_STUB_CANDIDATES
        )
        result = sync.TaskResult(
            key="global_markets",
            sheet_name="Global_Markets",
            status="success",
            start_utc="2026-10-06T00:00:00+00:00",
            rows_written=1,
            symbols_requested=1,
            _stamp_meta={
                "requested": 1,
                "fresh_lineage_known": True,
                "fetched_origin": 1,
                "noncurrent_fetched": len(noncurrent),
                "fetchfail_lineage_known": True,
                "ff_new_fetched": 0,
            },
        )
        self.assertEqual(sync._page_fresh_fetch_metrics(result), (0, 1, 0.0))

        class RaisingSheets:
            def read_values(self, *_args, **_kwargs):
                raise RuntimeError("prior read failed")

        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_KLG_FETCHFAIL": "off"}
        ), self.assertRaisesRegex(RuntimeError, "prior read failed"):
            sync._keep_last_good_rows(
                RaisingSheets(),
                "sheet",
                "Global_Markets",
                headers,
                [list(stub)],
            )
        # The caller consumes this in a finally block, so a Sheets exception
        # cannot erase the factual unusable-stub lineage.
        self.assertEqual(sync._LAST_KLG_STUB_CANDIDATES, ["A.US"])
        raised_noncurrent = sync._merge_noncurrent_fetched_lineage(
            fetched, set(), sync._LAST_KLG_STUB_CANDIDATES
        )
        self.assertEqual(raised_noncurrent, {"A.US"})

        good_prior = [
            "A.US", "Alpha Corp", 10.0, 1.0, 10.0, "eodhd", "", "old"
        ]
        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_KLG_FETCHFAIL": "off"}
        ):
            restored, swapped = sync._keep_last_good_rows(
                Sheets([headers, good_prior]),
                "sheet",
                "Global_Markets",
                headers,
                [list(stub)],
            )
        self.assertEqual(swapped, ["A.US"])
        self.assertEqual(restored[0][2], 10.0)
        # Candidate plus successful substitution is still one identity.
        deduped = sync._merge_noncurrent_fetched_lineage(
            fetched, set(), sync._LAST_KLG_STUB_CANDIDATES
        )
        deduped = sync._merge_noncurrent_fetched_lineage(
            fetched, deduped, swapped
        )
        self.assertEqual(deduped, {"A.US"})

    def test_runner_consumes_klg_candidates_when_sheet_read_raises(self):
        from scripts import run_dashboard_sync as sync

        headers = [
            "Symbol",
            "Name",
            "Current Price",
            "EPS (TTM)",
            "P/E (TTM)",
            "Data Provider",
            "Warnings",
            "Last Updated (UTC)",
        ]
        stub = [
            "A.US", "", "", "", "", "history", "no_data_stub", ""
        ]

        class Backend:
            async def post_json(self, _path, _payload):
                return {
                    "headers": list(headers),
                    "rows_matrix": [list(stub)],
                }, None, 200

        class RaisingWriter:
            def _get_service(self):
                return object()

            def read_values(self, *_args, **_kwargs):
                raise RuntimeError("prior read failed")

            def write_table(
                self, _sheet_id, _page, _start, _headers, rows
            ):
                return len(rows)

            def clear_from(self, *_args, **_kwargs):
                return None

        task = sync.TaskSpec(
            "GLOBAL_MARKETS", "Global_Markets", "analysis", max_symbols=1
        )
        env = {
            "TFB_MARKET_SYMBOL_READBACK": "0",
            "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
            "TFB_SYNC_PERSISTENCE_HARD": "0",
            "TFB_SYNC_ROW_ID_FIREWALL": "0",
            "TFB_SYNC_NAME_DEDUP_MODE": "off",
            "TFB_SYNC_OHLC_LAKE": "0",
            "TFB_SYNC_FALSE_GREEN_SCREEN": "0",
            "TFB_SYNC_STATUS_STAMP": "0",
            "TFB_SYNC_FETCHFAIL_TRUTH": "off",
            "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
            "TFB_SYNC_COHERENCE_TRIPWIRE": "0",
        }
        with mock.patch.dict("os.environ", env), mock.patch.object(
            sync, "_read_symbols", return_value=["A.US"]
        ):
            result = asyncio.run(
                sync._run_one_task(
                    task,
                    "sheet",
                    "A1",
                    -1,
                    False,
                    False,
                    Backend(),
                    RaisingWriter(),
                )
            )

        self.assertTrue(result._stamp_meta["fresh_lineage_known"])
        self.assertEqual(result._stamp_meta["fetched_origin"], 1)
        self.assertEqual(result._stamp_meta["noncurrent_fetched"], 1)
        self.assertEqual(sync._page_fresh_fetch_metrics(result), (0, 1, 0.0))
        self.assertTrue(
            any(
                "KEEP-LAST-GOOD" in warning and "prior read failed" in warning
                for warning in result.warnings
            )
        )

    def test_runner_freshness_with_symbol_persistence_disabled(self):
        from scripts import run_dashboard_sync as sync

        headers = [
            "Symbol", "Name", "Current Price", "EPS (TTM)", "P/E (TTM)",
            "Data Provider", "Warnings", "Last Updated (UTC)",
        ]
        symbols = ["A.US", "B.US", "C.US", "D.US"]
        healthy = [
            [symbol, symbol + " Corp", 10, 1, 10, "eodhd", "", ""]
            for symbol in symbols
        ]
        failed = [list(row) for row in healthy]
        failed[0][6] = "fetch_failed:timeout"
        stubbed = [list(row) for row in healthy]
        stubbed[0] = ["A.US", "", "", "", "", "history", "no_data_stub", ""]

        class Backend:
            def __init__(self, rows):
                self.rows = rows

            async def post_json(self, _path, _payload):
                return {
                    "headers": list(headers),
                    "rows_matrix": [list(row) for row in self.rows],
                }, None, 200

        class Writer:
            def __init__(self):
                self.written = []

            def _get_service(self):
                return object()

            def read_values(self, *_args, **_kwargs):
                return []

            def write_table(self, _sheet_id, _page, _start, _headers, rows):
                self.written = [list(row) for row in rows]
                return len(rows)

        task = sync.TaskSpec(
            "GLOBAL_MARKETS", "Global_Markets", "analysis", max_symbols=4
        )
        env = {
            "TFB_MARKET_SYMBOL_READBACK": "0",
            "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
            "TFB_SYNC_SYMBOL_PERSISTENCE": "0",
            "TFB_SYNC_PERSISTENCE_HARD": "0",
            "TFB_SYNC_KEEP_LAST_GOOD": "1",
            "TFB_SYNC_KLG_FETCHFAIL": "off",
            "TFB_SYNC_ROW_ID_FIREWALL": "0",
            "TFB_SYNC_NAME_DEDUP_MODE": "off",
            "TFB_SYNC_OHLC_LAKE": "0",
            "TFB_SYNC_FALSE_GREEN_SCREEN": "0",
            "TFB_SYNC_STATUS_STAMP": "0",
            "TFB_SYNC_FETCHFAIL_TRUTH": "off",
            "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
            "TFB_SYNC_COHERENCE_TRIPWIRE": "0",
            "TFB_SYNC_EODHD_QUOTA_GUARD": "off",
            "TFB_SYNC_STALE_SKIP_RED": "1",
            "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
            "TFB_AUDIT_MIN_FRESH_PCT": "95",
        }
        cases = (
            ("healthy", healthy, 4, 0, "ok"),
            ("missing", healthy[:3], 3, 0, "blocked"),
            ("fetchfailed", failed, 3, 0, "blocked"),
            ("unusable", stubbed, 3, 1, "blocked"),
        )
        for name, rows, fresh, noncurrent, audit_status in cases:
            with self.subTest(name=name), mock.patch.dict("os.environ", env), \
                    mock.patch.object(sync, "_read_symbols", return_value=symbols), \
                    mock.patch.object(sync, "_persist_missing_symbol_rows") as persist:
                writer = Writer()
                result = asyncio.run(sync._run_one_task(
                    task, "sheet", "A1", -1, False, False, Backend(rows), writer
                ))
                self.assertEqual(result.status, "success", result.error)
                self.assertEqual(writer.written, rows)
                persist.assert_not_called()
                self.assertTrue(result._stamp_meta["fresh_lineage_known"])
                self.assertEqual(result._stamp_meta["pre_persist_rows"], len(rows))
                self.assertEqual(result._stamp_meta["noncurrent_fetched"], noncurrent)
                self.assertEqual(
                    sync._page_fresh_fetch_metrics(result),
                    (fresh, 4, 100.0 * fresh / 4),
                )
                with mock.patch.object(sync.logger, "info") as info, \
                        mock.patch.object(sync, "_page_newest_stamp_age_h", return_value=1):
                    sync._apply_stale_skip_escalation([result], writer, "sheet")
                verdict = next(
                    call.args[0] % call.args[1:]
                    for call in info.call_args_list
                    if "fresh_rows=" in call.args[0]
                )
                other_pages = "".join(
                    self._line(page, fresh=10, requested=10)
                    for page in CRITICAL_MARKET_PAGES
                    if page != "Global_Markets"
                )
                audit = self._audit(other_pages + verdict + "\n")
                self.assertEqual(audit.status, audit_status)

    def test_runner_lineage_drives_page_status_and_upstream_coverage(self):
        from scripts import run_dashboard_sync as sync

        result = sync.TaskResult(
            key="global_markets",
            sheet_name="Global_Markets",
            status="success",
            start_utc="2026-10-06T00:00:00+00:00",
            end_utc="2026-10-06T00:01:00+00:00",
            rows_written=100,
            symbols_requested=100,
            _stamp_meta={
                "requested": 100,
                "pre_persist_rows": 100,
                "klg_kept": 0,
                "fresh_lineage_known": True,
                "fetched_origin": 100,
                "noncurrent_fetched": 6,
                "fetchfail_lineage_known": True,
                "ff_new_fetched": 0,
                # Exact lineage ignores the later raw census.
                "ff_new": 6,
                "ff_carried": 0,
            },
        )
        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_FETCHFAIL_TRUTH": "off"}
        ), mock.patch.object(
            sync, "_page_newest_stamp_age_h", return_value=1.0
        ), mock.patch.object(
            sync, "_stale_skip_red_enabled", return_value=True
        ), mock.patch.object(sync.logger, "info") as log_info:
            status_row = sync._status_stamp_row("Global_Markets", result, 10)
            upstream = sync._uv_page_state(result)
            sync._apply_stale_skip_escalation([result], None, "sheet")

        verdict_call = next(
            item for item in log_info.call_args_list
            if len(item.args) > 1 and item.args[1] == sync._PAGE_VERDICT_TAG
        )
        page_line = verdict_call.args[0] % verdict_call.args[1:]
        self.assertIn("fresh_rows=94", page_line)
        self.assertIn("fresh_pct=94.0000", page_line)
        self.assertEqual(status_row[2], "PARTIAL_FRESH")
        self.assertIn("fresh=94", status_row[3])
        self.assertIn("noncurrent=6", status_row[3])
        self.assertIn("fresh_cov=94.0%", status_row[3])
        self.assertEqual(upstream, ("STALE_COV", 94.0))

    def test_runner_fetchfail_lineage_unions_old_stamps_and_deduplicates(self):
        from scripts import run_dashboard_sync as sync

        headers = ["Symbol", "Warnings", "Last Updated (UTC)"]
        t0 = 1_800_000_000.0
        origin_rows = [
            # An old provider stamp is still a failed row in this response.
            ["A.US", "fetch_failed:HTTP 402", "2026-01-01T00:00:00+00:00"],
            # Blank/unparseable stamps are also factual fetch failures.
            ["B.US", "fetch_failed:timeout", "not-a-time"],
            ["C.US", "", ""],
            # An unsolicited failure is outside the requested-origin base.
            ["FOREIGN.US", "fetch_failed:HTTP 500", ""],
        ]
        fetched, origin_known = sync._fresh_symbol_lineage(
            headers, origin_rows, ["A.US", "B.US", "C.US", "MISSING.US"]
        )
        fetched_failure, failure_known = sync._fetchfail_symbol_lineage(
            headers,
            origin_rows,
            fetched,
        )
        self.assertTrue(origin_known)
        self.assertTrue(failure_known)
        self.assertEqual(fetched, {"A.US", "B.US", "C.US"})
        self.assertEqual(fetched_failure, {"A.US", "B.US"})
        alias_failure, alias_known = sync._fetchfail_symbol_lineage(
            ["Symbol", "Flags"],
            [["A.US", "fetch_failed:HTTP 503"]],
            {"A.US"},
        )
        self.assertTrue(alias_known)
        self.assertEqual(alias_failure, {"A.US"})

        # B is restored later and MISSING is appended from prior data. The
        # ordinary noncurrent set counts B once and MISSING zero times; the
        # disjoint fetchfail count then retains only A.
        noncurrent = sync._merge_noncurrent_fetched_lineage(
            fetched, set(), ["B.US", "MISSING.US"]
        )
        exact_fetchfail = fetched_failure - noncurrent
        self.assertEqual(noncurrent, {"B.US"})
        self.assertEqual(exact_fetchfail, {"A.US"})

        final_census = sync._fetchfail_count_rows(
            headers,
            origin_rows
            + [["MISSING.US", "fetch_failed:HTTP 404", ""]],
            t0,
        )
        self.assertGreaterEqual(final_census["ff_new"], 2)
        result = sync.TaskResult(
            key="global_markets",
            sheet_name="Global_Markets",
            status="success",
            start_utc="2026-10-06T00:00:00+00:00",
            rows_written=4,
            symbols_requested=4,
            _stamp_meta={
                "requested": 4,
                "pre_persist_rows": 3,
                "klg_kept": 0,
                "fresh_lineage_known": True,
                "fetched_origin": len(fetched),
                "noncurrent_fetched": len(noncurrent),
                "fetchfail_lineage_known": True,
                "ff_new_fetched": len(exact_fetchfail),
                # The initially missing prior row appears new to the legacy
                # final census. Exact lineage must ignore that later count.
                **final_census,
            },
        )
        self.assertEqual(sync._page_fresh_fetch_metrics(result), (1, 4, 25.0))

        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_FETCHFAIL_TRUTH": "off"}
        ):
            off_status = sync._status_stamp_row("Global_Markets", result, 10)
            off_feed = sync._uv_page_state(result)
        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_FETCHFAIL_TRUTH": "observe"}
        ):
            observe_status = sync._status_stamp_row(
                "Global_Markets", result, 10
            )
            observe_feed = sync._uv_page_state(result)
        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_FETCHFAIL_TRUTH": "enforce"}
        ):
            enforce_status = sync._status_stamp_row(
                "Global_Markets", result, 10
            )
            enforce_feed = sync._uv_page_state(result)

        self.assertIn("fresh=1", off_status[3])
        self.assertIn("fetchfail=1/0", off_status[3])
        self.assertIn("fresh=1", observe_status[3])
        # The old-stamped A origin is exact-new by provenance, never also
        # reported as carried; appended prior failures do not affect coverage.
        self.assertIn("fetchfail=1/0", observe_status[3])
        self.assertNotIn("would_cov=", observe_status[3])
        self.assertIn("fresh=1", enforce_status[3])
        self.assertIn("fetchfail=1/0", enforce_status[3])
        self.assertEqual(off_feed, ("STALE_COV", 25.0))
        self.assertEqual(observe_feed, ("STALE_COV", 25.0))
        self.assertEqual(enforce_feed, ("STALE_COV", 25.0))

    def test_runner_audit_coverage_counts_new_fetch_failures_as_not_fresh(self):
        from scripts import run_dashboard_sync as sync

        result = sync.TaskResult(
            key="global_markets",
            sheet_name="Global_Markets",
            status="success",
            start_utc="2026-10-06T00:00:00+00:00",
            rows_written=100,
            symbols_requested=100,
            _stamp_meta={
                "requested": 100,
                "pre_persist_rows": 100,
                "klg_kept": 0,
                "ff_new": 10,
                "ff_carried": 0,
            },
        )
        # Production currently observes fetch-fail truth. Audit telemetry is
        # factual regardless of that display rollout mode.
        with mock.patch.dict(
            "os.environ", {"TFB_SYNC_FETCHFAIL_TRUTH": "observe"}
        ):
            fresh, requested, pct = sync._page_fresh_fetch_metrics(result)
        self.assertEqual((fresh, requested, pct), (90, 100, 90.0))

    def test_scheduled_workflow_arms_full_fetch_gate(self):
        workflow = (
            Path(__file__).parents[1] / ".github/workflows/sync_outcome_audit.yml"
        ).read_text(encoding="utf-8")
        audit_job = workflow.split("  audit-scheduled-run:", 1)[1].split(
            "    steps:", 1
        )[0]
        self.assertIn('TFB_AUDIT_REQUIRE_FULL_FETCH: "1"', audit_job)
        self.assertIn('TFB_AUDIT_MIN_FRESH_PCT: "95"', audit_job)

    def test_manual_recovery_arms_full_fetch_gate_for_plan_and_page_audit(self):
        workflow = (
            Path(__file__).parents[1]
            / ".github/workflows/page_refresh_recovery.yml"
        ).read_text(encoding="utf-8")
        plan_job = workflow.split("  plan:", 1)[1].split(
            "  recover-page:", 1
        )[0]
        recover_job = workflow.split("  recover-page:", 1)[1].split(
            "    steps:", 1
        )[0]
        for name, job in (("plan", plan_job), ("recover-page", recover_job)):
            with self.subTest(job=name):
                self.assertIn("TFB_AUDIT_REQUIRE_FULL_FETCH: '1'", job)
                self.assertIn("TFB_AUDIT_MIN_FRESH_PCT: '95'", job)

    def test_missing_artifact_directory_raises(self):
        with self.assertRaises(OSError):
            audit_artifacts(Path("/definitely/not/present"))

    def test_canonical_preference_is_local_to_each_artifact_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            canonical_artifact = root / "tadawul-sync-logs-123-core-pages"
            fallback_artifact = root / "tadawul-sync-logs-123-global-markets"
            canonical_artifact.mkdir()
            fallback_artifact.mkdir()
            (canonical_artifact / "sync_execution.log").write_text(
                self._line("Market_Leaders")
                + self._line("Commodities_FX"),
                encoding="utf-8",
            )
            # A duplicate timestamp log in the same artifact must lose to its
            # canonical copy.
            (canonical_artifact / "sync_20261006_010000.log").write_text(
                self._line("Market_Leaders", "failed", 0),
                encoding="utf-8",
            )
            # This artifact was interrupted before the canonical copy step;
            # its timestamped evidence must still be read.
            (fallback_artifact / "sync_20261006_010100.log").write_text(
                self._line("Global_Markets")
                + self._line("Mutual_Funds"),
                encoding="utf-8",
            )
            result = audit_artifacts(root)

        self.assertEqual(result.status, "ok")
        self.assertFalse(result.missing_pages)
        self.assertEqual(len(result.log_files), 2)
        self.assertTrue(any(path.endswith("sync_execution.log") for path in result.log_files))
        self.assertTrue(any(path.endswith("sync_20261006_010100.log") for path in result.log_files))
        self.assertFalse(any(path.endswith("sync_20261006_010000.log") for path in result.log_files))

    # (2026-10-06) RECOVERY EVIDENCE WINS OVER THE FAILED LEG.
    # sync_outcome_audit.yml now downloads the recover-missing-market-pages
    # job's artifacts into 'downloaded-sync-artifacts/zz-recovery/'. The
    # ordering contract this relies on: _candidate_logs() sorts the log paths
    # and latest_by_page keeps the LAST verdict per page, so a 'zz-recovery'
    # directory sorts after 'tadawul-sync-logs-*' and the recovered page's
    # verdict replaces the failed one. Live shape: run 37370684206, where the
    # global-markets leg was cancelled before start and the recovery job then
    # wrote 6,609 Global_Markets rows.
    def _audit_two_roots(self, primary: str, recovery: str):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            leg = root / "tadawul-sync-logs-123-core-pages"
            leg.mkdir()
            (leg / "sync_execution.log").write_text(primary, encoding="utf-8")
            rec = root / "zz-recovery" / "page-refresh-123-global-markets"
            rec.mkdir(parents=True)
            (rec / "sync_execution.log").write_text(recovery, encoding="utf-8")
            return audit_artifacts(root)

    def _audit_recovery_cycles(
        self, primary: str, cycle_one: str, cycle_two: str
    ):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            leg = root / "tadawul-sync-logs-123-core-pages"
            leg.mkdir()
            (leg / "sync_execution.log").write_text(primary, encoding="utf-8")
            rec = root / "zz-recovery" / "page-refresh-123-global-markets"
            rec.mkdir(parents=True)
            (rec / "sync_execution.log").write_text(cycle_one, encoding="utf-8")
            (rec / "cycle2").mkdir()
            (rec / "cycle2" / "sync_execution.log").write_text(
                cycle_two, encoding="utf-8"
            )
            return audit_artifacts(root)

    def test_recovery_evidence_replaces_failed_leg_verdict(self):
        primary = "".join(
            self._line(
                page,
                "skipped" if page == "Global_Markets" else "success",
                0 if page == "Global_Markets" else 10,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        recovery = self._line("Global_Markets", "success", 6609)
        result = self._audit_two_roots(primary, recovery)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.failed_pages)
        self.assertFalse(result.missing_pages)

    def test_clean_recovery_replaces_primary_incomplete_fetch_evidence(self):
        primary = self._incomplete_fetch_log()
        recovery = self._line(
            "Global_Markets",
            "success",
            6609,
            fresh=6609,
            requested=6609,
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit_two_roots(primary, recovery)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.failed_pages)
        self.assertFalse(result.incomplete_pages)
        evidence = next(
            item for item in result.fetch_evidence
            if item["page"] == "Global_Markets"
        )
        self.assertIn("zz-recovery", evidence["source"])
        self.assertEqual(evidence["fresh_pct"], 100.0)

    def test_incomplete_recovery_remains_blocking(self):
        primary = "".join(self._line(page) for page in CRITICAL_MARKET_PAGES)
        recovery = (
            "[v6.64.0 TIME-BUDGET] Global_Markets: budget 3600s exhausted "
            "after 70/239 batches\n"
            "[v6.64.0 FLOOR-MERGE] Partial fetch on 'Global_Markets': "
            "70 fresh row(s) for 239 requested (29% coverage; last-good rows retained)\n"
            + self._line("Global_Markets", "success", 6609)
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit_two_roots(primary, recovery)
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.incomplete_pages, ("Global_Markets",))
        self.assertIn("Global_Markets", result.failed_pages)
        self.assertIn("zz-recovery", result.fetch_evidence[0]["source"])

    def test_later_recovery_cycle_success_wins(self):
        primary = "".join(self._line(page) for page in CRITICAL_MARKET_PAGES)
        result = self._audit_recovery_cycles(
            primary,
            self._line("Global_Markets", "failed", 0),
            self._line("Global_Markets", "success", 6609),
        )
        self.assertEqual(result.status, "ok")
        verdict = next(
            item for item in result.verdicts if item.page == "Global_Markets"
        )
        self.assertIn("cycle2", verdict.source)

    def test_later_recovery_cycle_failure_wins(self):
        primary = "".join(self._line(page) for page in CRITICAL_MARKET_PAGES)
        result = self._audit_recovery_cycles(
            primary,
            self._line("Global_Markets", "success", 6609),
            self._line("Global_Markets", "failed", 0),
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("Global_Markets", result.failed_pages)
        verdict = next(
            item for item in result.verdicts if item.page == "Global_Markets"
        )
        self.assertIn("cycle2", verdict.source)

    def test_later_clean_cycle_clears_incomplete_cycle_evidence(self):
        primary = "".join(self._line(page) for page in CRITICAL_MARKET_PAGES)
        cycle_one = (
            "[v6.64.0 TIME-BUDGET] Global_Markets: budget 3600s exhausted "
            "after 70/239 batches\n"
            "[v6.64.0 FLOOR-MERGE] Partial fetch on 'Global_Markets': "
            "70 fresh row(s) for 239 requested (29% coverage; retained)\n"
            + self._line("Global_Markets", "success", 6609)
        )
        cycle_two = self._line(
            "Global_Markets",
            "success",
            6609,
            fresh=6609,
            requested=6609,
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit_recovery_cycles(
                primary, cycle_one, cycle_two
            )
        self.assertEqual(result.status, "ok")
        self.assertFalse(result.incomplete_pages)
        evidence = next(
            item for item in result.fetch_evidence
            if item["page"] == "Global_Markets"
        )
        self.assertIn("cycle2", evidence["source"])
        self.assertEqual(evidence["fresh_pct"], 100.0)

    def test_later_incomplete_cycle_replaces_clean_cycle_evidence(self):
        primary = "".join(self._line(page) for page in CRITICAL_MARKET_PAGES)
        cycle_one = self._line(
            "Global_Markets",
            "success",
            6609,
            fresh=6609,
            requested=6609,
        )
        cycle_two = self._line(
            "Global_Markets",
            "success",
            6609,
            fresh=6000,
            requested=6609,
        )
        with mock.patch.dict(
            "os.environ",
            {
                "TFB_AUDIT_REQUIRE_FULL_FETCH": "1",
                "TFB_AUDIT_MIN_FRESH_PCT": "95",
            },
        ):
            result = self._audit_recovery_cycles(
                primary, cycle_one, cycle_two
            )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.incomplete_pages, ("Global_Markets",))
        evidence = next(
            item for item in result.fetch_evidence
            if item["page"] == "Global_Markets"
        )
        self.assertIn("cycle2", evidence["source"])
        self.assertEqual(evidence["fresh_rows"], 6000)

    def test_recovery_that_also_failed_still_blocks(self):
        primary = "".join(
            self._line(
                page,
                "skipped" if page == "Global_Markets" else "success",
                0 if page == "Global_Markets" else 10,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        recovery = self._line("Global_Markets", "success", 0)
        result = self._audit_two_roots(primary, recovery)
        self.assertIn("Global_Markets", result.failed_pages)
        self.assertEqual(result.exit_code, 2)


if __name__ == "__main__":
    unittest.main()
