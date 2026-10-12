from __future__ import annotations

import json
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.run_inline_page_recovery import run_inline_recovery


class InlinePageRecoveryTests(unittest.TestCase):
    @staticmethod
    def _verdict(page: str, status: str = "success", rows: int = 10) -> str:
        return (
            "2026-07-29 12:00:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v6.26.0] page={page} status={status} "
            f"rows_written={rows} newest_stamp_age_h=1 reason=test\n"
        )

    def _source_root(self, root: Path, *, failed_page: str | None = None) -> Path:
        source = root / "source"
        source.mkdir()
        pages = (
            "Market_Leaders",
            "Global_Markets",
            "Commodities_FX",
            "Mutual_Funds",
        )
        text = "".join(
            self._verdict(
                page,
                "skipped" if page == failed_page else "success",
                0 if page == failed_page else 10,
            )
            for page in pages
        )
        (source / "sync_execution.log").write_text(text, encoding="utf-8")
        return source

    def test_no_recovery_needed_is_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rc = run_inline_recovery(
                source_root=self._source_root(root),
                backend="https://example.test",
                sheet_id="sheet",
                evidence_root=root / "evidence",
                plan_out=root / "plan.json",
                summary_out=root / "summary.json",
            )
            self.assertEqual(rc, 0)
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "ok")
            self.assertFalse(summary["needs_recovery"])

    def test_targeted_page_process_has_independent_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_root(root, failed_page="Commodities_FX")

            def fake_stream(command, *, env, log_path):
                self.assertIn("COMMODITIES_FX", command)
                self.assertEqual(env["TFB_SYNC_PAGE_ORDER"], "Commodities_FX")
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(
                    self._verdict("Commodities_FX", "success", 453),
                    encoding="utf-8",
                )
                return 0

            with mock.patch(
                "scripts.run_inline_page_recovery._stream_process",
                side_effect=fake_stream,
            ):
                rc = run_inline_recovery(
                    source_root=source,
                    backend="https://example.test",
                    sheet_id="sheet",
                    evidence_root=root / "evidence",
                    plan_out=root / "plan.json",
                    summary_out=root / "summary.json",
                )

            self.assertEqual(rc, 0)
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["retry_pages"], ["Commodities_FX"])
            self.assertTrue(summary["results"][0]["passed"])
            self.assertTrue(
                (root / "evidence" / "commodities-fx" / "page-audit.json").exists()
            )

    def test_zero_exit_does_not_override_failed_page_verdict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_root(root, failed_page="Global_Markets")

            def fake_stream(command, *, env, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(
                    self._verdict("Global_Markets", "skipped", 0),
                    encoding="utf-8",
                )
                return 0

            with mock.patch(
                "scripts.run_inline_page_recovery._stream_process",
                side_effect=fake_stream,
            ):
                rc = run_inline_recovery(
                    source_root=source,
                    backend="https://example.test",
                    sheet_id="sheet",
                    evidence_root=root / "evidence",
                    plan_out=root / "plan.json",
                    summary_out=root / "summary.json",
                )

            self.assertEqual(rc, 2)
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "blocked")
            self.assertEqual(summary["failed_pages"], ["Global_Markets"])

    def test_actual_child_command_retains_distinct_cycle_reports_and_declared_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_root(root, failed_page="Commodities_FX")
            commands = []
            def fake_stream(command, *, env, log_path):
                commands.append(command)
                report_path = Path(command[command.index("--json-out") + 1])
                self.assertEqual(report_path.parent, log_path.parent)
                self.assertEqual(env["TFB_RANK_OVERALL_PAGE"], "enforce")
                self.assertEqual(env["TFB_WARNINGS_COMPACT"], "observe")
                cycle = len(commands)
                result = {"sheet_name": "Commodities_FX", "version": "6.67.0",
                          "request_id": f"cycle-{cycle}", "status": "skipped", "rows_written": 0}
                if cycle == 2:
                    result.update(status="success", rows_written=453,
                        write_receipt={"written_payload_sha8": "0123abcd", "rows": 453,
                                       "evidence": "successful_RAW_update", "live_readback": False})
                log_path.parent.mkdir(parents=True, exist_ok=True)
                report_path.write_text(json.dumps({"results": [result]}), encoding="utf-8")
                log_path.write_text(self._verdict("Commodities_FX", "skipped" if cycle == 1 else "success",
                                                0 if cycle == 1 else 453), encoding="utf-8")
                return 0
            with mock.patch.dict(os.environ, {"TFB_INLINE_RECOVERY_MAX_CYCLES": "2",
                    "TFB_INLINE_RECOVERY_QUOTA_GUARD": "off", "TFB_AUDIT_REQUIRE_FULL_FETCH": "0",
                    "TFB_RANK_OVERALL_PAGE": "enforce", "TFB_WARNINGS_COMPACT": "observe"}), mock.patch(
                    "scripts.run_inline_page_recovery._stream_process", side_effect=fake_stream):
                code = run_inline_recovery(source_root=source, backend="https://example.test", sheet_id="sheet",
                    evidence_root=root / "evidence", plan_out=root / "plan.json", summary_out=root / "summary.json")
            self.assertEqual(code, 0)
            summary = json.loads((root / "summary.json").read_text())
            self.assertEqual(len(commands), 2)
            first, second = [item["runner_report"] for item in summary["results"]]
            self.assertNotEqual(first["path"], second["path"])
            self.assertIn("cycle2", second["path"])
            for item in (first, second):
                self.assertEqual(item["status"], "available")
                self.assertFalse(item["live_readback"])
                self.assertEqual(item["sha256"], hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest())
            self.assertNotIn("write_receipt", first)
            self.assertEqual(second["write_receipt"]["written_payload_sha8"], "0123abcd")

    def test_missing_report_cannot_reuse_an_older_invocation_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_root(root, failed_page="Commodities_FX")
            report = root / "evidence" / "commodities-fx" / "sync_runner_receipt.json"
            report.parent.mkdir(parents=True)
            report.write_text('{"results":[{"sheet_name":"Commodities_FX","write_receipt":{"old":true}}]}')
            def fake_stream(command, *, env, log_path):
                self.assertEqual(Path(command[command.index("--json-out") + 1]), report)
                self.assertFalse(report.exists())
                log_path.write_text(self._verdict("Commodities_FX", "success", 453))
                return 0
            with mock.patch.dict(os.environ, {"TFB_INLINE_RECOVERY_MAX_CYCLES": "1", "TFB_INLINE_RECOVERY_QUOTA_GUARD": "off"}), mock.patch(
                    "scripts.run_inline_page_recovery._stream_process", side_effect=fake_stream):
                code = run_inline_recovery(source_root=source, backend="https://example.test", sheet_id="sheet",
                    evidence_root=root / "evidence", plan_out=root / "plan.json", summary_out=root / "summary.json")
            self.assertEqual(code, 0)
            item = json.loads((root / "summary.json").read_text())["results"][0]["runner_report"]
            self.assertEqual(item["status"], "missing")
            self.assertNotIn("write_receipt", item)

    def test_malformed_child_reports_never_become_update_or_live_readback_proof(self):
        receipt = {"written_payload_sha8": "0123abcd", "rows": 453,
                   "evidence": "successful_RAW_update", "live_readback": False}
        good = {"sheet_name": "Commodities_FX", "version": "6.67.0", "status": "success",
                "rows_written": 453, "write_receipt": receipt}
        payloads = ["not json", json.dumps({"results": []}), json.dumps({"results": [good, good]}),
                    json.dumps({"results": [dict(good, sheet_name="Global_Markets")]}),
                    json.dumps({"results": [dict(good, write_receipt=dict(receipt, live_readback=True))]}),
                    json.dumps({"results": [dict(good, write_receipt=dict(receipt, written_payload_sha8="bad"))]}),
                    json.dumps({"results": [dict(good, write_receipt=dict(receipt, rows=True))]})]
        for text in payloads:
            with self.subTest(payload=text), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = self._source_root(root, failed_page="Commodities_FX")
                def fake_stream(command, *, env, log_path):
                    log_path.parent.mkdir(parents=True, exist_ok=True)
                    log_path.write_text(self._verdict("Commodities_FX", "success", 453))
                    Path(command[command.index("--json-out") + 1]).write_text(text)
                    return 0
                with mock.patch.dict(os.environ, {"TFB_INLINE_RECOVERY_MAX_CYCLES": "1", "TFB_INLINE_RECOVERY_QUOTA_GUARD": "off"}), mock.patch(
                        "scripts.run_inline_page_recovery._stream_process", side_effect=fake_stream):
                    code = run_inline_recovery(source_root=source, backend="https://example.test", sheet_id="sheet",
                        evidence_root=root / "evidence", plan_out=root / "plan.json", summary_out=root / "summary.json")
                self.assertEqual(code, 0)
                item = json.loads((root / "summary.json").read_text())["results"][0]["runner_report"]
                self.assertEqual(item["status"], "invalid")
                self.assertFalse(item["live_readback"])
                self.assertNotIn("write_receipt", item)


if __name__ == "__main__":
    unittest.main()
