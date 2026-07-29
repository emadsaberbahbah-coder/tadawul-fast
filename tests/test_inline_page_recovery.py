from __future__ import annotations

import json
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


if __name__ == "__main__":
    unittest.main()
