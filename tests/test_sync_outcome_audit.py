from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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
    def _line(page: str, status: str = "success", rows: int = 10) -> str:
        return (
            "2026-07-29 01:00:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v6.26.0] page={page} status={status} "
            f"rows_written={rows} newest_stamp_age_h=1 reason=test\n"
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

    def test_missing_artifact_directory_raises(self):
        with self.assertRaises(OSError):
            audit_artifacts(Path("/definitely/not/present"))


if __name__ == "__main__":
    unittest.main()
