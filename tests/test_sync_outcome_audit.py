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
