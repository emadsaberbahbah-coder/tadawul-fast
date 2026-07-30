from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.audit_sync_outcome import CRITICAL_MARKET_PAGES, audit_artifacts


class SyncOutcomeAuditTests(unittest.TestCase):
    def _audit(self, text: str, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / "artifact"
            artifact.mkdir()
            (artifact / "sync_execution.log").write_text(text, encoding="utf-8")
            return audit_artifacts(root, **kwargs)

    @staticmethod
    def _legacy_line(page: str, status: str = "success", rows: int = 10) -> str:
        return (
            "2026-07-29 01:00:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v6.26.0] page={page} status={status} "
            f"rows_written={rows} newest_stamp_age_h=1 reason=test\n"
        )

    @staticmethod
    def _v2_line(
        page: str,
        *,
        status: str = "success",
        requested: int = 100,
        fresh: int = 100,
        preserved: int = 0,
        stale: object = 0,
        stubs: int = 0,
        identity_failures: int = 0,
        provider_failures: object = 0,
        rows_written: int = 100,
    ) -> str:
        coverage = 100.0 * fresh / requested if requested else 0.0
        return (
            "2026-07-30 01:00:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v2.0] page={page} status={status} "
            f"requested={requested} fresh={fresh} preserved={preserved} "
            f"stale={stale} stubs={stubs} identity_failures={identity_failures} "
            f"provider_failures={provider_failures} coverage_pct={coverage:.2f} "
            "oldest_source_age_h=24.0 newest_source_age_h=1.0 "
            f"api_units=NA rows_written={rows_written}\n"
        )

    def test_legacy_pages_pass_only_in_shadow_and_are_not_enforcement_ready(self):
        result = self._audit(
            "".join(self._legacy_line(page) for page in CRITICAL_MARKET_PAGES)
        )
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.enforcement_ready)
        self.assertEqual(result.incomplete_pages, CRITICAL_MARKET_PAGES)

    def test_enforce_v2_blocks_legacy_pages(self):
        result = self._audit(
            "".join(self._legacy_line(page) for page in CRITICAL_MARKET_PAGES),
            enforce_v2=True,
        )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.failed_pages, CRITICAL_MARKET_PAGES)

    def test_all_v2_pages_with_adequate_freshness_pass(self):
        result = self._audit(
            "".join(self._v2_line(page) for page in CRITICAL_MARKET_PAGES)
        )
        self.assertEqual(result.status, "ok")
        self.assertTrue(result.enforcement_ready)
        self.assertFalse(result.incomplete_pages)
        self.assertEqual(result.v2_pages, CRITICAL_MARKET_PAGES)

    def test_incomplete_v2_core_passes_shadow_but_not_enforcement(self):
        text = "".join(
            self._v2_line(page, stale="NA", provider_failures="NA")
            for page in CRITICAL_MARKET_PAGES
        )
        shadow = self._audit(text)
        self.assertEqual(shadow.status, "ok")
        self.assertFalse(shadow.enforcement_ready)
        self.assertEqual(shadow.incomplete_pages, CRITICAL_MARKET_PAGES)

        enforced = self._audit(text, enforce_v2=True)
        self.assertEqual(enforced.status, "blocked")
        self.assertEqual(enforced.failed_pages, CRITICAL_MARKET_PAGES)

    def test_low_coverage_v2_page_blocks_even_in_shadow_rollout(self):
        text = "".join(
            self._v2_line(
                page,
                fresh=90 if page == "Global_Markets" else 100,
                preserved=10 if page == "Global_Markets" else 0,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertIn("Global_Markets", result.failed_pages)
        verdict = next(v for v in result.verdicts if v.page == "Global_Markets")
        self.assertIn("fresh_coverage_below_threshold", verdict.failure_reasons)

    def test_identity_failure_v2_page_blocks(self):
        text = "".join(
            self._v2_line(
                page,
                identity_failures=1 if page == "Market_Leaders" else 0,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertIn("Market_Leaders", result.failed_pages)

    def test_partial_v2_page_can_pass_at_95_percent(self):
        text = "".join(
            self._v2_line(
                page,
                status="partial" if page == "Mutual_Funds" else "success",
                fresh=95 if page == "Mutual_Funds" else 100,
                preserved=5 if page == "Mutual_Funds" else 0,
            )
            for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertEqual(result.status, "ok")

    def test_success_with_zero_rows_blocks(self):
        text = "".join(
            self._legacy_line(page, "success", 0 if page == "Market_Leaders" else 10)
            for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertIn("Market_Leaders", result.failed_pages)

    def test_missing_required_page_blocks(self):
        text = "".join(self._v2_line(page) for page in CRITICAL_MARKET_PAGES[:-1])
        result = self._audit(text)
        self.assertEqual(result.missing_pages, ("Mutual_Funds",))

    def test_latest_verdict_for_page_wins(self):
        text = self._legacy_line("Global_Markets", "success", 10)
        text += self._v2_line("Global_Markets", fresh=80, preserved=20)
        text += "".join(
            self._v2_line(page)
            for page in CRITICAL_MARKET_PAGES
            if page != "Global_Markets"
        )
        result = self._audit(text)
        self.assertIn("Global_Markets", result.failed_pages)
        verdict = next(v for v in result.verdicts if v.page == "Global_Markets")
        self.assertEqual(verdict.evidence_version, "2.0")

    def test_force_refetch_evidence_is_counted(self):
        text = "[FORCE-REFETCH] symbol=BK provider=eodhd\n" + "".join(
            self._v2_line(page) for page in CRITICAL_MARKET_PAGES
        )
        result = self._audit(text)
        self.assertEqual(result.force_refetch_evidence_lines, 1)

    def test_missing_artifact_directory_raises(self):
        with self.assertRaises(OSError):
            audit_artifacts(Path("/definitely/not/present"))


if __name__ == "__main__":
    unittest.main()
