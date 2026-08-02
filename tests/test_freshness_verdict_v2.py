from __future__ import annotations

import unittest

from scripts.freshness_verdict_v2 import (
    assess_evidence,
    build_evidence,
    format_verdict_line,
    parse_verdict_line,
)


class FreshnessVerdictV2Tests(unittest.TestCase):
    def test_round_trip_and_pass(self):
        evidence = build_evidence(
            page="Global_Markets",
            status="partial",
            requested=100,
            fresh=96,
            preserved=4,
            rows_written=100,
            api_units=1200,
        )
        parsed = parse_verdict_line(format_verdict_line(evidence))
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.requested, 100)
        self.assertAlmostEqual(parsed.coverage_pct or 0, 96.0)
        self.assertTrue(assess_evidence(parsed).passed)

    def test_low_fresh_coverage_blocks(self):
        evidence = build_evidence(
            page="Global_Markets",
            status="partial",
            requested=100,
            fresh=80,
            preserved=20,
            rows_written=100,
        )
        result = assess_evidence(evidence)
        self.assertFalse(result.passed)
        self.assertIn("fresh_coverage_below_threshold", result.failure_reasons)

    def test_identity_failure_blocks(self):
        evidence = build_evidence(
            page="Market_Leaders",
            status="success",
            requested=100,
            fresh=100,
            identity_failures=1,
            rows_written=100,
        )
        self.assertFalse(assess_evidence(evidence).passed)

    def test_stub_limit_blocks(self):
        evidence = build_evidence(
            page="Mutual_Funds",
            status="partial",
            requested=100,
            fresh=99,
            stubs=1,
            rows_written=100,
        )
        self.assertFalse(assess_evidence(evidence).passed)
        self.assertTrue(assess_evidence(evidence, max_stubs=1).passed)

    def test_unknown_api_units_are_explicit_but_not_a_freshness_failure(self):
        evidence = build_evidence(
            page="Commodities_FX",
            status="success",
            requested=100,
            fresh=100,
            rows_written=100,
            api_units=None,
        )
        parsed = parse_verdict_line(format_verdict_line(evidence))
        self.assertIsNotNone(parsed)
        self.assertFalse(parsed.api_units_known)
        self.assertTrue(assess_evidence(parsed).passed)

    def test_row_buckets_cannot_exceed_requested(self):
        with self.assertRaises(ValueError):
            build_evidence(
                page="Global_Markets",
                status="partial",
                requested=10,
                fresh=8,
                preserved=3,
                rows_written=10,
            )


if __name__ == "__main__":
    unittest.main()
