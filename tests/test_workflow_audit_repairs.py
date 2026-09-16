"""Regression tests for the 2026-09-17 Actions repair; no credentials or I/O."""
import copy
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import audit_provider_target_coverage as ptc
from scripts.audit_full_refresh_coverage import parse_dt_precision
from scripts.workflow_audit_support import make_reader


def grid(share=50, rows=200):
    return [["Symbol", "Forecast Source"]] + [
        [f"S{i}", "provider_target" if i < rows * share / 100 else "phase_ii_synthetic"]
        for i in range(rows)
    ]


class BaselineLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "last-good.json")
        self.cfg = ptc.Config()
        self.base = {
            "schema": ptc.STATE_SCHEMA, "sheet": "***", "config_hash": self.cfg.hash(),
            "generated_at_utc": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
            "pages": {"GM": {"share_pct": 50.0, "rows": 200, "provider": 100}},
        }
        env = patch.dict(os.environ, {"TFB_PTC_FREEZE": "0"})
        env.start()
        self.addCleanup(env.stop)

    def save_base(self, value=None):
        Path(self.path).write_text(json.dumps(value if value is not None else self.base))

    def test_missing_baseline_failure_cannot_write_empty_reference(self):
        report = ptc.audit_pages({"GM": grid()}, None, self.cfg)
        self.assertEqual(report.exit_code, 2)
        self.assertEqual(ptc.save_last_good(self.path, report, "***"), "kept: control-health failure")
        self.assertFalse(Path(self.path).exists())

    def test_empty_baseline_is_not_accepted_on_schedule(self):
        report = ptc.audit_pages({"GM": grid()}, {**self.base, "pages": {}}, self.cfg)
        self.assertEqual(report.exit_code, 2)
        self.assertIn("CH_BASELINE_EMPTY", [f.code for f in report.findings])

    def test_stale_baseline_retains_actual_reference_for_diagnosis(self):
        base = {**self.base, "generated_at_utc": "2020-01-01T00:00:00+00:00"}
        report = ptc.audit_pages({"GM": grid(10)}, base, self.cfg)
        codes = {f.code for f in report.findings}
        self.assertIn("CH_BASELINE_STALE", codes)
        self.assertIn("PTC_SHARE_COLLAPSE", codes)
        self.assertNotIn("CH_BASELINE_MISSING", codes)
        self.assertEqual(report.exit_code, 2)

    def test_equal_healthy_observation_renews_clock_not_reference(self):
        self.save_base()
        report = ptc.audit_pages({"GM": grid()}, self.base, self.cfg)
        self.assertFalse(report.baseline_updated)
        self.assertEqual(ptc.save_last_good(self.path, report, "***"), "written")
        new = json.loads(Path(self.path).read_text())
        self.assertEqual(new["pages"], self.base["pages"])
        self.assertEqual(new["generated_at_utc"], self.base["generated_at_utc"])
        self.assertEqual(new["last_verified_at_utc"], report.generated_at_utc)

    def test_recent_verification_prevents_false_stale_on_old_reference(self):
        base = {**self.base, "generated_at_utc": "2020-01-01T00:00:00+00:00",
                "last_verified_at_utc": datetime.now(timezone.utc).isoformat()}
        report = ptc.audit_pages({"GM": grid()}, base, self.cfg)
        self.assertEqual(report.exit_code, 0)
        self.assertEqual(report.baseline_verified_at_utc, base["last_verified_at_utc"])

    def test_dip_cannot_renew_reference_or_clock(self):
        self.save_base()
        before = Path(self.path).read_bytes()
        report = ptc.audit_pages({"GM": grid(45)}, self.base, self.cfg)
        self.assertEqual(ptc.save_last_good(self.path, report, "***"), "unchanged")
        self.assertEqual(Path(self.path).read_bytes(), before)

    def test_collapse_cannot_overwrite_existing_state(self):
        self.save_base()
        before = Path(self.path).read_bytes()
        report = ptc.audit_pages({"GM": grid(10)}, self.base, self.cfg)
        self.assertEqual(report.exit_code, 1)
        ptc.save_last_good(self.path, report, "***")
        self.assertEqual(Path(self.path).read_bytes(), before)

    def test_explicit_bootstrap_can_repair_empty_initial_state(self):
        base = {**self.base, "pages": {}}
        self.save_base(base)
        report = ptc.audit_pages({"GM": grid()}, base, self.cfg, bootstrap=True)
        self.assertEqual(report.exit_code, 0)
        self.assertEqual(ptc.save_last_good(self.path, report, "***"), "written")
        self.assertEqual(json.loads(Path(self.path).read_text())["pages"]["GM"]["share_pct"], 50)

    def test_future_verification_fails_closed(self):
        base = {**self.base, "last_verified_at_utc": (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()}
        report = ptc.audit_pages({"GM": grid()}, base, self.cfg)
        self.assertEqual(report.exit_code, 2)
        self.assertIn("CH_BASELINE_FUTURE", [f.code for f in report.findings])

    def test_policy_change_does_not_reuse_incompatible_reference(self):
        cfg = copy.deepcopy(self.cfg)
        cfg.drop_pct = 50
        report = ptc.audit_pages({"GM": grid()}, self.base, cfg)
        self.assertEqual(report.exit_code, 2)
        self.assertIn("CH_BASELINE_POLICY_MISMATCH", [f.code for f in report.findings])

    def test_frozen_reference_remains_byte_identical(self):
        self.save_base()
        before = Path(self.path).read_bytes()
        report = ptc.audit_pages({"GM": grid(60)}, self.base, self.cfg)
        with patch.dict(os.environ, {"TFB_PTC_FREEZE": "1"}):
            self.assertEqual(ptc.save_last_good(self.path, report, "***"), "kept: frozen by TFB_PTC_FREEZE")
        self.assertEqual(before, Path(self.path).read_bytes())


class PrecisionReaderTests(unittest.TestCase):
    def test_requests_underlying_values_not_display_format(self):
        service = Mock()
        values = [["Last Updated"], [46200.5], [46200]]
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {"values": values}
        result = make_reader(lambda: service)("sheet", "_Status!A1:J100")
        service.spreadsheets.return_value.values.return_value.get.assert_called_once_with(
            spreadsheetId="sheet", range="_Status!A1:J100", majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE", dateTimeRenderOption="SERIAL_NUMBER",
        )
        self.assertEqual(result, values)
        self.assertEqual(parse_dt_precision(result[1][0])[1], "datetime")
        self.assertEqual(parse_dt_precision(result[2][0])[1], "date")

    def test_retry_preserved_and_read_errors_not_hidden(self):
        service = Mock()
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = RuntimeError("unavailable")
        retry = Mock(side_effect=lambda _name, operation: operation())
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            make_reader(lambda: service, retry)("sheet", "A1")
        self.assertEqual(retry.call_count, 1)

    def test_invalid_response_fails_closed(self):
        service = Mock()
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {"values": "wrong"}
        with self.assertRaises(TypeError):
            make_reader(lambda: service)("sheet", "A1")


if __name__ == "__main__":
    unittest.main()
