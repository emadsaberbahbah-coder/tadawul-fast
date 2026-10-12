"""Regression tests for the 2026-09-17 Actions repair; no credentials or I/O."""
import copy
import concurrent.futures
import json
import os
import tempfile
import threading
import time
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


class SharedAuditTransportTests(unittest.TestCase):
    class RaceSensitiveService:
        """A cached transport's current response is corrupted by overlapping I/O."""
        def __init__(self, failure=None):
            self.metric_lock = threading.Lock()
            self.active = self.peak = self.calls = 0
            self.current_range = None
            self.failure = failure

        def spreadsheets(self):
            return self

        def values(self):
            return self

        def get(self, **kwargs):
            backend = self
            requested = kwargs["range"]

            class Request:
                def execute(self):
                    with backend.metric_lock:
                        backend.active += 1
                        backend.peak = max(backend.peak, backend.active)
                        backend.calls += 1
                        failure, backend.failure = backend.failure, None
                    try:
                        if failure:
                            raise failure
                        backend.current_range = requested
                        # Release the GIL while the simulated shared connection
                        # carries a response; an unprotected reader returns
                        # another request's data rather than its own range.
                        time.sleep(0.003)
                        return {"values": [[backend.current_range]]}
                    finally:
                        with backend.metric_lock:
                            backend.active -= 1
            return Request()

    def test_distinct_readers_share_one_cached_service_without_overlapping_transport(self):
        from integrations.google_sheets_service import SheetsAPIClient
        backend = self.RaceSensitiveService()
        cache = SheetsAPIClient()
        cache._create_service = Mock(return_value=backend)
        # This is the production cache class: initialization is protected,
        # while all callers receive the identical cached service/transport.
        readers = [make_reader(cache.get_service), make_reader(cache.get_service)]
        barrier = threading.Barrier(11)

        def read(number):
            barrier.wait(timeout=3)
            requested = f"Synthetic_Page_{number}!A:A"
            return requested, readers[number % 2]("synthetic-sheet", requested)

        with concurrent.futures.ThreadPoolExecutor(max_workers=11) as executor:
            results = list(executor.map(read, range(44)))
        self.assertEqual(backend.peak, 1)
        self.assertEqual(backend.calls, 44)
        self.assertEqual(backend.active, 0)
        self.assertTrue(all(values == [[requested]] for requested, values in results))
        cache._create_service.assert_called_once_with()

    def test_request_failure_releases_lock_before_retry_backoff_and_other_reader(self):
        backend = self.RaceSensitiveService(OSError("synthetic TLS failure"))
        second_reader = make_reader(lambda: backend)
        attempts = []

        def retry(name, operation):
            attempts.append(name)
            try:
                return operation()
            except OSError:
                # Run another reader from a different thread during the retry
                # boundary. A lock around the entire retry callback deadlocks.
                values, errors = [], []
                def other_read():
                    try:
                        values.append(second_reader("synthetic-sheet", "Recovery!A:A"))
                    except Exception as exc:
                        errors.append(exc)
                worker = threading.Thread(target=other_read, daemon=True)
                worker.start()
                worker.join(timeout=2)
                self.assertFalse(worker.is_alive(), "retry callback retained transport lock")
                self.assertEqual(errors, [])
                self.assertEqual(values, [[["Recovery!A:A"]]])
                return operation()

        result = make_reader(lambda: backend, retry)("synthetic-sheet", "Main!A:A")
        self.assertEqual(result, [["Main!A:A"]])
        self.assertEqual(attempts, ["Read Audit Range"])
        self.assertEqual(backend.peak, 1)
        self.assertEqual(backend.calls, 3)
        self.assertEqual(backend.active, 0)

    def test_factory_failure_releases_lock_and_remains_visible(self):
        backend = self.RaceSensitiveService()
        reader = make_reader(Mock(side_effect=[RuntimeError("synthetic init failure"), backend]))
        failure = []
        failure_ready, release_failed_worker = threading.Event(), threading.Event()
        def failed_read():
            try:
                reader("synthetic-sheet", "Failed!A:A")
            except RuntimeError as exc:
                failure.append(str(exc))
            finally:
                failure_ready.set()
                # Keep this thread alive so a later thread cannot reuse its
                # identity and mask an unreleased reentrant transport lock.
                release_failed_worker.wait(timeout=4)
        worker = threading.Thread(target=failed_read, daemon=True)
        worker.start()
        self.assertTrue(failure_ready.wait(timeout=2))
        self.assertEqual(failure, ["synthetic init failure"])
        successful, errors = [], []
        def recovered_read():
            try:
                successful.append(reader("synthetic-sheet", "Recovered!A:A"))
            except Exception as exc:
                errors.append(exc)
        recovered_worker = threading.Thread(target=recovered_read, daemon=True)
        try:
            recovered_worker.start()
            recovered_worker.join(timeout=2)
            self.assertFalse(recovered_worker.is_alive(), "factory exception retained transport lock")
        finally:
            release_failed_worker.set()
            worker.join(timeout=2)
        self.assertEqual(errors, [])
        self.assertEqual(successful, [[["Recovered!A:A"]]])


if __name__ == "__main__":
    unittest.main()
