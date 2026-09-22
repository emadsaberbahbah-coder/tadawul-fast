"""P-154b (scripts/run_inline_page_recovery.py v1.2.0) -- QUOTA-AWARE RECOVERY.

Every inline page replay is a FULL page re-fetch (~53-60k EODHD calls for
Global_Markets, measured 2026-09-21/22). v1.2.0 reads the sync's own
[EODHD-QUOTA v6.60.0] line for the page from the same artifact logs the audit
reads and, behind TFB_INLINE_RECOVERY_QUOTA_GUARD (off | observe | enforce,
default off = v1.1.0 byte-identical), refuses a replay that cannot succeed:
state EXHAUSTED / ON_EXTRA, any NEW 402 row, or used% >= 
TFB_INLINE_RECOVERY_QUOTA_SKIP_PCT (default 90). observe annotates only;
enforce skips (last-good rows stay, next window retries, not a failed page).

Run: python -m pytest -q tests/test_inline_recovery_quota_guard_p154b.py
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scripts.run_inline_page_recovery as rip
from scripts.run_inline_page_recovery import run_inline_recovery

ENV = "TFB_INLINE_RECOVERY_QUOTA_GUARD"
PCT = "TFB_INLINE_RECOVERY_QUOTA_SKIP_PCT"
CYC = "TFB_INLINE_RECOVERY_MAX_CYCLES"
PAGES = ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds")


def _verdict(page, status="success", rows=10):
    return ("2026-09-22 08:24:00 | INFO | DashboardSync | "
            f"[PAGE-VERDICT v6.60.0] page={page} status={status} rows_written={rows} newest_stamp_age_h=1 reason=test\n")


def _quota(page, used, pct, state, new402=0, unknown=False):
    used_txt = "used=unknown (no_key)" if unknown else "used=%d/400000 (%.1f%%) date=2026-09-22 extra=0" % (used, pct)
    return ("2026-09-22 08:24:01 | INFO | DashboardSync | "
            f"[EODHD-QUOTA v6.60.0] {page} | {used_txt} | delta=first-sample | rows402 new={new402} carried=0 | "
            f"f429=0 f404=47 fetch_failed=47 | state={state} | selftest=PASS 5/5\n")


class QuotaGuardTests(unittest.TestCase):
    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in (ENV, PCT, CYC)}
        for k in (ENV, PCT, CYC):
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _source(self, root, failed_page, quota_line=None):
        source = root / "source"
        source.mkdir()
        text = "".join(_verdict(p, "skipped" if p == failed_page else "success", 0 if p == failed_page else 10) for p in PAGES)
        if quota_line:
            text += quota_line
        (source / "sync_execution.log").write_text(text, encoding="utf-8")
        return source

    def _run(self, root, source, fake_stream):
        with mock.patch("scripts.run_inline_page_recovery._stream_process", side_effect=fake_stream) as m:
            rc = run_inline_recovery(source_root=source, backend="https://example.test", sheet_id="sheet",
                                     evidence_root=root / "evidence", plan_out=root / "plan.json",
                                     summary_out=root / "summary.json")
        return rc, json.loads((root / "summary.json").read_text(encoding="utf-8")), m

    # --- T1 gate + clamp ---------------------------------------------------
    def test_t1_gate_words_and_clamp(self):
        for raw, want in ((None, "off"), ("", "off"), ("1", "off"), ("true", "off"), ("on", "off"),
                          ("observe", "observe"), (" ENFORCE ", "enforce"), ("garbage", "off")):
            if raw is None:
                os.environ.pop(ENV, None)
            else:
                os.environ[ENV] = raw
            self.assertEqual(rip._quota_guard_mode(), want, raw)
        for raw, want in (("", 90.0), ("abc", 90.0), ("10", 50.0), ("85", 85.0), ("120", 100.0)):
            os.environ[PCT] = raw
            self.assertEqual(rip._quota_skip_pct(), want, raw)

    # --- T2 off is v1.1.0 byte-identical ----------------------------------
    def test_t2_off_replays_and_summary_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root, "Global_Markets", _quota("Global_Markets", 379386, 94.8, "CRIT"))

            def fake(command, *, env, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(_verdict("Global_Markets", "success", 6609), encoding="utf-8")
                return 0
            rc, summary, m = self._run(root, source, fake)
            self.assertEqual(rc, 0)
            self.assertEqual(m.call_count, 1)
            for k in ("quota_guard", "quota_guard_mode", "quota_skip_pct", "skipped_pages"):
                self.assertNotIn(k, summary)
            self.assertEqual(summary["status"], "ok")

    # --- T3 observe annotates only -----------------------------------------
    def test_t3_observe_never_skips(self):
        os.environ[ENV] = "observe"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root, "Global_Markets", _quota("Global_Markets", 379386, 94.8, "CRIT"))

            def fake(command, *, env, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(_verdict("Global_Markets", "success", 6609), encoding="utf-8")
                return 0
            rc, summary, m = self._run(root, source, fake)
            self.assertEqual(rc, 0)
            self.assertEqual(m.call_count, 1)
            self.assertEqual(summary["quota_guard_mode"], "observe")
            self.assertEqual(summary["quota_guard"][0]["decision"], "skip")
            self.assertEqual(summary["quota_guard"][0]["quota"]["pct"], 94.8)
            self.assertEqual(summary["skipped_pages"], [])
            self.assertTrue(summary["results"][0]["passed"])

    # --- T4 enforce skips a CRIT replay -------------------------------------
    def test_t4_enforce_skips_crit(self):
        os.environ[ENV] = "enforce"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root, "Global_Markets", _quota("Global_Markets", 379386, 94.8, "CRIT"))
            rc, summary, m = self._run(root, source, lambda *a, **k: self.fail("replay must not run"))
            self.assertEqual(rc, 0)
            self.assertEqual(m.call_count, 0)
            self.assertEqual(summary["status"], "ok")
            self.assertEqual(summary["failed_pages"], [])
            self.assertEqual(summary["skipped_pages"], ["Global_Markets"])
            r = summary["results"][0]
            self.assertEqual((r["page"], r["skipped"], r["audit_status"], r["passed"]), ("Global_Markets", "quota", "skipped", False))

    # --- T5 enforce allows an OK replay -------------------------------------
    def test_t5_enforce_allows_ok(self):
        os.environ[ENV] = "enforce"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root, "Global_Markets", _quota("Global_Markets", 244362, 61.1, "OK"))

            def fake(command, *, env, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(_verdict("Global_Markets", "success", 6609), encoding="utf-8")
                return 0
            rc, summary, m = self._run(root, source, fake)
            self.assertEqual((rc, m.call_count), (0, 1))
            self.assertEqual(summary["quota_guard"][0]["decision"], "allow")
            self.assertEqual(summary["skipped_pages"], [])
            self.assertTrue(summary["results"][0]["passed"])

    # --- T6 cycle-2 stop from the replay's own log ---------------------------
    def test_t6_second_cycle_reads_previous_replay_log(self):
        os.environ[ENV] = "enforce"
        os.environ[CYC] = "3"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root, "Global_Markets", _quota("Global_Markets", 244362, 61.1, "OK"))

            def fake(command, *, env, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                # replay still incomplete AND it pushed the counter over the line
                log_path.write_text(_verdict("Global_Markets", "skipped", 0) + _quota("Global_Markets", 372000, 93.0, "CRIT"),
                                    encoding="utf-8")
                return 0
            rc, summary, m = self._run(root, source, fake)
            self.assertEqual(m.call_count, 1)                    # cycle 1 ran, cycle 2 skipped
            self.assertEqual(summary["cycles_used"], 2)
            self.assertEqual([e["decision"] for e in summary["quota_guard"]], ["allow", "skip"])
            self.assertEqual(summary["quota_guard"][1]["cycle"], 2)
            self.assertEqual(summary["skipped_pages"], ["Global_Markets"])
            self.assertEqual(rc, 0)

    # --- T7 no quota line -> v1.1.0 behaviour; UNKNOWN -> allow; new 402 -> skip
    def test_t7_no_line_unknown_and_new_402(self):
        self.assertEqual(rip._quota_decision(None, 90.0)[0], "allow")
        self.assertEqual(rip._quota_decision({"state": "UNKNOWN", "rows402_new": 0, "pct": None}, 90.0)[0], "allow")
        self.assertEqual(rip._quota_decision({"state": "OK", "rows402_new": 3, "pct": 40.0}, 90.0)[0], "skip")
        self.assertEqual(rip._quota_decision({"state": "EXHAUSTED", "rows402_new": 0, "pct": None}, 90.0)[0], "skip")
        self.assertEqual(rip._quota_decision({"state": "ON_EXTRA", "rows402_new": 0, "pct": 100.0}, 90.0)[0], "skip")
        self.assertEqual(rip._quota_decision({"state": "WARN", "rows402_new": 0, "pct": 85.0}, 90.0)[0], "allow")
        self.assertEqual(rip._quota_decision({"state": "WARN", "rows402_new": 0, "pct": 85.0}, 85.0)[0], "skip")
        os.environ[ENV] = "enforce"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root, "Global_Markets")   # no quota line at all

            def fake(command, *, env, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(_verdict("Global_Markets", "success", 6609), encoding="utf-8")
                return 0
            rc, summary, m = self._run(root, source, fake)
            self.assertEqual((rc, m.call_count), (0, 1))
            self.assertEqual(summary["quota_guard"][0]["reason"], "no_quota_line")

    # --- T8 parser on the real line shapes + last-line-wins -----------------
    def test_t8_parser_real_shapes_and_last_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a").mkdir()
            (root / "a" / "sync_execution.log").write_text(
                _quota("Global_Markets", 68119, 17.0, "OK") + _quota("Market_Leaders", 2130, 0.5, "OK")
                + _quota("Global_Markets", 120859, 30.2, "OK") + _quota("Commodities_FX", 0, 0.0, "UNKNOWN", unknown=True),
                encoding="utf-8")
            q = rip._latest_quota_for_page(root, "Global_Markets")
            self.assertEqual((q["used"], q["pct"], q["state"], q["rows402_new"]), (120859, 30.2, "OK", 0))
            u = rip._latest_quota_for_page(root, "Commodities_FX")
            self.assertEqual((u["pct"], u["state"]), (None, "UNKNOWN"))
            self.assertIsNone(rip._latest_quota_for_page(root, "Mutual_Funds"))
            self.assertIsNone(rip._latest_quota_for_page(root / "missing", "Global_Markets"))

    # --- T9 fail-open ----------------------------------------------------------
    def test_t9_guard_error_allows_replay(self):
        os.environ[ENV] = "enforce"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root, "Global_Markets", _quota("Global_Markets", 379386, 94.8, "CRIT"))

            def fake(command, *, env, log_path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(_verdict("Global_Markets", "success", 6609), encoding="utf-8")
                return 0
            with mock.patch("scripts.run_inline_page_recovery._latest_quota_for_page", side_effect=RuntimeError("boom")):
                rc, summary, m = self._run(root, source, fake)
            self.assertEqual((rc, m.call_count), (0, 1))
            self.assertEqual(summary["skipped_pages"], [])

    def test_t10_version(self):
        self.assertGreaterEqual(tuple(int(x) for x in rip.SCRIPT_VERSION.split(".")), (1, 2, 0))


if __name__ == "__main__":
    unittest.main()
