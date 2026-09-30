#!/usr/bin/env python3
"""tests/test_freshness_run_id.py — freshness v1.1.0 run-id lineage.

Eight cases driving _apply_run_id_lineage on fresh reports: extraction from
the live status-line format, match, mismatch INFO-by-default with a proven
exit_code 0 (verdict neutrality), mismatch FAIL when armed with exit_code 1,
absent-token behavior unarmed and armed, and preservation of pre-existing
findings. The flag is read at call time, so env wraps the calls.
"""
from __future__ import annotations

import importlib
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

fresh = importlib.import_module("scripts.audit_decision_surface_freshness")

LIVE_T10 = ("Last run 2026-08-01 01:07:05 | status: OK | req 198d9a7f3917 | "
            "pool Market_Leaders 300/1000")
LIVE_PF = "Last run 2026-08-01 01:03:19 | status: OK | req 198d9a7f3917"
LIVE_PF_OTHER = "Last run 2026-08-01 01:03:19 | status: OK | req aa12bb34cc56"


def _report():
    return fresh.DecisionSurfaceReport("2026-08-01T06:00:00+00:00", "***")


def _with_env(value, fn):
    saved = os.environ.get("TFB_FRESHNESS_REQUIRE_RUN_ID")
    if value is None:
        os.environ.pop("TFB_FRESHNESS_REQUIRE_RUN_ID", None)
    else:
        os.environ["TFB_FRESHNESS_REQUIRE_RUN_ID"] = value
    try:
        return fn()
    finally:
        if saved is None:
            os.environ.pop("TFB_FRESHNESS_REQUIRE_RUN_ID", None)
        else:
            os.environ["TFB_FRESHNESS_REQUIRE_RUN_ID"] = saved


def test_case1_extracts_live_format():
    assert fresh._extract_run_id(LIVE_T10) == "198d9a7f3917"


def test_case2_absent_token_extracts_none():
    assert fresh._extract_run_id("Last run 2026-08-01 01:07:05 | status: OK") is None
    assert fresh._extract_run_id(None) is None


def test_case3_match_sets_fields_no_finding():
    r = _report()
    _with_env(None, lambda: fresh._apply_run_id_lineage(r, LIVE_T10, LIVE_PF))
    assert r.run_id_match is True and r.top10_run_id == r.portfolio_run_id
    assert r.findings == [] and r.exit_code == 0


def test_case4_mismatch_default_is_info_and_verdict_neutral():
    r = _report()
    _with_env(None, lambda: fresh._apply_run_id_lineage(r, LIVE_T10, LIVE_PF_OTHER))
    assert r.run_id_match is False
    assert [f.code for f in r.findings] == ["RUN_ID_MISMATCH"]
    assert r.findings[0].severity == "INFO"
    assert r.exit_code == 0, "INFO must not move the verdict"


def test_case5_mismatch_armed_fails():
    r = _report()
    _with_env("1", lambda: fresh._apply_run_id_lineage(r, LIVE_T10, LIVE_PF_OTHER))
    assert r.findings[0].severity == "FAIL" and r.exit_code == 2


def test_case6_absent_unarmed_is_silent():
    r = _report()
    _with_env(None, lambda: fresh._apply_run_id_lineage(
        r, "Last run 2026-08-01 01:07:05 | status: OK", LIVE_PF))
    assert r.run_id_match is None and r.findings == [] and r.exit_code == 0


def test_case7_absent_armed_is_info_never_fail():
    r = _report()
    _with_env("1", lambda: fresh._apply_run_id_lineage(
        r, "Last run 2026-08-01 01:07:05 | status: OK", LIVE_PF))
    assert [f.code for f in r.findings] == ["RUN_ID_ABSENT"]
    assert r.findings[0].severity == "INFO" and r.exit_code == 0


def test_case8b_warn_exit_unchanged():
    r = _report()
    r.findings.append(fresh.Finding("WARN", "X", "Top_10_Investments", "seeded"))
    assert r.exit_code == 1, "WARN soft-fail must be byte-identical to prior"


def test_case8_existing_findings_preserved():
    r = _report()
    r.findings.append(fresh.Finding("FAIL", "T10_RUN_STALE",
                                    "Top_10_Investments", "seeded"))
    before = list(r.findings)
    _with_env(None, lambda: fresh._apply_run_id_lineage(r, LIVE_T10, LIVE_PF))
    assert r.findings == before, "lineage must never touch existing findings"


if __name__ == "__main__":
    _names = sorted(k for k in dir() if k.startswith("test_"))
    for fn in _names:
        globals()[fn]()
    print(f"SELFTEST {len(_names)}/{len(_names)} PASS — run-id lineage, "
          "verdict neutrality, arming proven")
