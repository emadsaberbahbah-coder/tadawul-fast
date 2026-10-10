"""WP-06 regressions: default reporting, untrusted receipts and plan limits."""
import datetime as dt
import hashlib
import json

import pytest

pytest.importorskip("openpyxl")

from scripts import tfb_export_audit as audit


NOW = dt.datetime(2026, 10, 3, 11, tzinfo=dt.timezone.utc)


@pytest.mark.parametrize("clean,baseline", [
    (False, "a674f9263b5aa25beee1ee0aec517afb6c84383b0b15d7a80aecec9f6d9f3f0d"),
    (True, "df70799b5cdb6370a27eec8cd974604f79cb9ddcd841d6a8546ff13363720ca9"),
])
def test_default_payload_matches_v111_before_version_stamp(tmp_path, clean, baseline):
    # Captured from v1.1.1 against the SAME generated export. Workbook archive
    # timestamps and version are omitted; every finding/metric/parameter stays.
    path = tmp_path / "fixture.xlsx"
    expect = audit._build_synthetic(path, clean)
    result = audit.audit_workbook(path, expect=expect, asof="2026-10-03", now_utc=NOW)
    payload = {k: result[k] for k in ["findings", "metrics", "params", "lanes", "overall", "fix_list"]}
    assert hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest() == baseline


@pytest.mark.parametrize("warnings,expected", [
    ("sheet_margin_unit:profit_margin:fraction:2.6", True),
    ("sheet_margin_unit:profit_margin:fraction:2.6000", True),
    ("sheet_margin_unit:profit_margin:fraction:26", False),
    ("sheet_margin_unit:gross_margin:fraction:2.6", False),
    ("sheet_margin_unit:profit_margin:percent_points:2.6", False),
    ("sheet_margin_unit:profit_margin:fraction:nan", False),
    ("sheet_margin_unit:profit_margin:fraction:inf", False),
    ("sheet_margin_unit:profit_margin:fraction:2.6;sheet_margin_unit:profit_margin:fraction:26", False),
    ("sheet_margin_unit:profit_margin:fraction:2.6;sheet_margin_unit:profit_margin:invalid:2.6", False),
    ("sheet_margin_unit:profit_margin:fraction:2.6;sheet_margin_unit:profit_margin:fraction:2.6", True),
])
def test_receipt_cannot_hide_invalid_or_stale_evidence(warnings, expected):
    assert audit._margin_fraction_receipt(warnings, "profit_margin", 2.6) is expected


class MemoryBook:
    def __init__(self, sheets):
        self.sheets = sheets
        self.state = {name: "visible" for name in sheets}

    def rows(self, name):
        return self.sheets.get(name)

    table = audit.Book.table


@pytest.mark.parametrize("values,zeros,blanks", [
    ([None, ""], 0, 1),
    ([0.0, 0], 1, 0),
    ([None, 0.0], 1, 0),
    ([0.0, 0.2], 0, 0),
    (["invalid", ""], 0, 0),
])
def test_blank_risk_is_not_numeric_zero(values, zeros, blanks):
    b = MemoryBook({"Performance_Log": [
        ["Record ID", "Key", "Status", "Outcome", "Volatility"],
        ["00000000-0000-0000-0000-000000000000", "A", "matured", "WIN", values[0]],
        ["00000001-0000-0000-0000-000000000000", "B", "matured", "LOSS", values[1]],
    ]})
    findings, metrics = audit.check_model(b, 3)
    fs = {f["check"]: f for f in findings}
    assert fs["performance_log_risk_columns_all_zero"]["count"] == zeros
    assert len(metrics.get("perf_blank_risk_cols", [])) == blanks
    if any(not audit._s(v) for v in values):
        assert fs["performance_log_risk_columns_not_measured"]["status"] == "INFO"


def run_quota(rows, pct=75):
    book = MemoryBook({"_Run_Log": [["Timestamp", "Level", "Action", "Page", "Status", "Message"]] + [
        ["2026-10-03 10:00:00", "INFO", "quota", "ALL", "OK",
         f"[EODHD-QUOTA] used={used}/{limit} date={date}"] for used, limit, date in rows
    ]})
    fs, metrics = audit.check_runlog(book, 3, 7, 90000, "17 4,12,20", NOW, "2026-10-03", pct)
    return {f["check"]: f for f in fs}, metrics


def test_plan_is_bound_to_usage_maximum_and_threshold_is_inclusive():
    fs, metrics = run_quota([(300000, 400000, "2026-10-01"), (100000, 900000, "2026-10-01")])
    assert metrics["eodhd_daily_limit"] == {"2026-10-01": 400000}
    assert fs["eodhd_daily_plan_threshold"]["status"] == "FAIL"
    assert fs["eodhd_daily_over_target"]["status"] == "WARN"


def test_tied_peak_uses_smaller_plan_and_partial_day_is_excluded():
    fs, metrics = run_quota([(100000, 400000, "2026-10-01"), (100000, 100000, "2026-10-01"),
                             (500000, 400000, "2026-10-03")])
    assert metrics["eodhd_daily_limit"]["2026-10-01"] == 100000
    assert fs["eodhd_daily_plan_threshold"]["count"] == 1


def test_zero_recorded_plan_cannot_produce_a_green_plan_check():
    fs, _ = run_quota([(1, 0, "2026-10-01")])
    assert fs["eodhd_daily_plan_threshold"]["status"] == "FAIL"


def test_cli_env_and_explicit_rollback(monkeypatch):
    monkeypatch.setenv("TFB_AUDIT_MARGIN_RECEIPTS", "1")
    monkeypatch.setenv("TFB_AUDIT_EODHD_FAIL_PCT", "75")
    args = audit.parse_args([])
    assert args.margin_receipts is True and args.eodhd_fail_pct == 75
    args = audit.parse_args(["--no-margin-receipts", "--eodhd-fail-pct", "0"])
    assert args.margin_receipts is False and args.eodhd_fail_pct == 0
    for invalid in ("nan", "inf", "-1", "101", "bad"):
        with pytest.raises(SystemExit):
            audit.parse_args(["--eodhd-fail-pct", invalid])
