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
    # v1.3.0 moved the default slots to the armed schedule (P-170c); the
    # fixture's _Run_Log predates it, so the v1.1.1 schedule is passed here.
    path = tmp_path / "fixture.xlsx"
    expect = audit._build_synthetic(path, clean)
    result = audit.audit_workbook(path, expect=expect, asof="2026-10-03", now_utc=NOW, cron="17 4,12,20")
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


# --------------------------------------------------------------------------- #
# v1.3.0 (P-170c): runs are classed by the provenance on their own rows
# --------------------------------------------------------------------------- #
HEADER = ["Timestamp", "Level", "Action", "Page", "Status", "Message", "Endpoint", "HTTP", "ms", "Details JSON"]


def sync_row(local, run="9000", event="", cron=""):
    det = {"run_id": run}
    if event:
        det["event"] = event
    if cron:
        det["schedule"] = cron
    return [local, "INFO", "run_dashboard_sync", "Market_Leaders", "OK", "leg", "", "", "", json.dumps(det)]


def dispatch_row(local, status="OK", action="tfbDispatchDailySync", http=204):
    return [local, "INFO" if status == "OK" else "ERROR", action, "daily_sync.yml", status,
            "workflow_dispatch", "", http, "", "{}"]


def lateness(rows, cron="17 4,12,20"):
    fs, metrics = audit.check_runlog(MemoryBook({"_Run_Log": [HEADER] + rows}), 3, 7, 90000, cron, NOW,
                                     "2026-10-03")
    return {f["check"]: f for f in fs}["schedule_lateness"], metrics


def test_slot_specs_accept_several_minute_hour_groups():
    assert audit._cron_slots("15 3;17 12,20") == [(3, 15), (12, 17), (20, 17)]
    assert audit._cron_slots("17 4,12,20") == [(4, 17), (12, 17), (20, 17)]
    assert audit._cron_slots(" 17 20 ; 15 3 ") == [(3, 15), (20, 17)]
    assert audit._cron_slots("") == audit._cron_slots("  ") == []
    for bad in ("17", "60 3", "15 24", "15 3;", "15 3;x 4", "15 3,"):
        with pytest.raises(ValueError):
            audit._cron_slots(bad)


def test_default_cron_is_the_schedule_rows_without_provenance_ran_under(monkeypatch):
    assert audit.DEFAULT_CRON == "17 4,12,20"
    monkeypatch.delenv("TFB_AUDIT_CRON", raising=False)
    assert audit.parse_args([]).cron == "17 4,12,20"
    monkeypatch.setenv("TFB_AUDIT_CRON", "17 12,20")
    assert audit.parse_args([]).cron == "17 12,20"
    monkeypatch.setenv("TFB_AUDIT_CRON", " ")
    assert audit.parse_args([]).cron == audit.DEFAULT_CRON


def test_dispatched_run_is_measured_from_its_dispatch():
    finding, metrics = lateness([dispatch_row("2026-10-03 06:14:00"),
                                 sync_row("2026-10-03 06:16:00", "501", "workflow_dispatch")])
    assert finding["status"] == "PASS" and metrics["schedule_worst_lateness_min"] == 2
    assert metrics["schedule_dispatched_runs"] == 1
    assert "run 501: dispatch 10-03 03:14Z started 03:16Z" in finding["examples"][-1]
    # Without provenance the same row is a cron run: the v1.2.0 419-min FAIL.
    legacy, legacy_metrics = lateness([sync_row("2026-10-03 06:16:00", "501")], cron="17 12,20")
    assert legacy["status"] == "FAIL" and legacy["count"] == 419
    assert "schedule_dispatched_runs" not in legacy_metrics


def test_late_cron_whose_first_row_follows_the_dispatch_keeps_its_lateness():
    # Reviewer case: 20:17Z cron fires ~03:00Z, the dispatch row lands at 03:05Z,
    # the cron's first row at 03:14Z; the dispatched run queues and writes from 05:25Z.
    rows = [dispatch_row("2026-10-03 06:05:00"),
            sync_row("2026-10-03 06:14:00", "401", "schedule", "17 12,20 * * *"),
            sync_row("2026-10-03 08:14:00", "401", "schedule", "17 12,20 * * *"),
            sync_row("2026-10-03 08:25:00", "402", "workflow_dispatch")]
    finding, metrics = lateness(rows)
    assert finding["status"] == "FAIL" and metrics["schedule_worst_lateness_min"] == 417
    assert any("run 401: slot 10-02 20:17Z started 03:14Z, +6h57m" in e for e in finding["examples"])
    assert any("run 402: dispatch 10-03 03:05Z started 05:25Z, +2h20m" in e for e in finding["examples"])


def test_schedule_runs_use_their_own_cron_line():
    # A pre-#781 04:17Z cron row: measured against its own line even with --cron "17 12,20".
    finding, metrics = lateness([sync_row("2026-10-03 07:31:00", "300", "schedule", "17 4,12,20 * * *")],
                                cron="17 12,20")
    assert finding["status"] == "PASS" and metrics["schedule_worst_lateness_min"] == 14
    # Rows without provenance use --cron (default = the schedule they ran under).
    legacy, m2 = lateness([sync_row("2026-10-03 07:31:00", "300")])
    assert legacy["status"] == "PASS" and m2["schedule_worst_lateness_min"] == 14


def test_one_run_with_a_long_recovery_gap_stays_one_run():
    finding, _ = lateness([sync_row("2026-10-03 15:20:00", "601", "schedule", "17 12,20 * * *"),
                           sync_row("2026-10-03 18:30:00", "601", "schedule", "17 12,20 * * *")])
    assert len(finding["examples"]) == 1 and "run 601: slot 10-03 12:17Z started 12:20Z" in finding["examples"][0]


def test_back_to_back_runs_are_split_by_run_id():
    finding, metrics = lateness([sync_row("2026-10-03 15:20:00", "601", "schedule", "17 12,20 * * *"),
                                 sync_row("2026-10-03 16:00:00", "601", "schedule", "17 12,20 * * *"),
                                 sync_row("2026-10-03 16:10:00", "602", "schedule", "17 12,20 * * *")])
    runs = [e for e in finding["examples"] if e.startswith("run ")]
    assert len(runs) == 2 and "run 602: slot 10-03 12:17Z started 13:10Z" in runs[1]


def test_unconfirmed_dispatch_counts_and_rejected_one_does_not():
    run = sync_row("2026-10-03 06:20:00", "501", "workflow_dispatch")
    finding, _ = lateness([dispatch_row("2026-10-03 06:14:00", "FAILED", http=-1), run])
    assert "unconfirmed dispatch 10-03 03:14Z started 03:20Z" in finding["examples"][-1]
    _, m = lateness([dispatch_row("2026-10-03 06:14:00", "FAILED", http=401), run])
    assert "schedule_dispatched_runs" not in m   # GitHub refused it: no dispatcher row explains the run


def test_github_ui_dispatch_and_second_run_are_not_attributable():
    finding, metrics = lateness([dispatch_row("2026-10-03 06:14:00"),
                                 sync_row("2026-10-03 06:16:00", "501", "workflow_dispatch"),
                                 sync_row("2026-10-03 09:00:00", "502", "workflow_dispatch")])
    assert metrics["schedule_dispatched_runs"] == 1 and len(finding["examples"]) == 1


def test_accepted_dispatch_with_no_run_is_reported():
    rows = [dispatch_row("2026-10-03 06:14:00"), sync_row("2026-10-03 13:00:00", "700", "schedule", "17 12,20 * * *")]
    finding, metrics = lateness(rows)
    assert finding["status"] == "WARN" and metrics["schedule_dispatches_without_run"] == 1
    assert "dispatch 10-03 03:14Z accepted, no sync run recorded" in finding["examples"][-1]
    # A dispatch less than 180 min before the export's last row may still be running.
    _, m = lateness([sync_row("2026-10-03 13:00:00", "700", "schedule", "17 12,20 * * *"),
                     dispatch_row("2026-10-03 14:00:00")])
    assert "schedule_dispatches_without_run" not in m


def test_run_that_started_before_the_window_is_dropped():
    # since = NOW - 7 days = 09-26 11:00Z; this run started 10:30Z (13:30 local).
    finding, _ = lateness([sync_row("2026-09-26 13:30:00", "800"), sync_row("2026-09-26 14:30:00", "800")])
    assert not [e for e in finding["examples"] if "run 800" in e]


def test_no_cron_slots_is_a_warning_not_a_silent_pass():
    finding, _ = lateness([sync_row("2026-10-03 06:16:00")], cron="")
    assert finding["status"] == "WARN" and "no cron slots" in finding["detail"]
