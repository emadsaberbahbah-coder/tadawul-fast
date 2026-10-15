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
# v1.3.0 (P-170c): dispatched runs are recognised by provenance, not clock time
# --------------------------------------------------------------------------- #
HEADER = ["Timestamp", "Level", "Action", "Page", "Status", "Message"]


def sync_row(local, i=0):
    return [local, "INFO", "run_dashboard_sync", "Market_Leaders", "OK", f"leg run={9000 + i}"]


def dispatch_row(local, status="OK", action="tfbDispatchDailySync"):
    return [local, "INFO" if status == "OK" else "ERROR", action, "daily_sync.yml", status,
            "workflow_dispatch accepted (HTTP 204)"]


def lateness(rows, cron="17 12,20"):
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


def test_default_cron_follows_the_daily_sync_workflow(tmp_path):
    def wf(*crons):
        path = tmp_path / "daily_sync.yml"
        path.write_text("on:\n  schedule:\n" + "".join(f'    - cron: "{c}"  # note\n' for c in crons))
        return str(path)
    assert audit._workflow_cron_default(wf("17 4,12,20 * * *")) == "17 4,12,20"
    assert audit._workflow_cron_default(wf("17 12,20 * * *")) == "17 12,20"
    assert audit._workflow_cron_default(wf("15 3 * * *", "17 12,20 * * *")) == "15 3;17 12,20"
    assert audit._workflow_cron_default(wf()) == audit.FALLBACK_CRON
    assert audit._workflow_cron_default(str(tmp_path / "missing.yml")) == audit.FALLBACK_CRON
    # The module default is the checked-in workflow's schedule.
    assert audit.DEFAULT_CRON == audit._workflow_cron_default(audit.DAILY_SYNC_WORKFLOW) != ""


def test_env_overrides_default_cron_and_blank_falls_back(monkeypatch):
    monkeypatch.delenv("TFB_AUDIT_CRON", raising=False)
    assert audit.parse_args([]).cron == audit.DEFAULT_CRON
    monkeypatch.setenv("TFB_AUDIT_CRON", "17 4,12,20")
    assert audit.parse_args([]).cron == "17 4,12,20"
    monkeypatch.setenv("TFB_AUDIT_CRON", " ")
    assert audit.parse_args([]).cron == audit.DEFAULT_CRON


def test_on_time_dispatched_morning_run_is_measured_from_its_dispatch():
    # Dispatcher OK row 06:14 Riyadh (03:14Z); first sync row 06:16 (03:16Z).
    finding, metrics = lateness([dispatch_row("2026-10-03 06:14:00"), sync_row("2026-10-03 06:16:00")])
    assert finding["status"] == "PASS" and metrics["schedule_worst_lateness_min"] == 2
    assert metrics["schedule_dispatched_runs"] == 1
    assert "dispatch 10-03 03:14Z started 03:16Z" in finding["examples"][-1]
    # Without the dispatcher row the v1.2.0 model matched it to the previous 20:17Z.
    legacy, legacy_metrics = lateness([sync_row("2026-10-03 06:16:00")])
    assert legacy["status"] == "FAIL" and legacy["count"] == 419
    assert "schedule_dispatched_runs" not in legacy_metrics


def test_late_cron_just_before_the_dispatch_is_not_claimed_by_it():
    # 20:17Z cron that fired at 03:00Z next day (06:00 Riyadh), then the 03:15Z
    # dispatch and its run: the cron keeps its ~403-min lateness.
    finding, metrics = lateness([sync_row("2026-10-03 06:00:00", 0), dispatch_row("2026-10-03 06:15:00"),
                                 sync_row("2026-10-03 07:50:00", 1)])
    assert finding["status"] == "FAIL" and metrics["schedule_worst_lateness_min"] == 403
    assert any("slot 10-02 20:17Z started 03:00Z" in e for e in finding["examples"])
    assert any("dispatch 10-03 03:15Z started 04:50Z" in e for e in finding["examples"])


def test_one_dispatch_claims_one_run_and_unaccepted_dispatches_claim_none():
    finding, metrics = lateness([dispatch_row("2026-10-03 06:14:00"), sync_row("2026-10-03 06:16:00", 0),
                                 sync_row("2026-10-03 08:30:00", 1)])
    assert metrics["schedule_dispatched_runs"] == 1
    # The second run (05:30Z) is unclaimed: a cron run, 553 min after 20:17Z.
    assert metrics["schedule_worst_lateness_min"] == 553
    for status in ("FAILED", "SKIPPED"):
        _, m = lateness([dispatch_row("2026-10-03 06:14:00", status), sync_row("2026-10-03 06:16:00")])
        assert "schedule_dispatched_runs" not in m and m["schedule_worst_lateness_min"] == 419


def test_manual_dispatch_is_labelled_and_link_window_is_bounded():
    finding, _ = lateness([dispatch_row("2026-10-03 06:14:00", action="tfbDispatchDailySyncNow"),
                           sync_row("2026-10-03 06:20:00")])
    assert "manual dispatch 10-03 03:14Z started 03:20Z" in finding["examples"][-1]
    _, m = lateness([dispatch_row("2026-10-02 22:00:00"), sync_row("2026-10-03 06:16:00")])
    assert "schedule_dispatched_runs" not in m   # 8h16m after the dispatch: not linked


def test_late_cron_slot_is_still_measured_against_its_own_slot():
    finding, metrics = lateness([sync_row("2026-10-02 16:30:00")])
    assert finding["status"] == "FAIL" and metrics["schedule_worst_lateness_min"] == 73
    assert "slot 10-02 12:17Z started 13:30Z" in finding["examples"][-1]


def test_pre_781_schedule_keeps_measuring_the_0417_cron():
    # Codex P1 on #783: before #781 merges, a 04:31Z run is +14 vs 04:17Z.
    finding, metrics = lateness([sync_row("2026-10-03 07:31:00")], cron="17 4,12,20")
    assert finding["status"] == "PASS" and metrics["schedule_worst_lateness_min"] == 14


def test_no_cron_slots_is_a_warning_not_a_silent_pass():
    finding, _ = lateness([sync_row("2026-10-03 06:16:00")], cron="")
    assert finding["status"] == "WARN" and "no cron slots" in finding["detail"]
