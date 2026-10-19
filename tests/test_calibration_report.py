"""Offline calibration proves dedup, measured probabilities and no lookahead."""
import json

import pytest

openpyxl = pytest.importorskip("openpyxl")

from scripts import calibration_report as report


def export_fixture(path):
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    perf = workbook.create_sheet("Performance_Log")
    for _ in range(4):
        perf.append([])
    header = ["Record ID", "Key", "Symbol", "Horizon", "Status", "Realized ROI %", "Entry Forecast Reliability",
              "Origin Tab", "Date Recorded (Riyadh)", "Volatility", "Max Drawdown %", "Sharpe Ratio", "Outcome"]
    perf.append(header)
    def row(key, roi, reliability, status="matured", symbol="AAA.US", risk=(0, 0, 0)):
        return [key + str(roi), key, symbol, "1W", status, roi, reliability, "Market_Leaders", "2026-10-01", *risk, ""]
    perf.append(row("A", 10, 80))
    perf.append(row("A", -10, 20))  # conflicting duplicate: FIRST decided row stands
    perf.append(row("B", -10, 20, symbol="BBB.US", risk=(None, None, None)))
    perf.append(row("C", 10, None))  # win remains; probability unavailable
    perf.append(row("D", 0, 90))    # breakeven never a decided probability outcome
    perf.append(row("E", float("nan"), 70))
    perf.append(row("F", None, 60, status="active"))
    perf.append(row("F", -10, 90))  # active-first cannot discard the only matured outcome
    signal = workbook.create_sheet("Signal_History")
    signal.append(["Snapshot ID", "Key", "Symbol", "Date (Riyadh)", "Origin Tab", "Forecast Reliability"])
    signal.append(["s1", "AAA|1", "AAA.US", "2026-10-01", "Market_Leaders", 1])
    signal.append(["s2", "AAA|2", "AAA.US", "2026-10-01", "Market_Leaders", 99])
    signal.append(["s3", "BBB|3", "BBB.US", "2026-10-02", "Market_Leaders", 99])  # later snapshot must not replace entry 20
    market = workbook.create_sheet("Global_Markets")
    market.append(["Symbol", "Last Updated (UTC)"])
    market.append(["AAA.US", "2026-10-06 12:00:00+00:00"])
    workbook.save(path)
    workbook.close()


def test_decided_dedup_probability_populations_and_risk_exclusions(tmp_path):
    path = tmp_path / "export.xlsx"
    export_fixture(path)
    result = report.analyze_workbook(path)
    overall = result["overall"]
    assert overall["matured"] == 5 and overall["decided"] == 4
    assert overall["wins"] == overall["losses"] == 2
    assert overall["win_rate_pct"] == 50
    assert overall["brier_n"] == 3
    assert overall["brier"] == pytest.approx((.04 + .04 + .81) / 3, abs=1e-8)
    assert overall["missing_or_invalid_reliability"] == 1
    assert result["exclusions"]["duplicate_matured_excluded"] == 1
    assert result["exclusions"]["duplicate_matured_conflicting_roi"] == 1
    assert result["exclusions"]["matured_missing_or_invalid_roi_excluded"] == 1
    assert result["risk_measurement_exclusions"]["all_zero_placeholder_triplets_excluded"] == 4
    assert result["risk_measurement_exclusions"]["blank_or_invalid_triplets_excluded"] == 1
    assert result["source"]["performance_header_row"] == 5
    assert result["source"]["global_markets_dominant_date"] == "2026-10-06"
    assert result["signals"]["ambiguous_entry_date_matches"] == 4
    assert result["signals"]["entry_date_matches"] == 4  # BBB's next-day snapshot is excluded


def test_report_is_deterministic_and_does_not_write_workbook(tmp_path):
    path = tmp_path / "export.xlsx"
    export_fixture(path)
    original = path.read_bytes()
    a, b = report.analyze_workbook(path), report.analyze_workbook(path)
    assert a == b and report.render_md(a) == report.render_md(b)
    assert path.read_bytes() == original
    assert report.main([str(path), "--json", str(tmp_path / "result.json"), "--md", str(tmp_path / "result.md")]) == 0
    assert json.loads((tmp_path / "result.json").read_text()) == a
    assert "Owner decision remains open" in (tmp_path / "result.md").read_text()


def test_missing_sheet_and_invalid_sample_fail_without_fabricated_report(tmp_path):
    path = tmp_path / "empty.xlsx"
    openpyxl.Workbook().save(path)
    with pytest.raises(ValueError, match="Performance_Log is required"):
        report.analyze_workbook(path)
    assert report.main([str(path)]) == 2
    with pytest.raises(ValueError, match="min_sample"):
        report.analyze_workbook(path, 0)


@pytest.mark.parametrize("value", [None, "", True, "nan", "inf", "missing"])
def test_unknown_numeric_inputs_stay_unavailable(value):
    assert report.number(value) is None


def test_no_decided_outcomes_reports_unavailable():
    summary = report.summarize([{"roi": 0, "reliability": 80}])
    assert summary["win_rate_pct"] is None and summary["brier"] is None
    assert summary["sufficient"] is False


def test_out_of_range_entry_reliability_is_excluded_without_clamping(tmp_path):
    path = tmp_path / "export.xlsx"
    export_fixture(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Performance_Log"].cell(6, 7, 180)
    workbook.save(path)
    workbook.close()
    result = report.analyze_workbook(path)
    assert result["overall"]["decided"] == 4
    assert result["overall"]["brier_n"] == 2
    assert result["overall"]["brier"] == pytest.approx((.04 + .81) / 2)


def learning_export_fixture(path):
    export_fixture(path)
    workbook = openpyxl.load_workbook(path)
    sheet = workbook["Performance_Log"]
    for column, title in enumerate(("Last Updated (Riyadh)", "Target Date (Riyadh)",
                                   "Maturity Date", "Notes"), 14):
        sheet.cell(5, column, title)
    for row in range(6, 14):
        sheet.cell(row, 9, "2026-10-01 10:00:00")
        sheet.cell(row, 14, "2026-10-19 02:30:19")
        sheet.cell(row, 15, "2026-10-08 10:00:00")
        if sheet.cell(row, 5).value == "matured":
            sheet.cell(row, 16, "2026-10-10 14:55:15")
    # Neither a newer duplicate nor an invalid outcome may freshen the
    # accepted calibration population. New recordings are a separate clock.
    sheet.cell(7, 16, "2026-10-19 02:30:19")  # duplicate A
    sheet.cell(11, 16, "2026-10-19 02:30:19")  # missing/nonfinite ROI
    sheet.cell(12, 9, "2026-10-19 02:30:19")  # active recommendation
    sheet.cell(12, 17, "private free text | " + report.OUTCOME_WITNESS + json.dumps({
        "schema": "tfb.outcome-price.v1", "state": "pending",
        "reason": "quote_session_unknown;quote_http_429", "evaluated_at": "2026-10-11T23:00:17+03:00",
        "provider": "private provider text which must not appear in report",
    }))
    calibration = workbook.create_sheet("_S1_Calibration")
    calibration.append(["As Of (Riyadh)", "State", "N Checkpoints"])
    calibration.append(["2026-10-11 23:00:18", "PASS", 6647])
    workbook.save(path)
    workbook.close()


def test_fresh_recording_and_export_do_not_freshen_matured_evidence(tmp_path):
    path = tmp_path / "learning.xlsx"
    learning_export_fixture(path)
    original = path.read_bytes()
    result = report.analyze_workbook(path, as_of="2026-10-19")
    clock = result["learning_clock"]
    assert clock["latest_recorded_at_riyadh"] == "2026-10-19T02:30:19+03:00"
    assert result["source"]["performance_updated_dates"]["latest"] == "2026-10-19"
    assert clock["latest_maturity_at_riyadh"] == "2026-10-10T14:55:15+03:00"
    assert clock["maturity_age_calendar_days"] == 9
    assert clock["active_target_before_as_of_date"] == 1
    assert clock["overdue_by_target_date"] == {"2026-10-08": 1}
    assert clock["overdue_outcome_witness_reasons"] == {"quote_session_unknown;quote_http_429": 1}
    assert clock["outcome_evaluation_age_calendar_days"] == 8
    assert clock["calibration_snapshot"] == {
        "available": True, "published_at_riyadh": "2026-10-11T23:00:18+03:00",
        "age_calendar_days": 8, "checkpoint_state": "PASS", "n_checkpoints": 6647,
        "timestamp_after_as_of": False,
    }
    assert result["overall"]["matured"] == 5 and result["overall"]["decided"] == 4
    rendered = report.render_md(result)
    assert "does not prove a new matured outcome" in rendered
    assert "not the overall S-1 promotion decision" in rendered
    assert "private free text" not in rendered and "private provider" not in rendered
    assert path.read_bytes() == original
    assert result == report.analyze_workbook(path, as_of="2026-10-19")


def test_no_as_of_has_no_invented_today_or_overdue_classification(tmp_path):
    path = tmp_path / "learning.xlsx"
    learning_export_fixture(path)
    clock = report.analyze_workbook(path)["learning_clock"]
    assert clock["as_of_riyadh_date"] is None
    assert clock["maturity_age_calendar_days"] is None
    assert clock["active_target_before_as_of_date"] is None
    assert clock["active_target_on_as_of_date"] is None
    assert clock["overdue_by_target_date"] == {}
    assert clock["calibration_snapshot"]["age_calendar_days"] is None


@pytest.mark.parametrize("target,overdue,today,unknown", [
    ("2026-10-18 23:59:59", 1, 0, 0),
    ("2026-10-19 00:00:00", 0, 1, 0),
    ("2026-10-20 00:00:00", 0, 0, 0),
    ("2026-10-18T22:30:00+00:00", 0, 1, 0),  # 19 Oct in Riyadh
    ("2026-10-18", 1, 0, 0),  # day evidence, no invented session close
    ("invalid", 0, 0, 1),
])
def test_backlog_is_explicit_riyadh_calendar_day_evidence(tmp_path, target, overdue, today, unknown):
    path = tmp_path / "learning.xlsx"
    learning_export_fixture(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Performance_Log"].cell(12, 15, target)
    workbook.save(path)
    workbook.close()
    result = report.analyze_workbook(path, as_of="2026-10-19")
    clock = result["learning_clock"]
    assert clock["active_target_before_as_of_date"] == overdue
    assert clock["active_target_on_as_of_date"] == today
    assert clock["active_missing_or_invalid_target_date"] == unknown
    assert result["overall"]["matured"] == 5  # target passage supplies no outcome


def test_missing_times_remain_unknown_and_future_publication_cannot_have_zero_age(tmp_path):
    path = tmp_path / "learning.xlsx"
    learning_export_fixture(path)
    workbook = openpyxl.load_workbook(path)
    for row in range(6, 14):
        workbook["Performance_Log"].cell(row, 16, "2026-10-10")
    workbook["_S1_Calibration"].cell(2, 1, "2026-10-20 00:00:00")
    workbook.save(path)
    workbook.close()
    clock = report.analyze_workbook(path, as_of="2026-10-19")["learning_clock"]
    assert clock["latest_maturity_at_riyadh"] is None
    assert clock["maturity_age_calendar_days"] is None
    assert clock["eligible_matured_rows_missing_maturity_timestamp"] == 5
    assert clock["calibration_snapshot"]["timestamp_after_as_of"] is True
    assert clock["calibration_snapshot"]["age_calendar_days"] is None


def test_unknown_publication_and_private_or_malformed_witnesses_are_disclosed(tmp_path):
    path = tmp_path / "export.xlsx"
    export_fixture(path)
    result = report.analyze_workbook(path, as_of="2026-10-19")
    assert result["learning_clock"]["calibration_snapshot"]["available"] is False
    assert report.outcome_witness("private note")[0] == "no_witness"
    assert report.outcome_witness(report.OUTCOME_WITNESS + "{")[0] == "invalid_witness"
    assert report.outcome_witness(report.OUTCOME_WITNESS + json.dumps({
        "schema": "tfb.outcome-price.v1", "reason": "private URL https://example.invalid?token=private",
    }))[0] == "unspecified_witness_reason"


@pytest.mark.parametrize("as_of", ["today", "2026-02-30", "2026-10-19T12:00:00"])
def test_invalid_explicit_as_of_is_rejected_before_workbook_read(tmp_path, as_of):
    with pytest.raises(ValueError, match="as_of"):
        report.analyze_workbook(tmp_path / "absent.xlsx", as_of=as_of)


def test_cli_binds_explicit_as_of_without_changing_outcomes(tmp_path):
    path = tmp_path / "learning.xlsx"
    learning_export_fixture(path)
    output = tmp_path / "report.json"
    markdown = tmp_path / "report.md"
    assert report.main([str(path), "--as-of", "2026-10-19", "--json", str(output), "--md", str(markdown)]) == 0
    result = json.loads(output.read_text())
    assert result["learning_clock"]["maturity_age_calendar_days"] == 9
    assert result["overall"]["matured"] == 5
