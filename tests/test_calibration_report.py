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
