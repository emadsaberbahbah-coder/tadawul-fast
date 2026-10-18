"""Actual default reports use the producer risk column without guessing data."""
import csv
import json

import pytest

from scripts import tfb_backtest as backtest


def report(tmp_path, risk_columns, *, options=("--all-signals",), canonical_blank=False):
    # Matches the public PerformanceStore names, including its row-5 header.
    headers = ["Record ID", "Key", "Status", "Outcome", "Realized ROI %",
               "Entry Forecast Reliability", "Entry Score", "Confidence",
               "Entry Investability", "Entry Recommendation", "Horizon", "Origin Tab",
               *risk_columns]
    source = tmp_path / "synthetic_Performance_Log.tsv"
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, delimiter="\t")
        stream.write("\n" * 4)
        writer.writeheader()
        for i, outcome in enumerate(("WIN", "LOSS", "WIN", "LOSS")):
            row = {"Record ID": str(i + 1), "Key": f"synthetic-{i}", "Status": "matured",
                   "Outcome": outcome, "Realized ROI %": "1" if outcome == "WIN" else "-1",
                   "Entry Forecast Reliability": "70", "Entry Score": "65", "Confidence": "HIGH",
                   "Entry Investability": "WATCHLIST", "Entry Recommendation": "HOLD",
                   "Horizon": "1M", "Origin Tab": "synthetic"}
            for column in risk_columns:
                # Deliberately disagree when both columns exist: selection
                # must bind one actual column, never merge their observations.
                row[column] = ("" if canonical_blank else ("LOW" if i < 2 else "MODERATE")) \
                    if column == "Risk Bucket" else "LEGACY_ONLY"
            writer.writerow(row)
    target = tmp_path / "result.json"
    assert backtest.main(["--export-dir", str(tmp_path), *options, "--json", str(target)]) == 0
    return json.loads(target.read_text(encoding="utf-8"))


def test_actual_all_signal_report_includes_canonical_producer_risk_column(tmp_path, capsys):
    payload = report(tmp_path, ["Risk Bucket"])
    assert payload["n"] == 4 and payload["missing"] == []
    assert len(payload["results"]) == 8
    risk = next(result for result in payload["results"] if result["signal"] == "Risk Bucket")
    assert risk["type"] == "categorical" and risk["n"] == 4
    assert {group["group"]: group["n"] for group in risk["groups"]} == {"LOW": 2, "MODERATE": 2}
    assert "signals not in header:" not in capsys.readouterr().out


def test_actual_all_signal_report_preserves_legacy_export_column(tmp_path):
    payload = report(tmp_path, ["Entry Risk Bucket"])
    assert payload["missing"] == [] and len(payload["results"]) == 8
    risk = next(result for result in payload["results"] if result["signal"] == "Entry Risk Bucket")
    assert risk["n"] == 4 and [group["group"] for group in risk["groups"]] == ["LEGACY_ONLY"]


def test_both_headers_prefer_canonical_default_without_double_counting(tmp_path):
    payload = report(tmp_path, ["Risk Bucket", "Entry Risk Bucket"])
    signals = [result["signal"] for result in payload["results"]]
    assert signals.count("Risk Bucket") == 1 and "Entry Risk Bucket" not in signals
    risk = next(result for result in payload["results"] if result["signal"] == "Risk Bucket")
    assert risk["n"] == 4 and "LEGACY_ONLY" not in {group["group"] for group in risk["groups"]}


def test_present_blank_canonical_column_never_borrows_legacy_values(tmp_path):
    payload = report(tmp_path, ["Risk Bucket", "Entry Risk Bucket"], canonical_blank=True)
    risk = next(result for result in payload["results"] if result["signal"] == "Risk Bucket")
    assert risk["n"] == 0 and risk["groups"] == []
    assert "Entry Risk Bucket" not in [result["signal"] for result in payload["results"]]


@pytest.mark.parametrize("requested,columns", [
    ("Risk Bucket", ["Entry Risk Bucket"]),
    ("Entry Risk Bucket", ["Risk Bucket"]),
])
def test_explicit_missing_request_is_not_silently_aliased(tmp_path, requested, columns):
    payload = report(tmp_path, columns, options=("--signal", requested))
    assert payload["results"] == [] and payload["missing"] == [requested]


def test_explicit_legacy_request_keeps_its_own_values_when_both_headers_exist(tmp_path):
    payload = report(tmp_path, ["Risk Bucket", "Entry Risk Bucket"],
                     options=("--all-signals", "--signal", "Entry Risk Bucket"))
    assert len(payload["results"]) == 1 and payload["missing"] == []
    risk = payload["results"][0]
    assert risk["signal"] == "Entry Risk Bucket" and risk["n"] == 4
    assert [group["group"] for group in risk["groups"]] == ["LEGACY_ONLY"]


def test_explicit_other_signal_overrides_all_signals_without_changing_analysis(tmp_path):
    payload = report(tmp_path, ["Risk Bucket"], options=("--all-signals", "--signal", "Entry Score"))
    assert [result["signal"] for result in payload["results"]] == ["Entry Score"]
    assert payload["results"][0]["n"] == 4 and payload["missing"] == []


def test_missing_both_risk_headers_stays_explicitly_unavailable(tmp_path):
    payload = report(tmp_path, [])
    assert payload["missing"] == ["Risk Bucket"] and len(payload["results"]) == 7
