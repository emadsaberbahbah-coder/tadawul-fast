"""Adverse timeline/cohort fixtures for purged, training-only research metrics."""
from copy import deepcopy
import ast
import csv
from datetime import datetime, timedelta, timezone
import json
import random
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts import tfb_backtest as backtest

RIYADH = ZoneInfo("Asia/Riyadh")
AS_OF = datetime(2026, 10, 6, tzinfo=timezone.utc)


def record(key, day, value, win, *, delay=1):
    decision = datetime(2026, 1, day, 9, tzinfo=RIYADH)
    target = decision + timedelta(days=delay)
    return {
        "Key": key, "Record ID": "R" + key, "Symbol": key + ".US",
        "Date Recorded (Riyadh)": decision.isoformat(),
        "Target Date (Riyadh)": target.isoformat(),
        "Maturity Date": (target + timedelta(minutes=5)).isoformat(),
        "Last Updated (Riyadh)": (target + timedelta(minutes=10)).isoformat(),
        "Status": "matured", "Outcome": "WIN" if win else "LOSS",
        "Realized ROI %": "1" if win else "-1", "Entry Score": str(value),
    }


@pytest.fixture
def ledger():
    return [record("A", 1, 10, False), record("B", 1, 90, True),
            record("C", 3, 10, False), record("D", 3, 90, True),
            record("E", 4, 10, False), record("F", 4, 90, True)]


def walk(rows, **kwargs):
    kwargs.setdefault("as_of", AS_OF)
    return backtest.walk_forward_brier(rows, "Entry Score", min_train=2, **kwargs)


def test_every_training_label_precedes_daily_boundary_and_model_base_share_rows(ledger):
    result = walk(ledger)
    assert result["status"] == "COMPLETE"
    assert result["heldout_rows"] == 4
    assert result["heldout_days"] == 2
    assert [row["key"] for row in result["predictions"]] == ["C", "D", "E", "F"]
    for fold in result["folds"]:
        assert fold["train_count"] == 2
        assert datetime.fromisoformat(fold["latest_training_label_utc"]) < datetime.fromisoformat(fold["availability_cutoff_utc"])
    assert result["folds"][1]["purged_past_count"] == 2  # C/D labels unavailable before Jan 4
    predictions = result["predictions"]
    model_loss = sum((row["model_probability"] - row["label"]) ** 2 for row in predictions) / 4
    base_loss = sum((row["baseline_probability"] - row["label"]) ** 2 for row in predictions) / 4
    assert result["model_brier"] == pytest.approx(model_loss)
    assert result["baseline_brier"] == pytest.approx(base_loss)
    assert result["paired_brier_gain"] == pytest.approx(base_loss - model_loss)


def test_future_label_changes_do_not_change_earlier_fit_or_predictions(ledger):
    future = record("G", 10, 50, True)
    original = walk(ledger + [future])
    changed = deepcopy(ledger + [future])
    changed[-1].update({"Outcome": "LOSS", "Realized ROI %": "-1"})
    perturbed = walk(changed)
    assert original["folds"][:2] == perturbed["folds"][:2]
    assert original["predictions"][:4] == perturbed["predictions"][:4]


def test_heldout_extremes_do_not_fit_training_quantiles(ledger):
    original = walk(ledger)
    changed = deepcopy(ledger)
    changed[2]["Entry Score"] = "-1000000000"
    changed[3]["Entry Score"] = "1000000000"
    perturbed = walk(changed)
    assert original["folds"][0]["training_cuts"] == [10.0, 10.0, 90.0, 90.0]
    assert original["folds"][0] == perturbed["folds"][0]


def test_training_base_does_not_use_heldout_labels(ledger):
    rows = deepcopy(ledger)
    for row in rows[:2]:
        row.update({"Outcome": "LOSS", "Realized ROI %": "-1"})
    for row in rows[2:]:
        row.update({"Outcome": "WIN", "Realized ROI %": "1"})
    result = walk(rows)
    assert all(row["baseline_probability"] == 0 for row in result["predictions"])
    assert result["baseline_brier"] == 1.0
    assert result["paired_brier_gain"] == 0.0


@pytest.mark.parametrize("column", ["Date Recorded (Riyadh)", "Target Date (Riyadh)", "Maturity Date", "Last Updated (Riyadh)"])
@pytest.mark.parametrize("value", ["", "bad timestamp"])
def test_missing_or_invalid_provenance_blocks_skill_verdict(ledger, column, value):
    rows = deepcopy(ledger)
    rows[-1][column] = value
    result = backtest.evaluate_signal(rows, "Entry Score", min_n=1, min_train=2)
    assert result["verdict"] == "PENDING"
    assert result["walk_forward"]["exclusions"]["label_provenance_unknown"] == 1
    json.dumps(result, allow_nan=False)


def test_label_matured_before_target_is_invalid_provenance(ledger):
    rows = deepcopy(ledger)
    rows[-1]["Maturity Date"] = rows[-1]["Date Recorded (Riyadh)"]
    result = walk(rows)
    assert result["status"] == "PENDING"
    assert result["exclusions"]["invalid_label_chronology"] == 1


def test_unresolved_outcome_cannot_enter_training_or_test(ledger):
    rows = deepcopy(ledger)
    rows[-1]["Status"] = "active"
    result = walk(rows)
    assert result["status"] == "PENDING"
    assert result["exclusions"]["outcome_unresolved"] == 1
    assert "F" not in {row["key"] for row in result["predictions"]}


def test_last_updated_bound_purges_a_later_label_revision(ledger):
    rows = deepcopy(ledger)
    rows[1]["Last Updated (Riyadh)"] = "2026-01-06 10:00:00"
    result = walk(rows)
    assert result["status"] == "PENDING"
    assert result["heldout_rows"] == 0
    assert "insufficient_training_history" in result["pending_reasons"]


def test_exact_boundary_availability_is_excluded(ledger):
    rows = deepcopy(ledger)
    rows[1]["Last Updated (Riyadh)"] = "2026-01-03 00:00:00"
    result = walk(rows, min_test_days=1)
    assert [fold["decision_day"] for fold in result["folds"]] == ["2026-01-04"]


def test_extra_gap_is_applied_before_test_start(ledger):
    result = walk(ledger, gap_days=1, min_test_days=1)
    assert [fold["decision_day"] for fold in result["folds"]] == ["2026-01-04"]
    fold = result["folds"][0]
    assert datetime.fromisoformat(fold["test_start_utc"]) - datetime.fromisoformat(fold["availability_cutoff_utc"]) == timedelta(days=1)


@pytest.mark.parametrize("spelling", ["2026-01-02", "20260102"])
def test_date_only_availability_uses_riyadh_day_end(ledger, spelling):
    rows = deepcopy(ledger)
    for row in rows[:2]:
        row["Target Date (Riyadh)"] = spelling
        row["Maturity Date"] = spelling
        row["Last Updated (Riyadh)"] = spelling
    result = walk(rows)
    assert result["status"] == "COMPLETE"
    first = result["folds"][0]
    available = datetime.fromisoformat(first["latest_training_label_utc"]).astimezone(RIYADH)
    assert available.hour == 23 and available.minute == 59 and available.second == 59


def test_date_only_maturity_with_same_day_precise_update_is_legitimate(ledger):
    rows = deepcopy(ledger)
    for row in rows[:2]:
        row["Maturity Date"] = "2026-01-02"
        row["Last Updated (Riyadh)"] = "2026-01-02 09:10:00"
    result = walk(rows)
    assert result["status"] == "COMPLETE"
    assert result["excluded_rows"] == 0
    assert result["folds"][0]["train_count"] == 2
    available = datetime.fromisoformat(result["folds"][0]["latest_training_label_utc"]).astimezone(RIYADH)
    assert available.hour == 23 and available.minute == 59


@pytest.mark.parametrize("limits", [
    {"min_train": True}, {"min_train": float("nan")}, {"min_test_days": False},
    {"gap_days": True}, {"gap_days": float("nan")},
    {"shrink": True}, {"shrink": float("nan")}, {"shrink": float("inf")},
])
def test_invalid_limits_cannot_produce_nan_metrics(ledger, limits):
    with pytest.raises(ValueError):
        backtest.walk_forward_brier(ledger, "Entry Score", **limits)


def test_shuffled_input_preserves_full_report(ledger):
    original = backtest.evaluate_signal(ledger, "Entry Score", min_n=1, min_train=2, as_of=AS_OF)
    shuffled = deepcopy(ledger)
    random.Random(11).shuffle(shuffled)
    actual = backtest.evaluate_signal(shuffled, "Entry Score", min_n=1, min_train=2, as_of=AS_OF)
    assert actual == original


@pytest.mark.parametrize("rows", [[], [record("A", 1, 10, False)], [record("A", 1, 10, False), record("B", 1, 90, True)]])
def test_insufficient_history_is_pending_with_null_metrics(rows):
    result = backtest.evaluate_signal(rows, "Entry Score", min_n=1, min_train=2)
    assert result["verdict"] == "PENDING"
    assert result["walk_forward"]["model_brier"] is None
    assert result["cv_gain_vs_base"] is None
    assert "Pending:" in backtest.render([result], "short history")
    json.dumps(result, allow_nan=False)


def test_same_day_rows_are_never_split_between_train_and_test(ledger):
    result = walk(ledger, min_test_days=1)
    assert result["folds"][0]["test_count"] == 2
    assert result["folds"][0]["train_count"] == 2
    assert result["warmup_rows"] == 2


def test_unknown_or_retrospective_feature_cannot_assert_skill(ledger):
    rows = deepcopy(ledger)
    for row in rows:
        row["Current Price"] = "1" if row["Outcome"] == "WIN" else "0"
    result = backtest.evaluate_signal(rows, "Current Price", min_n=1, min_train=2)
    assert result["verdict"] == "PENDING"
    assert "feature_not_known_at_entry" in result["walk_forward"]["pending_reasons"]


def test_compatibility_in_sample_statistics_are_explicitly_exploratory(ledger):
    result = backtest.evaluate_signal(ledger, "Entry Score", min_n=1, min_train=2)
    assert result["compatibility_fields_scope"]["base_brier"] == "in_sample_exploratory"
    assert result["compatibility_fields_scope"]["spread_z"] == "in_sample_exploratory"
    assert result["compatibility_fields_scope"]["cv_gain_vs_base"] == "paired_same_heldout_rows_training_only_base"
    assert result["verdict"] in ("GAIN", "NO_GAIN")
    assert result["exploratory_verdict"] in ("SEPARATES", "WEAK", "NONE")
    assert "not a skill verdict" in backtest.render([result], "cohort")


@pytest.mark.parametrize("reverse", [False, True])
def test_duplicate_source_survives_descriptive_deduplication(ledger, reverse):
    rows = deepcopy(ledger)
    rows.append(dict(rows[2], **{"Outcome": "WIN", "Realized ROI %": "1"}))
    if reverse:
        rows.reverse()
    selected = backtest.decided_cohorts(rows)
    assert len(selected) == 6  # Existing descriptive first-occurrence contract.
    result = backtest.evaluate_signal(selected, "Entry Score", min_n=1, min_train=2, as_of=AS_OF)
    assert result["verdict"] == "PENDING"
    assert result["walk_forward"]["exclusions"]["duplicate_cohort"] == 1
    assert "C" not in {row["key"] for row in result["walk_forward"]["predictions"]}


def run_export(tmp_path, rows, *extra):
    source = tmp_path / "Performance_Log.tsv"
    headers = ["Record ID"] + [column for column in rows[0] if column != "Record ID"]
    with source.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        writer.writerow(headers)
        writer.writerows([[row.get(header, "") for header in headers] for row in rows])
    destination = tmp_path / "result.json"
    assert backtest.main(["--export-dir", str(tmp_path), "--signal", "Entry Score",
                          "--min-n", "1", "--min-train", "2", "--json", str(destination),
                          "--as-of-utc", AS_OF.isoformat(), *extra]) == 0
    return json.loads(destination.read_text())


def test_actual_cli_export_cannot_certify_conflicting_duplicate_cohorts(ledger, tmp_path):
    rows = deepcopy(ledger)
    rows.append(dict(rows[2], **{"Outcome": "WIN", "Realized ROI %": "1"}))
    first = run_export(tmp_path, rows)
    second = run_export(tmp_path, list(reversed(rows)))
    for output in (first, second):
        assert output["results"][0]["verdict"] == "PENDING"
        assert output["results"][0]["walk_forward"]["exclusions"]["duplicate_cohort"] == 1
    assert first["results"][0]["walk_forward"] == second["results"][0]["walk_forward"]


@pytest.mark.parametrize("missing", [None, "", "nan", "inf", "unparseable"])
def test_unobserved_numeric_feature_cannot_assert_no_gain(ledger, missing):
    rows = deepcopy(ledger)
    for row in rows:
        if missing is None:
            row.pop("Entry Score")
        else:
            row["Entry Score"] = missing
    result = backtest.evaluate_signal(rows, "Entry Score", min_n=1, min_train=2, as_of=AS_OF)
    assert result["verdict"] == "PENDING"
    assert result["n"] == 0
    assert result["walk_forward"]["heldout_rows"] == 0
    assert result["walk_forward"]["feature_warmup_rows"] == 4
    assert "insufficient_training_feature_history" in result["walk_forward"]["pending_reasons"]
    json.dumps(result, allow_nan=False)


def test_training_floor_counts_observed_features_not_only_labels(ledger):
    rows = deepcopy(ledger)
    rows[0]["Entry Score"] = ""
    result = walk(rows)
    assert result["status"] == "PENDING"
    assert result["heldout_rows"] == 0
    assert result["feature_warmup_rows"] == 4


def test_missing_heldout_signal_reports_fallbacks_and_pending(ledger):
    rows = deepcopy(ledger)
    for row in rows[2:]:
        row["Entry Score"] = ""
    result = backtest.evaluate_signal(rows, "Entry Score", min_n=1, min_train=2, as_of=AS_OF)
    walk_result = result["walk_forward"]
    assert result["verdict"] == "PENDING"
    assert walk_result["heldout_rows"] == 4
    assert walk_result["heldout_feature_observed_rows"] == 0
    assert walk_result["heldout_feature_missing_rows"] == 4
    assert walk_result["heldout_baseline_fallback_rows"] == 4
    assert walk_result["model_brier"] == walk_result["baseline_brier"]
    assert "insufficient_observed_heldout_rows" in walk_result["pending_reasons"]


def test_heldout_observed_floor_retains_same_rows_for_model_and_base(ledger):
    rows = deepcopy(ledger)
    rows[2]["Entry Score"] = ""
    result = backtest.evaluate_signal(rows, "Entry Score", min_n=4, min_train=2, as_of=AS_OF)
    metric = result["walk_forward"]
    assert result["verdict"] == "PENDING"
    assert metric["heldout_rows"] == 4
    assert metric["heldout_feature_observed_rows"] == 3
    assert metric["heldout_feature_missing_rows"] == 1
    assert metric["heldout_baseline_fallback_rows"] == 1
    assert {row["key"] for row in metric["predictions"]} == {"C", "D", "E", "F"}
    assert metric["baseline_brier"] == pytest.approx(sum(
        (row["baseline_probability"] - row["label"]) ** 2 for row in metric["predictions"]) / 4)


def test_future_matured_labels_are_excluded_from_heldout_scoring(ledger):
    rows = deepcopy(ledger)
    for column in ("Target Date (Riyadh)", "Maturity Date", "Last Updated (Riyadh)"):
        rows[-1][column] = rows[-1][column].replace("2026", "2099")
    result = backtest.evaluate_signal(rows, "Entry Score", min_n=1, min_train=2, as_of=AS_OF)
    assert result["verdict"] == "PENDING"
    assert result["walk_forward"]["exclusions"]["future_label_provenance"] == 1
    assert "F" not in {row["key"] for row in result["walk_forward"]["predictions"]}


def test_future_guard_uses_shared_clock_skew_and_precise_boundary(ledger):
    as_of = datetime(2026, 1, 5, 9, 5, tzinfo=RIYADH)
    assert walk(ledger, as_of=as_of)["status"] == "COMPLETE"  # Updated exactly +300 seconds.
    rows = deepcopy(ledger)
    rows[-1]["Last Updated (Riyadh)"] = "2026-01-05T09:10:01+03:00"
    result = walk(rows, as_of=as_of)
    assert result["exclusions"]["future_label_provenance"] == 1
    assert result["max_clock_skew_seconds"] == backtest.MAX_CLOCK_SKEW_SECONDS == 300


def test_same_day_date_only_availability_does_not_prove_future_label(ledger):
    rows = deepcopy(ledger)
    for row in rows:
        row["Maturity Date"] = row["Maturity Date"][:10]
    result = walk(rows, as_of=datetime(2026, 1, 5, 12, tzinfo=RIYADH))
    assert result["status"] == "COMPLETE"
    assert result["excluded_rows"] == 0
    # Day-end upper bounds still purge training conservatively.
    assert datetime.fromisoformat(result["folds"][0]["latest_training_label_utc"]).astimezone(RIYADH).hour == 23


@pytest.mark.parametrize("as_of", [datetime(2026, 1, 5), "2026-01-05", True])
def test_invalid_evaluation_time_is_not_silently_assumed(ledger, as_of):
    with pytest.raises(ValueError):
        walk(ledger, as_of=as_of)


def test_cli_all_signals_share_one_evaluation_cutoff(ledger, tmp_path):
    rows = deepcopy(ledger)
    for row in rows:
        row["Entry Forecast Reliability"] = row["Entry Score"]
    result = run_export(tmp_path, rows, "--signal", "Entry Forecast Reliability")
    assert len(result["results"]) == 2
    assert all(item["walk_forward"]["as_of_utc"] == result["as_of_utc"] == AS_OF.isoformat()
               for item in result["results"])


@pytest.mark.parametrize("target,maturity,updated", [
    ("2026-01-04", "2026-01-04T00:01:00+03:00", "2026-01-04T00:02:00+03:00"),
    ("2026-01-04", "2026-01-04T00:01:00+03:00", "2026-01-04T10:02:00+03:00"),
    ("2026-01-04", "2026-01-04", "2026-01-04T00:02:00+03:00"),
    ("2026-01-04T10:00:00+03:00", "2026-01-04", "2026-01-04T09:00:00+03:00"),
])
def test_coarse_dates_cannot_hide_provably_impossible_label_order(ledger, target, maturity, updated):
    rows = deepcopy(ledger)
    rows[-1].update({"Target Date (Riyadh)": target, "Maturity Date": maturity,
                     "Last Updated (Riyadh)": updated})
    result = backtest.evaluate_signal(rows, "Entry Score", min_n=1, min_train=2, as_of=AS_OF)
    assert result["verdict"] == "PENDING"
    assert result["walk_forward"]["exclusions"]["invalid_label_chronology"] == 1
    assert "F" not in {row["key"] for row in result["walk_forward"]["predictions"]}


@pytest.mark.parametrize("maturity", ["2026-01-04", "2026-01-04T10:01:00+03:00"])
def test_same_day_coarse_target_preserves_later_real_label_events(ledger, maturity):
    rows = deepcopy(ledger)
    rows[-1].update({"Target Date (Riyadh)": "2026-01-04", "Maturity Date": maturity,
                     "Last Updated (Riyadh)": "2026-01-04T10:02:00+03:00"})
    result = walk(rows)
    assert result["status"] == "COMPLETE"
    assert result["excluded_rows"] == 0


@pytest.mark.parametrize("missing_risk", [False, True])
def test_all_signals_cli_uses_actual_tracker_risk_header(ledger, tmp_path, missing_risk):
    # The producer schema is the fixture contract, independent of the
    # consumer's default list. Reading its literal avoids tracker startup.
    producer = Path(backtest.__file__).with_name("track_performance.py")
    cls = next(node for node in ast.parse(producer.read_text()).body
               if isinstance(node, ast.ClassDef) and node.name == "PerformanceStore")
    header_assignment = next(node for node in cls.body if isinstance(node, ast.Assign)
                             and any(isinstance(target, ast.Name) and target.id == "HEADERS"
                                     for target in node.targets))
    headers = ast.literal_eval(header_assignment.value)
    rows = deepcopy(ledger)
    for row in rows:
        row.update({"Entry Forecast Reliability": row["Entry Score"], "Confidence": "High",
                    "Entry Investability": "INVESTABLE", "Entry Recommendation": "BUY",
                    "Risk Bucket": "" if missing_risk else "LOW", "Horizon": "1W",
                    "Origin Tab": "Top_10_Investments"})
    source = tmp_path / "Performance_Log.tsv"
    with source.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        writer.writerow(headers)
        writer.writerows([[row.get(header, "") for header in headers] for row in rows])
    output = tmp_path / "all_signals.json"
    assert backtest.main(["--export-dir", str(tmp_path), "--all-signals", "--min-n", "1",
                          "--min-train", "2", "--as-of-utc", AS_OF.isoformat(),
                          "--json", str(output)]) == 0
    result = json.loads(output.read_text())
    assert result["missing"] == []
    assert len(result["results"]) == 8
    risk = next(item for item in result["results"] if item["signal"] == "Risk Bucket")
    assert risk["verdict"] == ("PENDING" if missing_risk else "NO_GAIN")
    if missing_risk:
        assert risk["walk_forward"]["heldout_feature_observed_rows"] == 0
        assert "insufficient_training_feature_history" in risk["walk_forward"]["pending_reasons"]
