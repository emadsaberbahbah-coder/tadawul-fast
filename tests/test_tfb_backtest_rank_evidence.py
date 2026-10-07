"""Known-answer rank evidence and the actual backtest report boundary.

The references below are Pearson correlations of explicitly hand-ranked
vectors, calculated by the independent standard-library correlation routine.
No provider, workbook, model fitting, or optional scientific package is used.
"""
import csv
import itertools
import json
import math
import statistics

import pytest

from scripts import tfb_backtest as backtest


# With ties, the old no-ties shortcut returned -0.5 instead of -1, and +0.05
# instead of -1/18 for the asymmetric fixture (a reversal of the evidence sign).
RANK_FIXTURES = [
    ([1, 2, 3, 4, 5], [2, 4, 1, 5, 3], [1, 2, 3, 4, 5], [2, 4, 1, 5, 3], 0.3),
    ([1, 1, 2], [2, 2, 1], [1.5, 1.5, 3], [2.5, 2.5, 1], -1.0),
    ([1, 1, 2, 3], [4, 1, 1, 2], [1.5, 1.5, 3, 4], [4, 1.5, 1.5, 3], -1 / 18),
    ([1, 1, 2, 3], [1, 1, 2, 3], [1.5, 1.5, 3, 4], [1.5, 1.5, 3, 4], 1.0),
    ([1, 2, 3], [3, 2, 1], [1, 2, 3], [3, 2, 1], -1.0),
]


@pytest.mark.parametrize("scores,returns,score_ranks,return_ranks,expected", RANK_FIXTURES)
def test_spearman_matches_hand_ranked_independent_reference(
    scores, returns, score_ranks, return_ranks, expected
):
    reference = statistics.correlation(score_ranks, return_ranks)
    assert reference == pytest.approx(expected, abs=1e-12)
    assert backtest.spearman(scores, returns) == pytest.approx(reference, abs=1e-12)
    assert backtest.spearman(returns, scores) == pytest.approx(reference, abs=1e-12)


def test_rank_evidence_preserves_pairs_under_every_row_permutation():
    scores, returns = [1, 1, 2, 3], [4, 1, 1, 2]
    for permutation in itertools.permutations(range(4)):
        assert backtest.spearman(
            [scores[i] for i in permutation], [returns[i] for i in permutation]
        ) == pytest.approx(-1 / 18, abs=1e-12)


def test_rank_evidence_invariant_to_strictly_increasing_transforms():
    assert backtest.spearman([10, 10, 100, 1000], [16, 1, 1, 4]) == pytest.approx(-1 / 18)


@pytest.mark.parametrize("scores,returns", [
    ([], []), ([1], [2]), ([1, 2], [2, 1]),
    ([1, 1, 1], [1, 2, 3]), ([1, 2, 3], [1, 1, 1]), ([1, 1, 1], [2, 2, 2]),
])
def test_undefined_float_api_remains_nan(scores, returns):
    assert math.isnan(backtest.spearman(scores, returns))


@pytest.mark.parametrize("scores,returns", [
    ([1, 2, 3], [1, 2]), ([], [1]),
    ([1, float("nan"), 3], [1, 2, 3]), ([1, 2, 3], [1, float("inf"), 3]),
    ([float("-inf")], [1]), ([None], [1]),
])
def test_invalid_pairs_rejected_before_sample_size_guard(scores, returns):
    with pytest.raises(ValueError):
        backtest.spearman(scores, returns)


def _rows(scores, returns):
    return [
        {"Record ID": str(i), "Key": f"synthetic-{i}", "Status": "matured",
         "Outcome": "WIN" if roi > 0 else "LOSS", "Signal": str(score),
         "Realized ROI %": str(roi)}
        for i, (score, roi) in enumerate(zip(scores, returns), start=1)
    ]


@pytest.mark.parametrize("scores,returns,status", [
    ([75], [1], "insufficient_samples"),
    ([75, 75], [1, 2], "insufficient_samples"),
    ([75, 75, 75], [-1, 0, 1], "constant_signal"),
    ([25, 50, 75], [1, 1, 1], "constant_roi"),
    ([75, 75, 75], [1, 1, 1], "constant_signal_and_roi"),
])
def test_evaluator_reports_undefined_as_json_null_with_reason(scores, returns, status):
    result = backtest.evaluate_signal(_rows(scores, returns), "Signal")
    assert result["spearman_vs_roi"] is None
    assert result["spearman_status"] == status
    # Report consumers receive valid JSON, not an invented zero or NaN token.
    assert json.loads(json.dumps(result, allow_nan=False))["spearman_vs_roi"] is None
    assert f"Spearman vs ROI undefined ({status})" in backtest.render([result], "synthetic")


def test_evaluator_keeps_finite_observation_pairs_aligned_when_score_missing():
    rows = _rows(list(range(1, 11)), list(range(1, 11)))
    rows[4]["Signal"] = "NaN"
    result = backtest.evaluate_signal(backtest.decided_cohorts(rows), "Signal")
    assert result["n"] == 9
    assert result["spearman_vs_roi"] == 1.0
    assert result["spearman_status"] == "defined"


def test_decided_cohort_excludes_unwitnessed_returns_before_rank_evaluation():
    rows = _rows([1, 2, 3, 4], [4, 3, 2, 1])
    rows.append(dict(rows[0], **{"Record ID": "5", "Key": "synthetic-5", "Realized ROI %": ""}))
    result = backtest.evaluate_signal(backtest.decided_cohorts(rows), "Signal")
    assert result["n"] == 4
    assert result["spearman_vs_roi"] == -1.0


def test_console_displays_rank_evidence_for_numeric_signals_outside_probability_range():
    result = backtest.evaluate_signal(_rows([101, 101, 102, 103], [4, 1, 1, 2]), "Signal")
    assert "raw_brier_as_probability" not in result
    assert result["spearman_vs_roi"] == -0.056
    assert result["spearman_status"] == "defined"
    assert "Spearman vs ROI -0.056" in backtest.render([result], "synthetic")


def test_non_rank_metrics_preserve_pre_repair_golden_values():
    result = backtest.evaluate_signal(
        _rows([25, 25, 50, 75, 75, 100], [-2, -1, 1, 3, 2, 4]),
        "Signal", edges=[50, 75], min_n=2,
    )
    assert {k: v for k, v in result.items() if not k.startswith("spearman")} == {
        "signal": "Signal", "type": "numeric", "n": 6,
        "base_win_pct": 66.7, "base_brier": 0.2222,
        "groups": [
            {"group": "50-75", "n": 1, "win_pct": 100.0, "mean_roi_pct": 1.0, "median_roi_pct": 1.0},
            {"group": "<50", "n": 2, "win_pct": 0.0, "mean_roi_pct": -1.5, "median_roi_pct": -1.5},
            {"group": ">=75", "n": 3, "win_pct": 100.0, "mean_roi_pct": 3.0, "median_roi_pct": 3.0},
        ],
        "raw_brier_as_probability": 0.0833, "cv_brier_group_calibrated": 0.3137,
        "win_spread_pp": 100.0, "spread_z": 2.32, "cv_gain_vs_base": -0.0915, "verdict": "NONE",
    }


def test_actual_cli_serializes_null_and_prints_undefined_without_live_io(tmp_path, capsys):
    rows = _rows([75, 75, 75], [-1, 0, 1])
    path = tmp_path / "synthetic_Performance_Log.tsv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    report = tmp_path / "report.json"
    assert backtest.main(["--export-dir", str(tmp_path), "--signal", "Signal", "--json", str(report)]) == 0
    payload = json.loads(report.read_text(encoding="utf-8"), parse_constant=lambda token: pytest.fail(token))
    assert payload["version"] == "1.1.1"
    assert payload["results"][0]["spearman_vs_roi"] is None
    assert payload["results"][0]["spearman_status"] == "constant_signal"
    assert "Spearman vs ROI undefined (constant_signal)" in capsys.readouterr().out
