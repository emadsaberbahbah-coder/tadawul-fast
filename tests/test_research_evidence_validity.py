"""Correlation oracle fixtures and unknown CA/PIT evidence cannot promote S1."""
from datetime import date
import json
import math

import pytest

from core import corporate_actions as ca
from scripts import run_shadow_scorer as scorer
from scripts import tfb_backtest as backtest


# Golden values from scipy.stats.spearmanr (average ties, nan_policy='propagate').
@pytest.mark.parametrize("a,b,expected", [
    ([1, 2, 3, 4], [1, 2, 3, 4], 1.0),
    ([1, 2, 3, 4], [4, 3, 2, 1], -1.0),
    ([1, 2, 2, 3], [1, 2, 3, 4], 0.9486832980505138),
    ([1, 1, 2, 2], [1, 2, 1, 2], 0.0),
    ([1, 1, 2, 3], [3, 3, 2, 1], -1.0),
])
def test_spearman_matches_trusted_tie_and_reversal_fixtures(a, b, expected):
    assert backtest.spearman(a, b) == pytest.approx(expected, abs=1e-12)


@pytest.mark.parametrize("a,b", [
    ([70] * 5, [1, 2, 3, 4, 5]),
    ([1, 2, 3], [70] * 3),
    ([70] * 3, [70] * 3),
    ([], []), ([1, 2], [2, 1]), ([1, 2, 3], [1, 2]),
    ([1, math.nan, 3], [1, 2, 3]),
    ([1, math.inf, 3], [1, 2, 3]),
    ([1, 2, 3], [1, -math.inf, 3]),
])
def test_spearman_undefined_inputs_never_report_skill(a, b):
    assert math.isnan(backtest.spearman(a, b))


def test_constant_signal_result_uses_json_null_and_explicit_text():
    cohorts = [{"Score": "70", "Outcome": "WIN", "Realized ROI %": str(i)}
               for i in range(1, 6)]
    result = backtest.evaluate_signal(cohorts, "Score")
    assert result["spearman_vs_roi"] is None
    assert '"spearman_vs_roi": null' in json.dumps(result, allow_nan=False)
    assert "Spearman vs ROI undefined" in backtest.render([result], "constant input")


class Worksheet:
    def __init__(self, values):
        self.values = values

    def get_all_values(self):
        if isinstance(self.values, Exception):
            raise self.values
        return self.values


class Sheet:
    def __init__(self, actions=None, records=None):
        self.tabs = {}
        if actions is not None:
            self.tabs[ca.TAB_ACTIONS] = Worksheet(actions)
        if records is not None:
            self.tabs["Performance_Log"] = Worksheet(records)

    def worksheet(self, name):
        return self.tabs[name]


ACTION = ["A.US", "SPLIT", "2026-10-01", "2", "", "", "CONFIRMED", "", ""]
PL_HEADER = ["Record ID", "Symbol", "Date Recorded (Riyadh)", "Entry Price",
             "Target Price", "Status", "Current Price", "Notes"]
PL_ROW = ["R1", "A.US", "2026-10-02", "20", "25", "active", "20", ""]


@pytest.mark.parametrize("actions,records", [
    (None, [PL_HEADER, PL_ROW]),
    (RuntimeError("offline sheet"), [PL_HEADER, PL_ROW]),
    ([], [PL_HEADER, PL_ROW]),
    ([ca.ACTIONS_HEADER], [PL_HEADER, PL_ROW]),
    ([ca.ACTIONS_HEADER, ["garbled"]], [PL_HEADER, PL_ROW]),
    ([["wrong", "header"], ACTION], [PL_HEADER, PL_ROW]),
    ([ca.ACTIONS_HEADER, ACTION], None),
    ([ca.ACTIONS_HEADER, ACTION], RuntimeError("offline log")),
    ([ca.ACTIONS_HEADER, ACTION], []),
    ([ca.ACTIONS_HEADER, ACTION], [PL_HEADER]),
    ([ca.ACTIONS_HEADER, ACTION], [["Record ID", "Symbol"], ["R1", "A.US"]]),
    ([ca.ACTIONS_HEADER, ACTION], [PL_HEADER, ["R1", "A.US", "unknown", "20"]]),
    ([ca.ACTIONS_HEADER, ACTION], [PL_HEADER, ["R1", "A.US", "2026-10-02", "bad"]]),
])
def test_unknown_corporate_action_or_log_evidence_cannot_pass(actions, records):
    state = scorer.ca_is_clean(Sheet(actions, records))
    assert state is None
    gate = scorer.evaluate_s1(30, [], 5.0, "PASS", state, True, "history intact", "2026-10-01")
    assert gate["criteria"][4]["status"] == "PENDING"
    assert gate["verdict"] == "NOT_DECIDABLE"


def test_confirmed_action_with_no_outstanding_repairs_can_pass_recorded_check():
    assert scorer.ca_is_clean(Sheet([ca.ACTIONS_HEADER, ACTION], [PL_HEADER, PL_ROW])) is True


def test_outstanding_confirmed_repair_remains_failure():
    row = list(PL_ROW)
    row[2] = "2026-09-30"
    assert scorer.ca_is_clean(Sheet([ca.ACTIONS_HEADER, ACTION], [PL_HEADER, row])) is False


def test_known_repair_is_not_erased_by_unrelated_unknown_record():
    unrepaired = list(PL_ROW)
    unrepaired[2] = "2026-09-30"
    unrelated_unknown = ["", "B.US", "2026-10-02", "20", "25", "active", "20", ""]
    state = scorer.ca_is_clean(Sheet(
        [ca.ACTIONS_HEADER, ACTION], [PL_HEADER, unrepaired, unrelated_unknown],
    ))
    assert state is False
    gate = scorer.evaluate_s1(30, [], 5.0, "PASS", state, True, "history intact", "2026-10-01")
    assert gate["criteria"][4]["status"] == "FAIL"
    assert "UNREPAIRED" in gate["criteria"][4]["detail"]
    assert gate["verdict"] == "FAIL"


def test_unreviewed_action_proposal_is_unknown():
    action = list(ACTION)
    action[6] = ca.SOURCE_AUTO
    assert scorer.ca_is_clean(Sheet([ca.ACTIONS_HEADER, action], [PL_HEADER, PL_ROW])) is None


@pytest.mark.parametrize("history", [[], [{"basket": "B", "date": ""}], [{"basket": "B", "date": "bad"}]])
def test_missing_point_in_time_history_is_unknown(history):
    state, note = scorer.check_point_in_time(history)
    assert state is None
    gate = scorer.evaluate_s1(30, [], 5.0, "PASS", True, state, note, "2026-10-01")
    assert gate["criteria"][4]["status"] == "PENDING"
    assert gate["verdict"] == "NOT_DECIDABLE"


def test_unknown_pit_row_does_not_hide_duplicate_dates():
    state, note = scorer.check_point_in_time([
        {"basket": "B", "date": ""},
        {"basket": "B", "date": "2026-10-01"},
        {"basket": "B", "date": "2026-10-01"},
    ])
    assert state is False
    assert "duplicate dates" in note


def test_equivalent_iso_date_spellings_are_duplicate_pit_days():
    state, note = scorer.check_point_in_time([
        {"basket": "B", "date": "2026-10-01"},
        {"basket": "B", "date": "20261001"},
    ])
    assert state is False
    assert "duplicate dates" in note


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("ca_rows", [None, 0, 1])
def test_missing_evidence_does_not_erase_known_pit_breach(mode, ca_rows):
    gate = scorer.evaluate_s1(30, [], 5.0, "PASS", None, False, "duplicate dates", "2026-10-01")
    result = scorer.evaluate_s1_v2(gate, mode, 5.0, date(2026, 9, 16), 3.0, 2.0, ca_rows)
    assert result["criteria"][4]["status"] == "FAIL"
    assert result["verdict"] == "FAIL"
