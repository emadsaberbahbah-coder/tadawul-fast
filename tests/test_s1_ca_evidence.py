"""run_shadow_scorer 1.9.6: no corporate-action evidence is not a PASS (review of 14 Nov, P1).

An empty _Corporate_Actions register, or one that cannot be read, leaves criterion 5 PENDING and the gate
NOT_DECIDABLE. A populated register with an unrepaired confirmed action still FAILs. TFB_S1_CA_EVIDENCE=legacy
restores the 1.9.5 answer (True).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import run_shadow_scorer as ss  # noqa: E402

HEADER = ["Symbol", "Action Type", "Effective Date", "Ratio", "Old Symbol", "New Symbol", "Source", "Detected At",
          "Notes"]


class Tab:
    def __init__(self, values):
        self.values = values

    def get_all_values(self):
        if isinstance(self.values, Exception):
            raise self.values
        return self.values


class Book:
    def __init__(self, tabs):
        self.tabs = tabs

    def worksheet(self, name):
        if name not in self.tabs:
            raise KeyError(name)
        return Tab(self.tabs[name])


def _gate(ca):
    return ss.evaluate_s1(30, [], 1.0, "PASS", ca, True, "append-only integrity intact", "2026-11-01")


def test_an_empty_register_is_not_evaluable(monkeypatch):
    monkeypatch.delenv("TFB_S1_CA_EVIDENCE", raising=False)
    assert ss.ca_is_clean(Book({"_Corporate_Actions": [HEADER]})) is None
    assert ss.ca_is_clean(Book({"_Corporate_Actions": [HEADER, ["", "", ""], [" "]]})) is None
    g = _gate(None)
    assert g["criteria"][4]["status"] == "PENDING" and "not evaluable" in g["criteria"][4]["detail"]
    assert g["verdict"] == "NOT_DECIDABLE" and "5" in g["why"]


def test_an_unreadable_register_is_not_evaluable(monkeypatch):
    monkeypatch.delenv("TFB_S1_CA_EVIDENCE", raising=False)
    assert ss.ca_is_clean(Book({"_Corporate_Actions": ConnectionError("quota")})) is None
    assert ss.ca_is_clean(Book({})) is None


def test_a_register_with_rows_is_evidence(monkeypatch):
    monkeypatch.delenv("TFB_S1_CA_EVIDENCE", raising=False)
    rows = [HEADER, ["DDI.US", "DIVIDEND", "2026-10-15", "1.0"]]          # no split to repair: clean
    assert ss.ca_is_clean(Book({"_Corporate_Actions": rows, "Performance_Log": [["Symbol"]]})) is True


def test_evidence_still_decides_when_it_exists():
    assert _gate(True)["verdict"] == "PASS"
    bad = _gate(False)
    assert bad["verdict"] == "FAIL" and "UNREPAIRED" in bad["criteria"][4]["detail"]
    pit = ss.evaluate_s1(30, [], 1.0, "PASS", None, False, "duplicate date", "2026-11-01")
    assert pit["criteria"][4]["status"] == "FAIL"                   # a point-in-time breach still fails


def test_legacy_restores_the_old_answer(monkeypatch):
    monkeypatch.setenv("TFB_S1_CA_EVIDENCE", "legacy")
    assert ss.ca_is_clean(Book({"_Corporate_Actions": [HEADER]})) is True
    assert ss.ca_is_clean(Book({})) is True
    monkeypatch.setenv("TFB_S1_CA_EVIDENCE", "junk")
    assert ss.ca_is_clean(Book({"_Corporate_Actions": [HEADER]})) is None


# ----------------------------------------------------------------------------- 1.9.7: a check that could not run
LOG_HDR = ["Record ID", "Symbol", "Date Recorded (Riyadh)", "Entry Price", "Target Price", "Status", "Current Price", "Notes"]
SPLIT = ["WXYZ.US", "REVERSE_SPLIT", "2026-10-20", "20", "", "", "CONFIRMED", "", ""]
PROPOSAL = ["PROP.US", "SPLIT", "2026-10-12", "5", "", "", "AUTO_DETECT", "", ""]
OLD_ROW = ["r1", "WXYZ.US", "2026-09-24", "0.55", "0.70", "active", "0.50", ""]          # recorded before the split
DONE_ROW = ["r1", "WXYZ.US", "2026-09-24", "11.0", "14.0", "active", "7.0", "ca_adjusted x20"]


def test_a_register_of_proposals_only_is_not_evaluable(monkeypatch):
    """Review of 15 Nov: nothing was confirmed, so the repair plan is empty and proves nothing."""
    monkeypatch.delenv("TFB_S1_CA_EVIDENCE", raising=False)
    book = Book({"_Corporate_Actions": [HEADER, PROPOSAL, PROPOSAL], "Performance_Log": [LOG_HDR, OLD_ROW]})
    assert ss.ca_is_clean(book) is None
    assert ss.ca_is_clean(Book({"_Corporate_Actions": [HEADER, PROPOSAL]})) is None      # the log is not even needed
    monkeypatch.setenv("TFB_S1_CA_EVIDENCE", "legacy")
    assert ss.ca_is_clean(book) is True


def test_a_confirmed_action_with_a_log_that_cannot_be_evaluated_is_not_evaluable(monkeypatch):
    monkeypatch.delenv("TFB_S1_CA_EVIDENCE", raising=False)
    for bad_log in ([["Symbol"], OLD_ROW],                                              # no header row found
                    [],                                                                 # empty
                    [[c for c in LOG_HDR if c != "Entry Price"], OLD_ROW],              # a column the check reads is gone
                    [[c for c in LOG_HDR if c != "Notes"], OLD_ROW],
                    [[c for c in LOG_HDR if c != "Date Recorded (Riyadh)"], OLD_ROW],
                    [["Record ID"] + [c for c in LOG_HDR[2:]], OLD_ROW]):               # no Symbol column
        assert ss.ca_is_clean(Book({"_Corporate_Actions": [HEADER, SPLIT], "Performance_Log": bad_log})) is None, bad_log[:1]
    monkeypatch.setenv("TFB_S1_CA_EVIDENCE", "legacy")
    assert ss.ca_is_clean(Book({"_Corporate_Actions": [HEADER, SPLIT], "Performance_Log": [["Symbol"], OLD_ROW]})) is True


def test_a_confirmed_action_and_a_readable_log_are_judged_as_before(monkeypatch):
    """The live case of 15 Nov: one confirmed action beside proposals."""
    monkeypatch.delenv("TFB_S1_CA_EVIDENCE", raising=False)
    reg = [HEADER, SPLIT, PROPOSAL, PROPOSAL]
    assert ss.ca_is_clean(Book({"_Corporate_Actions": reg, "Performance_Log": [LOG_HDR, OLD_ROW]})) is False    # unrepaired
    assert ss.ca_is_clean(Book({"_Corporate_Actions": reg, "Performance_Log": [LOG_HDR, DONE_ROW]})) is True    # repaired
    assert ss.ca_is_clean(Book({"_Corporate_Actions": reg, "Performance_Log": [LOG_HDR]})) is True              # nothing to repair
    assert ss.ca_is_clean(Book({"_Corporate_Actions": reg, "Performance_Log": [LOG_HDR + ["Extra"], DONE_ROW + [""]]})) is True
    assert ss.SCRIPT_VERSION == "1.9.8"
