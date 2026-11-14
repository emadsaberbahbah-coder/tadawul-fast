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
