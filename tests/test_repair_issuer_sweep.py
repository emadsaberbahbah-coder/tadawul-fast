#!/usr/bin/env python3
"""tests/test_repair_issuer_sweep.py — repair_stores v1.1.0 Phase B4.

Nine cases over the pure helpers and a Fake-gspread integration: violations
found for both live poison classes, venue contradiction, clean rows and
already-stubbed rows skipped, DRY writes nothing, APPLY quarantines-then-
stubs with Symbol preserved, kill-switch skip, csi-missing skip, and the
known-defect seed report incl. the one-dispatch instructions.
"""
from __future__ import annotations

import importlib
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

rs = importlib.import_module("repair_stores")

HDR = ["Symbol", "Name", "Current Price", "EPS", "P/E", "Exchange",
       "Currency", "Country", "Warnings"]


def _grid():
    return [
        list(HDR),
        ["1120.SR", "Al Rajhi Bank", "98.5", "5.0", "19.7", "Tadawul", "SAR", "Saudi Arabia", ""],
        ["DENN.US", "Amer Sports, Inc.", "24.1", "1.0", "24.1", "NYSE", "USD", "USA", ""],
        ["BF-A", "Biofrontera Inc", "1.2", "0.1", "12.0", "NASDAQ", "USD", "USA", ""],
        ["TAQA.AB", "Abu Dhabi National Energy", "3.5", "0.2", "17.5", "NASDAQ", "USD", "", ""],
        ["BK.US", "", "", "", "", "", "", "", ""],
        ["VICI", "Random REIT Corp", "31.0", "2.0", "15.5", "NYSE", "USD", "USA", ""],
    ]


class FakeWS:
    def __init__(self, grid):
        self.grid = grid
        self.batch_updates = []
        self.appended = []

    def get_all_values(self):
        return [list(r) for r in self.grid]

    def batch_update(self, updates, value_input_option=None):
        self.batch_updates.append(updates)

    def append_row(self, row, value_input_option=None):
        self.appended.append(row)

    def append_rows(self, rows, value_input_option=None):
        self.appended.extend(rows)


class FakeSheet:
    def __init__(self, pages):
        self.pages = dict(pages)
        self.added = []

    def worksheet(self, name):
        if name in self.pages:
            return self.pages[name]
        raise KeyError(name)

    def add_worksheet(self, title, rows, cols):
        ws = FakeWS([])
        self.pages[title] = ws
        self.added.append(title)
        return ws


def _env(pages="Market_Leaders"):
    os.environ["REPAIR_PAGES"] = pages
    os.environ.pop("REPAIR_ISSUER_SWEEP", None)


def test_1_violations_found_both_live_cases_and_venue():
    v = rs._issuer_violations(HDR, _grid())
    syms = [x[1] for x in v]
    assert "DENN.US" in syms and "BF-A" in syms and "TAQA.AB" in syms


def test_2_clean_and_unlisted_rows_pass():
    v = rs._issuer_violations(HDR, _grid())
    syms = [x[1] for x in v]
    assert "1120.SR" not in syms and "VICI" not in syms


def test_3_already_stubbed_row_skipped():
    v = rs._issuer_violations(HDR, _grid())
    assert "BK.US" not in [x[1] for x in v], "clean stub must not be re-flagged"


def test_4_dry_writes_nothing():
    _env()
    ws = FakeWS(_grid())
    sheet = FakeSheet({"Market_Leaders": ws})
    out = rs.phase_b4_issuer(sheet, apply=False)
    assert out["pages"]["Market_Leaders"]["issuer_corrupt"] == 3
    assert ws.batch_updates == [] and ws.appended == [] and sheet.added == []


def test_5_apply_quarantines_then_stubs():
    _env()
    ws = FakeWS(_grid())
    sheet = FakeSheet({"Market_Leaders": ws})
    out = rs.phase_b4_issuer(sheet, apply=True)
    rep = out["pages"]["Market_Leaders"]
    assert rep.get("quarantined") == 3 and rep.get("repaired") == 3
    assert "_Identity_Quarantine" in sheet.added
    q = sheet.pages["_Identity_Quarantine"]
    assert q.appended[0][0] == "Timestamp"          # header row
    assert {r[3] for r in q.appended[1:]} == {"DENN.US", "BF-A", "TAQA.AB"}
    stub_rows = [u for batch in ws.batch_updates for u in batch]
    assert len(stub_rows) == 3
    for u in stub_rows:
        row = u["values"][0]
        assert row[0] in {"DENN.US", "BF-A", "TAQA.AB"}     # Symbol preserved
        assert row[-1].startswith("identity_repaired:issuer:v1.1.0")
        assert all(c == "" for c in row[1:-1])


def test_6_killswitch_skips_byte_identical():
    _env()
    os.environ["REPAIR_ISSUER_SWEEP"] = "0"
    ws = FakeWS(_grid())
    out = rs.phase_b4_issuer(FakeSheet({"Market_Leaders": ws}), apply=True)
    os.environ.pop("REPAIR_ISSUER_SWEEP", None)
    assert out.get("skipped") == "REPAIR_ISSUER_SWEEP=0"
    assert ws.batch_updates == [] and ws.appended == []


def test_7_csi_missing_skips():
    _env()
    saved = rs._csi_identity
    rs._csi_identity = None
    try:
        out = rs.phase_b4_issuer(FakeSheet({"Market_Leaders": FakeWS(_grid())}), apply=True)
        assert out.get("skipped") == "critical_symbol_identity unavailable"
    finally:
        rs._csi_identity = saved


def test_8_known_defect_report():
    kd = {e["symbol"]: e for e in rs._known_defect_status(HDR, _grid())}
    assert kd["BK.US"]["present"] and kd["BK.US"]["blank_stub"] and kd["BK.US"]["needs_seed"]
    assert not kd["FISV.US"]["present"] and kd["FISV.US"]["needs_seed"]
    assert len(kd) == 6


def test_9_seed_instructions_printed_fields():
    _env()
    out = rs.phase_b4_issuer(FakeSheet({"Market_Leaders": FakeWS(_grid())}), apply=False)
    assert any(t.endswith("BK.US") for t in out["seed_needed"])
    ins = out.get("seed_instructions", "")
    assert "TFB_SYNC_FORCE_REFETCH_SYMBOLS=" in ins and "ONE workflow dispatch" in ins
    assert "REMOVE the flag block" in ins


if __name__ == "__main__":
    for fn in sorted(k for k in dir() if k.startswith("test_")):
        globals()[fn]()
    print("SELFTEST 9/9 PASS — Phase B4 sweep, quarantine-then-stub, seed report proven")
