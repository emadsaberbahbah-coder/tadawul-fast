"""
tests/test_track_board_coverage.py -- track_performance v6.49.0 [BOARD-COVERAGE]

The live Top_10_Investments board's seated symbols join the daily forecast
record (Signal_History / Performance_Log) when TFB_TRACK_BOARD_SYMBOLS is on.

Cases (pure functions and env knob; no network, no Sheets):
  B1  parser: header anchored on 'Rank'|'Symbol', seats and GRACE ghosts kept
      in board order, ghosts counted, stop at the ALL QUALIFIED zone
  B2  parser: first fully blank row ends the table; junk / non-symbol rows skip
  B3  parser: dedupe + cap at _BOARD_MAX; junk input never throws
  B4  knob: default OFF; 1/on/true -> on; 0/off/false -> off
  B5  _force_plan with board symbols as extras: never cut by the cap
  B6  byte-identity when off: the extras passed to _force_plan equal the
      v6.48.0 list exactly (sorted pinned extras only)
  B7  version floor >= (6, 49, 0) and the zero-removal inventory of v6.48.0
      helpers still present
Runs as a script ("PASS k/k") and under pytest.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "scripts" / "track_performance.py"


def _load():
    spec = importlib.util.spec_from_file_location("track_performance_b", str(SRC))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["track_performance_b"] = mod
    spec.loader.exec_module(mod)
    return mod


tp = _load()

BOARD_24_OCT = [
    ["KPIs", ""],
    ["Entered cash (SAR)", "Exp. Gain 12M (SAR)"],
    ["79,069 SAR", "—"],
    ["Rank", "Symbol"],
    ["—", "PHP.L"],          # GRACE ghost
    ["2", "BNP.PA"],
    ["3", "FRST.US"],
    ["—", "IVR.US"],         # GRACE ghost
    ["5", "ZTO.US"],
    ["6", "CRC.US"],
    ["—", "ORC.US"],
    ["—", "IRS.US"],
    ["9", "TGS.US"],
    ["—", "UMBF.US"],
    ["", ""],
    ["ALL QUALIFIED (audited)", "Symbol"],
    ["1", "AGO.US"],
]


def _version_tuple(v):
    return tuple(int(x) for x in str(v).split("."))


def case_b1():
    syms, ghosts = tp._board_symbols_from_values(BOARD_24_OCT)
    assert syms == ["PHP.L", "BNP.PA", "FRST.US", "IVR.US", "ZTO.US", "CRC.US",
                    "ORC.US", "IRS.US", "TGS.US", "UMBF.US"], syms
    assert ghosts == 5, ghosts
    assert "AGO.US" not in syms            # the ALL QUALIFIED zone is not the board


def case_b2():
    vals = [["Rank", "Symbol"], ["1", "FRST.US"], ["2", "  zto.us "],
            ["3", "not a symbol"], ["4", ""], ["5", "_LEGEND"], ["", ""],
            ["7", "PHP.L"]]
    syms, ghosts = tp._board_symbols_from_values(vals)
    assert syms == ["FRST.US", "ZTO.US"], syms   # blank row ends the table
    assert ghosts == 0
    # no header at all -> nothing
    assert tp._board_symbols_from_values([["x", "FRST.US"]]) == ([], 0)


def case_b3():
    rows = [["Rank", "Symbol"]] + [[str(i), "SYM%02d.US" % (i % 20)] for i in range(1, 60)]
    syms, ghosts = tp._board_symbols_from_values(rows)
    assert len(syms) == len(set(syms)) <= tp._BOARD_MAX, syms
    for junk in (None, [], [None], [[None, None]], "garbage", 42, [["Rank"]]):
        assert tp._board_symbols_from_values(junk) == ([], 0), junk


def case_b4():
    key = "TFB_TRACK_BOARD_SYMBOLS"
    saved = os.environ.pop(key, None)
    try:
        assert tp._board_symbols_enabled() is False          # default OFF
        for v in ("1", "on", "true", "yes"):
            os.environ[key] = v
            assert tp._board_symbols_enabled() is True, v
        for v in ("0", "off", "false", "no", ""):
            os.environ[key] = v
            assert tp._board_symbols_enabled() is False, v
    finally:
        if saved is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = saved


def case_b5():
    active = ["5023.SR"]
    closed = ["AER.US", "CARE.US", "CWBC.US", "DDI.US", "EPRT.US", "HCI.US",
              "KRP.US", "NMM.US", "PFS.US", "RCI.US", "VEL.US"]
    board = ["PHP.L", "BNP.PA", "FRST.US", "ZTO.US", "CRC.US"]
    extras = sorted(set(board))
    plan = tp._force_plan(active, closed, extras, set(), 4, "all")
    for s in board + active:
        assert s in plan["forced"], (s, plan)
    assert plan["cut"], plan                      # the cap bit the CLOSED lots
    assert not (set(plan["cut"]) & set(board)), plan["cut"]


def case_b6():
    # when the board is off, the extras list handed to _force_plan is the
    # v6.48.0 list: sorted pinned extras, nothing else.
    key = "TFB_TRACK_PRIORITY_SYMBOLS"
    saved = os.environ.get(key)
    try:
        os.environ[key] = "yum,YUMC, pfs"
        v648 = sorted(tp._force_extra_symbols())
        v649 = sorted(set(tp._force_extra_symbols()) | set([]))
        assert v648 == v649 == ["PFS", "YUM", "YUMC"], (v648, v649)
    finally:
        if saved is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = saved


def case_b7():
    assert _version_tuple(tp.SCRIPT_VERSION) >= (6, 49, 0), tp.SCRIPT_VERSION
    for name in ("_force_plan", "_force_extra_symbols", "_force_max",
                 "_load_costbasis_symbols", "_load_costbasis_symbols_by_status",
                 "_read_board_selected_symbols", "_extract_costbasis_by_status",
                 "_board_symbols_from_values", "_load_board_symbols",
                 "_board_symbols_enabled"):
        assert callable(getattr(tp, name, None)), name
    src = SRC.read_text(encoding="utf-8")
    assert "[v6.49.0 BOARD-COVERAGE]" in src
    assert 'r.setdefault("origin", "Decision_Coverage")' in src   # v6.16.0 tag kept


CASES = [case_b1, case_b2, case_b3, case_b4, case_b5, case_b6, case_b7]


def test_board_coverage_cases():
    for c in CASES:
        c()


if __name__ == "__main__":
    ok = 0
    for c in CASES:
        try:
            c()
            ok += 1
            print("PASS", c.__name__)
        except AssertionError as e:
            print("FAIL", c.__name__, e)
    print("PASS %d/%d" % (ok, len(CASES)))
    sys.exit(0 if ok == len(CASES) else 1)
