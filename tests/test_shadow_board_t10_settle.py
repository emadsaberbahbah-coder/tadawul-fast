#!/usr/bin/env python3
"""tests/test_shadow_board_t10_settle.py - run_shadow_board v1.6.0 (D7).

WHY (2026-10-28): evening Shadow_Board runs read 0 candidates on 20, 21, 23
and 27 Oct while morning runs read 6-25. On 27 Oct the job read
Top_10_Investments at ~22:25 Riyadh, inside a Top-10 render by
26_Decision_Chain (renders closed 22:14:35, 22:19:58, 22:27:32): a render
writes "status: running..." into B2, clears the zones from row 18 and
repaints, so rows_to_records finds no "Symbol" header and returns []. With
TFB_SHADOW_T10_READ_RETRIES > 0 the board re-reads an empty or rendering
Top-10 until it is settled (bounded); 0 (default) is one read, as v1.5.0.

Runs the REAL module; only the Sheets / network seams main() already uses
are faked (the tests/test_sb_freeze_nontrading_p175.py pattern):
  S1 env parsing: default 0, junk 0, clamps; wait default 30, clamp 5..120
  S2 top10_rendering on real cockpit status lines
  S3 read_top10_settled: cleared -> rendering-with-old-rows -> settled
  S4 never settles: last read kept, bounded retries; armed but settled on the
     first read: no sleep
  S5 a failing re-read keeps the last values and the loop continues
  S6 main() off: exactly one Top_10 read, no READ line; armed: the settled
     board is evaluated (cands=3) and the READ line is printed
  S7 version, manifest pin, workflow arming line

Run: python tests/test_shadow_board_t10_settle.py   or   pytest -q <this file>
"""
from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _load():
    sp = importlib.util.spec_from_file_location("sb_t10_settle", _REPO / "scripts" / "run_shadow_board.py")
    m = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(m)
    return m


sb = _load()

HEADER = ["Rank", "Symbol", "Name", "Sector", "Ticket SAR", "ROI %", "Conf"]
PANEL = [["TOP 10 INVESTMENTS - DECISION"],
         ["Status:", "Last run 2026-10-27 22:20:31 | status: ok | output: WITHHELD | source readiness WITHHELD"],
         ["CONTROL PANEL"], ["T10: Universe Scope", "All Main Sheets"], ["KPIs"],
         ["Entered cash (SAR)", "Exp. Gain 12M (SAR)", "Selected"], ["68,997 SAR", "-", "0 / 10 (withheld)"]]
ROWS = [["1", "CRC.US", "California Resources", "Energy", "9,000", "31.7", "High"],
        ["2", "BHF.US", "Brighthouse Financial", "Financial Services", "9,000", "28.2", "High"],
        ["3", "AGO.US", "Assured Guaranty", "Financial Services", "9,000", "27.5", "High"]]
SETTLED = PANEL + [HEADER] + ROWS
RUNNING = "Last run 2026-10-27 22:25:31 | status: running…"
# mid-render, zones from row 18 cleared (no header, no rows)
CLEARED = [r[:] for r in PANEL]
CLEARED[1] = ["Status:", RUNNING]
# render just started: B2 says running, old rows still on the sheet
RENDER_START = [r[:] for r in SETTLED]
RENDER_START[1] = ["Status:", RUNNING]
# a settled board that genuinely has no rows
EMPTY_BOARD = PANEL + [HEADER]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for k in (sb.ENV_T10_READ_RETRIES, sb.ENV_T10_READ_WAIT_S, "TFB_BOARD_FREEZE_NONTRADING",
              "TFB_BOARD_FREEZE_TODAY", "TFB_BOARD_ENGINE_ROI"):
        monkeypatch.delenv(k, raising=False)
    yield


# ----------------------------------------------------------------------------- S1-S5 (pure)
def test_s1_env_parsing(monkeypatch):
    assert sb._t10_read_retries() == 0
    for raw, want in (("0", 0), ("junk", 0), ("-3", 0), ("6", 6), (" 4 ", 4), ("99", 20)):
        monkeypatch.setenv(sb.ENV_T10_READ_RETRIES, raw)
        assert sb._t10_read_retries() == want, raw
    assert sb._t10_read_wait_s() == 30.0
    for raw, want in (("junk", 30.0), ("1", 5.0), ("45", 45.0), ("999", 120.0)):
        monkeypatch.setenv(sb.ENV_T10_READ_WAIT_S, raw)
        assert sb._t10_read_wait_s() == want, raw


def test_s2_rendering_detection():
    assert sb.top10_rendering(CLEARED) is True
    assert sb.top10_rendering(RENDER_START) is True
    assert sb.top10_rendering([["x"], ["Status:", "Last run .. | STATUS: Running..."]]) is True
    assert sb.top10_rendering(SETTLED) is False
    assert sb.top10_rendering([["Status:", RUNNING]]) is False      # row 1 is the title row
    assert sb.top10_rendering([]) is False and sb.top10_rendering(None) is False
    # the parse the board depends on: cleared -> nothing, settled -> three
    assert sb.rows_to_records(CLEARED) == []
    assert [c["symbol"] for c in sb.rows_to_records(SETTLED)] == ["CRC.US", "BHF.US", "AGO.US"]


def _reader(seq):
    calls = {"n": 0}

    def read():
        i = min(calls["n"], len(seq) - 1)
        calls["n"] += 1
        item = seq[i]
        if isinstance(item, Exception):
            raise item
        return copy.deepcopy(item)
    return read, calls


def test_s3_waits_through_a_render():
    read, calls = _reader([CLEARED, RENDER_START, SETTLED])
    sleeps = []
    vals, cands, info = sb.read_top10_settled(read, 6, 30.0, sleeps.append)
    assert [c["symbol"] for c in cands] == ["CRC.US", "BHF.US", "AGO.US"]
    assert info == {"retries": 2, "first": "rendering", "final": "ok"}
    assert sleeps == [30.0, 30.0] and calls["n"] == 3
    assert sb.top10_rendering(vals) is False


def test_s4_bounded_and_free_when_settled():
    read, calls = _reader([EMPTY_BOARD])
    sleeps = []
    vals, cands, info = sb.read_top10_settled(read, 3, 30.0, sleeps.append)
    assert cands == [] and info == {"retries": 3, "first": "empty", "final": "empty"}
    assert len(sleeps) == 3 and calls["n"] == 4
    read2, calls2 = _reader([CLEARED])
    _, cands2, info2 = sb.read_top10_settled(read2, 2, 30.0, lambda s: None)
    assert cands2 == [] and info2["final"] == "rendering" and calls2["n"] == 3
    read3, calls3 = _reader([SETTLED, CLEARED])
    sleeps3 = []
    _, cands3, info3 = sb.read_top10_settled(read3, 6, 30.0, sleeps3.append)
    assert len(cands3) == 3 and info3 == {"retries": 0, "first": "ok", "final": "ok"}
    assert sleeps3 == [] and calls3["n"] == 1


def test_s5_failed_reread_keeps_last_values():
    read, calls = _reader([CLEARED, RuntimeError("APIError 503"), SETTLED])
    _, cands, info = sb.read_top10_settled(read, 4, 30.0, lambda s: None)
    assert len(cands) == 3 and info == {"retries": 2, "first": "rendering", "final": "ok"}
    read2, _ = _reader([CLEARED, RuntimeError("APIError 503")])
    vals2, cands2, info2 = sb.read_top10_settled(read2, 2, 30.0, lambda s: None)
    assert cands2 == [] and info2["final"] == "rendering" and vals2 == CLEARED


# ----------------------------------------------------------------------------- S6 (main)
HOLDS = [["PORTFOLIO DECISION"], ["Status:", "x"], [""], ["ACTIONS"],
         ["Action", "Symbol", "Name", "Qty", "Market Value (SAR)", "Confidence", "Expected ROI %"],
         ["HOLD", "FRST.US", "Primis Financial", "174", "9,685", "Low", "12.0"]]


class FakeWS:
    def __init__(self, name, values, log):
        self.name, self.values, self.log = name, values, log
        self.reads = 0

    def get_all_values(self):
        self.log.append(("read", self.name))
        v = self.values
        if callable(v):
            v = v(self.reads)
        self.reads += 1
        return copy.deepcopy(v)

    def update(self, values=None, range_name=None):
        self.log.append(("update", self.name, range_name, len(values or [])))

    def clear(self):
        self.log.append(("clear", self.name))

    def append_row(self, row, value_input_option=None):
        self.log.append(("append_row", self.name))

    def append_rows(self, rows, value_input_option=None):
        self.log.append(("append_rows", self.name, len(rows)))

    def freeze(self, rows=None):
        pass


class FakeSH:
    def __init__(self, log, top10):
        self.log = log
        self.tabs = {"Top_10_Investments": FakeWS("Top_10_Investments", top10, log),
                     "Portfolio_Decision": FakeWS("Portfolio_Decision", HOLDS, log),
                     "Shadow_Board": FakeWS("Shadow_Board", [], log),
                     "Regime_History": FakeWS("Regime_History", [], log),
                     "_Run_Log": FakeWS("_Run_Log", [], log)}

    def worksheet(self, name):
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        return self.tabs[title]


REGIME = {"sleeves": {"Global": {"state": "RISK_ON", "distance_pct": 9.3, "months_in_state": 6, "abs_mom_pct": 25.5}},
          "suggested_weights": {"Global": 0.7, "Cash": 0.3}, "errors": [], "version": "t", "governance": "advisory"}


def _run_main(monkeypatch, top10, env):
    mod = _load()
    log = []
    sheet = FakeSH(log, top10)
    mod._open_sheet = lambda cli: sheet
    mod.fetch_board_fundamentals = lambda syms, sleep_s=0.4: ({}, [])
    mod.build_regime_block = lambda: copy.deepcopy(REGIME)
    mod.fetch_engine_roi_map = lambda sh, symbols: ({}, [])

    class SA:
        @staticmethod
        def get_authority_index(force=True): return {}
        @staticmethod
        def get_monitor_map(): return {}
        @staticmethod
        def get_meta(): return {"rows": 408, "as_of": "2026-03-31"}
        @staticmethod
        def last_error(): return None

    class PA:
        @staticmethod
        def advisor_switch_scan(holds, cands): return {"verdict": "NONE", "proposals": [], "pairs_checked": 0}
    mod.sa, mod.pa = SA, PA
    import time as _time
    monkeypatch.setattr(_time, "sleep", lambda s: None)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = mod.main(["--dry-run"])
    return rc, log, buf.getvalue()


def test_s6_main_off_is_one_read_and_armed_waits(monkeypatch):
    seq = lambda i: [CLEARED, RENDER_START, SETTLED][min(i, 2)]   # noqa: E731
    rc, log, out = _run_main(monkeypatch, seq, {"TFB_BOARD_FREEZE_TODAY": "2026-10-27"})
    assert rc == 0
    assert log.count(("read", "Top_10_Investments")) == 1
    assert "[SHADOW-BOARD-READ" not in out
    assert "cands=0" in out                                   # v1.5.0 reads the render window
    rc2, log2, out2 = _run_main(monkeypatch, seq, {"TFB_BOARD_FREEZE_TODAY": "2026-10-27",
                                                  sb.ENV_T10_READ_RETRIES: "6"})
    assert rc2 == 0
    assert log2.count(("read", "Top_10_Investments")) == 3
    assert "[SHADOW-BOARD-READ v1.6.0] retries=2 first=rendering final=ok cands=3" in out2
    assert "cands=3" in out2.split("[SHADOW-BOARD v1.6.0]", 1)[1]


# ----------------------------------------------------------------------------- S7
def test_s7_version_pin_and_workflow():
    assert sb.SCRIPT_VERSION == "1.6.0"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_shadow_board", "1.6.0", "shadow board"),' in vd
    wf = (_REPO / ".github" / "workflows" / "shadow_board.yml").read_text(encoding="utf-8")
    assert 'TFB_SHADOW_T10_READ_RETRIES: "6"' in wf
    src = (_REPO / "scripts" / "run_shadow_board.py").read_text(encoding="utf-8")
    assert "        cands = rows_to_records(sh.worksheet(TAB_TOP10).get_all_values())\n" in src


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
