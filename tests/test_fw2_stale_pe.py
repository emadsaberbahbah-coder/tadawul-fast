#!/usr/bin/env python3
"""tests/test_fw2_stale_pe.py - run_dashboard_sync v6.71.0 FW2-STALE-PE.

WHY (2026-10-29)
----------------
FW-2 (v6.24.0) strips every outgoing row whose stated P/E differs from
Price/EPS by 5 % or more (outside the GBX and HK-FX unit bands); its design
note expects 0 strips on a healthy backend. _Run_Log: Market_Leaders strips
were 0-3 per write from 10 to 24 Oct, then 19 (25 Oct), 9 / 5 (26 Oct),
21 / 24 / 7 (27 Oct) and 6 / 16 / 13 / 10 (28 Oct); Global_Markets 4-377 per
write all month. The 12 KSA strips of 28 Oct had implied/stated P/E ratios
1.05-1.10 (one 0.91) on a day when many Tadawul names rose 4-10 %. 244 of
the 255 Market_Leaders rows carry yahoo_enrichment_applied and are coherent
to 0.6 %; the 11 testable rows without it were all off-identity. Render
logged "[YF-CB v6.9.1] OPEN reason=info_empty status=429 ... EODHD fallback
carries fundamentals" every minute from 15:30 to 15:45: such rows carry
EODHD's P/E, priced at EODHD's last snapshot, against the live quote. FW-KEEP
then restores yesterday's row, which is not acquired: Market_Leaders
acquisition read 243/255 (95.3 %, a pass by one row) and the 13:30
dispatched run failed at 94.9 %.

Pinned here:
  P1  gate TFB_SYNC_FW2_STALE_PE: off (default and any other word) |
      observe | enforce
  P2  off is the v6.70.2 FW-2 call: same matrix, strips and diagnostics, and
      no last-good read
  P3  observe strips exactly as off and names the rows it would keep
  P4  enforce keeps a witnessed band row with ONLY P/E (TTM) and EPS (TTM)
      blanked and pe_stale_cleared:fw2:v6.71.0 appended; every other cell and
      every other row exactly as off
  P5  the band [0.80, 1.25] and the +/-25 % price jump are inclusive and fixed
  P6  each witness leg, failing alone, leaves the row stripped exactly as off
      and names the reason
  P7  Name: case, punctuation and legal form ignored; different companies and
      different non-Latin names never match
  P8  the last-good read runs once, and only with a band candidate; a failed
      or raising read keeps nothing
  P9  a planning error keeps nothing; enforce never mutates the caller's rows
  P10 the exact observe / enforce log lines; '' when FW-2 stripped nothing
  P11 FW-3 verdict: stale_pe_kept=None is the v6.70.2 append; enforce adds
      ' | stale_pe_kept N' and a Details key
  P12 the tag is not an invalid-acquisition token
  E1  the REAL _run_one_task on Market_Leaders: off and observe publish what
      the untouched base 729a316e publishes (rows, warnings, _Run_Log, stamp
      meta, stdout, logs - observe adds one log line and one sheet read);
      enforce publishes the fresh row with P/E and EPS cleared and acquired,
      and FW-KEEP restores only the gross row
  G1  golden negative: the untouched base strips the witnessed row and
      FW-KEEP restores yesterday's row even with the variable set to enforce
  A1  FW-2, FW-1 and FW-KEEP sources are byte-identical to the base; no
      function is removed; the additions are exactly the eleven helpers
  W1  daily_sync.yml 1.0.8 maps the variable, default off, in both
      page-writing jobs (env mapping only vs the base); ci.yml runs this file
  V1  version pins

Base-dependent cases read the untouched base with `git show 729a316e` and
SKIP on a shallow checkout.

Run: pytest -q tests/test_fw2_stale_pe.py
"""
from __future__ import annotations

import ast
import asyncio
import copy
import importlib.util
import inspect
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from core.data_validity import _INVALID_WARNINGS, row_acquisition  # noqa: E402
from core.sheets.schema_registry import get_sheet_headers  # noqa: E402
from scripts import run_dashboard_sync as sync  # noqa: E402

BASE_SHA = "7473d06e"  # merge note 2026-10-31 (#778): the bundle head before main #875
# (main 729a316e + FW-2 SCOPE, run_dashboard_sync 6.71.0 there); on main the base is
# 729a316e (6.70.2). Every base case then proves exactly FW2-STALE-PE's change, as on main.
BASE_VERSION = "6.71.0"  # the base copy's SCRIPT_VERSION (6.70.2 at 729a316e)
NOW = datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc)
TAG = "pe_stale_cleared:fw2:v6.71.0"
ENV = "TFB_SYNC_FW2_STALE_PE"
ACQ = ("acquisition_status:success; acquisition_provider:eodhd; "
       "acquisition_acquired_at:2026-10-28T11:59:00Z; "
       "acquisition_quote_asof:2026-10-28T11:55:00Z")

# Compact header for the pure cases (the column names FW-2 resolves on real pages).
H = ["Symbol", "Name", "Current Price", "Currency", "EPS (TTM)", "P/E (TTM)",
     "Data Provider", "Warnings"]
I = {h: i for i, h in enumerate(H)}


def r(sym, name, px, eps, pe, ccy="SAR", prov="eodhd", warn=ACQ):
    return [sym, name, px, ccy, eps, pe, prov, warn]


def fresh_rows():
    return [
        r("1120.SR", "Al Rajhi Bank", 107.0, 5.0, 19.5),     # ratio 1.0974, witnessed
        r("2010.SR", "SABIC", 70.0, 2.0, 35.0),              # coherent (1.0)
        r("7010.SR", "stc", 40.0, 2.0, 5.0),                 # gross (4.0)
        r("1010.SR", "Riyad Bank", 30.0, 2.0, 13.0),         # ratio 1.1538, other Name on the sheet
    ]


def old_grid():
    return [list(H),
            r("1120.SR", "AL RAJHI BANK", 100.0, 5.0, 20.0),
            r("2010.SR", "SABIC", 69.0, 2.0, 34.5),
            r("7010.SR", "stc", 41.0, 2.0, 20.5),
            r("1010.SR", "Bank Albilad", 29.0, 2.0, 14.5)]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (ENV, "TFB_SYNC_FORCE_REFETCH_SYMBOLS", "TFB_SYNC_COH_HK_REPORTING_FX",
                 "TFB_SYNC_COH_HK_CNY_BAND", "TFB_SYNC_COH_HK_USD_BAND",
                 "TFB_SYNC_ROW_ID_FIREWALL", "TFB_SYNC_FW_KEEP_LAST_GOOD",
                 "TFB_SYNC_IDFW_RUNLOG", "TFB_SYNC_KLG_IDENTITY_GATE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    monkeypatch.setattr(sync.time, "sleep", lambda _s: None)


class Loader:
    def __init__(self, grid=None, *, fail=False, boom=False):
        self.grid, self.fail, self.boom, self.calls = grid, fail, boom, 0

    def __call__(self):
        self.calls += 1
        if self.boom:
            raise TimeoutError("The read operation timed out")
        return None if self.fail else copy.deepcopy(self.grid)


def run_off(rows):
    m = copy.deepcopy(rows)
    out, stripped = sync._row_identity_firewall(list(H), m)
    return out, stripped, copy.deepcopy(sync._LAST_FW2_DIAG)


def run_mode(rows, mode, loader):
    m = copy.deepcopy(rows)
    out, stripped, rep = sync._fw2_stale_pe_firewall(list(H), m, mode, loader)
    return out, stripped, rep, copy.deepcopy(sync._LAST_FW2_DIAG)


# ---------------------------------------------------------------- P1 -------
@pytest.mark.parametrize("raw,want", [
    (None, "off"), ("", "off"), ("off", "off"), ("OFF", "off"), ("observe", "observe"),
    (" Observe ", "observe"), ("enforce", "enforce"), ("ENFORCE", "enforce"),
    ("1", "off"), ("on", "off"), ("true", "off"), ("enforced", "off"), ("observe,enforce", "off"),
])
def test_p1_gate_words(monkeypatch, raw, want):
    if raw is None:
        monkeypatch.delenv(ENV, raising=False)
    else:
        monkeypatch.setenv(ENV, raw)
    assert sync._fw2_stale_pe_mode() == want


# ---------------------------------------------------------------- P2 -------
def test_p2_off_is_the_fw2_call():
    want_out, want_s, want_diag = run_off(fresh_rows())
    loader = Loader(old_grid())
    out, stripped, rep, diag = run_mode(fresh_rows(), "off", loader)
    assert (out, stripped, diag) == (want_out, want_s, want_diag)
    assert loader.calls == 0
    assert rep == {"mode": "off", "fw2_strips": 0, "band": 0, "kept": [],
                   "rejected": [], "read_failed": False}
    assert want_s == ["1120.SR", "7010.SR", "1010.SR"]


# ---------------------------------------------------------------- P3 -------
def test_p3_observe_strips_exactly_as_off():
    want_out, want_s, want_diag = run_off(fresh_rows())
    loader = Loader(old_grid())
    out, stripped, rep, diag = run_mode(fresh_rows(), "observe", loader)
    assert (out, stripped, diag) == (want_out, want_s, want_diag)
    assert loader.calls == 1
    assert rep["kept"] == [("1120.SR", 1.0974, 1.07, 0)]
    assert rep["rejected"] == [("1010.SR", "name")]
    assert (rep["fw2_strips"], rep["band"], rep["read_failed"]) == (3, 2, False)


# ---------------------------------------------------------------- P4 -------
def test_p4_enforce_clears_only_pe_and_eps_on_the_witnessed_row():
    want_out, want_s, want_diag = run_off(fresh_rows())
    out, stripped, rep, diag = run_mode(fresh_rows(), "enforce", Loader(old_grid()))
    src = fresh_rows()[0]
    kept = out[0]
    for h in H:
        if h in ("P/E (TTM)", "EPS (TTM)"):
            assert kept[I[h]] == ""
        elif h == "Warnings":
            assert kept[I[h]] == ACQ + "; " + TAG
        else:
            assert kept[I[h]] == src[I[h]], h
    assert out[1:] == want_out[1:]
    assert stripped == ["7010.SR", "1010.SR"] == [s for s in want_s if s != "1120.SR"]
    assert diag["stripped"] == [d for d in want_diag["stripped"] if d[0] != "1120.SR"]
    assert diag["hk_fx_exempt"] == want_diag["hk_fx_exempt"] == []
    assert rep["kept"] == [("1120.SR", 1.0974, 1.07, 0)]


def test_p4b_tag_starts_a_blank_warnings_cell_and_yf_is_reported():
    rows = [r("1120.SR", "Al Rajhi Bank", 107.0, 5.0, 19.5, warn="")]
    out, _s, rep, _d = run_mode(rows, "enforce", Loader(old_grid()))
    assert out[0][I["Warnings"]] == TAG
    rows = [r("1120.SR", "Al Rajhi Bank", 107.0, 5.0, 19.5, warn="yahoo_enrichment_applied")]
    _o, _s, rep, _d = run_mode(rows, "observe", Loader(old_grid()))
    assert rep["kept"] == [("1120.SR", 1.0974, 1.07, 1)]


# ---------------------------------------------------------------- P5 -------
@pytest.mark.parametrize("px,in_band", [(8.0, True), (12.5, True), (7.99, False), (12.51, False),
                                        (9.4, True), (10.6, True)])
def test_p5_ratio_band_is_inclusive(px, in_band):
    # EPS 1, stated P/E 10: ratio = px / 10. The last-good row is coherent at the same price.
    rows = [r("2222.SR", "Saudi Aramco", px, 1.0, 10.0)]
    grid = [list(H), r("2222.SR", "Saudi Aramco", px, 1.0, px)]
    want_out, want_s, _d = run_off(rows)
    assert want_s == ["2222.SR"]
    out, stripped, rep, _d = run_mode(rows, "enforce", Loader(grid))
    if in_band:
        assert stripped == [] and rep["band"] == 1 and len(rep["kept"]) == 1
        assert out[0][I["P/E (TTM)"]] == "" and out[0][I["Current Price"]] == px
    else:
        assert (out, stripped) == (want_out, want_s) and rep["band"] == 0


@pytest.mark.parametrize("px,kept", [(125.0, True), (75.0, True), (125.02, False), (74.98, False)])
def test_p5b_price_jump_is_inclusive(px, kept):
    # last-good price 100 (coherent: 100 / 5 = 20); the stated P/E keeps the ratio in band.
    pe = round(px / 5.0 / 1.1111, 4)
    rows = [r("2222.SR", "Saudi Aramco", px, 5.0, pe)]
    grid = [list(H), r("2222.SR", "Saudi Aramco", 100.0, 5.0, 20.0)]
    want_out, want_s, _d = run_off(rows)
    out, stripped, rep, _d = run_mode(rows, "enforce", Loader(grid))
    assert rep["band"] == 1
    if kept:
        assert stripped == [] and rep["kept"][0][2] == round(px / 100.0, 4)
    else:
        assert (out, stripped) == (want_out, want_s)
        assert rep["rejected"] == [("2222.SR", "px_jump")]


def test_p5c_band_and_jump_are_fixed_constants(monkeypatch):
    assert sync._FW2_STALE_PE_BAND == (0.80, 1.25)
    assert sync._FW2_STALE_PE_PX_JUMP == 0.25
    for name in ("TFB_SYNC_FW2_STALE_PE_BAND", "TFB_SYNC_FW2_STALE_PE_PX_JUMP"):
        monkeypatch.setenv(name, "0.1,10")
    rows = [r("2222.SR", "Saudi Aramco", 15.0, 1.0, 10.0)]   # ratio 1.5
    _o, stripped, rep, _d = run_mode(rows, "enforce", Loader([list(H)]))
    assert stripped == ["2222.SR"] and rep["band"] == 0


# ---------------------------------------------------------------- P6 -------
def _grid_with(**changes):
    row = r("1120.SR", "AL RAJHI BANK", 100.0, 5.0, 20.0)
    for k, v in changes.items():
        row[I[k]] = v
    return [list(H), row]


def _fresh_with(**changes):
    row = r("1120.SR", "Al Rajhi Bank", 107.0, 5.0, 19.5)
    for k, v in changes.items():
        row[I[k]] = v
    return [row]


WITNESS_CASES = [
    ("no_last_good", _fresh_with(), [list(H), r("2010.SR", "SABIC", 69.0, 2.0, 34.5)], {}),
    ("last_good_price", _fresh_with(), _grid_with(**{"Current Price": ""}), {}),
    ("last_good_price", _fresh_with(), _grid_with(**{"Current Price": 0}), {}),
    ("last_good_provider", _fresh_with(), _grid_with(**{"Data Provider": "fallback_error"}), {}),
    ("last_good_provider", _fresh_with(), _grid_with(**{"Data Provider": "placeholder_fallback"}), {}),
    ("last_good_fetch_failed", _fresh_with(), _grid_with(Warnings="fetch_failed:HTTP 402"), {}),
    ("last_good_identity", _fresh_with(), _grid_with(**{"P/E (TTM)": 30.0}), {}),
    ("last_good_identity", _fresh_with(), _grid_with(Name=""), {}),
    ("last_good_identity", _fresh_with(), _grid_with(Name="Market_Leaders 1120.SR"), {}),
    ("name", _fresh_with(Name="Bank Albilad"), _grid_with(), {}),
    ("name", _fresh_with(Name=""), _grid_with(), {}),
    ("name", _fresh_with(Name="Market_Leaders 1120.SR"), _grid_with(), {}),
    ("currency", _fresh_with(Currency="USD"), _grid_with(), {}),
    ("currency", _fresh_with(Currency=""), _grid_with(Currency=""), {}),
    ("currency", _fresh_with(Currency="sar"), _grid_with(), {}),
    ("px_jump", _fresh_with(**{"Current Price": 130.0, "P/E (TTM)": 24.0}), _grid_with(), {}),
    ("forced", _fresh_with(), _grid_with(), {"TFB_SYNC_FORCE_REFETCH_SYMBOLS": "2010.SR,1120.SR"}),
    ("fetch_failed", _fresh_with(Warnings="fetch_failed:timeout; " + ACQ), _grid_with(), {}),
    ("provider", _fresh_with(**{"Data Provider": "fallback_error"}), _grid_with(), {}),
]


@pytest.mark.parametrize("why,rows,grid,env", WITNESS_CASES,
                         ids=[f"{c[0]}-{i}" for i, c in enumerate(WITNESS_CASES)])
def test_p6_a_failed_witness_leg_strips_exactly_as_off(monkeypatch, why, rows, grid, env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    want_out, want_s, want_diag = run_off(rows)
    assert want_s == ["1120.SR"]
    for mode in ("observe", "enforce"):
        out, stripped, rep, diag = run_mode(rows, mode, Loader(grid))
        assert (out, stripped, diag) == (want_out, want_s, want_diag)
        assert rep["kept"] == [] and rep["rejected"] == [("1120.SR", why)]


def test_p6b_symbol_domain_leg():
    rows = [r("AL RAJHI", "Al Rajhi Bank", 107.0, 5.0, 19.5)]
    grid = [list(H), r("AL RAJHI", "Al Rajhi Bank", 100.0, 5.0, 20.0)]
    want_out, want_s, _d = run_off(rows)
    out, stripped, rep, _d = run_mode(rows, "enforce", Loader(grid))
    assert (out, stripped) == (want_out, want_s) == (want_out, ["AL RAJHI"])
    assert rep["rejected"] == [("AL RAJHI", "domain")]


def test_p6c_witness_holds_with_the_klg_identity_gate_off(monkeypatch):
    # FW-1 rules apply to the witness whatever TFB_SYNC_KLG_IDENTITY_GATE says.
    monkeypatch.setenv("TFB_SYNC_KLG_IDENTITY_GATE", "0")
    rows = _fresh_with()
    _o, stripped, rep, _d = run_mode(rows, "enforce", Loader(_grid_with(**{"P/E (TTM)": 30.0})))
    assert stripped == ["1120.SR"] and rep["rejected"] == [("1120.SR", "last_good_identity")]


def test_p6d_unit_conventions_are_never_band_candidates():
    rows = [r("TSCO.L", "Tesco PLC", 471.8, 0.27, 17.47, ccy="GBX"),      # GBX/GBP band
            r("1398.HK", "ICBC", 6.0, 1.0, 5.5, ccy="HKD")]                 # HK CNY band (1.09)
    want_out, want_s, want_diag = run_off(rows)
    loader = Loader([list(H)])
    out, stripped, rep, diag = run_mode(rows, "enforce", loader)
    assert (out, stripped, diag) == (want_out, want_s, want_diag)
    assert want_s == [] and want_diag["hk_fx_exempt"] == ["1398.HK"]
    assert rep["fw2_strips"] == 0 and loader.calls == 0


# ---------------------------------------------------------------- P7 -------
# Built from code points so this file stays ASCII: 'Masraf Al Rajhi', 'Bank Albilad'.
ARABIC_RAJHI = "".join(map(chr, (0x645, 0x635, 0x631, 0x641, 0x20, 0x627, 0x644, 0x631,
                                 0x627, 0x62C, 0x62D, 0x64A)))
ARABIC_BILAD = "".join(map(chr, (0x628, 0x646, 0x643, 0x20, 0x627, 0x644, 0x628, 0x644,
                                 0x627, 0x62F)))
CJK_CHINA = chr(0x4E2D) + chr(0x56FD)     # 'China'
CJK_JAPAN = chr(0x65E5) + chr(0x672C)     # 'Japan'


@pytest.mark.parametrize("a,b,want", [
    ("Apple Inc.", "APPLE INC", True),
    ("Apple Inc.", "Apple", True),
    ("Microsoft", "Microsoft Corporation", True),
    ("Airbnb, Inc.", "Airbnb Inc", True),
    ("  Saudi   Aramco ", "saudi aramco", True),
    ("Al Rajhi Bank", "Bank Albilad", False),
    ("Autoliv, Inc.", "Allianz SE", False),
    (ARABIC_RAJHI, ARABIC_RAJHI + " ", True),
    (ARABIC_RAJHI, ARABIC_BILAD, False),
    ("", "", False),
    (None, "Apple", False),
    ("Inc.", "Corp", False),
    ("Holdings", "Holdings", False),               # legal-form words only: unusable
    # Codex review on PR #875: shared Latin words must not merge different non-Latin names
    (CJK_CHINA + " Bank", CJK_JAPAN + " Bank", False),
    (CJK_CHINA + " Bank Ltd", CJK_CHINA + " BANK", True),
    (ARABIC_RAJHI + " PLC", ARABIC_BILAD + " PLC", False),
])
def test_p7_names_match(a, b, want):
    assert sync._fw2_names_match(a, b) is want
    assert sync._fw2_names_match(b, a) is want


def test_p7c_legal_form_words_are_symbol_dedups():
    from core.analysis import symbol_dedup
    assert sync._FW2_LEGAL_FORM_WORDS == {w.casefold() for w in symbol_dedup._NAME_NOISE}


def test_p7d_mixed_script_mismatch_stays_stripped():
    rows = [r("8306.T", CJK_CHINA + " Bank", 107.0, 5.0, 19.5, ccy="JPY")]
    grid = [list(H), r("8306.T", CJK_JAPAN + " Bank", 100.0, 5.0, 20.0, ccy="JPY")]
    want_out, want_s, _d = run_off(rows)
    out, stripped, rep, _d = run_mode(rows, "enforce", Loader(grid))
    assert (out, stripped) == (want_out, want_s) == (want_out, ["8306.T"])
    assert rep["rejected"] == [("8306.T", "name")]


def test_p7b_name_key_keeps_unicode_letters():
    assert sync._fw2_name_key(ARABIC_RAJHI) == ARABIC_RAJHI
    assert sync._fw2_name_key("A.B, C") == "a b c"
    assert sync._fw2_name_key(None) == ""


# ---------------------------------------------------------------- P8 -------
def test_p8_last_good_read_only_with_a_band_candidate():
    gross_only = [r("7010.SR", "stc", 40.0, 2.0, 5.0), r("2010.SR", "SABIC", 70.0, 2.0, 35.0)]
    for rows in (gross_only, [r("2010.SR", "SABIC", 70.0, 2.0, 35.0)]):
        loader = Loader(old_grid())
        run_mode(rows, "observe", loader)
        assert loader.calls == 0
    loader = Loader(old_grid())
    two_band = fresh_rows() + [r("2222.SR", "Saudi Aramco", 27.0, 1.8, 13.5)]
    run_mode(two_band, "enforce", loader)
    assert loader.calls == 1


@pytest.mark.parametrize("loader", [Loader(fail=True), Loader(boom=True), None])
def test_p8b_failed_read_keeps_nothing(loader):
    want_out, want_s, want_diag = run_off(fresh_rows())
    out, stripped, rep, diag = run_mode(fresh_rows(), "enforce", loader)
    assert (out, stripped, diag) == (want_out, want_s, want_diag)
    assert rep["kept"] == [] and rep["read_failed"] is True
    assert rep["rejected"] == [("1120.SR", "no_last_good"), ("1010.SR", "no_last_good")]
    assert sync._fw2_stale_pe_line("Market_Leaders", rep).endswith("; last-good READ FAILED")


def test_p8c_reader_retries_once_then_gives_up(monkeypatch):
    calls = []

    class S:
        def __init__(self, answers):
            self.answers = list(answers)

        def read_values(self, sid, page, block):
            calls.append((sid, page, block))
            return self.answers.pop(0)

    monkeypatch.setattr(sync, "_universe_cap_v2_enabled", lambda: False)
    assert sync._fw2_read_last_good(S([None, [["Symbol"]]]), "sid", "Market_Leaders") == [["Symbol"]]
    assert calls == [("sid", "Market_Leaders", "A1:ZZ6000")] * 2
    calls.clear()
    assert sync._fw2_read_last_good(S([None, None]), "sid", "Market_Leaders") is None
    assert len(calls) == 2
    assert sync._fw2_read_last_good(None, "sid", "Market_Leaders") is None


# ---------------------------------------------------------------- P9 -------
def test_p9_planning_error_keeps_nothing(monkeypatch):
    def boom(*_a, **_k):
        raise RuntimeError("synthetic")
    monkeypatch.setattr(sync, "_fw2_last_good_index", boom)
    want_out, want_s, want_diag = run_off(fresh_rows())
    out, stripped, rep, diag = run_mode(fresh_rows(), "enforce", Loader(old_grid()))
    assert (out, stripped, diag) == (want_out, want_s, want_diag)
    assert rep["error"] == "RuntimeError" and rep["kept"] == []
    assert sync._fw2_stale_pe_line("Market_Leaders", rep).endswith(
        "; plan error RuntimeError (nothing kept)")


def test_p9b_enforce_never_mutates_the_callers_row_objects():
    rows = fresh_rows()
    originals = [row for row in rows]
    snapshot = copy.deepcopy(rows)
    out, _s, _rep = sync._fw2_stale_pe_firewall(list(H), rows, "enforce", Loader(old_grid()))
    assert originals == snapshot                      # the row objects are untouched
    assert out is rows and out[0] is not originals[0]  # the matrix slot holds a copy


# ---------------------------------------------------------------- P10 ------
OBSERVE_LINE = ("[FW2-STALE-PE observe] 'Market_Leaders' would keep 1 row(s): 1120.SR "
                "ratio=1.0974 px/lkg=1.07 yf=0 | band 2 of 3 FW-2 strip(s); still stripped 1: "
                "1010.SR(name)")
ENFORCE_LINE = ("[FW2-STALE-PE enforce] 'Market_Leaders' kept 1 row(s) with P/E and EPS "
                "cleared: 1120.SR ratio=1.0974 px/lkg=1.07 yf=0 | band 2 of 3 FW-2 strip(s); "
                "still stripped 1: 1010.SR(name)")


def test_p10_exact_lines():
    _o, _s, rep, _d = run_mode(fresh_rows(), "observe", Loader(old_grid()))
    assert sync._fw2_stale_pe_line("Market_Leaders", rep) == OBSERVE_LINE
    _o, _s, rep, _d = run_mode(fresh_rows(), "enforce", Loader(old_grid()))
    assert sync._fw2_stale_pe_line("Market_Leaders", rep) == ENFORCE_LINE
    _o, _s, rep, _d = run_mode([r("2010.SR", "SABIC", 70.0, 2.0, 35.0)], "enforce", Loader())
    assert sync._fw2_stale_pe_line("Market_Leaders", rep) == ""
    _o, _s, rep, _d = run_mode([r("7010.SR", "stc", 40.0, 2.0, 5.0)], "observe", Loader())
    assert sync._fw2_stale_pe_line("Market_Leaders", rep) == (
        "[FW2-STALE-PE observe] 'Market_Leaders' would keep 0 row(s) | band 0 of 1 FW-2 strip(s)")
    assert all(ord(c) < 128 for c in OBSERVE_LINE + ENFORCE_LINE)


def test_p10b_long_lists_are_bounded():
    rep = {"mode": "enforce", "fw2_strips": 40, "band": 40, "read_failed": False,
           "kept": [(f"S{i}.US", 1.1, 1.0, 0) for i in range(22)],
           "rejected": [(f"R{i}.US", "name") for i in range(18)]}
    line = sync._fw2_stale_pe_line("Global_Markets", rep)
    assert "S19.US ratio=1.1 px/lkg=1.0 yf=0 (+2 more)" in line and "S20.US" not in line
    assert "still stripped 18: " in line and "R14.US(name) ..." in line and "R15.US" not in line


# ---------------------------------------------------------------- P11 ------
class _Svc:
    """googleapiclient double: spreadsheets().values().append/update/batchUpdate."""

    def __init__(self):
        self.appends, self.updates = [], []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def append(self, **kw):
        self.appends.append(copy.deepcopy(kw))
        return self

    def update(self, **kw):
        self.updates.append(copy.deepcopy(kw))
        return self

    def batchUpdate(self, **kw):
        return self

    def execute(self):
        return {}


class _SheetsStub:
    def __init__(self):
        self.svc = _Svc()

    def _get_service(self):
        return self.svc


def test_p11_verdict_bytes(monkeypatch):
    monkeypatch.setattr(sync.time, "strftime", lambda *_a, **_k: "2026-10-28 15:00:00")
    args = ("sid", "Market_Leaders", 0, [], ["7010.SR"], {}, [])
    plain, none, kept = _SheetsStub(), _SheetsStub(), _SheetsStub()
    sync._append_runlog_idfirewall(plain, *args)
    sync._append_runlog_idfirewall(none, *args, stale_pe_kept=None)
    sync._append_runlog_idfirewall(kept, *args, stale_pe_kept=["1120.SR"])
    assert plain.svc.appends == none.svc.appends and len(plain.svc.appends) == 1
    p_row = plain.svc.appends[0]["body"]["values"][0]
    k_row = kept.svc.appends[0]["body"]["values"][0]
    assert " | stale_pe_kept 1 | selftest=" in k_row[5]
    assert k_row[5].replace(" | stale_pe_kept 1", "") == p_row[5]
    assert k_row[:5] == p_row[:5] and k_row[6:9] == p_row[6:9]
    p_det, k_det = json.loads(p_row[9]), json.loads(k_row[9])
    assert k_det.pop("stale_pe_kept") == ["1120.SR"] and k_det == p_det
    assert list(json.loads(k_row[9]))[-1] == "stale_pe_kept"
    empty = _SheetsStub()
    sync._append_runlog_idfirewall(empty, *args, stale_pe_kept=[])
    assert " | stale_pe_kept 0 | selftest=" in empty.svc.appends[0]["body"]["values"][0][5]


# ---------------------------------------------------------------- P12 ------
def test_p12_tag_is_not_an_invalid_acquisition_token():
    assert sync._FW2_STALE_PE_TAG == TAG
    assert _INVALID_WARNINGS.search(TAG) is None
    assert all(ord(c) < 128 for c in TAG)
    for word in ("cap", "forecast", "target", "roi", "drop", "reject"):
        assert word not in TAG        # the engine's Warnings substring gates


# ---------------------------------------------------------------- base -----
def _base_source():
    try:
        return subprocess.run(
            ["git", "-C", str(_REPO), "show", BASE_SHA + ":scripts/run_dashboard_sync.py"],
            check=True, capture_output=True, text=True).stdout
    except Exception:
        return None


def _load_copy(name, src, tmp_path_factory):
    """A private copy of a run_dashboard_sync source: module state other test
    files left in `sync` (self-test verdict, cross-page prices) cannot leak in."""
    if name in sys.modules:
        return sys.modules[name]
    path = tmp_path_factory.mktemp(name) / (name + ".py")
    path.write_text(src, encoding="utf-8")
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    src = _base_source()
    if src is None:
        pytest.skip("base %s not available (shallow checkout)" % BASE_SHA)
    mod = _load_copy("rds_base_" + BASE_SHA, src, tmp_path_factory)
    assert mod.SCRIPT_VERSION == BASE_VERSION
    return mod


@pytest.fixture(scope="module")
def fresh(tmp_path_factory):
    src = (_REPO / "scripts" / "run_dashboard_sync.py").read_text(encoding="utf-8")
    mod = _load_copy("rds_fresh_v6_71_0", src, tmp_path_factory)
    # merge note 2026-10-31 (#778): FW-2 SCOPE is 6.72.0 over this 6.71.0; a floor.
    assert tuple(int(x) for x in mod.SCRIPT_VERSION.split(".")) >= (6, 71, 0)
    return mod


# ---------------------------------------------------------------- E1 / G1 --
ML_H = get_sheet_headers("Market_Leaders")


def ml_row(sym, name, px, eps, pe, upd="2026-10-28T11:59:00Z"):
    cells = {"Symbol": sym, "Name": name, "Currency": "SAR", "Current Price": px,
             "EPS (TTM)": eps, "P/E (TTM)": pe, "Data Provider": "eodhd",
             "Last Updated (UTC)": upd, "Last Updated (Riyadh)": "2026-10-28T14:59:00+03:00",
             "Warnings": ACQ}
    return [cells.get(h, "") for h in ML_H]


ML_FRESH = [ml_row("1120.SR", "Al Rajhi Bank", 107.0, 5.0, 19.5),
            ml_row("2010.SR", "SABIC", 70.0, 2.0, 35.0),
            ml_row("7010.SR", "stc", 40.0, 2.0, 5.0)]
ML_OLD = [list(ML_H),
          ml_row("1120.SR", "AL RAJHI BANK", 100.0, 5.0, 20.0, upd="2026-10-27T11:59:00Z"),
          ml_row("2010.SR", "SABIC", 69.0, 2.0, 34.5, upd="2026-10-27T11:59:00Z"),
          ml_row("7010.SR", "stc", 41.0, 2.0, 20.5, upd="2026-10-27T11:59:00Z")]


def _drive(mod, mode, monkeypatch, capsys, caplog):
    """One Market_Leaders leg through the REAL _run_one_task with fakes only."""
    if mode is None:
        monkeypatch.delenv(ENV, raising=False)
    else:
        monkeypatch.setenv(ENV, mode)
    monkeypatch.setattr(mod, "_utc_now", lambda: NOW)
    monkeypatch.setattr(mod, "_read_symbols", lambda *_a, **_k: [x[0] for x in ML_FRESH])
    monkeypatch.setattr(mod.time, "strftime", lambda *_a, **_k: "2026-10-28 15:00:00")
    # what main() sets at start-up, the same for both copies
    monkeypatch.setattr(mod, "_IDFW_SELFTEST_MSG", "PASS 9/9")
    monkeypatch.setattr(mod, "_IDFW_SELFTEST_OK", True)
    monkeypatch.setattr(mod, "_XPAGE_PRICES", {})
    svc = _Svc()
    reads = []
    published = []

    class Backend:
        async def post_json(self, _endpoint, _payload):
            return {"headers": list(ML_H), "rows_matrix": copy.deepcopy(ML_FRESH)}, None, 200

    class Writer(mod.SheetsWriter):
        def __init__(self):
            super().__init__()
            self._service = svc

        def _get_service(self):
            return svc

        def read_values(self, *args, **_kwargs):
            reads.append(tuple(args[1:]))
            return copy.deepcopy(ML_OLD)

        def clear_from(self, *_a, **_k):
            return None

        def write_table(self, _book, _page, _start, headers, rows):
            published.append(copy.deepcopy(rows))
            return len(rows)

    caplog.clear()
    capsys.readouterr()
    with caplog.at_level(logging.INFO, logger="DashboardSync"):
        res = asyncio.run(mod._run_one_task(
            mod.TaskSpec("MARKET_LEADERS", "Market_Leaders", "enriched"),
            "SYNTHETIC_BOOK", "A5", -1, False, False, Backend(), Writer()))
    out = capsys.readouterr().out
    v = mod.SCRIPT_VERSION

    def norm(x):
        return str(x).replace(v, "<V>")

    runlog = []
    for a in svc.appends:
        row = list(a["body"]["values"][0])
        row[0] = "<ts>"
        try:
            det = json.loads(row[9])
            det.pop("ts_utc", None)
            row[9] = json.dumps(det, sort_keys=True)
        except Exception:
            pass
        runlog.append((a.get("range"), [norm(c) for c in row]))
    return {
        "res": res,
        "status": (res.status, res.rows_written, res.rows_failed, res.symbols_requested),
        "warnings": [norm(w) for w in res.warnings],
        "meta": norm(json.dumps(res._stamp_meta, sort_keys=True, default=str)),
        "published": published,
        "runlog": runlog,
        "reads": reads,
        "stdout": norm(out),
        "logs": [norm(rec.getMessage()) for rec in caplog.records if rec.name == "DashboardSync"],
    }


def _by_symbol(rows):
    si = ML_H.index("Symbol")
    return {row[si]: dict(zip(ML_H, row)) for row in rows}


def test_e1_off_is_byte_identical_to_the_base(base, fresh, monkeypatch, capsys, caplog):
    want = _drive(base, None, monkeypatch, capsys, caplog)
    for mode in (None, "off", "nonsense"):
        got = _drive(fresh, mode, monkeypatch, capsys, caplog)
        for key in ("status", "warnings", "meta", "published", "runlog", "reads", "stdout", "logs"):
            assert got[key] == want[key], key
    assert want["status"] == ("success", 3, 0, 3)
    pub = _by_symbol(want["published"][0])
    assert pub["1120.SR"]["Current Price"] == 100.0 and pub["1120.SR"]["P/E (TTM)"] == 20.0
    # the compared surfaces are not vacuous
    assert len(want["runlog"]) == 1 and len(want["reads"]) >= 2
    assert any("FW-KEEP] 'Market_Leaders': restored 2/2" in line for line in want["logs"])
    assert any("out_stripped=2 (1120.SR, 7010.SR)" in line for line in want["logs"])


def test_e1b_observe_adds_one_line_and_one_read(base, fresh, monkeypatch, capsys, caplog):
    want = _drive(base, None, monkeypatch, capsys, caplog)
    got = _drive(fresh, "observe", monkeypatch, capsys, caplog)
    for key in ("status", "warnings", "meta", "published", "runlog", "stdout"):
        assert got[key] == want[key], key
    extra = [line for line in got["logs"] if line.startswith("[FW2-STALE-PE")]
    assert extra == ["[FW2-STALE-PE observe] 'Market_Leaders' would keep 1 row(s): 1120.SR "
                     "ratio=1.0974 px/lkg=1.07 yf=0 | band 1 of 2 FW-2 strip(s)"]
    assert [line for line in got["logs"] if line not in extra] == want["logs"]
    assert len(got["reads"]) == len(want["reads"]) + 1


def test_e1c_enforce_publishes_the_fresh_row_acquired(fresh, monkeypatch, capsys, caplog):
    off = _drive(fresh, None, monkeypatch, capsys, caplog)
    got = _drive(fresh, "enforce", monkeypatch, capsys, caplog)
    assert got["status"] == off["status"] == ("success", 3, 0, 3)
    pub, pub_off = _by_symbol(got["published"][0]), _by_symbol(off["published"][0])
    src_row = _by_symbol(ML_FRESH)["1120.SR"]
    kept = pub["1120.SR"]
    assert kept["Current Price"] == 107.0 and kept["Name"] == "Al Rajhi Bank"
    assert kept["P/E (TTM)"] == "" and kept["EPS (TTM)"] == ""
    assert TAG in kept["Warnings"] and "kept_last_good" not in kept["Warnings"]
    assert kept["Last Updated (UTC)"] == src_row["Last Updated (UTC)"]
    assert row_acquisition(kept, NOW, 86400).status == "SUCCESS"
    assert row_acquisition(pub_off["1120.SR"], NOW, 86400).status == "INVALID"
    # the gross row is stripped and FW-KEEP restores it exactly as off
    assert pub["7010.SR"] == pub_off["7010.SR"] and pub["2010.SR"] == pub_off["2010.SR"]
    assert row_acquisition(pub["7010.SR"], NOW, 86400).status == "INVALID"
    enforce_line = ("[FW2-STALE-PE enforce] 'Market_Leaders' kept 1 row(s) with P/E and EPS "
                    "cleared: 1120.SR ratio=1.0974 px/lkg=1.07 yf=0 | band 1 of 2 FW-2 strip(s)")
    assert enforce_line in got["warnings"] and enforce_line in got["logs"]
    assert any("quarantined 1 identity-broken outgoing row(s) on 'Market_Leaders': 7010.SR"
               in w for w in got["warnings"])
    assert "[v6.25.1 FW-KEEP] 'Market_Leaders': restored 1/1 quarantined row(s) from last-good." \
        in got["warnings"]
    (rng, verdict), = got["runlog"]
    assert rng == "'_Run_Log'!A1"
    assert "| out_stripped=1 (7010.SR) |" in verdict[5] and " | stale_pe_kept 1 | " in verdict[5]
    assert json.loads(verdict[9])["stale_pe_kept"] == ["1120.SR"]
    meta, meta_off = json.loads(got["meta"]), json.loads(off["meta"])
    assert meta["noncurrent_fetched"] == meta_off["noncurrent_fetched"] - 1 == 1


def test_g1_golden_negative_base_strips_the_witnessed_row(base, monkeypatch, capsys, caplog):
    assert not hasattr(base, "_fw2_stale_pe_firewall")
    got = _drive(base, "enforce", monkeypatch, capsys, caplog)
    pub = _by_symbol(got["published"][0])
    restored = pub["1120.SR"]
    assert restored["Current Price"] == 100.0 and restored["P/E (TTM)"] == 20.0
    assert "kept_last_good" in restored["Warnings"] and TAG not in restored["Warnings"]
    assert row_acquisition(restored, NOW, 86400).status == "INVALID"
    assert any("out_stripped=2 (1120.SR, 7010.SR)" in row[5] for _r, row in got["runlog"])
    assert not any("FW2-STALE-PE" in line for line in got["logs"])


# ---------------------------------------------------------------- A1 -------
NEW_HELPERS = {
    "_fw2_stale_pe_mode", "_fw2_name_key", "_fw2_names_match", "_fw2_ccy_col",
    "_fw2_row_cols", "_fw2_read_last_good", "_fw2_last_good_index",
    "_fw2_stale_pe_witness", "_fw2_stale_pe_plan", "_fw2_stale_pe_firewall",
    "_fw2_stale_pe_line",
}


def _defs(src):
    return {n.name for n in ast.parse(src).body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}


def test_a1_guards_untouched_and_nothing_removed(base):
    for name in ("_row_identity_firewall", "_klg_old_row_identity_ok", "_keep_last_good_rows",
                 "_coh_ratio_is_unit_convention", "_coh_hk_reporting_fx_match",
                 "_fabrication_tripwire", "_row_firewall_enabled", "_fw_keep_last_good_enabled"):
        assert inspect.getsource(getattr(sync, name)) == inspect.getsource(getattr(base, name)), name
    new_defs = _defs((_REPO / "scripts" / "run_dashboard_sync.py").read_text(encoding="utf-8"))
    base_defs = _defs(_base_source())
    assert base_defs <= new_defs
    assert new_defs - base_defs == NEW_HELPERS


def test_a1b_added_lines_are_ascii():
    diff = subprocess.run(["git", "-C", str(_REPO), "diff", BASE_SHA, "--", "scripts/run_dashboard_sync.py",
                           "scripts/verify_deployment.py", ".github/workflows/daily_sync.yml",
                           ".github/workflows/ci.yml"],
                          capture_output=True, text=True)
    if diff.returncode != 0:
        pytest.skip("base %s not available (shallow checkout)" % BASE_SHA)
    added = [ln for ln in diff.stdout.splitlines() if ln.startswith("+") and not ln.startswith("+++")]
    # The title line keeps the em dash it has carried since v6.0 (only the version changes).
    bad = [ln for ln in added if any(ord(c) > 127 for c in ln)
           and "DASHBOARD SYNC RUNNER (v%s)" % sync.SCRIPT_VERSION not in ln]
    assert bad == []
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))


# ---------------------------------------------------------------- W1 -------
WORKFLOW = _REPO / ".github" / "workflows" / "daily_sync.yml"
EXPR = "${{ vars.TFB_SYNC_FW2_STALE_PE || 'off' }}"
JOBS = ("sync-dashboard", "recover-missing-market-pages")


def _base_workflow():
    try:
        return subprocess.run(
            ["git", "-C", str(_REPO), "show", BASE_SHA + ":.github/workflows/daily_sync.yml"],
            check=True, capture_output=True, text=True).stdout
    except Exception:
        return None


def _mapped(doc):
    return {name: (job.get("env") or {}).get(ENV) for name, job in (doc.get("jobs") or {}).items()
            if ENV in (job.get("env") or {})}


def test_w1_both_page_writing_jobs_map_the_gate_default_off():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert _mapped(yaml.safe_load(text)) == {job: EXPR for job in JOBS}
    assert any(line.startswith("# VERSION 1.0.8 (2026-10-29)") for line in text.splitlines()[:6])
    assert ENV not in (yaml.safe_load(text).get("env") or {})


def test_w1b_env_mapping_only_against_the_base():
    base_text = _base_workflow()
    if base_text is None:
        pytest.skip("base %s not available (shallow checkout)" % BASE_SHA)
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    for job in (doc.get("jobs") or {}).values():
        (job.get("env") or {}).pop(ENV, None)
    assert doc == yaml.safe_load(base_text)
    assert _mapped(yaml.safe_load(base_text)) == {}          # golden negative


def test_w1c_ci_runs_this_file_in_the_lean_lane():
    ci = yaml.safe_load((_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    runs = [step.get("run", "") for step in ci["jobs"]["lean-unit"]["steps"]]
    assert any("tests/test_fw2_stale_pe.py" in run and "tests/test_sync_hk_reporting_fx.py" in run
               for run in runs)


# ---------------------------------------------------------------- V1 -------
def test_v1_version_pins():
    # merge note 2026-10-31 (#778): FW-2 SCOPE is 6.72.0 over this 6.71.0, so the pin
    # is a floor, the manifest pin follows the source, and the 6.71.0 block sits
    # below the bundle's 6.72.0 block (searched in the first 12000 characters).
    assert tuple(int(x) for x in sync.SCRIPT_VERSION.split(".")) >= (6, 71, 0)
    src = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_dashboard_sync", "%s", "dashboard sync")' % sync.SCRIPT_VERSION in src
    head = (_REPO / "scripts" / "run_dashboard_sync.py").read_text(encoding="utf-8")[:12000]
    assert "v6.71.0 - FW-2 keeps a witnessed row whose P/E is only stale (2026-10-29)" in head


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
