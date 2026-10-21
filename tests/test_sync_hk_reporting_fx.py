#!/usr/bin/env python3
"""tests/test_sync_hk_reporting_fx.py - run_dashboard_sync v6.70.0 HK-FX:
the identity checks accept the Hong Kong reporting-currency convention.

WHY (2026-10-19)
----------------
FW-2, FW-1 and the L3b tripwire test Price/EPS against the stated P/E with
one unit exemption (the 50-200x GBX/GBP band). Hong Kong-listed mainland
issuers quote in HKD and report EPS in CNY, so Price/EPS reads the stated
P/E times ~1.08 and the row was quarantined as identity-broken on every run:
run 39234358605 stripped 47 Global_Markets rows (2601.HK, 0728.HK, 2359.HK,
1766.HK, 0998.HK, 0285.HK, 1398.HK, 0386.HK, 0857.HK, 0883.HK ...). Seven had
no last-good row and were written as blank stubs whose empty Last Updated
cells make the coverage audit warn, and a coverage warning refuses every
decision-feed release.

Pinned here:
  H1  an H-share (HKD price, CNY EPS) passes FW-2 with the gate on and is
      quarantined exactly as before with the gate off (v6.69.0 decisions)
  H2  a USD reporter on .HK (ratio ~7.8, the HKD peg) passes
  H3  the band is scoped to .HK: the same ratio on another venue is stripped
  H4  a .HK row outside both bands is still stripped (transpositions caught)
  H5  the GBX/GBP band is unchanged and unconditional
  H6  FW-1 (_klg_old_row_identity_ok) agrees with FW-2 when given the symbol
      and keeps v6.69.0 behaviour without one
  H7  the L3b tripwire counts the same rows as FW-2
  H8  band overrides parse, invalid values fall back to the defaults
  H9  diagnostics: stripped rows carry ratio and Currency; exempt .HK rows
      are listed
  H10 the canned identity self-test still passes 9/9
  H11 version and manifest pin
  H12 scripts/repair_stores.py v1.0.1 B1 gives the same verdict as the sync
      on every fixture, gate on and off (a manual apply run must not blank
      the rows the sync now publishes)

Run: pytest -q tests/test_sync_hk_reporting_fx.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import repair_stores as repair  # noqa: E402
from scripts import run_dashboard_sync as sync  # noqa: E402

GATE = "TFB_SYNC_COH_HK_REPORTING_FX"
CNY_BAND = "TFB_SYNC_COH_HK_CNY_BAND"
USD_BAND = "TFB_SYNC_COH_HK_USD_BAND"
H = ["Symbol", "Name", "Price", "EPS (TTM)", "P/E (TTM)", "Currency", "Warnings"]
NI, PI, EI, QI = 1, 2, 3, 4

# implied 5.80/0.90 = 6.444, stated 5.95 -> ratio 1.083 (HKD per CNY), rel 8.3%
HSHARE = ["1398.HK", "Industrial and Commercial Bank of China", 5.80, 0.90, 5.95, "HKD", ""]
# implied 98.0/1.30 = 75.38, stated 9.70 -> ratio 7.77 (HKD per USD peg)
USD_REPORTER = ["0005.HK", "HSBC Holdings plc", 98.0, 1.30, 9.70, "HKD", ""]
# same numbers as the H-share but on another venue
NON_HK = ["XYZ.AX", "Some Australian Co", 5.80, 0.90, 5.95, "AUD", ""]
# a .HK row whose ratio fits neither band: 1.50 and 0.80
HK_BROKEN_HI = ["0386.HK", "Sinopec", 9.0, 1.0, 6.0, "HKD", ""]
HK_BROKEN_LO = ["0857.HK", "PetroChina", 4.0, 1.0, 5.0, "HKD", ""]
GBX = ["NG.L", "National Grid plc", 1050.0, 0.60, 17.5, "GBX", ""]
HEALTHY = ["MSFT", "Microsoft Corporation", 500.0, 12.5, 40.0, "USD", ""]
POISONED = ["GOOG", "Gulfport Energy Corporation", 213.5, 9.18, 3.6, "USD", ""]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (GATE, CNY_BAND, USD_BAND):
        monkeypatch.delenv(name, raising=False)


def _fw2(rows):
    matrix = [list(r) for r in rows]
    out, stripped = sync._row_identity_firewall(list(H), matrix)
    return out, stripped


# --- H1 / H2 -------------------------------------------------------------

def test_h1_hshare_passes_fw2_with_gate_on():
    out, stripped = _fw2([HSHARE, HEALTHY])
    assert stripped == []
    assert out[0] == HSHARE


@pytest.mark.parametrize("value", ["0", "false", "off", "no", " OFF "])
def test_h1b_gate_off_restores_v669_quarantine(monkeypatch, value):
    monkeypatch.setenv(GATE, value)
    out, stripped = _fw2([HSHARE, HEALTHY])
    assert stripped == ["1398.HK"]
    assert out[0][0] == "1398.HK" and out[0][1] == "" and out[0][2] == ""
    assert out[0][H.index("Warnings")] == "identity_quarantined:v6.24.0"
    assert out[1] == HEALTHY


def test_h2_usd_reporter_on_hk_passes():
    _, stripped = _fw2([USD_REPORTER])
    assert stripped == []


# --- H3 / H4 / H5 -----------------------------------------------------------

def test_h3_band_is_scoped_to_hk():
    _, stripped = _fw2([NON_HK, HSHARE])
    assert stripped == ["XYZ.AX"]


def test_h4_hk_rows_outside_both_bands_still_stripped():
    _, stripped = _fw2([HK_BROKEN_HI, HK_BROKEN_LO, POISONED, HSHARE])
    assert stripped == ["0386.HK", "0857.HK", "GOOG"]


@pytest.mark.parametrize("gate", [None, "0"])
def test_h5_gbx_band_unchanged(monkeypatch, gate):
    if gate is not None:
        monkeypatch.setenv(GATE, gate)
    _, stripped = _fw2([GBX, HEALTHY])
    assert stripped == []
    assert sync._coh_ratio_is_unit_convention(100.0) is True
    assert sync._coh_ratio_is_unit_convention(100.0, "NG.L") is True


# --- H6 ----------------------------------------------------------------------

def test_h6_fw1_agrees_with_fw2_given_the_symbol(monkeypatch):
    ok = sync._klg_old_row_identity_ok
    assert ok(HSHARE, NI, PI, EI, QI, "1398.HK") is True
    assert ok(USD_REPORTER, NI, PI, EI, QI, "0005.HK") is True
    assert ok(NON_HK, NI, PI, EI, QI, "XYZ.AX") is False
    assert ok(HK_BROKEN_HI, NI, PI, EI, QI, "0386.HK") is False
    # no symbol: exactly the v6.69.0 decision (only the GBX band)
    assert ok(HSHARE, NI, PI, EI, QI) is False
    assert ok(GBX, NI, PI, EI, QI) is True
    monkeypatch.setenv(GATE, "off")
    assert ok(HSHARE, NI, PI, EI, QI, "1398.HK") is False


# --- H7 ----------------------------------------------------------------------

def test_h7_tripwire_counts_the_same_rows(monkeypatch):
    rows = [list(r) for r in (HSHARE, USD_REPORTER, HEALTHY, POISONED, NON_HK, GBX)]
    testable, bad, _samples = sync._coherence_scan(list(H), rows)
    assert testable == 6 and bad == 2          # POISONED, NON_HK
    monkeypatch.setenv(GATE, "0")
    testable0, bad0, _ = sync._coherence_scan(list(H), [list(r) for r in rows])
    assert testable0 == 6 and bad0 == 4        # + HSHARE, USD_REPORTER (v6.69.0)


# --- H8 ----------------------------------------------------------------------

def test_h8_band_override_narrows(monkeypatch):
    monkeypatch.setenv(CNY_BAND, "1.00,1.05")
    _, stripped = _fw2([HSHARE])
    assert stripped == ["1398.HK"]
    monkeypatch.setenv(USD_BAND, "7.80,7.85")
    _, stripped = _fw2([USD_REPORTER])
    assert stripped == ["0005.HK"]           # 7.77 is below the narrowed band


@pytest.mark.parametrize("bad", ["abc", "1.2,1.1", "0,1.1", "-1,2", "nan,2", "1.1", ","])
def test_h8b_invalid_band_falls_back_to_default(monkeypatch, bad):
    monkeypatch.setenv(CNY_BAND, bad)
    assert sync._coh_band_env(CNY_BAND, sync._COH_HK_CNY_BAND_DEFAULT) == sync._COH_HK_CNY_BAND_DEFAULT
    _, stripped = _fw2([HSHARE])
    assert stripped == []


# --- H9 ----------------------------------------------------------------------

def test_h9_diagnostics_name_ratio_and_currency():
    _fw2([HSHARE, POISONED, NON_HK, HEALTHY])
    diag = sync._LAST_FW2_DIAG
    assert diag["hk_fx_exempt"] == ["1398.HK"]
    syms = [s for s, _r, _c in diag["stripped"]]
    assert syms == ["GOOG", "XYZ.AX"]
    goog = diag["stripped"][0]
    assert goog[2] == "USD" and abs(goog[1] - round(213.5 / 9.18 / 3.6, 4)) < 1e-9
    # a pass with nothing to report resets the record
    _fw2([HEALTHY])
    assert sync._LAST_FW2_DIAG == {"stripped": [], "hk_fx_exempt": []}


def test_h9b_missing_currency_column_still_works():
    hdr = [h for h in H if h != "Currency"]
    rows = [[c for i, c in enumerate(r) if H[i] != "Currency"] for r in (HSHARE, POISONED)]
    _out, stripped = sync._row_identity_firewall(hdr, rows)
    assert stripped == ["GOOG"]
    assert sync._LAST_FW2_DIAG["stripped"][0][2] == ""


# --- H10 / H11 ---------------------------------------------------------------

def test_h10_identity_selftest_still_passes():
    assert sync._idfw_selftest_() is True
    assert sync._IDFW_SELFTEST_MSG == "PASS 9/9"


def test_h11_version_and_manifest_pin():
    # 6.70.2 (2026-10-21): STAMP-RETRY; the HK-FX contract itself is unchanged.
    # merge note (2026-10-19, #778): FW-2 SCOPE moved the file to 6.71.0, so this
    # is a version floor and the manifest pin is read from the source constant.
    assert tuple(int(x) for x in sync.SCRIPT_VERSION.split(".")) >= (6, 70, 0)
    src = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert f'("run_dashboard_sync", "{sync.SCRIPT_VERSION}", "dashboard sync")' in src
    ci = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "tests/test_sync_hk_reporting_fx.py" in ci


# --- H12 ---------------------------------------------------------------------

ALL_FIXTURES = (HSHARE, USD_REPORTER, NON_HK, HK_BROKEN_HI, HK_BROKEN_LO, GBX, HEALTHY, POISONED)


@pytest.mark.parametrize("gate", [None, "0"])
def test_h12_repair_tool_agrees_with_the_sync(monkeypatch, gate):
    if gate is not None:
        monkeypatch.setenv(GATE, gate)
    _, stripped = _fw2(list(ALL_FIXTURES))
    broken = [r[0] for r in ALL_FIXTURES
              if repair._row_identity_broken(r[PI], r[EI], r[QI], r[0])]
    assert broken == stripped
    if gate is None:
        assert broken == ["XYZ.AX", "0386.HK", "0857.HK", "GOOG"]
    else:
        assert "1398.HK" in broken and "0005.HK" in broken


def test_h12b_repair_tool_mirrors_bands_and_version(monkeypatch):
    assert repair.TOOL_VERSION == "1.0.1"
    assert repair._COH_HK_CNY_BAND_DEFAULT == sync._COH_HK_CNY_BAND_DEFAULT
    assert repair._COH_HK_USD_BAND_DEFAULT == sync._COH_HK_USD_BAND_DEFAULT
    assert repair._COH_REL_TOL == sync._COH_REL_TOL
    # no symbol: the v1.0.0 decision (only the GBX band)
    assert repair._row_identity_broken(HSHARE[PI], HSHARE[EI], HSHARE[QI]) is True
    monkeypatch.setenv(CNY_BAND, "1.00,1.05")
    assert repair._row_identity_broken(HSHARE[PI], HSHARE[EI], HSHARE[QI], "1398.HK") is True


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
