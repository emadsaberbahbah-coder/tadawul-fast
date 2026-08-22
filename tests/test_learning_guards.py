# -*- coding: utf-8 -*-
"""
tests/test_learning_guards.py
================================================================================
Tests for the guards that protect the LEARNING layer — shipped 2026-07-27.

WHY THIS FILE EXISTS
--------------------
A repo-wide review on 2026-07-27 found the test suite inverted along a single
line: the modules that DECIDE were well covered (opportunity_builder,
portfolio_actions, top10_selector, scoring_engine, data_engine_v2, stats,
identity_guard, symbol_dedup, trend_signals, quality_gates), while ~24,700
lines that REMEMBER carried no tests at all — track_performance (7,568),
run_dashboard_sync (6,455), pit_snapshot, corporate_actions, regime, regret.

That is the project's history written in coverage: the engine was built to make
good calls, and only recently began trying to learn from them. Every guard
added that day was proven by a one-off harness and would have been unprotected
from the next refactor. This file makes those proofs permanent.

WHAT IS COVERED — each case is anchored to a REAL live defect, not a fixture
invented to make a function look tested:

  1. track_performance._entry_price_sane        (v6.31.0)
     24 Performance_Log records recorded 25-26 Jul carried entry prices
     102 / 103 / 104 / 105 for 1120.SR / AAPL / MSFT / NVDA — four consecutive
     integers in sorted-symbol order. Not prices.

  2. track_performance._row_is_placeholder      (v6.32.0)
     Root cause: fail-soft PLACEHOLDER rows recorded as HIGH-confidence
     forecasts. data_engine's own changelog documents the generator —
     current_price = 100 + idx, overall_score = 100 - idx*3 — and the live rows
     matched BOTH formulas over the same idx run.

  3. pit_snapshot.harvest_symbols               (v1.1.0)
     The venue-suffix guard silently dropped NTES and YUMC — NTES being 40.3%
     of the book — because a bare US ticker has no dot.

  4. run_shadow_board.build_regime_history_rows (v1.2.0)
     Regime was computed every run and overwritten every run.

  5. quarantine_placeholder_records.build_plan  (v1.0.0)
  6. intraday_quote_refresh.plan_page_updates   (v1.0.1)
     Surgical repair/patch planners: both write into a live production
     workbook, so their refusal paths matter more than their happy paths.

DEPENDENCY DISCIPLINE
---------------------
CI installs numpy + pytest ONLY (no gspread, no google-auth). Every module
under test confines those imports to function bodies, so importing the module
is safe here. Any module that cannot be imported is SKIPPED, never failed —
a missing optional dependency is not a regression.

Run: pytest tests/test_learning_guards.py
================================================================================
"""
import ast
import importlib.util
import os
import sys
import types

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# --------------------------------------------------------------------------- #
# loaders                                                                      #
# --------------------------------------------------------------------------- #
def _load_script(rel_path, alias):
    """Import a scripts/*.py module by path. Returns None if unavailable."""
    full = os.path.join(_ROOT, rel_path)
    if not os.path.exists(full):
        return None
    try:
        spec = importlib.util.spec_from_file_location(alias, full)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        return mod
    except Exception:
        return None


def _extract_funcs(rel_path, names, extra_globals=None):
    """Pull specific top-level functions out of a very large module WITHOUT
    executing it. track_performance is 7,568 lines and imports heavily; the
    guards under test are pure and can be lifted directly from the AST."""
    full = os.path.join(_ROOT, rel_path)
    if not os.path.exists(full):
        return None
    try:
        import statistics
        import typing
        src = open(full, encoding="utf-8").read()
        tree = ast.parse(src)
        mod = types.ModuleType("lifted_" + os.path.basename(rel_path))
        mod.__dict__.update({
            "os": os, "statistics": statistics,
            "Optional": typing.Optional, "List": typing.List,
            "Tuple": typing.Tuple, "Dict": typing.Dict, "Any": typing.Any,
        })
        mod.__dict__.update(extra_globals or {})
        # module-level simple assignments the functions rely on
        for node in tree.body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                try:
                    exec(compile(ast.Module([node], []), "<lift>", "exec"),
                         mod.__dict__)
                except Exception:
                    continue
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in names:
                exec(compile(ast.Module([node], []), "<lift>", "exec"),
                     mod.__dict__)
        for n in names:
            if n not in mod.__dict__:
                return None
        return mod
    except Exception:
        return None


_TP = _extract_funcs(
    "scripts/track_performance.py",
    {"_entry_price_sane", "_row_is_placeholder", "_env_bool", "_env_int",
     "_env_float", "_safe_str"},
)
_PIT = _load_script("scripts/pit_snapshot.py", "lg_pit")
_SB = _load_script("scripts/run_shadow_board.py", "lg_sb")
_QR = _load_script("scripts/quarantine_placeholder_records.py", "lg_qr")
_IQR = _load_script("scripts/intraday_quote_refresh.py", "lg_iqr")


@pytest.fixture(autouse=True)
def _clean_env():
    """Guards read env at call time. Ensure every test starts from defaults."""
    keys = ("TFB_TRACK_ENTRY_SANITY", "TFB_TRACK_ENTRY_SANITY_PCT",
            "TFB_TRACK_ENTRY_SANITY_MIN_N", "TFB_TRACK_PLACEHOLDER_GATE",
            "TFB_SB_REGIME_HISTORY")
    saved = {k: os.environ.pop(k, None) for k in keys}
    yield
    for k, v in saved.items():
        if v is not None:
            os.environ[k] = v
        else:
            os.environ.pop(k, None)


# =========================================================================== #
# 1. ENTRY-PRICE SANITY  (track_performance v6.31.0)                          #
# =========================================================================== #
pytestmark_tp = pytest.mark.skipif(_TP is None,
                                   reason="track_performance guards unavailable")

# The four live poisoned symbols and the medians they should have been near.
_LIVE_POISON = [
    ("1120.SR", 102.0, [65.90, 66.00, 66.03, 66.05]),
    ("AAPL",    103.0, [283.78, 296.20, 308.63, 333.02]),
    ("MSFT",    104.0, [385.66, 386.74, 387.79, 388.84]),
    ("NVDA",    105.0, [192.53, 194.83, 195.94, 196.93]),
]


@pytestmark_tp
@pytest.mark.parametrize("sym,bad,priors", _LIVE_POISON)
def test_entry_sanity_rejects_each_live_poisoned_price(sym, bad, priors):
    ok, med, dev = _TP._entry_price_sane(sym, bad, priors)
    assert ok is False, "%s entry %s must be rejected" % (sym, bad)
    assert med and med > 0
    assert dev > 0.35


@pytestmark_tp
@pytest.mark.parametrize("sym,bad,priors", _LIVE_POISON)
def test_entry_sanity_admits_the_true_price_for_the_same_symbol(sym, bad, priors):
    """The guard must not simply reject the symbol — the real price passes."""
    ok, _med, _dev = _TP._entry_price_sane(sym, priors[-1], priors)
    assert ok is True


@pytestmark_tp
def test_entry_sanity_never_blocks_a_symbol_for_being_new():
    """Fewer than MIN_N priors => always admitted. A first sighting must never
    be rejected merely because there is nothing to compare it against."""
    assert _TP._entry_price_sane("NEW.US", 9999.0, [])[0] is True
    assert _TP._entry_price_sane("NEW.US", 9999.0, [10.0, 11.0])[0] is True


@pytestmark_tp
def test_entry_sanity_uses_median_so_one_bad_prior_cannot_poison_it():
    ok, med, _ = _TP._entry_price_sane(
        "AAPL", 283.0, [283.0, 284.0, 285.0, 103.0, 286.0])
    assert ok is True
    assert abs(med - 284.0) < 1e-9


@pytestmark_tp
def test_entry_sanity_allows_a_genuine_large_move():
    assert _TP._entry_price_sane(
        "OK.US", 130.0, [100.0, 100.0, 100.0, 100.0])[0] is True


@pytestmark_tp
def test_entry_sanity_kill_switch_restores_prior_behaviour():
    os.environ["TFB_TRACK_ENTRY_SANITY"] = "0"
    for sym, bad, priors in _LIVE_POISON:
        assert _TP._entry_price_sane(sym, bad, priors)[0] is True


@pytestmark_tp
def test_entry_sanity_ignores_non_positive_input():
    """Zero/negative is the caller's existing check, not this gate's job."""
    for px in (0.0, -5.0):
        assert _TP._entry_price_sane("X", px, [1.0, 2.0, 3.0, 4.0])[0] is True


# =========================================================================== #
# 2. PLACEHOLDER REJECTION  (track_performance v6.32.0)                       #
# =========================================================================== #
@pytestmark_tp
def test_placeholder_caught_on_the_real_producer_contract():
    """The exact strings routes/analysis_sheet_rows._build_placeholder_rows
    sets. This is the producer contract, not a guessed marker."""
    row = {"symbol": "NVDA", "current_price": 105.0,
           "warnings": "Placeholder fallback — no live data available "
                       "for this symbol",
           "selection_reason": "Placeholder — upstream returned no usable "
                               "rows; no real ranking applied"}
    assert _TP._row_is_placeholder(row)[0] is True


@pytestmark_tp
@pytest.mark.parametrize("row", [
    {"data_provider": "PLACEHOLDER_NO_LIVE_DATA"},
    {"data_quality": "NO_DATA"},
    {"provider": "local_dictionary_fallback"},
    {"warnings": "auto-generated fallback row"},
    {"notes": "generated locally because upstream returned nothing"},
])
def test_placeholder_caught_on_each_declared_marker(row):
    row.setdefault("symbol", "X")
    assert _TP._row_is_placeholder(row)[0] is True


@pytestmark_tp
def test_placeholder_lets_a_genuine_row_through():
    row = {"symbol": "MA", "current_price": 539.66,
           "data_provider": "yahoo_chart", "data_quality": "OK",
           "warnings": "", "recommendation_reason": "momentum + value"}
    assert _TP._row_is_placeholder(row)[0] is False


@pytestmark_tp
def test_placeholder_reports_a_blind_spot_instead_of_a_clean_bill():
    """A row carrying NO marker field at all must return an explicit
    'cannot see' — not False-meaning-fine. The distinction is what lets the
    caller COUNT the blind spot instead of assuming it away."""
    assert _TP._row_is_placeholder({"symbol": "X", "current_price": 10.0}) \
        == (False, "no-marker-fields")


@pytestmark_tp
def test_placeholder_closes_the_hole_entry_sanity_cannot_cover():
    """The two guards are independent ON PURPOSE. A placeholder row for a
    symbol with NO history is invisible to the statistical gate and must be
    caught by the declarative one."""
    row = {"symbol": "BRAND.NEW", "current_price": 101.0,
           "data_provider": "PLACEHOLDER_NO_LIVE_DATA"}
    assert _TP._row_is_placeholder(row)[0] is True
    assert _TP._entry_price_sane("BRAND.NEW", 101.0, [])[0] is True  # blind


@pytestmark_tp
def test_placeholder_kill_switch_restores_prior_behaviour():
    os.environ["TFB_TRACK_PLACEHOLDER_GATE"] = "0"
    assert _TP._row_is_placeholder(
        {"symbol": "X", "data_provider": "PLACEHOLDER_NO_LIVE_DATA"}) \
        == (False, "")


# =========================================================================== #
# 3. PIT HARVEST ORACLE  (pit_snapshot v1.1.0)                                #
# =========================================================================== #
pytestmark_pit = pytest.mark.skipif(_PIT is None,
                                    reason="pit_snapshot unavailable")

_GRID = [["Symbol", "Name"], ["1150.SR", "Alinma"], ["NTES", "NetEase"],
         ["YUMC", "Yum China"], ["COUNT", "-"], ["FORECAST", "-"],
         ["NAME", "-"], ["SEMPRA", "-"]]


@pytestmark_pit
def test_pit_harvest_without_oracle_is_backward_compatible():
    assert _PIT.harvest_symbols(_GRID) == ["1150.SR"]


@pytestmark_pit
def test_pit_harvest_admits_bare_tickers_that_resolve():
    """NTES is 40.3% of the book and was absent from the archive every day."""
    got = _PIT.harvest_symbols(_GRID, known={"NTES", "YUMC"})
    assert got == ["1150.SR", "NTES", "YUMC"]


@pytestmark_pit
def test_pit_harvest_still_rejects_section_artifacts():
    """COUNT/NAME are the SAME SHAPE as NTES/YUMC — only resolution separates
    them, which is why the oracle replaced the regex rather than refining it."""
    got = _PIT.harvest_symbols(_GRID, known={"NTES", "YUMC"})
    for junk in ("COUNT", "FORECAST", "NAME", "SEMPRA"):
        assert junk not in got


@pytestmark_pit
def test_pit_header_extends_the_legacy_set_without_reordering_it():
    assert len(_PIT.HEADERS) == 24
    assert _PIT.HEADERS[:14] == _PIT.LEGACY_HEADERS_16[:14]
    assert _PIT.HEADERS[-2:] == ["Row Last Updated", "Captured At (UTC)"]


@pytestmark_pit
def test_pit_emitted_row_width_matches_header_width():
    idx = {"NTES": {"price": "119.55", "forecast_source": "provider_target"}}
    rows, _skipped, _missing = _PIT.build_rows(
        "2026-07-28", ["NTES"], [("Global_Markets", idx)], set())
    assert len(rows) == 1
    assert len(rows[0]) == len(_PIT.HEADERS)


@pytestmark_pit
def test_pit_captures_forecast_source_the_field_the_root_cause_turned_on():
    idx = {"NTES": {"forecast_source": "provider_target"}}
    rows, _s, _m = _PIT.build_rows("2026-07-28", ["NTES"],
                                   [("Global_Markets", idx)], set())
    assert "provider_target" in [str(c) for c in rows[0]]


# =========================================================================== #
# 4. REGIME HISTORY  (run_shadow_board v1.2.0)                                #
# =========================================================================== #
pytestmark_sb = pytest.mark.skipif(_SB is None,
                                   reason="run_shadow_board unavailable")

_LIVE_REGIME = {
    "version": "1.0.0",
    "sleeves": {"global": {"state": "RISK_ON", "distance_pct": 4.2,
                           "months_in_state": 3, "abs_mom_pct": 11.7},
                "saudi": {"state": "UNKNOWN", "distance_pct": None,
                          "months_in_state": None, "abs_mom_pct": None}},
    "suggested_weights": {"global": 0.7, "saudi": 0.3},
    "errors": [],
}


@pytestmark_sb
def test_regime_history_one_row_per_sleeve_at_header_width():
    rows = _SB.build_regime_history_rows(_LIVE_REGIME, "2026-07-27 20:10:00",
                                         "2026-07-27")
    assert [r[2] for r in rows] == ["global", "saudi"]
    assert all(len(r) == len(_SB.REGIME_HISTORY_HEADER) for r in rows)


@pytestmark_sb
def test_regime_history_records_an_unknown_sleeve_rather_than_dropping_it():
    """An absent reading at time t is itself point-in-time evidence."""
    rows = _SB.build_regime_history_rows(_LIVE_REGIME, "t", "d")
    saudi = [r for r in rows if r[2] == "saudi"][0]
    assert saudi[3] == "UNKNOWN"
    assert saudi[7] == 0.3


@pytestmark_sb
def test_regime_history_never_writes_the_string_none():
    rows = _SB.build_regime_history_rows(_LIVE_REGIME, "t", "d")
    assert not any(str(c) == "None" for r in rows for c in r)


@pytestmark_sb
def test_regime_history_survives_a_new_sleeve_without_schema_change():
    rows = _SB.build_regime_history_rows(
        {"sleeves": {"a": {}, "b": {}, "c": {}}}, "t", "d")
    assert len(rows) == 3


@pytestmark_sb
@pytest.mark.parametrize("block", [None, {}, {"sleeves": {}}])
def test_regime_history_empty_block_is_not_a_crash(block):
    assert _SB.build_regime_history_rows(block, "t", "d") == []


# =========================================================================== #
# 5. QUARANTINE PLANNER  (quarantine_placeholder_records v1.0.0)              #
# =========================================================================== #
pytestmark_qr = pytest.mark.skipif(_QR is None,
                                   reason="quarantine script unavailable")

_QR_HDR = ["Record ID", "Key", "Symbol", "Horizon", "Date Recorded (Riyadh)",
           "Entry Price", "Entry Recommendation", "Entry Score", "Risk Bucket",
           "Confidence", "Origin Tab", "Target Price", "Target ROI %",
           "Target Date (Riyadh)", "Status", "Current Price",
           "Unrealized ROI %", "Realized ROI %", "Outcome", "Volatility",
           "Max Drawdown %", "Sharpe Ratio", "Sector", "Factor Exposures",
           "Last Updated (Riyadh)", "Maturity Date", "Notes"]


def _qr_row(sym, date, entry, notes=""):
    r = [""] * len(_QR_HDR)
    r[2], r[3], r[4], r[5] = sym, "1M", date, str(entry)
    r[14], r[26] = "active", notes
    return r


@pytestmark_qr
def test_quarantine_targets_only_verified_poisoned_rows():
    values = [[""], [""], [""], [""], _QR_HDR,
              _qr_row("1120.SR", "2026-07-25", 102),
              _qr_row("MA", "2026-07-25", 539.66),
              _qr_row("AAPL", "2026-07-10", 283.78)]
    plan, anomalies, hdr_i, _cols = _QR.build_plan(values)
    assert hdr_i == 4
    assert [p["symbol"] for p in plan] == ["1120.SR"]
    assert anomalies == []


@pytestmark_qr
def test_quarantine_skips_a_drifted_value_rather_than_guessing():
    values = [[""], [""], [""], [""], _QR_HDR,
              _qr_row("MSFT", "2026-07-25", 999)]
    plan, anomalies, _h, _c = _QR.build_plan(values)
    assert plan == []
    assert any("DRIFTED" in a["why"] for a in anomalies)


@pytestmark_qr
def test_quarantine_is_idempotent():
    values = [[""], [""], [""], [""], _QR_HDR,
              _qr_row("NVDA", "2026-07-26", 105, notes="voided:v1.0.0:x")]
    plan, anomalies, _h, _c = _QR.build_plan(values)
    assert plan == []
    assert any(a["why"] == "already voided" for a in anomalies)


@pytestmark_qr
def test_quarantine_uses_the_existing_exclusion_status():
    """A NEW status would be silently coerced back to ACTIVE by
    track_performance's loader, re-activating every voided row."""
    assert _QR.VOID_STATUS == "expired"
    assert len(_QR.TARGETS) == 8
    assert _QR.EXPECTED_RECORDS == 24


# =========================================================================== #
# 6. INTRADAY QUOTE PATCH PLANNER  (intraday_quote_refresh v1.0.1)            #
# =========================================================================== #
pytestmark_iqr = pytest.mark.skipif(_IQR is None,
                                    reason="intraday refresh unavailable")

_IQR_HDR = ["Symbol", "Name", "Current Price", "Last Updated (Riyadh)"]
_IQR_PAGE = [_IQR_HDR,
             ["AAPL", "Apple", "100.0", "2026-07-27 11:00:00"],
             ["MSFT", "Microsoft", "380.0", "2026-07-27 11:00:00"],
             ["ZZZZ", "Other", "5.0", "2026-07-27 11:00:00"]]
_IQR_Q = {"AAPL": {"price": 333.02, "last_updated": "2026-07-27 13:45:00"},
          "MSFT": {"price": 381.70, "last_updated": "2026-07-27 10:00:00"},
          "NVDA": {"price": 206.84, "last_updated": "2026-07-27 13:45:00"}}


@pytestmark_iqr
def test_intraday_staleness_is_one_way():
    """MSFT's incoming stamp is OLDER than the page. The patch must never move
    a page backwards in time."""
    plan, stats = _IQR.plan_page_updates("Market_Leaders", _IQR_PAGE, _IQR_Q)
    assert [p["symbol"] for p in plan] == ["AAPL"]
    assert stats["skipped_not_newer"] == 1


@pytestmark_iqr
def test_intraday_never_inserts_a_symbol_absent_from_the_page():
    plan, _s = _IQR.plan_page_updates("X", _IQR_PAGE, _IQR_Q)
    assert all(p["symbol"] != "NVDA" for p in plan)


@pytestmark_iqr
def test_intraday_touches_exactly_two_columns():
    plan, _s = _IQR.plan_page_updates("X", _IQR_PAGE, _IQR_Q)
    assert plan[0]["price_col"] == 2 and plan[0]["stamp_col"] == 3
    assert plan[0]["sheet_row"] == 2


@pytestmark_iqr
def test_intraday_refuses_a_zero_price():
    plan, _s = _IQR.plan_page_updates(
        "X", _IQR_PAGE, {"AAPL": {"price": 0,
                                  "last_updated": "2026-07-27 13:45:00"}})
    assert plan == []


@pytestmark_iqr
def test_intraday_finds_a_header_block_that_is_not_row_one():
    """Top_10_Investments puts its 300-row candidate audit header at row 51.
    Reading row 1 harvested 8 symbols instead of 311 and silently missed the
    entire gated pool — the exact set the refresh exists to serve."""
    page = {"Top_10_Investments": [
        ["Decision Top 10", ""], ["generated", "2026-07-27"], ["", ""],
        ["Symbol", "Name", "Ticket"], ["MRP.US", "Millrose", "19773"],
        ["", ""],
        ["Symbol", "Name", "Market", "Verdict"],
        ["1120.SR", "Al Rajhi", "TASI", "BLOCKED"],
        ["EXE.US", "Expand", "NYSE", "BLOCKED"]]}
    assert _IQR.harvest_symbols(page) == ["MRP.US", "1120.SR", "EXE.US"]


@pytestmark_iqr
def test_intraday_missing_columns_and_empty_page_are_not_crashes():
    assert _IQR.plan_page_updates(
        "X", [["Symbol", "Name"], ["AAPL", "x"]], _IQR_Q)[0] == []
    assert _IQR.plan_page_updates("X", [], _IQR_Q)[0] == []


# =========================================================================== #
# meta                                                                        #
# =========================================================================== #
def test_at_least_one_guard_module_loaded():
    """If EVERY module fails to import, the skips above would make this file
    pass while testing nothing. Fail loudly instead."""
    assert any(m is not None for m in (_TP, _PIT, _SB, _QR, _IQR)), \
        "no learning-layer guard module could be imported"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))


# =========================================================================== #
# GUARDS 5-7 — shipped 2026-08-02 (Render Shell test campaign, same doctrine) #
# =========================================================================== #
# WHY THESE EXIST — anchored to REAL live defects found 2026-08-02:
#
#   5. routes/advanced_analysis._placeholder_value_for_key   (v4.15.0)
#      POST /v1/analysis/sheet-rows for the .AB/.PS chunk returned FABRICATED
#      rows: name "Market_Leaders ADNOCDIST.AB", prices 101.0/102.0/103.0
#      (100+idx), recommendation "Accumulate", FRESH now() timestamps,
#      provider "advanced_analysis.placeholder_fallback". The sync wrote them
#      verbatim: workbook Name cells "Global_Markets HELN.SW", open_price
#      104.00-108.00 on unrelated .SR rows, GAB$H.US at 107.00 / +7,900%
#      (percent 79.00 = 100-idx*3 rendered as a fraction x100). Same generator
#      family as defect #2 above — this time caught at the ROUTE, its source.
#      v4.15.0 stub mode (default ON) must emit ZERO fabricated values; the
#      legacy kill-switch must stay byte-faithful so rollback is honest.
#
#   6. scripts/run_dashboard_sync._fabrication_tripwire      (v6.31.0 FW-5)
#      Sync-side defense-in-depth: any outgoing row whose Name matches the
#      "<Page> <Symbol>" fabrication pattern or whose provider contains
#      "placeholder_fallback" is stripped to a tagged symbol-only stub.
#      Honest v4.15.0 no_data_stub rows must PASS (KLG owns them).
#
#   7. scripts/run_dashboard_sync heal-first HF-2            (v6.31.0)
#      18 rows poisoned with "Global_Markets <sym>" names never healed:
#      v6.24.2 HF-1 prioritized BLANK names only, and a poisoned name is
#      non-blank. HF-2 treats fabricated-pattern names as blank-equivalent.
#      The kill-switch must restore v6.30.0 ordering byte-identically.
#
# Loader note: same _extract_funcs AST-lift as the guards above — if the
# v4.15.0 / v6.31.0 symbols are not yet on the branch under test, the lift
# returns None and every guard SKIPS with a version-pinned reason instead of
# failing an unrelated PR.

import json as _g5_json
import re as _g5_re
import typing as _g5_typing
from datetime import datetime as _g5_dt, timezone as _g5_tz

_AA = _extract_funcs(
    "routes/advanced_analysis.py",
    {"_placeholder_stub_mode", "_stub_value_for_key", "_placeholder_value_for_key",
     "_build_placeholder_rows", "_normalize_key_name", "_normalize_symbol_token",
     "_strip"},
    extra_globals={"json": _g5_json, "re": _g5_re,
                   "Sequence": _g5_typing.Sequence, "Mapping": _g5_typing.Mapping,
                   "datetime": _g5_dt, "timezone": _g5_tz},
)
_RDS_FW = _extract_funcs(
    "scripts/run_dashboard_sync.py",
    {"_placeholder_guard_enabled", "_name_is_fabricated", "_fabrication_tripwire",
     "_guard_find_col", "_guard_is_blank", "_guard_norm",
     "_read_existing_page_symbols", "_heal_first_enabled",
     "_universe_cap_v2_enabled", "_page_read_row_bound",
     "_manual_hold_gate_enabled", "_mh_parse_hold_until", "_mh_read_hold",
     # v6.43.0 (W1A-6e): _read_existing_page_symbols now calls
     # _identity_refetch_enabled() unconditionally (tri-partition
     # blanks + suspects + named). Lift the full closure —
     # _identity_suspect_symbols and _name_dedup_min included — or the
     # HF-2 guards NameError inside <lift> instead of exercising the
     # DEFAULT-OFF path they exist to pin.
     "_identity_refetch_enabled", "_identity_suspect_symbols",
     "_name_dedup_min"},
    extra_globals={"re": _g5_re, "Sequence": _g5_typing.Sequence,
                   "datetime": _g5_dt, "timezone": _g5_tz,
                   "logger": types.SimpleNamespace(
                       info=lambda *a, **k: None,
                       warning=lambda *a, **k: None)},
)

# --------------------------------------------------------------------------- #
# v3 VERSION FLAGS — the guards adapt to the contract each file actually      #
# ships, so this suite is green on EVERY commit-order combination of the      #
# 2026-08-03 integrity-closeout wave (route v4.16.0 / sync v6.33.0).          #
# --------------------------------------------------------------------------- #
def _read_src(rel):
    try:
        with open(os.path.join(_ROOT, rel), "r", encoding="utf-8") as fh:
            return fh.read()
    except Exception:
        return ""

_AA_SRC = _read_src("routes/advanced_analysis.py")
_RDS_SRC = _read_src("scripts/run_dashboard_sync.py")
_ROUTE_V16 = "TFB_ALLOW_LEGACY_FABRICATION" in _AA_SRC


def _src_ver(src, pattern):
    """Parse a version constant into a comparable tuple; (0,) when absent."""
    m = __import__("re").search(pattern, src)
    if not m:
        return (0,)
    try:
        return tuple(int(x) for x in m.group(1).split("."))
    except Exception:
        return (0,)


_SYNC_VER = _src_ver(_RDS_SRC, r'SCRIPT_VERSION = "([0-9.]+)"')
_SYNC_V33 = _SYNC_VER >= (6, 33, 0)
_EOD_SRC = _read_src("core/providers/eodhd_provider.py")
_YC_SRC = _read_src("core/providers/yahoo_chart_provider.py")
_EOD_V17 = "_plan_restricted_applies" in _EOD_SRC
_YC_V13 = "history_high_low" in _YC_SRC
_SYNC_V34 = _SYNC_VER >= (6, 34, 0)

_G5_KEYS = ["symbol", "name", "current_price", "previous_close", "open_price",
            "percent_change", "expected_roi_12m", "overall_score",
            "recommendation", "recommendation_reason", "data_provider",
            "warnings", "last_updated_utc", "exchange", "currency", "country",
            "risk_bucket", "top10_rank", "horizon_days", "asset_class",
            "selection_reason", "criteria_snapshot"]


def _g5_mode(value):
    """Set/clear TFB_ANALYSIS_PLACEHOLDER_MODE; returns restore callable."""
    prev = os.environ.pop("TFB_ANALYSIS_PLACEHOLDER_MODE", None)
    if value is not None:
        os.environ["TFB_ANALYSIS_PLACEHOLDER_MODE"] = value
    def _restore():
        os.environ.pop("TFB_ANALYSIS_PLACEHOLDER_MODE", None)
        if prev is not None:
            os.environ["TFB_ANALYSIS_PLACEHOLDER_MODE"] = prev
    return _restore


@pytest.mark.skipif(_AA is None, reason="advanced_analysis v4.15.0 guards not present")
def test_stub_mode_is_the_armed_default():
    restore = _g5_mode(None)
    try:
        assert _AA._placeholder_stub_mode() is True
    finally:
        restore()


@pytest.mark.skipif(_AA is None, reason="advanced_analysis v4.15.0 guards not present")
def test_stub_rows_carry_zero_fabricated_values():
    restore = _g5_mode(None)
    try:
        rows = _AA._build_placeholder_rows(
            page="Global_Markets", keys=_G5_KEYS,
            requested_symbols=["BALN.SW", "ADNOCDIST.AB"], limit=10, offset=0)
        r = rows[0]
        assert r["symbol"] == "BALN.SW"
        assert r["name"] == ""                       # never "<Page> <Symbol>"
        assert r["current_price"] is None            # never 100+idx
        assert r["open_price"] is None               # the 104-108 artifact class
        assert r["percent_change"] is None           # the +7,900% artifact class
        assert r["overall_score"] is None and r["expected_roi_12m"] is None
        assert r["recommendation"] == ""             # never "Accumulate"
        assert r["last_updated_utc"] is None         # never fresh-stamped
        assert r["data_provider"] == "advanced_analysis.no_data_stub"
        assert r["warnings"] == "no_provider_data; placeholder_stub"
        assert r["exchange"] == ""                   # never "NASDAQ/NYSE" guess
        assert r["top10_rank"] is None
    finally:
        restore()


@pytest.mark.skipif(_AA is None, reason="advanced_analysis v4.15.0 guards not present")
def test_stub_keeps_only_structurally_certain_sr_identity():
    restore = _g5_mode(None)
    try:
        r = _AA._build_placeholder_rows(
            page="Market_Leaders", keys=_G5_KEYS,
            requested_symbols=["1120.SR"], limit=5, offset=0)[0]
        assert (r["exchange"], r["currency"], r["country"]) == (
            "Tadawul", "SAR", "Saudi Arabia")
        assert r["current_price"] is None
    finally:
        restore()


@pytest.mark.skipif(_AA is None, reason="advanced_analysis v4.15.0 guards not present")
def test_legacy_killswitch_reproduces_v414_fabrications_exactly():
    """Rollback honesty: 'legacy' must be byte-faithful to the defect it names,
    reproducing the live T13 evidence verbatim.
    v3: under route v4.16.0 the P0-1d hardening requires the dev-only second
    key TFB_ALLOW_LEGACY_FABRICATION=1 — supplied here so byte-fidelity of the
    legacy branch stays pinned; legacy-ALONE-is-blocked is Guard 11."""
    restore = _g5_mode("legacy")
    _prev_allow = os.environ.pop("TFB_ALLOW_LEGACY_FABRICATION", None)
    if _ROUTE_V16:
        os.environ["TFB_ALLOW_LEGACY_FABRICATION"] = "1"
    try:
        rows = _AA._build_placeholder_rows(
            page="Market_Leaders", keys=_G5_KEYS,
            requested_symbols=["ADNOCDIST.AB", "FAB.AB", "BPI.PS"],
            limit=10, offset=0)
        assert rows[0]["name"] == "Market_Leaders ADNOCDIST.AB"
        assert [r["current_price"] for r in rows] == [101.0, 102.0, 103.0]
        assert rows[0]["open_price"] == 101.0
        assert rows[0]["recommendation"] == "Accumulate"
        assert rows[0]["percent_change"] == 97.0
        assert rows[0]["data_provider"] == "advanced_analysis.placeholder_fallback"
        assert rows[0]["warnings"] == "placeholder"
        assert bool(rows[0]["last_updated_utc"])
    finally:
        os.environ.pop("TFB_ALLOW_LEGACY_FABRICATION", None)
        if _prev_allow is not None:
            os.environ["TFB_ALLOW_LEGACY_FABRICATION"] = _prev_allow
        restore()


@pytest.mark.skipif(_AA is None, reason="advanced_analysis v4.15.0 guards not present")
def test_top10_stub_rows_carry_no_fabricated_rank():
    restore = _g5_mode(None)
    try:
        rows = _AA._build_placeholder_rows(
            page="Top_10_Investments", keys=_G5_KEYS,
            requested_symbols=["AAA", "BBB"], limit=5, offset=0)
        assert all(r["top10_rank"] is None for r in rows)
    finally:
        restore()


_G6_HDR = ["Symbol", "Name", "Current Price", "Data Provider", "Warnings"]


@pytest.mark.skipif(_RDS_FW is None, reason="run_dashboard_sync v6.31.0 guards not present")
def test_fw5_name_pattern_matches_fabrications_not_real_names():
    fab = _RDS_FW._name_is_fabricated
    assert fab("Global_Markets HELN.SW") is True
    assert fab("Market_Leaders ADNOCDIST.AB") is True
    assert fab("Baloise Holding AG") is False
    assert fab("Global Markets Inc") is False        # space, not page token
    assert fab("Global_Markets") is False            # bare token, no symbol
    assert fab("") is False and fab(None) is False


@pytest.mark.skipif(_RDS_FW is None, reason="run_dashboard_sync v6.31.0 guards not present")
def test_fw5_strips_fabricated_passes_stub_and_healthy():
    rows = [
        ["BALN.SW", "Global_Markets BALN.SW", "106.0",
         "advanced_analysis.placeholder_fallback", "placeholder"],
        ["XYZ.US", "XYZ Corp", "55.2",
         "advanced_analysis.placeholder_fallback", ""],
        ["BK.US", "", "", "advanced_analysis.no_data_stub",
         "no_provider_data; placeholder_stub"],
        ["AAPL", "Apple Inc.", "213.1", "eodhd", "yahoo_enrichment_applied"],
    ]
    out, stripped = _RDS_FW._fabrication_tripwire(_G6_HDR, [list(r) for r in rows])
    assert "BALN.SW" in stripped and "XYZ.US" in stripped
    assert "BK.US" not in stripped and "AAPL" not in stripped
    assert out[0][0] == "BALN.SW" and out[0][1] == "" and out[0][2] == ""
    assert out[0][4] == "identity_quarantined:fabricated_placeholder:v6.31.0"
    assert out[3][1] == "Apple Inc."


@pytest.mark.skipif(_RDS_FW is None, reason="run_dashboard_sync v6.31.0 guards not present")
def test_fw5_failsafe_missing_columns_is_a_noop():
    _, stripped = _RDS_FW._fabrication_tripwire(["A", "B"], [["x", "y"]])
    assert stripped == []


class _G7MockSheets:
    def read_values(self, *_a, **_k):
        return [["Symbol", "Name", "Price", "D", "E"],
                ["GOOD.US", "Real Company", "1", "", ""],
                ["POIS.US", "Global_Markets POIS.US", "1", "", ""],
                ["BLNK.US", "", "1", "", ""]]


@pytest.mark.skipif(_RDS_FW is None, reason="run_dashboard_sync v6.31.0 guards not present")
def test_hf2_poisoned_names_jump_the_heal_queue():
    prev = os.environ.pop("TFB_SYNC_PLACEHOLDER_GUARD", None)
    try:
        out = _RDS_FW._read_existing_page_symbols(
            _G7MockSheets(), "sid", "Global_Markets", 50)
        assert out == ["POIS.US", "BLNK.US", "GOOD.US"]
    finally:
        if prev is not None:
            os.environ["TFB_SYNC_PLACEHOLDER_GUARD"] = prev


@pytest.mark.skipif(_RDS_FW is None, reason="run_dashboard_sync v6.31.0 guards not present")
def test_hf2_killswitch_restores_v6300_blankonly_order():
    prev = os.environ.pop("TFB_SYNC_PLACEHOLDER_GUARD", None)
    os.environ["TFB_SYNC_PLACEHOLDER_GUARD"] = "0"
    try:
        out = _RDS_FW._read_existing_page_symbols(
            _G7MockSheets(), "sid", "Global_Markets", 50)
        assert out == ["BLNK.US", "GOOD.US", "POIS.US"]
    finally:
        os.environ.pop("TFB_SYNC_PLACEHOLDER_GUARD", None)
        if prev is not None:
            os.environ["TFB_SYNC_PLACEHOLDER_GUARD"] = prev


# --------------------------------------------------------------------------- #
# GUARD 8 — MANUAL-HOLD bridge (v6.32.0): operator manual refresh priority    #
# --------------------------------------------------------------------------- #
from datetime import timedelta as _g8_td

@pytest.mark.skipif(_RDS_FW is None or not hasattr(_RDS_FW, "_mh_parse_hold_until"),
                    reason="run_dashboard_sync v6.32.0 manual-hold not present")
def test_manual_hold_parse_clamp_and_riyadh_naive():
    p = _RDS_FW._mh_parse_hold_until
    now = _g5_dt.now(_g5_tz.utc)
    z = (now + _g8_td(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert abs((p(z) - (now + _g8_td(hours=2))).total_seconds()) < 2
    naive = (now + _g8_td(hours=5)).strftime("%Y-%m-%d %H:%M:%S")  # +5h wall
    assert abs((p(naive) - (now + _g8_td(hours=2))).total_seconds()) < 2  # Riyadh-3h
    assert p("soon") is None and p("") is None and p(None) is None
    far = (now + _g8_td(hours=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if _SYNC_V33:
        # v6.33.0 P0-3b: far-future is REJECTED (fail-open), never re-clamped
        # into a perpetually-rolling hold.
        assert p(far) is None
        ok11 = now + _g8_td(hours=11)
        assert abs((p(ok11.strftime("%Y-%m-%dT%H:%M:%SZ")) - ok11).total_seconds()) < 2
        # v6.33.0 P0-3a: Google Sheets date-serials (epoch 1899-12-30) parse.
        tgt = now + _g8_td(hours=2)
        serial = ((tgt + _g8_td(hours=3)).replace(tzinfo=None)
                  - _g5_dt(1899, 12, 30)).total_seconds() / 86400.0
        assert abs((p(serial) - tgt).total_seconds()) < 2
        assert abs((p(str(serial)) - tgt).total_seconds()) < 2
        assert p("0.5") is None
    else:
        assert p(far) <= now + _g8_td(hours=12, minutes=1)  # v6.32 clamp pin


@pytest.mark.skipif(_RDS_FW is None or not hasattr(_RDS_FW, "_mh_read_hold"),
                    reason="run_dashboard_sync v6.32.0 manual-hold not present")
def test_manual_hold_read_is_fail_open():
    class _Boom:
        def read_values(self, *a): raise RuntimeError("no _Sync_Control tab")
    assert _RDS_FW._mh_read_hold(_Boom(), "sid") == (None, "")
    class _MS:
        def read_values(self, *a):
            return [["Key", "Value"], ["Manual Hold Until", "2020-01-01T00:00:00Z"]]
    until, raw = _RDS_FW._mh_read_hold(_MS(), "sid")
    assert until is not None and raw == "2020-01-01T00:00:00Z"


@pytest.mark.skipif(_RDS_FW is None or not hasattr(_RDS_FW, "_manual_hold_gate_enabled"),
                    reason="run_dashboard_sync v6.32.0 manual-hold not present")
def test_manual_hold_gate_default_on_with_killswitch():
    prev = os.environ.pop("TFB_SYNC_MANUAL_HOLD_GATE", None)
    try:
        assert _RDS_FW._manual_hold_gate_enabled() is True
        os.environ["TFB_SYNC_MANUAL_HOLD_GATE"] = "0"
        assert _RDS_FW._manual_hold_gate_enabled() is False
    finally:
        os.environ.pop("TFB_SYNC_MANUAL_HOLD_GATE", None)
        if prev is not None:
            os.environ["TFB_SYNC_MANUAL_HOLD_GATE"] = prev


# --------------------------------------------------------------------------- #
# GUARD 9 — EODHD plan_restricted isolation (v4.16.0): one uncovered symbol   #
# class must never trip the whole-provider circuit breaker                    #
# --------------------------------------------------------------------------- #
import time as _g9_time

_EODHD = _extract_funcs(
    "core/providers/eodhd_provider.py",
    {"_plan_restricted_isolation_enabled", "_plan_restricted_ttl_sec",
     "_plan_restricted_tokens", "_body_indicates_plan_restricted",
     "_plan_restricted_cache_active", "_plan_restricted_cache_set"},
    extra_globals={"time": _g9_time},
)


@pytest.mark.skipif(_EODHD is None, reason="eodhd_provider v4.16.0 isolation not present")
def test_plan_restricted_matcher_and_precedence_safety():
    m = _EODHD._body_indicates_plan_restricted
    assert m("you are not subscribed to this api.") is True
    assert m("your plan doesn't support this feature") is True
    assert m("api rate limit exceeded, too many requests") is False  # quota stays quota
    assert m("invalid api key") is False                              # auth stays auth
    assert m("") is False
    prev = os.environ.pop("TFB_EODHD_PLAN_RESTRICTED_TOKENS", None)
    try:
        os.environ["TFB_EODHD_PLAN_RESTRICTED_TOKENS"] = "custom probe marker"
        assert m("body with custom probe marker inside") is True
    finally:
        os.environ.pop("TFB_EODHD_PLAN_RESTRICTED_TOKENS", None)
        if prev is not None:
            os.environ["TFB_EODHD_PLAN_RESTRICTED_TOKENS"] = prev


@pytest.mark.skipif(_EODHD is None, reason="eodhd_provider v4.16.0 isolation not present")
def test_plan_restricted_cache_ttl_and_expiry():
    _EODHD._plan_restricted_cache_set("real-time/G9TEST.US")
    assert _EODHD._plan_restricted_cache_active("real-time/G9TEST.US") is True
    assert _EODHD._plan_restricted_cache_active("real-time/OTHER.US") is False
    _EODHD._PLAN_RESTRICTED_CACHE["real-time/G9TEST.US"] = _g9_time.monotonic() - 1
    assert _EODHD._plan_restricted_cache_active("real-time/G9TEST.US") is False
    prev = os.environ.pop("TFB_EODHD_PLAN_RESTRICTED_TTL_SEC", None)
    try:
        os.environ["TFB_EODHD_PLAN_RESTRICTED_TTL_SEC"] = "999999999"
        assert _EODHD._plan_restricted_ttl_sec() == 7 * 86400.0  # deadlock ceiling
    finally:
        os.environ.pop("TFB_EODHD_PLAN_RESTRICTED_TTL_SEC", None)
        if prev is not None:
            os.environ["TFB_EODHD_PLAN_RESTRICTED_TTL_SEC"] = prev


@pytest.mark.skipif(_EODHD is None, reason="eodhd_provider v4.16.0 isolation not present")
def test_plan_restricted_gate_default_on_with_killswitch():
    prev = os.environ.pop("TFB_EODHD_PLAN_RESTRICTED_ISOLATION", None)
    try:
        assert _EODHD._plan_restricted_isolation_enabled() is True
        os.environ["TFB_EODHD_PLAN_RESTRICTED_ISOLATION"] = "0"
        assert _EODHD._plan_restricted_isolation_enabled() is False
    finally:
        os.environ.pop("TFB_EODHD_PLAN_RESTRICTED_ISOLATION", None)
        if prev is not None:
            os.environ["TFB_EODHD_PLAN_RESTRICTED_ISOLATION"] = prev


# --------------------------------------------------------------------------- #
# GUARD 10 — yahoo_chart symmetric range fallbacks (v8.12.0, B8): a present   #
# 52w HIGH must never block healing a missing 52w LOW; day range heals from   #
# the latest candle; real provider values are never overridden               #
# --------------------------------------------------------------------------- #
import math as _g10_math

_YC = _extract_funcs(
    "core/providers/yahoo_chart_provider.py",
    {"_yc_range_fallbacks_enabled", "_apply_range_fallbacks",
     "_first_number", "_safe_float"},
    extra_globals={"math": _g10_math},
)


@pytest.mark.skipif(_YC is None, reason="yahoo_chart v8.12.0 fallbacks not present")
def test_range_fallbacks_heal_only_missing_sides():
    hist = [{"close": 20.0 + i * 0.01, "high": 27.1, "low": 26.0} for i in range(300)]
    prev = os.environ.pop("TFB_YC_RANGE_FALLBACKS", None)
    try:
        dh, dl, h52, l52 = _YC._apply_range_fallbacks(None, None, 30.5, 0.0, hist)
        assert h52 == 30.5                       # real high untouched
        if _YC_V13:
            assert l52 == 26.0                   # v8.13.0: candle-true LOW
        else:
            assert abs(l52 - 20.48) < 1e-9       # v8.12.0 closes-pin (legacy)
        assert dh == 27.1 and dl == 26.0         # day range from last candle
        assert _YC._apply_range_fallbacks(26.9, None, 1.0, 1.0, hist)[0] == 26.9
        assert _YC._apply_range_fallbacks(None, 5.0, None, 2.0, []) == (None, 5.0, None, 2.0)
        assert _YC._apply_range_fallbacks(None, None, None, None, ["x"]) == (None, None, None, None)
    finally:
        if prev is not None:
            os.environ["TFB_YC_RANGE_FALLBACKS"] = prev


@pytest.mark.skipif(_YC is None, reason="yahoo_chart v8.12.0 fallbacks not present")
def test_range_fallbacks_killswitch_restores_v8110():
    hist = [{"close": 21.0, "high": 27.1, "low": 26.0}] * 10
    prev = os.environ.pop("TFB_YC_RANGE_FALLBACKS", None)
    os.environ["TFB_YC_RANGE_FALLBACKS"] = "0"
    try:
        assert _YC._apply_range_fallbacks(None, None, 30.5, 0.0, hist) == (None, None, 30.5, 0.0)
        assert _YC._yc_range_fallbacks_enabled() is False
    finally:
        os.environ.pop("TFB_YC_RANGE_FALLBACKS", None)
        if prev is not None:
            os.environ["TFB_YC_RANGE_FALLBACKS"] = prev


# =========================================================================== #
# GUARD 8b — sync v6.33.0 INTEGRITY CLOSEOUT (audit P0-2 / P0-3c)             #
# =========================================================================== #
@pytest.mark.skipif(not _SYNC_V33, reason="run_dashboard_sync v6.33.0 not present")
def test_v633_manual_hold_skip_is_registered_benign():
    assert '"[MANUAL-HOLD",' in _RDS_SRC          # marker in _BENIGN_SKIP_MARKERS
    assert "deferred (benign)" in _RDS_SRC        # per-task TaskResult.warnings entry


@pytest.mark.skipif(not _SYNC_V33, reason="run_dashboard_sync v6.33.0 not present")
def test_v633_poison_predecessor_certification_closed():
    # KLG error set consults the fabricated-provider token via RAW casefold
    # (P0-2; _guard_norm strips underscores so it could never match through it)
    assert 'str(v or "").casefold()' in _RDS_SRC
    # Leg-1b: fabricated "<Page> <Symbol>" names are never last-GOOD
    assert "_name_is_fabricated(_cell(name_i))" in _RDS_SRC


@pytest.mark.skipif(not _SYNC_V33, reason="run_dashboard_sync v6.33.0 not present")
def test_v633_far_future_reject_is_logged_not_clamped():
    assert "rejected far-future hold" in _RDS_SRC
    assert "min(dt_utc, ceiling)" not in _RDS_SRC


# =========================================================================== #
# GUARD 11 — route v4.16.0 NO-FABRICATION CLOSEOUT (audit P0-1 a/b/c/d)       #
# =========================================================================== #
@pytest.mark.skipif(not _ROUTE_V16 or _AA is None,
                    reason="advanced_analysis v4.16.0 not present")
def test_v416_runtime_version_matches_file():
    assert 'ADVANCED_ANALYSIS_VERSION = "4.16.0"' in _AA_SRC


@pytest.mark.skipif(not _ROUTE_V16 or _AA is None,
                    reason="advanced_analysis v4.16.0 not present")
def test_v416_legacy_alone_is_blocked_fail_closed():
    """P0-1d: without the dev-only second key, 'legacy' (and any boolean-ish
    value) must fail CLOSED to honest stubs — no fabricated name/price/reco."""
    for value in ("legacy", "0", "false", "off"):
        restore = _g5_mode(value)
        prev = os.environ.pop("TFB_ALLOW_LEGACY_FABRICATION", None)
        try:
            r = _AA._build_placeholder_rows(
                page="Market_Leaders", keys=_G5_KEYS,
                requested_symbols=["FAB.AB"], limit=5, offset=0)[0]
            assert r["name"] == ""
            assert r["current_price"] is None
            assert r["recommendation"] == ""
            assert r["data_provider"] == "advanced_analysis.no_data_stub"
        finally:
            if prev is not None:
                os.environ["TFB_ALLOW_LEGACY_FABRICATION"] = prev
            restore()


@pytest.mark.skipif(not _ROUTE_V16, reason="advanced_analysis v4.16.0 not present")
def test_v416_insights_failure_no_generated_recommendations():
    # P0-1b: the Insights fallback stub branch exists and the fabricated
    # Accumulate/Watch generator survives ONLY inside the legacy-gated else.
    assert "fabricated fallback disabled (v4.16.0" in _AA_SRC
    stub_i = _AA_SRC.find("fabricated fallback disabled")
    legacy_i = _AA_SRC.find('"Watch" if idx > 2 else "Accumulate"', stub_i)
    assert legacy_i > stub_i > 0
    assert "if _placeholder_stub_mode():" in _AA_SRC[stub_i-1200:stub_i]


@pytest.mark.skipif(not _ROUTE_V16, reason="advanced_analysis v4.16.0 not present")
def test_v416_ensure_top10_exempts_stub_rows():
    # P0-1c: the second-pass ranker carries the stub exemption.
    assert "P0-1c" in _AA_SRC and _AA_SRC.count("stub row (no fabricated values)") >= 2


# =========================================================================== #
# GUARD 12 — provider corrections (audit P0-4 strict-403 / P1-6 candle range) #
# =========================================================================== #
_EODG12 = _extract_funcs(
    "core/providers/eodhd_provider.py",
    {"_plan_restricted_applies", "_plan_restricted_isolation_enabled",
     "_body_indicates_plan_restricted", "_plan_restricted_tokens"},
    extra_globals={"os": os},
) if _EOD_V17 else None


@pytest.mark.skipif(_EODG12 is None, reason="eodhd v4.17.0 not present")
def test_v417_plan_restricted_is_strictly_http_403():
    prev = os.environ.pop("TFB_EODHD_PLAN_RESTRICTED_ISOLATION", None)
    try:
        g = _EODG12._plan_restricted_applies
        body = "your plan does not include this endpoint"
        assert g(403, False, body) is True
        assert g(401, False, body) is False          # P0-4: 401 stays AuthError
        assert g(403, True, body) is False           # ip-block precedence kept
        assert g(403, False, "quota exceeded, too many calls") is False
    finally:
        if prev is not None:
            os.environ["TFB_EODHD_PLAN_RESTRICTED_ISOLATION"] = prev
    assert "sc == 403" in _EOD_SRC                   # gate literal on file


# reuse Guard 10's _YC lift — same module, proven-good scaffold.
@pytest.mark.skipif(_YC is None or not _YC_V13,
                    reason="yahoo_chart v8.13.0 not present")
def test_v813_52w_uses_candle_extremes_with_provenance():
    hist = ([{"close": 20.0, "high": 30.0, "low": 10.0}]
            + [{"close": 20.0, "high": 21.0, "low": 19.0}] * 260)
    prev = os.environ.pop("TFB_YC_RANGE_FALLBACKS", None)
    try:
        prov = {}
        _, _, h52, l52 = _YC._apply_range_fallbacks(
            None, None, None, None, hist, provenance=prov)
        assert h52 == 21.0 and l52 == 19.0           # 252-window candle truth
        assert prov["range_source"] == "history_high_low"
        # close-only last resort when candles lack high/low
        c_only = [{"close": 20.0 + i * 0.01} for i in range(300)]
        prov2 = {}
        _, _, h2, l2 = _YC._apply_range_fallbacks(
            None, None, None, None, c_only, provenance=prov2)
        assert abs(l2 - 20.48) < 1e-9 and h2 == max(
            c["close"] for c in c_only[-252:])
        assert prov2["range_source"] == "close_only_fallback"
        # provider-supplied values untouched, no range_source emitted
        prov3 = {}
        assert _YC._apply_range_fallbacks(
            5.0, 4.0, 50.0, 40.0, hist, provenance=prov3)[2:] == (50.0, 40.0)
        assert "range_source" not in prov3
        # backward-compatible: kwarg optional
        assert _YC._apply_range_fallbacks(None, None, None, None, hist)[2] == 21.0
    finally:
        if prev is not None:
            os.environ["TFB_YC_RANGE_FALLBACKS"] = prev


# =========================================================================== #
# GUARD 13 — persistence truth (v6.34.0 PV-1/2/3) + identity registry v1.1.0  #
# =========================================================================== #
_CSI13 = None
try:
    import importlib.util as _ilu13
    _sp13 = _ilu13.spec_from_file_location(
        "csi13", os.path.join(_ROOT, "scripts", "critical_symbol_identity.py"))
    _m13 = _ilu13.module_from_spec(_sp13)
    import sys as _sys13
    _sys13.modules["csi13"] = _m13
    _sp13.loader.exec_module(_m13)
    if str(getattr(_m13, "POLICY_VERSION", "0")) >= "1.1.0":
        _CSI13 = _m13
except Exception:
    _CSI13 = None


@pytest.mark.skipif(_CSI13 is None, reason="critical_symbol_identity v1.1.0 not present")
def test_v110_dead_tickers_are_registry_inactive():
    for dead in ("BK.US", "FI.US", "BJK.US", "8270.SR", "3001.SR"):
        assert dead in _CSI13.INACTIVE_SYMBOLS
    assert "BNY.US" in _CSI13.CRITICAL_IDENTITIES
    assert "BK.US" not in _CSI13.CRITICAL_IDENTITIES
    clean, changes = _CSI13.sanitize_active_universe(
        ["AAPL.US", "BK.US", "FI.US", "BJK.US", "BRK.B"])
    assert clean == ["AAPL.US", "BRK-B.US"]
    assert sum(1 for ch in changes if ch.action == "removed") == 3


_RDS13 = _extract_funcs(
    "scripts/run_dashboard_sync.py",
    {"_unpersisted_missing", "_persist_v2_enabled", "_guard_find_col",
     "_guard_is_blank", "_guard_norm", "_name_is_fabricated",
     "_universe_deny_patterns", "_universe_junk"},
    extra_globals={"os": os, "re": __import__("re"), "logger": type("_L13", (object,), {"warning": staticmethod(lambda *a, **k: None)})()},
) if _SYNC_V34 else None


@pytest.mark.skipif(_RDS13 is None, reason="run_dashboard_sync v6.34.0 not present")
def test_v634_hard_guard_counts_only_real_loss():
    H = ["Symbol", "Name", "Price"]
    matrix = [["AAPL.US", "Apple", 1]]
    req = ["AAPL.US", "GOOD.US", "STUB.US", "NEW.US"]
    nmap = {"AAPL.US": False, "GOOD.US": False, "STUB.US": True}
    prev = os.environ.pop("TFB_SYNC_PERSIST_V2", None)
    try:
        assert _RDS13._unpersisted_missing(H, matrix, req, nmap) == ["GOOD.US"]
        assert _RDS13._unpersisted_missing(H, matrix, req, None) == [
            "GOOD.US", "STUB.US", "NEW.US"]          # legacy shape intact
        os.environ["TFB_SYNC_PERSIST_V2"] = "0"      # kill-switch
        assert _RDS13._unpersisted_missing(H, matrix, req, nmap) == [
            "GOOD.US", "STUB.US", "NEW.US"]
    finally:
        os.environ.pop("TFB_SYNC_PERSIST_V2", None)
        if prev is not None:
            os.environ["TFB_SYNC_PERSIST_V2"] = prev


@pytest.mark.skipif(not _SYNC_V34, reason="run_dashboard_sync v6.34.0 not present")
def test_v634_second_chance_and_instrumentation_are_wired():
    assert "second-chance pass restored" in _RDS_SRC          # PV-2 at guard site
    assert "absent_blank_exempt" in _RDS_SRC                  # PV-3 scope log
    assert _RDS_SRC.count("_pv_log()") >= 5                   # PV-1 every exit named
    assert "grid_empty_retried" in _RDS_SRC                   # PV-1 retry
