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
            if isinstance(node, ast.Assign):
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
