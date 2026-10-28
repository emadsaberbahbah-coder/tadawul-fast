"""WP-2b (data_engine_v2 v5.153.0) - SETTLE-UNRESOLVED PUBLICATION RULE.

Gate TFB_SETTLE_UNRESOLVED = off | observe | enforce (explicit words only),
consulted only for a NON_CONVERGED F-7 settlement result:

  off (unset)  -> today's fail-closed holdback, byte-for-byte (blank scores,
                  f7_settle_failed:<status>, scoring_settlement JSON,
                  BLOCKED/WATCHLIST, DO_NOT_INVEST, 'clean rebuild required')
  observe      -> the same values PLUS one countable receipt composed from the
                  still-intact pass-1 row: settle_unresolved:observe:<reason>:
                  p<n>:ov=<pass-1 overall>:rc=<pass-1 recommendation>
  enforce      -> the pass-1 row is published (scores kept, rank withheld,
                  scoring_settlement JSON kept as evidence, no failure
                  receipt); the tag caps it at WATCHLIST in the investability
                  gate, keeps it off Top-10 and Rank (Overall) and stops any
                  portfolio ADD - tag-driven, so a later switch to off cannot
                  promote it. A fresh STABLE settlement retires a stale tag.

Status error stays fail-closed in every mode. Evidence: 2026-10-10 export
(run 38071484709) - 46 rows f7_settle_failed:non_converged (GM 45 + CFX 1),
all pass_limit p3, 0 INVESTABLE, 3 BLOCKED on HOLD.

Fixtures are synthetic or the six real 2026-09-22 Global_Markets market rows
reused from tests/test_de_f7_scoring_settle.py (no portfolio data). Every test
pins its own environment (all TFB_* cleared, then set) and restores it.

Run: python -m pytest -q tests/test_de_f7_settle_unresolved.py   (or python tests/test_de_f7_settle_unresolved.py)
"""
from __future__ import annotations

import copy
import functools
import inspect
import os
import re
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.data_engine_v2 as de  # noqa: E402
from tests import test_de_f7_scoring_settle as f7  # noqa: E402  (FIXTURES / _prep / _norm / _failure_row / _assert_withheld)

SETTLE_ENV = "TFB_SCORING_SETTLE"
MAX_ENV = "TFB_SCORING_SETTLE_MAX_PASSES"
UNRES_ENV = "TFB_SETTLE_UNRESOLVED"
FORBIDDEN = f7.FORBIDDEN
UNRES_RE = re.compile(r"^settle_unresolved:(observe|enforce):(pass_limit|oscillation):p\d+(:ov=[^:;]+)?(:rc=[^:;]+)?$")
REASON = "Scoring unresolved after settlement passes (settle_unresolved) -- research only, no new capital"
HINT = "Hold existing; no new capital (scoring unresolved)"
# Inputs that let the synthetic _failure_row clear the base gate chain to
# INVESTABLE (price, forecast, completeness, D/E + FCF, 52W range, risk).
GATE_READY = {"name": "Synthetic test equity", "asset_class": "Equity",
              "week_52_high": 120.0, "week_52_low": 80.0, "avg_volume_30d": 1000000.0,
              "pe_ttm": 15.0, "eps_ttm": 6.67, "market_cap": 1.0e10, "volatility_30d": 0.2,
              "debt_to_equity": 0.5, "free_cash_flow_ttm": 1.0e8, "risk_score": 30.0,
              "forecast_confidence": 0.8}


def _isolated(fn):
    """Clear every TFB_* variable for the test and restore the exact prior
    environment afterwards (the lean CI job runs ~75 files in one process)."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        saved = dict(os.environ)
        # integration 2026-10-11: f7._prep() seeds de._TGT_LKG_STORE; restore it too,
        # else a later suite sees stored 12M targets for the fixture symbols.
        saved_store = dict(de._TGT_LKG_STORE)
        try:
            for key in list(os.environ):
                if key.startswith("TFB_"):
                    del os.environ[key]
            return fn(*args, **kwargs)
        finally:
            os.environ.clear()
            os.environ.update(saved)
            de._TGT_LKG_STORE.clear()
            de._TGT_LKG_STORE.update(saved_store)
    return wrapper


def _env(settle=None, unres=None, max_passes=None, **extra):
    for name, value in ((SETTLE_ENV, settle), (UNRES_ENV, unres), (MAX_ENV, max_passes)):
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = str(value)
    for name, value in extra.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = str(value)


def _drift(r):
    r["overall_score"] += 1.0


def _oscillate(r):
    r["overall_score"] = 20.0 if r["overall_score"] == 80.0 else 80.0


def _settle(row, scorer, max_passes):
    """Drive the REAL _f7_settle_pass with a deterministic non-converging scorer
    (the t6b / t6c pattern of tests/test_de_f7_scoring_settle.py)."""
    os.environ[MAX_ENV] = str(max_passes)
    with patch.object(de, "_compute_scores_canonical_first", scorer), \
            patch.object(de, "_apply_phase_dd_enhancements", lambda r: r):
        return de._f7_settle_pass(row, row.get("symbol", ""), "Global_Markets")


def _parts(row):
    return [p.strip() for p in str(row.get("warnings") or "").split(";") if p.strip()]


def _unres_tags(row):
    return [p for p in _parts(row) if p.startswith("settle_unresolved:")]


def _without_unres(row):
    out = dict(row)
    out["warnings"] = "; ".join(p for p in _parts(row) if not p.startswith("settle_unresolved:"))
    return out


def _holdback_reference(scorer, max_passes):
    """Today's (v5.152.0) non-stable branch, computed directly: the result plus
    _f7_settle_holdback on the same row. v5.158.0 (F-7 v2b): under observe the
    v2 shadow's tag follows the holdback - the failure row carries no
    confidence marker, so its shadow is the result itself: stx:p<n> plus the
    pass-1 -> pass-2 diff, composed by _f7_settle_tag."""
    row = f7._failure_row()
    os.environ[MAX_ENV] = str(max_passes)
    with patch.object(de, "_compute_scores_canonical_first", scorer), \
            patch.object(de, "_apply_phase_dd_enhancements", lambda r: r):
        result = de._f7_settle_result(row)
    de._f7_settle_holdback(row, result)
    if de._f7_settle_mode() == "observe":
        de._v573_append_warning(row, de._f7_settle_tag("observe", None, result.passes_run, list(result.first_diff)))
    return row


def _gate_ready_row():
    row = f7._failure_row()
    row.update(GATE_READY)
    return row


@_isolated
def test_u1_gate_explicit_words_only():
    for raw, want in ((None, "off"), ("", "off"), ("1", "off"), ("true", "off"), ("on", "off"), ("yes", "off"),
                      ("off", "off"), ("garbage", "off"), ("observe", "observe"), ("OBSERVE ", "observe"),
                      (" ENFORCE ", "enforce"), ("enforce", "enforce")):
        _env(unres=raw)
        assert de._f7_unresolved_mode() == want, (raw, want)
        assert de.surface_gate_states().get("settle_unresolved") == want


@_isolated
def test_u2_off_is_todays_holdback_byte_for_byte():
    for settle in ("observe", "enforce"):
        for unres in (None, "off", "1", "true", "on"):
            for scorer, max_passes, reason in ((_drift, 2, "pass_limit"), (_oscillate, 4, "oscillation")):
                _env(settle=settle, unres=unres)
                reference = _holdback_reference(scorer, max_passes)
                out = _settle(f7._failure_row(), scorer, max_passes)
                assert out == reference, (settle, unres, reason)
                f7._assert_withheld(out)
                assert "f7_settle_failed:non_converged" in _parts(out)
                assert not _unres_tags(out)
                assert ('"reason":"%s"' % reason) in out["scoring_errors"]
    # the six real fixtures all converge: the new gate cannot touch them
    for settle in ("observe", "enforce"):
        for fx in f7.FIXTURES:
            outs = []
            for unres in (None, "observe", "enforce"):
                _env(settle=settle, unres=unres, max_passes=4)
                row = f7._prep(fx)
                outs.append(f7._norm(de._f7_settle_pass(row, row["symbol"], "Global_Markets")))
            assert outs[0] == outs[1] == outs[2], (settle, fx["row"]["symbol"])


@_isolated
def test_u3_observe_values_untouched_one_countable_tag():
    for settle in ("observe", "enforce"):
        for scorer, max_passes, reason, passes in ((_drift, 2, "pass_limit", 1), (_oscillate, 4, "oscillation", 2)):
            _env(settle=settle, unres="observe")
            reference = _holdback_reference(scorer, max_passes)
            out = _settle(f7._failure_row(), scorer, max_passes)
            tags = _unres_tags(out)
            assert len(tags) == 1, tags
            tag = tags[0]
            assert UNRES_RE.match(tag), tag
            assert not any(f in tag.lower() for f in FORBIDDEN), tag
            assert tag == "settle_unresolved:observe:%s:p%d:ov=80:rc=buy" % (reason, passes), tag
            # values are exactly today's holdback; only the receipt is added
            assert _without_unres(out) == reference
            f7._assert_withheld(out)
            assert de._f7_settle_failed(out)


@_isolated
def test_u4_enforce_publishes_pass1_capped_unranked_and_tag_driven():
    _env(settle="enforce", unres="enforce")
    out = _settle(f7._failure_row(), _drift, 2)
    assert not de._f7_settle_failed(out)
    assert de._f7_settle_unresolved(out)
    assert out["overall_score"] == 80.0 and out["opportunity_score"] == 85.0
    assert out["recommendation"] == "BUY"           # settle time: pass-1 label kept
    assert out["rank_overall"] is None
    assert _unres_tags(out) == ["settle_unresolved:enforce:pass_limit:p1:ov=80:rc=buy"]
    assert "f7_settle_failed:" not in str(out.get("warnings"))
    # C11: the scoring_settlement JSON stays as evidence beside the tag
    assert "scoring_settlement:" in out["scoring_errors"]
    assert '"status":"non_converged"' in out["scoring_errors"] and '"reason":"pass_limit"' in out["scoring_errors"]
    assert out["position_size_hint"] == HINT
    assert out["current_price"] == 100.0 and out["forecast_price_12m"] == 120.0

    # Gate: the SAME pass-1 values clear the base chain to INVESTABLE without
    # the tag (control); the tag caps the row at WATCHLIST / WATCH.
    _env(settle="enforce", unres="enforce", TFB_INVESTABILITY_GATE="1")
    control = _gate_ready_row()
    de._apply_investability_gate(control)
    assert control["investability_status"] == "INVESTABLE" and control["final_action"] == "INVEST"
    row = _settle(_gate_ready_row(), _drift, 2)
    de._apply_investability_gate(row)
    assert row["investability_status"] == "WATCHLIST" and row["final_action"] == "WATCH"
    assert row["block_reason"].startswith("Scoring unresolved") and row["block_reason"] == REASON
    assert not de._top10_row_is_eligible(row)
    other = {"symbol": "OTHER.US", "overall_score": 50.0}
    de._apply_rank_overall([row, other])
    assert row["rank_overall"] is None and other["rank_overall"] == 1
    assert "rank_skipped_settle_unresolved" in _parts(row)
    projected = de._strict_project_row(de.INSTRUMENT_CANONICAL_KEYS, copy.deepcopy(row))
    assert projected["investability_status"] == "WATCHLIST"
    assert projected["overall_score"] == 80.0
    assert projected["recommendation"] == "HOLD"     # Fix AP on the demoted buy-family row (C1)
    assert projected["rank_overall"] is None
    # Tag-driven: switching every F-7 gate off never promotes the cached row
    # (neither the projected HOLD copy nor the pre-projection BUY row).
    _env(settle=None, unres=None, TFB_INVESTABILITY_GATE="1")
    again = copy.deepcopy(projected)
    de._apply_investability_gate(again)
    assert again["investability_status"] == "WATCHLIST"
    assert not de._top10_row_is_eligible(again)
    cached = copy.deepcopy(row)
    cached.update(recommendation="BUY", recommendation_detailed="BUY", block_reason="")
    de._apply_investability_gate(cached)
    assert cached["investability_status"] == "WATCHLIST" and cached["block_reason"] == REASON
    assert not de._top10_row_is_eligible(cached)
    # A BLOCKED / WATCHLIST base verdict keeps its own reason (demote-only tier).
    blocked = _gate_ready_row()
    blocked.update(current_price=None, warnings="settle_unresolved:enforce:pass_limit:p3:ov=80:rc=buy")
    de._apply_investability_gate(blocked)
    assert blocked["investability_status"] == "BLOCKED" and blocked["block_reason"] == "Missing current price"
    held = _gate_ready_row()
    held.update(recommendation="HOLD", recommendation_detailed="HOLD",
                warnings="settle_unresolved:enforce:pass_limit:p3:ov=80:rc=hold")
    de._apply_investability_gate(held)
    assert held["investability_status"] == "WATCHLIST" and held["block_reason"] == "Engine neutral (HOLD)"
    # Re-publication keeps ONE receipt per row (a stale one is replaced).
    _env(settle="enforce", unres="enforce")
    stale = f7._failure_row()
    stale["warnings"] = "settle_unresolved:enforce:pass_limit:p3:ov=11:rc=sell; keep_me"
    out2 = _settle(stale, _drift, 2)
    assert _unres_tags(out2) == ["settle_unresolved:enforce:pass_limit:p1:ov=80:rc=buy"]
    assert "keep_me" in _parts(out2)


@_isolated
def test_u5_error_stays_fail_closed_and_settle_off_makes_gate_inert():
    def boom(r):
        r["current_price"] = 999
        raise RuntimeError("settle boom")

    for settle in ("observe", "enforce"):
        for unres in ("observe", "enforce"):
            _env(settle=settle, unres=unres)
            row = f7._failure_row()
            with patch.object(de, "_compute_scores_canonical_first", boom):
                out = de._f7_settle_pass(row)
            f7._assert_withheld(out)
            assert '"error_type":"RuntimeError"' in out["scoring_errors"]
            assert not _unres_tags(out)
    for unres in (None, "observe", "enforce"):
        _env(settle=None, unres=unres)
        row = f7._failure_row()
        before = copy.deepcopy(row)
        out = _settle(row, _drift, 2)
        assert out is row and row == before


@_isolated
def test_u6_cached_failure_receipt_always_wins():
    for settle in ("observe", "enforce"):
        for unres in (None, "observe", "enforce"):
            _env(settle=settle, unres=unres)
            row = f7._failure_row()
            row["warnings"] = "f7_settle_failed:non_converged"
            out = _settle(row, _drift, 2)
            assert de._f7_settle_failed(out)
            assert out["overall_score"] is None and out["rank_overall"] is None
            assert out["final_action"] == "DO_NOT_INVEST"
            assert not _unres_tags(out)


@_isolated
def test_u7_tag_composer_exact_safe_and_degrades():
    res = de.F7SettlementResult("non_converged", passes_run=3, reason="pass_limit")
    tag = de._f7_settle_unresolved_tag("enforce", res, {"overall_score": 52.11, "recommendation": "REDUCE"})
    assert tag == "settle_unresolved:enforce:pass_limit:p3:ov=52.11:rc=reduce"
    assert de._f7_settle_unresolved_tag("observe", res, {}) == "settle_unresolved:observe:pass_limit:p3:ov=na:rc=na"
    # recommendations are tokenized, so a forbidden substring is rewritten, not leaked
    safe = de._f7_settle_unresolved_tag("enforce", res, {"overall_score": 52.11, "recommendation": "roi_x"})
    assert safe == "settle_unresolved:enforce:pass_limit:p3:ov=52.11:rc=ri_x"
    assert not any(f in safe for f in FORBIDDEN)
    # defense in depth: if a token ever carried a forbidden substring the tag
    # degrades to its first four parts
    with patch.object(de, "_f7_settle_token", lambda v: str(v).lower() if v else "na"):
        degraded = de._f7_settle_unresolved_tag("enforce", res, {"overall_score": 52.11, "recommendation": "roi_x"})
    assert degraded == "settle_unresolved:enforce:pass_limit:p3"
    # exact prefix, never the substring 'unresolved' (margin receipts end ':unresolved')
    assert not de._f7_settle_unresolved({"warnings": "margin_publish:profit_margin:unresolved; x_settle_unresolved:y"})
    assert de._f7_settle_unresolved({"Warnings": ["a", "settle_unresolved:enforce:pass_limit:p3"]})
    assert de._f7_settle_unresolved_parts({"warnings": "a; settle_unresolved:observe:oscillation:p2; b"}) == \
        ["settle_unresolved:observe:oscillation:p2"]


@_isolated
def test_u8_wiring_and_version():
    src = inspect.getsource(de)
    assert src.count('"settle_unresolved": _f7_unresolved_mode(),') == 1
    assert src.count("settle_unresolved=%s") == 1
    assert src.count("_f7_unresolved_mode(),      # v5.153.0") == 1
    assert src.count("_v573_append_warning(row, _F7_UNRESOLVED_RANK_TAG)") == 1
    assert src.count('_F7_UNRESOLVED_RANK_TAG: str = "rank_skipped_" + _F7_UNRESOLVED_TAG') == 1
    assert src.count("_f7_settle_failed(row) or _f7_settle_unresolved(row)") == 1          # B1
    assert src.count("_f7_unresolved_capital_hold(r)") == 2                                # B1: both portfolio sites
    assert src.count("if status == \"INVESTABLE\" and _f7_settle_unresolved(row):") == 1
    assert src.count("if _f7_settle_unresolved(row):  # v5.153.0 (WP-2b): an unresolved score never seats a board row") == 1
    # prior contracts intact
    assert src.count("scoring_settle=%s") == 1 and src.count("tuple_noise=%s") == 1
    # v5.157.0 (F-7 v2): the call site passes the inferred market scope page
    assert src.count("merged = _f7_settle_pass(merged, sym, page_ctx, scope_page=self._f7_scope_page(sym))") == 1
    assert de._F7_UNRESOLVED_RANK_TAG == "rank_skipped_settle_unresolved"
    assert tuple(int(x) for x in de.__version__.split(".")[:3]) >= (5, 153, 0)
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v" + de.__version__ in src


@_isolated
def test_u9_unresolved_score_never_funds_an_add():
    _env(settle="enforce", unres="enforce")
    row = _settle(f7._failure_row(), _drift, 2)
    assert row["position_size_hint"] == HINT
    clean = f7._failure_row()
    assert de._portfolio_decision(clean, 80.0, 5.0, 60.0) == "ADD"          # control: same values, no tag
    assert de._portfolio_decision(row, 80.0, 5.0, 60.0) == "HOLD"
    row.update(position_value=400.0, target_weight=90.0, currency="USD")
    other = {"symbol": "OTHER.US", "position_value": 3600.0, "currency": "USD"}
    _env(settle="enforce", unres="enforce", TFB_PF_WEIGHT_FX="0")
    de._compute_portfolio_fields([row, other])
    assert row["weight_gap"] == 80.0
    assert row["action_flag"] == row["decision"] == "HOLD"
    row.update(recommendation="SELL", recommendation_detailed="SELL")
    assert de._portfolio_decision(row, 80.0, 5.0, 60.0) == "SELL"           # exits still flow
    # an ADD-family action already on the row is neutralised at publication
    pre = f7._failure_row()
    pre.update(action_flag="ADD", decision="ADD")
    _env(settle="enforce", unres="enforce")
    pre = _settle(pre, _drift, 2)
    assert pre["action_flag"] == pre["decision"] == "HOLD"


@_isolated
def test_u10_stable_settlement_retires_a_stale_unresolved_receipt():
    for settle in ("observe", "enforce"):
        for unres in (None, "enforce"):
            _env(settle=settle, unres=unres, max_passes=4)
            row = f7._prep(f7.FIXTURES[1])
            row["warnings"] = "; ".join(p for p in (
                str(row.get("warnings") or ""),
                "settle_unresolved:enforce:pass_limit:p3:ov=70:rc=buy", "rank_skipped_settle_unresolved",
                "keep_me") if p)
            assert de._f7_settle_unresolved(row)
            out = de._f7_settle_pass(row, row["symbol"], "Global_Markets")
            assert not de._f7_settle_unresolved(out) and not de._f7_settle_unresolved(row)
            assert "rank_skipped_settle_unresolved" not in _parts(out)
            assert "keep_me" in _parts(out)
            assert de._as_float(out.get("overall_score")) is not None
            other = {"symbol": "OTHER.US", "overall_score": -1.0}
            de._apply_rank_overall([out, other])
            assert out["rank_overall"] == 1 and other["rank_overall"] == 2
    # no receipt -> the strip is a no-op on the very same string object
    _env(settle="observe", max_passes=4)
    row = f7._prep(f7.FIXTURES[0])
    w = row.get("warnings")
    assert de._f7_unresolved_strip(row) == 0 and row.get("warnings") is w


TESTS = [test_u1_gate_explicit_words_only, test_u2_off_is_todays_holdback_byte_for_byte,
         test_u3_observe_values_untouched_one_countable_tag,
         test_u4_enforce_publishes_pass1_capped_unranked_and_tag_driven,
         test_u5_error_stays_fail_closed_and_settle_off_makes_gate_inert,
         test_u6_cached_failure_receipt_always_wins, test_u7_tag_composer_exact_safe_and_degrades,
         test_u8_wiring_and_version, test_u9_unresolved_score_never_funds_an_add,
         test_u10_stable_settlement_retires_a_stale_unresolved_receipt]

if __name__ == "__main__":
    import traceback
    fails = 0
    for t in TESTS:
        try:
            t()
            print("PASS", t.__name__)
        except Exception:
            fails += 1
            print("FAIL", t.__name__)
            traceback.print_exc()
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (len(TESTS) - fails, len(TESTS)),
          "engine", de.__version__)
    sys.exit(1 if fails else 0)
