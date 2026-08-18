#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/surface_action_invariants.py — W1A-1 / W1A-2 (v1.0.0, 2026-08-18)
================================================================================
THE TWO INVARIANTS (spec v1.4.x, W1A table):
  W1A-1  BLOCKED ⇒ DO_NOT_INVEST. A row carrying a non-empty block_reason (or
         status BLOCKED) can never surface a buy-family recommendation or an
         INVEST-class action. 14-Aug evidence: 107 Global_Markets rows served
         recommendation=BUY with a populated Block Reason; fixtures HDB.VN,
         SQM-B.SN, VPB.VN, VCB.VN.
  W1A-2  fetch_failed ⇒ BLOCKED. A row whose provider leg failed (any
         "fetch_failed" marker in warnings/error/status) is first made BLOCKED
         (reason injected if absent), then W1A-1 applies. 14-Aug evidence: all
         4 CFX INVEST rows carried `fetch_failed:HTTP 422`; fixtures ZL=F,
         KE=F, ALI=F, HG=F.

PLACEMENT (the relocation): the engine ALREADY demotes this class post-hoc
(the v5.123 gate.buy_has_no_block_reason chain), but the validator fires AFTER
the write — the sheet shows the contradiction for a full cycle. This module is
called at the single API choke point in data_engine_v2.get_sheet_rows(), after
identity-guard / rank / news-veto and IMMEDIATELY BEFORE rows_display and
rows_matrix are projected — so every header namespace inherits the corrected
row and no writer can ever see BUY+blocked.

VOCABULARY (house precedents, cited):
  action namespace  : final_action → "DO_NOT_INVEST"; investability_status →
                      "BLOCKED" when it currently reads an INVEST-class value
                      (the v5.123 "status BLOCKED / final_action DO_NOT_INVEST"
                      chain, engine WHY line ~664).
  reco namespace    : buy-family {STRONG_BUY, BUY, ACCUMULATE} → "HOLD", and
                      recommendation_detailed is set IDENTICALLY (Fix-A
                      contract: Recommendation must equal Detail — engine WHY
                      ~2120). Sell/HOLD-family recommendations are NEVER
                      touched; block_reason is NEVER cleared or shortened
                      (both rules copied from the v5.123 demoter's contract).
  markers           : one warning tag per applied rule —
                      blocked_invariant_applied:v1.0.0 /
                      fetchfail_blocked_applied:v1.0.0 — idempotent (a second
                      pass adds nothing and changes nothing).

GATES (backend/Render ENV — NOT GitHub Actions; per the project's ENV
placement rule these live in the Render dashboard):
  TFB_T10_BLOCKED_INVARIANT   default "0"  arms W1A-1
  TFB_T10_FETCHFAIL_BLOCKED   default "0"  arms W1A-2 (implies the W1A-1
                              demotion on the rows it blocks, even if W1A-1's
                              own flag is still off — a row this rule blocks
                              must never keep BUY)
Both unset/0 ⇒ apply() returns the rows object UNTOUCHED (same list identity,
zero mutation) — byte-behaviour of v5.127.x preserved.

FAIL-SAFE: every row is processed in its own try; any error leaves that row
untouched and counts in `errors`. The call site wraps the whole call again.
Never raises. Zero I/O, zero network — pure functions + os.getenv.

Self-test: `python -m core.surface_action_invariants --selftest` (also run by
the delivery validation). Exit 0 iff all fixtures pass.
"""
from __future__ import annotations

import os
import re
import sys
from typing import Any, Dict, List, Tuple

# v1.0.1 (2026-08-18, external Full Code Audit adjudicated — all four P0s
# reproduced against this file before acceptance):
#   P0-3 fetch_failed with a PRE-EXISTING block_reason skipped the demotion
#        when only W1A-2 was armed (row_blocked_by_2 keyed on injection, not
#        on the fetch failure itself). Fixed: the fetch_failed boolean now
#        drives the demotion gate independently of injection.
#   P0-4 _is_blocked ignored the engine's CANONICAL decision field —
#        investability_status=BLOCKED with a blank reason passed untouched.
#   P1-3 contract precision: HARD blocked (investability_status/status ==
#        BLOCKED) forces final_action=DO_NOT_INVEST from ANY value including
#        WATCH; a reason-only (soft) block demotes INVEST-class actions but
#        PRESERVES WATCH on WATCHLIST rows. Docs now match code exactly.
#   P2-1 TFB_SURFACE_* alias gates added (old TFB_T10_* names still honored —
#        the invariant guards every instrument surface, not only Top_10).
#   P2-2 selftest saves/restores prior ENV in finally.
#   P2-3 adversarial fixtures added (audit A3/A4 classes + soft-WATCH case).
# v1.1.0 (2026-08-18 night, W1A-8 — spec v1.4 table, verbatim scope):
#   "Identity-warning ⇒ not-INVESTABLE (all surfaces)": any of
#   xprovider_verified:*:0.0% · quote_current_price_missing ·
#   quote_exchange_missing · quote_currency_missing · name_unresolved
#   (the engine's v5.93.0 unresolved-identity tag) means the row cannot be
#   INVESTABLE/INVEST on Top_10, opportunity surfaces, OR My_Portfolio —
#   the portfolio surface was the named gap (fixture T82U.SI: My_Portfolio
#   INVESTABLE/INVEST at rel 90.4 while its GM twin sat BLOCKED and its
#   warnings carried xprovider_verified:yahoo_chart:0.0% + three quote_*).
#   SEMANTICS (deliberately SOFTER than W1A-1): identity doubt is not a
#   verdict of badness — investability INVEST-class → WATCHLIST and
#   final_action INVEST-class → WATCH; the recommendation text and
#   reliability are UNTOUCHED, block_reason is NOT written (this is not a
#   BLOCKED state), marker warn_invest_invariant_applied:v1.1.0 appended.
#   Surface effect: Top_10 seat eligibility requires INVESTABLE (v5.80), so
#   the demotion alone removes board eligibility on every path the shared
#   wrapper covers — including the Top_10 direct branch's post-veto
#   re-filter and My_Portfolio via the general seam.
#   GATE: TFB_WARN_INVEST_INVARIANT (alias TFB_SURFACE_WARN_INVEST),
#   DEFAULT OFF, arms independently of W1A-1/2. 0.0% is exact — a partial
#   verification (e.g. :12.5%) must NOT trigger.
__version__ = "1.1.0"
SAI_TAG = f"[SURFACE-INV v{__version__}]"

_BUY_FAMILY = {"STRONG_BUY", "BUY", "ACCUMULATE"}
_INVEST_CLASS = {"INVEST", "INVESTABLE", "BUY", "STRONG_BUY", "ACCUMULATE"}
_BLOCKED_MARK = f"blocked_invariant_applied:v{__version__}"
_FETCH_MARK = f"fetchfail_blocked_applied:v{__version__}"
_FETCH_TOKEN = "fetch_failed"
# --- W1A-8 identity-warning markers (spec-literal set) -----------------------
_WARN_MARK = f"warn_invest_invariant_applied:v{__version__}"
_IDW_SUBSTRINGS = ("quote_current_price_missing", "quote_exchange_missing",
                   "quote_currency_missing", "name_unresolved")
_IDW_XPROV_RE = re.compile(r"xprovider_verified:[^;\s]*:0\.0+%")
_INJECTED_REASON = "Provider fetch failed (W1A-2): row data is not decision-grade"


def _flag(name: str) -> bool:
    return (os.getenv(name) or "0").strip().lower() in ("1", "true", "yes", "on")


def blocked_invariant_enabled() -> bool:
    # v1.0.1 P2-1: SURFACE alias — the control covers every instrument page.
    return _flag("TFB_T10_BLOCKED_INVARIANT") or _flag(
        "TFB_SURFACE_BLOCKED_INVARIANT")


def fetchfail_blocked_enabled() -> bool:
    return _flag("TFB_T10_FETCHFAIL_BLOCKED") or _flag(
        "TFB_SURFACE_FETCHFAIL_BLOCKED")


def warn_invest_invariant_enabled() -> bool:
    """v1.1.0 W1A-8 gate (independent arming)."""
    return _flag("TFB_WARN_INVEST_INVARIANT") or _flag(
        "TFB_SURFACE_WARN_INVEST")


def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _warn_text(row: Dict[str, Any]) -> str:
    w = row.get("warnings")
    if isinstance(w, (list, tuple)):
        return "; ".join(_s(x) for x in w)
    return _s(w)


def _add_warning(row: Dict[str, Any], tag: str) -> None:
    w = row.get("warnings")
    if isinstance(w, list):
        if tag not in w:
            w.append(tag)
        return
    txt = _s(w)
    if tag in txt:
        return
    row["warnings"] = (txt + ("; " if txt else "") + tag)


def _is_fetch_failed(row: Dict[str, Any]) -> bool:
    blob = " ".join((
        _warn_text(row), _s(row.get("error")), _s(row.get("status")),
        _s(row.get("data_status")),
    )).lower()
    return _FETCH_TOKEN in blob


def _has_identity_warning(row: Dict[str, Any]) -> bool:
    """v1.1.0 W1A-8: spec marker set. 0.0% exact-by-regex; substrings for
    the three quote_* tags and the engine's name_unresolved tag."""
    blob = _warn_text(row)
    if any(t in blob for t in _IDW_SUBSTRINGS):
        return True
    return bool(_IDW_XPROV_RE.search(blob))


def _demote_identity_warning(row: Dict[str, Any]) -> bool:
    """W1A-8 soft demotion: INVEST-class → WATCHLIST/WATCH. Reco text,
    reliability, block_reason all untouched. Idempotent via marker."""
    changed = False
    inv = _s(row.get("investability_status")).upper()
    if inv in _INVEST_CLASS:
        row["investability_status"] = "WATCHLIST"
        changed = True
    fa = _s(row.get("final_action")).upper()
    if fa in _INVEST_CLASS:
        row["final_action"] = "WATCH"
        changed = True
    if changed:
        _add_warning(row, _WARN_MARK)
    return changed


def _is_hard_blocked(row: Dict[str, Any]) -> bool:
    """v1.0.1 P1-3: HARD block = the canonical decision verdict itself says
    BLOCKED (investability_status, or the generic status field)."""
    if _s(row.get("investability_status")).upper() == "BLOCKED":
        return True
    return _s(row.get("status")).upper() == "BLOCKED"


def _is_blocked(row: Dict[str, Any]) -> bool:
    if _s(row.get("block_reason")) or _s(row.get("Block Reason")):
        return True
    # v1.0.1 P0-4: recognize the engine's canonical field even when the
    # reason text is blank.
    return _is_hard_blocked(row)


def _demote_blocked(row: Dict[str, Any], hard: bool) -> bool:
    """W1A-1 demotion on ONE row. Returns True iff anything changed.

    v1.0.1 P1-3 contract (now exact): buy-family recommendations always
    demote to HOLD. HARD blocked forces final_action=DO_NOT_INVEST from ANY
    value (WATCH included); a soft/reason-only block demotes INVEST-class
    actions but preserves WATCH on WATCHLIST rows."""
    changed = False
    reco = _s(row.get("recommendation")).upper()
    if reco in _BUY_FAMILY:
        row["recommendation"] = "HOLD"
        if "recommendation_detailed" in row:
            row["recommendation_detailed"] = "HOLD"      # Fix-A: stay equal
        changed = True
    fa = _s(row.get("final_action")).upper()
    if fa and fa != "DO_NOT_INVEST" and (
            fa in _INVEST_CLASS or (hard and fa in {"WATCH", "WATCHLIST"})):
        row["final_action"] = "DO_NOT_INVEST"
        changed = True
    inv = _s(row.get("investability_status")).upper()
    if inv in _INVEST_CLASS:
        row["investability_status"] = "BLOCKED"
        changed = True
    if changed:
        _add_warning(row, _BLOCKED_MARK)
    return changed


def apply_surface_action_invariants(
    rows: List[Dict[str, Any]], sheet: str = ""
) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Apply W1A-2 then W1A-1 across `rows` in place.

    Returns (rows, n_blocked_demotions, n_fetchfail_blocks, n_row_errors).
    With both gates off this is a strict no-op returning the SAME object.
    """
    g1, g2 = blocked_invariant_enabled(), fetchfail_blocked_enabled()
    g3 = warn_invest_invariant_enabled()
    if not (g1 or g2 or g3) or not isinstance(rows, list):
        return rows, 0, 0, 0
    n1 = n2 = errs = 0
    for row in rows:
        try:
            if not isinstance(row, dict):
                continue
            # v1.0.1 P0-3: the fetch-failure FACT (not the injection event)
            # drives the demotion gate — an already-blocked fetch_failed row
            # must demote under W1A-2 alone.
            ff = bool(g2 and _is_fetch_failed(row))
            if ff and not _is_blocked(row):
                row["block_reason"] = _INJECTED_REASON
                _add_warning(row, _FETCH_MARK)
                n2 += 1
            if _is_blocked(row) and (g1 or ff):
                if _demote_blocked(row, hard=_is_hard_blocked(row)):
                    n1 += 1
            # v1.1.0 W1A-8 — after the blocked chain so a row that is BOTH
            # blocked and identity-doubted takes the STRONGER outcome; a
            # warned-only row takes the soft WATCHLIST/WATCH demotion.
            if g3 and _has_identity_warning(row) and not _is_blocked(row):
                if _demote_identity_warning(row):
                    n1 += 1
        except Exception:
            errs += 1
    return rows, n1, n2, errs


# ----------------------------------------------------------------------------
# Self-test — spec fixtures. No prod imports; runs anywhere.
# ----------------------------------------------------------------------------
def _selftest() -> int:
    import copy
    _KEYS = ("TFB_T10_BLOCKED_INVARIANT", "TFB_T10_FETCHFAIL_BLOCKED",
             "TFB_SURFACE_BLOCKED_INVARIANT", "TFB_SURFACE_FETCHFAIL_BLOCKED",
             "TFB_WARN_INVEST_INVARIANT", "TFB_SURFACE_WARN_INVEST")
    _prior = {k: os.environ.get(k) for k in _KEYS}   # v1.0.1 P2-2
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
    os.environ["TFB_T10_FETCHFAIL_BLOCKED"] = "1"
    for _k in _KEYS[2:]:
        os.environ.pop(_k, None)
    fails = []

    def ck(name, cond):
        print(("  PASS  " if cond else "  FAIL  ") + name)
        if not cond:
            fails.append(name)

    # (a) HDB.VN class — BUY + block_reason populated
    r = {"symbol": "HDB.VN", "recommendation": "BUY",
         "recommendation_detailed": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE",
         "block_reason": "identity_suspect", "warnings": []}
    _, n1, n2, e = apply_surface_action_invariants([r], "Global_Markets")
    ck("a1 BUY+blocked -> HOLD", r["recommendation"] == "HOLD")
    ck("a2 detail stays equal (Fix-A)",
       r["recommendation_detailed"] == "HOLD")
    ck("a3 final_action -> DO_NOT_INVEST",
       r["final_action"] == "DO_NOT_INVEST")
    ck("a4 investability -> BLOCKED",
       r["investability_status"] == "BLOCKED")
    ck("a5 reason NEVER cleared", r["block_reason"] == "identity_suspect")
    ck("a6 marker added once", r["warnings"] == [_BLOCKED_MARK])
    ck("a7 counts (1,0,0)", (n1, n2, e) == (1, 0, 0))
    snap = copy.deepcopy(r)
    apply_surface_action_invariants([r], "Global_Markets")
    ck("a8 idempotent second pass", r == snap)

    # (b) ZL=F class — INVEST on fetch_failed warning (string warnings)
    r = {"symbol": "ZL=F", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE",
         "warnings": "fetch_failed:HTTP 422; quote_currency_missing",
         "block_reason": ""}
    _, n1, n2, e = apply_surface_action_invariants([r], "Commodities_FX")
    ck("b1 fetch_failed -> block_reason injected",
       r["block_reason"] == _INJECTED_REASON)
    ck("b2 then demoted DO_NOT_INVEST",
       r["final_action"] == "DO_NOT_INVEST" and r["recommendation"] == "HOLD")
    ck("b3 both markers present",
       _FETCH_MARK in r["warnings"] and _BLOCKED_MARK in r["warnings"])
    ck("b4 counts (1,1,0)", (n1, n2, e) == (1, 1, 0))

    # (c) clean BUY row — untouched
    r = {"symbol": "OK1", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "block_reason": "",
         "warnings": []}
    snap = copy.deepcopy(r)
    apply_surface_action_invariants([r], "Market_Leaders")
    ck("c1 clean row byte-untouched", r == snap)

    # (d) SELL + blocked — reco never touched, action already safe stays
    r = {"symbol": "S1", "recommendation": "SELL",
         "final_action": "DO_NOT_INVEST", "investability_status": "WATCHLIST",
         "block_reason": "x", "warnings": []}
    snap = copy.deepcopy(r)
    apply_surface_action_invariants([r], "Global_Markets")
    ck("d1 sell-family untouched", r == snap)

    # (e) gates OFF — strict no-op, same object
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "0"
    os.environ["TFB_T10_FETCHFAIL_BLOCKED"] = "0"
    rows = [{"symbol": "HDB.VN", "recommendation": "BUY",
             "block_reason": "z", "final_action": "INVEST"}]
    snap = copy.deepcopy(rows)
    out, n1, n2, e = apply_surface_action_invariants(rows, "GM")
    ck("e1 OFF -> zero mutation", rows == snap and out is rows
       and (n1, n2, e) == (0, 0, 0))

    # (f) W1A-2 armed alone still finishes the demotion on ITS rows
    os.environ["TFB_T10_FETCHFAIL_BLOCKED"] = "1"
    r = {"symbol": "KE=F", "recommendation": "BUY", "final_action": "INVEST",
         "warnings": ["fetch_failed:HTTP 422"], "block_reason": ""}
    apply_surface_action_invariants([r], "CFX")
    ck("f1 fetchfail-only gate never leaves BUY on its blocks",
       r["recommendation"] == "HOLD"
       and r["final_action"] == "DO_NOT_INVEST")

    # (g) malformed row -> counted, siblings unaffected
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
    good = {"symbol": "G", "recommendation": "BUY", "block_reason": "y",
            "final_action": "INVEST", "warnings": []}
    _, n1, n2, e = apply_surface_action_invariants(
        [None, 42, good], "GM")  # type: ignore[list-item]
    ck("g1 non-dict rows skipped, good row still demoted",
       good["final_action"] == "DO_NOT_INVEST" and e == 0)

    # (h) audit A3 — W1A-2 ALONE + fetch_failed + PRE-EXISTING reason
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "0"
    os.environ["TFB_T10_FETCHFAIL_BLOCKED"] = "1"
    r = {"symbol": "HG=F", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE",
         "block_reason": "Stale price bar",
         "warnings": ["fetch_failed:HTTP 422"]}
    _, n1, n2, e = apply_surface_action_invariants([r], "CFX")
    ck("h1 P0-3: pre-blocked fetch_failed demotes under W1A-2 alone",
       r["recommendation"] == "HOLD"
       and r["final_action"] == "DO_NOT_INVEST" and (n1, n2) == (1, 0))
    ck("h2 P0-3: existing reason NEVER overwritten",
       r["block_reason"] == "Stale price bar")

    # (i) audit A4 — canonical investability_status=BLOCKED, blank reason
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
    r = {"symbol": "CANON", "recommendation": "BUY",
         "final_action": "WATCH", "investability_status": "BLOCKED",
         "block_reason": "", "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("i1 P0-4: canonical BLOCKED recognized -> BUY demoted",
       r["recommendation"] == "HOLD")
    ck("i2 P1-3: HARD blocked forces WATCH -> DO_NOT_INVEST",
       r["final_action"] == "DO_NOT_INVEST")

    # (j) soft reason-only block on a WATCHLIST row: WATCH preserved
    r = {"symbol": "SOFT", "recommendation": "BUY", "final_action": "WATCH",
         "investability_status": "WATCHLIST",
         "block_reason": "Negative news (test)", "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("j1 soft block: BUY->HOLD but WATCH preserved (P1-3)",
       r["recommendation"] == "HOLD" and r["final_action"] == "WATCH"
       and r["investability_status"] == "WATCHLIST")

    # (k) SURFACE alias names arm the gates (P2-1)
    for _k in _KEYS:
        os.environ.pop(_k, None)
    os.environ["TFB_SURFACE_BLOCKED_INVARIANT"] = "1"
    r = {"symbol": "AL1", "recommendation": "BUY", "block_reason": "x",
         "final_action": "INVEST", "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("k1 SURFACE alias arms W1A-1", r["final_action"] == "DO_NOT_INVEST")

    # (l) W1A-8 — the live T82U.SI signature (spec fixture, verbatim tags)
    for _k in _KEYS:
        os.environ.pop(_k, None)
    os.environ["TFB_WARN_INVEST_INVARIANT"] = "1"
    r = {"symbol": "T82U.SI", "recommendation": "BUY",
         "recommendation_detailed": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE",
         "forecast_reliability_score": 90.4, "block_reason": "",
         "warnings": "quote_current_price_missing; quote_exchange_missing; "
                     "quote_currency_missing; "
                     "xprovider_verified:yahoo_chart:0.0%; "
                     "yahoo_enrichment_applied"}
    _, n1, n2, e = apply_surface_action_invariants([r], "My_Portfolio")
    ck("l1 W1A-8 T82U: INVESTABLE/INVEST -> WATCHLIST/WATCH",
       r["investability_status"] == "WATCHLIST"
       and r["final_action"] == "WATCH")
    ck("l2 W1A-8 soft: BUY text + reliability UNTOUCHED",
       r["recommendation"] == "BUY"
       and r["forecast_reliability_score"] == 90.4)
    ck("l3 W1A-8: block_reason NOT written", r["block_reason"] == "")
    ck("l4 W1A-8 marker appended once",
       r["warnings"].count(_WARN_MARK) == 1 and (n1, n2) == (1, 0))
    import copy as _cp
    snap = _cp.deepcopy(r)
    apply_surface_action_invariants([r], "My_Portfolio")
    ck("l5 W1A-8 idempotent", r == snap)
    r = {"symbol": "OKX", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "block_reason": "",
         "warnings": "xprovider_verified:yahoo_chart:87.5%"}
    snap = _cp.deepcopy(r)
    apply_surface_action_invariants([r], "GM")
    ck("l6 partial verification (87.5%) does NOT trigger", r == snap)
    os.environ["TFB_WARN_INVEST_INVARIANT"] = "0"
    os.environ["TFB_SURFACE_WARN_INVEST"] = "1"
    r = {"symbol": "AL2", "final_action": "INVEST",
         "investability_status": "INVESTABLE",
         "warnings": ["name_unresolved"], "block_reason": ""}
    apply_surface_action_invariants([r], "GM")
    ck("l7 SURFACE alias arms W1A-8", r["final_action"] == "WATCH")
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
    r = {"symbol": "BOTH", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "block_reason": "bad",
         "warnings": ["name_unresolved"]}
    apply_surface_action_invariants([r], "GM")
    ck("l8 blocked+warned -> STRONGER outcome wins (DO_NOT_INVEST/BLOCKED)",
       r["final_action"] == "DO_NOT_INVEST"
       and r["investability_status"] == "BLOCKED"
       and _WARN_MARK not in " ".join(map(str, r["warnings"])))

    for _k, _v in _prior.items():                      # v1.0.1 P2-2 restore
        if _v is None:
            os.environ.pop(_k, None)
        else:
            os.environ[_k] = _v

    print(f"{SAI_TAG} selftest: "
          f"{'PASS' if not fails else 'FAIL'} ({len(fails)} failure(s))")
    return 0 if not fails else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    print(f"{SAI_TAG} module OK — use --selftest")
