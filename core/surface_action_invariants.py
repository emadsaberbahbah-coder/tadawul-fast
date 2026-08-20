#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/surface_action_invariants.py — W1A-1/2/7/8 (v1.4.1, 2026-08-20)
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
#   DEFAULT OFF, arms independently of W1A-1/2.
#   v1.4.1 CORRECTION: xprovider_verified:<prov>:<pct>% is NOT part of the
#   trigger set. The engine producer (data_engine_v2 ~10456) computes
#   pct = |px-alt|/min * 100 — a price-agreement DELTA — and writes the
#   tag only on agreement within threshold ("positive evidence, no gate
#   effect", engine WHY ~505). 0.0% is EXACT agreement, the strongest
#   positive verification. Disagreement writes a different tag,
#   xprovider_price_conflict, which the investability gate already
#   handles upstream. W1A-8 triggers ONLY on the four quote/name markers.
# v1.2.0 (2026-08-18 night, W1A-7 — spec v1.4 table, graded not blanket):
#   HARD QUARANTINE ⇒ BLOCKED (block_reason "row_sanity:<class> (W1A-7)"
#   written when blank, appended to warnings otherwise; investability forced
#   BLOCKED so the SAME-PASS W1A-1 chain demotes reco/action) for exactly
#   the spec classes: symbol contains whitespace; price ≤0/blank on an
#   ACTIONABLE row (INVEST-class action/status or buy-family reco — a HOLD
#   row with a blank price is NOT quarantined); Day High < Day Low (equal is
#   legal); field-type violations — an ISO currency code sitting in
#   Exchange, a non-ISO value sitting in Currency, a GICS sector sitting in
#   Country. Row width alone is never validity.
#   SOFT PATH — RANGE_ASYNC, NOT QUARANTINE: Current Price beyond the 52W
#   band by more than the 0.1% rounding tolerance gets an informational tag
#   range_async:52w_(high|low)_breach:v1.2.0 and NOTHING ELSE — a genuine
#   new 52-week extreme is exactly what a momentum system must surface (the
#   spec's own rationale). The same-snapshot re-fetch + persistence check
#   is provider I/O and is explicitly DEFERRED to the engine-side follow-up
#   (register item), so this module can stay pure. Fixture: a clean
#   new-high INVEST row must survive byte-untouched except the tag.
#   ENV-COMBINATION MATRIX (spec §"unsafe ENV combinations", due with the
#   first W1A delivery — that was tonight): TFB_ROW_SANITY_QUARANTINE=1 is
#   INVALID unless the W1A-1 blocked-invariant gate is also armed — a
#   quarantine that marks BLOCKED is worthless if BLOCKED can still surface
#   INVEST. env_combo_violations() names it; apply() SELF-DISABLES the
#   quarantine on violation with ONE loud named error per process, and the
#   engine exposes the violation in [GUARDS+] and health().
#   GATES: TFB_ROW_SANITY_QUARANTINE (alias TFB_SURFACE_ROW_SANITY),
#   DEFAULT OFF. apply() now returns a 5-tuple (…, n_row_sanity, errors).
# v1.3.0 (2026-08-19 early, external XLSX audit adjudicated — every P0/P1
# reproduced against source before acceptance; 11 adversarial FAILs closed):
#   F-01 P0: HARD BLOCKED with a BLANK final_action stayed blank — "from ANY
#        value" now literally includes blank/None: hard forces DO_NOT_INVEST
#        whenever final_action != DO_NOT_INVEST.
#   F-02 P0: fetch_failed rows exited with only a reason — W1A-2 says
#        "fetch_failed ⇒ BLOCKED", so ff now EXPLICITLY sets
#        investability_status=BLOCKED (pre-existing reason included) and the
#        demotion runs hard.
#   F-05 P1: the RANGE_ASYNC read used week52_* while the engine's canonical
#        keys are week_52_* (16 hits vs 0) — the tag was structurally dead on
#        real rows. Canonical first, legacy fallback.
#   F-06 P1: currency validation was shape-only — 'XYZ' passed. Membership
#        now checked against the full ISO-4217 active set + approved quote
#        subunits (GBX/ZAC/ILA); class "unsupported_currency_code".
#   F-07 P1: whitespace detection stripped first — ' AAPL ' bypassed. Raw
#        string judged now (leading/trailing/internal all quarantine).
#   F-08 P1: detail-only BUY survived on a blocked row — EITHER
#        recommendation field buy-family ⇒ BOTH become HOLD.
#   F-09/F-11: telemetry split — apply() now returns a SIX-tuple
#        (rows, n_blocked, n_fetchfail, n_warn_invest, n_row_sanity,
#        n_errors); annotation matches runtime.
#   F-10 P2: marker matching case-normalized (blob lowered once).
#   F-13 P2: price coalesce is value-usable, not key-present — blank
#        current_price + valid price alias is priced, never quarantined.
#   F-14 REVIEW (governance answer, behavior unchanged): the engine's
#        xprovider_verified:<provider>:<pct> tag is CROSS-PROVIDER IDENTITY
#        VERIFICATION COVERAGE — 0.0% means NO second source confirmed the
#        instrument's identity. The live evidence is the fixture itself:
#        T82U.SI's GM twin carrying that tag sat BLOCKED. Demotion on 0.0%
#        is therefore the conservative and intended reading.
# v1.4.0 (2026-08-20 morning, W1A-7b — ONE new HARD row-sanity class):
#   WHY: the 2026-08-20 board produced exactly ONE executable ticket,
#   1015.KL, and that row carried Open 4.64 against a Day Low of 7.05 —
#   34.2% outside the session range — while scoring Data Quality 100.00,
#   Forecast Reliability 88.10, and passing the Top_10 funnel with
#   First Fail = NONE. Neither the surface invariants nor the Top_10 gates
#   test Open against the range at all; day_high_lt_day_low was the only
#   geometry class and it caught 1 of 669 detectable violations on that
#   export. The ticket was therefore not a selection error — it was an
#   unguarded geometry class reaching the executable surface.
#   CLASS: open_outside_day_range — HARD, so the same-pass W1A-1 chain
#   finishes the demotion to DO_NOT_INVEST/BLOCKED, identical in shape to
#   day_high_lt_day_low. Keys coalesced open / open_price / day_open
#   (all three live in the engine). Guarded on hi >= lo so a High<Low row
#   is charged to that class only and never double-counted.
#   TOLERANCE: the module's own _RANGE_TOL (0.1%), reused deliberately —
#   sub-tolerance rounding is normalized exactly as the 52W tags do it and
#   is NEVER quarantined. Measured on the live export: 655 rows violate at
#   0% tolerance, 627 at 0.1%, 623 at 0.5% — only 32 rows sit in the
#   rounding tail, so 0.1% is the whole decision and a looser tolerance
#   buys nothing. All four live US holdings measure dev 0.000% and are
#   untouched; the IR-101 sub-tolerance class does not reproduce here.
#   BLAST RADIUS (measured, not projected): 627 rows quarantined pool-wide
#   — 448 GM / 107 CFX / 72 MF / 0 ML. Of those, 501 already read
#   DO_NOT_INVEST and 114 already read WATCH, so the guard only makes them
#   honest. Exactly 12 currently read INVEST: board INVEST 83 -> 71 with
#   this class alone. Market_Leaders returning 0 is the negative control —
#   its Open column is 100% unpopulated (0/255), so the class must be
#   structurally silent there, and it is.
#   NOT CHANGED: the SOFT 52W range_async path, every W1A-1/2/8 predicate,
#   and every existing class. _RS_MARK moves to :v1.4.0 because the
#   row-sanity VOCABULARY changed — the marker names the vocabulary, not
#   the module.
#   GATE: unchanged — TFB_ROW_SANITY_QUARANTINE (alias
#   TFB_SURFACE_ROW_SANITY), DEFAULT OFF, and still refused by the ENV
#   combination matrix unless the W1A-1 blocked-invariant gate is armed.
#   NO NEW ENV IS INTRODUCED BY THIS VERSION.
#   DEPLOY CONTRACT: data_engine_v2.py pins _SAI_REQUIRED_VERSION by strict
#   equality and returns None on any skew ("skew = broken deploy. Refuse,
#   never emulate."). This module CANNOT ship alone — data_engine_v2 must
#   move to _SAI_REQUIRED_VERSION = "1.4.0" in the same deploy.
# v1.4.1 (2026-08-20 midday, external audit F-01/F-04/F-05 — adjudicated
# against the engine producer before acceptance):
#   F-01 P0 ACCEPTED — W1A-8 SEMANTIC REVERSAL. The v1.3.0 F-14 governance
#   answer below is hereby RETRACTED (preserved verbatim per house rule,
#   exactly as engine v5.108.1 retracted its 4503.T conviction). Producer
#   evidence: data_engine_v2 line ~10456 computes
#   delta = abs(px-alt)/min*100 and returns
#   "xprovider_verified:%s:%.1f%%" only on AGREEMENT within threshold;
#   engine WHY ~505 calls it "positive evidence, no gate effect"; engine
#   WHY ~462 (v5.108.1) had ALREADY exonerated the exact-0.0% case as
#   "two truthful feeds agreeing, not circularity". Empirical (2026-08-20
#   GM export): 1,501 tags, all yahoo_chart, deltas a continuum
#   0.0–1.4%; the eighths values the l6 fixture imagined (12.5/87.5)
#   occur ZERO times. 1,300 rows carry :0.0%; 858 have no other identity
#   marker; 13 are INVEST; 10 carry no fetch_failed — ten healthy,
#   OHLC-coherent rows (LUND-B.ST, 0293.HK, ELF.TO, PINFRA.MX, 1398.HK,
#   RBI.VI, UCB.BR, SAFE.L, PHP.L, SGRO.L) that v1.3.0/v1.4.0 W1A-8
#   would have FALSELY demoted. The regex is removed; the trigger set is
#   the four quote/name markers ONLY. T82U.SI and 1015.KL still demote —
#   both carry quote_current_price_missing — so every row W1A-8 was
#   built to catch remains caught. The degenerate-0.0% class (350 rows
#   where the tag co-occurs with quote_current_price_missing because the
#   enrichment donor equals the reference source) is a PRODUCER item
#   (suppress verify when alternate == price donor), registered
#   separately; the consumer must not punish the healthy 858 for it.
#   F-04 P2 ACCEPTED — _row_is_actionable now treats a detail-only BUY
#   as actionable (defense-in-depth; the engine's Fix-A reconciliation
#   normally prevents the split upstream). Fixture q4.
#   F-05 P2 ACCEPTED — apply() annotation corrected to the six-tuple the
#   runtime has returned since v1.2.0/F-09.
#   NOT CHANGED: every hard row-sanity class incl. v1.4.0's
#   open_outside_day_range; _RS_MARK stays :v1.4.0 (row-sanity
#   vocabulary unchanged); all gates, aliases, combo matrix.
__version__ = "1.4.1"
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
# v1.4.1 (F-01): the former _IDW_XPROV_RE (matching :0.0%) is REMOVED —
# see the header correction and the changelog entry below.
# v1.3.0 (F-06): full ISO-4217 active alphabetic set + approved quote
# subunits. Membership, not shape, is the validity test.
_ISO4217 = {
 "AED","AFN","ALL","AMD","ANG","AOA","ARS","AUD","AWG","AZN","BAM","BBD",
 "BDT","BGN","BHD","BIF","BMD","BND","BOB","BRL","BSD","BTN","BWP","BYN",
 "BZD","CAD","CDF","CHF","CLP","CNY","COP","CRC","CUP","CVE","CZK","DJF",
 "DKK","DOP","DZD","EGP","ERN","ETB","EUR","FJD","FKP","GBP","GEL","GHS",
 "GIP","GMD","GNF","GTQ","GYD","HKD","HNL","HRK","HTG","HUF","IDR","ILS",
 "INR","IQD","IRR","ISK","JMD","JOD","JPY","KES","KGS","KHR","KMF","KPW",
 "KRW","KWD","KYD","KZT","LAK","LBP","LKR","LRD","LSL","LYD","MAD","MDL",
 "MGA","MKD","MMK","MNT","MOP","MRU","MUR","MVR","MWK","MXN","MYR","MZN",
 "NAD","NGN","NIO","NOK","NPR","NZD","OMR","PAB","PEN","PGK","PHP","PKR",
 "PLN","PYG","QAR","RON","RSD","RUB","RWF","SAR","SBD","SCR","SDG","SEK",
 "SGD","SHP","SLE","SOS","SRD","SSP","STN","SVC","SYP","SZL","THB","TJS",
 "TMT","TND","TOP","TRY","TTD","TWD","TZS","UAH","UGX","USD","UYU","UZS",
 "VED","VES","VND","VUV","WST","XAF","XCD","XOF","XPF","YER","ZAR","ZMW",
 "ZWL"}
_CCY_SUBUNITS = {"GBX", "ZAC", "ILA"}          # engine v5.77.22 quote units
_CCY_ALLOWED = _ISO4217 | _CCY_SUBUNITS
# --- W1A-7 row-sanity vocab (v1.4.0: +open_outside_day_range) ---------------
_RS_PREFIX = "row_sanity:"
_RS_MARK = ":v" + "1.4.0"   # v1.4.0: row-sanity vocabulary changed
_RANGE_TAG_HI = "range_async:52w_high_breach:v1.2.0"
_RANGE_TAG_LO = "range_async:52w_low_breach:v1.2.0"
_RANGE_TOL = 0.001  # 0.1% rounding tolerance (spec: normalize sub-tolerance)
_ISO_CCY = {"USD", "SAR", "EUR", "GBP", "JPY", "CNY", "HKD", "SGD", "AUD",
            "CAD", "CHF", "SEK", "NOK", "DKK", "INR", "KRW", "TWD", "MYR",
            "THB", "IDR", "PHP", "AED", "QAR", "KWD", "BHD", "OMR", "EGP",
            "TRY", "BRL", "MXN", "CLP", "ZAR", "PLN", "ILS", "NZD", "GBX",
            "GBP.", "RUB", "VND"}
_GICS_SECTORS = {"ENERGY", "MATERIALS", "INDUSTRIALS",
                 "CONSUMER DISCRETIONARY", "CONSUMER STAPLES",
                 "HEALTH CARE", "HEALTHCARE", "FINANCIALS",
                 "INFORMATION TECHNOLOGY", "COMMUNICATION SERVICES",
                 "UTILITIES", "REAL ESTATE"}
_env_combo_logged = {"done": False}
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


def row_sanity_enabled() -> bool:
    """v1.2.0 W1A-7 gate (raw flag; combo-checked at apply time)."""
    return _flag("TFB_ROW_SANITY_QUARANTINE") or _flag(
        "TFB_SURFACE_ROW_SANITY")


def env_combo_violations() -> list:
    """v1.2.0 — the spec's unsafe-combination matrix, named. Currently one
    rule; grows with the wave. Consumed by apply() (self-disable) and by
    the engine's [GUARDS+]/health()."""
    v = []
    if row_sanity_enabled() and not blocked_invariant_enabled():
        v.append("ROW_SANITY_REQUIRES_BLOCKED_INVARIANT: "
                 "TFB_ROW_SANITY_QUARANTINE=1 is invalid while the W1A-1 "
                 "blocked-invariant gate is OFF — quarantine marks BLOCKED, "
                 "and BLOCKED could still surface INVEST.")
    return v


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
    """v1.1.0 W1A-8 marker set; v1.3.0 (F-10) case-normalized — mixed-case
    upstream text triggers identically."""
    blob = _warn_text(row).lower()
    return any(t in blob for t in _IDW_SUBSTRINGS)


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


def _num(v: Any) -> Any:
    try:
        s = _s(v).replace(",", "")
        return float(s) if s else None
    except Exception:
        return None


def _first_num(row: Dict[str, Any], *keys: str) -> Any:
    """v1.3.0 (F-13/F-05): first USABLE numeric among keys — a present-but-
    blank cell must fall through to the next alias (dict.get default only
    covers absent keys, the exact adversarial miss)."""
    for k in keys:
        v = _num(row.get(k))
        if v is not None:
            return v
    return None


def _row_is_actionable(row: Dict[str, Any]) -> bool:
    if _s(row.get("investability_status")).upper() in _INVEST_CLASS:
        return True
    if _s(row.get("final_action")).upper() in _INVEST_CLASS:
        return True
    if _s(row.get("recommendation")).upper() in _BUY_FAMILY:
        return True
    # v1.4.1 (F-04): a detail-only BUY is exactly as actionable — the
    # Fix-A contract says the fields must agree, so disagreement is
    # itself suspect and must not weaken the blank-price quarantine.
    return _s(row.get("recommendation_detailed")).upper() in _BUY_FAMILY


def _row_sanity_classes(row: Dict[str, Any]) -> list:
    """v1.2.0 W1A-7 HARD classes only — pure detection, no mutation."""
    out = []
    sym = str(row.get("symbol") or row.get("Symbol") or "")
    # v1.3.0 (F-07): judge the RAW string — leading/trailing whitespace is
    # exactly as identity-corrupting as internal.
    if sym.strip() and (sym != sym.strip() or re.search(r"\s", sym)):
        out.append("symbol_whitespace")
    px = _first_num(row, "current_price", "price")   # F-13 coalesce
    if _row_is_actionable(row) and (px is None or px <= 0):
        out.append("nonpositive_or_blank_price_actionable")
    hi, lo = _num(row.get("day_high")), _num(row.get("day_low"))
    if hi is not None and lo is not None and hi < lo:
        out.append("day_high_lt_day_low")
    # v1.4.0 (W1A-7b): the session Open must lie inside [day_low, day_high].
    # 2026-08-20 evidence, measured on the live export: 627 rows pool-wide
    # carried an Open outside the range at _RANGE_TOL — 448 Global_Markets,
    # 107 Commodities_FX, 72 Mutual_Funds, 0 Market_Leaders (Open column
    # unpopulated there: negative control). 12 of the 627 surfaced INVEST,
    # including 1015.KL (Open 4.64 against Day Low 7.05 — 34.2% outside)
    # which took the day's ONLY executable ticket with First Fail = NONE.
    # The pre-existing day_high_lt_day_low class caught 1 of 669 detectable
    # violations; this class is the other 626.
    # Tolerance is the module's own _RANGE_TOL, so sub-tolerance rounding is
    # normalized exactly as the 52W tags do it — never quarantined. All four
    # live US holdings (OTIS/YUM/SBAC/PFS) measure dev 0.000% on this export
    # and are untouched; the IR-101 sub-tolerance class does not reproduce.
    # hi >= lo is required so a High<Low row is charged to that class ONLY
    # and never double-counted here.
    op = _first_num(row, "open", "open_price", "day_open")
    if (op is not None and op > 0 and hi is not None and lo is not None
            and hi >= lo and hi > 0 and lo > 0):
        if op > hi * (1.0 + _RANGE_TOL) or op < lo * (1.0 - _RANGE_TOL):
            out.append("open_outside_day_range")
    exch = _s(row.get("exchange")).upper()
    if exch in _CCY_ALLOWED:
        out.append("currency_in_exchange")
    ccy = _s(row.get("currency")).upper()
    if ccy:
        if not re.fullmatch(r"[A-Z]{3}", ccy) and ccy not in _CCY_SUBUNITS:
            out.append("invalid_currency_value")
        elif ccy not in _CCY_ALLOWED:
            # v1.3.0 (F-06): syntactically valid garbage ('XYZ') quarantines.
            out.append("unsupported_currency_code")
    if _s(row.get("country")).upper() in _GICS_SECTORS:
        out.append("sector_in_country")
    return out


def _apply_row_sanity(row: Dict[str, Any]) -> tuple:
    """Returns (hard_quarantined: bool, range_tagged: bool)."""
    tagged = False
    px = _first_num(row, "current_price", "price")
    # v1.3.0 (F-05): canonical engine keys FIRST (week_52_*), legacy fallback.
    w52h = _first_num(row, "week_52_high", "week52_high")
    w52l = _first_num(row, "week_52_low", "week52_low")
    if px is not None and px > 0:
        if (w52h is not None and w52h > 0
                and px > w52h * (1.0 + _RANGE_TOL)):
            _add_warning(row, _RANGE_TAG_HI)
            tagged = True
        elif (w52l is not None and w52l > 0
              and px < w52l * (1.0 - _RANGE_TOL)):
            _add_warning(row, _RANGE_TAG_LO)
            tagged = True
    classes = _row_sanity_classes(row)
    if not classes:
        return False, tagged
    reason = _RS_PREFIX + "+".join(classes) + " (W1A-7)"
    if not (_s(row.get("block_reason")) or _s(row.get("Block Reason"))):
        row["block_reason"] = reason
    _add_warning(row, "row_sanity_quarantined:" + "+".join(classes)
                 + _RS_MARK)
    row["investability_status"] = "BLOCKED"
    return True, tagged


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
    detail = _s(row.get("recommendation_detailed")).upper()
    # v1.3.0 (F-08): EITHER field buy-family ⇒ BOTH become HOLD (Fix-A
    # equality preserved even when upstream desynchronized them).
    if reco in _BUY_FAMILY or detail in _BUY_FAMILY:
        row["recommendation"] = "HOLD"
        if "recommendation_detailed" in row or detail:
            row["recommendation_detailed"] = "HOLD"
        changed = True
    fa = _s(row.get("final_action")).upper()
    # v1.3.0 (F-01): hard forces DO_NOT_INVEST from ANY value INCLUDING
    # blank/None — "from ANY value" now means exactly that.
    if hard:
        if fa != "DO_NOT_INVEST":
            row["final_action"] = "DO_NOT_INVEST"
            changed = True
    elif fa in _INVEST_CLASS:
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
) -> Tuple[List[Dict[str, Any]], int, int, int, int, int]:
    """Apply W1A-2 then W1A-1 across `rows` in place.

    Returns the v1.3.0 SIX-tuple (rows, n_blocked_demotions,
    n_fetchfail_blocks, n_warn_invest, n_row_sanity, n_row_errors) —
    telemetry split per F-09 so operators can distinguish hard blocked
    corrections from soft identity demotions. All gates off ⇒ strict
    no-op, SAME object back.
    """
    g1, g2 = blocked_invariant_enabled(), fetchfail_blocked_enabled()
    g3 = warn_invest_invariant_enabled()
    g4 = row_sanity_enabled()
    if g4 and env_combo_violations():
        if not _env_combo_logged["done"]:
            _env_combo_logged["done"] = True
            print("::error::%s ENV COMBO VIOLATION — %s Row-sanity "
                  "quarantine SELF-DISABLED this process."
                  % (SAI_TAG, env_combo_violations()[0]))
        g4 = False
    if not (g1 or g2 or g3 or g4) or not isinstance(rows, list):
        return rows, 0, 0, 0, 0, 0
    n1 = n2 = n3 = n4 = errs = 0
    for row in rows:
        try:
            if not isinstance(row, dict):
                continue
            # v1.2.0 W1A-7 FIRST — quarantine marks BLOCKED so the SAME
            # pass's blocked chain below finishes the demotion; the soft
            # 52W tag rides regardless of hard classes.
            if g4:
                _rs_hard, _ = _apply_row_sanity(row)
                if _rs_hard:
                    n4 += 1
            # v1.0.1 P0-3: the fetch-failure FACT (not the injection event)
            # drives the demotion gate — an already-blocked fetch_failed row
            # must demote under W1A-2 alone.
            ff = bool(g2 and _is_fetch_failed(row))
            if ff:
                _ff_applied = False
                if not (_s(row.get("block_reason"))
                        or _s(row.get("Block Reason"))):
                    row["block_reason"] = _INJECTED_REASON
                    _ff_applied = True
                # v1.3.0 (F-02): W1A-2 says BLOCKED — say it explicitly,
                # pre-existing reason or not, blank status or not.
                if _s(row.get("investability_status")).upper() != "BLOCKED":
                    row["investability_status"] = "BLOCKED"
                    _ff_applied = True
                if _ff_applied:
                    _add_warning(row, _FETCH_MARK)
                    n2 += 1
            if _is_blocked(row) and (g1 or ff):
                # ff rows demote HARD by definition (they ARE BLOCKED now).
                if _demote_blocked(row, hard=(ff or _is_hard_blocked(row))):
                    n1 += 1
            # v1.1.0 W1A-8 — after the blocked chain so a row that is BOTH
            # blocked and identity-doubted takes the STRONGER outcome; a
            # warned-only row takes the soft WATCHLIST/WATCH demotion.
            if g3 and _has_identity_warning(row) and not _is_blocked(row):
                if _demote_identity_warning(row):
                    n3 += 1
        except Exception:
            errs += 1
    return rows, n1, n2, n3, n4, errs


# ----------------------------------------------------------------------------
# Self-test — spec fixtures. No prod imports; runs anywhere.
# ----------------------------------------------------------------------------
def _selftest() -> int:
    import copy
    _KEYS = ("TFB_T10_BLOCKED_INVARIANT", "TFB_T10_FETCHFAIL_BLOCKED",
             "TFB_SURFACE_BLOCKED_INVARIANT", "TFB_SURFACE_FETCHFAIL_BLOCKED",
             "TFB_WARN_INVEST_INVARIANT", "TFB_SURFACE_WARN_INVEST",
             "TFB_ROW_SANITY_QUARANTINE", "TFB_SURFACE_ROW_SANITY")
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
    _, n1, n2, _nw, _n4, e = apply_surface_action_invariants([r], "Global_Markets")
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
    _, n1, n2, _nw, _n4, e = apply_surface_action_invariants([r], "Commodities_FX")
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
    out, n1, n2, _nw, _n4x, e = apply_surface_action_invariants(rows, "GM")
    ck("e1 OFF -> zero mutation", rows == snap and out is rows
       and (n1, n2, _nw, _n4x, e) == (0, 0, 0, 0, 0))

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
    _, n1, n2, _nw, _n4, e = apply_surface_action_invariants(
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
    _, n1, n2, _nw, _n4, e = apply_surface_action_invariants([r], "CFX")
    ck("h1 P0-3: pre-blocked fetch_failed demotes under W1A-2 alone",
       r["recommendation"] == "HOLD"
       and r["final_action"] == "DO_NOT_INVEST" and (n1, n2) == (1, 1)
       and r["investability_status"] == "BLOCKED")
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
    _, n1, n2, _nw, _n4, e = apply_surface_action_invariants([r], "My_Portfolio")
    ck("l1 W1A-8 T82U: INVESTABLE/INVEST -> WATCHLIST/WATCH",
       r["investability_status"] == "WATCHLIST"
       and r["final_action"] == "WATCH")
    ck("l2 W1A-8 soft: BUY text + reliability UNTOUCHED",
       r["recommendation"] == "BUY"
       and r["forecast_reliability_score"] == 90.4)
    ck("l3 W1A-8: block_reason NOT written", r["block_reason"] == "")
    ck("l4 W1A-8 marker appended once",
       r["warnings"].count(_WARN_MARK) == 1 and (_nw, n1) == (1, 0))
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

    # (m) W1A-7 — Copper-Futures-class + breakout-survives + combo matrix
    for _k in _KEYS:
        os.environ.pop(_k, None)
    os.environ["TFB_ROW_SANITY_QUARANTINE"] = "1"      # combo VIOLATION
    r = {"symbol": "HG=F", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "day_high": 90, "day_low": 100,
         "current_price": 95, "block_reason": "", "warnings": []}
    import copy as _c2; snap = _c2.deepcopy(r)
    out5 = apply_surface_action_invariants([r], "CFX")
    ck("m1 combo violation -> quarantine SELF-DISABLED, row untouched",
       r == snap and out5[4] == 0 and bool(env_combo_violations()))
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"      # combo satisfied
    ck("m2 combo satisfied -> no violations", env_combo_violations() == [])
    _, n1, n2, _nw, n4, e = apply_surface_action_invariants([r], "CFX")
    ck("m3 Copper class High<Low -> quarantined + W1A-1 finishes: "
       "HOLD/DO_NOT_INVEST/BLOCKED",
       n4 == 1 and r["recommendation"] == "HOLD"
       and r["final_action"] == "DO_NOT_INVEST"
       and r["investability_status"] == "BLOCKED"
       and r["block_reason"].startswith("row_sanity:day_high_lt_day_low"))
    r = {"symbol": "BRK B", "recommendation": "HOLD", "current_price": 10,
         "day_high": 11, "day_low": 9, "block_reason": "", "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("m4 whitespace symbol quarantined even on HOLD row",
       r["investability_status"] == "BLOCKED")
    r = {"symbol": "NOPX", "recommendation": "HOLD", "current_price": "",
         "block_reason": "", "warnings": []}
    snap = _c2.deepcopy(r)
    apply_surface_action_invariants([r], "GM")
    ck("m5 blank price on NON-actionable row is NOT quarantined", r == snap)
    r = {"symbol": "NOP2", "recommendation": "BUY", "current_price": "",
         "final_action": "INVEST", "block_reason": "", "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("m6 blank price on ACTIONABLE row quarantined + demoted",
       r["final_action"] == "DO_NOT_INVEST")
    r = {"symbol": "FT1", "recommendation": "HOLD", "current_price": 5,
         "day_high": 6, "day_low": 4, "exchange": "USD", "currency": "US",
         "country": "Financials", "block_reason": "", "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("m7 field-type triple detected in one reason",
       all(t in r["block_reason"] for t in
           ("currency_in_exchange", "invalid_currency_value",
            "sector_in_country")))
    r = {"symbol": "BRKOUT", "recommendation": "BUY", "final_action":
         "INVEST", "investability_status": "INVESTABLE",
         "current_price": 105.0, "week52_high": 100.0, "week52_low": 60.0,
         "day_high": 106, "day_low": 101, "block_reason": "",
         "warnings": []}
    _, n1, n2, _nw, n4, e = apply_surface_action_invariants([r], "GM")
    ck("m8 SPEC BREAKOUT TEST: new 52W high SURVIVES — tag only, "
       "INVEST intact, zero quarantine",
       n4 == 0 and r["final_action"] == "INVEST"
       and r["warnings"] == [_RANGE_TAG_HI]
       and r["investability_status"] == "INVESTABLE")
    r["warnings"] = []
    r["current_price"] = 100.05                       # 0.05% < tol
    apply_surface_action_invariants([r], "GM")
    ck("m9 sub-tolerance 52W breach normalized (no tag)",
       r["warnings"] == [])
    r = {"symbol": "L52", "recommendation": "HOLD", "current_price": 55.0,
         "week52_high": 100.0, "week52_low": 60.0, "block_reason": "",
         "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("m10 52W LOW breach -> low tag, no quarantine",
       r["warnings"] == [_RANGE_TAG_LO]
       and not _s(r.get("block_reason")))

    # (n) v1.3.0 — the eleven adversarial FAILs, closed
    for _k in _KEYS:
        os.environ.pop(_k, None)
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
    r = {"symbol": "BLK0", "recommendation": "", "final_action": "",
         "investability_status": "BLOCKED", "block_reason": "",
         "warnings": []}
    _, n1, n2, _nw, _n4, e = apply_surface_action_invariants([r], "GM")
    ck("n1 F-01/A-01: HARD BLOCKED + BLANK action -> DO_NOT_INVEST",
       r["final_action"] == "DO_NOT_INVEST" and n1 == 1)
    os.environ["TFB_T10_FETCHFAIL_BLOCKED"] = "1"
    r = {"symbol": "FF0", "recommendation": "", "final_action": "",
         "investability_status": "", "block_reason": "",
         "warnings": ["fetch_failed:HTTP 500"]}
    apply_surface_action_invariants([r], "GM")
    ck("n2 F-02/A-03: ff + all-blank -> explicit BLOCKED + DO_NOT_INVEST",
       r["investability_status"] == "BLOCKED"
       and r["final_action"] == "DO_NOT_INVEST")
    r = {"symbol": "FF1", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE",
         "block_reason": "Stale price bar",
         "warnings": ["fetch_failed:HTTP 422"]}
    apply_surface_action_invariants([r], "GM")
    ck("n3 F-02/A-04: ff + pre-existing reason -> reason kept, "
       "explicit BLOCKED",
       r["block_reason"] == "Stale price bar"
       and r["investability_status"] == "BLOCKED")
    r = {"symbol": "DET1", "recommendation": "HOLD",
         "recommendation_detailed": "BUY", "final_action": "WATCH",
         "investability_status": "WATCHLIST", "block_reason": "x",
         "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("n4 F-08/A-05: detail-only BUY on blocked row -> BOTH HOLD",
       r["recommendation"] == "HOLD"
       and r["recommendation_detailed"] == "HOLD")
    os.environ["TFB_ROW_SANITY_QUARANTINE"] = "1"
    r = {"symbol": "CANON52", "recommendation": "BUY",
         "final_action": "INVEST", "investability_status": "INVESTABLE",
         "current_price": 105.0, "week_52_high": 100.0,
         "week_52_low": 60.0, "day_high": 106, "day_low": 101,
         "block_reason": "", "warnings": []}
    _, n1, n2, _nw, n4, e = apply_surface_action_invariants([r], "GM")
    ck("n5 F-05/A-08: CANONICAL week_52_high breakout -> tag only, "
       "INVEST intact",
       n4 == 0 and r["final_action"] == "INVEST"
       and r["warnings"] == [_RANGE_TAG_HI])
    r = {"symbol": "XYZ1", "recommendation": "HOLD", "current_price": 5,
         "currency": "XYZ", "block_reason": "", "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("n6 F-06/A-10: syntactically-valid garbage currency quarantined",
       "unsupported_currency_code" in _s(r.get("block_reason")))
    ok = True
    for c in ("CZK", "HUF", "PEN", "GBX", "SAR"):
        r = {"symbol": "C" + c, "recommendation": "HOLD",
             "current_price": 5, "currency": c, "day_high": 6,
             "day_low": 4, "block_reason": "", "warnings": []}
        apply_surface_action_invariants([r], "GM")
        ok = ok and not _s(r.get("block_reason")) and not r["warnings"]
    ck("n7 F-06/A-11: CZK/HUF/PEN + GBX subunit + SAR all pass clean", ok)
    ok = True
    for s2 in (" AAPL", "AAPL ", "AA PL"):
        r = {"symbol": s2, "recommendation": "HOLD", "current_price": 5,
             "block_reason": "", "warnings": []}
        apply_surface_action_invariants([r], "GM")
        ok = ok and "symbol_whitespace" in _s(r.get("block_reason"))
    ck("n8 F-07/A-12: leading/trailing/internal whitespace ALL quarantine",
       ok)
    r = {"symbol": "PXA", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "current_price": "",
         "price": 10, "day_high": 11, "day_low": 9, "block_reason": "",
         "warnings": []}
    apply_surface_action_invariants([r], "GM")
    ck("n9 F-13/A-13: blank current_price + valid price alias is PRICED "
       "(no false quarantine)",
       not _s(r.get("block_reason")) and r["final_action"] == "INVEST")
    for _k in _KEYS:                       # isolate: warn gate ONLY
        os.environ.pop(_k, None)
    os.environ["TFB_WARN_INVEST_INVARIANT"] = "1"
    r = {"symbol": "MC1", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "current_price": 5,
         "warnings": "Quote_Currency_Missing", "block_reason": ""}
    _, n1, n2, _nw, n4, e = apply_surface_action_invariants([r], "GM")
    ck("n10 F-10/A-14: mixed-case marker demotes",
       r["final_action"] == "WATCH")
    ck("n11 F-09/A-17: telemetry split — warn counter, not blocked",
       (_nw, n1) == (1, 0))

    # (p) v1.4.0 W1A-7b — open_outside_day_range
    for _k in _KEYS:
        os.environ.pop(_k, None)
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
    os.environ["TFB_ROW_SANITY_QUARANTINE"] = "1"      # combo satisfied
    import copy as _c3
    # p1 — the 1015.KL class: Open far BELOW Day Low on an INVEST row
    r = {"symbol": "1015.KL", "recommendation": "BUY",
         "recommendation_detailed": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "current_price": 7.09,
         "open": 4.64, "day_high": 7.12, "day_low": 7.05,
         "block_reason": "", "warnings": []}
    _, n1, _n2, _nw, n4, e = apply_surface_action_invariants([r], "GM")
    ck("p1 Open below Day Low -> quarantined + W1A-1 finishes",
       n4 == 1 and e == 0 and r["final_action"] == "DO_NOT_INVEST"
       and r["investability_status"] == "BLOCKED"
       and r["recommendation"] == "HOLD"
       and "open_outside_day_range" in r["block_reason"])
    snap = _c3.deepcopy(r)
    apply_surface_action_invariants([r], "GM")
    ck("p2 idempotent second pass", r == snap)
    # p3 — Open ABOVE Day High (the BC-2 direction)
    r = {"symbol": "WSR.US", "recommendation": "HOLD", "current_price": 18.99,
         "open": 51000.0, "day_high": 19.0, "day_low": 18.98,
         "block_reason": "", "warnings": []}
    _, _n1, _n2, _nw, n4, _e = apply_surface_action_invariants([r], "GM")
    ck("p3 Open above Day High -> quarantined",
       n4 == 1 and "open_outside_day_range" in r["block_reason"])
    # p4 — SUB-TOLERANCE: the live-holdings guarantee. Must NOT quarantine.
    r = {"symbol": "OTIS", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "current_price": 71.95,
         "open": 72.9436, "day_high": 72.94, "day_low": 70.61,
         "block_reason": "", "warnings": []}
    snap = _c3.deepcopy(r)
    _, _n1, _n2, _nw, n4, _e = apply_surface_action_invariants([r], "GM")
    ck("p4 sub-tolerance Open>High normalized — INVEST byte-untouched",
       n4 == 0 and r == snap)
    # p5 — negative control: Market_Leaders carries NO Open (0/255 populated)
    r = {"symbol": "1050.SR", "recommendation": "ACCUMULATE",
         "final_action": "INVEST", "investability_status": "INVESTABLE",
         "current_price": 21.20, "day_high": 21.20, "day_low": 20.68,
         "block_reason": "", "warnings": []}
    snap = _c3.deepcopy(r)
    _, _n1, _n2, _nw, n4, _e = apply_surface_action_invariants([r], "ML")
    ck("p5 blank Open (Market_Leaders control) -> structurally silent",
       n4 == 0 and r == snap)
    # p6 — clean row inside the range survives untouched
    r = {"symbol": "CLEAN", "recommendation": "BUY", "final_action": "INVEST",
         "investability_status": "INVESTABLE", "current_price": 10.0,
         "open": 9.5, "day_high": 10.5, "day_low": 9.0,
         "block_reason": "", "warnings": []}
    snap = _c3.deepcopy(r)
    _, _n1, _n2, _nw, n4, _e = apply_surface_action_invariants([r], "GM")
    ck("p6 Open inside range -> no quarantine, INVEST intact",
       n4 == 0 and r == snap)
    # p7 — High<Low charged to that class ONLY, never double-counted
    r = {"symbol": "HG=F", "recommendation": "HOLD", "current_price": 6.597,
         "open": 6.6345, "day_high": 6.4955, "day_low": 6.728,
         "block_reason": "", "warnings": []}
    apply_surface_action_invariants([r], "CFX")
    ck("p7 High<Low charged once — no open_outside_day_range double-count",
       "day_high_lt_day_low" in r["block_reason"]
       and "open_outside_day_range" not in r["block_reason"])
    # p8 — key coalesce: open_price and day_open are live engine aliases
    for _alias in ("open_price", "day_open"):
        r = {"symbol": "ALIAS", "recommendation": "HOLD",
             "current_price": 5.0, _alias: 99.0, "day_high": 5.5,
             "day_low": 4.5, "block_reason": "", "warnings": []}
        _, _n1, _n2, _nw, n4, _e = apply_surface_action_invariants([r], "GM")
        ck("p8 alias %s coalesced" % _alias,
           n4 == 1 and "open_outside_day_range" in r["block_reason"])
    # p9 — gate OFF: the class is inert, row byte-untouched
    os.environ.pop("TFB_ROW_SANITY_QUARANTINE", None)
    r = {"symbol": "1015.KL", "recommendation": "BUY", "final_action":
         "INVEST", "investability_status": "INVESTABLE", "open": 4.64,
         "current_price": 7.09, "day_high": 7.12, "day_low": 7.05,
         "block_reason": "", "warnings": []}
    snap = _c3.deepcopy(r)
    _, _n1, _n2, _nw, n4, _e = apply_surface_action_invariants([r], "GM")
    ck("p9 ROW_SANITY gate OFF -> class inert, row untouched",
       n4 == 0 and r == snap)
    # p10 — zero/negative Open is the blank-price class's business, not ours
    r = {"symbol": "ZEROOPEN", "recommendation": "HOLD", "current_price": 5.0,
         "open": 0.0, "day_high": 5.5, "day_low": 4.5,
         "block_reason": "", "warnings": []}
    os.environ["TFB_ROW_SANITY_QUARANTINE"] = "1"
    _, _n1, _n2, _nw, n4, _e = apply_surface_action_invariants([r], "GM")
    ck("p10 Open<=0 not charged to open_outside_day_range",
       "open_outside_day_range" not in _s(r.get("block_reason")))

    # (q) v1.4.1 — F-01 corrected semantics + F-04
    for _k in _KEYS:
        os.environ.pop(_k, None)
    os.environ["TFB_WARN_INVEST_INVARIANT"] = "1"
    import copy as _c4
    r = {"symbol": "LUND-B.ST", "recommendation": "BUY", "final_action":
         "INVEST", "investability_status": "INVESTABLE", "block_reason": "",
         "warnings": "xprovider_verified:yahoo_chart:0.0%"}
    snap = _c4.deepcopy(r)
    _, _n1, _n2, _nw, _n4, _e = apply_surface_action_invariants([r], "GM")
    ck("q1 F-01: exact 0.0% agreement is POSITIVE — INVEST byte-untouched",
       r == snap and _nw == 0)
    r = {"symbol": "RBI.VI", "recommendation": "BUY", "final_action":
         "INVEST", "investability_status": "INVESTABLE", "block_reason": "",
         "warnings": "xprovider_verified:yahoo_chart:0.3%"}
    snap = _c4.deepcopy(r)
    apply_surface_action_invariants([r], "GM")
    ck("q2 F-01: small nonzero delta is positive too — untouched", r == snap)
    r = {"symbol": "T82U.SI", "recommendation": "BUY", "final_action":
         "INVEST", "investability_status": "INVESTABLE", "block_reason": "",
         "warnings": "quote_current_price_missing; "
                     "xprovider_verified:yahoo_chart:0.0%"}
    _, _n1, _n2, _nw, _n4, _e = apply_surface_action_invariants([r], "MP")
    ck("q3 F-01: 0.0% + quote-missing STILL demotes (T82U class preserved)",
       r["final_action"] == "WATCH"
       and r["investability_status"] == "WATCHLIST" and _nw == 1)
    r = {"symbol": "CONFLICT", "recommendation": "BUY", "final_action":
         "INVEST", "investability_status": "INVESTABLE", "block_reason": "",
         "warnings": "xprovider_price_conflict:eodhd=10:yahoo=12:20.0%"}
    snap = _c4.deepcopy(r)
    apply_surface_action_invariants([r], "GM")
    ck("q5 conflict tag is the investability gate's business — W1A-8 silent",
       r == snap)
    for _k in _KEYS:
        os.environ.pop(_k, None)
    os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
    os.environ["TFB_ROW_SANITY_QUARANTINE"] = "1"
    r = {"symbol": "DETB", "recommendation": "HOLD",
         "recommendation_detailed": "BUY", "current_price": "",
         "final_action": "", "investability_status": "",
         "block_reason": "", "warnings": []}
    _, _n1, _n2, _nw, _n4, _e = apply_surface_action_invariants([r], "GM")
    ck("q4 F-04: detail-only BUY + blank price -> quarantined + demoted",
       _n4 == 1 and r["investability_status"] == "BLOCKED"
       and r["final_action"] == "DO_NOT_INVEST"
       and "nonpositive_or_blank_price_actionable" in r["block_reason"])

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
