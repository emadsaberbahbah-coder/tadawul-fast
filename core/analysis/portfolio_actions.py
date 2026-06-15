# -*- coding: utf-8 -*-
"""
core/analysis/portfolio_actions.py — Action Engine for My_Portfolio
Version: 1.0.3   (TFB Final Execution Plan v5.0 — Phase P5, milestone M2;
                  Engineering Audit Phase 0 — cost-basis + data-trust gates)

v1.0.3 [PHASE0-DATA-TRUST-GATE]: opportunity_builder v1.0.6 added a Data-Trust
gate that EXCLUDES sparse/stale Top_10 candidates, but My_Portfolio applied no
such check — so the SAME name that is correctly dropped as a thin/stale
candidate still produced a CONFIDENT action as a holding (5023.SR: thin 0/6
secondary signals → ADD; a stale-priced holding → ADD on an old price). The
audit's holding done-condition ("no holding produces a confident recommendation
unless cost basis, symbol identity, quote freshness, and core data pass
validation") was only half-met. FIX (default ON; reuses opportunity_builder's
shared _data_trust_assessment so My_Portfolio and Top_10 apply byte-identical
freshness + coverage logic — one implementation, two consumers — with a
getattr guard + fail-open so an older builder simply disables the feature): a
new step "1c" inside the never-overridden step-1 BLOCK gate withholds the
action when the holding's quote is STALE (last_updated older than
max_data_age_hours; acting on an old price is acting blind) — emitting
"BLOCKED — stale quote (...); action withheld pending price refresh". THIN
coverage (fewer than min_trust_fields of the six secondary signals) is, by
default, FLAGGED via a low_data_coverage alert rather than blocked — because a
holding's action is valuation-driven and the owner still needs guidance on a
position they hold (this is the deliberate, documented divergence from the
candidate side, where thin → exclude); set TFB_PF_BLOCK_THIN_COVERAGE=1 to also
block thin holdings for strict parity. Per-run telemetry (assessed /
stale_blocked / thin_flagged / thin_blocked) lands in meta.trust_gate. No change
to the cost-basis gate, the action truth table, sizing, the L7 funding identity,
P&L math, or the engine-ROI display toggle. OFF (TFB_PF_TRUST_GATE=0) restores
v1.0.2 behavior — only the version stamp and the new controls-snapshot keys
change. Forward-compatible: a future engine trust_level plugs into the same
assessment without restructuring.

v1.0.2 [PHASE0-COST-BASIS-GATE]: the step-1 BLOCK data gate validated
PRESENCE only (price / fx / quantity not None). A holding could pass with
Quantity AND Buy Price both present yet carry an absurd basis — BBD.US held
4 sh at a recorded 250 against a ~2.50 quote (about 100x) — and the page then
emitted a CONFIDENT ADD on a row whose P&L economics were garbage. Presence is
not plausibility (Engineering Audit, Finding 2). FIX (default ON; kill-switch
TFB_PF_COST_BASIS_GATE=0 restores v1.0.1 behavior — only the version stamp and
the new controls-snapshot keys change): a plausibility step inside the step-1
gate (still NEVER overridden) BLOCKs any action when a held position's buy
price is present-but-implausible — greater than TFB_PF_COST_MAX_RATIO (default
5) x or less than TFB_PF_COST_MIN_RATIO (default 0.2) x the current price, or
present-but-zero/negative/non-numeric — emitting "BLOCKED — review cost basis
(<specific reason>)" so the failing fact is visible on the row. A cost-basis
BLOCK also marks the basis untrusted: its cost_sar / pnl_sar / pnl_pct are
nulled so neither the row nor the portfolio KPIs aggregate a P&L derived from
a basis we just rejected (market value, being price-based, is kept). A MISSING
(blank) Buy Price deliberately keeps the v1.0.0 contract documented below
("Missing Buy Price does NOT block an action"): the missing_cost_basis alert +
None P&L already handle it honestly and the action is valuation-driven, not
cost-driven — set TFB_PF_BLOCK_MISSING_COST_BASIS=1 to additionally block
blank-basis holdings (strict-audit mode). A distinct "cost_basis_blocked" alert
names the required fix; the existing "blocked_positions" alert now counts
presence blocks only, and "missing_cost_basis" excludes cost-basis blocks to
avoid double-flagging. No change to the action truth table thresholds, ADD
sizing, the L7 funding identity, the displayed valuation ROI, or the v1.0.1
engine-forecast display toggle.

v1.0.1 [ENGINE-ROI-DISPLAY]: each action row's "ROI %" is pure VALUATION upside
(cand.roi_pct = (ref - price)/price, per L5, computed in opportunity_builder),
while the engine's own 12-month forecast (cand.engine_roi_12m_pct) was carried
only in detail.engine_forecast_roi_pct and shown NOWHERE on the page. A holding
could therefore read 35% upside while the engine forecast for that name was
~0%. The action verdict already considers the engine recommendation (the EXIT
SELL-tier rule), so this is not a wrong-action risk; the defect is purely that
the displayed ROI overstates expected return vs the engine's view and the
spread is invisible. Mirrors the opportunity_builder v1.0.5 fix. FIX (env-gated,
default OFF; no change to the action truth table, sizing, the L7 funding
identity, or the actual P&L figures): when ON, each action row gains the
normalized engine forecast (engine_roi_pct) and the valuation ROI under an
explicit name (valuation_roi_pct); the advisor note states the engine 12M
forecast and frames the displayed ROI as a TARGET, not a forecast; and
detail.engine_forecast_roi_pct is normalized to percent (the raw field carried
a ratio for ratio-form providers). The rendered roi_pct / ann_roi_pct and the
actual pnl_sar / pnl_pct are LEFT INTACT. Toggle TFB_PF_ENGINE_ROI_DISPLAY=1 to
enable; OFF restores byte-identical v1.0.0 behavior (only the version stamp
changes). Note: My_Portfolio has no forecast "expected gain" to repoint — its
gain columns are actual cost-vs-market P&L, unaffected here.

WHY THIS MODULE EXISTS
----------------------
Plan v5.0 §3.2 + rulings L3/L7/L8/L13/L14: My_Portfolio stops being a static
holdings list and becomes a decision page — every holding gets exactly one
action from the L3 vocabulary (ADD / HOLD / TRIM / EXIT / BLOCK), sized in
SAR, funded under the L7 identity, with a one-sentence L8 advisor note. This
module is the intelligence layer between GAS-supplied holdings rows and the
page; the route (routes/advanced_analysis.py, P5b) exposes it as
POST /sheet-rows/portfolio-actions and 10_My_Portfolio.gs (P6) renders it.

ARCHITECTURE — REUSE OF opportunity_builder (P2, v1.0.1+)
---------------------------------------------------------
All row normalization (tolerant value parsing, display-header/snake aliases,
FX resolution incl. GBp/GBX/ZAC/ILA subunits, valuation ROI per L5, stop/TP/
R-R wealth math per L6, trend/risk/conflict normalization) is DELEGATED to
core/analysis/opportunity_builder.normalize_candidate — one implementation,
two consumers. Consumed surface (version floor 1.0.1, checked at runtime and
reported in meta.versions): normalize_candidate, confidence_band, make_criteria,
_to_float, _norm_token, OPPORTUNITY_BUILDER_VERSION. If the import fails the
module degrades to a status="unavailable" skeleton (fail-soft, mirrors P3).

HOLDINGS INPUT (GAS pool pattern, §5 amendment precedent)
---------------------------------------------------------
10_My_Portfolio.gs POSTs holdings rows using the SAME display headers as the
Top_10 pool (Symbol, Name, Sector, Exchange, Currency, Current Price,
Target Price, Intrinsic Value, Expected ROI 12M, Forecast Reliability Score,
Data Quality Score, Risk Bucket, Provider/Engine Conflict, Volatility 30D,
Avg Volume 30D, Recommendation Detail, Investability Status, Block Reason)
PLUS the two position fields: "Quantity" and "Buy Price" (average cost,
native currency, from _Portfolio_CostBasis — closing backlog bug #2 where
Quantity never reached the compute layer). Missing Buy Price does NOT block
an action (P&L fields go None + missing_cost_basis alert); missing Price/FX/
Quantity DOES (BLOCK — acting blind is worse than not acting).

ACTION TRUTH TABLE (v1.0.0 — pinned here for the external auditor; the plan
fixes the vocabulary L3 and the funding identity L7, not these thresholds)
---------------------------------------------------------------------------
Evaluated in strict precedence; first match wins:
  1. BLOCK  — data gate: price OR fx OR quantity unusable (presence); OR, when
     the v1.0.2 cost-basis gate is ON (default), a held position's Buy Price is
     present-but-implausible (> 5x / < 0.2x current price, or zero/negative/
     non-numeric); OR, when the v1.0.3 data-trust gate is ON (default), the
     holding's quote is STALE (last_updated older than max_data_age_hours) —
     and, if TFB_PF_BLOCK_THIN_COVERAGE=1, also when coverage is THIN. Never
     overridden. A MISSING Buy Price does NOT block unless
     TFB_PF_BLOCK_MISSING_COST_BASIS=1; THIN coverage is otherwise an alert.
  2. Low-confidence cap (L8 analog): reliability < 60 caps any computed
     ADD/TRIM/EXIT down to HOLD ("manual review") — weak signals never trade.
  3. EXIT   — valuation ROI ≤ exit_roi_pct (default −15: price ≳17.6% above
     reference) OR engine recommendation in the SELL tier
     (SELL/STRONG_SELL/EXIT/AVOID/REDUCE, case/space-insensitive).
  4. TRIM   — (a) position weight > Max Position % → trim exactly to cap;
              (b) sector weight > Max Sector % → pro-rata share of the
                  sector excess across that sector's holdings;
              (c) valuation: exit_roi_pct < ROI ≤ trim_roi_pct (default −5)
                  → trim VALUATION_TRIM_FRAC (0.5) of the position.
     Largest of (a)/(b)/(c) wins; proceeds_sar recorded.
  5. ADD    — reliability ≥ Min Reliability to Add AND dq ≥ Min DQ to Add
              AND valuation ROI ≥ add_roi_pct (default 12, = T10 Required
              ROI) AND conflict is not True AND position/sector headroom > 0.
              Sizing happens in the funding pass (below); a qualified ADD
              with zero deployable stays ADD with 0 SAR + note (L13).
  6. HOLD   — everything else; reason names the first binding fact.

FUNDING PASS (L7 identity, mode-aware via PF: Rebalance Mode)
-------------------------------------------------------------
  proceeds            = Σ proceeds_sar of suggested TRIM/EXIT
  cash_floor          = Target Cash % × (Σ market value + cash)
  deployable_for_adds = max(0, cash − cash_floor) + proceeds
      · 'New Cash Only'        → proceeds EXCLUDED from deployable
      · 'Advisory Only' / 'Rebalance to Targets' → proceeds included
        (v1.0.0 treats these two identically for sizing; ALL output is
        advisory regardless — meta.controls_snapshot carries the mode)
  ADD candidates are sized high-conviction-first (ann ROI desc, reliability
  desc, symbol asc): suggested = min(position-cap headroom, sector-cap
  headroom, remaining deployable), share-floored to lot_size; funded
  cash-first then proceeds; every ADD names funds_from. Σ adds ≤ deployable.

PAYLOAD (§5 zone discipline — FROZEN once P6 renders it)
--------------------------------------------------------
{ version, status: ok|empty|disabled|unavailable|error,
  kpis{ portfolio_value_sar, holdings_value_sar, cash_sar, cash_pct,
        cost_basis_sar, pnl_sar, pnl_pct, deployable_sar,
        proceeds_pending_sar, adds_funded_sar, capital_unallocated_sar,
        positions, action_counts{ADD,HOLD,TRIM,EXIT,BLOCK},
        blended_reliability },
  actions[ {symbol,name,market,sector,currency,fx_to_sar,quantity,
            avg_cost,price,price_sar,market_value_sar,cost_sar,pnl_sar,
            pnl_pct,weight_pct,action,action_reason,confidence_band,
            suggested_delta_sar,suggested_delta_shares,proceeds_sar,
            funds_from,stop_sar,tp1_sar,tp2_sar,roi_pct,ann_roi_pct,
            reliability,dq,advisor_note,
            detail{valuation_basis,target_price,intrinsic_value,
                   engine_forecast_roi_pct,engine_recommendation,risk_level,
                   news_trend,sector_trend,sector_weight_pct,
                   position_cap_pct,sector_cap_pct,rr,mos_pct,stop_pct,
                   key_facts,review_date}} ],
  sector_summary[ {sector,value_sar,weight_pct,cap_pct,over_cap,positions} ],
  alerts[ {type,count,required_action} ],
  meta{ controls_snapshot, action_counts, fx, versions, counts,
        generated_utc, upstream passthrough } }

ENV KILL-SWITCHES (read per call; explicit controls > env > defaults)
  TFB_PF_ENABLED          "1"    "0" ⇒ status="disabled" skeleton
  TFB_PF_ADD_ROI_PCT      "12"   ADD valuation-upside threshold
  TFB_PF_TRIM_ROI_PCT     "-5"   valuation TRIM threshold (ROI ≤ this)
  TFB_PF_EXIT_ROI_PCT     "-15"  valuation EXIT threshold (ROI ≤ this)
  TFB_PF_VALUATION_TRIM_FRAC "0.5"  fraction trimmed on valuation TRIM
  TFB_PF_REVIEW_DAYS      "30"   review-by horizon
  TFB_PF_LOT_SIZE         "1"    share lot rounding for ADD sizing
  TFB_PF_MAX_HOLDINGS     "0"    0 = unlimited; CPU safety clamp
  TFB_PF_COST_BASIS_GATE  "1"    v1.0.2: "0" ⇒ skip cost-basis plausibility BLOCK
  TFB_PF_COST_MAX_RATIO   "5"    v1.0.2: Buy Price > this x price ⇒ BLOCK (high)
  TFB_PF_COST_MIN_RATIO   "0.2"  v1.0.2: Buy Price < this x price ⇒ BLOCK (low)
  TFB_PF_BLOCK_MISSING_COST_BASIS "0" v1.0.2: "1" ⇒ blank Buy Price also BLOCKs
  TFB_PF_TRUST_GATE       "1"    v1.0.3: "0" ⇒ skip the data-trust (stale) gate
  TFB_PF_MAX_DATA_AGE_HOURS "168" v1.0.3: stale quote older than this ⇒ BLOCK
  TFB_PF_MIN_TRUST_FIELDS "2"    v1.0.3: fewer core signals ⇒ thin (alert)
  TFB_PF_BLOCK_THIN_COVERAGE "0" v1.0.3: "1" ⇒ thin holdings also BLOCK

HONESTY RULES (L13): zero ADDs, all-HOLD, empty holdings, unknown trends and
None P&L are CORRECT outputs; nothing is upgraded or padded to fill space.
"""

from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone

PORTFOLIO_ACTIONS_VERSION = "1.0.3"
_OB_VERSION_FLOOR = (1, 0, 1)

# --- opportunity_builder import (package → relative → flat), fail-soft -----
_ob = None
_OB_IMPORT_ERROR = ""
try:
    from core.analysis import opportunity_builder as _ob  # type: ignore
except Exception as _e1:  # pragma: no cover - environment dependent
    try:
        from . import opportunity_builder as _ob  # type: ignore
    except Exception as _e2:
        try:
            import opportunity_builder as _ob  # type: ignore
        except Exception as _e3:
            _ob = None
            _OB_IMPORT_ERROR = "%s | %s | %s" % (_e1, _e2, _e3)

ACTION_ADD = "ADD"
ACTION_HOLD = "HOLD"
ACTION_TRIM = "TRIM"
ACTION_EXIT = "EXIT"
ACTION_BLOCK = "BLOCK"
ACTIONS = (ACTION_ADD, ACTION_HOLD, ACTION_TRIM, ACTION_EXIT, ACTION_BLOCK)

_SELL_TIER = {"SELL", "STRONGSELL", "EXIT", "AVOID", "REDUCE"}

# v1.0.2: every cost-basis BLOCK reason contains this marker. One predicate
# (_is_cost_basis_block) keys off it so the scrub, the alert count, and the
# missing-basis exclusion can never drift apart if a reason is reworded.
_CB_BLOCK_MARKER = "review cost basis"

# v1.0.3: data-trust BLOCK reason markers (stale always blocks; thin blocks
# only when block_thin_coverage). One predicate keeps the alert split + the
# blocked_positions exclusion in lockstep with the reason strings.
_STALE_BLOCK_MARKER = "stale quote"
_THIN_BLOCK_MARKER = "thin data"

REBALANCE_ADVISORY = "Advisory Only"
REBALANCE_TARGETS = "Rebalance to Targets"
REBALANCE_NEW_CASH = "New Cash Only"

DEFAULT_CONTROLS = {
    # §3.2 PF panel (labels mapped by the route: "PF: Cash Available SAR" …)
    "cash_available_sar": 0.0,
    "target_cash_pct": 10.0,
    "max_position_pct": 15.0,
    "max_sector_pct": 30.0,
    "min_reliability_add": 70.0,
    "min_dq_add": 80.0,
    "rebalance_mode": REBALANCE_ADVISORY,
    # v1.0.0 pinned thresholds (env-overridable; see policy block)
    "add_roi_pct": 12.0,
    "trim_roi_pct": -5.0,
    "exit_roi_pct": -15.0,
    "valuation_trim_frac": 0.5,
    "review_days": 30,
    "lot_size": 1,
    "max_holdings": 0,
    "period_months": 12,
    # v1.0.1 engine-forecast display (env-tunable; see policy block)
    "engine_roi_display_enabled": False,
    # v1.0.2 cost-basis plausibility gate (Phase 0; env-tunable, see policy)
    "cost_basis_gate_enabled": True,
    "cost_max_ratio": 5.0,
    "cost_min_ratio": 0.2,
    "block_missing_cost_basis": False,
    # v1.0.3 data-trust gate (Phase 0 parity with opportunity_builder v1.0.6)
    "trust_gate_enabled": True,
    "max_data_age_hours": 168.0,
    "min_trust_fields": 2,
    "block_thin_coverage": False,
}

_CONTROLS_FLOAT = ("cash_available_sar", "target_cash_pct", "max_position_pct",
                   "max_sector_pct", "min_reliability_add", "min_dq_add",
                   "add_roi_pct", "trim_roi_pct", "exit_roi_pct",
                   "valuation_trim_frac", "cost_max_ratio", "cost_min_ratio",
                   "max_data_age_hours")
_CONTROLS_INT = ("review_days", "lot_size", "max_holdings", "period_months",
                 "min_trust_fields")
_CONTROLS_BOOL = ("engine_roi_display_enabled", "cost_basis_gate_enabled",
                  "block_missing_cost_basis", "trust_gate_enabled",
                  "block_thin_coverage")


def _env_str(name, default):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _env_float(name, default):
    try:
        return float(_env_str(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name, default):
    try:
        return int(float(_env_str(name, default)))
    except (TypeError, ValueError):
        return int(default)


def _env_enabled():
    return str(_env_str("TFB_PF_ENABLED", "1")).strip().lower() not in (
        "0", "false", "no", "off")


def _env_cost_basis_gate():
    """v1.0.2: master switch for the cost-basis plausibility BLOCK. Default ON;
    set TFB_PF_COST_BASIS_GATE=0 to restore v1.0.1 behavior (gate skipped)."""
    return str(_env_str("TFB_PF_COST_BASIS_GATE", "1")).strip().lower() \
        not in ("0", "false", "no", "off")


def _env_block_missing_cost_basis():
    """v1.0.2: strict-audit toggle. Default OFF; set
    TFB_PF_BLOCK_MISSING_COST_BASIS=1 to BLOCK holdings whose Buy Price is
    blank (otherwise a missing basis is tolerated per the v1.0.0 contract)."""
    return str(_env_str("TFB_PF_BLOCK_MISSING_COST_BASIS", "0")).strip().lower() \
        in ("1", "true", "yes", "on")


def _env_trust_gate():
    """v1.0.3: master switch for the data-trust (stale-quote) BLOCK. Default ON;
    set TFB_PF_TRUST_GATE=0 to restore v1.0.2 behavior (no trust assessment)."""
    return str(_env_str("TFB_PF_TRUST_GATE", "1")).strip().lower() not in (
        "0", "false", "no", "off")


def _env_block_thin_coverage():
    """v1.0.3: strict-parity toggle. Default OFF (thin coverage is flagged via
    the low_data_coverage alert, not blocked); set TFB_PF_BLOCK_THIN_COVERAGE=1
    to also BLOCK thinly-covered holdings like the Top_10 candidate side."""
    return str(_env_str("TFB_PF_BLOCK_THIN_COVERAGE", "0")).strip().lower() \
        in ("1", "true", "yes", "on")


def _env_engine_roi_display():
    """v1.0.1: engine-forecast display toggle. Default OFF; set
    TFB_PF_ENGINE_ROI_DISPLAY=1 to surface the engine 12M forecast on every
    action row. OFF is byte-identical v1.0.0."""
    return str(_env_str("TFB_PF_ENGINE_ROI_DISPLAY", "0")).strip().lower() \
        in ("1", "true", "yes", "on")


def _engine_roi_to_pct(value):
    """v1.0.1: normalize the engine 12M forecast ROI to a PERCENT (mirrors
    opportunity_builder._engine_roi_to_pct, kept local so the consumed-surface
    contract is unchanged). Providers deliver it as a ratio (e.g. -0.20) or a
    percent (e.g. -20.0); |v| < 1.5 is treated as a ratio and scaled x100.
    Returns None when absent so 'Unknown' stays Unknown."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v * 100.0 if abs(v) < 1.5 else v


def _env_overrides():
    return {
        "add_roi_pct": _env_float("TFB_PF_ADD_ROI_PCT",
                                  DEFAULT_CONTROLS["add_roi_pct"]),
        "trim_roi_pct": _env_float("TFB_PF_TRIM_ROI_PCT",
                                   DEFAULT_CONTROLS["trim_roi_pct"]),
        "exit_roi_pct": _env_float("TFB_PF_EXIT_ROI_PCT",
                                   DEFAULT_CONTROLS["exit_roi_pct"]),
        "valuation_trim_frac": _env_float(
            "TFB_PF_VALUATION_TRIM_FRAC",
            DEFAULT_CONTROLS["valuation_trim_frac"]),
        "review_days": _env_int("TFB_PF_REVIEW_DAYS",
                                DEFAULT_CONTROLS["review_days"]),
        "lot_size": _env_int("TFB_PF_LOT_SIZE", DEFAULT_CONTROLS["lot_size"]),
        "max_holdings": _env_int("TFB_PF_MAX_HOLDINGS",
                                 DEFAULT_CONTROLS["max_holdings"]),
        "engine_roi_display_enabled": _env_engine_roi_display(),
        "cost_basis_gate_enabled": _env_cost_basis_gate(),
        "cost_max_ratio": _env_float("TFB_PF_COST_MAX_RATIO",
                                     DEFAULT_CONTROLS["cost_max_ratio"]),
        "cost_min_ratio": _env_float("TFB_PF_COST_MIN_RATIO",
                                     DEFAULT_CONTROLS["cost_min_ratio"]),
        "block_missing_cost_basis": _env_block_missing_cost_basis(),
        "trust_gate_enabled": _env_trust_gate(),
        "max_data_age_hours": _env_float(
            "TFB_PF_MAX_DATA_AGE_HOURS",
            DEFAULT_CONTROLS["max_data_age_hours"]),
        "min_trust_fields": _env_int(
            "TFB_PF_MIN_TRUST_FIELDS", DEFAULT_CONTROLS["min_trust_fields"]),
        "block_thin_coverage": _env_block_thin_coverage(),
    }


def make_controls(overrides=None):
    """DEFAULTS < env policy block < explicit overrides; coerced types."""
    ctl = dict(DEFAULT_CONTROLS)
    ctl.update(_env_overrides())
    for key, val in (overrides or {}).items():
        k = str(key).strip().lower()
        if k not in ctl or val in (None, ""):
            continue
        if k in _CONTROLS_FLOAT:
            f = _to_float(val)
            if f is not None:
                ctl[k] = f
        elif k in _CONTROLS_INT:
            f = _to_float(val)
            if f is not None:
                ctl[k] = int(f)
        elif k in _CONTROLS_BOOL:
            ctl[k] = str(val).strip().lower() in ("1", "true", "yes", "on")
        else:
            ctl[k] = str(val).strip()
    if ctl["lot_size"] < 1:
        ctl["lot_size"] = 1
    if not (0.0 < ctl["valuation_trim_frac"] <= 1.0):
        ctl["valuation_trim_frac"] = DEFAULT_CONTROLS["valuation_trim_frac"]
    if ctl["target_cash_pct"] < 0:
        ctl["target_cash_pct"] = 0.0
    # v1.0.2: keep the plausibility band sane (high > 1x, low in (0,1),
    # low < high); reset to defaults on any misconfiguration.
    if ctl["cost_max_ratio"] <= 1.0:
        ctl["cost_max_ratio"] = DEFAULT_CONTROLS["cost_max_ratio"]
    if not (0.0 < ctl["cost_min_ratio"] < 1.0):
        ctl["cost_min_ratio"] = DEFAULT_CONTROLS["cost_min_ratio"]
    if ctl["cost_min_ratio"] >= ctl["cost_max_ratio"]:
        ctl["cost_min_ratio"] = DEFAULT_CONTROLS["cost_min_ratio"]
        ctl["cost_max_ratio"] = DEFAULT_CONTROLS["cost_max_ratio"]
    # v1.0.3 data-trust threshold sanity
    if ctl["max_data_age_hours"] <= 0:
        ctl["max_data_age_hours"] = DEFAULT_CONTROLS["max_data_age_hours"]
    if ctl["min_trust_fields"] < 0:
        ctl["min_trust_fields"] = 0
    mode = str(ctl.get("rebalance_mode") or REBALANCE_ADVISORY)
    low = mode.strip().lower()
    if "new cash" in low:
        ctl["rebalance_mode"] = REBALANCE_NEW_CASH
    elif "target" in low:
        ctl["rebalance_mode"] = REBALANCE_TARGETS
    else:
        ctl["rebalance_mode"] = REBALANCE_ADVISORY
    return ctl


def _to_float(v):
    """Delegates to the builder when bound; minimal fallback otherwise."""
    if _ob is not None:
        return _ob._to_float(v)
    try:
        f = float(str(v).replace(",", "").replace("%", "").strip())
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _pos_float(v):
    """v1.0.2: finite, strictly-positive float or None (bool-guarded). Used by
    the cost-basis gate so a 'present but zero/negative/non-numeric' value
    coerces to None while genuine positive values pass through. Delegates the
    parse to _to_float so coercion stays consistent with the rest of the
    module."""
    if isinstance(v, bool):
        return None
    f = _to_float(v)
    if f is None or not math.isfinite(f) or f <= 0:
        return None
    return f


def _ob_version_ok():
    if _ob is None:
        return False
    try:
        parts = tuple(int(p) for p in
                      str(_ob.OPPORTUNITY_BUILDER_VERSION).split("."))
        return parts >= _OB_VERSION_FLOOR
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Holding normalization (delegates row science to opportunity_builder)
# ---------------------------------------------------------------------------

_QTY_KEYS = ("quantity", "qty", "shares", "units", "holdingqty")
_COST_KEYS = ("buyprice", "avgcost", "averagecost", "costbasis",
              "purchaseprice", "avgbuyprice", "costpershare")


def _position_fields(row):
    """Extract Quantity / Buy Price from a raw row via normalized tokens.

    v1.0.2: also returns the RAW Buy Price cell (cost_raw) — the first present
    (non-empty) value seen in any cost-alias column, pre-coercion — so the
    cost-basis gate can tell an EMPTY cell (no basis, tolerated) from a
    PRESENT-but-unparseable one ("N/A", "TBD" — a basis the user typed wrong,
    which the gate should block). _to_float collapses both to None, so the raw
    is the only thing that preserves the distinction. Returns (qty, cost,
    cost_raw)."""
    qty = None
    cost = None
    cost_raw = None
    for key, val in (row or {}).items():
        tok = _ob._norm_token(key) if _ob is not None else str(key).lower()
        if qty is None and tok in _QTY_KEYS:
            qty = _to_float(val)
        elif tok in _COST_KEYS:
            if cost_raw is None and val is not None and str(val).strip() != "":
                cost_raw = val
            if cost is None:
                f = _to_float(val)
                if f is not None:
                    cost = f
    return qty, cost, cost_raw


def normalize_holding(row, fx_rates, controls):
    """opportunity_builder.normalize_candidate + position economics."""
    crit = _ob.make_criteria({"period_months": controls["period_months"]})
    cand = _ob.normalize_candidate(row, fx_rates, crit)
    qty, avg_cost, avg_cost_raw = _position_fields(row)
    cand["quantity"] = qty
    cand["avg_cost"] = avg_cost
    # v1.0.2: internal-only; never emitted in a row (kept for the gate's
    # blank-vs-unparseable distinction). Not surfaced by _action_row.
    cand["avg_cost_raw"] = avg_cost_raw
    fx = cand.get("fx_to_sar")
    price = cand.get("price")
    if (qty is not None and qty > 0 and price is not None and
            fx is not None):
        cand["market_value_sar"] = qty * price * fx
    else:
        cand["market_value_sar"] = None
    if (qty is not None and qty > 0 and avg_cost is not None and
            avg_cost > 0 and fx is not None):
        # NOTE v1.0.0: cost converted at CURRENT fx (historical trade-date fx
        # is not available in the sheet); documented simplification.
        cand["cost_sar"] = qty * avg_cost * fx
    else:
        cand["cost_sar"] = None
    if cand["market_value_sar"] is not None and cand["cost_sar"]:
        cand["pnl_sar"] = cand["market_value_sar"] - cand["cost_sar"]
        cand["pnl_pct"] = (cand["pnl_sar"] / cand["cost_sar"]) * 100.0
    else:
        cand["pnl_sar"] = None
        cand["pnl_pct"] = None
    # v1.0.3: internal-only trust detail (stale / thin / age / signals), shared
    # with opportunity_builder. Never surfaced by _action_row; drives step 1c
    # and the trust alerts/telemetry.
    cand["_trust"] = _trust_assess(cand, controls)
    return cand


# ---------------------------------------------------------------------------
# Action engine (v1.0.0 truth table — precedence documented in the WHY block)
# ---------------------------------------------------------------------------

def _is_sell_tier(recommendation):
    if not recommendation:
        return False
    tok = "".join(ch for ch in str(recommendation).upper() if ch.isalnum())
    for sell in _SELL_TIER:
        if tok == sell or tok.startswith(sell):
            return True
    return False


def _is_cost_basis_block(action, reason):
    """v1.0.2: True iff this entry is a BLOCK raised by the cost-basis gate
    (vs a presence/data-gate BLOCK). Single source of truth for the scrub +
    alert split."""
    return action == ACTION_BLOCK and _CB_BLOCK_MARKER in (reason or "").lower()


def _is_stale_block(action, reason):
    """v1.0.3: True iff this BLOCK was raised by the data-trust gate for a
    STALE quote."""
    return action == ACTION_BLOCK and _STALE_BLOCK_MARKER in (
        reason or "").lower()


def _is_trust_block(action, reason):
    """v1.0.3: True iff this BLOCK was raised by the data-trust gate (stale or
    thin). Used to exclude trust blocks from the presence (blocked_positions)
    count."""
    r = (reason or "").lower()
    return action == ACTION_BLOCK and (
        _STALE_BLOCK_MARKER in r or _THIN_BLOCK_MARKER in r)


def _trust_assess(cand, controls):
    """v1.0.3: the shared trust detail dict for a holding, or None when the
    trust gate is disabled or the shared assessment is unavailable. REUSES
    opportunity_builder._data_trust_assessment so My_Portfolio and Top_10 apply
    identical freshness + coverage logic (one implementation, two consumers).
    getattr-guarded + fail-open: an older builder without the helper, or any
    error, yields None (the trust gate then withholds NO action)."""
    if not controls.get("trust_gate_enabled"):
        return None
    assess = getattr(_ob, "_data_trust_assessment", None)
    if assess is None:
        return None
    try:
        _ok, _cur, detail = assess(cand, {
            "max_data_age_hours": controls.get("max_data_age_hours"),
            "min_trust_fields": controls.get("min_trust_fields"),
        })
        return detail
    except Exception:
        return None


def _cost_basis_block_reason(avg_cost_raw, current_price, position_qty,
                             controls):
    """v1.0.2 Phase-0 plausibility gate for a held position's cost basis.

    Takes the RAW Buy Price cell (pre-coercion) as avg_cost_raw and coerces it
    internally, so a present-but-unparseable value ("N/A") is distinguished
    from an empty cell — _to_float upstream would otherwise collapse both to
    None and hide the difference.

    Returns (ok, reason): ok=False => the caller emits a BLOCK (step 1, never
    overridden). Self-contained so audit_data_quality / future callers can
    reuse the identical rule.

    A position is 'held' only when position_qty is a finite number > 0; the
    qty presence gate already enforces this upstream, re-checked here so the
    helper is safe to reuse. Policy (controls-tunable):
      • Buy Price present but zero/negative/non-numeric         -> BLOCK
      • Buy Price present and > cost_max_ratio  × current price -> BLOCK (high)
      • Buy Price present and < cost_min_ratio  × current price -> BLOCK (low)
      • Buy Price MISSING (None/blank) -> BLOCK only when
        block_missing_cost_basis is set; otherwise OK (documented v1.0.0
        behaviour: missing_cost_basis alert + None P&L, action still valid).
    """
    if _pos_float(position_qty) is None:
        return True, None  # no valid holding — the qty presence gate owns this

    cost = _pos_float(avg_cost_raw)
    if cost is None:
        is_blank = (avg_cost_raw is None or
                    (isinstance(avg_cost_raw, str) and
                     avg_cost_raw.strip() == ""))
        if is_blank:
            if controls.get("block_missing_cost_basis"):
                return (False,
                        "BLOCKED — review cost basis (no buy price recorded "
                        "for a held position)")
            return True, None  # tolerated; missing_cost_basis alert handles it
        return (False,
                "BLOCKED — review cost basis (buy price is zero, negative, or "
                "non-numeric while a holding exists)")

    price = _pos_float(current_price)
    if price is None:
        return (False,
                "BLOCKED — review cost basis (no usable current price to "
                "validate the buy price against)")

    max_r = controls.get("cost_max_ratio") or DEFAULT_CONTROLS["cost_max_ratio"]
    min_r = controls.get("cost_min_ratio") or DEFAULT_CONTROLS["cost_min_ratio"]
    ratio = cost / price
    if ratio > max_r:
        return (False,
                "BLOCKED — review cost basis (buy price %s is %.1fx the "
                "current price %s — likely a data-entry error)"
                % (_fmt(cost), ratio, _fmt(price)))
    if ratio < min_r:
        return (False,
                "BLOCKED — review cost basis (buy price %s is %.2fx the "
                "current price %s — likely a data-entry error)"
                % (_fmt(cost), ratio, _fmt(price)))
    return True, None


def decide_action(cand, controls, weight_pct, sector_weight_pct,
                  sector_excess_share_sar):
    """Pure: one holding → (action, reason, proceeds_sar, capped_from).

    `sector_excess_share_sar` is this holding's pro-rata share of its
    sector's value above Max Sector % (0 when the sector is within cap).
    Returns proceeds for TRIM/EXIT; ADD sizing happens in the funding pass.
    """
    mv = cand.get("market_value_sar")
    rel = cand.get("reliability")
    conf = _ob.confidence_band(rel) if _ob is not None else "Low"

    # 1. BLOCK — data gate (presence: price / fx / quantity)
    missing = []
    if cand.get("price") is None:
        missing.append("price")
    if cand.get("fx_to_sar") is None:
        missing.append("fx")
    if not (cand.get("quantity") is not None and cand["quantity"] > 0):
        missing.append("quantity")
    if missing:
        return (ACTION_BLOCK,
                "Data gate: missing " + "/".join(missing) +
                " — manual review required", 0.0, None)

    # 1b. BLOCK — cost-basis plausibility (v1.0.2; Phase 0, Finding 2).
    #     Presence (gate above) is not plausibility: a held position can pass
    #     with qty AND buy price present yet carry an absurd basis (BBD: 250
    #     against a ~2.50 quote = 100x → a confident ADD on garbage economics).
    #     When ON (default) a present-but-implausible basis BLOCKs here, still
    #     inside the never-overridden step-1 gate. A MISSING basis keeps the
    #     v1.0.0 contract unless block_missing_cost_basis is set.
    if controls.get("cost_basis_gate_enabled"):
        cb_ok, cb_reason = _cost_basis_block_reason(
            cand.get("avg_cost_raw"), cand.get("price"),
            cand.get("quantity"), controls)
        if not cb_ok:
            return (ACTION_BLOCK, cb_reason, 0.0, None)

    # 1c. BLOCK — data-trust (v1.0.3; Phase 0 parity with opportunity_builder
    #     v1.0.6). The SAME name excluded as a sparse/stale Top_10 candidate
    #     must not yield a confident action as a holding. Acting on a STALE
    #     price is acting blind, so stale ALWAYS withholds the action; THIN
    #     coverage withholds only when block_thin_coverage is set (default:
    #     flagged via the low_data_coverage alert, since the action is
    #     valuation-driven and the owner still needs guidance on a held
    #     position). Detail is computed once in normalize_holding.
    det = cand.get("_trust")
    if det:
        if det.get("stale"):
            age_h = det.get("age_hours")
            age_txt = "" if age_h is None else " (%dd old)" % int(age_h / 24.0)
            return (ACTION_BLOCK,
                    "BLOCKED — stale quote%s; action withheld pending price "
                    "refresh" % age_txt, 0.0, None)
        if det.get("thin") and controls.get("block_thin_coverage"):
            return (ACTION_BLOCK,
                    "BLOCKED — thin data (%d/6 core signals); action withheld"
                    % int(det.get("signals_present") or 0), 0.0, None)

    roi = cand.get("roi_pct")
    reco = cand.get("recommendation")

    # 3. EXIT
    exit_reason = None
    if roi is not None and roi <= controls["exit_roi_pct"]:
        exit_reason = ("Valuation ROI %.1f%% <= exit threshold %.1f%%"
                       % (roi, controls["exit_roi_pct"]))
    elif _is_sell_tier(reco):
        exit_reason = "Engine recommendation '%s' is sell-tier" % reco
    if exit_reason:
        if conf == "Low":  # 2. low-confidence cap
            return (ACTION_HOLD,
                    "Low confidence (reliability %s) capped EXIT -> HOLD: %s"
                    % (_fmt(rel), exit_reason), 0.0, ACTION_EXIT)
        return (ACTION_EXIT, exit_reason, mv or 0.0, None)

    # 4. TRIM — largest of position-cap / sector-cap / valuation trims
    trims = []
    cap_pct = controls["max_position_pct"]
    if weight_pct is not None and weight_pct > cap_pct and mv:
        target_mv = mv * (cap_pct / weight_pct)
        trims.append((mv - target_mv,
                      "Position %.1f%% > Max Position %.1f%% — trim to cap"
                      % (weight_pct, cap_pct)))
    if sector_excess_share_sar and sector_excess_share_sar > 0:
        trims.append((sector_excess_share_sar,
                      "Sector %.1f%% > Max Sector %.1f%% — pro-rata trim"
                      % (sector_weight_pct or 0.0,
                         controls["max_sector_pct"])))
    if (roi is not None and roi <= controls["trim_roi_pct"] and mv):
        trims.append((mv * controls["valuation_trim_frac"],
                      "Valuation ROI %.1f%% <= trim threshold %.1f%% — trim "
                      "%d%% of position"
                      % (roi, controls["trim_roi_pct"],
                         int(controls["valuation_trim_frac"] * 100))))
    if trims:
        trims.sort(key=lambda t: -t[0])
        proceeds, reason = trims[0]
        if conf == "Low":  # 2. low-confidence cap
            return (ACTION_HOLD,
                    "Low confidence (reliability %s) capped TRIM -> HOLD: %s"
                    % (_fmt(rel), reason), 0.0, ACTION_TRIM)
        return (ACTION_TRIM, reason, round(proceeds, 0), None)

    # 5. ADD qualification (sizing deferred to the funding pass)
    dq = cand.get("dq")
    add_ok = (rel is not None and rel >= controls["min_reliability_add"] and
              dq is not None and dq >= controls["min_dq_add"] and
              roi is not None and roi >= controls["add_roi_pct"] and
              cand.get("conflict") is not True)
    if add_ok:
        headroom = (weight_pct is None or
                    weight_pct < controls["max_position_pct"])
        sector_room = (sector_weight_pct is None or
                       sector_weight_pct < controls["max_sector_pct"])
        if headroom and sector_room:
            if conf == "Low":  # 2. low-confidence cap
                return (ACTION_HOLD,
                        "Low confidence (reliability %s) capped ADD -> HOLD"
                        % _fmt(rel), 0.0, ACTION_ADD)
            return (ACTION_ADD,
                    "Upside %.1f%% >= %.1f%%, reliability %s, DQ %s, "
                    "headroom available"
                    % (roi, controls["add_roi_pct"], _fmt(rel), _fmt(dq)),
                    0.0, None)
        return (ACTION_HOLD,
                "Qualified to add but no %s headroom"
                % ("position-cap" if not headroom else "sector-cap"),
                0.0, None)

    # 6. HOLD — name the first binding fact
    if roi is None:
        why = "No valuation reference (target/intrinsic) — upside unknown"
    elif roi < controls["add_roi_pct"]:
        why = ("Upside %.1f%% below add threshold %.1f%%; within all caps"
               % (roi, controls["add_roi_pct"]))
    elif rel is None or rel < controls["min_reliability_add"]:
        why = ("Reliability %s below add minimum %.0f"
               % (_fmt(rel), controls["min_reliability_add"]))
    elif dq is None or dq < controls["min_dq_add"]:
        why = ("Data quality %s below add minimum %.0f"
               % (_fmt(dq), controls["min_dq_add"]))
    elif cand.get("conflict") is True:
        why = "Provider/engine conflict flagged"
    else:
        why = "Within caps and thresholds"
    return (ACTION_HOLD, why, 0.0, None)


def _fmt(v):
    if v is None:
        return "?"
    try:
        f = float(v)
        return ("%d" % round(f)) if abs(f - round(f)) < 1e-9 else (
            "%.1f" % f)
    except (TypeError, ValueError):
        return str(v)


# ---------------------------------------------------------------------------
# Funding pass (L7) — sizes qualified ADDs against deployable capital
# ---------------------------------------------------------------------------

def fund_adds(entries, controls, cash_sar, total_value_sar):
    """entries: list of dicts {cand, action, ...} mutated in place.

    Returns (deployable, proceeds_included, adds_funded, cash_floor).
    """
    proceeds = 0.0
    for e in entries:
        if e["action"] in (ACTION_TRIM, ACTION_EXIT):
            proceeds += e["proceeds_sar"] or 0.0
    include_proceeds = controls["rebalance_mode"] != REBALANCE_NEW_CASH
    cash_floor = (controls["target_cash_pct"] / 100.0) * (
        total_value_sar or 0.0)
    deployable = max(0.0, (cash_sar or 0.0) - cash_floor)
    if include_proceeds:
        deployable += proceeds
    cash_left = max(0.0, (cash_sar or 0.0) - cash_floor)
    proceeds_left = proceeds if include_proceeds else 0.0

    adds = [e for e in entries if e["action"] == ACTION_ADD]
    adds.sort(key=lambda e: (-(e["cand"].get("ann_roi_pct") or 0.0),
                             -(e["cand"].get("reliability") or 0.0),
                             e["cand"].get("symbol") or ""))
    remaining = deployable
    funded_total = 0.0
    lot = max(1, int(controls["lot_size"]))
    for e in adds:
        cand = e["cand"]
        mv = cand.get("market_value_sar") or 0.0
        price_sar = (cand.get("price") or 0.0) * (cand.get("fx_to_sar") or 0.0)
        cap_room = max(0.0, (controls["max_position_pct"] / 100.0) *
                       (total_value_sar or 0.0) - mv)
        sector_room = e.get("_sector_room_sar")
        if sector_room is None:
            sector_room = float("inf")
        budget = min(cap_room, sector_room, remaining)
        shares = 0
        suggested = 0.0
        if price_sar > 0 and budget > 0:
            shares = int(budget // price_sar)
            shares = (shares // lot) * lot
            suggested = round(shares * price_sar, 0)
        if shares > 0 and suggested > 0:
            take_cash = min(suggested, cash_left)
            take_proc = min(suggested - take_cash, proceeds_left)
            cash_left -= take_cash
            proceeds_left -= take_proc
            remaining -= suggested
            funded_total += suggested
            if take_proc > 0 and take_cash > 0:
                ff = ("cash %d + proceeds %d SAR"
                      % (round(take_cash), round(take_proc)))
            elif take_proc > 0:
                ff = "TRIM/EXIT proceeds"
            else:
                ff = "cash"
            e["suggested_delta_sar"] = suggested
            e["suggested_delta_shares"] = shares
            e["funds_from"] = ff
        else:
            e["suggested_delta_sar"] = 0.0
            e["suggested_delta_shares"] = 0
            e["funds_from"] = None
            e["action_reason"] += " — no deployable capital to size the add"
    return deployable, (proceeds if include_proceeds else 0.0), \
        funded_total, cash_floor

# ---------------------------------------------------------------------------
# Payload assembly
# ---------------------------------------------------------------------------

def _round(v, nd=2):
    if v is None:
        return None
    try:
        f = float(v)
        if not math.isfinite(f):
            return None
        return round(f, nd)
    except (TypeError, ValueError):
        return None


def _advisor_sentence(entry, controls, review_date):
    cand = entry["cand"]
    act = entry["action"]
    conf = entry["confidence_band"]
    fx = cand.get("fx_to_sar") or 0.0
    bits = []
    if act == ACTION_ADD:
        sar = entry.get("suggested_delta_sar") or 0.0
        sh = entry.get("suggested_delta_shares") or 0
        if sh > 0:
            bits.append("ADD %d sh (~%s SAR, %s)"
                        % (sh, _fmt(round(sar)), entry.get("funds_from")))
        else:
            bits.append("ADD (unsized — no deployable capital)")
    elif act == ACTION_TRIM:
        bits.append("TRIM ~%s SAR" % _fmt(round(entry["proceeds_sar"] or 0)))
    elif act == ACTION_EXIT:
        bits.append("EXIT full position (~%s SAR)"
                    % _fmt(round(entry["proceeds_sar"] or 0)))
    elif act == ACTION_BLOCK:
        bits.append("BLOCKED")
    else:
        bits.append("HOLD")
    bits.append(entry["action_reason"])
    if act in (ACTION_ADD, ACTION_HOLD) and cand.get("stop") is not None:
        bits.append("stop %s / TP1 %s / TP2 %s SAR"
                    % (_fmt(_round((cand["stop"] or 0) * fx)),
                       _fmt(_round((cand["tp1"] or 0) * fx)),
                       _fmt(_round((cand["tp2"] or 0) * fx))))
    bits.append("confidence %s" % conf)
    bits.append("review by %s" % review_date)
    return "; ".join(bits) + "."


def _action_row(entry, review_date, controls):
    cand = entry["cand"]
    fx = cand.get("fx_to_sar")
    # v1.0.1: surface the engine 12M forecast alongside (never substituted into)
    # the valuation roi_pct. OFF => engine_pct stays None and every assignment
    # below is byte-identical v1.0.0.
    _eng_display = bool(controls.get("engine_roi_display_enabled"))
    engine_pct = (_engine_roi_to_pct(cand.get("engine_roi_12m_pct"))
                  if _eng_display else None)
    detail_engine_roi = _round(cand.get("engine_roi_12m_pct"), 1)
    note = _advisor_sentence(entry, controls, review_date)
    if _eng_display:
        detail_engine_roi = _round(engine_pct, 1)
        if engine_pct is None:
            note = note[:-1] + ("; engine 12M forecast: unavailable (ROI shown "
                                "is a valuation target, not a forecast).")
        else:
            note = note[:-1] + ("; engine 12M forecast %s%% (ROI shown is a "
                                "valuation target, not a forecast)."
                                % _fmt(_round(engine_pct, 1)))
    row = {
        "symbol": cand.get("symbol"),
        "name": cand.get("name"),
        "market": cand.get("market"),
        "sector": cand.get("sector"),
        "currency": cand.get("currency"),
        "fx_to_sar": _round(fx, 4),
        "quantity": cand.get("quantity"),
        "avg_cost": _round(cand.get("avg_cost")),
        "price": _round(cand.get("price")),
        "price_sar": _round((cand.get("price") or 0) * fx) if fx else None,
        "market_value_sar": _round(cand.get("market_value_sar"), 0),
        "cost_sar": _round(cand.get("cost_sar"), 0),
        "pnl_sar": _round(cand.get("pnl_sar"), 0),
        "pnl_pct": _round(cand.get("pnl_pct"), 1),
        "weight_pct": _round(entry.get("weight_pct"), 1),
        "action": entry["action"],
        "action_reason": entry["action_reason"],
        "confidence_band": entry["confidence_band"],
        "suggested_delta_sar": _round(entry.get("suggested_delta_sar"), 0),
        "suggested_delta_shares": entry.get("suggested_delta_shares"),
        "proceeds_sar": _round(entry.get("proceeds_sar"), 0),
        "funds_from": entry.get("funds_from"),
        "stop_sar": _round((cand.get("stop") or 0) * fx) if fx else None,
        "tp1_sar": _round((cand.get("tp1") or 0) * fx) if fx else None,
        "tp2_sar": _round((cand.get("tp2") or 0) * fx) if fx else None,
        "roi_pct": _round(cand.get("roi_pct"), 1),
        "ann_roi_pct": _round(cand.get("ann_roi_pct"), 1),
        "reliability": _round(cand.get("reliability"), 1),
        "dq": _round(cand.get("dq"), 1),
        "advisor_note": note,
        "detail": {
            "valuation_basis": cand.get("valuation_basis"),
            "target_price": _round(cand.get("target_price")),
            "intrinsic_value": _round(cand.get("intrinsic_value")),
            "engine_forecast_roi_pct": detail_engine_roi,
            "engine_recommendation": cand.get("recommendation"),
            "risk_level": cand.get("risk_level") or "Unknown",
            "news_trend": cand.get("news_trend"),
            "sector_trend": cand.get("sector_trend"),
            "sector_weight_pct": _round(entry.get("sector_weight_pct"), 1),
            "position_cap_pct": controls["max_position_pct"],
            "sector_cap_pct": controls["max_sector_pct"],
            "rr": _round(cand.get("rr")),
            "mos_pct": _round(cand.get("mos_pct"), 1),
            "stop_pct": _round(cand.get("stop_pct"), 1),
            "capped_from": entry.get("capped_from"),
            "review_date": review_date,
        },
    }
    if _eng_display:
        row["engine_roi_pct"] = _round(engine_pct, 1)
        row["valuation_roi_pct"] = _round(cand.get("roi_pct"), 1)
    return row


def _json_sanitize(obj):
    if isinstance(obj, dict):
        return {str(k): _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_sanitize(v) for v in obj]
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _skeleton(status, reason, controls=None):
    return {
        "version": PORTFOLIO_ACTIONS_VERSION,
        "status": status,
        "kpis": {
            "portfolio_value_sar": None, "holdings_value_sar": None,
            "cash_sar": None, "cash_pct": None, "cost_basis_sar": None,
            "pnl_sar": None, "pnl_pct": None, "deployable_sar": None,
            "proceeds_pending_sar": None, "adds_funded_sar": None,
            "capital_unallocated_sar": None, "positions": 0,
            "action_counts": {a: 0 for a in ACTIONS},
            "blended_reliability": None,
        },
        "actions": [],
        "sector_summary": [],
        "alerts": [],
        "meta": {
            "reason": reason,
            "controls_snapshot": controls or {},
            "versions": {
                "portfolio_actions": PORTFOLIO_ACTIONS_VERSION,
                "opportunity_builder": getattr(
                    _ob, "OPPORTUNITY_BUILDER_VERSION", None),
            },
            "generated_utc": datetime.now(timezone.utc).isoformat(),
        },
    }


def build_portfolio_actions(rows, controls=None, fx_rates=None,
                            upstream_meta=None):
    """Main entry point. rows = GAS holdings dicts (display headers ok)."""
    ctl = make_controls(controls)
    if not _env_enabled():
        return _json_sanitize(_skeleton("disabled", "TFB_PF_ENABLED=0", ctl))
    if _ob is None:
        return _json_sanitize(_skeleton(
            "unavailable",
            "opportunity_builder import failed: " + _OB_IMPORT_ERROR, ctl))
    try:
        return _json_sanitize(_build(rows or [], ctl, fx_rates or {},
                                     upstream_meta or {}))
    except Exception as exc:  # honest failure, never a fabricated page
        sk = _skeleton("error", "%s: %s" % (type(exc).__name__, exc), ctl)
        return _json_sanitize(sk)


def _build(rows, ctl, fx_rates, upstream_meta):
    if ctl["max_holdings"] and len(rows) > ctl["max_holdings"]:
        rows = rows[:ctl["max_holdings"]]
    cash = ctl["cash_available_sar"] or 0.0

    if not rows:
        sk = _skeleton("empty", "no holdings rows supplied", ctl)
        sk["kpis"]["cash_sar"] = _round(cash, 0)
        sk["kpis"]["portfolio_value_sar"] = _round(cash, 0)
        sk["kpis"]["cash_pct"] = 100.0 if cash > 0 else None
        sk["meta"]["controls_snapshot"] = ctl
        return sk

    # Pass 1 — normalize + market values + totals
    cands = [normalize_holding(r, fx_rates, ctl) for r in rows]
    holdings_value = sum(c["market_value_sar"] or 0.0 for c in cands)
    total_value = holdings_value + cash

    # Sector aggregation for caps (valued holdings only)
    sector_value = {}
    for c in cands:
        if c["market_value_sar"]:
            sec = c.get("sector") or "Unknown"
            sector_value[sec] = sector_value.get(sec, 0.0) + \
                c["market_value_sar"]
    sector_excess = {}
    for sec, val in sector_value.items():
        cap_val = (ctl["max_sector_pct"] / 100.0) * total_value
        sector_excess[sec] = max(0.0, val - cap_val)

    # Pass 2 — actions
    entries = []
    for c in cands:
        mv = c["market_value_sar"]
        weight = (mv / total_value * 100.0) if (mv and total_value) else None
        sec = c.get("sector") or "Unknown"
        sec_val = sector_value.get(sec, 0.0)
        sec_weight = (sec_val / total_value * 100.0) if total_value else None
        excess_share = 0.0
        if mv and sector_excess.get(sec, 0.0) > 0 and sec_val > 0:
            excess_share = sector_excess[sec] * (mv / sec_val)
        action, reason, proceeds, capped_from = decide_action(
            c, ctl, weight, sec_weight, excess_share)
        sec_room = None
        if total_value:
            sec_room = max(0.0, (ctl["max_sector_pct"] / 100.0) *
                           total_value - sec_val)
        entries.append({
            "cand": c, "action": action, "action_reason": reason,
            "proceeds_sar": proceeds, "capped_from": capped_from,
            "weight_pct": weight, "sector_weight_pct": sec_weight,
            "confidence_band": _ob.confidence_band(c.get("reliability")),
            "suggested_delta_sar": None, "suggested_delta_shares": None,
            "funds_from": None, "_sector_room_sar": sec_room,
        })

    # v1.0.2: a cost-basis BLOCK rejects the basis as untrusted — null its
    # cost / P&L so neither the row (_action_row) nor the portfolio KPIs
    # aggregate a figure derived from a basis we just rejected. Market value is
    # price-based and stays. Mirrors the honest handling of a missing basis.
    for e in entries:
        if _is_cost_basis_block(e["action"], e["action_reason"]):
            cb = e["cand"]
            cb["cost_sar"] = None
            cb["pnl_sar"] = None
            cb["pnl_pct"] = None

    # Pass 3 — L7 funding
    deployable, proceeds_inc, adds_funded, cash_floor = fund_adds(
        entries, ctl, cash, total_value)

    review_date = (datetime.now(timezone.utc) +
                   timedelta(days=ctl["review_days"])).date().isoformat()
    order = {ACTION_EXIT: 0, ACTION_TRIM: 1, ACTION_ADD: 2, ACTION_BLOCK: 3,
             ACTION_HOLD: 4}
    entries.sort(key=lambda e: (order.get(e["action"], 9),
                                -(e["cand"].get("market_value_sar") or 0.0)))
    action_rows = [_action_row(e, review_date, ctl) for e in entries]

    counts = {a: 0 for a in ACTIONS}
    for e in entries:
        counts[e["action"]] += 1

    cost_total = sum(c["cost_sar"] or 0.0 for c in cands
                     if c["cost_sar"] is not None)
    pnl_known = [c for c in cands if c.get("pnl_sar") is not None]
    pnl_total = sum(c["pnl_sar"] for c in pnl_known) if pnl_known else None
    pnl_pct = (pnl_total / cost_total * 100.0) if (pnl_total is not None and
                                                   cost_total) else None
    rel_pairs = [(c["market_value_sar"], c.get("reliability"))
                 for c in cands
                 if c.get("market_value_sar") and c.get("reliability")
                 is not None]
    blended_rel = None
    if rel_pairs:
        wsum = sum(p[0] for p in rel_pairs)
        if wsum:
            blended_rel = sum(p[0] * p[1] for p in rel_pairs) / wsum

    # Sector summary
    sector_summary = []
    for sec in sorted(sector_value, key=lambda s: -sector_value[s]):
        val = sector_value[sec]
        wpct = val / total_value * 100.0 if total_value else None
        sector_summary.append({
            "sector": sec, "value_sar": _round(val, 0),
            "weight_pct": _round(wpct, 1),
            "cap_pct": ctl["max_sector_pct"],
            "over_cap": bool(sector_excess.get(sec, 0.0) > 0),
            "positions": sum(1 for c in cands
                             if (c.get("sector") or "Unknown") == sec),
        })

    # Alerts
    alerts = []

    def _alert(atype, count, action_text):
        if count:
            alerts.append({"type": atype, "count": count,
                           "required_action": action_text})

    # v1.0.2: split BLOCK alerts so the required action is accurate. Cost-basis
    # blocks get their own alert; presence blocks keep theirs; missing_cost_
    # basis excludes cost-basis blocks (whose cost was just nulled) so a row is
    # never flagged twice.
    cb_blocked = sum(1 for e in entries
                     if _is_cost_basis_block(e["action"], e["action_reason"]))
    # v1.0.3: data-trust block/flag counts (keyed off reason markers + the
    # shared trust detail). Trust blocks are excluded from blocked_positions so
    # each row carries exactly one accurate remediation.
    trust_blocked = sum(1 for e in entries
                        if _is_trust_block(e["action"], e["action_reason"]))
    stale_blocked = sum(1 for e in entries
                        if _is_stale_block(e["action"], e["action_reason"]))
    thin_flagged = sum(1 for e in entries
                       if (e["cand"].get("_trust") or {}).get("thin"))
    _alert("blocked_positions", counts[ACTION_BLOCK] - cb_blocked - trust_blocked,
           "Fix missing price/FX/quantity for blocked rows")
    _alert("cost_basis_blocked", cb_blocked,
           "Correct the Buy Price on flagged holdings — implausible vs the "
           "current price")
    _alert("stale_quote_blocked", stale_blocked,
           "Refresh the quote on stale holdings — action withheld until the "
           "price updates")
    _alert("low_data_coverage", thin_flagged,
           "Limited secondary data on these holdings — verify before acting")
    _alert("missing_cost_basis",
           sum(1 for e in entries
               if e["cand"].get("cost_sar") is None and
               e["cand"].get("market_value_sar") is not None and
               not _is_cost_basis_block(e["action"], e["action_reason"])),
           "Fill Buy Price in _Portfolio_CostBasis for accurate P&L")
    _alert("over_position_cap",
           sum(1 for e in entries
               if e["action"] == ACTION_TRIM and
               "Position" in e["action_reason"]),
           "Review position-cap trims")
    _alert("over_sector_cap",
           sum(1 for s in sector_summary if s["over_cap"]),
           "Review sector-cap trims")
    _alert("low_confidence_capped",
           sum(1 for e in entries if e.get("capped_from")),
           "Improve data reliability; actions were capped to HOLD")
    if (any(e["action"] == ACTION_ADD for e in entries) and
            adds_funded <= 0):
        _alert("no_deployable_capital", 1,
               "Qualified ADDs are unsized — raise cash or lower Target "
               "Cash %")
    unalloc = max(0.0, deployable - adds_funded)

    payload = {
        "version": PORTFOLIO_ACTIONS_VERSION,
        "status": "ok",
        "kpis": {
            "portfolio_value_sar": _round(total_value, 0),
            "holdings_value_sar": _round(holdings_value, 0),
            "cash_sar": _round(cash, 0),
            "cash_pct": _round(cash / total_value * 100.0, 1)
            if total_value else None,
            "cost_basis_sar": _round(cost_total, 0) if cost_total else None,
            "pnl_sar": _round(pnl_total, 0),
            "pnl_pct": _round(pnl_pct, 1),
            "deployable_sar": _round(deployable, 0),
            "proceeds_pending_sar": _round(proceeds_inc, 0),
            "adds_funded_sar": _round(adds_funded, 0),
            "capital_unallocated_sar": _round(unalloc, 0),
            "positions": len(cands),
            "action_counts": counts,
            "blended_reliability": _round(blended_rel, 1),
        },
        "actions": action_rows,
        "sector_summary": sector_summary,
        "alerts": alerts,
        "meta": {
            "controls_snapshot": ctl,
            "cash_floor_sar": _round(cash_floor, 0),
            "fx": {"provided": sorted(fx_rates.keys()),
                   "static_fallback_used": sorted(
                       {c.get("currency") for c in cands
                        if c.get("fx_source") == "static"} - {None})},
            "versions": {
                "portfolio_actions": PORTFOLIO_ACTIONS_VERSION,
                "opportunity_builder": getattr(
                    _ob, "OPPORTUNITY_BUILDER_VERSION", None),
                "opportunity_builder_floor_ok": _ob_version_ok(),
            },
            "trust_gate": {
                "enabled": bool(ctl.get("trust_gate_enabled")),
                "available": getattr(
                    _ob, "_data_trust_assessment", None) is not None,
                "assessed": sum(1 for c in cands
                                if c.get("_trust") is not None),
                "stale_blocked": stale_blocked,
                "thin_flagged": thin_flagged,
                "thin_blocked": trust_blocked - stale_blocked,
                "max_data_age_hours": ctl.get("max_data_age_hours"),
                "min_trust_fields": ctl.get("min_trust_fields"),
                "block_thin_coverage": bool(ctl.get("block_thin_coverage")),
            },
            "counts": {"rows_in": len(rows), "normalized": len(cands)},
            "upstream": upstream_meta or {},
            "generated_utc": datetime.now(timezone.utc).isoformat(),
        },
    }
    return payload


__all__ = [
    "PORTFOLIO_ACTIONS_VERSION", "ACTIONS", "DEFAULT_CONTROLS",
    "make_controls", "normalize_holding", "decide_action", "fund_adds",
    "build_portfolio_actions",
]
