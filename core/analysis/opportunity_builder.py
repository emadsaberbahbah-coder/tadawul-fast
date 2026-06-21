# -*- coding: utf-8 -*-
"""
core/analysis/opportunity_builder.py — Opportunity Engine for Top_10_Investments
Version: 1.0.9   (TFB Final Execution Plan v5.0 — Phase P2;
                 Engineering Audit Phase 1 — unfunded-ticket reclass + optional
                 engine-ROI ordering, both env-gated DEFAULT-OFF)

v1.0.9 [UNFUNDED-WATCH + ENGINE-ROI-ORDERING — two Phase-1 corrections, each
env-gated and DEFAULT OFF; OFF => byte-identical v1.0.8 selection, sizing,
ranking, KPIs, near-miss, alerts, and verdict<->gate-trace contract. Every
v1.0.8 byte carried forward verbatim].

(1) UNFUNDED-WATCH (TFB_OPP_UNFUNDED_WATCH / criteria unfunded_watch_enabled,
default OFF). The greedy §4.4 sizer funds picks top-down until deployable
capital is exhausted; a pick reached after exhaustion is still appended to the
selected list with suggested_sar = 0 and rendered as an INVEST ticket showing
"no size (no deployable capital)". The live 2026-06-21 build did exactly this:
SAN.PA and MDT were counted in "Selected 10/10" while sized at 0 SAR — the
headline over-counted executable tickets, and a 0-SAR row advertised INVEST.
The picks are NOT wrong (they passed every gate and ranked); they simply were
not funded. FIX (default OFF): when ON, a 0-SAR pick is NOT an executable
ticket — it is removed from `selected` (so selected_count and the SELECTED
section header count only funded tickets), tagged in the audit grid with an
"Unfunded — cash exhausted" deferral (selected = No), surfaced as a WATCH
near-miss under a "Funding" gate with an explicit how-to-fund note, and counted
in a new unfunded_candidates alert. The L7 funding identity is preserved
(unfunded picks contributed 0 to Σ suggested and Σ gain, so deployable −
Σ suggested and kpi gain == Σ ticket gains are unchanged); only the count and
the row's classification move. This is the honest reading: an unfunded name is
a WATCH idea, not an executed position.

(2) ENGINE-ROI-ORDERING (TFB_OPP_RANK_BY_ENGINE_ROI / criteria
rank_by_engine_roi_enabled, default OFF). The displayed ticket "ROI %" is a
valuation TARGET capped at max_valuation_roi_pct (so it clusters near the cap,
e.g. ~35%), while the engine's own 12M forecast (engine_roi_12m_pct, surfaced
since v1.0.5 when TFB_OPP_ENGINE_ROI_DISPLAY=1) carries the real differentiation
(15.8–30.4% across the live picks). The selection pool, however, was ordered by
opportunity_score alone. FIX (default OFF): when ON, the INVEST pool is ordered
by the normalized engine forecast (desc) as the PRIMARY key, with
opportunity_score / ann_roi / symbol as tie-breakers, so the names the engine
forecasts highest are funded first. A missing/unparseable forecast sorts last
(never invents a rank). This is a DISCLOSED, reversible ordering policy — it
changes which names are funded first, not any gate or verdict; OFF restores the
exact v1.0.8 opportunity_score-primary order. Recommend enabling only alongside
TFB_OPP_ENGINE_ROI_DISPLAY so the basis for the order is visible on the page.

v1.0.8 [INVESTABILITY-GATE DEFAULT-OFF — live-evidence correction]: a live
Top_10 build (2026-06-18, selector v4.19.0 + engine v5.91.0) disproved the
v1.0.7 premise. v1.0.7 assumed the Investability gate would be a NO-OP in the
normal path because rows reaching the builder are already INVESTABLE. They are
NOT: top10_selector deliberately TIER-2 BACKFILLS — when fewer than `limit`
INVESTABLE names clear Tier-1 (it found only 5 of 10), it fills the remainder
with its best WATCHLIST / low-reliability rows, each carrying an explicit tier
label. The live run selected 5 Tier-2 names into the Top 10 (4 WATCHLIST:
NVDA / CNX / NEM / VRTX at reliability 70-75, plus TCOM as a low-reliability
INVESTABLE). The selector OWNS the Top_10_Investments page; the builder feeds a
SEPARATE opportunity-candidates surface. So a default-ON Investability gate
would MAJOR-fail those 4 WATCHLIST names out of the opportunity surface while
they remain on the Top_10 page — an un-asked-for divergence between two views.
DECISION: ship the gate DEFAULT OFF (TFB_OPP_INVESTABILITY_GATE / criteria
investability_gate_enabled both default off). It is now an explicit OPT-IN for
an operator who wants strict INVESTABLE-only executable tickets; with it off the
gate list and verdict are byte-identical to v1.0.6. The v1.0.7 GATE_ORDER fix
(Valuation Sanity + Investability placed at their true positions) is RETAINED
and active — pure ordering metadata, no behavior change. Also bumps
OPPORTUNITY_BUILDER_VERSION, which v1.0.7 left at "1.0.6" by oversight (the
header said 1.0.7 but the reported constant did not). The honest framing holds:
this gate is a DISCLOSED, reversible policy choice, not a silent override of the
selector's deliberate full-list design.

v1.0.7 [INVESTABILITY-GATE + GATE_ORDER-FIX]: two Phase-0 corrections.
(1) INVESTABILITY GATE — the engine's authoritative verdict
(investability_status in INVESTABLE / WATCHLIST / BLOCKED) was captured into
cand.engine_gate.investability (v1.0.6 itself noted "engine_gate was carried
and ignored") but no gate ever enforced it: the builder re-derived its own
truth-table and could therefore SELECT a name the engine had benched to
WATCHLIST or BLOCKED whenever the builder's independent gates happened to pass.
In the live engine Top_10 path data_engine_v2._top10_row_is_eligible already
requires INVESTABLE, so the gap is COVERED there and latent — but the
engine<->builder verdict contract was implicit, and any path that feeds the
builder an unfiltered candidate set (a broader selector ingest, a manual call)
had no backstop. FIX (default ON; mirrors the v1.0.3/v1.0.4/v1.0.6 local-gate
pattern — no engine change, no edit to L2/L5/L6/L7, scoring, sizing, or the
verdict<->gate-trace contract): a new "Investability" gate fails MAJOR (=>
DO_NOT_INVEST; the name still shows in the audit grid / near-miss but is NEVER a
selected ticket) when the engine verdict is WATCHLIST or BLOCKED. INVESTABLE, or
a blank/unrecognized token, PASSES (fail-open + traced, so a row that simply
never carried the field is never penalized). Appended ONLY when
investability_gate_enabled, so the gate list and verdict are byte-identical to
v1.0.6 when TFB_OPP_INVESTABILITY_GATE=0. [v1.0.8 CORRECTION: the v1.0.7 claim
here that this gate is a NO-OP "in the normal pre-filtered engine path" is FALSE
— a live build proved the selector backfills Tier-2 (WATCHLIST / low-reliability)
rows, so the gate WOULD drop real selections. v1.0.8 therefore ships it DEFAULT
OFF; see the v1.0.8 note above.]
This also makes the engine's v5.91.0 reliability-calibration flow coherent end
to end: the calibrated forecast_reliability_score the builder reads (the tiered
Reliability gate >=70 pass / >=Min-15 WATCH / below MAJOR, the confidence band,
and the score's reliability component) and this verdict gate now agree on the
same engine source of truth.
(2) GATE_ORDER FIX — the v1.0.4 "Valuation Sanity" gate is appended in
evaluate_gates but was never added to GATE_ORDER, so first_failed_gate fell back
to sort-order 99 for it and could mis-attribute the near-miss "failed_gate" when
a row failed Valuation Sanity alongside a later gate. "Valuation Sanity" (and
the new "Investability") now sit in GATE_ORDER at their true append positions,
so near-miss attribution is correct. Pure ordering metadata — no verdict, gate,
score, or sizing change. Every v1.0.6 byte carried forward verbatim.


each row's investability verdict + last_updated into cand.engine_gate) but
that detection NEVER gated selection — engine_gate was carried and ignored,
and there was no freshness or coverage check anywhere. The hard fields each
have their own MAJOR gate (Price/FX/Valuation/ROI/DQ/R-R/Reliability), so a
name that RANKS despite being sparse has those filled while the SECONDARY
signals (risk_level, vol_30d, avg-volume/liquidity, recommendation, news/sector
trend) are all Unknown — and the gates let Unknown pass by design ("until P9").
5023.SR rode that path: warned upstream, then ranked, then held. Detection
existed; it just never became a decision (Engineering Audit, Phase 0). FIX
(default ON; mirrors the v1.0.3/v1.0.4 local-gate pattern — no engine change,
no edit to L2/L5/L6/L7, scoring, sizing, or the verdict↔gate-trace contract):
a new "Data Trust" gate fails MAJOR (=> DO_NOT_INVEST, so the name still
appears in the audit grid / near-miss but is NEVER a selected ticket) when a
candidate is STALE — last_updated older than max_data_age_hours (default 168h;
an UNPARSEABLE/absent timestamp is never treated as stale, so freshness only
fails on PROVEN staleness) — OR THINLY COVERED — fewer than min_trust_fields
(default 2, deliberately conservative) of the six secondary signals present.
The gate is appended ONLY when trust_gate_enabled, so the gate list and verdict
are byte-identical to v1.0.5 when TFB_OPP_TRUST_GATE=0 (only the version stamp
moves). Per-run telemetry (evaluated / blocked / blocked_stale / blocked_thin)
is surfaced in meta.trust_gate and each gate carries trust_detail, so the
exclusion's effect is measurable run-over-run and the coverage bar can be tuned
from data rather than guessed. Forward-compatible: when data_engine_v2 later
emits an explicit trust_level, this same gate absorbs it with no restructuring.
Every v1.0.5 byte carried forward verbatim.

v1.0.5 [ENGINE-ROI-DISPLAY]: the executable ticket "ROI %" and "Exp Gain 12M"
are PURE VALUATION upside — roi_pct = (ref - price)/price — while the engine's
own 12-month forecast (engine_roi_12m_pct) was extracted and carried only in
detail.engine_forecast_roi_pct and shown NOWHERE on the page. A selected
ticket therefore advertised, e.g., 35% upside / a large SAR gain while the
engine forecast for that name was ~0%. The picks are NOT traps (the v1.0.3
Forecast gate already blocks engine-negative names from selection), but the
HEADLINE number overstates expected return vs the engine's own view and hides
the spread. FIX (env-gated, default OFF; no change to selection, ranking,
gates, sizing, or the funding/identity contract): when ON, every ticket gains
the normalized engine forecast (engine_roi_pct), an engine-based expected gain
(engine_exp_gain_12m_sar), and the valuation figures under explicit names
(valuation_roi_pct, valuation_exp_gain_12m_sar); the advisor note states the
engine 12M forecast and frames the displayed upside as a TARGET, not a
forecast; detail.engine_forecast_roi_pct is normalized to percent (the raw
field carried a ratio for ratio-form providers); and kpis gains a parallel
engine_expected_gain_12m_sar. The existing rendered roi_pct / ann_roi_pct /
exp_gain_12m_sar and the kpis.expected_gain_12m_sar are LEFT INTACT so the
"KPI gain == Σ ticket gains" reproducibility identity is preserved and the
ROI%/Gain columns stay internally consistent; the engine figures are additive
for the audit/API and for an optional dedicated "Engine ROI %" sheet column.
Toggle TFB_OPP_ENGINE_ROI_DISPLAY=1 to enable; OFF restores byte-identical
v1.0.4 behavior (only the version stamp changes). Every v1.0.4 byte carried
forward verbatim.

v1.0.2 [CONFLICT-PARSE FIX]: `_norm_conflict`'s free-text fallback matched
"conflict" anywhere in the string, so descriptive negations — "No conflict",
"no provider/engine conflict", "No Conflict Detected" — were read as conflict
PRESENT and MAJOR-failed the Conflict gate (wrong DO_NOT_INVEST). Clean
"Yes"/"No" already parsed correctly, so the live canonical's boolean
`provider_engine_conflict` was unaffected; this hardens the path for any row
that carries descriptive conflict text (e.g. an aliased `conflict_type`).
Negation is now detected on the spaced original text so a genuine "notable
conflict" still scores as a conflict. Bugfix, not env-gated (mirrors the GBp
/100 precedent); only `_norm_conflict` changed.

v1.0.1 [ALIAS-FIX, P3 integration finding]: the live 115-key canonical
(schema_registry v2.13.0 / route v4.6.0) emits `forecast_reliability_score`,
`recommendation_detailed`, and `block_reason`; the v1.0.0 alias map missed
those exact compact forms, so live selector rows would have shown reliability
as missing and MAJOR-failed every candidate. Three aliases added; no other
change.

WHY THIS MODULE EXISTS
----------------------
Plan v5.0 §4.2–§4.4, §5 (rulings L2, L5–L8, L13): the Top_10_Investments page
stops being a ranked list and becomes sized, funded, executable trade tickets
in SAR. This module is the intelligence layer between selector/engine rows and
the page: it applies the hard-gate truth table (§4.2), computes the 0–100
Opportunity Score (§4.3), performs the wealth math (§4.4, L5/L6/L7 — stops,
targets, R/R, SAR sizing against Deployable Capital), and emits the FROZEN
zone payload of §5 for `/opportunity-candidates` (P3) plus the full audit grid
for `_Opportunity_Candidates`.

DESIGN RULES HONORED
--------------------
* L2  — Selected ⇔ INVEST, capped at Max Selected; no forced fill. Empty
        Selected zone is a CORRECT output (L13) — kpis/near-miss still render.
* L5  — Valuation ROI = (Target/Fair − Price)/Price; target_price preferred,
        intrinsic_value fallback. Engine forecast ROI carried alongside in
        detail.engine_forecast_roi_pct (never substituted).
* L6  — Stop = Price×(1−max(8%, 2.5×monthlyized vol_30d)) clamped ≤ 35%;
        TP1 = Price+0.5×(Ref−Price); TP2 = Ref. Missing FX ⇒ FX gate MAJOR
        fail (never a silent 1.0). Subunits GBp/GBX/ZAC/ILA = parent/100.
        News/Sector trend "Unknown" passes the gates (scores 40) until P9.
* L7  — Deployable = cash_available + pending TRIM/EXIT proceeds; Σ suggested
        ≤ Deployable; every ticket names funds_from (cash vs proceeds split).
* L8  — advisor_note is one sentence: ACTION + SIZE + LEVELS + REASON +
        CONFIDENCE + REVIEW DATE. Confidence: High ≥75 reliability, Medium
        60–74, Low <60; Low caps the verdict at WATCH.
* L13 — Honesty over fullness: fewer-than-max selections, empty zones, and
        Unknown values are correct; nothing is upgraded to fill space.

VERDICT ↔ GATE-TRACE CONTRACT (sign-off #19/§8): every candidate's verdict is
reproducible from its own `gates` list via `derive_verdict(gates, reliability)`
— the same pure function the builder uses. Diversification deferrals and the
Max Selected cap NEVER change a verdict; they only block selection and are
recorded separately in `deferral` / near-miss `failed_gate`.

SCORE NORMALIZATION CONSTANTS (v1.0.0 — §4.3 curves not fixed by the plan are
pinned here, in one block, so the external auditor can tune them):
  AnnROI   : linear 0% → 0 pts, ≥40% annualized → 100 pts
  R/R      : linear 0 → 0 pts, ≥4.0 → 100 pts
  MoS      : (IV−P)/IV floored at 0; linear, ≥50% → 100 pts
  Liquidity: avg daily traded value in SAR; ≥5,000,000 → 100; missing → 40
  Diversification benefit: sector absent from portfolio+selection → 100;
             present below cap → 60; at/over cap → 0; no context → 60
  Trend map: Positive 100 / Neutral 60 / Unknown 40 / Negative 0  (plan-fixed)
  Risk map : Low 100 / Medium 70 / High 40                        (plan-fixed)

ENV KILL-SWITCHES (policy block — read per call, not at import)
  TFB_OPP_ENABLED          "1"   "0" ⇒ build returns status="disabled" skeleton
  TFB_OPP_NEAR_MISS_N      "10"  near-miss rows
  TFB_OPP_MAX_WEIGHT_PCT   "15"  per-ticket cap, % of (portfolio+deployable)
  TFB_OPP_LOT_SIZE         "1"   share lot rounding
  TFB_OPP_STOP_FLOOR_PCT   "8"   minimum stop distance %
  TFB_OPP_STOP_VOL_MULT    "2.5" multiplier on monthlyized vol_30d
  TFB_OPP_STOP_MAX_PCT     "35"  stop distance clamp %
  TFB_OPP_REVIEW_DAYS      "30"  review-by horizon for advisor sentence
  TFB_OPP_MAX_CANDIDATES   "0"   0 = unlimited; CPU safety clamp on input rows
  TFB_OPP_PF_MAX_SECTOR_PCT "30" post-action portfolio sector cap (§4.2)
  TFB_OPP_TRUST_GATE       "1"   v1.0.6: "0" ⇒ skip the Data Trust MAJOR gate
  TFB_OPP_MAX_DATA_AGE_HOURS "168" v1.0.6: last_updated older ⇒ stale ⇒ fail
  TFB_OPP_MIN_TRUST_FIELDS "2"   v1.0.6: fewer core signals present ⇒ thin ⇒ fail
  TFB_OPP_INVESTABILITY_GATE "0" v1.0.8: "1" ⇒ add Investability MAJOR gate (opt-in)
Explicit `criteria` overrides > env > defaults.

INTEGRATION
-----------
Pure-compute, stdlib-only; no provider or selector import at module load.
P3 (`routes/advanced_analysis.py`) feeds rows from top10_selector v4.19.0's
ingest and FX rates from `_Lists_Config`, then calls
`build_opportunity_payload(rows, criteria=…, portfolio=…, fx_rates=…,
upstream_meta=…)`. `collect_candidates_via_selector()` is a best-effort
convenience hook only — the authoritative wiring lands in P3 after the live
selector exports are confirmed on Render (never trust /mnt/project copies).

Volatility note: `vol_30d` is consumed as the 30-day realized volatility in
percent (≈ monthly), matching the engine's "Volatility 30D" column; it is NOT
de-annualized here. If the live engine emits annualized vol, recalibrate
TFB_OPP_STOP_VOL_MULT rather than editing formulas.
"""

from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timedelta, timezone

OPPORTUNITY_BUILDER_VERSION = "1.0.9"

# ---------------------------------------------------------------------------
# v1.0.5 [ENGINE-ROI-DISPLAY] — surface the engine forecast (env-gated, OFF)
# ---------------------------------------------------------------------------
# ROOT CAUSE (live 2026-06-15): the ticket "ROI %"/"Ann ROI %" and the derived
# "Exp Gain 12M" are pure VALUATION upside (roi_pct = (ref - price)/price), and
# the engine's own 12-month forecast (engine_roi_12m_pct) — already extracted
# and carried in detail.engine_forecast_roi_pct since v1.0.3 — is rendered on
# the page NOWHERE. A selected ticket can show 35% upside / a large SAR gain
# while the engine forecasts ~0% for the same name. The Forecast gate (v1.0.3)
# already prevents engine-NEGATIVE names from being selected, so these are not
# value traps; the defect is purely that the HEADLINE number overstates
# expected return vs the engine's view and the spread is invisible.
#
# FIX (preserves L5/L6/L7 and the funding/identity contract; no change to
# selection, ranking, gates, or sizing): when the flag is ON, each ticket gains
# engine_roi_pct (normalized %), engine_exp_gain_12m_sar (suggested ×
# engine_roi_pct), and the valuation figures under explicit names
# (valuation_roi_pct, valuation_exp_gain_12m_sar); the advisor note states the
# engine 12M forecast and frames the displayed upside as a TARGET not a
# forecast; detail.engine_forecast_roi_pct is normalized to percent; and kpis
# gains a parallel engine_expected_gain_12m_sar. The existing rendered roi_pct /
# ann_roi_pct / exp_gain_12m_sar and kpis.expected_gain_12m_sar are LEFT INTACT
# (the "kpi gain == Σ ticket gains" identity holds; ROI%/Gain stay consistent),
# so the engine figures are additive for the audit/API and for an optional
# dedicated "Engine ROI %" sheet column. Env-toggled by
# TFB_OPP_ENGINE_ROI_DISPLAY (default OFF) — set it to 1 to enable; OFF keeps
# byte-identical v1.0.4 behavior (only the version stamp moves). Every v1.0.4
# byte is carried forward verbatim.

# ---------------------------------------------------------------------------
# v1.0.3 [FORECAST-GATE] — engine-forecast safety gate (env-gated, default ON)
# ---------------------------------------------------------------------------
# ROOT CAUSE (live 2026-06-13): a name can screen as a top INVEST ticket on
# pure VALUATION upside — roi_pct = (target/intrinsic - price)/price — while
# the engine's own 12-month forecast points DOWN. 4321.SR ranked #1 with a
# "136% ROI" tag (intrinsic ~136% above price) even though Expected ROI 12M
# was -20% and the source page rated it SELL. The engine forecast WAS
# extracted (engine_roi_12m_pct) and carried in detail, but used in NO gate,
# so a value-trap could become a sized BUY.
#
# FIX (preserves L5 — valuation stays the roi_pct basis; the engine forecast
# is NOT substituted into roi_pct): a new "Forecast" gate fails MAJOR (=>
# DO_NOT_INVEST, so the name appears in the audit / near-miss but never as a
# selected ticket) when the engine 12M forecast ROI is below
# min_engine_roi_pct. Default floor 0% blocks only names the engine forecasts
# to LOSE money; Unknown forecast passes (News/Sector convention). The gate is
# env-toggled by TFB_OPP_FORECAST_GATE (default ON) — set it to 0 to restore
# byte-identical v1.0.2 behavior — and the floor is tunable via
# TFB_OPP_MIN_ENGINE_ROI_PCT. Every other byte is carried forward verbatim.

# ---------------------------------------------------------------------------
# v1.0.4 [VALUATION-SANITY-GATE] — implausible-upside guard (env-gated, ON)
# ---------------------------------------------------------------------------
# ROOT CAUSE (live 2026-06-14): the ticket "ROI %" is pure valuation upside,
# roi_pct = (ref - price)/price * 100 with ref = target_price (preferred) else
# intrinsic_value. Both refs are produced upstream by the engine and are, for
# a large swath of global names, systematically inflated: the engine's
# intrinsic-value model is calibrated to permit fair value up to 3x price (its
# _INTRINSIC_UPSIDE_MAX_PCT = 200) and estimates fair value from sector-average
# P/E and P/B, so any name trading below its sector multiple screens as deeply
# undervalued — hundreds of ordinary large-caps (VZ, HPQ, MKC, ...) show ~100%
# "upside", and a separate analyst-target cluster sits at exactly 3x price
# (200%). COL.MC surfaced as a 109.8% ticket (intrinsic 11.98 vs price 5.71).
# The builder faithfully renders (ref-price)/price, so the garbage upstream
# valuation flows straight onto the decision tickets.
#
# FIX (no change to the LOCKED engine; preserves L5 — valuation is still the
# roi_pct basis and roi_pct itself is left intact for the audit grid): a new
# "Valuation Sanity" gate fails MAJOR (=> DO_NOT_INVEST, so the name appears in
# the audit / near-miss but is NEVER a selected ticket) when roi_pct exceeds
# max_valuation_roi_pct. This catches BOTH inflated sources (intrinsic-based
# and target-based) at the point the ticket is built. Default ceiling 80%
# removes the implausible cluster while sparing genuine high-conviction value;
# env-toggled by TFB_OPP_VALUATION_SANITY_GATE (default ON) — set it to 0 to
# restore byte-identical v1.0.3 behavior — and the ceiling is tunable via
# TFB_OPP_MAX_VALUATION_ROI_PCT. Every v1.0.3 byte is carried forward verbatim.
# NOTE: this fixes the Top_10 decision page only; the same upstream inflation
# also affects the engine's valuation/ROI columns on the market pages, which is
# a separate (engine-side) change.

# ---------------------------------------------------------------------------
# v1.0.6 [DATA-TRUST-GATE] — sparse/stale exclusion (env-gated, default ON)
# ---------------------------------------------------------------------------
# ROOT CAUSE (live 2026-06-15): normalize_candidate already CAPTURES the
# engine's own trust signals — investability verdict, block reasons, provider,
# last_updated — into cand["engine_gate"], but evaluate_gates reads NONE of
# them, and there is no freshness or coverage check at all. Each hard field has
# its own MAJOR gate, so any name that survives to RANK has price / fx /
# valuation / dq / r-r / reliability filled; what's left thin is the SECONDARY
# signal set (risk_level, vol_30d, avg-volume→liquidity, recommendation,
# news_trend, sector_trend), and the gates pass Unknown by design until P9. A
# genuinely sparse row (5023.SR) therefore screens through: the engine warned
# on it, the builder ranked it anyway, and the portfolio held it. The detection
# was there; it never escalated into a selection decision.
#
# FIX (mirrors the v1.0.3 Forecast and v1.0.4 Valuation-Sanity gates — a local,
# env-gated MAJOR gate; the LOCKED engine is untouched and L2/L5/L6/L7, the
# score, the sizing, and the verdict↔gate-trace contract are all preserved): a
# "Data Trust" gate fails MAJOR (=> DO_NOT_INVEST; the name shows in the audit
# grid / near-miss but is NEVER selected) when the candidate is STALE
# (engine_gate.last_updated older than max_data_age_hours — an unparseable or
# absent timestamp is NOT treated as stale, so freshness fails only on PROVEN
# staleness) OR THINLY COVERED (fewer than min_trust_fields of the six secondary
# signals present). Defaults: 168h staleness (a wide net that spares weekend /
# holiday gaps and only catches abandoned quotes) and a conservative coverage
# floor of 2 (a typical good name carries ~4 of the six; a barren row carries
# 0-1) so the day-one false-positive surface is small and the bar is tuned UP
# from telemetry, not guessed. Per-run counts land in meta.trust_gate and each
# gate carries trust_detail (stale / thin / age_hours / signals_present), so the
# exclusion is measurable and auditable run-over-run. Env-toggled by
# TFB_OPP_TRUST_GATE (default ON) — set it to 0 to restore byte-identical v1.0.5
# behavior — with TFB_OPP_MAX_DATA_AGE_HOURS and TFB_OPP_MIN_TRUST_FIELDS
# tuning the two thresholds. Forward-compatible: a future engine trust_level
# plugs into this same gate without restructuring. Every v1.0.5 byte verbatim.

# ---------------------------------------------------------------------------
# §4.1 control-panel defaults (mirrors _Lists_Config TFB_PANEL_DEFAULTS T10:*)
# ---------------------------------------------------------------------------

DEFAULT_CRITERIA = {
    "universe_scope": "All Main Sheets",
    "max_selected": 3,
    "period_months": 12,
    "required_roi_pct": 12.0,
    "required_ann_roi_pct": 10.0,
    "risk_profile": "Moderate",
    "min_reliability": 70.0,
    "min_dq": 80.0,
    "min_rr": 2.0,
    "max_risk_level": "Medium",
    "allow_conflict": False,
    "allow_negative_news": False,
    "allow_negative_sector": False,
    "max_per_sector": 2,
    "max_per_market": 4,
    "include_portfolio_holdings": False,
    "base_currency": "SAR",
    # sizing / mechanics (env-overridable; see policy block)
    "max_weight_pct": 15.0,
    "lot_size": 1,
    "near_miss_n": 10,
    "review_days": 30,
    "stop_floor_pct": 8.0,
    "stop_vol_mult": 2.5,
    "stop_max_pct": 35.0,
    "pf_max_sector_pct": 30.0,
    "max_candidates": 0,
    # v1.0.3 forecast safety gate (env-tunable; see policy block)
    "forecast_gate_enabled": True,
    "min_engine_roi_pct": 0.0,
    # v1.0.4 valuation-sanity gate (env-tunable; see policy block)
    "valuation_sanity_gate_enabled": True,
    "max_valuation_roi_pct": 80.0,
    # v1.0.5 engine-forecast display (env-tunable; see policy block)
    "engine_roi_display_enabled": False,
    # v1.0.6 data-trust gate (env-tunable; see policy block)
    "trust_gate_enabled": True,
    "max_data_age_hours": 168.0,
    "min_trust_fields": 2,
    # v1.0.7 investability gate (env-tunable; see policy block)
    # v1.0.8: DEFAULT OFF (opt-in) — the selector backfills Tier-2 rows, so
    # default-ON would diverge the opportunity surface from the Top_10 page.
    "investability_gate_enabled": False,
    # v1.0.9: DEFAULT OFF (opt-in). When ON, a sized ticket whose suggested
    # SAR is 0 (capital exhausted before it could be funded) is NOT counted as
    # a selected/executable ticket: it is removed from `selected`, excluded
    # from selected_count, and surfaced as a WATCH near-miss ("Funding" gate)
    # plus an unfunded_candidates alert. OFF => byte-identical v1.0.8 (every
    # pick — funded or 0-SAR — remains a selected ticket and is counted).
    "unfunded_watch_enabled": False,
    # v1.0.9: DEFAULT OFF (opt-in). When ON, the INVEST selection pool is
    # ordered by the engine's normalized 12M forecast (engine_roi_12m_pct,
    # desc) as the PRIMARY key — opportunity_score / ann_roi / symbol remain
    # the tie-breakers — so the names the engine forecasts highest are funded
    # first. OFF => byte-identical v1.0.8 ordering (opportunity_score primary).
    # A missing/unparseable engine forecast sorts last (never invents a rank).
    "rank_by_engine_roi_enabled": False,
}

_CRITERIA_FLOAT_KEYS = (
    "required_roi_pct", "required_ann_roi_pct", "min_reliability", "min_dq",
    "min_rr", "max_weight_pct", "stop_floor_pct", "stop_vol_mult",
    "stop_max_pct", "pf_max_sector_pct", "min_engine_roi_pct",
    "max_valuation_roi_pct", "max_data_age_hours",
)
_CRITERIA_INT_KEYS = (
    "max_selected", "period_months", "max_per_sector", "max_per_market",
    "lot_size", "near_miss_n", "review_days", "max_candidates",
    "min_trust_fields",
)
_CRITERIA_BOOL_KEYS = (
    "allow_conflict", "allow_negative_news", "allow_negative_sector",
    "include_portfolio_holdings", "forecast_gate_enabled",
    "valuation_sanity_gate_enabled", "engine_roi_display_enabled",
    "trust_gate_enabled", "investability_gate_enabled",
    "unfunded_watch_enabled", "rank_by_engine_roi_enabled",
)

# ---------------------------------------------------------------------------
# §4.3 score weights (sum = 100) and plan-fixed maps
# ---------------------------------------------------------------------------

SCORE_WEIGHTS = {
    "ann_roi": 20.0,
    "risk_reward": 18.0,
    "reliability": 15.0,
    "data_quality": 10.0,
    "margin_of_safety": 12.0,
    "sector_trend": 8.0,
    "news_trend": 8.0,
    "liquidity": 4.0,
    "diversification": 5.0,
}

TREND_SCORE = {"Positive": 100.0, "Neutral": 60.0, "Unknown": 40.0,
               "Negative": 0.0}
RISK_LEVEL_SCORE = {"Low": 100.0, "Medium": 70.0, "High": 40.0}
RISK_ORDER = {"Low": 1, "Medium": 2, "High": 3}

# v1.0.0 normalization anchors (documented in WHY block)
ANNROI_FULL_AT_PCT = 40.0
RR_FULL_AT = 4.0
MOS_FULL_AT_PCT = 50.0
LIQUIDITY_FULL_AT_SAR = 5_000_000.0
LIQUIDITY_UNKNOWN_SCORE = 40.0
DIVERSIFICATION_NEW_SECTOR = 100.0
DIVERSIFICATION_BELOW_CAP = 60.0
DIVERSIFICATION_AT_CAP = 0.0
DIVERSIFICATION_NO_CONTEXT = 60.0

# §4.2 gate evaluation order (first fail in this order = headline failed_gate)
GATE_ORDER = (
    "Price", "FX", "Valuation", "ROI", "Annualized ROI", "Valuation Sanity",
    "Forecast", "Reliability", "Data Quality", "Data Trust", "Investability",
    "Risk Level", "Risk/Reward", "Conflict", "News", "Sector Trend", "Portfolio",
)

FAIL_MAJOR = "MAJOR"
FAIL_NON_CRITICAL = "NON_CRITICAL"
FAIL_STRUCTURAL = "STRUCTURAL"

VERDICT_INVEST = "INVEST"
VERDICT_WATCH = "WATCH"
VERDICT_DNI = "DO_NOT_INVEST"

CONF_HIGH = "High"
CONF_MEDIUM = "Medium"
CONF_LOW = "Low"

DAYS_PER_MONTH = 30.4375  # L5

# ---------------------------------------------------------------------------
# FX (L6) — static fallbacks mirror _Lists_Config; provided rates win.
# Subunit currencies resolve to parent/100. Missing FX ⇒ gate fail, never 1.0.
# ---------------------------------------------------------------------------

FX_STATIC_TO_SAR = {
    "SAR": 1.0, "USD": 3.75, "EUR": 4.10, "GBP": 4.75, "JPY": 0.024,
    "CHF": 4.20, "CAD": 2.70, "AUD": 2.45, "HKD": 0.48, "CNY": 0.52,
    "ZAR": 0.20, "ILS": 1.05, "AED": 1.0211, "KWD": 12.25, "QAR": 1.0302,
    "BHD": 9.95, "OMR": 9.74, "EGP": 0.075, "INR": 0.044, "TRY": 0.09,
}
FX_SUBUNIT_PARENT = {"GBP_SUB": "GBP", "GBX": "GBP", "ZAC": "ZAR",
                     "ILA": "ILS"}
# "GBp" lowercases to "gbp" == parent code, so subunit detection must happen
# BEFORE case folding — handled in _resolve_fx via the raw token check below.
_RAW_SUBUNIT_TOKENS = {"GBp": "GBP", "GBX": "GBP", "ZAC": "ZAR", "ZAc": "ZAR",
                       "ILA": "ILS", "ILa": "ILS"}

# ---------------------------------------------------------------------------
# Env helpers (read per call — Render env changes apply without reimport)
# ---------------------------------------------------------------------------


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
    return str(_env_str("TFB_OPP_ENABLED", "1")).strip().lower() not in (
        "0", "false", "no", "off")


def _env_forecast_gate():
    """v1.0.3: engine-forecast safety gate toggle. Default ON; set
    TFB_OPP_FORECAST_GATE=0 to restore byte-identical v1.0.2 behavior."""
    return str(_env_str("TFB_OPP_FORECAST_GATE", "1")).strip().lower() not in (
        "0", "false", "no", "off")


def _env_valuation_sanity_gate():
    """v1.0.4: implausible-upside guard toggle. Default ON; set
    TFB_OPP_VALUATION_SANITY_GATE=0 to restore byte-identical v1.0.3 behavior."""
    return str(_env_str("TFB_OPP_VALUATION_SANITY_GATE", "1")).strip().lower() \
        not in ("0", "false", "no", "off")


def _env_engine_roi_display():
    """v1.0.5: engine-forecast display toggle. Default OFF; set
    TFB_OPP_ENGINE_ROI_DISPLAY=1 to surface the engine 12M forecast (and the
    engine-based gain) on every ticket and in the kpis. OFF is byte-identical
    v1.0.4."""
    return str(_env_str("TFB_OPP_ENGINE_ROI_DISPLAY", "0")).strip().lower() \
        in ("1", "true", "yes", "on")


def _env_trust_gate():
    """v1.0.6: data-trust gate toggle. Default ON; set TFB_OPP_TRUST_GATE=0 to
    restore byte-identical v1.0.5 behavior (the Data Trust gate is not
    appended)."""
    return str(_env_str("TFB_OPP_TRUST_GATE", "1")).strip().lower() not in (
        "0", "false", "no", "off")


def _env_investability_gate():
    """v1.0.7 gate; v1.0.8: DEFAULT OFF (opt-in). Set
    TFB_OPP_INVESTABILITY_GATE=1 to append the Investability MAJOR gate
    (strict INVESTABLE-only executable tickets). Off => byte-identical to
    v1.0.6 selection."""
    return str(_env_str(
        "TFB_OPP_INVESTABILITY_GATE", "0")).strip().lower() in (
        "1", "true", "yes", "on", "enabled", "enable")


def _env_unfunded_watch():
    """v1.0.9: unfunded-ticket reclassification toggle. Default OFF; set
    TFB_OPP_UNFUNDED_WATCH=1 so a 0-SAR (capital-exhausted) pick is reclassed
    WATCH (near-miss, "Funding" gate) instead of counting as a selected
    executable ticket. OFF => byte-identical v1.0.8 (0-SAR picks stay selected
    and counted)."""
    return str(_env_str("TFB_OPP_UNFUNDED_WATCH", "0")).strip().lower() in (
        "1", "true", "yes", "on", "enabled", "enable")


def _env_rank_by_engine_roi():
    """v1.0.9: selection-ordering toggle. Default OFF; set
    TFB_OPP_RANK_BY_ENGINE_ROI=1 to order the INVEST pool by the engine's
    normalized 12M forecast (desc) as the primary key (opportunity_score /
    ann_roi / symbol remain tie-breakers). OFF => byte-identical v1.0.8
    ordering (opportunity_score primary)."""
    return str(_env_str(
        "TFB_OPP_RANK_BY_ENGINE_ROI", "0")).strip().lower() in (
        "1", "true", "yes", "on", "enabled", "enable")


def _env_overrides():
    """Mechanics block of criteria, env-tunable (policy block)."""
    return {
        "near_miss_n": _env_int("TFB_OPP_NEAR_MISS_N",
                                DEFAULT_CRITERIA["near_miss_n"]),
        "max_weight_pct": _env_float("TFB_OPP_MAX_WEIGHT_PCT",
                                     DEFAULT_CRITERIA["max_weight_pct"]),
        "lot_size": _env_int("TFB_OPP_LOT_SIZE",
                             DEFAULT_CRITERIA["lot_size"]),
        "stop_floor_pct": _env_float("TFB_OPP_STOP_FLOOR_PCT",
                                     DEFAULT_CRITERIA["stop_floor_pct"]),
        "stop_vol_mult": _env_float("TFB_OPP_STOP_VOL_MULT",
                                    DEFAULT_CRITERIA["stop_vol_mult"]),
        "stop_max_pct": _env_float("TFB_OPP_STOP_MAX_PCT",
                                   DEFAULT_CRITERIA["stop_max_pct"]),
        "review_days": _env_int("TFB_OPP_REVIEW_DAYS",
                                DEFAULT_CRITERIA["review_days"]),
        "max_candidates": _env_int("TFB_OPP_MAX_CANDIDATES",
                                   DEFAULT_CRITERIA["max_candidates"]),
        "pf_max_sector_pct": _env_float("TFB_OPP_PF_MAX_SECTOR_PCT",
                                        DEFAULT_CRITERIA["pf_max_sector_pct"]),
        "min_engine_roi_pct": _env_float(
            "TFB_OPP_MIN_ENGINE_ROI_PCT",
            DEFAULT_CRITERIA["min_engine_roi_pct"]),
        "forecast_gate_enabled": _env_forecast_gate(),
        "max_valuation_roi_pct": _env_float(
            "TFB_OPP_MAX_VALUATION_ROI_PCT",
            DEFAULT_CRITERIA["max_valuation_roi_pct"]),
        "valuation_sanity_gate_enabled": _env_valuation_sanity_gate(),
        "engine_roi_display_enabled": _env_engine_roi_display(),
        "trust_gate_enabled": _env_trust_gate(),
        "max_data_age_hours": _env_float(
            "TFB_OPP_MAX_DATA_AGE_HOURS",
            DEFAULT_CRITERIA["max_data_age_hours"]),
        "min_trust_fields": _env_int(
            "TFB_OPP_MIN_TRUST_FIELDS",
            DEFAULT_CRITERIA["min_trust_fields"]),
        "investability_gate_enabled": _env_investability_gate(),
        "unfunded_watch_enabled": _env_unfunded_watch(),
        "rank_by_engine_roi_enabled": _env_rank_by_engine_roi(),
    }


def make_criteria(overrides=None):
    """DEFAULTS < env policy block < explicit overrides; coerced types."""
    crit = dict(DEFAULT_CRITERIA)
    crit.update(_env_overrides())
    for key, val in (overrides or {}).items():
        k = str(key).strip().lower()
        if k not in crit or val in (None, ""):
            continue
        if k in _CRITERIA_BOOL_KEYS:
            crit[k] = _coerce_bool(val)
        elif k in _CRITERIA_FLOAT_KEYS:
            f = _to_float(val)
            if f is not None:
                crit[k] = f
        elif k in _CRITERIA_INT_KEYS:
            f = _to_float(val)
            if f is not None:
                crit[k] = int(f)
        else:
            crit[k] = str(val).strip()
    if crit["max_selected"] < 0:
        crit["max_selected"] = 0
    if crit["lot_size"] < 1:
        crit["lot_size"] = 1
    if crit["period_months"] < 1:
        crit["period_months"] = 1
    return crit


def _coerce_bool(v):
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


# ---------------------------------------------------------------------------
# Value parsing — tolerant of sheet/provider artifacts ("12.5%", "1,234",
# "N/A", "-", "", NaN/inf) without ever inventing a number.
# ---------------------------------------------------------------------------

_NUM_RE = re.compile(r"^[+-]?\d*\.?\d+(?:[eE][+-]?\d+)?$")
_MISSING_TOKENS = {"", "-", "—", "n/a", "na", "none", "null", "nan", "#n/a",
                   "missing", "unknown", "?"}


def _to_float(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return f if math.isfinite(f) else None
    s = str(v).strip()
    if s.lower() in _MISSING_TOKENS:
        return None
    s = s.replace(",", "").replace("%", "").replace("SAR", "").strip()
    if not _NUM_RE.match(s):
        return None
    try:
        f = float(s)
    except ValueError:
        return None
    return f if math.isfinite(f) else None


def _to_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() not in _MISSING_TOKENS else None


def _norm_token(k):
    """Lowercase, strip everything non-alphanumeric — header-form agnostic."""
    return re.sub(r"[^a-z0-9]", "", str(k).lower())

# ---------------------------------------------------------------------------
# Candidate normalization — alias map covers display headers AND snake_case
# keys (live schema_registry v2.13.x vs selector ingest dicts).
# ---------------------------------------------------------------------------

_FIELD_ALIASES = {
    "symbol": ("symbol", "ticker", "code"),
    "name": ("name", "companyname", "instrumentname", "longname"),
    "market": ("market", "page", "sheetname", "sourcepage", "exchange",
               "marketregion"),
    "sector": ("sector", "gicssector", "industrysector"),
    "industry": ("industry",),
    "asset_class": ("assetclass", "instrumenttype", "type"),
    "currency": ("currency", "tradingcurrency", "ccy", "currencycode"),
    "price": ("currentprice", "price", "lastprice", "last", "close",
              "pricenative"),
    "fx_to_sar": ("fxtosar", "fxrate", "fxratetosar", "sarfx"),
    "target_price": ("targetprice", "pricetarget", "analysttarget",
                     "targetprice12m", "target"),
    "intrinsic_value": ("intrinsicvalue", "fairvalue", "fairprice",
                        "intrinsicestimate"),
    "engine_roi_12m_pct": ("expectedroi12m", "expectedroi", "forecastroi12m",
                           "engineroi12m", "expectedroipct"),
    "reliability": ("reliabilityscore", "reliability", "rel",
                    "forecastreliability", "forecastreliabilityscore"),
    "dq": ("dataqualityscore", "dataquality", "dq", "dqscore",
           "dataqualitypct"),
    "risk_level": ("risklevel", "riskbucket", "riskband", "riskcategory"),
    "news_trend": ("newstrend", "newssentiment", "newssignal"),
    "sector_trend": ("sectortrend", "sectorsignal", "sectormomentum"),
    "conflict": ("conflict", "conflictflag", "providerconflict",
                 "providerengineconflict", "signalconflict"),
    "vol_30d_pct": ("volatility30d", "vol30d", "volatility30", "vol30"),
    "avg_volume_30d": ("avgvolume30d", "averagevolume30d", "avgvolume",
                       "avgvol30d"),
    "recommendation": ("recommendationdetail", "recommendationdetailed",
                       "recommendation", "reco", "recommendationcanonical"),
    "recommendation_reason": ("recommendationreason", "recoreason",
                              "advisornote", "reasoning"),
    "investability": ("investability", "investabilitystatus",
                      "investabilitygate", "gatestatus"),
    "investability_reasons": ("investabilityreasons", "gatereasons",
                              "blockreasons", "blockreason",
                              "investabilitynotes"),
    "last_updated": ("lastupdatedutc", "lastupdated", "asof", "timestamp"),
    "data_provider": ("dataprovider", "provider", "primaryprovider"),
}

_TREND_MAP = {
    "positive": "Positive", "bullish": "Positive", "up": "Positive",
    "improving": "Positive",
    "neutral": "Neutral", "flat": "Neutral", "stable": "Neutral",
    "mixed": "Neutral",
    "negative": "Negative", "bearish": "Negative", "down": "Negative",
    "deteriorating": "Negative",
}

_RISK_MAP = {
    "low": "Low", "conservative": "Low",
    "medium": "Medium", "moderate": "Medium", "mid": "Medium",
    "high": "High", "aggressive": "High", "veryhigh": "High",
    "elevated": "High",
}


def _row_lookup(row):
    """Build a normalized-key view of a raw row dict (last write wins is
    avoided: first non-empty value per normalized key is kept)."""
    out = {}
    for k, v in row.items():
        nk = _norm_token(k)
        if nk and nk not in out:
            out[nk] = v
        elif nk in out and (out[nk] is None or out[nk] == "") and v not in (
                None, ""):
            out[nk] = v
    return out


def _field(view, field):
    for alias in _FIELD_ALIASES.get(field, ()):
        if alias in view:
            v = view[alias]
            if v not in (None, ""):
                return v
    return None


def _norm_trend(v):
    t = _to_text(v)
    if t is None:
        return "Unknown"
    return _TREND_MAP.get(_norm_token(t), "Unknown")


def _norm_risk(v):
    t = _to_text(v)
    if t is None:
        return None
    return _RISK_MAP.get(_norm_token(t))


def _norm_conflict(v):
    """True / False / None(unknown→treated as no-conflict but traced)."""
    if isinstance(v, bool):
        return v
    t = _to_text(v)
    if t is None:
        return None
    nt = _norm_token(t)
    if nt in ("yes", "true", "1", "conflict", "flagged"):
        return True
    if nt in ("no", "false", "0", "none", "clear", "ok", "noconflict"):
        return False
    # free-text flag fields: a mention of "conflict" counts ONLY when it is
    # not negated. Negation is checked on the spaced original (norm_token
    # strips spaces, which would make "No conflict" indistinguishable from a
    # real "conflict") so "notable conflict" stays a true conflict.
    if "conflict" in nt:
        low = " " + t.lower().replace("-", " ") + " "
        negated = any(tok in low for tok in
                      (" no ", " not ", " none ", " zero ", " without ",
                       " free ", " absent "))
        return False if negated else True
    return None


def _resolve_fx(currency_raw, fx_rates):
    """(rate_to_sar | None, source_str). Subunits detected on the RAW token
    before case folding (GBp lowercases into GBP). Provided rates win over
    static fallbacks; missing ⇒ (None, 'missing') ⇒ FX gate MAJOR fail."""
    if currency_raw is None:
        return None, "missing"
    raw = str(currency_raw).strip()
    if not raw:
        return None, "missing"
    divisor = 1.0
    token = raw
    if raw in _RAW_SUBUNIT_TOKENS:
        token = _RAW_SUBUNIT_TOKENS[raw]
        divisor = 100.0
    else:
        up = raw.upper()
        if up in ("GBX", "ZAC", "ILA"):
            token = {"GBX": "GBP", "ZAC": "ZAR", "ILA": "ILS"}[up]
            divisor = 100.0
    code = token.upper()
    provided = fx_rates or {}
    # provided map may itself be keyed by the subunit code — honor the EXACT
    # raw key only (never case-fold: "GBp".upper() collides with parent GBP)
    r = _to_float(provided.get(raw))
    if r is not None and r > 0:
        return r, "provided"
    r = _to_float(provided.get(code))
    if r is not None and r > 0:
        return r / divisor, "provided" if divisor == 1.0 else "provided/100"
    if code in FX_STATIC_TO_SAR:
        return FX_STATIC_TO_SAR[code] / divisor, (
            "static" if divisor == 1.0 else "static/100")
    return None, "missing"


def normalize_candidate(row, fx_rates, criteria):
    """Raw engine/selector row → internal candidate dict. Never raises on a
    malformed row; missing facts surface as None and fail their gates."""
    view = _row_lookup(row if isinstance(row, dict) else {})
    symbol = _to_text(_field(view, "symbol")) or "?"
    currency_raw = _to_text(_field(view, "currency"))
    if currency_raw is None and symbol.upper().endswith(".SR"):
        currency_raw = "SAR"

    price = _to_float(_field(view, "price"))
    if price is not None and price <= 0:
        price = None

    fx, fx_source = _resolve_fx(currency_raw, fx_rates)
    row_fx = _to_float(_field(view, "fx_to_sar"))
    if row_fx is not None and row_fx > 0:
        fx, fx_source = row_fx, "row"

    target = _to_float(_field(view, "target_price"))
    iv = _to_float(_field(view, "intrinsic_value"))
    if target is not None and target <= 0:
        target = None
    if iv is not None and iv <= 0:
        iv = None
    ref = target if target is not None else iv
    valuation_basis = ("target_price" if target is not None
                       else "intrinsic_value" if iv is not None else None)

    roi_pct = None
    if price and ref:
        roi_pct = (ref - price) / price * 100.0

    months = max(1, int(criteria["period_months"]))
    days = months * DAYS_PER_MONTH
    ann_roi_pct = None
    if roi_pct is not None and roi_pct > -100.0:
        ann_roi_pct = (math.pow(1.0 + roi_pct / 100.0, 365.0 / days)
                       - 1.0) * 100.0

    vol30 = _to_float(_field(view, "vol_30d_pct"))
    stop_pct = criteria["stop_floor_pct"]
    if vol30 is not None and vol30 > 0:
        stop_pct = max(stop_pct, criteria["stop_vol_mult"] * vol30)
    stop_pct = min(stop_pct, criteria["stop_max_pct"])

    stop = tp1 = tp2 = rr = None
    if price:
        stop = price * (1.0 - stop_pct / 100.0)
        if ref:
            tp1 = price + 0.5 * (ref - price)
            tp2 = ref
        if roi_pct is not None and stop_pct > 0:
            rr = roi_pct / stop_pct  # downside% == stop distance% by L6

    mos_pct = None
    if iv and price and iv > 0:
        mos_pct = max(0.0, (iv - price) / iv * 100.0)

    avg_vol = _to_float(_field(view, "avg_volume_30d"))
    liquidity_sar = None
    if avg_vol and price and fx:
        liquidity_sar = avg_vol * price * fx

    reliability = _to_float(_field(view, "reliability"))
    dq = _to_float(_field(view, "dq"))

    return {
        "symbol": symbol,
        "name": _to_text(_field(view, "name")) or symbol,
        "market": _to_text(_field(view, "market")) or "Unknown",
        "sector": _to_text(_field(view, "sector")) or "Unknown",
        "asset_class": _to_text(_field(view, "asset_class")),
        "currency": currency_raw,
        "fx_to_sar": fx,
        "fx_source": fx_source,
        "price": price,
        "price_sar": (price * fx) if (price and fx) else None,
        "target_price": target,
        "intrinsic_value": iv,
        "valuation_ref": ref,
        "valuation_basis": valuation_basis,
        "roi_pct": roi_pct,
        "ann_roi_pct": ann_roi_pct,
        "engine_roi_12m_pct": _to_float(_field(view, "engine_roi_12m_pct")),
        "reliability": reliability,
        "dq": dq,
        "risk_level": _norm_risk(_field(view, "risk_level")),
        "news_trend": _norm_trend(_field(view, "news_trend")),
        "sector_trend": _norm_trend(_field(view, "sector_trend")),
        "conflict": _norm_conflict(_field(view, "conflict")),
        "vol_30d_pct": vol30,
        "stop_pct": stop_pct,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr,
        "mos_pct": mos_pct,
        "liquidity_sar": liquidity_sar,
        "recommendation": _to_text(_field(view, "recommendation")),
        "recommendation_reason": _to_text(
            _field(view, "recommendation_reason")),
        "engine_gate": {
            "investability": _to_text(_field(view, "investability")),
            "reasons": _to_text(_field(view, "investability_reasons")),
            "provider": _to_text(_field(view, "data_provider")),
            "last_updated": _to_text(_field(view, "last_updated")),
        },
    }


# ---------------------------------------------------------------------------
# §4.2 hard gates → verdict (truth table)
# ---------------------------------------------------------------------------

def _gate(name, passed, fail_class, current, required, note=None):
    return {"gate": name, "passed": bool(passed),
            "fail_class": None if passed else fail_class,
            "current": current, "required": required, "note": note}


def _engine_roi_to_pct(value):
    """v1.0.3: normalize the engine 12M forecast ROI to a PERCENT for the
    Forecast gate. Providers deliver it either as a ratio (e.g. -0.20) or a
    percent (e.g. -20.0); |v| < 1.5 is treated as a ratio and scaled x100. The
    sign — the only thing the default 0% floor depends on — is preserved
    either way. Returns None when absent so 'Unknown passes'."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v * 100.0 if abs(v) < 1.5 else v


# v1.0.6 [DATA-TRUST-GATE] helpers ------------------------------------------
# The six secondary signals the gate measures for "thin coverage". The hard
# fields already have their own MAJOR gates, so trust keys off what the engine
# leaves Unknown on a sparse row. Trend fields read "Unknown" (never None) when
# absent; the rest read None.
_TRUST_SIGNAL_FIELDS = ("risk_level", "vol_30d_pct", "liquidity_sar",
                        "recommendation", "news_trend", "sector_trend")


def _trust_signal_count(cand):
    """How many of the six secondary signals are actually present on a
    candidate (Unknown trend == absent). Max 6; a typical good name carries
    ~4, a barren row 0-1."""
    n = 0
    if cand.get("risk_level"):
        n += 1
    if cand.get("vol_30d_pct") is not None:
        n += 1
    if cand.get("liquidity_sar") is not None:  # avg_volume_30d presence proxy
        n += 1
    if cand.get("recommendation"):
        n += 1
    if cand.get("news_trend") not in (None, "Unknown"):
        n += 1
    if cand.get("sector_trend") not in (None, "Unknown"):
        n += 1
    return n


def _parse_age_hours(last_updated_text):
    """Age in hours (float) of an ISO-ish timestamp vs now(UTC), or None when
    unparseable/absent so the freshness sub-check is SKIPPED (never a false
    stale block). Naive timestamps are assumed UTC; a future timestamp (clock
    skew) is treated as age 0, not stale. stdlib-only."""
    if not last_updated_text:
        return None
    s = str(last_updated_text).strip()
    if not s:
        return None
    iso = s.replace("Z", "+00:00").replace("z", "+00:00")
    dt = None
    try:
        dt = datetime.fromisoformat(iso)
    except (ValueError, TypeError):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except (ValueError, TypeError):
                continue
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    return 0.0 if age < 0 else age


def _data_trust_assessment(cand, criteria):
    """v1.0.6 Phase-0 trust signal for a Top_10 candidate. Returns
    (passed, current_str, detail). passed=False => the caller emits a MAJOR
    'Data Trust' gate (=> DO_NOT_INVEST; audit/near-miss only, never selected).
    Pure; never raises. Fails when the row is STALE (last_updated older than
    max_data_age_hours; unparseable/absent is NOT stale) OR THINLY COVERED
    (fewer than min_trust_fields of the six secondary signals present)."""
    max_age = criteria.get("max_data_age_hours")
    min_fields = int(criteria.get("min_trust_fields") or 0)
    eng = cand.get("engine_gate") or {}
    age_h = _parse_age_hours(eng.get("last_updated"))
    stale = (max_age is not None and max_age > 0 and
             age_h is not None and age_h > max_age)
    present = _trust_signal_count(cand)
    thin = present < min_fields
    passed = not (stale or thin)
    if passed:
        cur = ("ok (%d/6 signals%s)"
               % (present, "" if age_h is None
                  else ", %.1fd old" % (age_h / 24.0)))
    else:
        bits = []
        if stale:
            bits.append("stale %.1fd" % (age_h / 24.0))
        if thin:
            bits.append("thin %d/6 signals" % present)
        cur = "; ".join(bits)
    detail = {"stale": bool(stale), "thin": bool(thin),
              "age_hours": (None if age_h is None else round(age_h, 1)),
              "signals_present": present}
    return passed, cur, detail


def evaluate_gates(cand, criteria, held_symbols=None):
    """Per-row §4.2 gates in plan order. Diversification is selection-time
    (handled in the pick loop) and intentionally absent here."""
    held = held_symbols or set()
    g = []

    g.append(_gate("Price", cand["price"] is not None, FAIL_MAJOR,
                   cand["price"], "> 0"))

    g.append(_gate("FX", cand["fx_to_sar"] is not None and
                   cand["fx_to_sar"] > 0, FAIL_MAJOR,
                   cand["fx_to_sar"],
                   "fx_to_sar > 0 (ccy=" + str(cand["currency"]) + ")"))

    g.append(_gate("Valuation", cand["valuation_ref"] is not None, FAIL_MAJOR,
                   cand["valuation_basis"] or "none",
                   "target_price or intrinsic_value present"))

    roi_ok = (cand["roi_pct"] is not None and
              cand["roi_pct"] >= criteria["required_roi_pct"])
    g.append(_gate("ROI", roi_ok, FAIL_NON_CRITICAL,
                   _round1(cand["roi_pct"]),
                   ">= " + _fmt_num(criteria["required_roi_pct"]) + "%"))

    ann_ok = (cand["ann_roi_pct"] is not None and
              cand["ann_roi_pct"] >= criteria["required_ann_roi_pct"])
    g.append(_gate("Annualized ROI", ann_ok, FAIL_NON_CRITICAL,
                   _round1(cand["ann_roi_pct"]),
                   ">= " + _fmt_num(criteria["required_ann_roi_pct"]) + "%"))

    # v1.0.4 [VALUATION-SANITY-GATE]: the ticket roi_pct is pure valuation
    # upside (ref/price); upstream the engine's intrinsic-value model permits
    # fair value up to 3x price and a target cluster sits at exactly 3x, so a
    # name can screen with an implausible 100-200% "upside". This gate fails
    # MAJOR (=> DO_NOT_INVEST; appears in the audit / near-miss but never as a
    # selected ticket) when roi_pct exceeds max_valuation_roi_pct, catching
    # both inflated refs (intrinsic- and target-based) without altering roi_pct
    # itself. Appended ONLY when valuation_sanity_gate_enabled, so the gate list
    # and verdict are byte-identical to v1.0.3 when TFB_OPP_VALUATION_SANITY_GATE=0.
    if criteria.get("valuation_sanity_gate_enabled"):
        vmax = criteria.get("max_valuation_roi_pct", 80.0)
        val_roi = cand["roi_pct"]
        val_ok = val_roi is None or val_roi <= vmax
        g.append(_gate(
            "Valuation Sanity", val_ok, FAIL_MAJOR,
            ("n/a" if val_roi is None else _round1(val_roi)),
            "<= " + _fmt_num(vmax) + "% implied upside (valuation guard)"))

    # v1.0.3 [FORECAST-GATE]: a "best investments to BUY" surface must never
    # size a ticket the engine itself forecasts to fall. roi_pct is VALUATION
    # upside (target/intrinsic vs price) and can disagree with the engine's
    # 12M forecast; when the forecast is below the floor this gate fails MAJOR
    # => DO_NOT_INVEST, so the name can appear in the audit / near-miss but
    # never as a selected ticket. Unknown forecast passes (News/Sector
    # convention). Floor default 0% blocks only forecast LOSSES; tune via
    # min_engine_roi_pct. Appended ONLY when forecast_gate_enabled, so the gate
    # list and verdict are byte-identical to v1.0.2 when TFB_OPP_FORECAST_GATE=0.
    if criteria.get("forecast_gate_enabled"):
        fcst_pct = _engine_roi_to_pct(cand.get("engine_roi_12m_pct"))
        fcst_floor = criteria.get("min_engine_roi_pct", 0.0)
        fcst_ok = fcst_pct is None or fcst_pct >= fcst_floor
        g.append(_gate(
            "Forecast", fcst_ok, FAIL_MAJOR,
            ("Unknown" if fcst_pct is None else _round1(fcst_pct)),
            ">= " + _fmt_num(fcst_floor) + "% engine 12M (Unknown passes)"))

    rel = cand["reliability"]
    min_rel = criteria["min_reliability"]
    if rel is not None and rel >= min_rel:
        g.append(_gate("Reliability", True, None, _round1(rel),
                       ">= " + _fmt_num(min_rel)))
    elif rel is not None and rel >= min_rel - 15.0:
        g.append(_gate("Reliability", False, FAIL_NON_CRITICAL, _round1(rel),
                       ">= " + _fmt_num(min_rel),
                       "tiered: within Min-15 band"))
    else:
        g.append(_gate("Reliability", False, FAIL_MAJOR, _round1(rel),
                       ">= " + _fmt_num(min_rel),
                       "tiered: below Min-15 ⇒ MAJOR"))

    dq_ok = cand["dq"] is not None and cand["dq"] >= criteria["min_dq"]
    g.append(_gate("Data Quality", dq_ok, FAIL_MAJOR, _round1(cand["dq"]),
                   ">= " + _fmt_num(criteria["min_dq"])))

    # v1.0.6 [DATA-TRUST-GATE]: the engine captures investability / last_updated
    # into cand.engine_gate but it gated nothing, and there was no freshness or
    # coverage check; a sparse row (hard fields filled, secondary signals all
    # Unknown) screened straight through to ranking (5023.SR). This gate fails
    # MAJOR (=> DO_NOT_INVEST; appears in audit / near-miss but never selected)
    # when the row is STALE (last_updated older than max_data_age_hours;
    # unparseable/absent is NOT stale) OR THINLY COVERED (fewer than
    # min_trust_fields of the six secondary signals present). Appended ONLY when
    # trust_gate_enabled, so the gate list and verdict are byte-identical to
    # v1.0.5 when TFB_OPP_TRUST_GATE=0. trust_detail rides on the gate for the
    # audit grid and meta.trust_gate telemetry.
    if criteria.get("trust_gate_enabled"):
        t_ok, t_cur, t_detail = _data_trust_assessment(cand, criteria)
        tg = _gate(
            "Data Trust", t_ok, FAIL_MAJOR, t_cur,
            ("fresh (<= " + _fmt_num(criteria.get("max_data_age_hours")) +
             "h) AND >= " + str(int(criteria.get("min_trust_fields") or 0)) +
             "/6 core signals"))
        tg["trust_detail"] = t_detail
        g.append(tg)

    # v1.0.7 [INVESTABILITY-GATE]: enforce the engine's authoritative verdict.
    # normalize_candidate captures investability_status into
    # cand.engine_gate.investability but, before v1.0.7, evaluate_gates
    # re-derived its own truth-table and never read it — so a name the engine
    # benched (WATCHLIST/BLOCKED) could still be SELECTED if the builder's
    # independent gates happened to pass. The live engine Top_10 path already
    # requires INVESTABLE (data_engine_v2._top10_row_is_eligible), so this is a
    # defense-in-depth backstop + an EXPLICIT engine<->builder contract for any
    # path that feeds an unfiltered set. Fails MAJOR (=> DO_NOT_INVEST; shows in
    # audit / near-miss, never selected) on WATCHLIST or BLOCKED; INVESTABLE or
    # a blank/unrecognized token PASSES (fail-open + traced). Appended ONLY when
    # investability_gate_enabled, so the gate list + verdict are byte-identical
    # to v1.0.6 when TFB_OPP_INVESTABILITY_GATE=0; a no-op on a pre-filtered
    # (all-INVESTABLE) input.
    if criteria.get("investability_gate_enabled"):
        inv_raw = (cand.get("engine_gate") or {}).get("investability")
        inv_norm = _norm_token(_to_text(inv_raw) or "")
        inv_ok = inv_norm not in ("watchlist", "blocked")
        g.append(_gate(
            "Investability", inv_ok, FAIL_MAJOR,
            (_to_text(inv_raw) or "Unknown"),
            "engine verdict INVESTABLE (blank/Unknown passes)"))

    cap = _norm_risk(criteria["max_risk_level"]) or "Medium"
    risk = cand["risk_level"]
    eff_risk = risk or "Medium"  # unknown treated as Medium, traced below
    risk_ok = RISK_ORDER[eff_risk] <= RISK_ORDER[cap]
    risk_class = (FAIL_MAJOR if (not risk_ok and eff_risk == "High")
                  else FAIL_NON_CRITICAL)
    g.append(_gate("Risk Level", risk_ok, risk_class,
                   (risk or "Unknown→Medium"), "<= " + cap))

    rr_ok = cand["rr"] is not None and cand["rr"] >= criteria["min_rr"]
    g.append(_gate("Risk/Reward", rr_ok, FAIL_MAJOR, _round2(cand["rr"]),
                   ">= " + _fmt_num(criteria["min_rr"])))

    conflict = cand["conflict"]
    conflict_ok = criteria["allow_conflict"] or conflict is not True
    g.append(_gate("Conflict", conflict_ok, FAIL_MAJOR,
                   ("Yes" if conflict is True else
                    "No" if conflict is False else "Unknown"),
                   "No (or Allow Conflict = Yes)"))

    news_ok = (criteria["allow_negative_news"] or
               cand["news_trend"] != "Negative")
    g.append(_gate("News", news_ok, FAIL_MAJOR, cand["news_trend"],
                   "not Negative (Unknown passes)"))

    sect_ok = (criteria["allow_negative_sector"] or
               cand["sector_trend"] != "Negative")
    g.append(_gate("Sector Trend", sect_ok, FAIL_MAJOR, cand["sector_trend"],
                   "not Negative (Unknown passes)"))

    held_hit = (not criteria["include_portfolio_holdings"] and
                cand["symbol"] in held)
    g.append(_gate("Portfolio", not held_hit, FAIL_STRUCTURAL,
                   "held" if held_hit else "not held",
                   "exclude holdings (Include Portfolio Holdings = No)"))

    return g


def confidence_band(reliability):
    """L8: High ≥75, Medium 60–74, Low <60 (missing reliability ⇒ Low)."""
    if reliability is None:
        return CONF_LOW
    if reliability >= 75.0:
        return CONF_HIGH
    if reliability >= 60.0:
        return CONF_MEDIUM
    return CONF_LOW


def derive_verdict(gates, reliability):
    """PURE verdict derivation — the 1:1 verdict↔gate-trace contract.
    MAJOR fail ⇒ DO_NOT_INVEST; else any NON_CRITICAL ⇒ WATCH; else INVEST.
    STRUCTURAL never changes verdict (blocks selection only).
    L8: Low confidence caps INVEST at WATCH."""
    has_major = any(g["fail_class"] == FAIL_MAJOR for g in gates)
    if has_major:
        return VERDICT_DNI
    has_non_critical = any(
        g["fail_class"] == FAIL_NON_CRITICAL for g in gates)
    verdict = VERDICT_WATCH if has_non_critical else VERDICT_INVEST
    if verdict == VERDICT_INVEST and confidence_band(reliability) == CONF_LOW:
        return VERDICT_WATCH
    return verdict


def first_failed_gate(gates):
    order = {name: i for i, name in enumerate(GATE_ORDER)}
    failed = [g for g in gates if not g["passed"]]
    failed.sort(key=lambda g: order.get(g["gate"], 99))
    return failed[0] if failed else None


# ---------------------------------------------------------------------------
# §4.3 opportunity score
# ---------------------------------------------------------------------------

def _clamp01x100(x):
    return max(0.0, min(100.0, x))


def score_components(cand, sector_context):
    """sector_context: {"sectors": {sector: weight_or_count}, "cap_hit":
    set_of_sectors, "available": bool} — built once per run."""
    comps = {}
    comps["ann_roi"] = _clamp01x100(
        (cand["ann_roi_pct"] or 0.0) / ANNROI_FULL_AT_PCT * 100.0)
    comps["risk_reward"] = _clamp01x100(
        (cand["rr"] or 0.0) / RR_FULL_AT * 100.0)
    comps["reliability"] = _clamp01x100(cand["reliability"] or 0.0)
    comps["data_quality"] = _clamp01x100(cand["dq"] or 0.0)
    comps["margin_of_safety"] = _clamp01x100(
        (cand["mos_pct"] or 0.0) / MOS_FULL_AT_PCT * 100.0)
    comps["sector_trend"] = TREND_SCORE[cand["sector_trend"]]
    comps["news_trend"] = TREND_SCORE[cand["news_trend"]]
    if cand["liquidity_sar"] is None:
        comps["liquidity"] = LIQUIDITY_UNKNOWN_SCORE
    else:
        comps["liquidity"] = _clamp01x100(
            cand["liquidity_sar"] / LIQUIDITY_FULL_AT_SAR * 100.0)
    if not sector_context.get("available"):
        comps["diversification"] = DIVERSIFICATION_NO_CONTEXT
    elif cand["sector"] in sector_context.get("cap_hit", set()):
        comps["diversification"] = DIVERSIFICATION_AT_CAP
    elif cand["sector"] in sector_context.get("sectors", {}):
        comps["diversification"] = DIVERSIFICATION_BELOW_CAP
    else:
        comps["diversification"] = DIVERSIFICATION_NEW_SECTOR
    return comps


def opportunity_score(components):
    total = 0.0
    for key, weight in SCORE_WEIGHTS.items():
        total += components.get(key, 0.0) * weight / 100.0
    return round(total, 1)


# ---------------------------------------------------------------------------
# small formatting helpers (shared by trace + sentences)
# ---------------------------------------------------------------------------

def _round1(v):
    return None if v is None else round(float(v), 1)


def _round2(v):
    return None if v is None else round(float(v), 2)


def _fmt_num(v):
    if v is None:
        return "?"
    f = float(v)
    return str(int(f)) if f == int(f) else ("%.1f" % f)


def _fmt_sar(v):
    if v is None:
        return "? SAR"
    return "{:,.0f} SAR".format(v)


def _fmt_px(v):
    return "?" if v is None else "{:,.2f}".format(v)

# ---------------------------------------------------------------------------
# §4.4 wealth math + L7 funding loop (selection-time)
# ---------------------------------------------------------------------------

def _normalize_portfolio(portfolio):
    p = portfolio or {}
    cash = _to_float(p.get("cash_available_sar")) or 0.0
    proceeds = _to_float(p.get("pending_proceeds_sar")) or 0.0
    pv = _to_float(p.get("portfolio_value_sar")) or 0.0
    holdings = []
    for h in (p.get("holdings") or []):
        if not isinstance(h, dict):
            continue
        holdings.append({
            "symbol": _to_text(h.get("symbol")) or "?",
            "sector": _to_text(h.get("sector")) or "Unknown",
            "market": _to_text(h.get("market")) or "Unknown",
            "value_sar": _to_float(h.get("value_sar")) or 0.0,
        })
    if pv <= 0 and holdings:
        pv = sum(h["value_sar"] for h in holdings)
    return {"cash": max(0.0, cash), "proceeds": max(0.0, proceeds),
            "portfolio_value": max(0.0, pv), "holdings": holdings}


def _sector_context(pf, criteria):
    sectors = {}
    for h in pf["holdings"]:
        sectors[h["sector"]] = sectors.get(h["sector"], 0.0) + h["value_sar"]
    cap_hit = set()
    if pf["portfolio_value"] > 0:
        cap = criteria["pf_max_sector_pct"]
        for s, v in sectors.items():
            if v / pf["portfolio_value"] * 100.0 >= cap:
                cap_hit.add(s)
    return {"available": bool(pf["holdings"]), "sectors": sectors,
            "cap_hit": cap_hit}


def _size_one(cand, criteria, budget_base, remaining):
    """§4.4: Suggested SAR = min(MaxWeight% × budget_base, remaining);
    shares lot-floored; suggested re-derived from shares (honest funding)."""
    cap_sar = criteria["max_weight_pct"] / 100.0 * budget_base
    alloc = max(0.0, min(cap_sar, remaining))
    price_sar = cand["price_sar"] or 0.0
    lot = max(1, int(criteria["lot_size"]))
    shares = 0
    if price_sar > 0 and alloc >= price_sar * lot:
        shares = int(alloc // (price_sar * lot)) * lot
    suggested = shares * price_sar
    return suggested, shares


def _funds_from(suggested, cash_left, proceeds_left):
    """L7: every ADD names its funding source; split cash-first."""
    from_cash = min(suggested, cash_left)
    from_proceeds = min(max(0.0, suggested - from_cash), proceeds_left)
    parts = []
    if from_cash > 0:
        parts.append("Cash " + _fmt_sar(from_cash))
    if from_proceeds > 0:
        parts.append("TRIM/EXIT proceeds " + _fmt_sar(from_proceeds))
    label = " + ".join(parts) if parts else "Unfunded (no deployable capital)"
    return label, cash_left - from_cash, proceeds_left - from_proceeds


def _select_and_size(invest_cands, criteria, pf, sector_ctx):
    """L2 cap + §4.2 diversification (selection-time, defer) + §4.4 sizing.
    Returns (tickets_raw, deferrals{symbol: reason})."""
    deployable = pf["cash"] + pf["proceeds"]
    budget_base = pf["portfolio_value"] + deployable
    remaining = deployable
    cash_left, proceeds_left = pf["cash"], pf["proceeds"]

    sector_counts, market_counts = {}, {}
    pf_sector_sar = dict(sector_ctx["sectors"])
    picked, deferrals = [], {}

    for cand in invest_cands:
        if len(picked) >= criteria["max_selected"]:
            break
        sec, mkt = cand["sector"], cand["market"]
        if sector_counts.get(sec, 0) >= criteria["max_per_sector"]:
            deferrals[cand["symbol"]] = (
                "Diversification: sector cap " +
                str(criteria["max_per_sector"]) + "/" +
                str(criteria["max_per_sector"]) + " (" + sec + ")")
            continue
        if market_counts.get(mkt, 0) >= criteria["max_per_market"]:
            deferrals[cand["symbol"]] = (
                "Diversification: market cap reached (" + mkt + ")")
            continue
        suggested, shares = _size_one(cand, criteria, budget_base, remaining)
        # §4.2 combined post-action portfolio sector cap (only if sized & ctx)
        if (suggested > 0 and pf["portfolio_value"] > 0 and
                sector_ctx["available"]):
            post_total = pf["portfolio_value"] + suggested
            post_sector = pf_sector_sar.get(sec, 0.0) + suggested
            if post_sector / post_total * 100.0 > criteria[
                    "pf_max_sector_pct"]:
                deferrals[cand["symbol"]] = (
                    "Diversification: post-action sector weight would exceed "
                    + _fmt_num(criteria["pf_max_sector_pct"]) + "% (" + sec +
                    ")")
                continue
        funds_label, cash_left, proceeds_left = _funds_from(
            suggested, cash_left, proceeds_left)
        remaining -= suggested
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        market_counts[mkt] = market_counts.get(mkt, 0) + 1
        pf_sector_sar[sec] = pf_sector_sar.get(sec, 0.0) + suggested
        picked.append({"cand": cand, "suggested_sar": suggested,
                       "suggested_shares": shares,
                       "funds_from": funds_label})
    return picked, deferrals, deployable, remaining


# ---------------------------------------------------------------------------
# Ticket + L8 advisor sentence
# ---------------------------------------------------------------------------

def _entry_zone(cand):
    if not cand["price"] or not cand["fx_to_sar"]:
        return None
    fx = cand["fx_to_sar"]
    low = max(cand["stop"] or 0.0, cand["price"] * 0.97) * fx
    high = cand["price"] * 1.01 * fx
    return _fmt_px(low) + "\u2013" + _fmt_px(high) + " SAR"


def _advisor_sentence(cand, suggested_sar, shares, conf, review_date):
    """L8: ACTION + SIZE + LEVELS + one-line REASON + CONFIDENCE + REVIEW."""
    fx = cand["fx_to_sar"] or 0.0
    size_txt = (_fmt_sar(suggested_sar) + " (" + "{:,}".format(shares) +
                " sh)") if shares > 0 else "no size (no deployable capital)"
    levels = ("entry " + (_entry_zone(cand) or "?") + "; stop " +
              _fmt_px((cand["stop"] or 0) * fx) + ", TP1 " +
              _fmt_px((cand["tp1"] or 0) * fx) + ", TP2 " +
              _fmt_px((cand["tp2"] or 0) * fx) + " SAR")
    reason = ("valuation upside " + _fmt_num(_round1(cand["roi_pct"])) +
              "% (ann " + _fmt_num(_round1(cand["ann_roi_pct"])) +
              "%) via " + (cand["valuation_basis"] or "?") + ", R/R " +
              _fmt_num(_round2(cand["rr"])) + ", reliability " +
              _fmt_num(_round1(cand["reliability"])))
    return ("INVEST \u2014 " + size_txt + " @ " + levels + "; " + reason +
            ". Confidence " + conf + ". Review by " + review_date + ".")


def _build_ticket(rank, pick, criteria, review_date):
    cand = pick["cand"]
    fx = cand["fx_to_sar"]
    conf = confidence_band(cand["reliability"])
    # Reproducibility contract: exp_gain is derived from the DISPLAYED
    # (rounded) suggested_sar and ann_roi_pct so the sheet can re-verify it.
    suggested = round(pick["suggested_sar"], 0)
    ann = _round1(cand["ann_roi_pct"]) or 0.0
    exp_gain = round(suggested * ann / 100.0, 0)
    # v1.0.5: surface the engine 12M forecast alongside (never substituted into)
    # the valuation roi_pct. OFF => engine_pct stays None and every assignment
    # below is byte-identical v1.0.4.
    _eng_display = bool(criteria.get("engine_roi_display_enabled"))
    engine_pct = (_engine_roi_to_pct(cand["engine_roi_12m_pct"])
                  if _eng_display else None)
    detail_engine_roi = _round1(cand["engine_roi_12m_pct"])
    note = _advisor_sentence(cand, suggested, pick["suggested_shares"], conf,
                             review_date)
    if _eng_display:
        detail_engine_roi = _round1(engine_pct)
        if engine_pct is None:
            note = note + (" Engine 12M forecast: unavailable \u2014 the upside "
                           "shown is a valuation target, not a forecast.")
        else:
            note = note + (" Engine 12M forecast "
                           + _fmt_num(_round1(engine_pct))
                           + "% \u2014 the upside shown is a valuation target, "
                           "not a forecast.")
    ticket = {
        "rank": rank,
        "symbol": cand["symbol"],
        "name": cand["name"],
        "market": cand["market"],
        "sector": cand["sector"],
        "currency": cand["currency"],
        "fx_to_sar": _round4(fx),
        "price": _round2(cand["price"]),
        "price_sar": _round2(cand["price_sar"]),
        "entry_zone": _entry_zone(cand),
        "suggested_sar": suggested,
        "suggested_shares": pick["suggested_shares"],
        "stop_sar": _round2((cand["stop"] or 0) * fx if fx else None),
        "tp1_sar": _round2((cand["tp1"] or 0) * fx if fx else None),
        "tp2_sar": _round2((cand["tp2"] or 0) * fx if fx else None),
        "roi_pct": _round1(cand["roi_pct"]),
        "ann_roi_pct": _round1(cand["ann_roi_pct"]),
        "exp_gain_12m_sar": round(exp_gain, 0),
        "reliability": _round1(cand["reliability"]),
        "dq": _round1(cand["dq"]),
        "confidence_band": conf,
        "advisor_note": note,
        "detail": {
            "target_price": _round2(cand["target_price"]),
            "intrinsic_value": _round2(cand["intrinsic_value"]),
            "valuation_basis": cand["valuation_basis"],
            "engine_forecast_roi_pct": detail_engine_roi,
            "engine_recommendation": cand["recommendation"],
            "risk_level": cand["risk_level"] or "Unknown",
            "news_trend": cand["news_trend"],
            "sector_trend": cand["sector_trend"],
            "max_weight_pct": criteria["max_weight_pct"],
            "stop_pct": _round1(cand["stop_pct"]),
            "mos_pct": _round1(cand["mos_pct"]),
            "rr": _round2(cand["rr"]),
            "liquidity_sar": _round0(cand["liquidity_sar"]),
            "opportunity_score": cand.get("_score"),
            "score_components": cand.get("_components"),
            "catalyst": cand["recommendation_reason"],  # P9 upgrades this
            "key_risk": _key_risk(cand),
            "funds_from": pick["funds_from"],
            "review_date": review_date,
        },
    }
    if _eng_display:
        engine_gain = (round(suggested * engine_pct / 100.0, 0)
                       if engine_pct is not None else None)
        ticket["engine_roi_pct"] = _round1(engine_pct)
        ticket["valuation_roi_pct"] = _round1(cand["roi_pct"])
        ticket["engine_exp_gain_12m_sar"] = engine_gain
        ticket["valuation_exp_gain_12m_sar"] = ticket["exp_gain_12m_sar"]
    return ticket


def _key_risk(cand):
    if cand["conflict"] is True:
        return "Provider/engine conflict flagged"
    if (cand["vol_30d_pct"] or 0) > 12:
        return "Elevated volatility (30D " + _fmt_num(
            _round1(cand["vol_30d_pct"])) + "%)"
    if cand["risk_level"] == "High":
        return "High risk classification"
    if (cand["dq"] or 100) < 85:
        return "Data quality at the low end (" + _fmt_num(
            _round1(cand["dq"])) + ")"
    if cand["news_trend"] == "Unknown" and cand["sector_trend"] == "Unknown":
        return "News/sector trend unknown until P9 wiring"
    return "Standard market risk"


def _round0(v):
    return None if v is None else round(float(v), 0)


def _round4(v):
    return None if v is None else round(float(v), 4)


# ---------------------------------------------------------------------------
# Near miss + alerts + audit grid
# ---------------------------------------------------------------------------

def _near_miss_rows(audit, selected_syms, deferrals, criteria):
    pool = [a for a in audit if a["symbol"] not in selected_syms]
    pool.sort(key=lambda a: (-(a["opportunity_score"] or 0.0), a["symbol"]))
    rows = []
    for a in pool[:criteria["near_miss_n"]]:
        if a["symbol"] in deferrals:
            gate, cur, req = "Diversification", deferrals[a["symbol"]], (
                "within sector/market caps")
            note = "Qualified (INVEST) \u2014 deferred by diversification cap"
        elif a["verdict"] == VERDICT_INVEST:
            gate, cur, req = "Capacity", "rank beyond Max Selected", (
                "Max Selected = " + str(criteria["max_selected"]))
            note = "Qualified (INVEST) \u2014 ranked below the cap"
        else:
            ff = a.get("first_fail")
            gate = ff["gate"] if ff else "?"
            cur = ff["current"] if ff else None
            req = ff["required"] if ff else None
            note = _improve_note(ff)
        rows.append({"symbol": a["symbol"], "failed_gate": gate,
                     "current": cur, "required": req,
                     "verdict": a["verdict"], "improve_note": note})
    return rows


def _improve_note(ff):
    if not ff:
        return "No failing gate recorded"
    cur, req = ff.get("current"), ff.get("required")
    if isinstance(cur, (int, float)) and req:
        return ("Lift " + ff["gate"] + " from " + _fmt_num(cur) + " to " +
                str(req))
    return "Resolve " + ff["gate"] + " (now: " + str(cur) + "; needs: " + \
        str(req) + ")"


def _build_alerts(audit, deployable, selected, upstream_meta):
    counts = {}
    fx_ccys = set()
    for a in audit:
        ff = a.get("first_fail")
        if not ff:
            continue
        gate = ff["gate"]
        if gate == "FX":
            counts["missing_fx"] = counts.get("missing_fx", 0) + 1
            fx_ccys.add(str(a.get("currency")))
        elif gate == "Valuation":
            counts["missing_valuation"] = counts.get("missing_valuation",
                                                     0) + 1
        elif gate == "Data Quality":
            counts["low_dq"] = counts.get("low_dq", 0) + 1
        elif gate == "Conflict":
            counts["conflict"] = counts.get("conflict", 0) + 1
        elif gate == "News":
            counts["negative_news"] = counts.get("negative_news", 0) + 1
    actions = {
        "missing_fx": "Add FX rate(s) to _Lists_Config: " + ", ".join(
            sorted(fx_ccys)) if fx_ccys else "Add missing FX rates",
        "missing_valuation": "No target/intrinsic value \u2014 check engine "
                             "forecast coverage for these rows",
        "low_dq": "Investigate provider coverage (DQ below minimum)",
        "conflict": "Resolve provider/engine conflicts before investing",
        "negative_news": "Negative news gate \u2014 review or allow "
                         "explicitly in the control panel",
    }
    alerts = [{"type": t, "count": n, "required_action": actions[t]}
              for t, n in sorted(counts.items())]
    if selected and deployable <= 0:
        alerts.append({"type": "no_deployable_capital", "count": 1,
                       "required_action": "Set PF: Cash Available SAR (My_"
                       "Portfolio controls / _Lists_Config defaults) \u2014 "
                       "tickets are sized at 0"})
    meta = upstream_meta or {}
    budget = meta.get("budget") or {}
    if budget.get("exhausted") or meta.get("budget_exhausted"):
        alerts.append({"type": "budget_exhausted", "count": 1,
                       "required_action": "Upstream ingest budget exhausted "
                       "\u2014 treat coverage as partial; re-run warm"})
    return alerts


# ---------------------------------------------------------------------------
# §5 payload assembly (fail-soft, JSON-safe)
# ---------------------------------------------------------------------------

def build_opportunity_payload(rows, criteria=None, portfolio=None,
                              fx_rates=None, upstream_meta=None):
    """The one entry point P3 calls. Never raises: hard failures return an
    'error' skeleton so the page degrades zone-by-zone (§5)."""
    try:
        return _build(rows, criteria, portfolio, fx_rates, upstream_meta)
    except Exception as exc:  # noqa: BLE001 — fail-soft by contract
        return _json_safe(_skeleton(
            status="error",
            message=type(exc).__name__ + ": " + str(exc),
            criteria=make_criteria(criteria)))


def _build(rows, criteria, portfolio, fx_rates, upstream_meta):
    crit = make_criteria(criteria)
    if not _env_enabled():
        return _json_safe(_skeleton("disabled",
                                    "TFB_OPP_ENABLED=0", crit))
    rows = list(rows or [])
    if crit["max_candidates"] > 0:
        rows = rows[:crit["max_candidates"]]
    pf = _normalize_portfolio(portfolio)
    sector_ctx = _sector_context(pf, crit)
    held = {h["symbol"] for h in pf["holdings"]}

    # 1) normalize → gates → verdict → score (audit grid, 1:1 trace)
    audit = []
    gate_fail_counts = {}
    trust_stats = {"evaluated": 0, "blocked": 0,
                   "blocked_stale": 0, "blocked_thin": 0}
    for raw in rows:
        cand = normalize_candidate(raw, fx_rates, crit)
        gates = evaluate_gates(cand, crit, held)
        verdict = derive_verdict(gates, cand["reliability"])
        comps = score_components(cand, sector_ctx)
        score = opportunity_score(comps)
        cand["_components"] = comps
        cand["_score"] = score
        ff = first_failed_gate(gates)
        for g in gates:
            if not g["passed"]:
                bucket = gate_fail_counts.setdefault(
                    g["gate"], {"MAJOR": 0, "NON_CRITICAL": 0,
                                "STRUCTURAL": 0})
                bucket[g["fail_class"]] += 1
            if g["gate"] == "Data Trust":
                trust_stats["evaluated"] += 1
                td = g.get("trust_detail") or {}
                if not g["passed"]:
                    trust_stats["blocked"] += 1
                    if td.get("stale"):
                        trust_stats["blocked_stale"] += 1
                    if td.get("thin"):
                        trust_stats["blocked_thin"] += 1
        structural_block = any(g["fail_class"] == FAIL_STRUCTURAL
                               for g in gates)
        audit.append({
            "symbol": cand["symbol"], "name": cand["name"],
            "market": cand["market"], "sector": cand["sector"],
            "currency": cand["currency"], "fx_source": cand["fx_source"],
            "price": _round2(cand["price"]),
            "price_sar": _round2(cand["price_sar"]),
            "roi_pct": _round1(cand["roi_pct"]),
            "ann_roi_pct": _round1(cand["ann_roi_pct"]),
            "rr": _round2(cand["rr"]),
            "reliability": _round1(cand["reliability"]),
            "dq": _round1(cand["dq"]),
            "risk_level": cand["risk_level"] or "Unknown",
            "news_trend": cand["news_trend"],
            "sector_trend": cand["sector_trend"],
            "conflict": cand["conflict"],
            "verdict": verdict,
            "confidence_band": confidence_band(cand["reliability"]),
            "opportunity_score": score,
            "score_components": comps,
            "gates": gates,
            "first_fail": ff,
            "failure_reason": (ff["gate"] + ": " + str(ff["current"]) +
                               " vs " + str(ff["required"])) if ff else None,
            "structural_block": structural_block,
            "engine_gate": cand["engine_gate"],
            "selected": False,
            "deferral": None,
            "_cand": cand,
        })

    # 2) selection pool: INVEST verdict, not structurally blocked, by score
    invest = [a for a in audit
              if a["verdict"] == VERDICT_INVEST and not a["structural_block"]]
    # v1.0.9: optional engine-ROI ordering (default OFF => the original
    # opportunity_score-primary sort is byte-identical). When ON, the engine's
    # normalized 12M forecast is the primary key; opportunity_score / ann_roi /
    # symbol stay as tie-breakers. A missing/unparseable forecast sorts last.
    if crit.get("rank_by_engine_roi_enabled"):
        def _rank_engine_key(a):
            _er = _engine_roi_to_pct(a["_cand"]["engine_roi_12m_pct"])
            _er = _er if _er is not None else float("-inf")
            return (-_er, -(a["opportunity_score"] or 0.0),
                    -(a["_cand"]["ann_roi_pct"] or 0.0), a["symbol"])
        invest.sort(key=_rank_engine_key)
    else:
        invest.sort(key=lambda a: (-(a["opportunity_score"] or 0.0),
                                   -(a["_cand"]["ann_roi_pct"] or 0.0),
                                   a["symbol"]))
    picks, deferrals, deployable, remaining = _select_and_size(
        [a["_cand"] for a in invest], crit, pf, sector_ctx)

    review_date = (datetime.now(timezone.utc) +
                   timedelta(days=crit["review_days"])).date().isoformat()
    # v1.0.9: when unfunded_watch is ON, a 0-SAR pick (capital exhausted before
    # it could be funded) is NOT an executable ticket — it is removed from
    # `selected`, excluded from the count, and reclassed as a WATCH near-miss.
    # OFF => funded_picks == picks and unfunded_picks == [] (byte-identical).
    if crit.get("unfunded_watch_enabled"):
        funded_picks = [p for p in picks if (p["suggested_sar"] or 0) > 0]
        unfunded_picks = [p for p in picks if (p["suggested_sar"] or 0) <= 0]
    else:
        funded_picks = picks
        unfunded_picks = []
    tickets = [_build_ticket(i + 1, p, crit, review_date)
               for i, p in enumerate(funded_picks)]
    selected_syms = {t["symbol"] for t in tickets}
    by_symbol = {a["symbol"]: a for a in audit}
    for sym in selected_syms:
        by_symbol[sym]["selected"] = True
    for sym, reason in deferrals.items():
        if sym in by_symbol:
            by_symbol[sym]["deferral"] = reason
    # v1.0.9: tag unfunded picks in the audit grid and build their WATCH
    # near-miss rows (empty list when the flag is OFF).
    unfunded_syms = set()
    unfunded_nm = []
    for p in unfunded_picks:
        usym = p["cand"]["symbol"]
        unfunded_syms.add(usym)
        if usym in by_symbol and not by_symbol[usym]["deferral"]:
            by_symbol[usym]["deferral"] = (
                "Unfunded \u2014 passed all gates and ranked, but deployable "
                "capital was exhausted before funding")
        unfunded_nm.append({
            "symbol": usym,
            "failed_gate": "Funding",
            "current": "0 SAR (capital exhausted)",
            "required": "deployable capital to fund \u2265 1 lot",
            "verdict": VERDICT_WATCH,
            "improve_note": (
                "Passed every gate and was selected by rank, but cash ran out "
                "before this name could be funded \u2014 add Cash Available "
                "(or lower Max Selected) to fund it."),
        })

    # 3) kpis (L7 funding identity: unallocated = deployable − Σ suggested)
    total_suggested = sum(t["suggested_sar"] for t in tickets)
    kpis = {
        "deployable_sar": round(deployable, 0),
        "expected_gain_12m_sar": round(
            sum(t["exp_gain_12m_sar"] for t in tickets), 0),
        "selected_count": len(tickets),
        "max_selected": crit["max_selected"],
        "blended_reliability": _blend(tickets, "reliability"),
        "blended_rr": _blend_detail(tickets, "rr"),
        "scanned": len(audit),
        "passed": len(invest),
        "capital_unallocated_sar": round(deployable - total_suggested, 0),
    }
    # v1.0.5: parallel engine-based expected gain (additive; the valuation-based
    # expected_gain_12m_sar above is left intact so kpi gain == Σ ticket gains).
    if crit.get("engine_roi_display_enabled"):
        _eng_gains = [t.get("engine_exp_gain_12m_sar") for t in tickets
                      if t.get("engine_exp_gain_12m_sar") is not None]
        kpis["engine_expected_gain_12m_sar"] = (
            round(sum(_eng_gains), 0) if _eng_gains else None)

    near_miss = unfunded_nm + _near_miss_rows(
        audit, selected_syms | unfunded_syms, deferrals, crit)
    alerts = _build_alerts(audit, deployable, tickets, upstream_meta)
    # v1.0.9: surface the capital-exhausted tail as an explicit alert (no-op
    # when unfunded_watch is OFF, since unfunded_nm is empty).
    if unfunded_nm:
        alerts.append({
            "type": "unfunded_candidates",
            "count": len(unfunded_nm),
            "required_action": (
                "These name(s) passed every gate and ranked, but deployable "
                "capital was exhausted before funding \u2014 increase Cash "
                "Available (or reduce Max Selected). Shown as WATCH, not "
                "executable tickets."),
        })

    # 4) audit grid sorted by score; strip internals
    audit.sort(key=lambda a: (-(a["opportunity_score"] or 0.0), a["symbol"]))
    for a in audit:
        a.pop("_cand", None)

    meta_in = upstream_meta or {}
    meta = {
        "criteria_snapshot": crit,
        "gate_trace_counts": gate_fail_counts,
        "trust_gate": {
            "enabled": bool(crit.get("trust_gate_enabled")),
            "max_data_age_hours": crit.get("max_data_age_hours"),
            "min_trust_fields": crit.get("min_trust_fields"),
            "evaluated": trust_stats["evaluated"],
            "blocked": trust_stats["blocked"],
            "blocked_stale": trust_stats["blocked_stale"],
            "blocked_thin": trust_stats["blocked_thin"],
        },
        "coverage": meta_in.get("coverage"),
        "budget": meta_in.get("budget"),
        "timeouts": meta_in.get("timeouts"),
        "freshness": meta_in.get("freshness"),
        "versions": {
            "opportunity_builder": OPPORTUNITY_BUILDER_VERSION,
            "selector": (meta_in.get("versions") or {}).get("selector")
            if isinstance(meta_in.get("versions"), dict)
            else meta_in.get("selector_version"),
            "engine": (meta_in.get("versions") or {}).get("engine")
            if isinstance(meta_in.get("versions"), dict)
            else meta_in.get("engine_version"),
        },
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    status = "ok" if audit else "no_candidates"
    payload = {
        "version": OPPORTUNITY_BUILDER_VERSION,
        "status": status,
        "kpis": kpis,
        "selected": tickets,
        "near_miss": near_miss,
        "alerts": alerts,
        "candidates_rows": audit,
        "meta": meta,
    }
    return _json_safe(payload)


def _blend(tickets, field):
    """suggested_sar-weighted blend; simple mean when all sizes are 0."""
    vals = [(t.get(field), t["suggested_sar"]) for t in tickets
            if t.get(field) is not None]
    if not vals:
        return None
    wsum = sum(w for _, w in vals)
    if wsum > 0:
        return round(sum(v * w for v, w in vals) / wsum, 1)
    return round(sum(v for v, _ in vals) / len(vals), 1)


def _blend_detail(tickets, key):
    vals = [(t["detail"].get(key), t["suggested_sar"]) for t in tickets
            if t["detail"].get(key) is not None]
    if not vals:
        return None
    wsum = sum(w for _, w in vals)
    if wsum > 0:
        return round(sum(v * w for v, w in vals) / wsum, 2)
    return round(sum(v for v, _ in vals) / len(vals), 2)


def _skeleton(status, message, criteria):
    """Zone-degradable empty payload (§5) — every zone present and typed."""
    return {
        "version": OPPORTUNITY_BUILDER_VERSION,
        "status": status,
        "message": message,
        "kpis": {"deployable_sar": 0, "expected_gain_12m_sar": 0,
                 "selected_count": 0,
                 "max_selected": criteria.get("max_selected", 3),
                 "blended_reliability": None, "blended_rr": None,
                 "scanned": 0, "passed": 0, "capital_unallocated_sar": 0},
        "selected": [],
        "near_miss": [],
        "alerts": [],
        "candidates_rows": [],
        "meta": {"criteria_snapshot": criteria,
                 "gate_trace_counts": {},
                 "trust_gate": {
                     "enabled": bool(criteria.get("trust_gate_enabled")),
                     "max_data_age_hours": criteria.get("max_data_age_hours"),
                     "min_trust_fields": criteria.get("min_trust_fields"),
                     "evaluated": 0, "blocked": 0,
                     "blocked_stale": 0, "blocked_thin": 0},
                 "coverage": None, "budget": None, "timeouts": None,
                 "freshness": None,
                 "versions": {"opportunity_builder":
                              OPPORTUNITY_BUILDER_VERSION,
                              "selector": None, "engine": None},
                 "generated_at_utc":
                     datetime.now(timezone.utc).isoformat()},
    }


def _json_safe(obj):
    """NaN/inf → None, datetimes → ISO, sets → sorted lists; recursive."""
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, set):
        return sorted(_json_safe(v) for v in obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


# ---------------------------------------------------------------------------
# Best-effort selector hook (authoritative wiring lands in P3 after the live
# Render exports are confirmed — /mnt/project copies are NEVER trusted).
# ---------------------------------------------------------------------------

def collect_candidates_via_selector(scope="All Main Sheets"):
    """Try known selector v4.19.x ingest entry points. Returns (rows, meta)
    or ([], {'ingest': 'unavailable', ...}) — callers fall back to passing
    rows explicitly."""
    try:
        from core.analysis import top10_selector as sel  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return [], {"ingest": "unavailable",
                    "reason": "import failed: " + str(exc)}
    for fn_name in ("collect_candidate_rows", "ingest_universe",
                    "collect_universe_rows", "load_candidates"):
        fn = getattr(sel, fn_name, None)
        if callable(fn):
            try:
                result = fn(scope) if fn.__code__.co_argcount else fn()
                if isinstance(result, tuple) and len(result) == 2:
                    return list(result[0] or []), dict(result[1] or {})
                return list(result or []), {
                    "ingest": fn_name,
                    "versions": {"selector": getattr(
                        sel, "TOP10_SELECTOR_VERSION", None)}}
            except Exception as exc:  # noqa: BLE001
                return [], {"ingest": "failed", "entry": fn_name,
                            "reason": str(exc)}
    return [], {"ingest": "unavailable",
                "reason": "no known entry point exported",
                "versions": {"selector": getattr(
                    sel, "TOP10_SELECTOR_VERSION", None)}}


if __name__ == "__main__":  # smoke: empty input must produce a valid payload
    print(json.dumps(build_opportunity_payload([]), indent=2)[:400])
