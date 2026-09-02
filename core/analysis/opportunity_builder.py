# -*- coding: utf-8 -*-
"""
core/analysis/opportunity_builder.py — Opportunity Engine for Top_10_Investments
Version: 1.0.19  (TFB Final Execution Plan v5.0 — Phase P2;
                 Engineering Audit Phase 1 — unfunded-ticket reclass + optional
                 engine-ROI ordering + minimum-ticket floor + floor near-miss
                 labeling + issuer-level cross-listing dedup + duplicate-issuer
                 near-miss labeling, all env-gated DEFAULT-OFF;
                 A2 — Yahoo->GICS sector map relocated to core.sectors)

v1.9.0 [B-6 SHARIAH MODEL GATE — the resolver's own "until the Gen-2
wiring" note, closed]: compliance_rule_sets() has merged the operator's
global model-screen verdicts (TFB_EXIT_BY_RULE_EXTRA, "any venue") since
v1.5.0 — but the opportunity surface only ever consulted the fail set for
.SR symbols. Evidence (2026-07-27 morning audit): MRP.US took the run's
ONLY executable ticket (19,666 SAR) while carrying MODEL_SCREEN_FAIL on
the weekly board; portfolio_actions blocks the same class at §4.6 EXIT-BY-
RULE, so the two decision surfaces disagreed about one rulebook. A new
"Shariah (Model)" gate fails MAJOR (=> DO_NOT_INVEST; audit/near-miss,
never selected) for a NON-.SR candidate whose symbol is in the resolver
fail set. Same one-resolver principle as Shariah (KSA): no second rulebook,
no drift; the fail set is fed today by TFB_EXIT_BY_RULE_EXTRA (the board's
published verdicts, operator-applied) and inherits any future automated
feed for free. DEFAULT OFF (TFB_OPP_SHARIAH_MODEL_GATE unset) = gate list
and verdict byte-identical to v1.8.0 — arming alters candidate eligibility
mid-window, so the flip is the operator's explicit decision and a declared
version break. Fail-open by construction: a symbol absent from the fail
set passes ("model pass/unscreened") — an empty or unreachable list can
never empty the board.
ALSO [GATE_ORDER — the v1.0.7 lesson completed]: "Quote Freshness",
"Shariah (KSA)", "Eligibility (KSA)" and "Activity Screen" append in
evaluate_gates but were never added to GATE_ORDER, so first_failed_gate
sorted them to 99 and could mis-attribute the near-miss "failed gate" and
the DATA GAPS grouping whenever one of them co-failed with a tuple member.
All four (plus the new gate) now sit at their TRUE append positions.
Selection, verdicts, scoring, tickets: untouched — this corrects
ATTRIBUTION on the audit surfaces only; tonight's gap-table counts may
shift toward the true first gate, which is the fix working. Zero function
removals; additions: _env_shariah_model_gate.]

v1.7.0 [SELL-CLASS GATE — replace the all-or-nothing Investability gate with
the narrow guard the audit contract actually needs. WHY (2026-07-23 evening,
measured on the live 10,465-row pool with the deployed code): the operator's
Render env has TFB_OPP_INVESTABILITY_GATE=1, but the engine marks only 59 of
10,465 rows INVESTABLE (WATCHLIST 9,829 · BLOCKED 285 · blank 292), so the
gate is not a backstop — it IS the buy funnel, and it collapses it. A/B on
identical data: gate OFF -> 120 passed / 10 tickets; gate ON -> 7 passed /
6 tickets, and after held-exclusion + the KSA Shariah/eligibility MAJOR
gates the operator saw ZERO candidates for days. The v1.0.8 header already
warned this exact failure ("a live build proved the selector backfills
Tier-2 (WATCHLIST / low-reliability) rows, so the gate WOULD drop real
selections") and shipped it DEFAULT OFF; production overrode that.
Inspection of the 10 suppressed tickets showed the suppression is mostly
UNJUSTIFIED: 3 were engine-INVESTABLE outright (1831.SR, 1321.SR — both
independently bought and held by the operator — plus 4503.T), and 6 were
WATCHLIST only because of a two-point conservative-score margin ("overall
66 < 68") while carrying reliability 77-92 and DQ 100. Exactly ONE carried
a genuine sell-side verdict (0023.HK, "Engine recommends REDUCE").
FIX: a new "Sell-Class" gate fails MAJOR when the engine's own
recommendation is an explicit sell-tier token (SELL / STRONG_SELL /
STRONG SELL / REDUCE / AVOID / UNDERPERFORM / UNDERWEIGHT). This is a
STRICT SUBSET of the Investability gate: it drops the one row that gate
was right about and keeps the nine it was wrong about, and it enforces
standing audit gate #2 ("no INVEST on SELL-class") explicitly rather than
by side effect. Blank/unknown/buy-tier tokens PASS (fail-open + traced),
matching the Investability gate's own convention. The two gates are
INDEPENDENT and compose: with both enabled, Investability still dominates
(it is strictly stricter), so enabling this gate can never widen the
funnel of an Investability-gated run — it can only narrow an ungated one.
Default ON via TFB_OPP_SELL_CLASS_GATE; =0 restores v1.6.0 gate lists and
verdicts byte-for-byte. Added to GATE_ORDER at its true append position so
first_failed_gate attribution stays correct (the v1.0.7 GATE_ORDER lesson).
OPERATOR ACTION REQUIRED, flagged separately: this build only takes effect
once TFB_OPP_INVESTABILITY_GATE is set to 0 in Render. Zero functions
removed; addition: _env_sell_class_gate.]

v1.6.0 [PREGATE-ORDER — quality-ordered candidate clamp; kills the
adverse-selection cut. WHY (2026-07-22 evening audit, workbook evidence):
the Top_10 run scanned exactly TFB_OPP_MAX_CANDIDATES=300 of a 10,311-row
pool because the v1.0.10 clamp is POSITIONAL (`rows[:max_candidates]`) and
runs BEFORE any gate — whichever 300 rows arrive first get the only scan
slots. Tonight's 300 were dominated by junk micro-caps whose implausible
fair-value gaps then CORRECTLY failed Valuation Sanity (291/300 = 97%),
leaving 1 survivor (already held) and 0 executable tickets — a permanently
empty buy surface while 10,011 rows were never examined. The gates were
right; the ordering starved them. FIX: when the clamp WILL cut (flag ON,
len(rows) > max_candidates > 0), _pregate_quality_order() first re-orders
the pool CHEAP-ELIGIBLE-FIRST using row-local mirrors of the same gates —
price+valuation-ref present, trust-style freshness (last_updated age <=
max_data_age_hours; absent/unparseable is NOT stale, same semantics as
_data_trust_assessment), Valuation Sanity bound (roi_pct <= max_valuation_
roi_pct; None passes), Forecast floor (engine 12M >= min_engine_roi_pct;
None passes), and the min_reliability floor — then inside each bucket by
reliability desc, engine forecast desc, symbol, arrival index (total order,
deterministic). Every mirror reads the IDENTICAL raw fields via the same
_field tokens normalize_candidate uses, including the v1.3.0
REF-CONSERVATIVE min() so the sanity mirror sees the same roi_pct the real
gate will. The clamp then cuts the re-ordered list; everything downstream
(normalize, gates, verdicts, scoring, selection, funding) is UNTOUCHED.
Full-pool funnel telemetry lands in kpis["pregate"] (additive key, absent
when the reorder did not run) and one [PREGATE] log line. Kill-switch
TFB_OPP_PREGATE_ORDER=0 restores the v1.5.0 positional cut byte-for-byte;
no clamp (max_candidates=0) or no cut (len<=cap) also bypasses the reorder
entirely => byte-identical output. Zero functions removed; additions:
_env_pregate_order, _pregate_quality_order.]

v1.0.19 [PANEL-DEFAULT RECALIBRATION — Max Selected 3->10, Max Per Market
4->10. WHY: the owner's standing instruction (2026-07-02) is a TEN-pick
Selected list. Two DEFAULT_CRITERIA literals blocked that as the standard:
"max_selected": 3 capped the list at three even when ten candidates qualified
and were fundable, and "max_per_market": 4 made ten UNREACHABLE for the
KSA-heavy universe regardless of max_selected (four picks from any one market
was the hard ceiling). Both defaults now read 10; the secondary fallback in
the empty-result KPI block (criteria.get("max_selected", 3)) is aligned to 10
so an empty run reports the same ceiling. "max_per_sector" stays at 2 ON
PURPOSE — it is the remaining diversification guard, and with 5+ qualifying
sectors it does not block a ten-pick list; raise it via the panel cell only if
the Selected list stalls short with a sector-cap audit reason. NOTE these are
FALLBACK defaults: the _Lists_Config TFB_PANEL_DEFAULTS panel (T10: Max
Selected / T10: Max Per Market) is sent with every cockpit refresh and always
wins when present — the panel cells remain the runtime control and the
reversibility path (set the cells back to 3 / 4 to restore the old behaviour
without any deploy). No gates, no ranking logic, no schema change; every
qualification / funding / min-ticket rule applies unchanged, so ten is a
CEILING, never a forced fill. Two literals + one fallback changed; zero
functions added or removed.]

v1.0.18 [A2 SECTOR-MAP SINGLE-SOURCE — no behaviour change. WHY: the six-entry
Yahoo->GICS sector map (_YAHOO_TO_GICS_SECTOR, added v1.0.13) was a PRIVATE copy
in this file; top10_selector's W-5 cap (A2) needs the identical map, and two
copies are exactly the cross-file taxonomy drift A2 exists to kill. The map now
lives in core.sectors as the single source of truth and is imported here; the
inline literal is retained ONLY as an import fallback. _normalize_sector(), the
TFB_OPP_SECTOR_NORMALIZE gate and every downstream behaviour are unchanged --
with core.sectors present (normal) the imported dict is byte-identical to the
former literal, so verdicts/ranks/labels are identical. No functions added or
removed; one literal converted to an import-with-fallback.]

v1.0.17 [DUPLICATE-ISSUER NEAR-MISS LABELING — display correctness, no gate
change. WHY: v1.0.16's issuer dedup records a deferred cross-listing in the same
`deferrals` dict ("Duplicate issuer — already funded {SYM}"), but _near_miss_rows
only special-cased the floor reason — every other deferral fell through to the
"Diversification" gate with "within sector/market caps" / "deferred by
diversification cap". So a duplicate-issuer name surfacing in NEAR MISS (likely,
since the deferred sibling carries a near-identical score) was mislabeled as a
diversification cap it never hit — the exact bug class fixed for the floor in
v1.0.15, reintroduced by the new deferral category not being wired into the
classifier. FIX: add a branch in _near_miss_rows — a deferral containing
"Duplicate issuer" is classified as the "Duplicate" gate (Required = "one listing
per issuer", How-To = a higher-ranked listing of this issuer is already funded;
cross-listing of the same company). All other deferrals keep byte-identical
labeling. No gate, verdict, ticket, sizing or funding-identity change — purely
the gate/required/how-to text for duplicate-issuer near-miss rows. Reachable only
when issuer dedup is ON, so OFF => byte-identical v1.0.16. No new functions;
_near_miss_rows body only.


(TFB_OPP_ISSUER_DEDUP / criteria issuer_dedup_enabled). WHY: one company can
list under several symbols — a true cross-listing (Takeda Tokyo 4502.T + NYSE
ADR TAK.US) or a symbol-spelling twin on one exchange (BMW.DE + BMW.XETRA;
MUV2.XETRA + MUV2.DE). Each occupied a separate Top_10 slot (2026-06-27 live),
wasting selection capacity and concentrating one issuer. This sits one layer
above the v1.0.11 market-cap canonicalization (which collapsed NYSE/NASDAQ vs
NASDAQ/NYSE): now we collapse multiple SYMBOLS of one ISSUER. FIX (default OFF
=> byte-identical v1.0.15): when enabled, the greedy selector keys each candidate
to an issuer and, once an issuer is FUNDED, defers any later listing of the same
issuer with a 'Duplicate issuer — already funded {SYM}' reason (the existing
deferrals path; the duplicate stays a valid INVEST row in the audit, it just
cannot take a second funded slot). HYBRID KEY: a curated override map
(_ISSUER_DEDUP_MAP, default empty) wins first — it can FORCE-MERGE listings whose
names diverge (e.g. an ADR named differently from the local line) or FORCE-SPLIT
genuinely-distinct same-named issuers; otherwise the key is the normalized
company name (legal suffixes + punctuation stripped), which already collapses the
three live dupes because their names are identical across listings. SAFETY: a
nameless row (name missing or == symbol) keys to its own symbol, so it can never
false-merge into another issuer; the dropped duplicate is always shown with an
explicit reason, never silently removed. KEYED AT FUNDING (not at first
encounter): an issuer whose top-ranked symbol is sector-capped or floored is NOT
pre-empted — a fundable listing of the same issuer can still be chosen. One new
criterion key + one env reader + one issuer-key helper + a guarded check/record
pair in _select_and_size; every v1.0.15 function carried verbatim, none removed.


v1.0.14's minimum-ticket floor records a sub-floor pick in the same `deferrals`
dict the diversification caps use ("Unfunded — sized ticket X below minimum
ticket floor Y"). _near_miss_rows classified EVERY deferred symbol as the
"Diversification" gate with "within sector/market caps" / "deferred by
diversification cap" — so a floor-deferred name that surfaced in NEAR MISS
(e.g. 0939.HK, 2026-06-27 live) was mislabeled: the reason string was correct
but the Failed-Gate column, the Required column, and the How-To-Qualify line all
described a diversification cap it never hit. FIX: split the deferrals branch in
_near_miss_rows — a deferral whose reason contains "minimum ticket floor" is
classified as the "Funding" gate (Required = "fundable amount >= minimum ticket
floor (Y SAR)", How-To = add Cash Available / lower Max Selected / lower the
floor), consistent with the existing capital-exhausted Funding near-miss rows;
all other deferrals keep the byte-identical diversification labeling. No gate,
verdict, ticket, sizing or funding-identity change — purely the gate/required/
how-to text for floor-deferred near-miss rows. Floor deferrals only exist when
min_ticket_sar > 0, so with the floor OFF this branch is never taken =>
byte-identical v1.0.14. No new functions; _near_miss_rows body only.


WHY: the greedy §4.4 sizer funds picks top-down until deployable capital is
exhausted. With the engine-ROI reorder (v1.0.9) packing the high-forecast names
first, the last few hundred SAR of cash were still spent on the next ranked
names, producing 1-2 share "executable tickets" worth a token amount (live
2026-06-27 build: G.US 214 SAR / 2 sh, BHC.US 75 SAR / 4 sh) sitting beside the
properly-sized ~15k positions. A sub-floor scrap is not an executable position.
FIX (default OFF when min_ticket_sar <= 0 => byte-identical v1.0.13): when
min_ticket_sar > 0, a sized ticket whose suggested SAR is 0 < x < floor is NOT
appended as a funded ticket — it is deferred with an explicit "below minimum
ticket floor" reason (selected = No), exactly like the diversification-cap
deferrals. Because `remaining` only shrinks as funding proceeds, once it drops
below the floor every later pick is sub-floor too and is likewise deferred, so
the greedy tail of scraps stops and the funded list holds only properly-sized
tickets. suggested == 0 (capital FULLY exhausted) is intentionally left to the
existing v1.0.9 unfunded_watch path; this floor covers the 0 < x < floor band
it did not. Recommend enabling alongside TFB_OPP_UNFUNDED_WATCH so both the
0-SAR and sub-floor bands reclass consistently. SAR-only by design: a
share-count floor would wrongly cut a legitimate large-SAR position in a
high-priced name (e.g. one share of an 11,000-SAR stock). One new criterion key
(min_ticket_sar) + one env read + one clamp + one guarded block in
_select_and_size; every v1.0.13 function carried verbatim, none removed.

v1.0.22 [SECTOR-NORMALIZE DEFAULT ON + TP-LADDER COHERENCE — two fixes.
FIX 1 (default flip): TFB_OPP_SECTOR_NORMALIZE now defaults ON (=0 is the
kill switch, byte-identical v1.0.12 buckets). WHY: the v1.0.13 fix exists
precisely for the live damage class still occurring with it dormant — the
2026-07-06/07 audits show 5023.SR (sukuk, sector Unknown by nature) blocked
over_sector_cap 2/2 (Unknown) on the Top_10 funnel: a DATA GAP consumed a
real diversification slot and barred a fixed-income name for "concentration"
in a sector that does not exist. Same documented-control rationale as the
2026-07-07 provider-guard default flips: the control is documented in this
file's own v1.0.13 WHY, was violated in production, and the switch is
conservative by construction (the post-action PORTFOLIO weight cap still
applies to Unknown — real-money concentration remains controlled).
FIX 2 (TP-COHERENCE): the ticket ladder built TP1/TP2 whenever a reference
target existed, including a target AT/BELOW price — printing an inverted
BUY ladder (both TPs under entry) on any candidate whose surviving target
sits below spot. The ladder now requires ref > price; otherwise TP fields
stay blank (stop and the signed roi/RR math are untouched — the Forecast
gate keeps judging the signed truth). ZERO functions added or removed;
everything else byte-identical to v1.0.21.]

v1.0.13 [DIVERSIFIER SECTOR-QUALITY — env-gated DEFAULT-OFF. Two related
selection-time corrections behind ONE switch, TFB_OPP_SECTOR_NORMALIZE.
WHY: the per-sector diversification cap (max_per_sector) buckets on the raw
cand["sector"] string. Two data-quality leaks fragmented or mis-bucketed it.
(a) A KSA name NOT in the _KSA_SYMBOL_SECTOR map (data_engine_v2) falls through
to the provider's Yahoo sector vocabulary ("Basic Materials", "Healthcare",
"Consumer Cyclical", "Consumer Defensive", "Technology", "Financial Services"),
which are DIFFERENT strings from the GICS spellings the map uses ("Materials",
"Health Care", "Consumer Discretionary", "Consumer Staples", "Information
Technology", "Financials"). Because the cap compares exact strings, a Yahoo-
vocab straggler forms its OWN cap bucket instead of counting against its GICS
peers — so the sector cap silently under-counts and concentration leaks through.
(b) A name with NO sector from any provider becomes "Unknown" (the `or
"Unknown"` default); several such names then collide in a single "Unknown"
bucket and are capped at max_per_sector as if they were one real sector, wrongly
deferring good picks whose true (unknown) sectors may all differ.
FIX (default OFF): when TFB_OPP_SECTOR_NORMALIZE=1 — (a) _normalize_sector()
translates the six differing Yahoo spellings to GICS at the sector source (both
the candidate view and the portfolio holdings), so stragglers bucket with their
peers; (b) the "Unknown"/"" data-gap bucket is EXEMPT from the per-sector COUNT
cap (an unknown sector is a data gap, not a concentration bucket). The post-
action PORTFOLIO weight cap (pf_max_sector_pct) is deliberately left applied to
"Unknown" — real-money concentration is still controlled. OFF => byte-identical
v1.0.12 (raw sector strings, Unknown capped as before). Two new defs added
(_env_sector_normalize, _normalize_sector) + one constant
(_YAHOO_TO_GICS_SECTOR); all v1.0.12 functions carried verbatim, none removed.
v1.0.22 ADDENDUM: the default is now ON — see the v1.0.22 WHY at top.]

v1.0.12 [ENGINE-ROI-AUDIT — surface the engine's normalized 12M forecast on
every candidates_rows audit record via one new field, engine_roi_pct, so the
GAS audit grid can render an "Engine ROI %" column across ALL QUALIFIED /
CANDIDATES instead of only on the 8 selected tickets. The value is the SAME
normalization the Forecast gate tests against (_engine_roi_to_pct of
engine_roi_12m_pct), so the column equals the number the gate compares to
min_engine_roi_pct — making the gate floor tunable from the visible
target-vs-forecast divergence rather than a guess. PURELY ADDITIVE: exactly
one dict key added to the per-candidate audit record; ZERO functions added or
removed; ZERO gate / scoring / sizing / selection / ordering change; output is
byte-identical except for the new key. Missing/unparseable forecast => None
(renders blank), never invents a number.]

v1.0.11 [MARKET-CAP CANONICALIZATION — diversification correctness fix.
THE BUG: the per-market cap (criteria max_per_market) keyed market_counts on
the raw market string, so the SAME venue written two ways — "NYSE/NASDAQ" vs
"NASDAQ/NYSE" — landed in two separate buckets and each filled its own cap.
Live evidence (full-universe run, max_per_market=4): SELECTED held 6 US names
(TTEK/UHS/BAH/GPOR as "NYSE/NASDAQ" = 4, plus META/NVDA as "NASDAQ/NYSE" = 2),
bypassing the cap of 4 and starving non-US diversification. THE FIX: a new
canonical cap key _market_cap_key() splits the market on "/", trims+uppercases
each token, sorts, and rejoins, so both spellings map to one bucket; single-
token markets (SAU, Tokyo, HKEX, ...) are unaffected. ONLY the cap counter key
is canonicalized — the DISPLAYED market string (cand["market"], every rendered
row) is byte-identical, so no schema/column/display change. Env-gated with a
KILL-SWITCH, DEFAULT ON (TFB_OPP_CANON_MARKET, set =0 to restore byte-identical
v1.0.10 per-spelling counting). One helper + one gate added (2 new defs); all
v1.0.10 functions carried verbatim, none removed. Nothing else changes:
scoring, sizing, gates, ordering, KPIs, near-miss, alerts, and the audit cap
are all untouched.]

v1.0.10 [AUDIT-CAP — env-gated written-audit ceiling (TFB_OPP_AUDIT_ROWS_MAX,
DEFAULT 0 = unlimited = byte-identical v1.0.9); see the inline block at the
candidates_rows assembly. No selection/scoring/sizing change.]

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
  TFB_OPP_SELL_CLASS_GATE  "1"   v1.7.0: MAJOR-fail a candidate whose ENGINE
                                 recommendation is explicit sell-tier
                                 (SELL/STRONG_SELL/REDUCE/AVOID/...);
                                 the narrow replacement for the
                                 all-or-nothing Investability gate;
                                 0 = v1.6.0 byte-for-byte
  TFB_OPP_PREGATE_ORDER    "1"   v1.6.0: when the clamp above WILL cut the
                                 pool, first re-order it eligible-first
                                 (cheap row-local gate mirrors) by
                                 reliability desc so the scan slots go to
                                 trustworthy names; 0 = v1.5.0 positional
                                 cut byte-for-byte
  TFB_OPP_PF_MAX_SECTOR_PCT "30" post-action portfolio sector cap (§4.2)
  TFB_OPP_TRUST_GATE       "1"   v1.0.6: "0" ⇒ skip the Data Trust MAJOR gate
  TFB_OPP_MAX_DATA_AGE_HOURS "168" v1.0.6: last_updated older ⇒ stale ⇒ fail
  TFB_OPP_MIN_TRUST_FIELDS "2"   v1.0.6: fewer core signals present ⇒ thin ⇒ fail
  TFB_OPP_INVESTABILITY_GATE "0" v1.0.8: "1" ⇒ add Investability MAJOR gate (opt-in)
  TFB_OPP_SHARIAH_MODEL_GATE "0" v1.9.0: "1" ⇒ add "Shariah (Model)" MAJOR gate:
                                 NON-.SR symbol in the resolver fail set
                                 (TFB_SHARIAH_FAIL_LIST ∪ TFB_EXIT_BY_RULE_EXTRA)
                                 ⇒ DO_NOT_INVEST. Default OFF (operator flip
                                 = declared version break)
  TFB_OPP_AUDIT_ROWS_MAX   "0"   v1.0.10: 0 = unlimited; >0 caps the written
                                 candidates_rows audit grid to N highest-score
                                 rows (selected / INVEST-qualified / near-miss
                                 always kept) so a full-universe scan stays
                                 inside the GAS/Sheets write limit. Decisions
                                 are unaffected — only the low-score tail is
                                 dropped from the WRITTEN audit.
  TFB_OPP_CANON_MARKET     "1"   v1.0.11: KILL-SWITCH, default ON. Canonicalizes
                                 the per-market cap key so a venue written two
                                 ways ("NYSE/NASDAQ" vs "NASDAQ/NYSE") counts as
                                 ONE market against max_per_market. "0" restores
                                 byte-identical v1.0.10 per-spelling counting.
                                 The displayed market string is never altered.
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
import logging
import math
import os
import re
from datetime import datetime, timedelta, timezone

# =============================================================================
# v1.0.21 [QUALIFIED-GRID BASIS PARITY + HELD-SYMBOL VARIANT MATCH]
# (Engineering Audit Fix #3 completion + Fix #4; two independent switches)
# =============================================================================
# (a) QUALIFIED-GRID BASIS PARITY — completes v1.0.20. The payload has TWO
#     ROI-bearing surfaces: the SELECTED tickets (fixed in v1.0.20) and the
#     candidates_rows audit grid ("ALL QUALIFIED" on the sheet). v1.0.20 left
#     the grid's primary roi_pct/ann_roi_pct on the capped valuation basis,
#     so under TFB_OPP_PRIMARY_ROI_BASIS=engine one page showed two tables
#     with different meanings for the same "ROI %" header. Now, under the
#     SAME switch (no new env), each audit record's primary roi_pct /
#     ann_roi_pct speaks the engine 12M forecast when present (per-row
#     honest fallback to valuation when absent, tagged primary_roi_basis),
#     with valuation_roi_pct / valuation_ann_roi_pct parallels. cand fields,
#     gates, verdicts, scoring, selection: untouched — this swaps the
#     SERIALIZED record only. Default basis "valuation" -> byte-identical.
# (b) HELD-SYMBOL VARIANT MATCH (Fix #4) — the live 2026-07-05 BBD case:
#     "T10: Include Portfolio Holdings = No", portfolio holds BBD.US,
#     Global_Markets carries the same listing as bare "BBD" -> the Portfolio
#     gate's exact-match `cand["symbol"] in held` missed, and the selector
#     recommended a 14,992-SAR NEW ticket in a name already held (541 GM
#     symbols are bare-form; SHG has the same exposure). FIX: the held set
#     is expanded to normalized variants (uppercased; X <-> X.US both
#     directions; suffixed non-US symbols like 1211.SR get no variant) and
#     the gate probes with the normalized symbol. DEFAULT ON — this enforces
#     the documented meaning of an existing user control; kill switch
#     TFB_OPP_HELD_MATCH_VARIANTS=0 restores exact-match byte-identically.
# =============================================================================
# v1.0.20 [ENGINE-PRIMARY-BASIS] — the 35%-cap-as-forecast honesty fix
# (Engineering Audit Fix #3; env-gated TFB_OPP_PRIMARY_ROI_BASIS, DEFAULT
# "valuation" = byte-identical v1.0.19)
# =============================================================================
# ROOT CAUSE (live 2026-07-05 audit): every qualified Top_10 row rendered
# ROI % = Ann ROI % = exactly 35.0 and the KPI "Exp. Gain 12M" = 34,917 SAR
# (~35% x deployed). The valuation reference is capped at
# max_valuation_roi_pct (35), qualified names cluster AT the cap, and the
# rendered primary roi_pct / ann_roi_pct / exp_gain_12m_sar / kpi are all
# derived from that capped VALUATION figure — so the headline "expected
# gain" is a cap artifact, not a forecast. Worst live case: INSW.US, engine
# 12M forecast +12.7%, displayed ROI 35% and Gain 3,466 SAR (2.75x the
# engine's own number). The v1.0.5 display fix surfaced the engine figures
# ALONGSIDE but deliberately left the primary figures intact; this fix lets
# the primary figures themselves speak the engine's forecast.
# FIX: TFB_OPP_PRIMARY_ROI_BASIS = "valuation" (default) | "engine".
#   basis="engine" (per ticket, when an engine 12M forecast EXISTS):
#     roi_pct / ann_roi_pct <- normalized engine 12M %,
#     exp_gain_12m_sar <- suggested x engine %/100 (the reproducibility
#     contract exp_gain == suggested x displayed ann/100 HOLDS under both
#     bases), valuation figures stay under explicit names
#     (valuation_roi_pct / valuation_exp_gain_12m_sar — populated in this
#     mode even if the v1.0.5 display flag is off), the advisor note frames
#     the primary as the engine forecast and the entry/stop/TP ladder as
#     valuation-based LEVELS, and ticket["primary_roi_basis"]="engine".
#   Engine forecast ABSENT for a ticket -> that ticket honestly falls back
#     to the valuation basis, notes it, and tags
#     ticket["primary_roi_basis"]="valuation" (fail-open, never invents).
#   kpis.expected_gain_12m_sar stays == SUM(ticket exp_gain) — the identity
#     is preserved, the BASIS of the addends changes; a parallel
#     kpis.valuation_expected_gain_12m_sar is added in engine mode.
#   SELECTION, GATES, SIZING, ORDERING, STOP/TP LADDER: untouched — the cap
#     still constrains the valuation TARGET and sizing math; it just no
#     longer masquerades as the forecast.
# KILL SWITCH: leave TFB_OPP_PRIMARY_ROI_BASIS unset (or "valuation") ->
# byte-identical v1.0.19 payload.
# =============================================================================
# v1.0.24 (2026-07-18) — SECTOR CAP BASIS: CASH-AWARE DENOMINATOR
# --------------------------------------------------------------------------
# WHY (evidence: cockpit boards 2026-07-16/17 — 18-19 qualified candidates
# across FIVE different sectors ALL deferred "post-action sector weight
# would exceed 30%", Selected 0/10, with Deployable 100,000 SAR against a
# ~5K-SAR current book): both sector-weight checks divided by the CURRENT
# portfolio value only — (a) _sector_context existing-holdings
# pre-saturation, v/pv; (b) the §4.2 post-action check,
# (sector+ticket)/(pv+ticket). With cash >> holdings, ANY first ticket
# (§4.4 sizes ~15% of pv+deployable ≈ 15.7K here) becomes 60-90% of the
# tiny pv-based denominator, so every sector trips and the selector
# deadlocks at 0 forever — while the §4.4 sizer itself already correctly
# uses budget_base = pv + deployable.
# FIX: TFB_OPP_SECTOR_CAP_BASIS = "budget" (new default) makes BOTH checks
# divide by budget_base — the true post-round portfolio (every deployable
# SAR is portfolio after the round, as cash or positions). "legacy"
# restores both v1.0.23 formulas byte-identically (kill-switch).
#
# v1.0.23 (2026-07-08) — TP1 EXECUTION-PLAN ROI BASIS (new default "plan")
# -----------------------------------------------------------------------------
# WHY (operator conviction, live sheet 2026-07-08): the three rendered columns
# ROI % / Engine ROI % / Ann ROI % showed ONE number three times. Mechanism:
# qualified names cluster AT max_valuation_roi_pct (35) — the header's own
# v1.0.20 note predicted this — while the engine's DISPLAYED 12M forecast is
# independently ceiling-capped at the same 35 (engine v5.87.0 display cap +
# the expected-roi cap audit measured 0 rows above 35), and a 12-month panel
# period makes annualization the identity. Result: a 35/35/35 wall that says
# nothing an investor can act on.
# FIX: a third primary_roi_basis value "plan" — the TP1 EXECUTION plan the
# ticket actually proposes: roi = (TP1 - entry)/entry (by construction half
# the valuation upside, since TP1 = price + 0.5*(ref - price)), ann = that
# plan compounded over the panel period (same formula as
# normalize_candidate), exp_gain = suggested x ann/100 (reproducibility
# contract preserved; KPI sum identity holds automatically), detail.rr =
# plan/stop (the R/R of the plan you would actually run). Engine ROI % and
# valuation_roi_pct parallels are ALWAYS emitted under this basis, so the
# three columns finally mean three things: MY PLAN to first target / the
# ENGINE's honest forecast / the plan ANNUALIZED. Honest fallback: a ticket
# without a TP ladder (v1.0.22 TP-COHERENCE blanked it) renders the
# valuation basis for that ticket and is stamped so. Selection, gates,
# scoring, sizing, ordering: UNTOUCHED — this is display semantics only.
# DEFAULT: "plan" (per the 2026-07-08 operator decision). Reversible:
# TFB_OPP_PRIMARY_ROI_BASIS=valuation -> byte-identical v1.0.22 rendering;
# =engine -> the v1.0.20 engine basis. All prior WHYs preserved verbatim.
# Zero functions removed (AST-verified).
# =============================================================================
# -----------------------------------------------------------------------------
# v1.1.0 (2026-07-18) — GEN-2 ADVISOR MATH, STAGE 1 (Wave A, script #7)
# WHY (Master Plan v2.1 §18.2/§18.5/§19.3): tickets must speak COST. Adds a
# per-venue Derayah cost/floor model (official schedule constants of
# 2026-07-18; slippage ledger reconciles), a NET-EDGE annotation
# (p_hit_proxy x ROI − round-trip cost vs hurdle max(3xRT, 1.5%)) and an
# opt-in venue-floor raise feeding the EXISTING v1.0.14 min-ticket deferral
# branch. Both additions are env-gated OFF by default => byte-identical
# champion behavior until flipped:
#   TFB_OPP_NETEDGE_ANNOTATE=1  -> annotation fields only (champion-safe)
#   TFB_OPP_VENUE_FLOORS=1      -> BEHAVIORAL: sub-floor picks defer
# p_hit is an honest PROXY (confidence-band map) until the calibrator
# graduates; the proxy used is printed on every ticket.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.2.0 (2026-07-19) — STOP VOLATILITY UNIT FIX (env-gated, DEFAULT OFF)
# WHY (measured on the live board 2026-07-19): the stop model is
#     stop_pct = max(stop_floor_pct, stop_vol_mult * vol_30d), cap stop_max_pct
# and it has NEVER fired the volatility term. The provider supplies
# `Volatility 30D` as an ANNUALIZED FRACTION (CL.US 0.2802 = 28.0%,
# HSIC.US 0.2375 = 23.7%, SNDK.US 1.4548 = 145.5% — cross-checked against
# real names), while the formula's own documentation calls for a MONTHLYIZED
# PERCENT. So 2.5 * 0.28 = 0.70%, always below the 8% floor, on every row.
# EVIDENCE: all ten names on today's board print a stop distance of exactly
# 8.00% — a tanker and a Japanese pharma given identical stops.
# FIX: convert fraction-shaped input to monthlyized percent
# (v * 100 / sqrt(12)) before the multiplier. Shape-detected, so a provider
# that later supplies percent-shaped values is left alone.
# CONSEQUENCE — READ BEFORE ARMING: correct stops are WIDER (Colgate-like
# 28% vol -> ~20% stop at mult 2.5, vs 8% today). Wider stops mean SMALLER
# positions for the same 0.75% risk budget (0.75/20 = 3.75% of equity vs
# 0.75/8 = 9.4%), which pushes tickets BELOW several venue floors — Japan's
# 13.2K in particular. Arming this without lowering stop_vol_mult will make
# Japan unaffordable. Recommended pairing: TFB_OPP_STOP_VOL_MULT=1.5.
# GATE: TFB_OPP_STOP_VOL_UNITS_FIX (default OFF) — committing changes NOTHING.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.3.0 (2026-07-20) — REF-CONSERVATIVE: ROI% 35.0/17.5 SATURATION ROOT FIX (D-12)
# WHY (R1 defect; audit 2026-07-19 "ROI column saturated despite armed softcap"):
# TFB_SCORE_ROI_SOFTCAP (scoring.py v5.10.0) differentiates expected_roi_*, but
# the displayed ROI% never reads that field: ref = target_price|intrinsic_value,
# and both arrive display-capped (engine v5.87.0 Fix AG pins intrinsic_value at
# exactly price*1.35 on every overshooting row) -> roi_pct = exactly 35.0 and
# the TP1 plan basis (price+0.5*(ref-price)) = exactly 17.5. The armed softcap
# was structurally unreachable from the column whose saturation it names.
# FIX: env TFB_OPP_REF_CONSERVATIVE (default OFF -> byte-identical v1.2.0).
# When ON in normalize_candidate: ref = min(valuation ref, price*(1+engine 12M
# forecast/100)) for a POSITIVE engine forecast only; valuation_basis =
# "engine_forecast_min" when it binds. Only ever SHRINKS an inflated claim;
# tp1/tp2/roi_pct/ann_roi/rr and the plan basis inherit the engine's order-
# preserving differentiation; honest refs stay untouched; mos_pct remains a
# raw-intrinsic valuation concept. KILL: unset/0 -> v1.2.0 exactly.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.4.0 (2026-07-21) — W-2 QUOTE-FRESHNESS GATE (Execution Plan v2.1; Register §4.3)
# WHY (evening audit 2026-07-20, export __39_): the cockpit built its #1 ticket
# on EXE.US at 88.13 while Market_Leaders held the 86.95 Monday close (~1.3%
# stale at build); NFG.US and 0083.HK rode 2-day-old rows; ENKAI.IS repeated
# its ~14% lesson. The existing v1.0.6 Data-Trust ceiling (168h) passed every
# one of them — a weekly-scale guard cannot catch session-scale staleness.
# FIX — venue-session-aware freshness on the price feeding a ticket:
#   * quote age <= TFB_TICKET_MAX_QUOTE_AGE_MIN (default 15) => LIVE, pass;
#   * else, venue calendar (exchange_calendars, lazy, fully defended):
#       session OPEN  => FAIL "STALE_PRICE intraday" (an old quote while the
#                        market trades is exactly the EXE failure);
#       session CLOSED => pass iff quote_ts >= last scheduled close − 2min
#                        (the last regular close is fresh by definition —
#                        Register §4.3 — so Friday closes stay valid all
#                        weekend); older => FAIL "STALE_PRICE pre-close";
#   * calendar unavailable / unknown suffix => permissive fallback: pass iff
#     age <= TFB_TICKET_FALLBACK_MAX_AGE_H (default 78h — covers Fri→Mon on
#     any venue), never a false weekend block;
#   * unparseable/absent timestamp => SKIPPED (pass), preserving the v1.0.6
#     philosophy: freshness fails only on PROVEN staleness.
# Failure class MAJOR => DO_NOT_INVEST: the candidate DEFERS — visible in the
# audit grid / Why-Not with the STALE_PRICE tag, never sized, never selected.
# GATE: TFB_TICKET_FRESHNESS_GATE, DEFAULT ON (protective guards ship armed —
# the v5.116.0 default-OFF lesson). Kill: =0 => gate list byte-identical to
# v1.3.0. Venue map: .SR→XSAU ·.T→XTKS ·.HK→XHKG ·.IS→XIST ·.L→XLON ·.AS→XAMS
# ·.BR→XBRU ·.PA→XPAR ·.DE→XETR ·.F→XFRA ·.MI→XMIL ·.MC→XMAD ·.ST→XSTO
# ·.TO→XTSE ·.SW→XSWX · US/none→XNYS.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.4.1 (2026-07-21 morning audit, export __47_) — VENUE-CALENDAR RESILIENCE
# EVIDENCE (defect-class, window-legal): at the 07:22 Riyadh cockpit run the
# board sized PAM.US (16.4h old, XNYS closed, quote PREDATES Monday's close),
# MRP.US (16.3h, same class) and 4503.T (6.2h old DURING the Tokyo session) —
# all three are exactly what the venue path must defer, and all three pass the
# 78h fallback. Conclusion: _venue_state is degrading to fallback in
# production while the same code+exchange_calendars 4.13.2 works in the test
# environment. Prime suspect: tz-AWARE Timestamp rejection inside
# previous_close/previous_open under the Render pandas build.
# FIX: (a) tz-robust lookup — try the aware Timestamp, on ANY exception retry
# tz-naive-UTC; (b) the silent degradation becomes VISIBLE: one WARNING per
# venue-suffix per process, `[FRESHNESS v1.4.1] venue calendar unavailable
# for '.US' -> 78h fallback (…)`, carrying the captured error. The fallback
# REMAINS the safety net — this build makes the precise path work and makes
# any remaining degradation announce itself, never widen silently.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.5.0 (2026-07-21 PM, operator-approved) — COMPLIANCE + ELIGIBILITY GATES
# WHY: the same morning the operator exited 1050.SR by rule (authority FAIL),
# the universe surface ranked it #1 INVEST — the rulebook lived beside the
# decision surfaces, not inside them. And two operator-eligibility facts are
# now law: he is Nomu-UNQUALIFIED (all 9xxx.SR barred) and foreign-restricted
# from specific symbols (4030.SR broker-rejected). FIX — two MAJOR gates on
# every candidate, decision-surface-enforced:
#   "Shariah (KSA)":     .SR on the official authority FAIL list => blocked
#                        (AUTHORITY_FAIL). List sources, in order: env
#                        TFB_SHARIAH_FAIL_LIST (CSV, replaces) else the
#                        compiled Al-Rajhi Q1-2026 default (12 names,
#                        as_of 2026-03-31 — refreshed quarterly).
#   "Eligibility (KSA)": 9xxx.SR => NOMU_BLOCKED; symbols in
#                        TFB_KSA_FOREIGN_RESTRICTED (default "4030.SR")
#                        => FOREIGN_RESTRICTED.
# Globals stay outside these gates by design (official list governs KSA;
# the model screen governs globals in the Gen-2 layer — the operator's
# rule). compliance_rule_sets() is the single resolver, imported by
# portfolio_actions for the held-side EXIT-BY-RULE.
# Kills: TFB_COMPLIANCE_SURFACE_GATE=0 / TFB_ELIGIBILITY_GATE=0 restore the
# v1.4.1 gate list byte-for-byte. Guards ship armed.
# -----------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# v1.9.1 (2026-08-05) [B1b-1/2 FORECAST-SOURCE PASSTHROUGH]
# WHY: the 2026-08-05 workbook audit proved 7 of 10 portfolio holdings on
# phase_ii_synthetic forecast basis, and Portfolio_Decision (01:01) emitted a
# valuation TRIM on 2222.SR from a synthetic -9.7% while the engine's live
# read was BUY +23.4%. portfolio_actions.decide_action cannot defer on basis
# because the candidate contract never carried it: the sheet column
# "Forecast Source" (My_Portfolio col 106 / GM col 100) was dropped at
# normalize_candidate. CHANGE (additive only): a "forecast_source" alias +
# one always-present string key on the cand dict ("" when absent). No gate
# reads it here; payload rows project explicit keys, so served bytes are
# unchanged. Consumer: portfolio_actions v1.7.4 TFB_PF_REQUIRE_FRESH_BASIS
# (default OFF). Zero removals.
# ---------------------------------------------------------------------------
# =============================================================================
# v1.9.2 (Fix SC — SCAN COVERAGE + UNCAPPED GUARANTEE)  2026-08-06
# -----------------------------------------------------------------------------
# ROOT CAUSE (live evidence, Top_10 run 2026-08-06 09:07:44): the cockpit KPI
# showed Scanned 2,000 of a sheets pool of 9,824 (footer pool=body_rows/9824).
# kpis["scanned"] = len(audit), and audit is built AFTER
# rows = rows[:crit["max_candidates"]] — so "scanned" reports the POST-CLAMP
# count, not the pool. make_criteria precedence is DEFAULTS < env < request
# overrides; the run's request carried max_candidates=2000, which no Render
# env value can undo. Consequence proven in the 2026-08-06 morning audit:
# GM INVEST rows with reliability 86–92 (O39.SI, NA.TO, 8306.T, AROW, OBT,
# PLBC) were absent from the 300-row audit grid AND from every gate-failure
# line — never evaluated, not filtered.
# FIX (both DEFAULT OFF / additive — byte-identical v1.9.1 until enabled):
#   SC-1  TFB_OPP_SCAN_UNCAPPED=1 forces max_candidates → 0 (unlimited) even
#         against a request-supplied cap (one INFO line when it fires; the
#         criteria_snapshot then shows the EFFECTIVE 0).
#   SC-2  Coverage telemetry, additive kpis on every run: pool_received and
#         scan_coverage_pct; plus scan_clamped / scan_clamp / pregate_ordered
#         when a clamp actually cut, and scan_cap_overridden when SC-1 fired.
#         GAS ignores unknown keys; run_daily_brief reads kpis tolerantly.
# ZERO-REMOVAL: no function removed; clamp path and PREGATE-ORDER (default
# ON) untouched when the switch is OFF.
# =============================================================================
# ---------------------------------------------------------------------------
# v1.10.0 — [FORECAST-PROVENANCE GATE]
# ---------------------------------------------------------------------------
# EVIDENCE (workbook export 2026-08-06 14:17 board vs 14:18-14:28 sync leg,
# adjudicated the same day). The board is built on forecast values that
# EVAPORATE minutes later. Same symbols, board value -> post-leg row value:
#   F34.SI  (selected #1)  rel 86.8 / ROI 34.7%  ->  rel 52.9 / 9.4%  HOLD
#   6592.T  (selected)     rel 80.9 / ROI 35.0%  ->  rel 52.9 / 12.3% HOLD
#   3401.T  (the one sized executable ticket, 20,890 SAR)
#                          rel 84.3 / ROI 35.0%  ->  rel 53.0 / 13.2% HOLD
#   2503.T                 rel 87.6              ->  rel 53.2        HOLD
#   DDI.US                 rel 70.4              ->  rel 31.3        SELL
#   0083.HK                rel 76.5              ->  rel 31.3        SELL
# Every one of those rows carries Forecast Source = "phase_ii_synthetic"
# after the leg. The rows written BEFORE the run and still matching the
# board (CHDRAUIB.MX rel 70.4 @12:58, SON.LS rel 89.5 @14:05) carry
# "provider_target". Universe-wide the split is 3,801 synthetic vs 2,210
# provider-backed on Global_Markets (151/104 on Market_Leaders) — 63% of
# the pool is a synthesized forecast wearing a reliability score.
# CONSEQUENCE: the raw selection churns every leg, all ten incumbents fall
# to GRACE, and the board cannot converge — the empty board of 2026-08-06
# is a SYMPTOM of this, not a stability-layer defect.
# REJECTED APPROACH: value-fingerprint matching on {70.4, 71.5, 75.4, 76.5}
# (the original B4 sketch). The same export DISPROVES it — CHDRAUIB.MX
# carries 70.4 with a genuine provider_target, so fingerprinting would
# have produced false positives on real forecasts. Provenance is the only
# sound discriminator, and the field is already captured (v1.9.1, the
# "forecast_source" alias in normalize_candidate) but was never gated.
# FIX: a "Forecast Provenance" MAJOR gate — a candidate whose
# forecast_source is a synthesized token (default "phase_ii_synthetic";
# tune via TFB_T10_SYNTHETIC_SOURCES) fails MAJOR => DO_NOT_INVEST: it
# still appears in the audit grid and near-miss surface with its true
# blocking gate, but can never be sized. BLANK / UNKNOWN PASSES —
# fail-open, matching the Investability and Sell-Class convention: a
# missing provenance column must never empty the board.
# GATE PLACEMENT: appended immediately after "Forecast" (both concern the
# engine forecast) and registered at that TRUE position in GATE_ORDER —
# the v1.0.7 lesson, so first_failed_gate attributes the near-miss and the
# DATA GAPS blocking-gate table correctly instead of sorting it to 99.
# ENV STYLE: read directly at gate time (the v1.4.0 Quote Freshness
# precedent) rather than through criteria — the token list is non-scalar
# and the criteria coercion tuples stay untouched.
# GATE DEFAULT: **OFF**. This gate CHANGES recommendations, and the S-1
# certification window (closes 2026-08-16) forbids silent alteration of
# recommendations/tickets/shadow-board evidence. TFB_T10_EXCLUDE_DEFAULT_CONF
# unset/0 => v1.9.2 gate list, verdicts and tickets byte-for-byte. The
# operator arms it deliberately. (The default-armed rule for guards is
# deliberately NOT applied here: an armed selection-changing gate would
# restart the 28-day evidence clock.)
# ZERO functions removed. Additions: _env_forecast_provenance_gate,
# _env_synthetic_source_tokens, _forecast_provenance_assessment.
# ---------------------------------------------------------------------------
# v1.10.3 [B4b RELIABILITY-CLUSTER GATE — the fingerprint B4 cannot see]
# WHY: the v1.10.0 Forecast Provenance gate keys on forecast_source ==
# phase_ii_synthetic and fails open on blank / provider_target — correct
# for its threat (a synthesized forecast sized as a ticket), but the
# operator-confirmed default-confidence ARITHMETIC fingerprint rides rows
# whose forecast_source is provider_target or blank: reliability lands
# EXACTLY on the cluster 70.4 / 71.5 / 75.4 / 76.5. Evidence (2026-08-11
# board, run 09:18:48, req 23ee5d199004, gate armed): DDI.US rel 70.4
# seated as a day-1 FAST-TRACK ticket with earnings <=0d; PCG.US rel 70.4
# pending 1/3; SAFE.L / ARCO.US 71.5 and the 76.5 crowd across the
# qualified 50 — zero "Forecast Provenance" First-Fails in 300 audit rows
# because the key never matches. Reliability flapping on these rows is the
# documented churn driver (the 08-09 seven-name GRACE cascade). Until now
# the only defense was the operator's manual Phase-0 discard rule.
# WHAT: a "Reliability Cluster" gate, appended immediately AFTER Forecast
# Provenance and BEFORE the tiered Reliability gate (same GATE_ORDER logic:
# provenance-class rejections must attribute first_failed_gate ahead of the
# generic reliability tiers). ok=False only when round(reliability, 1)
# equals a member of the cluster set; None / unparseable reliability PASSES
# (fail-open — the tiered Reliability gate below owns the missing-value
# verdict). Fails MAJOR => DO_NOT_INVEST; appears in audit / near-miss /
# DATA GAPS, never selected, never sized.
# ENV STYLE: read directly at gate time (the v1.4.0 Quote Freshness
# precedent; v1.10.0 followed it) — the value set is non-scalar and the
# criteria coercion tuples stay untouched.
#   TFB_T10_EXCLUDE_REL_CLUSTER   (kill-switch, DEFAULT OFF: unset/0 =>
#                                  v1.10.2 gate list, verdicts and tickets
#                                  byte-for-byte)
#   TFB_T10_REL_CLUSTER_VALUES    (csv override of the cluster values;
#                                  ';' accepted as ','; blank/unparseable
#                                  => default 70.4,71.5,75.4,76.5 — a newly
#                                  identified cluster value needs no code
#                                  change)
# GATE DEFAULT: **OFF**. This gate CHANGES recommendations, and the S-1
# certification window (closes 2026-08-16) forbids silent alteration of
# recommendations/tickets/shadow-board evidence. The operator arms it
# deliberately, as a declared separate act, exactly like v1.10.0.
# ZERO functions removed. Additions: _env_rel_cluster_gate,
# _env_rel_cluster_values, _rel_cluster_values_text,
# _rel_cluster_assessment.
# ---------------------------------------------------------------------------
# v1.11.0 [F-1 VENUE BOARD LOTS — the sizing defect the floors could not see]
# WHY: _size_one has lot-floored sizing since §4.4 — but the lot is one
# GLOBAL criterion (TFB_OPP_LOT_SIZE, default 1). Venues with mandatory
# board lots therefore received odd-lot tickets: the 2026-08-11 boards
# sized 6960.T at 64 sh, 4503.T at 327 sh and 3401.T at 438 sh against
# TSE's uniform 100-share unit — three consecutive un-executable tickets
# in one day. The v1.1.0 venue FLOORS (§18.5/§19.3) raise the minimum
# ticket in SAR but know nothing about share multiples, so a ticket can
# clear the floor and still be unplaceable.
# WHAT: per-venue board lots by symbol suffix, layered over the global
# criterion inside _size_one (lot = max(1, criteria lot, venue lot)), plus
# an honest deferral at the pick loop when the venue lot alone prices a
# name out (allocation buys >= 1 share but < 1 lot) — otherwise 6960.T
# would have read as capital exhaustion with 18k SAR still on the table.
# ENV (read at gate time, the v1.4.0/v1.10.x precedent):
#   TFB_T10_VENUE_LOTS   unset/blank => feature OFF, sizing and reasons
#                        byte-identical to v1.10.3 (S-1 window law).
#                        "default" => the built-in certain set
#                        T:100, SI:100, KL:100 (venues with a uniform,
#                        exchange-mandated board lot). Or an explicit csv
#                        "T:100,TW:1000,BK:100" (';' accepted as ',');
#                        unknown suffixes keep lot 1. Varying-lot venues
#                        (e.g. HK) are deliberately NOT defaulted — the
#                        operator adds them only with confirmed values.
# GATE DEFAULT: **OFF**. Arming changes tickets, so the flip is the
# operator's explicit decision, exactly like v1.10.0/v1.10.3.
# ZERO functions removed. Additions: _env_venue_lots,
# _venue_lot_for_symbol. One line changed inside _size_one; one deferral
# block added in the pick loop.
# ---------------------------------------------------------------------------
# v1.12.0 [B4c CAP-BAND GATE — the manufactured-target fingerprint]
# WHY (2026-08-11 evening forensics, runs eabc8e962bdb → a10e7cb52282 +
# Actions #3271 artifacts): the inline FULL-FILL recovery (12:56–13:56,
# 3 cycles, each blocked at 230–239/268 batches yet writing all 6,626 rows
# live) minted 12M targets that are EXACTLY price × ~1.35 and stamped them
# provider_target with re-rolled off-cluster reliabilities: VTMX.US
# ×1.3445 (rel 76.5→82.4), MCY.US ×1.3463 (75.4→86.5), PTTEP.BK ×1.3498
# (31.3→77.2), 0016.HK ×1.3500 (76.5→73.7). B4 gates the LABEL and B4b
# gates the old cluster values — both blind to a manufactured VALUE wearing
# a passing label. Until B4a (true value-level provenance from the engine)
# lands post-S-1, this gate blocks the fingerprint itself: any candidate
# whose implied 12M ratio (1 + engine_roi_12m_pct/100, normalized) falls
# inside the manufactured band is excluded. Honest cost, stated plainly:
# genuine ~35% forecasts are also excluded while armed — pre-calibrator,
# a real 35% and a manufactured 35% are indistinguishable, and tonight
# proved which is more common.
# ENV (read at gate time):
#   TFB_T10_EXCLUDE_CAP_BAND  unset/0 => gate absent, list and verdicts
#                             byte-identical to v1.11.0 (S-1 window law).
#                             1/true/yes/on => armed.
#   TFB_T10_CAP_BAND          band as "lo-hi" (also ':' or ','), default
#                             1.335-1.365. Junk falls back to the default.
# ALSO [GATE_ORDER — self-caught v1.10.3 omission, owned]: the
# "Reliability Cluster" gate was appended in evaluate_gates but never
# registered in GATE_ORDER, so first_failed_gate sorted it to 99 and could
# mis-attribute the near-miss surface (the v1.0.7 lesson, violated by the
# v1.10.3 build itself). Both "Reliability Cluster" and the new
# "Forecast Cap Band" are now registered at their true append positions.
# Selection was never affected — attribution ordering only.
# GATE DEFAULT: **OFF**. ZERO functions removed. Additions:
# _env_cap_band_gate, _env_cap_band, _cap_band_assessment.
# ---------------------------------------------------------------------------
# =============================================================================
# v1.14.0 (2026-08-24) — ROI-TRUTH: ONE MEANING PER KEY ON THE AUDIT TAIL
# =============================================================================
# EVIDENCE (Morning Review + adjudication 2026-08-24): the same symbol carried
#   three ROI stories — board plan-TP1 10.7% (ENELCHILE.SN), audit tail 21.5%
#   in the SAME rendered column, engine 21.5% — because audit records emit
#   VALUATION upside in the primary roi_pct key while the board renders
#   plan-TP1 under the "ROI % (TP1)" header. MECHANISM ADJUDICATION: the 12%
#   ROI gate tests VALUATION upside and was never fed engine values (external
#   "gate bypass by substitution" claim REFUTED at this file); the defect is
#   emission ambiguity, not gating. FIX: under the live default basis "plan",
#   _audit_align_plan_roi() rewrites the audit record's primary roi/ann to
#   plan-TP1 AFTER gates/verdict/score (selection byte-identical); valuation
#   preserved in valuation_roi_pct/_ann; missing ladder => None + D-25 tag
#   roi_basis_note=TP1_UNAVAILABLE(DATA_GAP). New pure helper _tp1_plan_roi()
#   is the single plan-TP1 definition. KILL SWITCH TFB_OPP_AUDIT_ROI_LEGACY=1
#   restores v1.13.0 bytes. Zero removals; all prior WHYs preserved verbatim.
# =============================================================================
# =============================================================================
# v1.15.0 (2026-08-26, One-Pass Batch #5b) — CAPITAL HONESTY SWITCHES
# WHY (2026-08-26 audit): Portfolio_Decision displayed Deployable 22,923 SAR
# on broker cash of 14,700 — 74.7% of it UNEXECUTED OTIS/SHG sale proceeds,
# because the L7 doctrine defines Deployable = cash + pending TRIM/EXIT
# proceeds, and sizing divides by CURRENT price while the advisor advertises
# entry up to price*1.01. Two opt-in gates, both DEFAULT OFF = v1.14.0
# byte-identical; the operator arms each against evidence:
#   TFB_OPP_FUNDING_SETTLED_ONLY=1  -> proceeds contribute ZERO to funding
#       (deployable = settled cash only; _funds_from proceeds leg starts 0).
#   TFB_OPP_SIZE_AT_ENTRY_HIGH=1    -> share sizing divides by the WORST
#       advertised entry (price_sar * 1.01), so a fill at entry-high cannot
#       breach the allocation the ticket promised.
# Additive, ungated: kpis expose deployable_current_sar (cash-only view) and
# deployable_proforma_sar (doctrine view) so every surface can label which
# number it is showing. No recommendation logic touched.
# =============================================================================
# v1.15.1 (2026-08-26, A4 self-audit catch): under SIZE_AT_ENTRY_HIGH the
# v1.15.0 draft sized shares at price*1.01 but still RESERVED shares*price —
# a ~1% under-reservation, the exact failure the switch exists to prevent.
# Fixed: in that mode `suggested` (the reserved/booked ticket) is
# shares * worst-entry too, so Σ suggested can never be breached by a fill
# at the advertised entry-high. OFF remains v1.14.0 byte-identical.
OPPORTUNITY_BUILDER_VERSION = "1.19.2"
# -----------------------------------------------------------------------------
# v1.19.2 (2026-09-02) - ELIGIBILITY (VENUE): the operator's tradable venues
# -----------------------------------------------------------------------------
# EVIDENCE (Top_10 11:10 Riyadh, first populated board: 112 qualified): the
# single Selected ticket was HDFCBANK.NS (NSE India) and the qualified set
# carried 2317.TW, TRAN.BA, BMRI.JK - venues a foreign retail account at
# IBKR/Derayah generally cannot trade. Eligibility (KSA) fences Tadawul
# (Nomu, foreign-restricted) but nothing fenced the VENUE for the other
# 9,500 rows, so "Passed" - the KPI for board-originated trades - counted
# names that can never become a ticket.
# CHANGE: a new "Eligibility (Venue)" gate (FAIL_MAJOR, right after
# Eligibility (KSA) in GATE_ORDER) passes a candidate iff its market name
# OR its symbol suffix is in TFB_T10_VENUE_ALLOWLIST (CSV, case-insensitive;
# a bare ticker counts as suffix US). Examples of tokens: US, NYSE, NASDAQ,
# SR, TADAWUL, L, PA, DE, SW, T, HK, TO, AX. Fail-open: an unknown market
# AND an absent suffix pass ("venue unknown") so a mapping gap can never
# empty the board.
# GATE: TFB_T10_VENUE_ALLOWLIST unset/empty = gate NOT appended -> v1.19.1
# gate list, verdicts and payload byte-identical. Arming is the operator's
# explicit decision (declared version break; alters candidate eligibility).
# Functions added: 2 (_env_venue_allowlist, _venue_eligibility). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.19.1 (2026-09-02) - AUDIT-DEPTH-ORDER: the written audit and the near-miss
#          surface are ordered by HOW FAR a row got through the gate chain,
#          not by opportunity score.
# -----------------------------------------------------------------------------
# EVIDENCE (Top_10 exports 09-01 00:51 and 09-02 09:10/09:40, pool 9,786):
# the written audit is capped (TFB_OPP_AUDIT_ROWS_MAX=300 on the instance) and
# the remaining slots were filled by the HIGHEST-SCORING failures. Junk
# micro-caps with absurd fair-value gaps score 76.9-82.0, so all 300 slots and
# all 10 near-miss slots were Valuation Sanity failures every day (294/300,
# 297/300, 290/300), while the 14 sane candidates that pass Rel/DQ/Sanity/ROI
# by hand were NOT WRITTEN AT ALL - their first-fail gate was unknowable from
# the sheet. A controlled probe (panel Min R/R 2.0 -> 1.0) took Passed 0 -> 3
# and the 11 others were still invisible. The operator could not see the
# system's own best candidates or why they failed.
# CHANGE (display-only; selection, tickets, kpis, alerts byte-identical):
#   TFB_OPP_AUDIT_ORDER = depth (default) rows ordered by (INVEST first,
#                                 deepest first-fail gate in GATE_ORDER, then
#                                 reliability desc, score desc, symbol). Rows
#                                 that died at gate 5 (Valuation Sanity) sort
#                                 behind rows that reached gate 24
#                                 (Risk/Reward). Applied to the pre-sort, the
#                                 cap fill and the near-miss pool.
#                       = score   kill-switch: v1.19.0 ordering byte-for-byte.
# Functions added: 2 (_env_audit_order, _audit_depth_key). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.19.0 (2026-09-01) - RELIABILITY FLOOR MODE (H-28 consequence, unarmed)
# -----------------------------------------------------------------------------
# EVIDENCE (H-28 backtest, 5,401 deduplicated decided cohorts, 2026-09-01):
# stated Forecast Reliability carries no outcome information - win rate 61-63%
# in every band below 85 (51.6% at 85-100, n=91), raw value as probability
# Brier 0.268 (worse than a coin flip), every calibration scheme equals the
# base rate out of sample, Spearman vs realized ROI 0.033. The Top-10 floor
# "Min Reliability >= 70" therefore removes candidates (6 of 16 sane names on
# 08-31) without changing their odds.
# CHANGE (env-gated, DEFAULT = v1.18.1 behaviour; the operator decides):
#   TFB_T10_REL_FLOOR_MODE = gate    (default) reliability floor excludes as before
#                          = display reliability is shown, never excludes: the
#                                    Reliability gate passes with the note
#                                    "display-only (H-28)"; the first-pass
#                                    eligibility filter ignores the floor too.
#   In 'display' the L8 low-confidence cap (same score) is lifted as well.
#   The DQ floor, the reliability-cluster gate (B4a/b) and every other gate are
#   untouched: this only stops a non-separating score from gating.
# Functions added: 1 (_env_rel_floor_mode). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.18.1 (2026-09-01) - ROTATION RULE READS THE HOLDINGS FIELDS THE GAS NOW SENDS
# -----------------------------------------------------------------------------
# 16_Decision_Top10.gs v1.11.0 adds market / buy_date / tp1_sar (and, when it
# carries it, price_sar) to each holding in the request. Two ratified rotation
# criteria that v1.18.0 could not evaluate become live, fail-open per field:
#   - held >= TFB_OPP_ROTATION_MIN_HELD_DAYS calendar days (default 7 ~ 5
#     trading days); a holding without buy_date is NOT excluded (unknown age).
#   - not within TFB_OPP_ROTATION_TP1_PROXIMITY_PCT (default 3) below its TP1:
#     needs BOTH tp1_sar and price_sar on the holding; otherwise not applied.
# Functions added: 1 (_holding_rotation_eligible). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.18.0 (2026-08-31, 10-day program Day 3) - FUNDING-STATE LAYER: DISCOVERY IS
#          INDEPENDENT OF CASH (operator principle ratified 2026-08-31)
# -----------------------------------------------------------------------------
# WHY: the board's job is to name the best opportunities in the universe
# regardless of the wallet; funding is a SEPARATE layer. Until now the two
# capital mechanisms (v1.0.9 unfunded_watch, v1.0.14 ticket floor) only HID a
# qualified name ("Unfunded ...") - they never said how it could be taken.
# CHANGE (additive; response contract preserved; kill-switch below):
#   Every rank-ordered name that qualified but could not be funded gets ONE
#   funding state, appended to its existing Funding near-miss reason text and
#   counted in kpis/alerts:
#     FUNDABLE_NOW          - already a sized ticket (unchanged path; the
#                             Selected count is FUNDABLE_NOW only, as before).
#     FUNDABLE_BY_ROTATION  - a held equity is worse by >= edge_pp of engine
#                             12M forecast after round-trip cost: propose
#                             TRIM/EXIT <holding> with the SAR proceeds and the
#                             edge. Max ONE rotation proposal per run; sukuk
#                             excluded (TFB_OPP_ROTATION_EXCLUDE, default
#                             5023.SR). Holdings' forecasts are read from the
#                             same scanned pool (exact symbol, then .US alias).
#     CAPITAL_CALL          - deposit >= shortfall to take the ticket at the
#                             venue floor / sized amount (top-N only in the
#                             alert, TFB_OPP_CAPITAL_CALL_TOPN default 3).
#   kpis: fundable_now, fundable_by_rotation, capital_call,
#         capital_call_topn_sar. alerts: rotation_proposal, capital_call.
#   NOT implementable from today's payload (holdings carry symbol/sector/
#   market/value_sar only): "not within 3% of TP1" and "held >= 5 trading
#   days" - both need tp1_sar/buy_date in the GAS holdings payload
#   (16_Decision_Top10.gs); registered, applied when the payload carries them.
#   A rotation proposal is a TICKET, never an order: written GO with a live
#   quote still governs execution.
# GATE: TFB_OPP_FUNDING_PLAN default ON; =0/false/off/no => v1.17.0
#   byte-identical (no text, no kpis, no alerts). Knobs:
#   TFB_OPP_ROTATION_EDGE_PP (8.0), TFB_OPP_ROTATION_COST_PCT (1.1),
#   TFB_OPP_ROTATION_EXCLUDE ("5023.SR"), TFB_OPP_CAPITAL_CALL_TOPN (3).
# Functions added: 9. Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.17.0 (2026-08-31, 10-day program Day 2) - TWO SEAMS THE 08-31 BOARD DIED ON
# -----------------------------------------------------------------------------
# EVIDENCE (Top_10 tab 08:47:17 + page census): Scanned 9,786, Passed 0. Of the
# 16 names that clear BUY/INVESTABLE/DQ>=80/Rel>=70, ELEVEN sit in the 30-35%
# band because the engine soft-caps genuine analyst targets to the phase-II
# ceiling (151 GM rows tagged provider_target_12m_capped_to_phase_ii_ceiling)
# and the armed B4c cap-band gate then excludes the wall as a minted
# fingerprint - NOS.LS blocked at exactly x1.3500 on a provider_target row.
# B4c's own WHY block states the cost: pre-provenance, a real 35% and a
# manufactured 35% were indistinguishable. Engine v5.134.0 (Fix AI) now
# publishes the RAW analyst target into the Target Price column, which gives
# the value-level provenance B4c was waiting for:
#   (1) CAP-BAND RAW-TARGET EXEMPTION: a candidate whose engine ratio sits in
#       the band BUT whose RAW target/price ratio is strictly ABOVE it
#       (> hi + 0.01) is a genuine target the engine compressed, not a
#       price x 1.35 mint (a mint lands IN the band on both readings). Such a
#       row passes the gate with "(raw xR)" shown; a row with no raw target,
#       or a raw ratio inside/below the band, is treated exactly as v1.16.0.
#       Kill-switch TFB_T10_CAP_BAND_RAW_EXEMPT (default ON; 0 = v1.16.0).
#   (2) _to_float ACCEPTS THE SHEET'S DISPLAY GLYPHS: 255/255 ML and 38% of GM
#       cells arrive as "(up-arrow) 31.90%" / "(down-arrow) -3.82%"; the old
#       parser returned None, so the Forecast gate read "Unknown passes" on
#       those rows and the reliability/DQ floors could not see them. The
#       glyphs are stripped before the numeric regex; every value that parsed
#       before parses identically (pure widening, no unit conversion here).
# Functions added: 1 (_env_cap_band_raw_exempt). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v1.16.0 (2026-08-28) - CONSTRAINTS PACK, ALL THREE ENV-GATED / DEFAULT-OFF
# -----------------------------------------------------------------------------
# FORENSIC WHY (2026-08-27 board, live): the 1015.KL ticket (6,293 SAR MYR,
# 957 shares) cleared every gate while breaching, on PD arithmetic, the 30%
# sector cap (Financials 30.41%) and the operator cash floor (left 8,407 vs
# 8,915), and while ignoring the Bursa 100-share board lot - because:
#   (a) the 'budget' sector basis divides by pv+deployable, diluting the cap
#       (26.1% pass vs 30.41% on the PD base);
#   (b) no cash-floor concept existed builder-side at all;
#   (c) TFB_T10_VENUE_LOTS carried the typo "defult", which the parser
#       silently collapsed to {} = feature OFF - a gate asleep for weeks with
#       zero telemetry; and _VENUE_COSTS had no .KL row, so even an armed
#       floor gate returned None for Bursa.
# CHANGES (each byte-identical until its ENV is set):
#   (1) TFB_OPP_SECTOR_CAP_BASIS gains 'pd': denominators use
#       portfolio_value ONLY, matching Portfolio_Decision arithmetic, in BOTH
#       _sector_context and the S4.2 application. 'budget' stays the default.
#   (2) NEW TFB_OPP_CASH_FLOOR_SAR (default '' = off): an absolute SAR
#       reserve subtracted from cash_left AND deployable before any pick is
#       funded. Semantics-free by design (no pv-inclusion guessing).
#   (3) _env_venue_lots(): an unparsable NON-EMPTY value now emits ONE loud
#       _LOG.warning naming the raw value and stays OFF (observability only;
#       no behavior flip on a live misconfig). _VENUE_COSTS gains a .KL
#       row so the ALREADY-GATED floor feature covers Bursa when armed
#       (.SI pre-existed at 16,700 further down the map - left untouched).
# Functions added: 1 (_cash_floor_sar). Removed: 0.
# -----------------------------------------------------------------------------

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
    "max_selected": 10,
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
    "max_per_market": 10,
    "include_portfolio_holdings": False,
    "base_currency": "SAR",
    # sizing / mechanics (env-overridable; see policy block)
    "max_weight_pct": 15.0,
    "lot_size": 1,
    # v1.0.14: minimum executable-ticket floor in SAR. DEFAULT 0.0 = OFF
    # (byte-identical). When >0, a sized ticket whose suggested SAR is below
    # this floor is NOT rendered as an executable ticket — it is deferred with
    # a "below minimum ticket floor" reason (selected = No), so the greedy
    # sizer stops spending the last scraps of cash on sub-floor 1-2 share
    # positions and the funded list contains only properly-sized tickets.
    "min_ticket_sar": 0.0,
    "near_miss_n": 10,
    "review_days": 30,
    "stop_floor_pct": 8.0,
    "stop_vol_mult": 2.5,
    "stop_max_pct": 35.0,
    "pf_max_sector_pct": 30.0,
    "max_candidates": 0,
    "sell_class_gate_enabled": True,
    # v1.0.3 forecast safety gate (env-tunable; see policy block)
    "forecast_gate_enabled": True,
    "min_engine_roi_pct": 0.0,
    # v1.0.4 valuation-sanity gate (env-tunable; see policy block)
    "valuation_sanity_gate_enabled": True,
    "max_valuation_roi_pct": 80.0,
    # v1.0.5 engine-forecast display (env-tunable; see policy block)
    "engine_roi_display_enabled": False,
    # v1.0.20: basis of the PRIMARY rendered roi/ann/gain figures + KPI.
    # v1.0.23: "plan" (TP1 execution plan, NEW DEFAULT per operator) |
    # "engine" (Fix #3 honesty mode) | "valuation" (v1.0.19 rendering).
    "primary_roi_basis": "plan",
    # v1.0.6 data-trust gate (env-tunable; see policy block)
    "trust_gate_enabled": True,
    "max_data_age_hours": 168.0,
    "min_trust_fields": 2,
    # v1.0.7 investability gate (env-tunable; see policy block)
    # v1.0.8: DEFAULT OFF (opt-in) — the selector backfills Tier-2 rows, so
    # default-ON would diverge the opportunity surface from the Top_10 page.
    "investability_gate_enabled": False,
    # v1.10.2 [G-1]: BLOCKED-identity hard gate (env TFB_OPP_BLOCKED_IDENTITY_GATE).
    "blocked_identity_gate_enabled": False,
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
    # v1.0.16: issuer-level cross-listing dedup. DEFAULT False = OFF
    # (byte-identical). When True, a later listing of an already-funded issuer
    # is deferred instead of taking a second Top_10 slot.
    "issuer_dedup_enabled": False,
    # v1.0.10: DEFAULT 0 = unlimited (byte-identical). When >0, the
    # candidates_rows audit grid that GAS writes back to the sheet is capped
    # to this many rows so a full-universe scan stays inside the GAS/Sheets
    # write limit. The cap is applied AFTER `selected`, `near_miss`, `alerts`
    # and the `scanned` KPI are computed from the FULL pool, and it ALWAYS
    # retains every selected, INVEST-qualified and near-miss row plus the next
    # highest-score rows up to the cap — only the low-score DO_NOT_INVEST /
    # WATCH tail is dropped from the WRITTEN audit. SELECTED / ALL-QUALIFIED /
    # NEAR-MISS are never truncated. 0 => byte-identical v1.0.9 (full audit).
    "audit_rows_max": 0,
}

_CRITERIA_FLOAT_KEYS = (
    "required_roi_pct", "required_ann_roi_pct", "min_reliability", "min_dq",
    "min_rr", "max_weight_pct", "stop_floor_pct", "stop_vol_mult",
    "stop_max_pct", "pf_max_sector_pct", "min_engine_roi_pct",
    "max_valuation_roi_pct", "max_data_age_hours", "min_ticket_sar",
)
_CRITERIA_INT_KEYS = (
    "max_selected", "period_months", "max_per_sector", "max_per_market",
    "lot_size", "near_miss_n", "review_days", "max_candidates",
    "min_trust_fields", "audit_rows_max",
)
_CRITERIA_BOOL_KEYS = (
    "sell_class_gate_enabled",   # v1.7.0

    "allow_conflict", "allow_negative_news", "allow_negative_sector",
    "include_portfolio_holdings", "forecast_gate_enabled",
    "valuation_sanity_gate_enabled", "engine_roi_display_enabled",
    "trust_gate_enabled", "investability_gate_enabled",
    "blocked_identity_gate_enabled",
    "unfunded_watch_enabled", "rank_by_engine_roi_enabled",
    "issuer_dedup_enabled",
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
    "Forecast",
    # v1.10.0: appended immediately after "Forecast" in evaluate_gates —
    # registered here at its TRUE position (the v1.0.7 GATE_ORDER lesson).
    "Forecast Provenance",
    # v1.12.0: BOTH lines below are registration fixes at true append
    # positions — "Reliability Cluster" was appended in evaluate_gates by
    # v1.10.3 but never registered here (self-caught omission, header WHY);
    # "Forecast Cap Band" appends immediately after it.
    "Reliability Cluster",
    "Forecast Cap Band",
    "Reliability", "Data Quality", "Data Trust",
    # v1.9.0: the v1.0.7 lesson COMPLETED — these four have appended between
    # Data Trust and Investability since v1.0.6/v1.5.0/v1.8.0 but were never
    # added here, so first_failed_gate sorted them to 99 and could
    # mis-attribute the near-miss surface. True append positions:
    "Quote Freshness", "Shariah (KSA)", "Eligibility (KSA)",
    "Eligibility (Venue)",   # v1.19.2 (appends after Eligibility (KSA))
    "Shariah (Model)",       # v1.9.0 B-6 (appends after Eligibility (Venue))
    "Activity Screen",
    "Investability",
    # v1.10.2 [G-1]: appended immediately after Investability in evaluate_gates.
    "Blocked Identity",
    # v1.7.0: appended at its true position (the v1.0.7 GATE_ORDER lesson —
    # a gate missing from this tuple sorts to 99 and can mis-attribute
    # first_failed_gate on the near-miss surface).
    "Sell-Class",
    "Risk Level", "Risk/Reward", "Conflict", "News", "Sector Trend", "Portfolio",
)

# v1.7.0: explicit sell-tier tokens (normalized). Everything else — including
# blanks, HOLD, and every buy tier — passes the Sell-Class gate.
_SELL_CLASS_TOKENS = frozenset((
    "sell", "strongsell", "reduce", "avoid", "underperform", "underweight",
))

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
    # v1.10.1 (2026-08-08): 16 join in LOCKSTEP with 15_Lists_Config.gs
    # v1.3.2 (the sheet reaches 55 seeded currencies; BHD/OMR above were the
    # only two of the audit's 18-gap already covered here). The sheet's
    # POSTed fx_rates stays primary; this map is the safety net so a row in
    # any of these currencies can never fail the FX MAJOR gate on missing
    # FX alone when the request map is thin. 2026-08 approximations,
    # SAR pegged 3.75/USD.
    "BDT": 0.0307, "BRL": 0.68, "CNH": 0.525, "COP": 0.00091,
    "CZK": 0.163, "HUF": 0.0106, "JOD": 5.29, "KES": 0.029,
    "LKR": 0.0125, "MAD": 0.38, "NGN": 0.0024, "PEN": 1.00,
    "PHP": 0.065, "PKR": 0.0133, "RON": 0.82, "VND": 0.000148,
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


_DEFAULT_SYNTHETIC_SOURCES = ("phase_ii_synthetic",)


def _env_forecast_provenance_gate():
    """v1.10.0 [FORECAST-PROVENANCE GATE] kill-switch — DEFAULT OFF.
    TFB_T10_EXCLUDE_DEFAULT_CONF=1 arms the gate; unset/0 restores the
    v1.9.2 gate list, verdicts and tickets byte-for-byte (S-1 window law
    — see the header WHY block)."""
    return str(_env_str("TFB_T10_EXCLUDE_DEFAULT_CONF", "0")).strip().lower() \
        in ("1", "true", "yes", "on")


def _env_synthetic_source_tokens():
    """v1.10.0: the forecast_source tokens treated as SYNTHESIZED, as a
    normalized frozenset. Operator-tunable via TFB_T10_SYNTHETIC_SOURCES
    (csv; ';' accepted as ','), so a new engine fallback token can be
    covered without a code change. Empty/blank env => the default set."""
    raw = str(_env_str("TFB_T10_SYNTHETIC_SOURCES", "") or "").replace(";", ",")
    toks = [_norm_token(t) for t in raw.split(",") if str(t).strip()]
    toks = [t for t in toks if t]
    if not toks:
        toks = [_norm_token(t) for t in _DEFAULT_SYNTHETIC_SOURCES]
    return frozenset(toks)


def _forecast_provenance_assessment(cand):
    """v1.10.0: (ok, current_text) for the Forecast Provenance gate.
    ok=False only when the candidate's forecast_source normalizes to a
    token in the synthesized set. BLANK / UNKNOWN / provider-backed all
    PASS — fail-open by design (header WHY): a missing provenance column
    must never empty the board."""
    raw = _to_text((cand or {}).get("forecast_source")) or ""
    norm = _norm_token(raw)
    if not norm:
        return True, "Unknown"
    return (norm not in _env_synthetic_source_tokens()), raw


_DEFAULT_REL_CLUSTER_VALUES = (70.4, 71.5, 75.4, 76.5)


def _env_rel_cluster_gate():
    """v1.10.3 [B4b RELIABILITY-CLUSTER GATE] kill-switch — DEFAULT OFF.
    TFB_T10_EXCLUDE_REL_CLUSTER=1 arms the gate; unset/0 restores the
    v1.10.2 gate list, verdicts and tickets byte-for-byte (S-1 window law
    — see the header WHY block)."""
    return str(_env_str("TFB_T10_EXCLUDE_REL_CLUSTER", "0")).strip().lower() \
        in ("1", "true", "yes", "on")


def _env_rel_cluster_values():
    """v1.10.3: the reliability values treated as the default-confidence
    cluster, as a frozenset of one-decimal strings. Operator-tunable via
    TFB_T10_REL_CLUSTER_VALUES (csv; ';' accepted as ','), so a newly
    identified cluster value can be covered without a code change.
    Empty / blank / fully-unparseable env => the default set."""
    raw = str(_env_str("TFB_T10_REL_CLUSTER_VALUES", "") or "").replace(";", ",")
    vals = []
    for t in raw.split(","):
        t = str(t).strip()
        if not t:
            continue
        try:
            vals.append("%.1f" % float(t))
        except (TypeError, ValueError):
            continue
    if not vals:
        vals = ["%.1f" % v for v in _DEFAULT_REL_CLUSTER_VALUES]
    return frozenset(vals)


def _rel_cluster_values_text():
    """v1.10.3: stable display text for the armed cluster set (sorted,
    one-decimal), used in the gate's required-text."""
    return "{" + ", ".join(sorted(_env_rel_cluster_values())) + "}"


def _rel_cluster_assessment(cand):
    """v1.10.3: (ok, current_text) for the Reliability Cluster gate.
    ok=False only when the candidate's reliability, rounded to one
    decimal, equals a member of the cluster set. None / unparseable
    reliability PASSES this gate — fail-open by design (header WHY): the
    tiered Reliability gate below owns the missing-value verdict; this
    gate exists solely to catch the default-confidence fingerprint."""
    rel = (cand or {}).get("reliability")
    try:
        rel_f = float(rel)
    except (TypeError, ValueError):
        return True, "Unknown"
    cur = "%.1f" % rel_f
    return (cur not in _env_rel_cluster_values()), cur


_DEFAULT_VENUE_LOTS = {"T": 100, "SI": 100, "KL": 100}


def _env_venue_lots():
    """v1.11.0 [F-1]: {SUFFIX: board_lot} from TFB_T10_VENUE_LOTS.
    unset/blank => {} (feature OFF => v1.10.3 byte-for-byte).
    "default" => _DEFAULT_VENUE_LOTS (uniform-lot venues only).
    Else csv "T:100,TW:1000" (';' accepted as ','); bad entries skipped."""
    raw = str(_env_str("TFB_T10_VENUE_LOTS", "") or "").strip()
    if not raw:
        return {}
    if raw.lower() == "default":
        return dict(_DEFAULT_VENUE_LOTS)
    out = {}
    for tok in raw.replace(";", ",").split(","):
        tok = tok.strip()
        if not tok or ":" not in tok:
            continue
        suf, _, val = tok.partition(":")
        suf = suf.strip().upper().lstrip(".")
        try:
            lot = int(float(val.strip()))
        except (TypeError, ValueError):
            continue
        if suf and lot > 1:
            out[suf] = lot
    if not out:  # v1.16.0: non-empty value parsed to nothing (e.g. "defult")
        try:
            _LOG.warning("[VENUE-LOTS] CONFIG INVALID: %r yields no entries - "
                         "feature stays OFF; set TFB_T10_VENUE_LOTS=default "
                         "or a csv like T:100,KL:100", raw)
        except Exception:
            pass
    return out


def _venue_lot_for_symbol(cand_or_symbol):
    """v1.11.0 [F-1]: the venue board lot for a symbol's suffix, or 0 when
    the feature is off / the suffix is unmapped / the symbol has no suffix.
    Never raises."""
    try:
        sym = cand_or_symbol
        if isinstance(cand_or_symbol, dict):
            sym = cand_or_symbol.get("symbol")
        sym = str(sym or "")
        if "." not in sym:
            return 0
        suf = sym.rsplit(".", 1)[1].strip().upper()
        if not suf:
            return 0
        return int(_env_venue_lots().get(suf, 0))
    except Exception:
        return 0


_DEFAULT_CAP_BAND = (1.335, 1.365)


def _env_cap_band_gate():
    """v1.12.0 [B4c] kill-switch — DEFAULT OFF. Unset/0 keeps the gate
    list and every verdict byte-identical to v1.11.0."""
    return str(_env_str("TFB_T10_EXCLUDE_CAP_BAND", "0") or "0").strip().lower() \
        in ("1", "true", "yes", "on")


def _env_cap_band():
    """v1.12.0 [B4c]: (lo, hi) implied-ratio band from TFB_T10_CAP_BAND
    ("lo-hi", ':' or ',' accepted). Junk/inverted/out-of-domain values fall
    back to _DEFAULT_CAP_BAND. Never raises."""
    raw = str(_env_str("TFB_T10_CAP_BAND", "") or "").strip()
    if not raw:
        return _DEFAULT_CAP_BAND
    try:
        norm = raw.replace(":", "-").replace(",", "-")
        parts = [p for p in (x.strip() for x in norm.split("-")) if p]
        lo, hi = float(parts[0]), float(parts[1])
        if 1.0 < lo < hi:
            return (lo, hi)
    except Exception:
        pass
    return _DEFAULT_CAP_BAND


def _env_cap_band_raw_exempt():
    """v1.17.0: value-level provenance exemption for the cap-band gate.
    Default ON; TFB_T10_CAP_BAND_RAW_EXEMPT=0/false/off/no restores the
    v1.16.0 assessment byte-identically."""
    return str(_env_str("TFB_T10_CAP_BAND_RAW_EXEMPT", "1") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _cap_band_assessment(cand):
    """v1.12.0 [B4c]: (ok, current_text) for the Forecast Cap Band gate.
    ok=True when the implied 12M ratio sits OUTSIDE the manufactured band;
    blank/unparseable forecasts PASS (the Forecast gate owns those).
    v1.17.0: a RAW analyst target/price ratio strictly ABOVE the band proves
    the in-band engine ratio is a soft-capped genuine target, not a mint."""
    try:
        pct = _engine_roi_to_pct(cand.get("engine_roi_12m_pct"))
    except Exception:
        pct = None
    if pct is None:
        return True, "\u2014"
    ratio = round(1.0 + (pct / 100.0), 4)
    lo, hi = _env_cap_band()
    if not (lo <= ratio <= hi):
        return True, ("\u00d7%.4f" % ratio)
    if _env_cap_band_raw_exempt():
        try:
            tp = _to_float(cand.get("target_price"))
            px = _to_float(cand.get("price"))
            if tp is not None and px is not None and tp > 0 and px > 0:
                raw_ratio = round(tp / px, 4)
                if raw_ratio > hi + 0.01:
                    return True, ("\u00d7%.4f (raw \u00d7%.4f)" % (ratio, raw_ratio))
        except Exception:
            pass
    return False, ("\u00d7%.4f" % ratio)


def _env_forecast_gate():
    """v1.0.3: engine-forecast safety gate toggle. Default ON; set
    TFB_OPP_FORECAST_GATE=0 to restore byte-identical v1.0.2 behavior."""
    return str(_env_str("TFB_OPP_FORECAST_GATE", "1")).strip().lower() not in (
        "0", "false", "no", "off")


def _env_sell_class_gate():
    """v1.7.0 [SELL-CLASS GATE] kill-switch — DEFAULT ON. Set
    TFB_OPP_SELL_CLASS_GATE=0 to restore the v1.6.0 gate list and verdicts
    byte-for-byte."""
    return str(_env_str("TFB_OPP_SELL_CLASS_GATE", "1")).strip().lower() \
        not in ("0", "false", "off", "no")


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


def _ref_conservative_enabled():
    """v1.3.0 (D-12): master switch for the conservative ticket reference —
    ref = min(valuation ref, engine 12M forecast price). Default OFF ->
    normalize_candidate byte-identical to v1.2.0."""
    return str(_env_str("TFB_OPP_REF_CONSERVATIVE", "0")).strip().lower() in (
        "1", "true", "yes", "on")


def _tp1_plan_roi(cand):
    """v1.14.0 [ROI-TRUTH-1]: the plan-TP1 ROI for ANY candidate dict —
    (tp1 - price) / price * 100 — or None when the ladder/price is absent.
    Pure and total: bad inputs return None, never raise. This is the ONE
    definition of \"plan TP1 ROI\" for audit alignment; the ticket path keeps
    its own _plan_roi (identical formula at the selected seam)."""
    try:
        tp1 = cand.get("tp1")
        price = cand.get("price")
        tp1 = float(tp1) if tp1 is not None else None
        price = float(price) if price is not None else None
        if tp1 is None or price is None or price <= 0:
            return None
        return round((tp1 - price) / price * 100.0, 1)
    except Exception:
        return None


def _audit_align_plan_roi(rec, crit):
    """v1.14.0 [ROI-TRUTH-2 / D-25]: under the LIVE default basis (\"plan\")
    the audit record's PRIMARY roi/ann now speak the same language as the
    rendered board: plan-TP1. Before this fix the audit carried VALUATION
    upside in the same key the board renders as plan-TP1 — one key, three
    meanings ("three ROI stories", Morning Review 2026-08-24 §4). Gates,
    verdict, score and selection were computed BEFORE this call and are
    byte-untouched; valuation stays in valuation_roi_pct / _ann. When no
    TP1 ladder exists the primary field becomes None (renders blank) and
    roi_basis_note = TP1_UNAVAILABLE(DATA_GAP) — an evidence gap stated
    honestly, never a silently borrowed number (D-25 default: DATA_GAP).
    KILL SWITCH: TFB_OPP_AUDIT_ROI_LEGACY=1 restores v1.13.0 emission
    byte-identically. Returns rec (mutated in place) for harness use."""
    try:
        if str(_env_str("TFB_OPP_AUDIT_ROI_LEGACY", "0")).strip().lower() \
                in ("1", "true", "yes", "on"):
            return rec
        if str((crit or {}).get("primary_roi_basis") or "").strip().lower() \
                != "plan":
            return rec
        _p = _tp1_plan_roi(rec.get("_cand") or {})
        rec["roi_pct"] = _p
        rec["ann_roi_pct"] = _p
        rec["primary_roi_basis"] = "plan"
        if _p is None:
            rec["roi_basis_note"] = "TP1_UNAVAILABLE(DATA_GAP)"
    except Exception:
        pass
    return rec


def _env_primary_roi_basis():
    """v1.0.20: basis of the primary rendered ROI/gain figures. v1.0.23
    adds "plan" (the TP1 execution plan: (TP1-entry)/entry) and makes it
    the DEFAULT per the 2026-07-08 operator decision — the visible triple
    ROI/Engine ROI/Ann ROI had collapsed into one number (the 35-cap wall
    pinned both the capped valuation AND the capped engine display, and a
    12-month period makes annualization the identity). "plan" gives the
    three columns three honest meanings: my plan to first target / the
    engine's forecast / the plan annualized. Set
    TFB_OPP_PRIMARY_ROI_BASIS=valuation for the byte-identical v1.0.22
    rendering, or =engine for the v1.0.20 engine basis."""
    v = str(_env_str("TFB_OPP_PRIMARY_ROI_BASIS", "plan")).strip().lower()
    if v in ("engine", "valuation", "plan"):
        return v
    return "plan"


def _env_held_variant_match():
    """v1.0.21 (Fix #4): held-symbol variant matching for the Portfolio gate.
    DEFAULT ON — enforces the documented meaning of "Include Portfolio
    Holdings = No" across bare/.US symbol forms (the live BBD vs BBD.US
    duplicate-ticket case). Set TFB_OPP_HELD_MATCH_VARIANTS=0 to restore
    v1.0.20 exact-match behavior byte-identically."""
    return str(_env_str("TFB_OPP_HELD_MATCH_VARIANTS", "1")).strip().lower() \
        not in ("0", "false", "no", "off")


def _symbol_variants(symbol):
    """v1.0.21 (Fix #4): normalized membership variants for ONE symbol.
    Uppercased/stripped; a bare US-style symbol also matches its .US form
    and vice versa. Non-US suffixes (.SR/.T/.HK/...), indices (^), FX/
    futures (=) and crypto (-USD) get NO cross-form variant — 1211.SR can
    only ever match 1211.SR. Never raises; blank -> empty set."""
    s = str(symbol or "").strip().upper()
    if not s:
        return set()
    out = {s}
    if s.startswith("^") or "=" in s or s.endswith("-USD"):
        return out
    if s.endswith(".US"):
        out.add(s[:-3])
    elif "." not in s:
        out.add(s + ".US")
    return out


def _env_trust_gate():
    """v1.0.6: data-trust gate toggle. Default ON; set TFB_OPP_TRUST_GATE=0 to
    restore byte-identical v1.0.5 behavior (the Data Trust gate is not
    appended)."""
    return str(_env_str("TFB_OPP_TRUST_GATE", "1")).strip().lower() not in (
        "0", "false", "no", "off")


def _env_blocked_identity_gate():
    """v1.10.2 [G-1]: TFB_OPP_BLOCKED_IDENTITY_GATE — default OFF (house
    law: ship byte-identical, arm deliberately). When on, an engine
    investability of exactly BLOCKED MAJOR-fails regardless of the retired
    Require-Investable setting. Reader mirrors _env_investability_gate."""
    raw = (os.getenv("TFB_OPP_BLOCKED_IDENTITY_GATE") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


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


# v1.0.13: Yahoo-provider sector vocabulary -> GICS vocabulary. The
# _KSA_SYMBOL_SECTOR map and the diversifier bucket on GICS strings; an unmapped
# name whose sector fell through to the provider's Yahoo string (e.g.
# "Basic Materials") otherwise fragments into its OWN cap bucket instead of
# joining its GICS peers ("Materials"). Only these six spellings differ; every
# other Yahoo sector string already equals its GICS counterpart.
#
# v1.0.18 (A2): this map now lives in core.sectors as the SINGLE source of truth,
# imported here and by top10_selector so the two decision tabs' sector
# canonicalization can never drift. Behaviour is identical -- the same six
# Yahoo->GICS pairs, consumed by the same _normalize_sector() under the same
# TFB_OPP_SECTOR_NORMALIZE gate. The inline literal is retained ONLY as a
# fallback so this module stays importable if core.sectors is ever unavailable;
# in normal operation the imported dict is used and the literal is dead.
try:
    from core.sectors import YAHOO_TO_GICS_SECTOR as _YAHOO_TO_GICS_SECTOR  # type: ignore
except Exception:  # pragma: no cover
    _YAHOO_TO_GICS_SECTOR = {
        "Basic Materials": "Materials",
        "Healthcare": "Health Care",
        "Consumer Cyclical": "Consumer Discretionary",
        "Consumer Defensive": "Consumer Staples",
        "Technology": "Information Technology",
        "Financial Services": "Financials",
    }


def _alias_key_used(view, field):
    """v1.13.0 [TRUST-001]: which alias KEY supplied the value _field()
    returned — per-field provenance for the lineage audit (which spelling
    of DQ/reliability actually won on this row)."""
    for alias in _FIELD_ALIASES.get(field, ()):
        if alias in view and view[alias] not in (None, ""):
            return alias
    return None


def _env_trust_lineage_mode():
    """v1.13.0 [TRUST-001 / IR-093]: consumer-side defense for the DQ
    rewrite class (2026-08-19 audits, Claude-adjudicated: 20 rows whose
    SOURCE page carried low_data_trust @ DQ 73.7 arrived at the Top-10
    decision layer as DQ 100.0 — crossing the Min DQ 80 gate). At HEAD the
    backend is a proven pass-through (route: no live refresh; builder:
    alias passthrough; trend enrichment: dq-free), so the inflation enters
    upstream of the POST — this gate defends the LAST line regardless of
    where, and its meta counters localize the origin from one live run.
    Returns "" (off, DEFAULT — gate list, verdicts, candidate keys and
    every row byte-identical to v1.12.0) | "tag" (candidates carry the
    lineage fields + run counters count; NO gate, NO verdict change) |
    "gate" (the contradiction — low_data_trust source flag AND DQ passing
    min_dq — fails MAJOR => DO_NOT_INVEST, visible in audit/near-miss).
    TFB_OPP_TRUST_LINEAGE = unset/0 | tag/observe/1 | gate/enforce/2."""
    v = (os.getenv("TFB_OPP_TRUST_LINEAGE") or "").strip().lower()
    if v in ("gate", "enforce", "2"):
        return "gate"
    if v in ("tag", "observe", "1"):
        return "tag"
    return ""


def _env_sector_normalize():
    """v1.0.13: diversifier sector-quality toggle. Default OFF; set
    v1.0.22: DEFAULT ON (dormant fix + live 5023.SR damage — see WHY).
    Set TFB_OPP_SECTOR_NORMALIZE=0 to restore raw buckets; =1 (or unset) to (a) translate the six differing Yahoo-provider
    sector spellings to the GICS vocabulary the map + diversifier bucket on, and
    (b) exempt the "Unknown"/"" data-gap bucket from the per-sector COUNT cap (an
    unknown sector is a data gap, not a real concentration bucket, so two unmapped
    names are not capped as if they were one sector). The post-action PORTFOLIO
    weight cap still applies to "Unknown". OFF => byte-identical v1.0.12."""
    return str(_env_str("TFB_OPP_SECTOR_NORMALIZE", "1")).strip().lower() in (
        "1", "true", "yes", "on", "enabled", "enable")


def _normalize_sector(s):
    """Yahoo->GICS sector translation (v1.0.13). Already-GICS strings and
    "Unknown"/"" pass through unchanged. Gated by the CALLER via
    _env_sector_normalize()."""
    t = (s or "").strip()
    return _YAHOO_TO_GICS_SECTOR.get(t, t)


def _env_audit_order():
    """v1.19.1 [AUDIT-DEPTH-ORDER]: TFB_OPP_AUDIT_ORDER = depth (default) |
    score (kill-switch, v1.19.0 ordering byte-for-byte). Display-only."""
    raw = str(_env_str("TFB_OPP_AUDIT_ORDER", "depth") or "depth").strip().lower()
    return "score" if raw == "score" else "depth"


def _env_rank_by_engine_roi():
    """v1.0.9: selection-ordering toggle. Default OFF; set
    TFB_OPP_RANK_BY_ENGINE_ROI=1 to order the INVEST pool by the engine's
    normalized 12M forecast (desc) as the primary key (opportunity_score /
    ann_roi / symbol remain tie-breakers). OFF => byte-identical v1.0.8
    ordering (opportunity_score primary)."""
    return str(_env_str(
        "TFB_OPP_RANK_BY_ENGINE_ROI", "0")).strip().lower() in (
        "1", "true", "yes", "on", "enabled", "enable")


def _env_issuer_dedup():
    """v1.0.16: issuer-level cross-listing dedup toggle. Default OFF; set
    TFB_OPP_ISSUER_DEDUP=1 to collapse multiple symbols of one issuer (true
    cross-listings or symbol-spelling twins) so a single issuer cannot occupy
    more than one funded Top_10 slot. OFF => byte-identical v1.0.15."""
    return str(_env_str(
        "TFB_OPP_ISSUER_DEDUP", "0")).strip().lower() in (
        "1", "true", "yes", "on", "enabled", "enable")


def _env_pregate_order():
    """v1.6.0 [PREGATE-ORDER] kill-switch — DEFAULT ON. Set
    TFB_OPP_PREGATE_ORDER=0 to restore the v1.5.0 arrival-order clamp
    byte-for-byte."""
    return str(_env_str("TFB_OPP_PREGATE_ORDER", "1")).strip().lower() \
        not in ("0", "false", "off", "no")


def _env_scan_uncapped():
    """v1.9.2 [SC-1] guarantee switch — DEFAULT OFF (byte-identical v1.9.1).
    TFB_OPP_SCAN_UNCAPPED=1 forces max_candidates to 0 (unlimited) even when
    the REQUEST criteria carry a positive cap. Needed because make_criteria
    precedence is DEFAULTS < env < request, so a GAS-seeded cap (live run
    2026-08-06: 2,000 of a 9,824-row pool) wins over any Render env value."""
    return str(_env_str("TFB_OPP_SCAN_UNCAPPED", "0")).strip().lower() in (
        "1", "true", "yes", "on")


def _env_canon_market():
    """v1.0.11: per-market cap canonicalization toggle. KILL-SWITCH, default ON;
    set TFB_OPP_CANON_MARKET=0 to restore byte-identical v1.0.10 behavior (the
    per-market diversification cap keys on the raw market string, so a venue
    written two ways counts as two separate markets)."""
    return str(_env_str("TFB_OPP_CANON_MARKET", "1")).strip().lower() not in (
        "0", "false", "no", "off")


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
        "sell_class_gate_enabled": _env_sell_class_gate(),
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
        "primary_roi_basis": _env_primary_roi_basis(),
        "trust_gate_enabled": _env_trust_gate(),
        "max_data_age_hours": _env_float(
            "TFB_OPP_MAX_DATA_AGE_HOURS",
            DEFAULT_CRITERIA["max_data_age_hours"]),
        "min_trust_fields": _env_int(
            "TFB_OPP_MIN_TRUST_FIELDS",
            DEFAULT_CRITERIA["min_trust_fields"]),
        "investability_gate_enabled": _env_investability_gate(),
        "blocked_identity_gate_enabled": _env_blocked_identity_gate(),
        "unfunded_watch_enabled": _env_unfunded_watch(),
        "rank_by_engine_roi_enabled": _env_rank_by_engine_roi(),
        "issuer_dedup_enabled": _env_issuer_dedup(),
        "min_ticket_sar": _env_float(
            "TFB_OPP_MIN_TICKET_SAR",
            DEFAULT_CRITERIA["min_ticket_sar"]),
        "audit_rows_max": _env_int("TFB_OPP_AUDIT_ROWS_MAX",
                                   DEFAULT_CRITERIA["audit_rows_max"]),
    }



# ---- v1.1.0 Gen-2 venue cost / floor / net-edge model (§18.2, §18.5) ------- #
_VENUE_COSTS = {
    # suffix: (comm_pct_per_side, min_fee_sar_per_side, spread_rt_pct, floor_sar)
    "":    (0.0,   0.0,  0.10,  5000),   # US via Derayah Global Lite (zero comm)
    ".US": (0.0,   0.0,  0.10,  5000),
    ".SR": (0.155, 0.0,  0.05,  4000),   # conservative ceiling until zero-tier confirmed
    ".T":  (0.199, 46.0, 0.10, 13200),
    ".KL": (0.199, 51.0, 0.15, 45000),   # v1.16.0: Bursa - measured non-USD floor
    ".HK": (0.199, 95.0, 0.15, 27200),
    ".L":  (0.199, 97.2, 0.15, 27700),
    ".PA": (0.199, 85.5, 0.12, 24500), ".AS": (0.199, 85.5, 0.12, 24500),
    ".BR": (0.199, 85.5, 0.12, 24500), ".DE": (0.199, 85.5, 0.12, 24500),
    ".MI": (0.199, 85.5, 0.12, 24500), ".MC": (0.199, 85.5, 0.12, 24500),
    ".LS": (0.199, 85.5, 0.12, 24500), ".VI": (0.199, 85.5, 0.12, 24500),
    ".SW": (0.199, 84.0, 0.12, 24200),
    ".TO": (0.199, 54.0, 0.12, 15500),
    ".AX": (0.199, 49.0, 0.12, 14100),
    ".OL": (0.199, 70.0, 0.15, 20100),
    ".SI": (0.199, 58.0, 0.12, 16700),
    ".MX": (0.199, 38.0, 0.20, 11000),
}
_P_HIT_PROXY = {"High": 0.65, "Medium": 0.55, "Low": 0.45}


def _env_flag01(name, default="0"):
    v = os.environ.get(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _env_netedge_annotate():
    return _env_flag01("TFB_OPP_NETEDGE_ANNOTATE", "0")


def _env_venue_floors():
    return _env_flag01("TFB_OPP_VENUE_FLOORS", "0")


def _venue_cost_row(symbol):
    s = _to_text(symbol).strip().upper()
    suf = ""
    if "." in s:
        suf = "." + s.rsplit(".", 1)[1]
    if suf == ".US":
        suf = ".US"
    return _VENUE_COSTS.get(suf if suf else "")


def _venue_floor(symbol):
    row = _venue_cost_row(symbol)
    return row[3] if row else None


def rt_cost_pct(symbol, ticket_sar):
    """Round-trip cost %% of ticket (2 sides commission-with-minimum + spread).
    FX omitted per the tranche-batching policy (§18.7). None = no model."""
    row = _venue_cost_row(symbol)
    t = _to_float(ticket_sar)
    if not row or not t or t <= 0:
        return None
    comm, min_fee, spread, _floor = row
    per_side = max(comm / 100.0 * t, min_fee)
    return (2.0 * per_side / t) * 100.0 + spread


def _annotate_cost_edge(ticket, suggested):
    """§18.2 net-edge stamp on one output ticket. Annotation only; never raises."""
    if not _env_netedge_annotate():
        return
    try:
        sym = ticket.get("symbol")
        floor = _venue_floor(sym)
        t = _to_float(suggested)
        t_eff = t if (t and t > 0) else float(floor or 10000.0)
        rt = rt_cost_pct(sym, t_eff)
        ticket["venue_floor_sar"] = floor
        if rt is None:
            ticket["edge_verdict"] = "NO_COST_MODEL"
            return
        ticket["rt_cost_pct"] = round(rt, 2)
        roi = _to_float(ticket.get("roi_pct"))
        p = _P_HIT_PROXY.get(_to_text(ticket.get("confidence_band")).strip(), 0.50)
        ticket["p_hit_proxy"] = p
        if roi is None:
            ticket["edge_verdict"] = "NO_ROI"
            return
        hurdle = max(3.0 * rt, 1.5)
        ne = p * roi - rt
        ticket["net_edge_pct"] = round(ne, 2)
        ticket["edge_hurdle_pct"] = round(hurdle, 2)
        ticket["edge_verdict"] = "TRADE" if ne >= hurdle else "EDGE_BELOW_COST"
    except Exception as exc:  # annotation must never break tickets
        ticket["edge_verdict"] = "EDGE_ERR:" + type(exc).__name__


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
    # v1.0.20: fold any unknown basis literal to the safe default.
    _prb = str(crit.get("primary_roi_basis") or "").strip().lower()
    # v1.0.23: tri-value basis. Unknown/blank -> "plan" (the new default);
    # "valuation" and "engine" restore the prior modes byte-identically.
    crit["primary_roi_basis"] = (_prb if _prb in ("plan", "engine",
                                                  "valuation") else "plan")
    if crit.get("min_ticket_sar", 0.0) < 0:
        crit["min_ticket_sar"] = 0.0
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
    # v1.17.0: the sheet renders ROI/change cells with direction glyphs
    # (U+25B2 / U+25BC) in front of the number; strip them before the numeric
    # test. Nothing that parsed before parses differently.
    s = s.replace("\u25b2", "").replace("\u25bc", "").strip()
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
    # v1.9.1: sheet header "Forecast Source" -> compact "forecastsource".
    "forecast_source": ("forecastsource", "forecastbasis"),
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
    # v1.13.0 [TRUST-001]: the sheet row's Warnings column travels in
    # body_rows; the low_data_trust flag it carries is the SOURCE engine's
    # own trust verdict and is the lineage witness this build consumes.
    "warnings": ("warnings", "warning", "rowwarnings"),
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

    # v1.3.0 [REF-CONSERVATIVE] (D-12): the valuation ref (target_price /
    # intrinsic_value) arrives display-capped upstream (engine Fix AG pins
    # intrinsic_value at exactly price*1.35 on every overshooting row), which
    # pinned roi_pct/TP2 at 35.0 and the TP1 plan basis at 17.5 across the
    # board — the armed TFB_SCORE_ROI_SOFTCAP differentiates expected_roi_*
    # in core/scoring.py but is structurally unreachable from this column.
    # When armed, bound the ladder reference by the engine's own softcap-
    # differentiated 12M forecast — min(), so an inflated claim only ever
    # SHRINKS and an honest ref already below the forecast is untouched.
    # Positive forecasts only; missing/<=0 keeps the v1.2.0 ref exactly (the
    # v1.0.3 FORECAST-GATE already governs forecast losers).
    if _ref_conservative_enabled() and price and ref:
        _eng_pct_ref = _engine_roi_to_pct(
            _to_float(_field(view, "engine_roi_12m_pct")))
        if _eng_pct_ref is not None and _eng_pct_ref > 0.0:
            _ref_eng = price * (1.0 + _eng_pct_ref / 100.0)
            if _ref_eng < ref:
                ref = _ref_eng
                valuation_basis = "engine_forecast_min"

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
    vol_for_stop = _stop_vol_input(vol30)
    stop_pct = criteria["stop_floor_pct"]
    if vol_for_stop is not None and vol_for_stop > 0:
        stop_pct = max(stop_pct, criteria["stop_vol_mult"] * vol_for_stop)
    stop_pct = min(stop_pct, criteria["stop_max_pct"])

    stop = tp1 = tp2 = rr = None
    if price:
        stop = price * (1.0 - stop_pct / 100.0)
        # v1.0.22 (TP-COHERENCE): a reference target at/below price would
        # print an inverted BUY ladder (TP1/TP2 under entry). Build the
        # ladder only for a target strictly above price; otherwise the TP
        # fields stay blank. Stop and the signed roi/RR math are untouched.
        if ref and ref > price:
            tp1 = price + 0.5 * (ref - price)
            tp2 = ref
        if roi_pct is not None and stop_pct > 0:
            rr = roi_pct / stop_pct  # downside% == stop distance% by L6

    mos_pct = None
    if iv and price and iv > 0:
        mos_pct = max(0.0, (iv - price) / iv * 100.0)

    _tl_mode = _env_trust_lineage_mode()

    avg_vol = _to_float(_field(view, "avg_volume_30d"))
    liquidity_sar = None
    if avg_vol and price and fx:
        liquidity_sar = avg_vol * price * fx

    reliability = _to_float(_field(view, "reliability"))
    dq = _to_float(_field(view, "dq"))

    cand = {
        "symbol": symbol,
        "name": _to_text(_field(view, "name")) or symbol,
        "market": _to_text(_field(view, "market")) or "Unknown",
        "sector": (_normalize_sector(_to_text(_field(view, "sector")))
                   if _env_sector_normalize()
                   else _to_text(_field(view, "sector"))) or "Unknown",
        "asset_class": _to_text(_field(view, "asset_class")),
        # v1.8.0 [1.4]: additive — third input to the global activity blob.
        "industry": _to_text(_field(view, "industry")),
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
        # v1.9.1: basis passthrough for portfolio_actions' synthetic-basis
        # deferral (TFB_PF_REQUIRE_FRESH_BASIS). Always a string; "" absent.
        "forecast_source": _to_text(_field(view, "forecast_source")) or "",
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
    # v1.13.0 [TRUST-001]: lineage fields attach ONLY when the mode is
    # armed, so the OFF candidate dict (and therefore every downstream
    # row/payload outside meta) is byte-identical to v1.12.0.
    if _tl_mode:
        _w_txt = _to_text(_field(view, "warnings")) or ""
        cand["trust_low_source"] = ("low_data_trust" in _w_txt
                                    or "rank_skipped_low_trust" in _w_txt)
        cand["dq_alias_key"] = _alias_key_used(view, "dq")
        cand["rel_alias_key"] = _alias_key_used(view, "reliability")
    return cand


# ---------------------------------------------------------------------------
# §4.2 hard gates → verdict (truth table)
# ---------------------------------------------------------------------------

def _env_rel_floor_mode():
    """v1.19.0: 'gate' (default, v1.18.1) or 'display' (H-28: the floor never
    excludes). Anything else -> 'gate'."""
    v = str(_env_str("TFB_T10_REL_FLOOR_MODE", "gate") or "gate").strip().lower()
    return "display" if v == "display" else "gate"


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


# --------------------------------------------------------------------------- #
# v1.4.0 (W-2): venue-session-aware quote freshness                           #
# --------------------------------------------------------------------------- #
def _parse_ts_utc(last_updated_text):
    """The timestamp behind _parse_age_hours, as an aware UTC datetime (or
    None). Same parsing ladder, same naive⇒UTC assumption; extracted so the
    freshness gate can compare against a venue close, not just an age."""
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
    return dt


def _env_freshness_gate():
    """W-2 kill-switch — DEFAULT ON (protective guards ship armed)."""
    return (os.getenv("TFB_TICKET_FRESHNESS_GATE") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _env_quote_max_age_min():
    try:
        v = float(os.getenv("TFB_TICKET_MAX_QUOTE_AGE_MIN") or 15.0)
        return v if v > 0 else 15.0
    except (TypeError, ValueError):
        return 15.0


def _env_freshness_fallback_h():
    try:
        v = float(os.getenv("TFB_TICKET_FALLBACK_MAX_AGE_H") or 78.0)
        return v if v > 0 else 78.0
    except (TypeError, ValueError):
        return 78.0


# v1.5.0: official authority FAIL list — compiled default (Al-Rajhi Q1-2026,
# as_of 2026-03-31; quarterly refresh via env or the next authority upload).
_KSA_AUTHORITY_FAIL_DEFAULT = (
    "1010.SR", "1030.SR", "1050.SR", "1060.SR", "1080.SR", "1180.SR",
    "4011.SR", "4072.SR", "4280.SR", "8100.SR", "8310.SR", "9642.SR")


# =============================================================================
# v1.8.0 (2026-07-26) — PY-1 BLENDED R/R BASIS + GLOBAL ACTIVITY SCREEN (1.4)
# =============================================================================
# [PY-1] THE 1.25 LIE. Evidence (workbook audit 2026-07-24/25): the Top-10 KPI
#   strip reported Blended R/R 1.25 while the two rendered tickets showed 2.89
#   and 2.10 (mean 2.495). Root cause, proven arithmetically: under the
#   v1.0.23 "plan" primary basis (the live default) detail.rr is deliberately
#   the PLAN R/R — _plan_roi / stop_pct — and _plan_roi is BY CONSTRUCTION
#   half the valuation ROI, because TP1 = price + 0.5*(ref - price).
#   kpis.blended_rr blended THAT field, so the KPI printed ~half the R/R every
#   other surface printed. 2.495 / 2 = 1.2475 -> 1.25: the observed number.
#   The three other surfaces already agree with each other and with the
#   ladder: the Risk/Reward GATE tests cand["rr"], the advisor sentence prints
#   cand["rr"], and the audit grid emits cand["rr"] — all equal to
#   (tp2 - price) / (price - stop), the true reward-to-risk of the ticket's
#   own ladder. The KPI was the sole outlier.
#   FIX: detail.rr_tp2 is now emitted explicitly (always, every basis) and
#   kpis.blended_rr blends IT. detail.rr is BYTE-UNTOUCHED — the plan-basis
#   decision R/R keeps its meaning for every existing reader.
#   DEFAULT IS THE FIX, deliberately, against the usual backward-safe rule:
#   blended_rr has ZERO consumers in this repo outside the GAS KPI-CHECK that
#   is currently FIRING on the contradiction (repo-wide grep). It feeds no
#   gate, no ticket, no recommendation and no evidence row — it is a dashboard
#   number that is presently wrong. Shipping the fix defaulted OFF would
#   commit a correction that does nothing while the workbook keeps printing a
#   false KPI. Under any basis other than "plan", detail.rr already EQUALS
#   rr_tp2, so the new default is byte-identical there.
#   KILL-SWITCH: TFB_OPP_BLENDED_RR_BASIS=plan restores v1.7.0 exactly.
#
# [1.4] GLOBAL ACTIVITY SCREEN. Operator policy recorded 2026-07-25: haram
#   activities (tobacco, alcohol, gambling) are excluded from investment
#   consideration GLOBALLY, not only for Saudi listings — the decision that
#   killed the 2914.T (Japan Tobacco) fast-track candidate. Until now the
#   activity screen ran only inside core.compliance_gate.model_screen on the
#   KSA path, so a Tokyo- or US-listed tobacco name faced no activity test.
#   WIRING: the screen reuses core.compliance_gate's regexes as the single
#   source of truth (the compliance_rule_sets() precedent — one resolver, no
#   second copy of the rulebook), adding a MAJOR "Activity Screen" gate for
#   symbols of EVERY venue.
#   TWO SCOPES, because they are NOT the same policy question:
#     haram (recommended)  blocks casino/gambling/betting/lottery/alcohol/
#                          brewer/distiller/winer/tobacco/cigarette/adult/pork
#     full                 ALSO blocks conventional finance (bank/insurance/
#                          assurance/reinsurance) worldwide
#   The distinction is deliberate and load-bearing: compliance_gate's block
#   list is a SHARIAH screen, so it includes conventional banking. Applying it
#   globally at face value would silently exclude every non-Islamic bank and
#   insurer on every exchange — a sweeping change to the recommendation
#   universe that the recorded policy did not authorize. "haram" implements
#   the policy as stated; "full" is available if the operator decides the
#   broader screen is intended.
#   FAIL-OPEN ON MISSING DATA: activity_screen returns "activity_undisclosed"
#   for an empty name/sector/industry blob. That verdict PASSES this gate.
#   Blocking it would turn every provider gap into an exclusion — a data
#   outage would silently empty the board.
#   DEFAULT OFF (TFB_GLOBAL_ACTIVITY_SCREEN unset) = byte-identical v1.7.0.
#   This one keeps the backward-safe default because it DOES change the
#   recommendation universe, and that requires the operator's explicit arming.
#
# SAFE SCOPE: zero function removals; no change to scoring, ranking, sizing,
# funding, the ticket ladder, stop/TP math, or any existing gate. cand gains
# one additive key ("industry"); ticket detail gains one ("rr_tp2").
# =============================================================================


def _env_csv_set(name, default_csv=""):
    raw = (os.getenv(name) or default_csv).strip()
    return {p.strip().upper() for p in raw.split(",") if p.strip()}


def _env_compliance_gate():
    return (os.getenv("TFB_COMPLIANCE_SURFACE_GATE") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _env_venue_allowlist():
    """v1.19.2: frozenset of upper-cased venue tokens from
    TFB_T10_VENUE_ALLOWLIST (CSV). Empty -> the venue gate is not appended."""
    raw = (os.getenv("TFB_T10_VENUE_ALLOWLIST") or "").strip()
    if not raw:
        return frozenset()
    return frozenset(t.strip().upper().lstrip(".") for t in raw.split(",")
                     if t.strip())


def _venue_eligibility(cand, allow):
    """v1.19.2: (ok, current). Market name OR symbol suffix in the allow-list
    passes; a bare ticker counts as suffix US; unknown market with no
    suffix passes fail-open as "venue unknown"."""
    sym = str(cand.get("symbol") or "").strip().upper()
    mkt = str(cand.get("market") or "").strip().upper()
    suffix = sym.rsplit(".", 1)[1] if "." in sym else ("US" if sym else "")
    if not mkt or mkt == "UNKNOWN":
        mkt = ""
    if not mkt and not suffix:
        return True, "venue unknown"
    ok = (mkt in allow) or (suffix in allow)
    return ok, f"{mkt or '?'}/.{suffix or '?'}"


def _env_eligibility_gate():
    return (os.getenv("TFB_ELIGIBILITY_GATE") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _env_shariah_model_gate():
    """v1.9.0 B-6: DEFAULT OFF — arming alters candidate eligibility inside
    the S-1 evidence window, so the flip is the operator's explicit decision
    (declared version break). "1"/"true"/"on"/"yes" arms the gate."""
    return (os.getenv("TFB_OPP_SHARIAH_MODEL_GATE") or "").strip().lower() \
        in ("1", "true", "on", "yes")


def compliance_rule_sets():
    """(authority_fail_set, foreign_restricted_set) — the ONE resolver both
    decision surfaces share. Env layers: TFB_SHARIAH_FAIL_LIST replaces the
    compiled default; TFB_EXIT_BY_RULE_EXTRA adds (any venue — the
    operator's model-screen verdicts for globals live here until the Gen-2
    wiring); TFB_KSA_FOREIGN_RESTRICTED defaults to 4030.SR."""
    fail = _env_csv_set("TFB_SHARIAH_FAIL_LIST")
    if not fail:
        fail = set(_KSA_AUTHORITY_FAIL_DEFAULT)
    fail |= _env_csv_set("TFB_EXIT_BY_RULE_EXTRA")
    restricted = _env_csv_set("TFB_KSA_FOREIGN_RESTRICTED", "4030.SR")
    return fail, restricted


# --------------------------------------------------------------------------- #
# v1.8.0 [PY-1] blended R/R basis                                              #
# --------------------------------------------------------------------------- #
def _env_blended_rr_basis():
    """"tp2" (default, the fix) | "plan" (v1.7.0 behaviour)."""
    v = (os.getenv("TFB_OPP_BLENDED_RR_BASIS") or "tp2").strip().lower()
    return "plan" if v == "plan" else "tp2"


# --------------------------------------------------------------------------- #
# v1.8.0 [1.4] global activity screen                                          #
# --------------------------------------------------------------------------- #
# Conventional-finance terms live in compliance_gate's block list because that
# list is a SHARIAH screen. The global "haram" scope subtracts exactly these
# and nothing else, so a term added to compliance_gate later is inherited by
# BOTH scopes automatically — no second rulebook, no drift.
_ACTIVITY_FINANCE_TERMS = frozenset((
    "bank", "banks", "banking", "insurance", "assurance", "reinsurance"))

_ACTIVITY_SCREEN_FN = {"fn": None, "loaded": False}


def _env_global_activity_screen():
    """"" (OFF, default) | "haram" | "full"."""
    v = (os.getenv("TFB_GLOBAL_ACTIVITY_SCREEN") or "").strip().lower()
    if v in ("1", "true", "on", "yes", "haram"):
        return "haram"
    if v == "full":
        return "full"
    return ""


def _activity_screen_fn():
    """Lazy, cached, failure-tolerant import of the single source of truth.
    A missing/broken compliance_gate must never break the builder: the screen
    then resolves to unavailable and the gate is simply not appended."""
    if not _ACTIVITY_SCREEN_FN["loaded"]:
        _ACTIVITY_SCREEN_FN["loaded"] = True
        try:
            from core.compliance_gate import activity_screen as _fn
            _ACTIVITY_SCREEN_FN["fn"] = _fn
        except Exception:
            try:
                from compliance_gate import activity_screen as _fn
                _ACTIVITY_SCREEN_FN["fn"] = _fn
            except Exception:
                _LOG.warning(
                    "[v1.8.0 ACTIVITY-SCREEN] core.compliance_gate "
                    "unavailable — global activity gate not applied")
                _ACTIVITY_SCREEN_FN["fn"] = None
    return _ACTIVITY_SCREEN_FN["fn"]


def global_activity_verdict(name, sector, industry, scope):
    """-> (ok, reason) for the global screen, or (None, reason) when the
    screen cannot run. Blocks ONLY on an explicit activity_blocked hit;
    undisclosed/exempt/clean all PASS (fail-open on missing data)."""
    if not scope:
        return None, "screen_off"
    fn = _activity_screen_fn()
    if fn is None:
        return None, "screen_unavailable"
    try:
        _clean, why = fn(name or "", sector or "", industry or "")
    except Exception:
        return None, "screen_error"
    why = str(why or "")
    if not why.startswith("activity_blocked:"):
        # activity_clean / activity_islamic_exempt / activity_undisclosed
        return True, why
    term = why.split(":", 1)[1].strip().lower()
    if scope == "haram" and term in _ACTIVITY_FINANCE_TERMS:
        return True, "activity_finance_out_of_global_scope:" + term
    return False, why


_NOMU_RE = re.compile(r"^9\d{3}\.SR$")


_VENUE_CAL_MAP = {
    "SR": "XSAU", "T": "XTKS", "HK": "XHKG", "IS": "XIST", "L": "XLON",
    "AS": "XAMS", "BR": "XBRU", "PA": "XPAR", "DE": "XETR", "F": "XFRA",
    "MI": "XMIL", "MC": "XMAD", "ST": "XSTO", "TO": "XTSE", "SW": "XSWX",
    "US": "XNYS",
}
_VENUE_CAL_CACHE = {}
_LOG = logging.getLogger("core.analysis.opportunity_builder")
_VENUE_LAST_ERROR = {"msg": ""}
_FRESHNESS_FALLBACK_LOGGED = set()


def _venue_state(symbol, now_utc):
    """(is_open, prev_close_utc_datetime) for the symbol's venue at now_utc,
    or None when the calendar layer cannot answer (unknown suffix, import
    failure, any exception) — the caller then uses the permissive fallback.
    Lazy exchange_calendars + pandas; NEVER raises."""
    try:
        s = (str(symbol or "").strip().upper())
        suffix = s.rsplit(".", 1)[1] if "." in s else "US"
        code = _VENUE_CAL_MAP.get(suffix)
        if not code:
            return None
        cal = _VENUE_CAL_CACHE.get(code)
        if cal is None:
            import exchange_calendars as _xc  # lazy; present on Render
            cal = _xc.get_calendar(code)
            _VENUE_CAL_CACHE[code] = cal
        import pandas as _pd  # lazy
        ts = _pd.Timestamp(now_utc)
        try:
            prev_close = cal.previous_close(ts)
            prev_open = cal.previous_open(ts)
        except Exception:
            # v1.4.1: some pandas/exchange_calendars builds reject tz-AWARE
            # minutes here — retry tz-naive UTC (same instant).
            ts_n = ts.tz_convert("UTC").tz_localize(None) \
                if ts.tzinfo is not None else ts
            prev_close = cal.previous_close(ts_n)
            prev_open = cal.previous_open(ts_n)
        is_open = bool(prev_open > prev_close)
        pc = prev_close.to_pydatetime()
        if pc.tzinfo is None:
            pc = pc.replace(tzinfo=timezone.utc)
        return is_open, pc
    except Exception as e_vs:
        try:
            _VENUE_LAST_ERROR["msg"] = ("%s: %s" % (type(e_vs).__name__,
                                                    e_vs))[:120]
        except Exception:
            pass
        return None


def _quote_freshness_assessment(cand):
    """v1.4.0 (W-2). Returns (passed, current_str, detail). Pure-defensive:
    the only FAIL paths are PROVEN staleness; absent/unparseable timestamps
    and calendar outages never block a candidate on their own."""
    eng = cand.get("engine_gate") or {}
    qdt = _parse_ts_utc(eng.get("last_updated"))
    detail = {"quote_ts": (None if qdt is None else qdt.isoformat()),
              "mode": None, "age_min": None}
    if qdt is None:
        detail["mode"] = "skipped_no_timestamp"
        return True, "no timestamp (skipped)", detail
    now = datetime.now(timezone.utc)
    age_min = max(0.0, (now - qdt).total_seconds() / 60.0)
    detail["age_min"] = round(age_min, 1)
    max_min = _env_quote_max_age_min()
    if age_min <= max_min:
        detail["mode"] = "live"
        return True, "live %.0fm old" % age_min, detail
    vs = _venue_state(cand.get("symbol") or "", now)
    if vs is not None:
        is_open, prev_close = vs
        detail["prev_close"] = prev_close.isoformat()
        if is_open:
            detail["mode"] = "session_open"
            return False, ("STALE_PRICE intraday: quote %.0fm old while the "
                           "venue trades (max %.0fm)" % (age_min, max_min)), \
                detail
        detail["mode"] = "session_closed"
        if qdt >= prev_close - timedelta(minutes=2):
            return True, ("last close, %.1fh old" % (age_min / 60.0)), detail
        return False, ("STALE_PRICE pre-close: quote %.1fh old predates the "
                       "venue close %s"
                       % (age_min / 60.0,
                          prev_close.strftime("%Y-%m-%d %H:%M UTC"))), detail
    detail["mode"] = "fallback_no_calendar"
    try:
        _sfx = (str(cand.get("symbol") or "").strip().upper()
                .rsplit(".", 1)[-1]) or "?"
        if _sfx not in _FRESHNESS_FALLBACK_LOGGED:
            _FRESHNESS_FALLBACK_LOGGED.add(_sfx)
            _LOG.warning(
                "[FRESHNESS v%s] venue calendar unavailable for '.%s' -> "
                "78h fallback in effect (last err: %s)",
                OPPORTUNITY_BUILDER_VERSION, _sfx,
                _VENUE_LAST_ERROR.get("msg") or "none captured")
    except Exception:
        pass
    fb_h = _env_freshness_fallback_h()
    if age_min <= fb_h * 60.0:
        return True, ("fallback: %.1fh old, no venue calendar"
                      % (age_min / 60.0)), detail
    return False, ("STALE_PRICE %.1fh old (no venue calendar; cap %.0fh)"
                   % (age_min / 60.0, fb_h)), detail


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

    # v1.10.0 [FORECAST-PROVENANCE GATE]: a synthesized forecast must never
    # be sized as a ticket (header WHY — the 2026-08-06 board evaporation).
    # Appended ONLY when armed, so the gate list and every verdict are
    # byte-identical to v1.9.2 while TFB_T10_EXCLUDE_DEFAULT_CONF is unset.
    if _env_forecast_provenance_gate():
        _pv_ok, _pv_cur = _forecast_provenance_assessment(cand)
        g.append(_gate(
            "Forecast Provenance", _pv_ok, FAIL_MAJOR, _pv_cur,
            "provider-backed forecast (synthesized basis blocked; "
            "blank/Unknown passes)"))

    # v1.10.3 [B4b RELIABILITY-CLUSTER GATE]: the default-confidence
    # arithmetic fingerprint (reliability exactly on the operator-confirmed
    # cluster 70.4 / 71.5 / 75.4 / 76.5) must never be sized as a ticket,
    # regardless of forecast_source — provider_target / blank rows carry it
    # too (header WHY: the 2026-08-11 board — DDI.US 70.4 seated day-1,
    # PCG.US 70.4 pending). Appended ONLY when armed, so the gate list and
    # every verdict are byte-identical to v1.10.2 while
    # TFB_T10_EXCLUDE_REL_CLUSTER is unset (S-1 window law).
    if _env_rel_cluster_gate():
        _rc_ok, _rc_cur = _rel_cluster_assessment(cand)
        g.append(_gate(
            "Reliability Cluster", _rc_ok, FAIL_MAJOR, _rc_cur,
            "reliability off the default-confidence cluster "
            + _rel_cluster_values_text() + " (blank/Unknown passes)"))

    # v1.12.0 [B4c FORECAST CAP-BAND GATE]: the manufactured-target
    # fingerprint (12M target ≈ price × ~1.35, minted by degraded
    # enrichment and stamped provider_target — header WHY: the 2026-08-11
    # recovery cycles) must never be sized as a ticket. Value-level check,
    # source-agnostic. Appended ONLY when armed, so the gate list and every
    # verdict are byte-identical to v1.11.0 while TFB_T10_EXCLUDE_CAP_BAND
    # is unset (S-1 window law).
    if _env_cap_band_gate():
        _cb_ok, _cb_cur = _cap_band_assessment(cand)
        _cb_lo, _cb_hi = _env_cap_band()
        g.append(_gate(
            "Forecast Cap Band", _cb_ok, FAIL_MAJOR, _cb_cur,
            "implied 12M ratio outside the manufactured band "
            "\u00d7[%.3f\u2013%.3f] (blank passes)" % (_cb_lo, _cb_hi)))

    rel = cand["reliability"]
    min_rel = criteria["min_reliability"]
    if _env_rel_floor_mode() == "display":   # v1.19.0
        g.append(_gate("Reliability", True, None, _round1(rel),
                       ">= " + _fmt_num(min_rel),
                       "display-only (H-28: floor does not separate outcomes)"))
    elif rel is not None and rel >= min_rel:
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

    # v1.13.0 [TRUST-LINEAGE GATE — TRUST-001]: fires ONLY on the exact
    # contradiction — the source engine flagged this row low_data_trust,
    # yet the decision-layer DQ PASSES min_dq. A low-trust row whose DQ is
    # also low already fails the Data Quality gate above; a clean row is
    # untouched. Appended ONLY in mode "gate", so "tag" and off leave the
    # gate list and verdict byte-identical to v1.12.0. MAJOR =>
    # DO_NOT_INVEST: visible in audit / near-miss, never a selected ticket.
    if (_env_trust_lineage_mode() == "gate"
            and cand.get("trust_low_source") and dq_ok):
        g.append(_gate(
            "Trust Lineage", False, FAIL_MAJOR,
            "low_data_trust@dq=" + _fmt_num(_round1(cand["dq"])),
            "no low_data_trust source flag when DQ passes",
            "TRUST-001: DQ passed min_dq without enrichment provenance"))

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

    # v1.4.0 [W-2 QUOTE-FRESHNESS-GATE]: session-scale freshness on the price
    # feeding the ticket (the 168h trust ceiling passed EXE@88.13-vs-86.95,
    # NFG/0083.HK 2-day rows, ENKAI — evening audit 2026-07-20). MAJOR fail =>
    # DO_NOT_INVEST: candidate DEFERS with the STALE_PRICE tag in the audit
    # grid, never sized. DEFAULT ON; TFB_TICKET_FRESHNESS_GATE=0 restores the
    # v1.3.0 gate list byte-for-byte.
    if _env_freshness_gate():
        f_ok, f_cur, f_detail = _quote_freshness_assessment(cand)
        fg = _gate(
            "Quote Freshness", f_ok, FAIL_MAJOR, f_cur,
            ("live <= %.0fm in-session; else >= venue last close; "
             "fallback <= %.0fh"
             % (_env_quote_max_age_min(), _env_freshness_fallback_h())))
        fg["freshness_detail"] = f_detail
        g.append(fg)

    # v1.5.0 [COMPLIANCE GATE — KSA]: the rulebook enforced ON the surface.
    _sym_u = (str(cand.get("symbol") or "").strip().upper())
    if _env_compliance_gate() and _sym_u.endswith(".SR"):
        _fail_set, _restr = compliance_rule_sets()
        _ok_sh = _sym_u not in _fail_set
        g.append(_gate(
            "Shariah (KSA)", _ok_sh, FAIL_MAJOR,
            ("authority pass/uncovered" if _ok_sh else "AUTHORITY_FAIL"),
            "official authority list — FAIL is a structural block (§4.6)"))
    # v1.5.0 [ELIGIBILITY GATE — KSA]: the operator's tradable universe.
    if _env_eligibility_gate() and _sym_u.endswith(".SR"):
        _f2, _restr2 = compliance_rule_sets()
        _nomu = bool(_NOMU_RE.match(_sym_u))
        _ok_el = (not _nomu) and (_sym_u not in _restr2)
        g.append(_gate(
            "Eligibility (KSA)", _ok_el, FAIL_MAJOR,
            ("NOMU_BLOCKED" if _nomu else
             ("FOREIGN_RESTRICTED" if _sym_u in _restr2 else "eligible")),
            "Main Market only; foreign-resident tradable set"))

    # v1.19.2 [ELIGIBILITY (VENUE)]: the operator's tradable venues. Gate
    # is appended only when TFB_T10_VENUE_ALLOWLIST is set (byte-identical
    # otherwise). FAIL_MAJOR: an untradable venue is never a ticket.
    _venue_allow = _env_venue_allowlist()
    if _venue_allow:
        _ok_v, _v_cur = _venue_eligibility(cand, _venue_allow)
        g.append(_gate(
            "Eligibility (Venue)", _ok_v, FAIL_MAJOR, _v_cur,
            "market name or symbol suffix in TFB_T10_VENUE_ALLOWLIST"))

    # v1.9.0 [B-6 SHARIAH MODEL GATE — GLOBAL]: the resolver fail set,
    # finally consulted for NON-.SR candidates too. Same one-resolver
    # principle as Shariah (KSA); fed by TFB_EXIT_BY_RULE_EXTRA (the
    # board's published model-screen verdicts) until the automated feed
    # lands. Appended ONLY when armed, so the gate list and verdict are
    # byte-identical to v1.8.0 while TFB_OPP_SHARIAH_MODEL_GATE is unset.
    # Fail-open: absent-from-list passes — a missing list never empties
    # the board.
    if _env_shariah_model_gate() and _sym_u and not _sym_u.endswith(".SR"):
        _f3, _r3 = compliance_rule_sets()
        _ok_m = _sym_u not in _f3
        g.append(_gate(
            "Shariah (Model)", _ok_m, FAIL_MAJOR,
            ("model pass/unscreened" if _ok_m else "MODEL_SCREEN_FAIL"),
            "published model-screen FAIL set (one resolver) — "
            "FAIL is a structural block (§4.6)"))

    # v1.8.0 [1.4 GLOBAL ACTIVITY SCREEN]: haram-activity exclusion on EVERY
    # venue, not just .SR. Appended only when armed, so the gate list and the
    # verdict are byte-identical to v1.7.0 while TFB_GLOBAL_ACTIVITY_SCREEN is
    # unset. Fails MAJOR => DO_NOT_INVEST (audit/near-miss, never selected).
    _act_scope = _env_global_activity_screen()
    if _act_scope:
        _act_ok, _act_why = global_activity_verdict(
            cand.get("name"), cand.get("sector"), cand.get("industry"),
            _act_scope)
        if _act_ok is not None:
            g.append(_gate(
                "Activity Screen", _act_ok, FAIL_MAJOR, _act_why,
                ("no haram activity (global, all venues)"
                 if _act_scope == "haram"
                 else "no haram or conventional-finance activity (global)")))

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

    # v1.10.2 [G-1 BLOCKED-IDENTITY GATE]: BLOCKED is not WATCHLIST.
    # EVIDENCE (independent audit, 2026-08-09 board 10:37): five .VN rows
    # (CTG/MBB/TCB/VIB/VPB) sat INSIDE the 125-name qualified set with
    # investability = BLOCKED — carrying VND-scale prices mislabeled
    # USD/'NASDAQ/NYSE' (an identity/currency corruption class). They
    # slipped because the operator's deliberate Require-Investable=No
    # (the 2026-07-24 retirement, measured 9:1 false-bench on WATCHLIST)
    # switches off the WHOLE Investability check — including the one
    # verdict that retirement was never meant to cover. WATCHLIST is a
    # conservative OPINION; BLOCKED is an identity/tradability HARD STATE.
    # This gate fails MAJOR on exactly the token "blocked" — nothing
    # else — regardless of the retired gate's setting. Blank/Unknown/
    # WATCHLIST all PASS here untouched (fail-open + traced, the house
    # convention). Appended ONLY when blocked_identity_gate_enabled =>
    # byte-identical v1.10.1 when TFB_OPP_BLOCKED_IDENTITY_GATE=0.
    if criteria.get("blocked_identity_gate_enabled"):
        _bi_raw = (cand.get("engine_gate") or {}).get("investability")
        _bi_norm = _norm_token(_to_text(_bi_raw) or "")
        _bi_ok = _bi_norm != "blocked"
        g.append(_gate(
            "Blocked Identity", _bi_ok, FAIL_MAJOR,
            (_to_text(_bi_raw) or "Unknown"),
            "engine identity/tradability not BLOCKED (blank/Unknown/WATCHLIST pass)"))

    # v1.7.0 [SELL-CLASS GATE]: the narrow guard — MAJOR-fail only an
    # EXPLICIT engine sell-tier verdict, instead of the Investability gate's
    # all-or-nothing "must be INVESTABLE" (which benched 9 legitimate names
    # for every 1 it was right about; see the header WHY block). Enforces
    # standing audit gate #2 ("no INVEST on SELL-class") explicitly.
    # Blank/unknown/buy-tier PASSES (fail-open + traced), matching the
    # Investability gate convention. Appended ONLY when
    # sell_class_gate_enabled => byte-identical v1.6.0 when
    # TFB_OPP_SELL_CLASS_GATE=0.
    if criteria.get("sell_class_gate_enabled"):
        _sc_raw = cand.get("recommendation")
        _sc_norm = _norm_token(_to_text(_sc_raw) or "")
        _sc_ok = _sc_norm not in _SELL_CLASS_TOKENS
        g.append(_gate(
            "Sell-Class", _sc_ok, FAIL_MAJOR,
            (_to_text(_sc_raw) or "Unknown"),
            "engine reco not sell-tier (blank/Unknown passes)"))

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
                (str(cand["symbol"] or "").strip().upper() in held
                 if _env_held_variant_match()
                 else cand["symbol"] in held))
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
    # v1.19.0: in display mode the confidence band (derived from the same
    # non-separating score; H-28: HIGH-confidence cohorts win LESS than MEDIUM)
    # no longer caps INVEST at WATCH either.
    if (verdict == VERDICT_INVEST and confidence_band(reliability) == CONF_LOW
            and _env_rel_floor_mode() != "display"):
        return VERDICT_WATCH
    return verdict


def first_failed_gate(gates):
    order = {name: i for i, name in enumerate(GATE_ORDER)}
    failed = [g for g in gates if not g["passed"]]
    failed.sort(key=lambda g: order.get(g["gate"], 99))
    return failed[0] if failed else None


_GATE_DEPTH_INDEX = {name: i for i, name in enumerate(GATE_ORDER)}  # v1.19.1


def _audit_depth_key(a):
    """v1.19.1 [AUDIT-DEPTH-ORDER] sort key for the written audit / near-miss
    pool: INVEST first, then the row whose FIRST failing gate sits deepest in
    GATE_ORDER (it survived more gates), then reliability desc, score desc,
    symbol. A row with no first_fail that is not INVEST (structural) sorts
    as deepest-possible so it is never hidden behind a gate-5 failure."""
    ff = a.get("first_fail") or None
    if ff:
        depth = _GATE_DEPTH_INDEX.get(ff.get("gate"), -1)
    else:
        depth = len(GATE_ORDER)
    return (0 if a.get("verdict") == VERDICT_INVEST else 1,
            -depth,
            -float(a.get("reliability") or 0.0),
            -float(a.get("opportunity_score") or 0.0),
            str(a.get("symbol") or ""))


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
            "sector": (_normalize_sector(_to_text(h.get("sector")))
                       if _env_sector_normalize()
                       else _to_text(h.get("sector"))) or "Unknown",
            "market": _to_text(h.get("market")) or "Unknown",
            "value_sar": _to_float(h.get("value_sar")) or 0.0,
        })
    if pv <= 0 and holdings:
        pv = sum(h["value_sar"] for h in holdings)
    return {"cash": max(0.0, cash), "proceeds": max(0.0, proceeds),
            "portfolio_value": max(0.0, pv), "holdings": holdings}


def _cash_floor_sar() -> float:
    """v1.16.0: absolute cash reserve in SAR (TFB_OPP_CASH_FLOOR_SAR,
    default '' = 0.0 = off). Unparsable values are loudly ignored."""
    raw = (os.getenv("TFB_OPP_CASH_FLOOR_SAR") or "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw.replace(",", "")))
    except (TypeError, ValueError):
        try:
            _LOG.warning("[CASH-FLOOR] CONFIG INVALID: %r is not a number - "
                         "floor INACTIVE; fix TFB_OPP_CASH_FLOOR_SAR", raw)
        except Exception:
            pass
        return 0.0


def _sector_cap_basis() -> str:
    """v1.0.24: 'budget' (default) -> sector-weight checks divide by
    budget_base = portfolio_value + deployable; 'legacy' -> v1.0.23
    portfolio-value-only denominators, byte-identical."""
    v = (os.getenv("TFB_OPP_SECTOR_CAP_BASIS") or "budget").strip().lower()
    return v if v in {"budget", "legacy", "pd"} else "budget"


def _sector_context(pf, criteria, deployable=0.0):
    sectors = {}
    for h in pf["holdings"]:
        sectors[h["sector"]] = sectors.get(h["sector"], 0.0) + h["value_sar"]
    cap_hit = set()
    # v1.0.24: cash-aware base under the default 'budget' basis; 'legacy'
    # keeps the v1.0.23 pv-only formula exactly.
    _scb = _sector_cap_basis()
    if _scb == "budget":
        _base = pf["portfolio_value"] + max(0.0, float(deployable or 0.0))
        if _base > 0:
            cap = criteria["pf_max_sector_pct"]
            for s, v in sectors.items():
                if v / _base * 100.0 >= cap:
                    cap_hit.add(s)
    elif pf["portfolio_value"] > 0:
        # 'legacy' and v1.16.0 'pd' share the pv-only denominator here.
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
    # v1.11.0 [F-1]: venue board lot layered over the global criterion.
    # _venue_lot_for_symbol returns 0 when TFB_T10_VENUE_LOTS is unset, so
    # the unarmed path is byte-identical to v1.10.3.
    lot = max(1, int(criteria["lot_size"]), _venue_lot_for_symbol(cand))
    shares = 0
    # v1.15.0: optional worst-entry sizing — divide by the advertised
    # entry-high (price*1.01) so the ticket stays affordable at its own
    # worst fill. OFF (default) = v1.14.0 byte-identical.
    _sz_px = price_sar * 1.01 if _size_at_entry_high() else price_sar
    if _sz_px > 0 and alloc >= _sz_px * lot:
        shares = int(alloc // (_sz_px * lot)) * lot
    # v1.15.1: the reserved ticket must survive the worst advertised fill —
    # book at the same price the sizing assumed. OFF: shares*price verbatim.
    suggested = shares * _sz_px
    return suggested, shares


_LAST_DEPLOYABLE_SPLIT = {"current": 0, "proforma": 0}
# v1.18.0: symbol -> {"need_sar", "sized_sar", "remaining_sar"} recorded at the
# funding-deferral sites of _select_and_size (reset per call).
_LAST_FUNDING_NEEDS: dict = {}


def _env_funding_plan():
    """v1.18.0 kill-switch. Default ON; 0/false/off/no = v1.17.0 byte-identical."""
    return str(_env_str("TFB_OPP_FUNDING_PLAN", "1") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _env_rotation_edge_pp():
    try:
        v = float(_env_str("TFB_OPP_ROTATION_EDGE_PP", "8") or 8.0)
        return v if v >= 0 else 8.0
    except (TypeError, ValueError):
        return 8.0


def _env_rotation_cost_pct():
    try:
        v = float(_env_str("TFB_OPP_ROTATION_COST_PCT", "1.1") or 1.1)
        return v if v >= 0 else 1.1
    except (TypeError, ValueError):
        return 1.1


def _env_rotation_exclude():
    raw = str(_env_str("TFB_OPP_ROTATION_EXCLUDE", "5023.SR") or "").upper()
    return {t.strip() for t in raw.replace(";", ",").split(",") if t.strip()}


def _env_capital_call_topn():
    try:
        v = int(float(_env_str("TFB_OPP_CAPITAL_CALL_TOPN", "3") or 3))
        return v if v > 0 else 3
    except (TypeError, ValueError):
        return 3


def _holding_roi_map(audit_rows, holdings):
    """v1.18.0 PURE: held symbol -> engine 12M forecast (pct) read from the
    scanned pool: exact symbol first, then the .US alias either way."""
    pool = {}
    for a in (audit_rows or []):
        c = a.get("_cand") if isinstance(a, dict) else None
        if not isinstance(c, dict):
            continue
        s = str(a.get("symbol") or "").strip().upper()
        if s:
            pool[s] = _engine_roi_to_pct(c.get("engine_roi_12m_pct"))
    out = {}
    for h in (holdings or []):
        s = str(h.get("symbol") or "").strip().upper()
        if not s:
            continue
        for k in (s, s + ".US", s[:-3] if s.endswith(".US") else s):
            if k in pool and pool[k] is not None:
                out[s] = pool[k]
                break
    return out


def _env_rotation_min_held_days():
    try:
        v = float(_env_str("TFB_OPP_ROTATION_MIN_HELD_DAYS", "7") or 7)
        return v if v >= 0 else 7.0
    except (TypeError, ValueError):
        return 7.0


def _env_rotation_tp1_proximity_pct():
    try:
        v = float(_env_str("TFB_OPP_ROTATION_TP1_PROXIMITY_PCT", "3") or 3)
        return v if v >= 0 else 3.0
    except (TypeError, ValueError):
        return 3.0


def _holding_rotation_eligible(h, today=None):
    """v1.18.1 PURE: (eligible, reason). Fail-open per field: a holding that
    lacks buy_date / tp1_sar / price_sar is not excluded on that criterion."""
    import datetime as _dt
    today = today or _dt.date.today()
    bd = h.get("buy_date")
    if bd:
        try:
            d = _dt.date.fromisoformat(str(bd)[:10])
            held = (today - d).days
            if held < _env_rotation_min_held_days():
                return False, f"held {held}d < {_env_rotation_min_held_days():g}d"
        except Exception:
            pass
    tp1 = _to_float(h.get("tp1_sar"))
    px = _to_float(h.get("price_sar"))
    if tp1 and px and tp1 > 0 and px > 0:
        gap_pct = (tp1 - px) / tp1 * 100.0
        if 0 <= gap_pct <= _env_rotation_tp1_proximity_pct():
            return False, f"within {gap_pct:.1f}% of TP1"
    return True, ""


def _rotation_pick(opp_roi_pct, holdings, hold_roi, exclude, edge_pp, cost_pct):
    """v1.18.0 PURE: the held equity with the LOWEST engine forecast that is
    worse than the opportunity by >= edge_pp after round-trip cost; None when
    nothing qualifies. Excluded symbols (sukuk) and holdings without a
    forecast in the pool never rotate."""
    if opp_roi_pct is None:
        return None
    best = None
    for h in (holdings or []):
        s = str(h.get("symbol") or "").strip().upper()
        if not s or s in exclude or (h.get("value_sar") or 0) <= 0:
            continue
        if not _holding_rotation_eligible(h)[0]:   # v1.18.1
            continue
        r = hold_roi.get(s)
        if r is None:
            continue
        edge = float(opp_roi_pct) - float(r) - float(cost_pct)
        if edge < float(edge_pp):
            continue
        if best is None or r < best["roi_pct"]:
            best = {"symbol": s, "value_sar": float(h.get("value_sar") or 0.0),
                    "roi_pct": float(r), "edge_pp": round(edge, 1)}
    return best


def _funding_plans(ordered_needs, remaining, holdings, hold_roi, exclude,
                   edge_pp, cost_pct):
    """v1.18.0 PURE: sequential funding plans for rank-ordered unfunded names.
    ordered_needs: [(symbol, need_sar, opp_roi_pct)]. Cash covers plans in rank
    order; ONE rotation proposal per run; the rest are capital calls."""
    plans, avail, rotation_used = [], max(0.0, float(remaining or 0.0)), False
    for sym, need, opp_roi in ordered_needs:
        need = float(need or 0.0)
        if need <= 0:
            continue
        short = max(0.0, need - avail)
        avail = max(0.0, avail - need)
        plan = {"symbol": sym, "need_sar": round(need, 0),
                "shortfall_sar": round(short, 0), "state": "FUNDABLE_NOW",
                "rotation": None}
        if short > 0:
            rot = None
            if not rotation_used:
                rot = _rotation_pick(opp_roi, holdings, hold_roi, exclude,
                                     edge_pp, cost_pct)
            if rot is not None:
                rotation_used = True
                proceeds = min(rot["value_sar"], short)
                rot = dict(rot, proceeds_sar=round(proceeds, 0),
                           action=("EXIT" if proceeds >= rot["value_sar"] - 0.5
                                   else "TRIM"))
                plan["rotation"] = rot
                plan["shortfall_sar"] = round(max(0.0, short - proceeds), 0)
                plan["state"] = "FUNDABLE_BY_ROTATION"
            else:
                plan["state"] = "CAPITAL_CALL"
        plans.append(plan)
    return plans


def _funding_plan_text(plan, remaining):
    """v1.18.0 PURE: the clause appended to the Funding near-miss reason."""
    st = plan.get("state")
    if st == "FUNDABLE_BY_ROTATION":
        r = plan["rotation"]
        txt = (" | FUNDABLE_BY_ROTATION: " + r["action"].lower() + " " +
               r["symbol"] + " " + _fmt_sar(r["proceeds_sar"]) +
               " (engine 12M edge +" + _fmt_num(r["edge_pp"]) + "pp after cost)"
               " \u2192 ticket " + _fmt_sar(plan["need_sar"]))
        if (plan.get("shortfall_sar") or 0) > 0:
            txt += "; residual CAPITAL_CALL " + _fmt_sar(plan["shortfall_sar"])
        return txt
    if st == "CAPITAL_CALL":
        return (" | CAPITAL_CALL: deposit \u2265 " + _fmt_sar(plan["shortfall_sar"]) +
                " for a " + _fmt_sar(plan["need_sar"]) + " ticket (cash " +
                _fmt_sar(max(0.0, float(remaining or 0.0))) + ")")
    return ""


def _funding_settled_only() -> bool:
    """v1.15.0: TFB_OPP_FUNDING_SETTLED_ONLY — default OFF (doctrine L7,
    cash + pending proceeds). ON: only settled cash funds ADDs."""
    return str(os.getenv("TFB_OPP_FUNDING_SETTLED_ONLY") or "0").strip() \
        .lower() in ("1", "true", "yes", "on")


def _size_at_entry_high() -> bool:
    """v1.15.0: TFB_OPP_SIZE_AT_ENTRY_HIGH — default OFF (size at current
    price, v1.14.0 verbatim). ON: size at price*1.01 (worst advertised
    entry) so the promised allocation survives an entry-high fill."""
    return str(os.getenv("TFB_OPP_SIZE_AT_ENTRY_HIGH") or "0").strip() \
        .lower() in ("1", "true", "yes", "on")


def _funds_from(suggested, cash_left, proceeds_left):
    """L7: every ADD names its funding source; split cash-first.
    v1.15.0: settled-only mode zeroes the proceeds leg at the source."""
    if _funding_settled_only():
        proceeds_left = 0.0
    from_cash = min(suggested, cash_left)
    from_proceeds = min(max(0.0, suggested - from_cash), proceeds_left)
    parts = []
    if from_cash > 0:
        parts.append("Cash " + _fmt_sar(from_cash))
    if from_proceeds > 0:
        parts.append("TRIM/EXIT proceeds " + _fmt_sar(from_proceeds))
    label = " + ".join(parts) if parts else "Unfunded (no deployable capital)"
    return label, cash_left - from_cash, proceeds_left - from_proceeds


def _market_cap_key(market):
    """v1.0.11: canonical key for the per-market diversification cap so the same
    venue written two ways (e.g. 'NYSE/NASDAQ' vs 'NASDAQ/NYSE') counts as ONE
    market. Splits on '/', trims+uppercases each token, sorts, and rejoins;
    single-token markets are returned trimmed+uppercased unchanged. This affects
    ONLY the cap counter key — the displayed market string is never altered."""
    s = "" if market is None else str(market)
    parts = [p.strip().upper() for p in s.split("/")]
    parts = [p for p in parts if p]
    if not parts:
        return s.strip().upper()
    parts.sort()
    return "/".join(parts)


# ---------------------------------------------------------------------------
# v1.0.16: issuer-level cross-listing dedup key (hybrid: curated override map
# first, normalized company name otherwise). Used by _select_and_size only when
# issuer_dedup_enabled is True.
# ---------------------------------------------------------------------------
# Override map (default EMPTY). A curator may add entries to:
#   - FORCE-MERGE: point two symbols at the SAME id when their names diverge
#     across listings (e.g. an ADR named differently from the local line).
#   - FORCE-SPLIT: point a symbol at a UNIQUE id to stop a wrong merge of two
#     genuinely-distinct issuers that happen to share a normalized name.
# Empty => pure normalized-name behavior, which already collapses the live dupes
# (Takeda 4502.T/TAK.US, BMW.DE/BMW.XETRA, MUV2.XETRA/MUV2.DE) because their
# company names are identical across listings.
_ISSUER_DEDUP_MAP = {}

# Legal-form suffixes / connectors stripped before keying, so the same company
# keys identically across exchanges regardless of local legal suffix.
_ISSUER_SUFFIX_RE = re.compile(
    r"\b("
    r"incorporated|corporation|company|limited|holdings|holding|group|"
    r"inc|corp|co|ltd|plc|llc|lp|"
    r"ag|aktiengesellschaft|kgaa|se|nv|bv|sa|spa|ab|asa|oyj|oy|as|"
    r"sab|adr|ads|the"
    r")\b", re.IGNORECASE)


def _issuer_key(cand):
    """v1.0.16: stable issuer key for cross-listing dedup. Curated override map
    wins (force-merge / force-split); otherwise a normalized company name with
    legal suffixes and punctuation removed. Falls back to the symbol when the
    name is missing or equals the symbol, so a nameless row can NEVER false-merge
    into another issuer."""
    sym = str(cand.get("symbol") or "").strip().upper()
    if sym in _ISSUER_DEDUP_MAP:
        return _ISSUER_DEDUP_MAP[sym]
    name = str(cand.get("name") or "").strip()
    if not name or name.upper() == sym:
        return "sym:" + sym  # no usable name -> keyed to itself, never merges
    k = _ISSUER_SUFFIX_RE.sub(" ", name.lower())
    k = re.sub(r"[^a-z0-9]+", " ", k)   # drop punctuation / non-ascii to spaces
    k = re.sub(r"\s+", " ", k).strip()
    return ("name:" + k) if k else ("sym:" + sym)


def _select_and_size(invest_cands, criteria, pf, sector_ctx):
    """L2 cap + §4.2 diversification (selection-time, defer) + §4.4 sizing.
    Returns (tickets_raw, deferrals{symbol: reason})."""
    # v1.15.0: settled-only mode removes pending proceeds from funding.
    _LAST_FUNDING_NEEDS.clear()  # v1.18.0
    _LAST_DEPLOYABLE_SPLIT["current"] = round(pf["cash"], 0)
    _LAST_DEPLOYABLE_SPLIT["proforma"] = round(pf["cash"] + pf["proceeds"], 0)
    _floor = _cash_floor_sar()
    deployable = pf["cash"] + (0.0 if _funding_settled_only()
                               else pf["proceeds"])
    budget_base = pf["portfolio_value"] + deployable
    remaining = deployable
    cash_left, proceeds_left = pf["cash"], pf["proceeds"]
    if _floor > 0:  # v1.16.0: absolute reserve, never funded from
        _res = min(cash_left, _floor)
        cash_left -= _res
        deployable = max(0.0, deployable - _res)

    sector_counts, market_counts = {}, {}
    canon_market = _env_canon_market()  # v1.0.11 kill-switch (default ON)
    pf_sector_sar = dict(sector_ctx["sectors"])
    picked, deferrals = [], {}
    # v1.0.16: issuer-level cross-listing dedup (default OFF).
    issuer_dedup = bool(criteria.get("issuer_dedup_enabled", False))
    funded_issuers = {}

    for cand in invest_cands:
        if len(picked) >= criteria["max_selected"]:
            break
        # v1.0.16: issuer-level cross-listing dedup (default OFF). Once an issuer
        # is FUNDED, a later listing of the SAME issuer (e.g. Takeda 4502.T then
        # TAK.US, or BMW.DE then BMW.XETRA) is deferred rather than taking a
        # second slot. Keyed at funding (below), so an issuer whose top symbol
        # was sector-capped or floored is not pre-empted from a fundable listing.
        if issuer_dedup:
            _ikey = _issuer_key(cand)
            if _ikey in funded_issuers:
                deferrals[cand["symbol"]] = (
                    "Duplicate issuer \u2014 already funded " +
                    funded_issuers[_ikey])
                continue
        sec, mkt = cand["sector"], cand["market"]
        # v1.0.11: canonical cap key so "NYSE/NASDAQ" and "NASDAQ/NYSE" count as
        # one market (kill-switch off => raw string, byte-identical v1.0.10).
        mkt_key = _market_cap_key(mkt) if canon_market else mkt
        sector_cap_hit = sector_counts.get(sec, 0) >= criteria["max_per_sector"]
        if _env_sector_normalize() and sec in ("", "Unknown"):
            sector_cap_hit = False  # v1.0.13: data-gap bucket is not a real sector
        if sector_cap_hit:
            deferrals[cand["symbol"]] = (
                "Diversification: sector cap " +
                str(criteria["max_per_sector"]) + "/" +
                str(criteria["max_per_sector"]) + " (" + sec + ")")
            continue
        if market_counts.get(mkt_key, 0) >= criteria["max_per_market"]:
            deferrals[cand["symbol"]] = (
                "Diversification: market cap reached (" + mkt + ")")
            continue
        suggested, shares = _size_one(cand, criteria, budget_base, remaining)
        # v1.11.0 [F-1 VENUE BOARD LOTS]: when the venue lot alone priced the
        # name out (allocation buys >= 1 share but < 1 lot), say so honestly
        # instead of letting it read as capital exhaustion. Inert while
        # TFB_T10_VENUE_LOTS is unset (_vlot == 0 => v1.10.3 byte-for-byte).
        _vlot = _venue_lot_for_symbol(cand)
        if _vlot > 1 and shares == 0:
            _p = cand["price_sar"] or 0.0
            _alloc = max(0.0, min(
                criteria["max_weight_pct"] / 100.0 * budget_base, remaining))
            if _p > 0 and _alloc >= _p:
                deferrals[cand["symbol"]] = (
                    "Unfunded \u2014 one board lot (" +
                    "{:,}".format(_vlot) + " sh \u2248 " +
                    _fmt_sar(_p * _vlot) + ") exceeds the sized allocation " +
                    _fmt_sar(_alloc))
                continue
        # v1.0.14: minimum-ticket floor (OFF when min_ticket_sar <= 0). A sized
        # ticket below the floor is not a meaningful executable position (e.g.
        # 75 SAR / 4 sh from the last scraps of cash). Defer it instead of
        # funding it; `remaining` only shrinks, so once it falls below the floor
        # every later pick is sub-floor too and is likewise deferred. suggested
        # == 0 (capital fully exhausted) is intentionally left to the existing
        # unfunded_watch path; this floor handles the 0 < suggested < min band.
        _min_ticket = criteria.get("min_ticket_sar", 0.0) or 0.0
        # v1.1.0 (§18.5/§19.3, opt-in): the venue floor RAISES the operator
        # floor; the existing deferral + near-miss machinery does the rest.
        if _env_venue_floors():
            _vf = _venue_floor(cand["symbol"])
            if _vf and float(_vf) > _min_ticket:
                _min_ticket = float(_vf)
        if _min_ticket > 0.0 and 0.0 < suggested < _min_ticket:
            deferrals[cand["symbol"]] = (
                "Unfunded \u2014 sized ticket " + _fmt_sar(suggested) +
                " below minimum ticket floor " + _fmt_sar(_min_ticket))
            _LAST_FUNDING_NEEDS[cand["symbol"]] = {   # v1.18.0
                "need_sar": float(_min_ticket), "sized_sar": float(suggested),
                "remaining_sar": float(remaining)}
            continue
        # §4.2 combined post-action portfolio sector cap (only if sized & ctx)
        # v1.0.24: 'budget' basis divides by budget_base (pv + deployable) —
        # the true post-round portfolio; 'legacy' keeps pv + suggested.
        _scb = _sector_cap_basis()
        _scb_budget = _scb == "budget"
        _cap_base_ok = (budget_base > 0) if _scb_budget else (pf["portfolio_value"] > 0)
        if (suggested > 0 and _cap_base_ok and
                sector_ctx["available"]):
            # v1.16.0 'pd': PD-consistent pv-only denominator (post_sector
            # includes the ticket; the total does not double-count cash).
            post_total = (budget_base if _scb_budget else
                          (pf["portfolio_value"] if _scb == "pd"
                           else pf["portfolio_value"] + suggested))
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
        market_counts[mkt_key] = market_counts.get(mkt_key, 0) + 1
        pf_sector_sar[sec] = pf_sector_sar.get(sec, 0.0) + suggested
        picked.append({"cand": cand, "suggested_sar": suggested,
                       "suggested_shares": shares,
                       "funds_from": funds_label})
        if issuer_dedup:
            funded_issuers[_ikey] = cand["symbol"]
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


def _stop_vol_input(vol30: Optional[float]) -> Optional[float]:
    """v1.2.0: normalize the volatility input the stop model consumes.

    Flag OFF -> returns the raw value, i.e. v1.1.0 behaviour verbatim.
    Flag ON  -> a fraction-shaped value (annualized, e.g. 0.28 = 28%) is
    converted to MONTHLYIZED PERCENT, which is what stop_vol_mult expects.
    Shape detection (< 5.0) keeps a percent-shaped provider untouched, so
    the fix cannot double-apply."""
    if vol30 is None or vol30 <= 0:
        return None
    raw = float(vol30)
    if not _stop_vol_units_fix_enabled():
        return raw
    if raw < 5.0:                       # annualized fraction
        return raw * 100.0 / math.sqrt(12.0)
    return raw                          # already percent-shaped


def _stop_vol_units_fix_enabled() -> bool:
    val = os.environ.get("TFB_OPP_STOP_VOL_UNITS_FIX", "0")
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _build_ticket(rank, pick, criteria, review_date):
    cand = pick["cand"]
    fx = cand["fx_to_sar"]
    conf = confidence_band(cand["reliability"])
    # Reproducibility contract: exp_gain is derived from the DISPLAYED
    # (rounded) suggested_sar and ann_roi_pct so the sheet can re-verify it.
    suggested = round(pick["suggested_sar"], 0)
    ann = _round1(cand["ann_roi_pct"]) or 0.0
    exp_gain = round(suggested * ann / 100.0, 0)
    # v1.0.20 (Fix #3): the swap happens AFTER the valuation figures are
    # computed so both sets exist. Under the engine basis the reproducibility
    # contract (exp_gain == suggested x displayed ann/100) HOLDS with
    # ann = the engine 12M % (the sheet's "Ann ROI %" and "Gain 12M" columns
    # are 12-month figures, exactly the engine forecast's native horizon).
    _val_roi = _round1(cand["roi_pct"])
    _val_ann = ann
    _val_exp_gain = exp_gain
    # v1.0.5: surface the engine 12M forecast alongside (never substituted into)
    # the valuation roi_pct. OFF => engine_pct stays None and every assignment
    # below is byte-identical v1.0.4.
    _eng_display = bool(criteria.get("engine_roi_display_enabled"))
    # v1.0.20 (Fix #3): engine-primary basis. Engine mode implies the v1.0.5
    # enrichment (both figures always visible), so engine_pct is computed
    # whenever EITHER switch is on. Default mode with display off keeps
    # engine_pct None -> every assignment below is byte-identical v1.0.19.
    _prb = str(criteria.get("primary_roi_basis") or "valuation").strip().lower()
    _basis_engine = _prb == "engine"
    # v1.0.23: TP1 execution-plan basis (the NEW default). Plan mode, like
    # engine mode, implies both parallel figures are visible so the sheet's
    # three ROI columns carry three DIFFERENT honest meanings.
    _basis_plan = _prb == "plan"
    if _basis_engine or _basis_plan:
        _eng_display = True
    engine_pct = (_engine_roi_to_pct(cand["engine_roi_12m_pct"])
                  if _eng_display else None)
    # v1.0.23: per-ticket plan figures. Honest fallback: no TP ladder (the
    # v1.0.22 TP-COHERENCE guard blanked it) -> plan undefined -> valuation
    # basis for THIS ticket (never invents a plan).
    _plan_roi = None
    if (_basis_plan and cand["price"] and cand["tp1"]
            and cand["price"] > 0):
        _plan_roi = _round1(
            (cand["tp1"] - cand["price"]) / cand["price"] * 100.0)
    _ticket_plan_primary = _plan_roi is not None
    # Per-ticket effective basis: engine mode falls back HONESTLY to the
    # valuation basis when the engine forecast is absent (never invents).
    _ticket_engine_primary = ((not _ticket_plan_primary)
                              and _basis_engine and engine_pct is not None)
    detail_engine_roi = _round1(cand["engine_roi_12m_pct"])
    note = _advisor_sentence(cand, suggested, pick["suggested_shares"], conf,
                             review_date)
    if _ticket_plan_primary:
        # Reproducibility contract preserved: exp_gain == suggested x
        # displayed ann / 100, with ann = the PLAN ROI annualized over the
        # panel period (same compound formula as normalize_candidate).
        _pm = max(1, int(criteria["period_months"]))
        _pd = _pm * DAYS_PER_MONTH
        ann = (_round1((math.pow(1.0 + _plan_roi / 100.0, 365.0 / _pd)
                        - 1.0) * 100.0)
               if _plan_roi > -100.0 else 0.0) or 0.0
        exp_gain = round(suggested * ann / 100.0, 0)
    if _ticket_engine_primary:
        ann = _round1(engine_pct) or 0.0
        exp_gain = round(suggested * ann / 100.0, 0)
    if _eng_display:
        detail_engine_roi = _round1(engine_pct)
        if _ticket_plan_primary:
            note = note + (" Primary ROI/gain are the TP1 execution plan ("
                           + _fmt_num(_plan_roi)
                           + "% to first target); engine 12M forecast "
                           + (_fmt_num(_round1(engine_pct))
                              if engine_pct is not None else "unavailable")
                           + "%; full valuation target "
                           + _fmt_num(_val_roi)
                           + "% \u2014 entry/stop/TP are valuation-based "
                           "levels, not forecasts.")
        elif engine_pct is None:
            note = note + (" Engine 12M forecast: unavailable \u2014 the upside "
                           "shown is a valuation target, not a forecast.")
        elif _ticket_engine_primary:
            note = note + (" Primary ROI/gain are the engine 12M forecast ("
                           + _fmt_num(_round1(engine_pct))
                           + "%); valuation target "
                           + _fmt_num(_val_roi)
                           + "% \u2014 entry/stop/TP are valuation-based "
                           "levels, not forecasts.")
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
        "roi_pct": (_plan_roi if _ticket_plan_primary
                    else _round1(engine_pct) if _ticket_engine_primary
                    else _val_roi),
        "ann_roi_pct": (ann if (_ticket_plan_primary
                                or _ticket_engine_primary)
                        else _round1(cand["ann_roi_pct"])),
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
            # v1.0.23: under the plan basis the decision R/R speaks the PLAN
            # (plan ROI / stop distance); valuation R/R stays in cand for
            # the audit grid. Other bases: byte-identical valuation R/R.
            "rr": (_round2(_plan_roi / cand["stop_pct"])
                   if (_ticket_plan_primary and cand["stop_pct"]
                       and cand["stop_pct"] > 0)
                   else _round2(cand["rr"])),
            # v1.8.0 [PY-1]: the TP2-ladder R/R — (tp2-price)/(price-stop) —
            # i.e. exactly the number the Risk/Reward gate tests, the advisor
            # sentence prints and the audit grid emits. Emitted on EVERY
            # basis so kpis.blended_rr can never again speak a different
            # language from the tickets it summarizes.
            "rr_tp2": _round2(cand["rr"]),
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
        ticket["valuation_roi_pct"] = _val_roi
        ticket["engine_exp_gain_12m_sar"] = engine_gain
        # v1.0.20: under the engine basis exp_gain_12m_sar IS the engine
        # figure, so the valuation parallel must carry the true valuation
        # gain (pre-swap), not a copy of the primary.
        ticket["valuation_exp_gain_12m_sar"] = _val_exp_gain
    if _basis_engine:
        ticket["primary_roi_basis"] = ("engine" if _ticket_engine_primary
                                       else "valuation")
    elif _basis_plan:
        # v1.0.23: stamp the effective basis; a no-ladder fallback is
        # labelled honestly as valuation for THIS ticket.
        ticket["primary_roi_basis"] = ("plan" if _ticket_plan_primary
                                       else "valuation")
    _annotate_cost_edge(ticket, suggested)  # v1.1.0 net-edge stamp (env-gated)
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
    # v1.19.1 [AUDIT-DEPTH-ORDER]: near-miss = the rows that got FURTHEST, not
    # the highest-scoring failures. Kill-switch TFB_OPP_AUDIT_ORDER=score.
    if _env_audit_order() == "depth":
        pool.sort(key=_audit_depth_key)
    else:
        pool.sort(key=lambda a: (-(a["opportunity_score"] or 0.0), a["symbol"]))
    rows = []
    for a in pool[:criteria["near_miss_n"]]:
        if a["symbol"] in deferrals:
            _reason = deferrals[a["symbol"]]
            # v1.0.15: a floor deferral (min-ticket floor, v1.0.14) is a FUNDING
            # near-miss, not a diversification one — classify it distinctly so
            # the gate / required / how-to columns are accurate. Real
            # diversification deferrals keep their byte-identical labeling.
            if "minimum ticket floor" in _reason:
                gate, cur, req = "Funding", _reason, (
                    "fundable amount \u2265 minimum ticket floor (" +
                    _fmt_sar(criteria.get("min_ticket_sar", 0.0) or 0.0) + ")")
                note = ("Qualified (INVEST) \u2014 ranked, but the fundable "
                        "amount was below the minimum ticket floor; add Cash "
                        "Available, lower Max Selected, or lower the floor to "
                        "fund it.")
            elif "Duplicate issuer" in _reason:
                # v1.0.17: the v1.0.16 issuer-dedup adds a "Duplicate issuer"
                # deferral to the same dict — classify it distinctly so it is
                # not mislabeled as a diversification cap (same bug class as the
                # v1.0.15 floor fix). Reachable only when issuer dedup is ON.
                gate, cur, req = "Duplicate", _reason, "one listing per issuer"
                note = ("Qualified (INVEST) \u2014 a higher-ranked listing of "
                        "this issuer is already funded; this is a cross-listing "
                        "of the same company, not a separate position.")
            else:
                gate, cur, req = "Diversification", _reason, (
                    "within sector/market caps")
                note = (
                    "Qualified (INVEST) \u2014 deferred by diversification cap")
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

def _pregate_quality_order(rows, crit):
    """v1.6.0 [PREGATE-ORDER]: re-order the incoming pool ELIGIBLE-FIRST
    before the max_candidates clamp cuts it, using cheap row-local mirrors
    of the real gates (field-token-identical to normalize_candidate /
    evaluate_gates; see the header WHY-block). Returns (reordered_rows,
    stats). Pure and deterministic: ties break on symbol then arrival
    index; malformed rows never raise — they fail their mirrors and sink.
    Cost: a handful of dict lookups + float parses per row (~10k rows well
    under a second); no FX resolve, no ticket math, no venue calendar."""
    stats = {"pool": len(rows), "eligible": 0, "fail_price_or_ref": 0,
             "fail_fresh": 0, "fail_sanity": 0, "fail_forecast": 0,
             "fail_reliability": 0}
    max_age = crit.get("max_data_age_hours")
    vmax = (crit.get("max_valuation_roi_pct", 80.0)
            if crit.get("valuation_sanity_gate_enabled") else None)
    f_floor = (crit.get("min_engine_roi_pct", 0.0)
               if crit.get("forecast_gate_enabled") else None)
    rel_floor = crit.get("min_reliability")
    if _env_rel_floor_mode() == "display":   # v1.19.0: never pre-filters either
        rel_floor = None
    keyed = []
    for i, raw in enumerate(rows):
        view = _row_lookup(raw if isinstance(raw, dict) else {})
        symbol = _to_text(_field(view, "symbol")) or "?"
        price = _to_float(_field(view, "price"))
        if price is not None and price <= 0:
            price = None
        target = _to_float(_field(view, "target_price"))
        iv = _to_float(_field(view, "intrinsic_value"))
        if target is not None and target <= 0:
            target = None
        if iv is not None and iv <= 0:
            iv = None
        ref = target if target is not None else iv
        eng_pct = _engine_roi_to_pct(
            _to_float(_field(view, "engine_roi_12m_pct")))
        # mirror of the v1.3.0 REF-CONSERVATIVE bound so the sanity mirror
        # sees the same roi_pct the real Valuation Sanity gate will see.
        if (_ref_conservative_enabled() and price and ref
                and eng_pct is not None and eng_pct > 0.0):
            _ref_eng = price * (1.0 + eng_pct / 100.0)
            if _ref_eng < ref:
                ref = _ref_eng
        roi_pct = None
        if price and ref:
            roi_pct = (ref - price) / price * 100.0
        rel = _to_float(_field(view, "reliability"))
        age_h = _parse_age_hours(_to_text(_field(view, "last_updated")))
        ok_pr = price is not None and ref is not None
        ok_fresh = not (max_age is not None and max_age > 0
                        and age_h is not None and age_h > max_age)
        ok_sane = (vmax is None or roi_pct is None or roi_pct <= vmax)
        ok_fcst = (f_floor is None or eng_pct is None
                   or eng_pct >= f_floor)
        ok_rel = (rel_floor is None
                  or (rel is not None and rel >= float(rel_floor)))
        eligible = (ok_pr and ok_fresh and ok_sane and ok_fcst and ok_rel)
        if eligible:
            stats["eligible"] += 1
        else:
            if not ok_pr:
                stats["fail_price_or_ref"] += 1
            if not ok_fresh:
                stats["fail_fresh"] += 1
            if not ok_sane:
                stats["fail_sanity"] += 1
            if not ok_fcst:
                stats["fail_forecast"] += 1
            if not ok_rel:
                stats["fail_reliability"] += 1
        rel_k = rel if rel is not None else -1.0e18
        eng_k = eng_pct if eng_pct is not None else -1.0e18
        keyed.append(((0 if eligible else 1, -rel_k, -eng_k, symbol, i),
                      raw))
    keyed.sort(key=lambda kv: kv[0])
    return [raw for _k, raw in keyed], stats


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
    pregate_stats = None
    # v1.9.2 [SC-2]: remember the pool size BEFORE any clamp so coverage can
    # be stated honestly (kpis["scanned"] counts the post-clamp audit).
    _pool_received = len(rows)
    _scan_cap_overridden = False
    if _env_scan_uncapped() and crit["max_candidates"] > 0:
        # v1.9.2 [SC-1]: request/env supplied a positive cap; the operator's
        # guarantee switch restores full-pool coverage. criteria_snapshot in
        # meta reflects the EFFECTIVE value (0).
        try:
            _LOG.info(
                "[SCAN-UNCAPPED v%s] max_candidates=%d overridden to 0 "
                "(pool=%d) via TFB_OPP_SCAN_UNCAPPED",
                OPPORTUNITY_BUILDER_VERSION, crit["max_candidates"],
                _pool_received)
        except Exception:
            pass
        crit["max_candidates"] = 0
        _scan_cap_overridden = True
    _scan_clamped = (crit["max_candidates"] > 0
                     and _pool_received > crit["max_candidates"])
    if crit["max_candidates"] > 0:
        # v1.6.0 [PREGATE-ORDER]: the clamp is about to discard everything
        # past max_candidates, so make the kept slice the QUALITY slice,
        # not the arrival slice (2026-07-22 evidence: 10,311 -> first 300
        # -> 291 Valuation-Sanity kills -> 0 tickets). OFF-switch, no-clamp
        # and no-cut paths leave rows untouched => byte-identical v1.5.0.
        if _env_pregate_order() and len(rows) > crit["max_candidates"]:
            rows, pregate_stats = _pregate_quality_order(rows, crit)
            pregate_stats["kept"] = crit["max_candidates"]
            try:
                _LOG.info(
                    "[PREGATE v%s] pool=%d eligible=%d kept=%d fail("
                    "fresh=%d sanity=%d forecast=%d rel=%d price_ref=%d)",
                    OPPORTUNITY_BUILDER_VERSION, pregate_stats["pool"],
                    pregate_stats["eligible"], pregate_stats["kept"],
                    pregate_stats["fail_fresh"],
                    pregate_stats["fail_sanity"],
                    pregate_stats["fail_forecast"],
                    pregate_stats["fail_reliability"],
                    pregate_stats["fail_price_or_ref"])
            except Exception:
                pass
        rows = rows[:crit["max_candidates"]]
    pf = _normalize_portfolio(portfolio)
    sector_ctx = _sector_context(pf, crit, pf["cash"] + pf["proceeds"])
    held = {h["symbol"] for h in pf["holdings"]}
    # v1.0.21 (Fix #4): expand to normalized bare/.US variants so the
    # Portfolio gate honors "Include Portfolio Holdings = No" regardless of
    # which form a page carries (BBD vs BBD.US). Kill switch restores the
    # exact-match set above unchanged.
    if _env_held_variant_match():
        _hv = set()
        for _hs in held:
            _hv |= _symbol_variants(_hs)
        held = _hv

    # 1) normalize → gates → verdict → score (audit grid, 1:1 trace)
    audit = []
    gate_fail_counts = {}
    trust_stats = {"evaluated": 0, "blocked": 0,
                   "blocked_stale": 0, "blocked_thin": 0,
                   "lineage_low": 0, "lineage_contradiction": 0}
    for raw in rows:
        cand = normalize_candidate(raw, fx_rates, crit)
        gates = evaluate_gates(cand, crit, held)
        # v1.13.0 [TRUST-001] run telemetry — counts in tag AND gate mode;
        # zeros when off (keys always present, the v1.0.6 meta precedent).
        if cand.get("trust_low_source"):
            trust_stats["lineage_low"] += 1
            if (cand.get("dq") is not None
                    and cand["dq"] >= (crit.get("min_dq") or 0)):
                trust_stats["lineage_contradiction"] += 1
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
            "valuation_roi_pct": _round1(cand["roi_pct"]),
            "valuation_ann_roi_pct": _round1(cand["ann_roi_pct"]),
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
            "engine_roi_pct": _round1(_engine_roi_to_pct(cand["engine_roi_12m_pct"])),
            "selected": False,
            "deferral": None,
            "_cand": cand,
        })
        # v1.0.21 (Fix #3 completion): under the engine basis the audit
        # record's PRIMARY roi/ann speak the engine 12M forecast (per-row
        # fallback to valuation when absent) so the ALL QUALIFIED grid and
        # the SELECTED tickets read the same language. The cand dict, gates,
        # verdict and score above were computed BEFORE this swap and are
        # untouched. Default basis "valuation" -> this block is inert.
        if str(crit.get("primary_roi_basis") or "").strip().lower() == "engine":
            _rec = audit[-1]
            _rec_eng = _rec.get("engine_roi_pct")
            if _rec_eng is not None:
                _rec["roi_pct"] = _rec_eng
                _rec["ann_roi_pct"] = _rec_eng
                _rec["primary_roi_basis"] = "engine"
            else:
                _rec["primary_roi_basis"] = "valuation"
        # v1.14.0 [ROI-TRUTH-2]: plan-basis alignment (the live default) —
        # see _audit_align_plan_roi. Runs AFTER gates/verdict/score; display
        # truthfulness only, selection byte-identical.
        _audit_align_plan_roi(audit[-1], crit)

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
    # v1.18.0: under the funding layer, Selected = FUNDABLE_NOW only - a
    # 0-SAR pick is never an executable ticket, whatever unfunded_watch says.
    if crit.get("unfunded_watch_enabled") or _env_funding_plan():
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
    # --- v1.18.0 FUNDING-STATE LAYER (additive; kill-switch) -----------------
    _fp_plans = []
    if _env_funding_plan():
        try:
            _needs = dict(_LAST_FUNDING_NEEDS)
            _min_floor = float(crit.get("min_ticket_sar", 0.0) or 0.0)
            for _p in unfunded_picks:  # 0-SAR picks (capital exhausted)
                _s = _p["cand"]["symbol"]
                _nf = _min_floor
                if _env_venue_floors():
                    _vf = _venue_floor(_s)
                    if _vf and float(_vf) > _nf:
                        _nf = float(_vf)
                if _s not in _needs and _nf > 0:
                    _needs[_s] = {"need_sar": _nf, "sized_sar": 0.0,
                                  "remaining_sar": float(remaining)}
            _ordered = []
            for _a in invest:  # rank order
                _s = _a["symbol"]
                if _s in _needs:
                    _ordered.append((
                        _s, _needs[_s]["need_sar"],
                        _engine_roi_to_pct(_a["_cand"].get("engine_roi_12m_pct"))))
            if _ordered:
                _hold_roi = _holding_roi_map(audit, pf["holdings"])
                _fp_plans = _funding_plans(
                    _ordered, remaining, pf["holdings"], _hold_roi,
                    _env_rotation_exclude(), _env_rotation_edge_pp(),
                    _env_rotation_cost_pct())
                _by_sym_plan = {pl["symbol"]: pl for pl in _fp_plans}
                for _s, _pl in _by_sym_plan.items():
                    _txt = _funding_plan_text(_pl, remaining)
                    if not _txt:
                        continue
                    if _s in deferrals:
                        deferrals[_s] = deferrals[_s] + _txt
                        if _s in by_symbol:
                            by_symbol[_s]["deferral"] = deferrals[_s]
                    for _nm in unfunded_nm:
                        if _nm.get("symbol") == _s:
                            _nm["current"] = str(_nm.get("current") or "") + _txt
        except Exception:  # noqa: BLE001 - the layer is additive, never fatal
            _fp_plans = []
    total_suggested = sum(t["suggested_sar"] for t in tickets)
    kpis = {
        "deployable_sar": round(deployable, 0),
        # v1.15.0 additive: label-able views — settled cash only vs doctrine.
        "deployable_current_sar": _LAST_DEPLOYABLE_SPLIT["current"],
        "deployable_proforma_sar": _LAST_DEPLOYABLE_SPLIT["proforma"],
        "expected_gain_12m_sar": round(
            sum(t["exp_gain_12m_sar"] for t in tickets), 0),
        "selected_count": len(tickets),
        "max_selected": crit["max_selected"],
        "blended_reliability": _blend(tickets, "reliability"),
        # v1.8.0 [PY-1]: blend the TP2 R/R the tickets actually display.
        # TFB_OPP_BLENDED_RR_BASIS=plan restores the v1.7.0 "rr" blend.
        "blended_rr": _blend_detail(
            tickets,
            "rr" if _env_blended_rr_basis() == "plan" else "rr_tp2"),
        "scanned": len(audit),
        "passed": len(invest),
        "capital_unallocated_sar": round(deployable - total_suggested, 0),
        # v1.9.2 [SC-2] additive coverage keys (absent-key-safe consumers).
        "pool_received": _pool_received,
        "scan_coverage_pct": (round(100.0 * len(audit) / _pool_received, 1)
                              if _pool_received else 100.0),
    }
    if _env_funding_plan():  # v1.18.0 additive keys
        _topn = _env_capital_call_topn()
        kpis["fundable_now"] = len(tickets)
        kpis["fundable_by_rotation"] = sum(
            1 for pl in _fp_plans if pl["state"] == "FUNDABLE_BY_ROTATION")
        kpis["capital_call"] = sum(
            1 for pl in _fp_plans if pl["state"] == "CAPITAL_CALL")
        kpis["capital_call_topn_sar"] = round(sum(
            (pl.get("shortfall_sar") or 0.0) for pl in _fp_plans[:_topn]), 0)
    if _scan_clamped:
        kpis["scan_clamped"] = True
        kpis["scan_clamp"] = int(crit["max_candidates"])
        kpis["pregate_ordered"] = pregate_stats is not None
    if _scan_cap_overridden:
        kpis["scan_cap_overridden"] = True
    # v1.6.0 [PREGATE-ORDER]: full-pool funnel telemetry. Additive key,
    # absent whenever the reorder did not run, so every v1.5.0 consumer
    # sees an unchanged kpis dict on the OFF/no-cut paths.
    if pregate_stats is not None:
        kpis["pregate"] = pregate_stats
    # v1.0.5: parallel engine-based expected gain (additive; the primary
    # expected_gain_12m_sar above stays == Σ ticket exp_gain — v1.0.20 note:
    # under TFB_OPP_PRIMARY_ROI_BASIS=engine the ADDENDS are engine-based, so
    # the identity is preserved while the KPI stops being a 35%-cap artifact).
    if crit.get("engine_roi_display_enabled"):
        _eng_gains = [t.get("engine_exp_gain_12m_sar") for t in tickets
                      if t.get("engine_exp_gain_12m_sar") is not None]
        kpis["engine_expected_gain_12m_sar"] = (
            round(sum(_eng_gains), 0) if _eng_gains else None)
    # v1.0.20: in engine mode also surface the valuation-based total under an
    # explicit name so the target-vs-forecast spread is visible at KPI level.
    if str(crit.get("primary_roi_basis") or "").strip().lower() == "engine":
        _val_gains = [t.get("valuation_exp_gain_12m_sar") for t in tickets
                      if t.get("valuation_exp_gain_12m_sar") is not None]
        kpis["valuation_expected_gain_12m_sar"] = (
            round(sum(_val_gains), 0) if _val_gains else None)

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

    if _env_funding_plan() and _fp_plans:  # v1.18.0 additive alerts
        _rot = [pl for pl in _fp_plans if pl["state"] == "FUNDABLE_BY_ROTATION"]
        if _rot:
            _r = _rot[0]["rotation"]
            alerts.append({
                "type": "rotation_proposal", "count": len(_rot),
                "required_action": (
                    _rot[0]["symbol"] + " is fundable by " + _r["action"].lower() +
                    " of " + _r["symbol"] + " " + _fmt_sar(_r["proceeds_sar"]) +
                    " (engine 12M edge +" + _fmt_num(_r["edge_pp"]) +
                    "pp after cost). A rotation is a ticket, not an order: "
                    "written GO with a live quote required."),
            })
        _topn = _env_capital_call_topn()
        _cc = [pl for pl in _fp_plans[:_topn] if (pl.get("shortfall_sar") or 0) > 0]
        if _cc:
            alerts.append({
                "type": "capital_call", "count": len(_cc),
                "required_action": (
                    "Deposit \u2265 " + _fmt_sar(sum(pl["shortfall_sar"] for pl in _cc)) +
                    " to take the top-" + str(len(_cc)) + " qualified ticket(s): " +
                    ", ".join(pl["symbol"] for pl in _cc) +
                    ". Qualified names are shown regardless of cash; funding is "
                    "the operator's decision."),
            })
    # 4) audit grid sorted by score; strip internals
    # v1.19.1 [AUDIT-DEPTH-ORDER]: depth order by default (display-only).
    if _env_audit_order() == "depth":
        audit.sort(key=_audit_depth_key)
    else:
        audit.sort(key=lambda a: (-(a["opportunity_score"] or 0.0), a["symbol"]))
    for a in audit:
        a.pop("_cand", None)

    # v1.0.10 [AUDIT-CAP]: optional ceiling on the WRITTEN candidates_rows
    # audit grid so a full-universe scan stays inside the GAS/Sheets write
    # limit. `selected` (tickets), `near_miss`, `alerts` and kpis["scanned"]
    # above were all computed from the FULL pool, so capping here shrinks ONLY
    # the written audit. Every selected, INVEST-qualified and near-miss row is
    # retained; remaining slots are filled by the next highest-scoring rows, so
    # the only rows dropped are the low-score DO_NOT_INVEST / WATCH tail.
    # 0 => unlimited (byte-identical v1.0.9 — full audit returned).
    _audit_cap = crit.get("audit_rows_max") or 0
    if _audit_cap > 0 and len(audit) > _audit_cap:
        _nm_syms = {r.get("symbol") for r in near_miss if isinstance(r, dict)}
        _keep_syms = set(selected_syms) | set(unfunded_syms) | _nm_syms
        _must, _rest = [], []
        for a in audit:
            if a.get("symbol") in _keep_syms or a.get("verdict") == VERDICT_INVEST:
                _must.append(a)
            else:
                _rest.append(a)
        _room = _audit_cap - len(_must)
        # v1.19.1 [AUDIT-DEPTH-ORDER]: the free slots go to the rows that got
        # furthest through the gate chain, so a Risk/Reward failure is written
        # before a Valuation Sanity failure. Kill-switch: score order.
        if _env_audit_order() == "depth":
            _rest.sort(key=_audit_depth_key)
        audit = _must + (_rest[:_room] if _room > 0 else [])
        if _env_audit_order() == "depth":
            audit.sort(key=_audit_depth_key)
        else:
            audit.sort(key=lambda a: (-(a["opportunity_score"] or 0.0), a["symbol"]))

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
        "trust_lineage": {
            "mode": _env_trust_lineage_mode() or "off",
            "low_trust_rows": trust_stats["lineage_low"],
            "contradictions": trust_stats["lineage_contradiction"],
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
        "kpis": {"deployable_sar": 0, "deployable_current_sar": 0,
                 "deployable_proforma_sar": 0, "expected_gain_12m_sar": 0,
                 "selected_count": 0,
                 "max_selected": criteria.get("max_selected", 10),
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
