#!/usr/bin/env python3
"""
scripts/track_performance.py
===========================================================
TADAWUL FAST BRIDGE – ADVANCED PERFORMANCE ANALYTICS ENGINE (v6.18.0)
===========================================================

Why this revision (v6.18.0 vs v6.17.0) — COHORT RESCUE
(PF-GEOM + PRICE-FEED-LOUD + CA-GUARD + BT-ADJ)
  EVIDENCE (live export 2026-07-12 21:15, 2,264 records): 1,850 records
  entered >= 2 days earlier showed Current Price == Entry Price on 100.0%
  of rows and Unrealized ROI == 0.00 everywhere, while Last Updated
  stamped the same run on all of them — the job runs, touches every
  record, and never lands a single fresh price. First Target Date =
  2026-07-28 (1M x 1,128 + 3M x 1,128): without a working feed the
  v6.17.0 fresh-only gate — correctly — expires the ENTIRE first
  calibration cohort UNPRICED starting ~Aug 2. Four fixes:

  v6.18.0 A [PF-GEOM]  update_summary wrote its 13-row KPI block to
       A1:E13 — straight across the record grid: START_ROW=5 means the
       HEADER row's first five cells (Record ID, Key, Symbol, Horizon,
       Date Recorded) were overwritten every run ('Wins' sat where
       'Record ID' belongs) and data rows 6-13 lost their identity cells
       (the 8 earliest records are orphaned on-sheet; the loader —
       positional by canonical HEADERS — already drops them via the
       blank-symbol filter). FIX: the summary block moves BESIDE the grid
       (AG1:AK13); _ensure_headers' brand-new-sheet seed moves with it,
       and its existing header-mismatch repair restores the clobbered
       A5:AE5 header row automatically on the next run. Kill-switch
       TFB_TRACK_SUMMARY_SIDE=0 restores A1:E13 (not recommended). The 8
       orphaned rows self-heal on the next full grid save.
  v6.18.0 B [PRICE-FEED-LOUD]  the price map was built as
       `fetch_prices(syms) if self.backend.base_url else {}` — an EMPTY
       base_url (workflow ENV missing) or an all-zero response produced a
       silent {} and the run looked green. FIX: a dead/degraded feed now
       logs an ERROR with the [v6.18.0 PRICE-FEED] tag naming the cause
       (no-base-url vs 0/N priced vs n/N priced), so a cohort-threatening
       outage is one grep away, never silent.
  v6.18.0 C [CA-GUARD]  maturation computed realized ROI as
       current_print / entry_print - 1. A corporate action between entry
       and maturity puts the two prints in different share-count units: a
       Tadawul 1:1 bonus issue (routine) matures a fake -50% LOSS; a
       1-for-10 US reverse split a fake +900% WIN — straight into the
       Calibrator's tails. FIX: when |print ROI| >= TFB_TRACK_CA_VERIFY_PCT
       (default 15) the maturation is verified against the ADJUSTED
       history: agree -> mature as-is; disagree by > 12pp -> mature on the
       ADJUSTED ROI with a [v6.18.0 CA-ADJUST] note; history unavailable
       -> EXPIRED outcome CORP_ACTION_SUSPECT (structurally excluded, like
       UNPRICED — never a fake WIN/LOSS). TFB_TRACK_CA_GUARD=0 restores
       the v6.17.0 maturation verbatim.
  v6.18.0 D [BT-ADJ]  _yf_deep_history pinned auto_adjust=False, feeding
       UNADJUSTED closes to every hypothesis backtest while EODHD bars
       arrive adjusted — the same split-brain fixed provider-side in
       yahoo_chart v8.8.0. FIX: auto_adjust=True (TFB_BT_ADJUSTED=0
       restores raw), and each bar now also carries "adjclose" so
       _bt_bar_close's adjusted-first preference actually engages.

Why the previous revision (v6.17.0 vs v6.16.0) — MATURITY PRICE-INTEGRITY
(fresh-price-or-hold; evidence: 2026-07-10 workbook audits, first 1M
cohort maturing ~2026-07-18)
  ROOT CAUSE. audit_active_records matured every past-due record on
  WHATEVER unrealized_roi it carried: (a) a symbol whose price fetch
  failed on maturity day matured on a STALE roi from the last successful
  audit; (b) a symbol whose price was NEVER fetchable (the July symbol-
  amputation class — 2222.SR/1010.SR-era records; delisted or renamed
  tickers) carried the dataclass default unrealized_roi = 0.0 and matured
  as a FAKE BREAKEVEN. Both silently poison the exact statistics the
  2026-07-18 cohort exists to produce (hit rates, Brier, calibration).
  v6.17.0 A  A record may transition ACTIVE -> MATURED only on a FRESH
       positive price fetched THIS run (and entry_price > 0). Past-due
       records without a fresh price stay ACTIVE and are retried on
       every subsequent run — logged as 'maturity pending price'.
  v6.17.0 B  After TFB_TRACK_MATURE_GRACE_DAYS (default 5, floor 0) days
       past target_date with still no fresh price, the record becomes
       PerformanceStatus.EXPIRED with realized_roi = None and outcome
       'UNPRICED' (+ a tagged note). EXPIRED is an EXISTING enum value:
       the sheet round-trips it, and every statistics consumer already
       filters (status == MATURED and realized_roi is not None), so
       unpriceable records are structurally invisible to the summary,
       hit rates, calibrator, and Brier — no consumer changes needed.
  v6.17.0 C  Kill-switch TFB_TRACK_MATURE_FRESH_ONLY=0/false/off/no runs
       the v6.16.0 maturation block VERBATIM (stale/fake-breakeven
       behavior restored whole). Default ON.
  v6.17.0 D  Per-run integrity log line: matured-fresh / pending-price /
       expired-unpriced counts with symbol samples, tag
       [v6.17.0 MATURE-FRESH-ONLY]. New helpers:
       _mature_fresh_only_enabled, _mature_grace_days (2 added, 0
       removed, 0 signature changes; every other line verbatim v6.16.0).

Why this revision (v6.16.0 vs v6.15.0) — DECISION-SYMBOL FORCED COVERAGE
(Engineering Audit Fix #5; env-gated kill-switch
TFB_TRACK_FORCE_DECISION_SYMBOLS, DEFAULT ON)

The 2026-07-05 audit: Signal_History held ~50 snapshots/day but FIVE of the
ten portfolio holdings (1835.SR, 3092.SR, 4150.SR, 5023.SR, 7030.SR) had
ZERO snapshots ever, and fresh Top_10 picks (4503.T, ADAM.US, NMM.US,
INSW.US) had none — precisely the symbols where day-to-day action-trend
history matters most (three of the zero-history names carry live EXIT
recommendations). ROOT CAUSE: run_once feeds BOTH pipelines (Performance_Log
records + Signal_History snapshots) exclusively from get_top10_rows(), so
any decision symbol outside the Top_10 candidate pool that day is
structurally invisible — holdings on SELL/HOLD never qualify, so they are
never recorded, and every day of the gap is trend data lost forever
(strategy requirement #2 mandates daily recording for Top_10 + holdings).

FIX (additive; both pipelines covered by one augmentation):
  v6.16.0 A  _load_costbasis_symbols(spreadsheet_id): reads the Symbol
       column of _Portfolio_CostBasis (tab name via TFB_TRACK_COSTBASIS_TAB)
       on the ALREADY-CONFIGURED spreadsheet using the same SA-credential
       pattern as the three existing stores. Fail-open: any problem -> []
       with one debug line (a Sheets hiccup must never break --record).
  v6.16.0 B  BackendClient.get_rows_for_symbols(symbols, page): POSTs
       {"symbols": chunk, "page": page} in chunks of 25 to
       TFB_TRACK_FORCE_ROWS_ENDPOINT (default /v1/analysis/sheet-rows —
       the same engine boundary the dashboard sync uses, so forced rows
       carry the full 115-field verdict shape) and aggregates via the
       existing envelope extractor.
  v6.16.0 C  _augment_with_decision_symbols(prefetched): priority set =
       cost-basis holdings UNION TFB_TRACK_PRIORITY_SYMBOLS (csv, optional
       extras, e.g. pinned picks); MISSING = priority − symbols already in
       the prefetched Top_10 rows, capped at TFB_TRACK_FORCE_MAX (default
       40, edge-timeout protection); fetched rows are deduped, tagged
       origin="Decision_Coverage" (visible in Origin Tab), and appended.
       RESILIENCE: on a total Top_10 fetch failure the augmentation still
       returns the forced holdings rows, so snapshot day-continuity for
       held names survives a Top_10 outage (previously the whole snapshot
       day was silently lost).
  v6.16.0 D  run_once wiring: one call between the Top_10 prefetch and the
       two consumers, try/except fail-open. KILL SWITCH: with
       TFB_TRACK_FORCE_DECISION_SYMBOLS=0 the call is skipped entirely and
       v6.15.0 behavior (including None-on-failure semantics) is
       byte-identical.
  v6.16.0 E  SCRIPT_VERSION 6.15.0 -> 6.16.0 (SERVICE_VERSION follows).
       No function removed; all prior WHY-blocks carried verbatim.
       New ENV: TFB_TRACK_FORCE_DECISION_SYMBOLS ("1"),
       TFB_TRACK_PRIORITY_SYMBOLS (csv, ""), TFB_TRACK_FORCE_MAX ("40"),
       TFB_TRACK_FORCE_ROWS_ENDPOINT ("/v1/analysis/sheet-rows"),
       TFB_TRACK_FORCE_ROWS_PAGE ("Market_Leaders"),
       TFB_TRACK_COSTBASIS_TAB ("_Portfolio_CostBasis").

Why this revision (v6.15.0 vs v6.14.0) — F1 WIRING: CONSUME Calendar_Events
(owner greenlight 2026-07-05, Option B: sheet-as-bus, off the request path)

v6.14.0 added the Days-To-Earnings/ExDiv columns but the Top10 endpoint rows
carry no next_earnings_date / next_ex_div_date keys yet (adding them backend-
side would put live calendar calls on the ~100s request path). Option B keeps
the backend untouched: a daily Actions job (calendar_sync.yml ->
scripts/run_calendar_sync.py -> core.providers.calendar_provider) publishes a
small Calendar_Events tab, and THIS script — the consumer — reads it:

  v6.15.0 A  _load_calendar_context(): best-effort read of the
       Calendar_Events tab (name via TFB_CALENDAR_SHEET, default
       "Calendar_Events") piggybacking the ALREADY-OPEN Signal_History
       spreadsheet handle — zero new auth, zero new stores. Header-keyed
       (Symbol / Next Earnings Date / Next Ex-Div Date); returns {} on any
       problem (tab absent, gspread down, gate off) with one debug line.

  v6.15.0 B  _merge_calendar_context(rows, ctx): pure helper that
       setdefault()s the two keys onto each Top10 row — a backend-supplied
       value ALWAYS wins if it ever appears, so future backend wiring can
       supersede the sheet with no change here. Called once inside
       record_signal_snapshots, before snapshots are built; the v6.14.0
       _days_until math then fills the day columns exactly as tested.

  v6.15.0 C  Gating unchanged: the whole path sits behind the existing
       TRACK_EVENT_CONTEXT (default ON; =0 restores v6.14.0-blank behaviour
       byte-for-byte) — no new flag. New ENV: TFB_CALENDAR_SHEET (name only).

  v6.15.0 D  SCRIPT_VERSION 6.14.0 -> 6.15.0 (SERVICE_VERSION follows).
       No function removed; all prior WHY-blocks carried verbatim.

Why this revision (v6.14.0 vs v6.13.0) — F4: EVENT-CONTEXT CAPTURE
(owner greenlight 2026-07-05; Forward-Looking Layer plan, Phase F4)

The 1M Performance_Log cohort matures ~2026-07-18 and the Calibrator will
then start conditioning claimed-vs-realized reliability on context. Context
that is not captured alongside each daily verdict can never be back-filled
— every uncaptured day is conditioning history lost. v6.14.0 therefore adds
four columns to Signal_History (and ONLY Signal_History; Performance_Log is
deliberately untouched — a record's context is recoverable by joining
symbol|date against the daily snapshots, so no record-schema churn):

  v6.14.0 A  SignalSnapshot gains analyst_rating / target_price (captured
       from the already-fetched Top10 row dicts when the backend supplies
       them; header-keyed best-effort, blank when absent) and
       days_to_earnings / days_to_exdiv (computed from next_earnings_date /
       next_ex_div_date row keys IF present — these keys arrive with the
       F1 calendar layer; until then the columns are schema-ready blanks,
       never fabricated). analyst_rating/target_price are the raw series
       from which Phase F2 revision deltas (H-REV-01) will be computed.

  v6.14.0 B  SignalHistoryStore.HEADERS appends "Analyst Rating",
       "Target Price", "Days To Earnings", "Days To ExDiv". The existing
       _ensure_headers self-heal rewrites row 1 on first run (same
       discipline as every prior widening); old data rows load fine via
       the existing right-padding in load_snapshots. Header shape is NOT
       env-gated (a per-env header would flip-flop the sheet row 1 between
       runs); only VALUE population is gated.

  v6.14.0 C  Gate: TRACK_EVENT_CONTEXT (default ON; kill-switch =0 writes
       blanks in the four columns while leaving every prior column and all
       other behaviour byte-identical). Default-ON matches the tracker's
       TRACK_SIGNAL_HISTORY / TRACK_CALIBRATION precedent and the explicit
       owner request that capture start before the Jul-18 maturity.

  v6.14.0 D  New pure helper _days_until(raw) -> Optional[int]: Riyadh-date
       day difference, None on blank/unparseable — no exceptions escape.

  v6.14.0 E  SCRIPT_VERSION 6.13.0 -> 6.14.0 (SERVICE_VERSION follows).
       No function removed; all prior WHY-blocks carried verbatim.

Why this revision (v6.13.0 vs v6.12.0)
--------------------------------------
v6.19.0 -- DAY-KEYED TREND WINDOW + INTRADAY-AWARE FLIP COUNT (Fix TR-1)
--------------------------------------------------------------------------
EVIDENCE (Signal_Trends row for NMM.US, 2026-07-13 19:36): Flip Count 0.0,
Stability DRIFTING, Direction STRENGTHENING -- while Signal_History showed
the SAME symbol whipsawing BUY -> REDUCE -> HOLD -> SELL -> REDUCE -> HOLD
across 48h (Rel 76.5<->31.3 flap, price flat). ROOT CAUSE: Signal_History
carries MULTIPLE same-day rows per symbol|YYYYMMDD key (the documented
first-of-day idempotency is not enforced on the live sheet), so
SignalTrendAnalyzer.analyze's window of 5 SNAPSHOTS spanned ~18 HOURS of
same-day REDUCE rows -- the BUY->REDUCE side flip fell OUTSIDE the window
and flip_count computed 0 on a textbook whipsaw. days_in_current_action
counted snapshots, not days, for the same reason.

FIX TR-1 (env TFB_TREND_DAY_KEYED, default ON; =0/false restores the
v6.18.0 analyzer byte-identically):
  * The per-symbol series is collapsed to ONE representative per Riyadh
    DAY (the day's LAST snapshot = the standing end-of-day verdict);
    `window` now means DAYS, matching the column header's intent and the
    SignalSnapshot docstring.
  * days_in_current_action / distinct_actions / score slope compute over
    the day representatives (slope header becomes "Score Slope (pts/day)";
    the writer self-heals the header row on the next write).
  * flip_count computes over ALL snapshots inside the window's day span
    (chronological, side-transition formula unchanged) -- so an INTRADAY
    bull->bear crossing is counted, not hidden by the day collapse. The
    live NMM.US series now yields flip_count>=1, Stability CHOPPY,
    Direction FLIPPED.
Zero functions removed; addition: _trend_day_keyed_enabled. The analyze()
body gains the day-keyed branch; the legacy branch is preserved verbatim
under the kill switch.


SURFACE THE ACTION-TREND TRAJECTORY (new Signal_Trends tab; env-gated
TFB_SIGNAL_TRENDS_TAB, default OFF -> the --analyze path is byte-identical
to v6.12.0). Since v6.7.0 this script has COMPUTED the full day-to-day
trajectory for every decision symbol -- SignalTrend.action_direction
(STRENGTHENING/WEAKENING/STABLE/FLIPPED/NEW), days_in_current_action,
score_slope, flip_count, stability -- but only ever wrote it to an
EPHEMERAL local <base>_signal_trends.json that the GitHub Actions runner
discards at job end, and ONLY under --export, which the daily cron does
NOT pass (it runs --record --audit --analyze). So the trajectory -- the
entire value of the action-trend layer: confirmation that a multi-day
STABLE verdict is more trustworthy than a one-day flip, and early warning
that a verdict decaying day over day flags thesis erosion BEFORE any
1M/3M maturity check fires -- never reached the user. v6.13.0 persists
that SAME, already-computed trajectory to a user-visible Signal_Trends
tab via a new SignalTrendStore that mirrors SignalHistoryStore's
auth/header discipline. PURELY ADDITIVE: no new computation, and NO
recommendation, score, gate, reliability, snapshot, or schema change.
Unlike the append-only Signal_History snapshots, trends are a
CURRENT-STATE view (one row per decision symbol, recomputed each cycle),
so write_trends is a bounded REPLACE-ALL (overwrite the data region,
clear any trailing stale rows) and the tab always shows each symbol's
latest trajectory. Fail-open at every Sheets boundary -- it must NEVER
break --record / --audit / --analyze. Reverts by leaving
TFB_SIGNAL_TRENDS_TAB unset (the store is never constructed and
write_trends never runs).

Why this revision (v6.12.0 vs v6.11.0)
--------------------------------------
INFORMATION COEFFICIENT (disclosure-only, env-gated default OFF). The reliability
calibrator already reports a Brier score, per-band realized win-rates, a binary
band-monotonicity flag, and an INVESTABLE-vs-WATCHLIST CI discrimination test --
but nothing that measures, continuously, whether the engine's own `entry_score`
rank-orders realized returns. v6.12.0 adds exactly that: a Spearman rank
correlation of entry_score vs realized_roi over DECIDED records, with a t-stat
and t-distribution p-value (reused from core.stats.information_coefficient, which
ships in the same commit). It is strictly more data-efficient than hit-rate -- it
uses every decided pick, not only STRONG-labelled ones -- which directly helps the
current small-sample regime. ENTIRELY ADDITIVE and disclosure-only: it feeds NO
recommendation, score, gate, or reliability number, mirroring the existing
"measure, never auto-apply" calibration stance. Gated behind TFB_HARNESS_IC
(default OFF); computed only when core.stats is importable and there are >= 3
decided outcomes, otherwise the report's IC fields stay None and every other
metric is byte-identical. Reverts by leaving TFB_HARNESS_IC unset.

Why this revision (v6.9.0 vs v6.8.0)
-------------------------------------
TOP10 FETCH REPOINT — the calibration clock was recording ZERO picks despite
the sheet machinery working. Root cause (diagnosed live, not guessed): --record
fetched from /v1/advanced/top10-investments, an `investment_advisor` endpoint
that bridges to advanced_analysis under a 20s server-side cap. A cold full
Top_10 build takes ~50-90s, so that bridge TIMES OUT every call and returns
HTTP 200 with status="partial" and zero rows — which the tracker faithfully
recorded as nothing. The advanced_analysis /sheet-rows route runs the SAME
build with no cap and returns the full populated page (confirmed live: 200,
status="success", 50 rows). Fixes here, all in the BackendClient fetch layer
(no change to recording, scoring, snapshots, or schema):
  - /sheet-rows is now the PREFERRED Top10 endpoint; the advisor endpoints are
    kept as fallbacks. Per-endpoint request body (/sheet-rows wants
    {"page": <page>}; advisor wants criteria).
  - EMPTY-200 FALL-THROUGH: a 200 with zero rows (a timed-out partial) no longer
    counts as success — it falls through to the next endpoint. Closes the
  v6.10.1 D  Backtest history now routes yahoo_chart FIRST then EODHD (was
             EODHD-only, which 404'd every .SR ticker and dropped the whole KSA
             book). Bars date-sorted ascending for provider-safe no-lookahead.
    v6.4.0 gap where the first non-error response, even empty, stopped the chain.
  - Client fetch timeout raised 45s -> 100s (the full build exceeds 45s).
  - Fully reversible: TRACK_TOP10_ENDPOINTS / TRACK_TOP10_PAGE /
    TRACK_TOP10_TIMEOUT_SEC. Set TRACK_TOP10_ENDPOINTS to the old advisor-first
    chain to restore v6.8.0 behavior exactly.
GSPREAD 6.x DEPRECATION FIX — all five worksheet.update(range, values) calls
switched to the named-arg form update(values=, range_name=). gspread 6.2.1 is
live and warns on the old positional order; a future 7.x makes it a hard error
that would silently kill recording again. Behavior-identical on 6.x.

Why this revision (v6.8.0 vs v6.7.0)
-------------------------------------
RELIABILITY CALIBRATION — the "Measure" step of the project's Calibration &
Self-Correction Loop, folded IN HERE rather than shipped as a separate
sidecar. This file already owns and loads Performance_Log (matured, gate-
tagged records) and Signal_History (daily verdicts); calibration is just
"analyse those same matured records against their realized outcomes," so it
belongs alongside PerformanceAnalyzer and reuses the rows already in memory —
no second tool, no duplicate sheet I/O.

The engine (data_engine_v2) STATES a forecast_reliability_score /
investability verdict and its own WHY-blocks admit, verbatim, that this is
"an unbacktested ESTIMATE — only a forward-return backtest can [ground it]."
Nothing ever read outcomes back. v6.8.0 closes that loop, honestly:

  v6.8.0 A  wilson_interval / shrink_factor — pure calibration math. Wilson
       score interval (not normal-approx) because it stays in [0,100] and is
       honest at the small n this project lives at; a sample-shrunk
       calibration factor (1 + (raw-1)*n/(n+K)) so a 3-record bucket cannot
       assert a large correction.

  v6.8.0 B  CalibrationBucket / CalibrationReport models + ReliabilityCalibrator:
       realized win-rate vs stated confidence, broken out by investability
       bucket, stated-reliability band, recommendation tier, and horizon —
       each with a Wilson 95% interval and a minimum-sample gate (below
       min-sample decided outcomes the point estimate is SUPPRESSED as
       misleading and labelled). Plus a Brier score (with an explicit "if
       reliability is read as P(win)" caveat), a monotonicity check (does
       higher stated reliability -> higher realized win-rate?), and an
       INVESTABLE-vs-WATCHLIST discrimination check (do their intervals
       actually separate, or just overlap?).

  v6.8.0 C  Wiring: --analyze (gated TRACK_CALIBRATION, default ON) and the
       new --calibrate flag print a CALIBRATION block built from the records
       already loaded this cycle (zero extra I/O); the current verdict-
       stability mix is derived from the signal-trends already computed.
       --export writes <base>_calibration.json. Knobs: CAL_MIN_SAMPLE
       (default 10), CAL_SHRINKAGE_K (default 20).

  v6.8.0 D  SCRIPT_VERSION 6.7.0 -> 6.8.0; __all__ exports the new public
       symbols.

The honest line (why this survives an adversarial audit):
  * YES — calibrate and DISCLOSE stated confidence vs realized outcomes.
  * NO  — auto-tune the pick/score weights to past outcomes. On a handful of
    matured picks that is fitting noise; it manufactures false confidence.
    The advisory calibration factor is REPORTED, clamped, sample-shrunk, and
    NEVER applied to any recommendation/score/gate/reliability. Applying a
    haircut is a separate, default-OFF, engine-side phase — not this file.

Honest default state: until Performance_Log accumulates >= min-sample MATURED
records, the report says "INSUFFICIENT EVIDENCE — start the clock." Correct
behavior, not a bug — the engine's confidence stays an unbacktested estimate
exactly until enough outcomes exist to measure it.

[PRESERVED — strictly] All v6.7.0 day-to-day action-trend, all v6.6.0
investability-gate capture/segmentation, all v6.5.0 8-tier vocabulary, all
v6.4.0 hardening. v6.8.0 is purely additive: no removals, no renames, no
breaking signatures. Records/snapshots from prior versions load identically.

Why this revision (v6.7.0 vs v6.6.0)
-------------------------------------
DAY-TO-DAY ACTION-TREND TRACKING (Layer 2). v6.6.0 measures whether a
pick PANNED OUT at a horizon (1M/3M); it has no memory of how the
engine's verdict for a symbol MOVED day to day. So it can say "INVESTABLE
hit 64%" but never "4030.SR has been BUY and strengthening for 6 straight
days" vs "flipped to BUY only yesterday" -- the confirmation / early-
warning signal that separates a confirmed trend from one-day noise.
v6.7.0 adds that, purely additively, on the existing daily-snapshot
backbone:

  v6.7.0 A  New SignalSnapshot model + Signal_History tab: one verdict
       snapshot per decision symbol PER DAY (recommendation / final action /
       investability / overall score / forecast reliability / data quality /
       risk / price). Keyed symbol|YYYYMMDD (Riyadh) -> idempotent on a
       same-day re-run (first verdict of the day wins).

  v6.7.0 B  New SignalHistoryStore (gspread, row-1 header / row-2 data,
       same hardened-header + best-effort auth pattern as PerformanceStore;
       self-contained, adds NO change to PerformanceStore). append_snapshots
       writes only unseen symbol-day keys.

  v6.7.0 C  New SignalTrendAnalyzer + SignalTrend: per-symbol trajectory
       over a rolling window (default 5) -- days_in_current_action, action
       direction (STRENGTHENING / WEAKENING / STABLE / FLIPPED / NEW via the
       canonical 8-tier rank with HOLD as the side-flip midline), score
       slope (pts/snapshot), distinct actions, flip count, stability
       (STABLE / DRIFTING / CHOPPY). DESCRIPTIVE ONLY -- it characterises the
       engine's own verdict movement and predicts nothing.

  v6.7.0 D  Wiring: --record fetches the Top10 universe ONCE and feeds both
       the pick recorder AND the daily snapshot logger (no second backend
       call, no added edge pressure -- record_from_top10 gained an optional
       pre-fetched-rows arg; default None preserves self-fetch). --analyze
       prints an "ACTION-TREND" block; --export writes
       <base>_signal_trends.json. All gated by TRACK_SIGNAL_HISTORY
       (default ON; kill-switch =0), sheet TRACK_SIGNAL_SHEET (default
       Signal_History), window TRACK_TREND_WINDOW (default 5).

  v6.7.0 E  SCRIPT_VERSION 6.6.0 -> 6.7.0 (SERVICE_VERSION follows);
       __all__ exports the four new public symbols.

NOT in scope (deliberately): this measures and describes the verdict
trajectory; it does NOT change any recommendation, forecast, gate, or the
engine, and it sharpens no 12-month prediction. Trend direction needs >=2
days of accumulated Signal_History to mean anything (day 1 shows NEW) --
the value is starting the daily clock. Surfacing trends ON Top_10 is a
GAS-side follow-on; this delivery records + analyzes them. Rendering the
day-to-day candle STRUCTURE (Layer 1) is separate engine work, not here.

[PRESERVED — strictly] All v6.6.0 investability-gate capture/segmentation,
all v6.5.0 8-tier vocabulary, all v6.4.0 endpoint-canonicalization +
hardening. v6.7.0 is purely additive: no removals, no renames, no breaking
signature changes (record_from_top10's new arg is optional). Records and
snapshots written by prior versions load byte-identically.

Why this revision (v6.6.0 vs v6.5.0)
-------------------------------------
INVESTABILITY-GATE CAPTURE + SEGMENTATION. v6.5.0 snapshots the legacy
signal at entry (recommendation / score / risk / confidence) but NOT the
investability-gate verdict that data_engine_v2 v5.78+ now emits. So the
tracker could report "BUY hit 58%" but never "INVESTABLE hit 64% vs
WATCHLIST 41%" -- which is the ONLY question that validates the gate, and
the only thing that turns the dashboard's estimated "reliability %" into a
measured one. v6.6.0 closes that gap, purely additively:

  v6.6.0 A  PerformanceRecord gains four ENTRY-snapshot fields, appended
       at the END of the field list (defaults => any v6.5.0-serialized
       row still deserializes; old rows carry blanks):
         entry_data_quality            (data_quality_score at entry)
         entry_forecast_reliability    (forecast_reliability_score)
         entry_investability_status    (INVESTABLE / WATCHLIST / BLOCKED)
         entry_final_action            (INVEST / WATCH / DO_NOT_INVEST)
       to_dict / from_sheet_row carry them through JSON, CSV, and Sheets.

  v6.6.0 B  record_from_top10 captures the four gate fields off each
       Top10 row (engine emits them as canonical snake_case keys). Absent
       on a pre-gate deploy -> safe zeros/blanks, no behavior change.

  v6.6.0 C  PerformanceStore.HEADERS extended 27 -> 31, the four new
       columns APPENDED at the end ("Entry Data Quality",
       "Entry Forecast Reliability", "Entry Investability",
       "Entry Final Action"). End-placement is mandatory: inserting
       mid-list would re-map every historical 27-column row when read
       back. _record_to_row writes the four extra cells in the same order.

  v6.6.0 D  _ensure_headers HARDENED to actually widen an existing sheet.
       v6.5.0 rewrote the header row only when cell A5 differed from
       HEADERS[0] ("Record ID", which never changes), so a live
       Performance_Log would NEVER gain the new columns -- and the values
       written by _record_to_row at positions 28-31 would be re-read under
       a stale 27-name header map and silently dropped on the next load.
       v6.6.0 rewrites the header row whenever the live header differs from
       HEADERS by length OR content, while seeding the zero summary block +
       freeze ONLY on a brand-new sheet (so widening never wipes live KPIs).

  v6.6.0 E  PerformanceSummary gains hit_rate_by_investability and
       performance_by_investability; PerformanceAnalyzer.analyze fills
       them by grouping matured outcomes by entry_investability_status
       (win-rate, avg ROI, wins, losses, count per bucket). The
       --analyze stdout adds a "Win-rate by investability:" line, and the
       JSON/CSV exports carry the breakdown via summary.to_dict().

  v6.6.0 F  SCRIPT_VERSION 6.5.0 -> 6.6.0 (SERVICE_VERSION follows).

NOT in scope (deliberately): the gate fields are GOVERNANCE inputs, not
predictions -- this revision measures the gate, it does not change any
recommendation, forecast, or the engine. A meaningful hit-rate still
requires matured records (a 1M number needs a month of data; 3M a
quarter); no code shortens that. The summary SHEET block is intentionally
left at its v6.5.0 A1:E13 layout (pre-existing region) -- investability
KPIs surface via analyze() stdout + JSON/CSV, not by extending that block.

[PRESERVED — strictly] All v6.5.0 8-tier vocabulary work and all v6.4.0
endpoint-canonicalization + hardening. v6.6.0 is purely additive: no
removals, no renames, no signature changes. Records written by v6.5.0
load byte-identically (their four gate fields read blank).

Why this revision (v6.5.0 vs v6.4.0)  [PRESERVED BELOW VERBATIM]
-------------------------------------
8-TIER RECOMMENDATION VOCABULARY (rebased on v6.4.0's endpoint canonical-
ization). Closes the last 5-tier vocabulary leak in the Python codebase.
Every other consumer of `recommendation` tokens (engine, advisor, scoring,
insights_builder, top10_selector, schemas, yahoo_fundamentals_provider)
has been on the canonical 8-tier vocabulary since the May 2026 v8.0.0
family rollout — only this analytics tooling still spoke 5-tier.

Two real impacts on v6.4.0 (now fixed):

  Impact 1 (HISTORICAL LOG MISCATEGORIZATION).
    Loading historical Top10 logs at `PerformanceRecord.from_sheet_row`
    calls `_parse_enum_value(RecommendationType, rec_raw, HOLD)`. For
    any historical entry with `Entry Recommendation = "ACCUMULATE"`,
    `"REDUCE"`, or `"AVOID"`, v6.4.0's 5-tier enum had no match —
    `_parse_enum_value` fell back to the default HOLD. The entry was
    SILENTLY MISCATEGORIZED in every subsequent analytic: win-rate, ROI
    averages, Sharpe/Sortino by band — all computed against the wrong
    tier. Any report grouped by recommendation showed inflated HOLD
    counts and missing ACCUMULATE/REDUCE/AVOID bands entirely.

  Impact 2 (NEW-ROW MISCLASSIFICATION).
    `_recommendation_from_row` classified rows from the live Top10
    feed via substring matching (`if "BUY" in s`, etc.). The 5-tier
    branches matched STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL but had no
    ACCUMULATE/REDUCE/AVOID branches, and broker-vocab aliases like
    OUTPERFORM/OVERWEIGHT/UNDERPERFORM also fell through to HOLD.
    Same silent-collapse bug as Impact 1, just on the read-side path.

v6.5.0 closes both gaps without touching anything else:

  v6.5.0 A  `RecommendationType` enum EXPANDED from 5 to 8 members.
       Existing members (STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL) preserved
       byte-identical so any serialized state from v6.4.0 (pickles,
       JSON exports, CSV cells, Sheets rows) still deserializes. Three
       new members appended:
         ACCUMULATE = "ACCUMULATE"
         REDUCE     = "REDUCE"
         AVOID      = "AVOID"
       Matches schemas.py v7.0.0's `Recommendation` enum vocabulary
       exactly (the engine's canonical authority). `_parse_enum_value`
       continues to work without changes — its existing whitespace /
       underscore / hyphen normalization handles "ACCUMULATE", "REDUCE",
       "AVOID" naturally via the `enum_cls(s)` constructor call.

  v6.5.0 B  `_recommendation_from_row` REWRITTEN.
       Precedence-ordered substring matching — longer / more specific
       tokens checked first to prevent the silent-collapse bug. Drives
       off the same vocabulary table used by yahoo_fundamentals_provider
       v6.3.0's `map_recommendation`, so historical rows ingested from
       either source classify identically:
         "STRONG BUY" / "STRONG_BUY" / "STRONGBUY"          -> STRONG_BUY
         "STRONG SELL" / "STRONG_SELL" / "STRONGSELL"       -> STRONG_SELL
         "CONVICTION SELL" / "DEEP SELL"                    -> STRONG_SELL
         "ACCUMULATE" / "SCALE IN" / "SCALE_IN"             -> ACCUMULATE
         "MODERATE BUY" / "MOD BUY"                         -> ACCUMULATE
         "AVOID" / "HARD PASS" / "DO NOT BUY" / "DNB"       -> AVOID
         "UNINVESTABLE"                                     -> AVOID
         "OUTPERFORM" / "OVERWEIGHT" / "ADD"                -> BUY
         "BUY"                                              -> BUY
         "MARKET PERFORM" / "NEUTRAL" / "EQUAL WEIGHT"      -> HOLD
         "HOLD"                                             -> HOLD
         "UNDERPERFORM" / "UNDERWEIGHT" / "REDUCE" / "TRIM" -> REDUCE
         "EXIT"                                             -> SELL
         "SELL"                                             -> SELL
         unknown / blank                                    -> HOLD

  v6.5.0 C  Cross-stack roster (May 2026 v8.0.0 family floor) documented
       in the header below.

  v6.5.0 D  SCRIPT_VERSION bumped 6.4.0 -> 6.5.0; SERVICE_VERSION follows.

[PRESERVED — strictly] All v6.4.0 endpoint-canonicalization work
(/v1/advanced/top10-investments, /v1/enriched/quotes with format=items,
tolerant _extract_rows_from_envelope, /quotes legacy fallback chain,
/v1/enriched/sheet-rows + /v1/analysis/sheet-rows fallbacks); all v6.4.0
hardening (lazy _CPU_EXECUTOR, wait=True+cancel_futures shutdown,
realized_roi/unrealized_roi None coercion before format spec, _TRUTHY/
_FALSY vocabulary, _env_bool/_env_int/_env_csv helpers, SERVICE_VERSION
alias, TRACK_* env var defaults for every CLI flag, _resolve_spreadsheet_id
returns "" instead of raising); the full PerformanceRecord dataclass
schema (it gains 3 new valid values for `entry_recommendation` but the
field name and type are unchanged); every external-facing CLI argument;
the Performance_Log sheet's 27-column HEADERS contract; daemon mode +
signal handling. v6.5.0 is PURELY ADDITIVE at the vocabulary level —
no removals, no renames. Historical performance logs written by v6.4.0
deserialize byte-identically; ACCUMULATE/REDUCE/AVOID rows now resolve
to their correct tier instead of silent HOLD.

Cross-stack alignment (current floor)
----------------------------------------------------
  - core.reco_normalize             v8.0.0
  - core.scoring                    v5.7.4
  - core.scoring_engine             v3.7.0
  - core.enriched_quote             v4.7.0
  - core.schemas                    v7.0.0  (Recommendation enum 8-tier
                                              -- canonical authority)
  - core.sheets.schema_registry     v2.13.0
  - core.data_engine_v2             v5.79.4  (investability gate, 115 cols)
  - core.investment_advisor_engine  v4.5.0
  - core.analysis.insights_builder  v8.0.1
  - core.analysis.top10_selector    v4.14.0
  - core.analysis.criteria_model    v3.2.0
  - core.providers.yahoo_fundamentals  v6.3.0
  - core.providers.yahoo_chart         v8.2.0  (no rating surface)
  - core.providers.eodhd               v4.6.0+ (no rating surface)
  - scripts.track_performance          v6.8.0  (THIS DELIVERY)

===========================================================

Why this revision (v6.4.0 vs v6.3.0)  [PRESERVED BELOW VERBATIM]
-------------------------------------
- 🔑 FIX CRITICAL: `/v1/analysis/top10` does NOT exist in this project.
    Canonical endpoints are `POST /v1/advanced/top10-investments` and
    `POST /v1/advanced/top10` (from `routes/investment_advisor.py`
    v2.13.1). v6.3.0's `--record` flag has NEVER worked — every request
    returned 404. v6.4.0:
      - Tries `/v1/advanced/top10-investments` first, then `/v1/advanced/top10`.
      - Legacy `/v1/analysis/top10` still tried as the last-resort fallback
        for any custom deployment that genuinely exposes it.
      - Accepts the canonical envelope with `rows`, `row_objects`, `items`,
        `records`, `data`, or `quotes` (all equivalent row list aliases).

- 🔑 FIX CRITICAL: `/quotes` price fetch path reordered and expanded.
    v6.3.0 checked only `data.<SYM>` shape; the canonical router emits
    `items` (preferred), `data`, `results`, or `quotes` from
    `/v1/enriched/quotes` (the endpoint `run_market_scan v5.3.0` uses).
    v6.4.0 tries `/v1/enriched/quotes` FIRST with `format="items"`,
    extracts from `items`/`data`/`results`/`quotes`, then falls back to
    `/quotes`, `/v1/enriched/sheet-rows`, `/v1/analysis/sheet-rows`.

- 🔑 FIX HIGH: `ReportGenerator.generate_html` crashed when a record's
    `realized_roi` AND `unrealized_roi` were both `None`. v6.4.0 coerces
    to 0.0 BEFORE the f-string format specifier.

- FIX MEDIUM: `_CPU_EXECUTOR` is now lazy-initialized (was eager at
    module import, spawning 8 threads on any `import track_performance`).
- FIX MEDIUM: `_shutdown_executor()` uses `wait=True` with
    `cancel_futures=True` fallback. v6.3.0 used `wait=False` abandoning
    in-flight tasks on interrupt.

- FIX: Dead imports removed: `Sequence` (typing), `urlparse`
    (urllib.parse), `pd`/`pandas` (soft-imported, flag never read),
    `stats`/`minimize` from scipy (unused), `aiohttp.client_exceptions`
    (redundant sub-import).
- FIX: Added project-standard `_TRUTHY`/`_FALSY` vocabulary matching
    `main._TRUTHY`/`_FALSY`. `_TRACING_ENABLED` now uses the canonical
    8-value set (was partial 5-value in v6.3.0).
- FIX: Added `_env_bool`/`_env_int`/`_env_csv` helpers.
- FIX: Added `SERVICE_VERSION = SCRIPT_VERSION` alias.
- FIX: Added `TRACK_*` env var defaults for every CLI flag so the
    runner can be driven purely from the environment (cron/CI).
- FIX: `_get_spreadsheet_id` returns `""` instead of raising; main()
    handles missing id with a clean `return 1`.
- FIX: Exit codes documented: 0/1/130.

Core Capabilities (kept)
- Performance log store in Google Sheets (default tab: Performance_Log)
- KPI summary (win-rate, avg ROI, Sharpe/Sortino)
- Monte Carlo win-rate CI (when numpy available)
- Export reports (json/csv/html)
- Daemon mode (signal-safe; clean shutdown)

Notes
- Best-effort. Never fails just because optional deps
  (numpy/scipy/gspread/aiohttp/matplotlib) are missing.

Environment
-----------
  TRACK_SHEET_ID           spreadsheet id (also DEFAULT_SPREADSHEET_ID)
  TRACK_SHEET_NAME         performance log tab name (default Performance_Log)
  TRACK_RECORD             truthy = record new from Top10
  TRACK_AUDIT              truthy = audit active records
  TRACK_ANALYZE            truthy = analyze + write summary block
  TRACK_SIMULATE           truthy = Monte Carlo simulation
  TRACK_EXPORT             truthy = export report
  TRACK_DAEMON             truthy = run daemon mode
  TRACK_HORIZONS           comma-separated list (default "1M,3M")
  TRACK_MAX_RECORDS        max records to load (default 10000)
  TRACK_CONFIDENCE         simulation confidence (default 0.95)
  TRACK_ITERATIONS         Monte Carlo iterations (default 10000)
  TRACK_FORMAT             json|csv|html|all (default html)
  TRACK_OUTPUT             output file base path
  TRACK_INTERVAL           daemon interval seconds (default 3600)
  TRACK_VERBOSE            truthy = verbose logging
  TRACK_EVENT_CONTEXT      v6.14.0: populate the four Signal_History event-
                           context columns (default ON; =0 writes blanks)
  TFB_CALENDAR_SHEET       v6.15.0: Calendar_Events tab name read for the
                           earnings/ex-div dates (default Calendar_Events)
  BACKEND_BASE_URL /
  TFB_BASE_URL             TFB API base URL
  TFB_TOKEN / APP_TOKEN /
  BACKEND_TOKEN / X_APP_TOKEN  Auth token (Bearer + X-APP-TOKEN)
  GOOGLE_SHEETS_CREDENTIALS /
  GOOGLE_CREDENTIALS       Service account (JSON or base64-encoded JSON)
  CORE_TRACING_ENABLED /
  TRACING_ENABLED          truthy = OpenTelemetry tracing
  LOG_LEVEL                logger level (default info)

Exit codes
----------
  0   success
  1   fatal error / missing config
  130 interrupted (SIGINT)

===========================================================
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import csv
import io
import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from threading import Event, Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import HTTPError, URLError


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "6.19.0"
# v6.11.0: BACKTEST HARDENING (additive, default-OFF -- OFF path byte-identical
#   to v6.10.1). Two independently-gated corrections, both proven on the live
#   KSA+US grid before folding here:
#   (1) NON-OVERLAPPING t-stat [env TFB_BACKTEST_NONOVERLAP=1, or run_backtest(
#       ..., nonoverlap=True)]: the existing verdict pools EVERY day, so at long
#       horizons consecutive forward-return windows overlap (h-1)/h and the t-stat
#       is massively inflated -- a throwaway grid showed 4 "ACCEPTs" at t~2 that
#       ALL collapsed (t -> 0.0-0.4, n_indep 1-17) once windows were made
#       independent. When ON, the verdict is re-decided on independent windows
#       (advance the cursor by h whenever a window is consumed); the 't-stat'
#       column then carries the HONEST (independent) statistic and Notes disclose
#       both it and the inflated overlapping value. Optional multiple-testing
#       floor via TFB_BACKTEST_TSTAT_FLOOR (e.g. 2.94 for a 32-test Bonferroni
#       bar) raises the effective |t| threshold in this mode only.
#   (2) DEEP KSA HISTORY [env TFB_BACKTEST_KSA_YF=1]: yahoo_chart's shim defaults
#       to period=1mo so .SR tickers returned only ~22 bars -- too few for any
#       horizon, silently making the "KSA+US" backtest ~98% US. When ON, symbols
#       that come back shallow (< TFB_BACKTEST_KSA_MIN_BARS, default 60) are
#       re-pulled directly from yfinance at period=2y (~505 KSA bars), the same
#       working source the grid used. US stays on EODHD. Both flags MUST be set
#       to 1 before testing (b)/(c) rotation hypotheses, or the harness will
#       over-accept exactly the way the lenient grid did.
SERVICE_VERSION = SCRIPT_VERSION  # v6.4.0: cross-script alias (preserved)
SCRIPT_NAME = "PerformanceTracker"


# ---------------------------------------------------------------------------
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


def _env_int(
    name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None
) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        v = int(float(raw))
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_float(name: str, default: float) -> float:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        return float(raw)
    except Exception:
        return default


def _env_csv(name: str, default: Optional[List[str]] = None) -> Optional[List[str]]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items or default


# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=opt, default=str).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(
            v, indent=(indent if indent else None), default=str, ensure_ascii=False
        )

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False


# ---------------------------------------------------------------------------
# Optional imports (SAFE)
# ---------------------------------------------------------------------------
try:
    import numpy as np  # type: ignore
    NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

# scipy (v6.4.0: dead imports scrubbed — only track availability)
try:
    import scipy  # type: ignore  # noqa: F401
    SCIPY_AVAILABLE = True
except Exception:
    SCIPY_AVAILABLE = False

# Google Sheets (gspread)
try:
    import gspread  # type: ignore
    from google.oauth2 import service_account  # type: ignore

    GSPREAD_AVAILABLE = True
except Exception:
    gspread = None  # type: ignore
    service_account = None  # type: ignore
    GSPREAD_AVAILABLE = False

# Async HTTP (optional)
try:
    import aiohttp  # type: ignore

    ASYNC_HTTP_AVAILABLE = True
except Exception:
    aiohttp = None  # type: ignore
    ASYNC_HTTP_AVAILABLE = False

# Visualization (matplotlib only — NO seaborn)
try:
    import matplotlib  # type: ignore

    matplotlib.use("Agg")
    from matplotlib.figure import Figure  # type: ignore

    PLOT_AVAILABLE = True
except Exception:
    Figure = None  # type: ignore
    PLOT_AVAILABLE = False

# Monitoring & Tracing (safe dummy)
try:
    from prometheus_client import Counter, Gauge  # type: ignore

    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

    class _DummyMetric:  # type: ignore
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

    Counter = Gauge = _DummyMetric  # type: ignore


# Optional project imports (best-effort)
settings = None
sheets_service = None


# --- v6.16.0 (Fix #5): decision-symbol forced-coverage knobs -----------------
def _force_decision_enabled() -> bool:
    """Master switch. DEFAULT ON — enforces strategy requirement #2 (daily
    recording for Top_10 + holdings). TFB_TRACK_FORCE_DECISION_SYMBOLS=0
    restores v6.15.0 behavior byte-identically."""
    return _env_bool("TFB_TRACK_FORCE_DECISION_SYMBOLS", True)


def _force_extra_symbols() -> List[str]:
    """Optional pinned extras (csv), unioned with the cost-basis holdings."""
    return [s.strip().upper() for s in (_env_csv("TFB_TRACK_PRIORITY_SYMBOLS") or [])
            if s and s.strip()]


def _force_max() -> int:
    """Cap on forced fetches per run (edge-timeout protection)."""
    return _env_int("TFB_TRACK_FORCE_MAX", 40, lo=1, hi=200)


def _force_rows_endpoint() -> str:
    return (os.getenv("TFB_TRACK_FORCE_ROWS_ENDPOINT")
            or "/v1/analysis/sheet-rows").strip() or "/v1/analysis/sheet-rows"


def _force_rows_page() -> str:
    return (os.getenv("TFB_TRACK_FORCE_ROWS_PAGE")
            or "Market_Leaders").strip() or "Market_Leaders"


def _costbasis_tab() -> str:
    return (os.getenv("TFB_TRACK_COSTBASIS_TAB")
            or "_Portfolio_CostBasis").strip() or "_Portfolio_CostBasis"


def _load_costbasis_symbols(spreadsheet_id: str) -> List[str]:
    """v6.16.0 (Fix #5): Symbol column of _Portfolio_CostBasis, uppercased,
    deduped, header-skipped. Fail-open: ANY problem -> [] with a debug line
    (a Sheets hiccup must never break --record). Mirrors the stores'
    SA-credential pattern; runs in the executor (blocking gspread I/O)."""
    if not spreadsheet_id or not GSPREAD_AVAILABLE or gspread is None:
        return []
    raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS")
           or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
    if not raw or service_account is None:
        return []
    s = raw
    if not s.startswith("{"):
        try:
            dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
            if dec.startswith("{"):
                s = dec
        except Exception:
            pass
    try:
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return []
        creds = service_account.Credentials.from_service_account_info(
            obj, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(spreadsheet_id).worksheet(_costbasis_tab())
        col = ws.col_values(1) or []
        out: List[str] = []
        seen: set = set()
        for v in col:
            sym = _safe_str(v).strip().upper()
            if not sym or sym == "SYMBOL":
                continue
            if sym not in seen:
                seen.add(sym)
                out.append(sym)
        return out
    except Exception as e:
        logger.debug("cost-basis symbol read skipped: %s", e)
        return []


def _ensure_project_root_on_path() -> None:
    try:
        script_dir = Path(__file__).parent.absolute()
        project_root = script_dir.parent
        for p in (script_dir, project_root):
            ps = str(p)
            if ps and ps not in sys.path:
                sys.path.insert(0, ps)
    except Exception:
        pass


_ensure_project_root_on_path()

try:
    from env import settings as _settings  # type: ignore
    settings = _settings
except Exception:
    settings = None

try:
    import google_sheets_service as sheets_service  # type: ignore
except Exception:
    sheets_service = None

# core.stats -- Information Coefficient for the reliability calibrator (v6.12.0).
# Best-effort, in the same style as the project imports above and AFTER the path
# setup. If core.stats is not importable (e.g. this file run in isolation), IC is
# simply not computed and every other calibration metric is unaffected.
# Disclosure-only: it changes no recommendation, score, or gate.
try:
    from core.stats import information_coefficient as _information_coefficient  # type: ignore
    _IC_AVAILABLE = True
except Exception:
    try:
        from stats import information_coefficient as _information_coefficient  # type: ignore
        _IC_AVAILABLE = True
    except Exception:
        _information_coefficient = None  # type: ignore
        _IC_AVAILABLE = False


# =============================================================================
# Logging & global state
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
)
logger = logging.getLogger("PerfTracker")

_RIYADH_TZ = timezone(timedelta(hours=3))

# v6.4.0: tracing uses project-canonical _env_bool
_TRACING_ENABLED = _env_bool("CORE_TRACING_ENABLED", False) or _env_bool(
    "TRACING_ENABLED", False
)

# v6.4.0: Lazy-initialized executor (was eager at module level).
_CPU_EXECUTOR: Optional[concurrent.futures.ThreadPoolExecutor] = None


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _CPU_EXECUTOR
    if _CPU_EXECUTOR is None:
        _CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="PerfWorker"
        )
    return _CPU_EXECUTOR


def _shutdown_executor() -> None:
    global _CPU_EXECUTOR
    if _CPU_EXECUTOR is None:
        return
    try:
        _CPU_EXECUTOR.shutdown(wait=True, cancel_futures=True)
    except TypeError:
        # Python 3.8 doesn't support cancel_futures
        try:
            _CPU_EXECUTOR.shutdown(wait=True)
        except Exception:
            pass
    except Exception:
        pass
    finally:
        _CPU_EXECUTOR = None


if PROMETHEUS_AVAILABLE:
    perf_records_processed = Counter(
        "perf_records_processed_total", "Total performance records processed"
    )
    perf_daemon_cycles = Counter("perf_daemon_cycles_total", "Total daemon cycles")
    perf_win_rate = Gauge("perf_overall_win_rate", "Overall win rate percentage")
else:
    perf_records_processed = Counter()  # type: ignore
    perf_daemon_cycles = Counter()  # type: ignore
    perf_win_rate = Gauge()  # type: ignore


# =============================================================================
# Utilities
# =============================================================================
def _out(s: str) -> None:
    sys.stdout.write(s + "\n")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _riyadh_now() -> datetime:
    return datetime.now(_RIYADH_TZ)


def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float)):
            v = float(x)
            if math.isnan(v) or math.isinf(v):
                return float(default)
            return v
        s = str(x).strip()
        if not s or s.lower() in {"na", "n/a", "null", "none", "-", "—"}:
            return float(default)
        s = s.replace(",", "").replace("%", "")
        v = float(s)
        if math.isnan(v) or math.isinf(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _days_until(raw: Any) -> Optional[int]:
    """v6.14.0 (F4): whole days from today (Riyadh) until a date-like value.
    Accepts date/datetime/ISO-ish strings; returns None on blank/unparseable
    input — never raises. Negative values are valid (event already passed)."""
    s = _safe_str(raw)
    if not s:
        return None
    dt: Optional[datetime] = None
    if isinstance(raw, datetime):
        dt = raw
    else:
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(s[: len(fmt) + 2].strip(), fmt)
                break
            except Exception:
                continue
        if dt is None:
            dt = RiyadhTime.parse(s)
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_RIYADH_TZ)
        today = RiyadhTime.now().astimezone(_RIYADH_TZ).date()
        return (dt.astimezone(_RIYADH_TZ).date() - today).days
    except Exception:
        return None


def _merge_calendar_context(rows: List[Dict[str, Any]],
                            ctx: Dict[str, Dict[str, Any]]) -> int:
    """v6.15.0 (F1 wiring): setdefault next_earnings_date / next_ex_div_date
    onto each row from the Calendar_Events context. Backend-supplied values
    always win (setdefault). Returns how many rows received at least one key."""
    if not rows or not ctx:
        return 0
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        c = ctx.get(_safe_str(row.get("symbol")).upper())
        if not c:
            continue
        hit = False
        for k in ("next_earnings_date", "next_ex_div_date"):
            v = c.get(k)
            existing = row.get(k)
            # NOTE: _safe_str(None) returns the string 'None' in this module,
            # so the emptiness check must be explicit, not _safe_str-based.
            if v and (existing is None or not str(existing).strip()):
                row[k] = v
                hit = True
        n += 1 if hit else 0
    return n


def _col_to_a1(col: int) -> str:
    # 1 -> A, 26 -> Z, 27 -> AA
    if col <= 0:
        return "A"
    out = []
    n = col
    while n > 0:
        n, r = divmod(n - 1, 26)
        out.append(chr(65 + r))
    return "".join(reversed(out))


def _a1_range(start_col: int, start_row: int, end_col: int, end_row: int) -> str:
    return f"{_col_to_a1(start_col)}{start_row}:{_col_to_a1(end_col)}{end_row}"


# ---------------------------------------------------------------------------
# v6.8.0: calibration math (pure, unit-tested)
# ---------------------------------------------------------------------------
_Z95 = 1.959963984540054  # 95% two-sided normal quantile


def wilson_interval(wins: int, decided: int, z: float = _Z95) -> Tuple[float, float]:
    """Wilson score interval for a binomial proportion, returned as PERCENT.
    Stays inside [0,100] and is honest at small n. (0,0) when decided==0."""
    n = int(decided)
    if n <= 0:
        return 0.0, 0.0
    w = max(0, min(int(wins), n))
    p = w / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denom
    half = (z * math.sqrt((p * (1.0 - p) / n) + (z2 / (4.0 * n * n)))) / denom
    lo = max(0.0, min(100.0, (center - half) * 100.0))
    hi = max(0.0, min(100.0, (center + half) * 100.0))
    return round(lo, 1), round(hi, 1)


def shrink_factor(raw: float, decided: int, k: int) -> float:
    """Shrink a raw calibration factor toward 1.0 by sample size:
    1 + (raw-1) * n/(n+k). Small n -> ~1.0 (no correction asserted)."""
    n = max(0, int(decided))
    if n <= 0:
        return 1.0
    return 1.0 + (raw - 1.0) * (n / (n + max(1, int(k))))


class FullJitterBackoff:
    def __init__(
        self, max_retries: int = 5, base_delay: float = 0.8, max_delay: float = 30.0
    ):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = float(base_delay)
        self.max_delay = float(max_delay)

    async def execute_async(
        self, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        for attempt in range(self.max_retries):
            try:
                res = fn(*args, **kwargs)
                if hasattr(res, "__await__"):
                    res = await res
                return res
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                await asyncio.sleep(random.uniform(0.0, cap))

    def execute_sync(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        for attempt in range(self.max_retries):
            try:
                return fn(*args, **kwargs)
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                time.sleep(random.uniform(0.0, cap))


class RiyadhTime:
    _tz = _RIYADH_TZ

    @classmethod
    def now(cls) -> datetime:
        return datetime.now(cls._tz)

    @classmethod
    def parse(cls, s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        raw = str(s).strip()
        if not raw:
            return None

        # ISO first (supports Z)
        try:
            iso = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=cls._tz)
            return dt.astimezone(cls._tz)
        except Exception:
            pass

        fmts = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
            "%d/%m/%Y",
            "%d/%m/%Y %H:%M:%S",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(raw, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=cls._tz)
                return dt.astimezone(cls._tz)
            except Exception:
                continue
        return None

    @classmethod
    def format(
        cls, dt: Optional[datetime] = None, fmt: str = "%Y-%m-%d %H:%M:%S"
    ) -> str:
        d = dt or cls.now()
        if d.tzinfo is None:
            d = d.replace(tzinfo=cls._tz)
        return d.astimezone(cls._tz).strftime(fmt)


# =============================================================================
# v6.18.0 — cohort-rescue helpers (PF-GEOM / PRICE-FEED-LOUD / CA-GUARD)
_CA_TAG = "[v6.18.0 CA-GUARD]"
_PRICE_FEED_TAG = "[v6.18.0 PRICE-FEED]"


def _summary_side_enabled() -> bool:
    """v6.18.0 (PF-GEOM): write the KPI summary BESIDE the record grid
    (AG1:AK13) instead of on top of it (A1:E13, which overwrote the header
    row's identity cells and orphaned the 8 earliest records). Default ON.
    TFB_TRACK_SUMMARY_SIDE=0 restores the v6.17.0 placement."""
    return (os.getenv("TFB_TRACK_SUMMARY_SIDE") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _summary_block_range(n_rows: int) -> str:
    """v6.18.0 (PF-GEOM): A1-range for an n_rows x 5 KPI block. Side placement
    = column AG (33), safely right of the 31-column record grid."""
    if _summary_side_enabled():
        return f"AG1:AK{int(n_rows)}"
    return f"A1:E{int(n_rows)}"


def _price_feed_dead(syms: List[str], price_map: Dict[str, float]) -> Optional[str]:
    """v6.18.0 (PRICE-FEED-LOUD): classify a dead/degraded feed. None when
    healthy, else a short cause for the ERROR log. 'Dead' = symbols were
    requested but not one positive price arrived — the exact condition that
    silently froze all 2,264 records at entry price in the 2026-07-12 export."""
    if not syms:
        return None
    priced = sum(1 for s in syms if float(price_map.get(s, 0.0) or 0.0) > 0.0)
    if priced == 0:
        return f"0/{len(syms)} symbols priced — feed dead"
    if priced * 4 < len(syms):
        return f"only {priced}/{len(syms)} symbols priced"
    return None


def _ca_guard_enabled() -> bool:
    """v6.18.0 (CA-GUARD) master switch. Default ON. TFB_TRACK_CA_GUARD=0
    restores the v6.17.0 maturation verbatim."""
    return (os.getenv("TFB_TRACK_CA_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _ca_verify_pct() -> float:
    """|print ROI| (pp) above which maturation is verified vs adjusted
    history. Default 15."""
    try:
        v = float((os.getenv("TFB_TRACK_CA_VERIFY_PCT") or "").strip() or 15.0)
    except Exception:
        v = 15.0
    return max(0.0, v)


def _ca_decide(print_roi: float, adj_roi: Optional[float],
               tol_pp: float = 12.0) -> Tuple[str, float]:
    """v6.18.0 (CA-GUARD) pure decision core. Returns (verdict, roi_to_use):
      ('ok', print_roi)      adjusted agrees within tol_pp — real move.
      ('adjust', adj_roi)    prints disagree with the adjusted series by
                             > tol_pp — a corporate action re-based the share
                             count; the ADJUSTED number is the honest outcome
                             (bonus 1:1: print -50 vs adj ~0 -> 0).
      ('suspect', print_roi) verification impossible — never let a possibly
                             fake WIN/LOSS into the calibration cohort."""
    if adj_roi is None:
        return "suspect", print_roi
    if abs(print_roi - adj_roi) > tol_pp:
        return "adjust", float(adj_roi)
    return "ok", print_roi


def _ca_adjusted_roi(symbol: str, entry_date: Any) -> Optional[float]:
    """ROI% from the ADJUSTED close series between entry_date and the latest
    bar (deep-history pull, adjusted via v6.18.0 D). Start bar = nearest bar
    within 7 days of entry_date. Fail-soft None on any doubt."""
    try:
        sym = _safe_str(symbol).upper()
        if not sym:
            return None
        if sym.endswith(".US"):
            sym = sym[:-3]  # yfinance wants bare US tickers
        bars = _yf_deep_history(sym, period="1y")
        if not bars:
            return None
        want = _safe_str(entry_date)[:10]
        if not want:
            return None
        best = None
        best_gap = 99
        for b in bars:
            d = _safe_str(b.get("date"))[:10]
            if not d:
                continue
            try:
                gap = abs((datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(want, "%Y-%m-%d")).days)
            except Exception:
                continue
            if gap < best_gap:
                best, best_gap = b, gap
        if best is None or best_gap > 7:
            return None
        c0 = _bt_bar_close(best)
        c1 = _bt_bar_close(bars[-1])
        if not c0 or not c1 or c0 <= 0:
            return None
        return (c1 / c0 - 1.0) * 100.0
    except Exception:
        return None


# v6.17.0 — maturity price-integrity switches
# =============================================================================
_MATURE_FRESH_TAG = "[v6.17.0 MATURE-FRESH-ONLY]"


def _mature_fresh_only_enabled() -> bool:
    """v6.17.0 (C) master switch. Default ON; set
    TFB_TRACK_MATURE_FRESH_ONLY=0/false/off/no to restore the v6.16.0
    maturation verbatim (past-due records mature on stale/default
    unrealized_roi — the fake-breakeven behavior)."""
    return (os.getenv("TFB_TRACK_MATURE_FRESH_ONLY") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _mature_grace_days() -> int:
    """v6.17.0 (B): days past target_date a price-less record stays ACTIVE
    (retried every run) before expiring UNPRICED. TFB_TRACK_MATURE_GRACE_DAYS,
    default 5, floor 0; unparseable values fall back to 5."""
    try:
        return max(0, int(float(os.getenv("TFB_TRACK_MATURE_GRACE_DAYS") or "5")))
    except Exception:
        return 5


# =============================================================================
# Enums & Data Models
# =============================================================================
class PerformanceStatus(str, Enum):
    ACTIVE = "active"
    MATURED = "matured"
    EXPIRED = "expired"
    STOPPED = "stopped"
    PENDING = "pending"


class HorizonType(str, Enum):
    WEEK_1 = "1W"
    MONTH_1 = "1M"
    MONTH_3 = "3M"
    MONTH_6 = "6M"
    YEAR_1 = "1Y"
    YEAR_3 = "3Y"
    YEAR_5 = "5Y"

    @property
    def days(self) -> int:
        return {
            "1W": 7, "1M": 30, "3M": 90, "6M": 180,
            "1Y": 365, "3Y": 1095, "5Y": 1825,
        }[self.value]


class RecommendationType(str, Enum):
    """
    8-tier recommendation vocabulary (v6.5.0).

    Aligned with schemas.py v7.0.0 `Recommendation` enum -- the engine's
    canonical authority. Tier ordering (most-positive to most-negative):
      STRONG_BUY > BUY > ACCUMULATE > HOLD > REDUCE > SELL > STRONG_SELL > AVOID

    Member order below preserves v6.4.0's first five members byte-
    identical so any serialized state (pickles / JSON exports / CSV
    cells / Sheets rows) written by v6.4.0 continues to deserialize
    without change. The three new members (ACCUMULATE / REDUCE / AVOID)
    are appended at the end.

    Enum value strings use plain ASCII upper-case with a space for
    multi-word tiers ("STRONG BUY", "STRONG SELL") -- matching v6.4.0's
    convention. `_parse_enum_value` handles "STRONG_BUY" / "STRONG-BUY" /
    "STRONGBUY" / "strong buy" via its existing normalization
    (replace "_"->" ", replace "-"->" ", strip, upper) without any
    changes required to that helper.
    """
    # v6.4.0 vocabulary -- preserved byte-identical for backwards compat.
    STRONG_BUY = "STRONG BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG SELL"
    # v6.5.0 8-tier additions (appended at the end -- pickle order-stable).
    ACCUMULATE = "ACCUMULATE"
    REDUCE = "REDUCE"
    AVOID = "AVOID"


def _parse_enum_value(enum_cls: Any, raw: Any, default: Any) -> Any:
    s = _safe_str(raw).upper()
    if not s:
        return default
    s = s.replace("_", " ").replace("-", " ").strip()
    if enum_cls is HorizonType:
        s = s.replace("WEEK", "W").replace("MONTH", "M").replace("YEAR", "Y")
    try:
        return enum_cls(s)
    except Exception:
        norm = s.replace(" ", "")
        for m in enum_cls:
            if str(m.value).replace(" ", "") == norm:
                return m
        return default


def _risk_bucket_from_score(risk_score: Optional[float]) -> str:
    if risk_score is None:
        return "MODERATE"
    try:
        rs = float(risk_score)
        if rs <= 33:
            return "LOW"
        if rs <= 66:
            return "MODERATE"
        return "HIGH"
    except Exception:
        return "MODERATE"


def _confidence_bucket(conf: Optional[float]) -> str:
    if conf is None:
        return "MEDIUM"
    try:
        c = float(conf)
        if c >= 0.75:
            return "HIGH"
        if c >= 0.50:
            return "MEDIUM"
        return "LOW"
    except Exception:
        return "MEDIUM"


@dataclass(slots=True)
class PerformanceRecord:
    record_id: str
    symbol: str
    horizon: HorizonType
    date_recorded: datetime

    entry_price: float
    entry_recommendation: RecommendationType
    entry_score: float
    entry_risk_bucket: str
    entry_confidence: str
    origin_tab: str

    target_price: float
    target_roi: float
    target_date: datetime

    status: PerformanceStatus
    current_price: float = 0.0
    unrealized_roi: float = 0.0
    realized_roi: Optional[float] = None
    outcome: Optional[str] = None  # WIN, LOSS, BREAKEVEN

    volatility: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0

    sector: Optional[str] = None
    factor_exposures: Dict[str, float] = field(default_factory=dict)

    last_updated: datetime = field(default_factory=_utc_now)
    maturity_date: Optional[datetime] = None
    notes: str = ""

    # v6.6.0: investability-gate snapshot captured at ENTRY (from
    # data_engine_v2 v5.78+ gate output). Appended at the END of the
    # dataclass field list so (a) the non-default-after-default ordering
    # stays valid, (b) existing keyword construction is unaffected, and
    # (c) any v6.5.0-serialized state still deserializes (old records
    # simply carry blank gate fields). These let PerformanceAnalyzer
    # segment win-rate by the gate's verdict -- i.e. measure whether
    # INVESTABLE actually outperforms WATCHLIST, which is the whole point
    # of the gate. NEVER reorder these above the existing fields.
    entry_data_quality: float = 0.0
    entry_forecast_reliability: float = 0.0
    entry_investability_status: str = ""
    entry_final_action: str = ""

    @property
    def key(self) -> str:
        return (
            f"{self.symbol}|{self.horizon.value}|"
            f"{self.date_recorded.astimezone(_RIYADH_TZ).strftime('%Y%m%d')}"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "key": self.key,
            "symbol": self.symbol,
            "horizon": self.horizon.value,
            "date_recorded_riyadh": RiyadhTime.format(self.date_recorded),
            "entry_price": self.entry_price,
            "entry_recommendation": self.entry_recommendation.value,
            "entry_score": self.entry_score,
            "risk_bucket": self.entry_risk_bucket,
            "confidence": self.entry_confidence,
            # v6.6.0: investability-gate snapshot (grouped with entry data).
            "entry_data_quality": self.entry_data_quality,
            "entry_forecast_reliability": self.entry_forecast_reliability,
            "entry_investability_status": self.entry_investability_status,
            "entry_final_action": self.entry_final_action,
            "origin_tab": self.origin_tab,
            "target_price": self.target_price,
            "target_roi_pct": self.target_roi,
            "target_date_riyadh": RiyadhTime.format(self.target_date),
            "status": self.status.value,
            "current_price": self.current_price,
            "unrealized_roi_pct": self.unrealized_roi,
            "realized_roi_pct": self.realized_roi,
            "outcome": self.outcome,
            "volatility": self.volatility,
            "max_drawdown_pct": self.max_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "sector": self.sector,
            "factor_exposures": self.factor_exposures,
            "last_updated_utc": self.last_updated.astimezone(timezone.utc).isoformat(),
            "maturity_date_riyadh": (
                RiyadhTime.format(self.maturity_date) if self.maturity_date else None
            ),
            "notes": self.notes,
        }

    @classmethod
    def from_sheet_row(cls, row: List[Any], headers: List[str]) -> "PerformanceRecord":
        hmap = {str(h).strip(): i for i, h in enumerate(headers)}

        def get(name: str) -> Any:
            idx = hmap.get(name)
            if idx is None or idx < 0 or idx >= len(row):
                return None
            return row[idx]

        symbol = _safe_str(get("Symbol")).upper()
        horizon = _parse_enum_value(HorizonType, get("Horizon"), HorizonType.MONTH_1)
        dt_rec = RiyadhTime.parse(_safe_str(get("Date Recorded (Riyadh)"))) or RiyadhTime.now()
        dt_tgt = RiyadhTime.parse(_safe_str(get("Target Date (Riyadh)"))) or (
            dt_rec + timedelta(days=horizon.days)
        )
        dt_upd = RiyadhTime.parse(_safe_str(get("Last Updated (Riyadh)"))) or RiyadhTime.now()
        dt_mat = (
            RiyadhTime.parse(_safe_str(get("Maturity Date")))
            if _safe_str(get("Maturity Date"))
            else None
        )

        status_raw = _safe_str(get("Status")).lower() or "active"
        try:
            status = PerformanceStatus(status_raw)
        except Exception:
            status = PerformanceStatus.ACTIVE

        rec_raw = _safe_str(get("Entry Recommendation")).upper() or "HOLD"
        rec = _parse_enum_value(RecommendationType, rec_raw, RecommendationType.HOLD)

        realized = _safe_str(get("Realized ROI %"))
        realized_roi = None if realized == "" else _safe_float(realized, default=0.0)

        factor_json = _safe_str(get("Factor Exposures"))
        factors: Dict[str, float] = {}
        if factor_json:
            try:
                obj = json.loads(factor_json)
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        factors[str(k)] = _safe_float(v, default=0.0)
            except Exception:
                factors = {}

        return cls(
            record_id=_safe_str(get("Record ID")) or str(uuid.uuid4()),
            symbol=symbol,
            horizon=horizon,
            date_recorded=dt_rec,
            entry_price=_safe_float(get("Entry Price"), default=0.0),
            entry_recommendation=rec,
            entry_score=_safe_float(get("Entry Score"), default=0.0),
            entry_risk_bucket=_safe_str(get("Risk Bucket")) or "MODERATE",
            entry_confidence=_safe_str(get("Confidence")) or "MEDIUM",
            origin_tab=_safe_str(get("Origin Tab")) or "Unknown",
            target_price=_safe_float(get("Target Price"), default=0.0),
            target_roi=_safe_float(get("Target ROI %"), default=0.0),
            target_date=dt_tgt,
            status=status,
            current_price=_safe_float(get("Current Price"), default=0.0),
            unrealized_roi=_safe_float(get("Unrealized ROI %"), default=0.0),
            realized_roi=realized_roi,
            outcome=_safe_str(get("Outcome")) or None,
            volatility=_safe_float(get("Volatility"), default=0.0),
            max_drawdown=_safe_float(get("Max Drawdown %"), default=0.0),
            sharpe_ratio=_safe_float(get("Sharpe Ratio"), default=0.0),
            sector=_safe_str(get("Sector")) or None,
            factor_exposures=factors,
            last_updated=dt_upd.astimezone(timezone.utc),
            maturity_date=dt_mat,
            notes=_safe_str(get("Notes")),
            # v6.6.0: read the investability-gate snapshot by header name.
            # Absent on rows written before v6.6.0 -> safe zeros / blanks.
            entry_data_quality=_safe_float(get("Entry Data Quality"), default=0.0),
            entry_forecast_reliability=_safe_float(
                get("Entry Forecast Reliability"), default=0.0
            ),
            entry_investability_status=_safe_str(get("Entry Investability") or ""),
            entry_final_action=_safe_str(get("Entry Final Action") or ""),
        )


@dataclass(slots=True)
class PerformanceSummary:
    total_records: int = 0
    active_records: int = 0
    matured_records: int = 0
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    avg_roi: float = 0.0
    median_roi: float = 0.0
    roi_std: float = 0.0
    best_roi: float = 0.0
    worst_roi: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    win_rate: float = 0.0
    hit_rate_by_horizon: Dict[str, float] = field(default_factory=dict)
    performance_by_sector: Dict[str, Any] = field(default_factory=dict)
    # v6.6.0: win-rate / ROI segmented by the ENTRY investability verdict.
    # hit_rate_by_investability maps bucket -> win-rate %; the richer
    # performance_by_investability maps bucket -> {win_rate, avg_roi, wins,
    # losses, count}. Both empty until matured gate-tagged records exist.
    hit_rate_by_investability: Dict[str, float] = field(default_factory=dict)
    performance_by_investability: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# v6.7.0 — Day-to-day ACTION-TREND models (Signal_History)
# =============================================================================
@dataclass(slots=True)
class SignalSnapshot:
    """
    One per decision symbol PER DAY: the day's standing verdict (action /
    recommendation / score / gate). Distinct in purpose from
    PerformanceRecord -- a PerformanceRecord is opened ONCE per pick and
    matured at a horizon; a SignalSnapshot is logged EVERY day so the
    trajectory of the verdict itself (strengthening / weakening / stable /
    flipped) can be read day to day. Keyed symbol|YYYYMMDD (Riyadh) so a
    re-run on the same day is idempotent (first verdict of the day wins;
    see SignalHistoryStore.append_snapshots).
    """
    snapshot_id: str
    symbol: str
    date_recorded: datetime
    recommendation: RecommendationType
    final_action: str
    investability_status: str
    overall_score: float
    forecast_reliability: float
    data_quality: float
    risk_score: float
    price: float
    origin_tab: str
    recorded_at: datetime = field(default_factory=_utc_now)
    # v6.14.0 (F4) event-context capture — defaults keep old call sites valid
    analyst_rating: str = ""
    target_price: float = 0.0
    days_to_earnings: Optional[int] = None
    days_to_exdiv: Optional[int] = None

    @property
    def date_key(self) -> str:
        return self.date_recorded.astimezone(_RIYADH_TZ).strftime("%Y%m%d")

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.date_key}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "key": self.key,
            "symbol": self.symbol,
            "date_riyadh": RiyadhTime.format(self.date_recorded, fmt="%Y-%m-%d"),
            "recorded_at_riyadh": RiyadhTime.format(
                self.recorded_at.astimezone(_RIYADH_TZ)
            ),
            "recommendation": self.recommendation.value,
            "final_action": self.final_action,
            "investability_status": self.investability_status,
            "overall_score": self.overall_score,
            "forecast_reliability": self.forecast_reliability,
            "data_quality": self.data_quality,
            "risk_score": self.risk_score,
            "price": self.price,
            "origin_tab": self.origin_tab,
            "analyst_rating": self.analyst_rating,
            "target_price": self.target_price,
            "days_to_earnings": self.days_to_earnings,
            "days_to_exdiv": self.days_to_exdiv,
        }

    @classmethod
    def from_sheet_row(cls, row: List[Any], headers: List[str]) -> "SignalSnapshot":
        hmap = {str(h).strip(): i for i, h in enumerate(headers)}

        def get(name: str) -> Any:
            idx = hmap.get(name)
            if idx is None or idx < 0 or idx >= len(row):
                return None
            return row[idx]

        dt_rec = RiyadhTime.parse(_safe_str(get("Date (Riyadh)"))) or RiyadhTime.now()
        dt_at = RiyadhTime.parse(_safe_str(get("Recorded At (Riyadh)"))) or dt_rec
        rec = _parse_enum_value(
            RecommendationType,
            _safe_str(get("Recommendation")) or "HOLD",
            RecommendationType.HOLD,
        )
        return cls(
            snapshot_id=_safe_str(get("Snapshot ID")) or str(uuid.uuid4()),
            symbol=_safe_str(get("Symbol")).upper(),
            date_recorded=dt_rec,
            recommendation=rec,
            final_action=_safe_str(get("Final Action")),
            investability_status=_safe_str(get("Investability")),
            overall_score=_safe_float(get("Overall Score"), default=0.0),
            forecast_reliability=_safe_float(get("Forecast Reliability"), default=0.0),
            data_quality=_safe_float(get("Data Quality"), default=0.0),
            risk_score=_safe_float(get("Risk Score"), default=0.0),
            price=_safe_float(get("Price"), default=0.0),
            origin_tab=_safe_str(get("Origin Tab")) or "Top_10_Investments",
            recorded_at=dt_at.astimezone(timezone.utc),
            # v6.14.0 (F4): tolerate old (short) rows — blanks parse to defaults
            analyst_rating=_safe_str(get("Analyst Rating")),
            target_price=_safe_float(get("Target Price"), default=0.0),
            days_to_earnings=(int(_safe_float(get("Days To Earnings")))
                              if _safe_str(get("Days To Earnings")) else None),
            days_to_exdiv=(int(_safe_float(get("Days To ExDiv")))
                           if _safe_str(get("Days To ExDiv")) else None),
        )


@dataclass(slots=True)
class SignalTrend:
    """
    Per-symbol day-to-day trajectory derived from its SignalSnapshot
    history. PURELY DESCRIPTIVE -- it characterises how the engine's own
    verdict has moved over the recent window; it makes no price prediction.
    Its value is confirmation (a multi-day-stable action is more trustworthy
    than a one-day flip) and early warning (a verdict decaying day over day
    flags thesis erosion before any 1M/3M maturity check fires).
    """
    symbol: str
    n_observations: int
    window: int
    current_action: str
    current_investability: str
    latest_score: float
    latest_date: str
    days_in_current_action: int
    action_direction: str   # STRENGTHENING / WEAKENING / STABLE / FLIPPED / NEW
    score_slope: float       # score points per snapshot over the window
    distinct_actions: int
    flip_count: int
    stability: str           # STABLE / DRIFTING / CHOPPY / NEW
    summary_label: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# v6.8.0 — Reliability calibration models
# =============================================================================
@dataclass
class CalibrationBucket:
    name: str
    n: int
    decided: int
    wins: int
    losses: int
    breakeven: int
    realized_win_rate: float        # wins/decided (%), 0.0 if decided==0
    ci_low: float                   # Wilson 95% low (%)
    ci_high: float                  # Wilson 95% high (%)
    mean_realized_roi: float
    mean_claimed_reliability: float
    claimed_win_rate: float         # mean stated reliability read as implied P(win) %
    calibration_factor_raw: float       # ADVISORY (realized/claimed, clamped)
    calibration_factor_shrunk: float    # ADVISORY, sample-shrunk; NEVER applied
    sufficient: bool                # decided >= min_sample
    note: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CalibrationReport:
    generated_riyadh: str
    total_records: int
    matured_records: int
    decided_records: int
    min_sample: int
    shrinkage_k: int
    global_sufficient: bool
    headline: str
    brier_score: Optional[float]
    brier_baseline: float
    by_investability: List[CalibrationBucket] = field(default_factory=list)
    by_reliability_band: List[CalibrationBucket] = field(default_factory=list)
    by_recommendation: List[CalibrationBucket] = field(default_factory=list)
    by_horizon: List[CalibrationBucket] = field(default_factory=list)
    monotonic_reliability: Optional[bool] = None
    discrimination_note: str = ""
    # v6.12.0: Information Coefficient -- rank-correlation of entry_score vs
    # realized_roi across decided records (disclosure-only; stays None unless
    # TFB_HARNESS_IC is enabled and there are >= 3 decided outcomes).
    information_coefficient: Optional[float] = None
    ic_t_stat: Optional[float] = None
    ic_p_value: Optional[float] = None
    ic_n: int = 0
    signal_trend_mix: Dict[str, int] = field(default_factory=dict)
    advisory_note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Backend Client (optional)
# =============================================================================
# v6.9.0: canonical Top10 endpoint chain — REORDERED to fix empty recording.
# The advisor endpoints (/v1/advanced/top10-investments, /v1/advanced/top10)
# bridge to advanced_analysis under a 20s SERVER-SIDE cap (meta
# bridge_timeout_sec=20.0). A cold full Top_10 build takes ~50-90s, so that
# bridge almost always TIMES OUT and returns HTTP 200 with status="partial" and
# ZERO rows (meta bridge_call_outcome="timeout"). track_performance then
# recorded nothing. The advanced_analysis /sheet-rows route runs the SAME build
# with no such cap and returns the full populated page (confirmed live: HTTP 200,
# status="success", 50 rows). So /sheet-rows is now PREFERRED; the advisor
# endpoints are kept as fallbacks. NOTE: /sheet-rows takes a DIFFERENT request
# body ({"page": <page>}) than the advisor endpoints (criteria) — get_top10_rows
# selects the body per-endpoint. Chain + page + client timeout are env-overridable
# (set TRACK_TOP10_ENDPOINTS to the old advisor-first chain to fully revert).
_TOP10_PAGE_DEFAULT = "Top_10_Investments"
_TOP10_ENDPOINTS_DEFAULT: Tuple[str, ...] = (
    "/sheet-rows",                     # advanced_analysis; full page, no 20s cap (PREFERRED)
    "/v1/advanced/sheet-rows",         # alias of the above
    "/v1/advanced/top10-investments",  # advisor; subject to the 20s bridge cap (fallback)
    "/v1/advanced/top10",              # advisor alias
    "/v1/analysis/top10",              # legacy; usually 404 — see v6.3.0 bug note
)
# Back-compat alias (kept in case anything imports the old name).
_TOP10_ENDPOINTS = _TOP10_ENDPOINTS_DEFAULT

try:
    # /sheet-rows runs the full ~50-90s build synchronously, so the CLIENT
    # timeout must exceed it (the old 45s would cut a healthy build off). The
    # advisor fallbacks return their partial at ~20s regardless, so one higher
    # ceiling is safe for the whole chain.
    _TOP10_FETCH_TIMEOUT_SEC = float(os.getenv("TRACK_TOP10_TIMEOUT_SEC", "100") or "100")
except Exception:
    _TOP10_FETCH_TIMEOUT_SEC = 100.0


def _top10_endpoints() -> Tuple[str, ...]:
    """v6.9.0: the Top10 endpoint chain. Override via TRACK_TOP10_ENDPOINTS
    (comma-separated); set it to the old advisor-first chain to revert."""
    raw = (os.getenv("TRACK_TOP10_ENDPOINTS") or "").strip()
    if raw:
        eps = tuple(e.strip() for e in raw.split(",") if e.strip())
        if eps:
            return eps
    return _TOP10_ENDPOINTS_DEFAULT


def _top10_page() -> str:
    """v6.9.0: page name sent in the body to /sheet-rows endpoints. Override
    via TRACK_TOP10_PAGE."""
    return (os.getenv("TRACK_TOP10_PAGE") or _TOP10_PAGE_DEFAULT).strip() or _TOP10_PAGE_DEFAULT

# Quotes: preferred -> fallbacks.
_QUOTES_ENDPOINT_PRIMARY = "/v1/enriched/quotes"
_QUOTES_ENDPOINT_LEGACY = "/quotes"
_SHEET_ROWS_ENRICHED = "/v1/enriched/sheet-rows"
_SHEET_ROWS_ANALYSIS = "/v1/analysis/sheet-rows"


def _extract_rows_from_envelope(data: Any) -> List[Dict[str, Any]]:
    """
    v6.4.0: tolerant extractor matching the canonical TFB envelope.
    Router emits any of `rows`, `row_objects`, `items`, `records`, `data`,
    `quotes`, `results` — all equivalent row list aliases.
    """
    if not isinstance(data, dict):
        return []
    for k in ("rows", "row_objects", "items", "records", "data", "quotes", "results"):
        v = data.get(k)
        if isinstance(v, list) and v and all(isinstance(r, (dict, Mapping)) for r in v if r is not None):  # type: ignore[name-defined]
            return [dict(r) for r in v if isinstance(r, dict)]
    return []


# Python 3.9+ has collections.abc.Mapping; use a simpler check.
try:
    from collections.abc import Mapping
except Exception:  # pragma: no cover
    Mapping = dict  # type: ignore


class BackendClient:
    def __init__(self, base_url: str, token: str = ""):
        self.base_url = (base_url or "").strip().rstrip("/")
        self.token = (token or "").strip()

    def _headers(self) -> Dict[str, str]:
        h = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": f"TFB-PerfTracker/{SCRIPT_VERSION}",
        }
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
            h["X-APP-TOKEN"] = self.token
        return h

    async def post_json(
        self, path: str, payload: Dict[str, Any], timeout_sec: float = 30.0
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        url = f"{self.base_url}{path}"
        if not self.base_url:
            return None, "BACKEND_BASE_URL not set", 0

        # aiohttp fast path
        if ASYNC_HTTP_AVAILABLE and aiohttp is not None:
            try:
                t = aiohttp.ClientTimeout(total=timeout_sec)
                async with aiohttp.ClientSession(timeout=t, headers=self._headers()) as session:
                    async with session.post(url, json=payload) as resp:
                        code = int(resp.status)
                        raw = await resp.read()
                        if code != 200:
                            return None, f"HTTP {code}: {raw[:200]!r}", code
                        try:
                            return json_loads(raw), None, code
                        except Exception:
                            return None, "Non-JSON response", code
            except Exception as e:
                return None, str(e), 0

        # urllib fallback
        try:
            data = json_dumps(payload).encode("utf-8")
            req = UrlRequest(url, data=data, method="POST")
            for k, v in self._headers().items():
                req.add_header(k, v)
            with urlopen(req, timeout=timeout_sec) as resp:
                code = int(getattr(resp, "status", 200))
                raw = resp.read()
                if code != 200:
                    return None, f"HTTP {code}: {raw[:200]!r}", code
                return json_loads(raw), None, code
        except HTTPError as e:
            try:
                raw = e.read()
            except Exception:
                raw = b""
            return None, f"HTTPError {e.code}: {raw[:200]!r}", int(
                getattr(e, "code", 0) or 0
            )
        except URLError as e:
            return None, f"URLError: {e}", 0
        except Exception as e:
            return None, str(e), 0

    async def get_top10_rows(
        self, criteria_overrides: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        v6.9.0: tries the canonical endpoint chain in order, PREFERRING the
        advanced_analysis /sheet-rows route (full populated page, no 20s advisor
        bridge cap) and falling back to the advisor endpoints. Two fixes vs
        v6.4.0:
          (1) PER-ENDPOINT body — a /sheet-rows endpoint expects {"page": <page>};
              the advisor endpoints expect the criteria overrides.
          (2) EMPTY-200 FALL-THROUGH — a 200 carrying ZERO rows (e.g. the advisor
              bridge timing out to status="partial") no longer counts as success;
              it falls through to the next endpoint. Any error likewise falls
              through (the chain is heterogeneous, so a broken first endpoint
              must still reach the working fallback).
        Chain / page / timeout overridable via TRACK_TOP10_ENDPOINTS /
        TRACK_TOP10_PAGE / TRACK_TOP10_TIMEOUT_SEC.
        """
        criteria_body = dict(criteria_overrides or {})
        page = _top10_page()
        last_err: Optional[str] = None
        for endpoint in _top10_endpoints():
            # (1) choose the body matching this endpoint's contract.
            body: Dict[str, Any] = {"page": page} if endpoint.endswith("/sheet-rows") else criteria_body
            data, err, code = await self.post_json(endpoint, body, timeout_sec=_TOP10_FETCH_TIMEOUT_SEC)
            if isinstance(data, dict) and not err:
                rows = _extract_rows_from_envelope(data)
                if rows:
                    meta = dict(data.get("meta") or {})
                    meta["ok"] = True
                    meta["endpoint"] = endpoint
                    meta["count"] = len(rows)
                    return rows, meta
                # (2) 200 but no rows — record why, then try the next endpoint.
                dmeta = data.get("meta") or {}
                last_err = (
                    f"{endpoint}: 200/{data.get('status')} but 0 rows "
                    f"(outcome={dmeta.get('bridge_call_outcome')}, "
                    f"warnings={dmeta.get('warnings')})"
                )
                continue
            if err:
                # v6.9.0: fall through on ANY error (incl. non-404) so the
                # working fallback is still reached. (v6.4.0 returned here.)
                last_err = f"{endpoint}: {err}"
                continue
        return [], {"ok": False, "error": last_err or "no_data", "endpoint": None}

    async def get_rows_for_symbols(
        self, symbols: List[str], page: str
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """v6.16.0 (Fix #5): full engine rows for an EXPLICIT symbol list —
        the forced-coverage fetch for decision symbols outside the Top_10
        pool. Chunks of 25 (the dashboard sync's proven batch size) against
        TFB_TRACK_FORCE_ROWS_ENDPOINT; aggregates via the same envelope
        extractor as get_top10_rows. Best-effort: chunk errors are recorded
        in meta and the remaining chunks still run."""
        syms = [s.strip().upper() for s in (symbols or []) if s and str(s).strip()]
        syms = list(dict.fromkeys(syms))
        if not syms or not self.base_url:
            return [], {"ok": False, "error": "no_symbols_or_backend", "count": 0}
        endpoint = _force_rows_endpoint()
        rows: List[Dict[str, Any]] = []
        errs: List[str] = []
        for i in range(0, len(syms), 25):
            chunk = syms[i:i + 25]
            data, err, code = await self.post_json(
                endpoint, {"symbols": chunk, "page": page},
                timeout_sec=_TOP10_FETCH_TIMEOUT_SEC,
            )
            if err or not isinstance(data, dict):
                errs.append("%s..+%d: %s" % (chunk[0], len(chunk) - 1, err or "bad_payload"))
                continue
            rows.extend(_extract_rows_from_envelope(data))
        return rows, {
            "ok": bool(rows), "endpoint": endpoint, "count": len(rows),
            "requested": len(syms), "errors": errs or None,
        }

    async def fetch_prices(self, symbols: List[str]) -> Dict[str, float]:
        syms = [s.strip().upper() for s in (symbols or []) if s and str(s).strip()]
        syms = list(dict.fromkeys(syms))
        if not syms:
            return {}

        # v6.4.0: preferred canonical endpoint — /v1/enriched/quotes
        # (same one used by run_market_scan v5.3.0).
        data, err, code = await self.post_json(
            _QUOTES_ENDPOINT_PRIMARY,
            {"symbols": syms, "format": "items", "include_raw": False, "debug": False},
            timeout_sec=30.0,
        )
        if isinstance(data, dict) and code == 200 and not err:
            rows = _extract_rows_from_envelope(data)
            if rows:
                out: Dict[str, float] = {}
                for r in rows:
                    sym = _safe_str(r.get("symbol")).upper()
                    price = _safe_float(
                        r.get("current_price")
                        or r.get("price")
                        or r.get("last")
                        or r.get("last_price"),
                        default=0.0,
                    )
                    if sym and price > 0:
                        out[sym] = price
                if out:
                    return out
            # shape 2: {"data": {SYM: {...}}}
            blob = data.get("data") if isinstance(data.get("data"), dict) else None
            if isinstance(blob, dict):
                out = {}
                for k, v in blob.items():
                    if isinstance(v, dict):
                        p = (
                            v.get("current_price")
                            or v.get("price")
                            or v.get("last")
                            or v.get("last_price")
                        )
                        fv = _safe_float(p, default=0.0)
                        if fv > 0:
                            out[str(k).upper()] = fv
                if out:
                    return out

        # Legacy /quotes endpoint
        data, err, code = await self.post_json(
            _QUOTES_ENDPOINT_LEGACY,
            {"symbols": syms, "include_raw": False},
            timeout_sec=30.0,
        )
        if isinstance(data, dict) and code == 200 and not err:
            blob = data.get("data") if isinstance(data.get("data"), dict) else data
            if isinstance(blob, dict):
                out = {}
                for k, v in blob.items():
                    if isinstance(v, dict):
                        p = (
                            v.get("current_price")
                            or v.get("price")
                            or v.get("last")
                            or v.get("last_price")
                        )
                        fv = _safe_float(p, default=0.0)
                        if fv > 0:
                            out[str(k).upper()] = fv
                if out:
                    return out

        # /v1/enriched/sheet-rows fallback
        data, err, code = await self.post_json(
            _SHEET_ROWS_ENRICHED,
            {
                "page": "Global_Markets",
                "symbols": syms,
                "include_matrix": False,
                "top_n": len(syms),
            },
            timeout_sec=45.0,
        )
        if isinstance(data, dict) and code == 200 and not err:
            rows = _extract_rows_from_envelope(data)
            out = {}
            for r in rows:
                sym = _safe_str(r.get("symbol")).upper()
                price = _safe_float(
                    r.get("current_price") or r.get("price"), default=0.0
                )
                if sym and price > 0:
                    out[sym] = price
            if out:
                return out

        # /v1/analysis/sheet-rows fallback
        data, err, code = await self.post_json(
            _SHEET_ROWS_ANALYSIS,
            {
                "page": "Global_Markets",
                "symbols": syms,
                "include_matrix": False,
                "top_n": len(syms),
            },
            timeout_sec=45.0,
        )
        if isinstance(data, dict) and code == 200 and not err:
            rows = _extract_rows_from_envelope(data)
            out = {}
            for r in rows:
                sym = _safe_str(r.get("symbol")).upper()
                price = _safe_float(
                    r.get("current_price") or r.get("price"), default=0.0
                )
                if sym and price > 0:
                    out[sym] = price
            if out:
                return out

        return {}


# =============================================================================
# Store (Google Sheets) — best-effort, gspread-based
# =============================================================================
class PerformanceStore:
    SHEET_DEFAULT = "Performance_Log"
    START_ROW = 5  # headers at row 5, data from row 6
    DATA_ROW0 = 6

    HEADERS = [
        "Record ID",
        "Key",
        "Symbol",
        "Horizon",
        "Date Recorded (Riyadh)",
        "Entry Price",
        "Entry Recommendation",
        "Entry Score",
        "Risk Bucket",
        "Confidence",
        "Origin Tab",
        "Target Price",
        "Target ROI %",
        "Target Date (Riyadh)",
        "Status",
        "Current Price",
        "Unrealized ROI %",
        "Realized ROI %",
        "Outcome",
        "Volatility",
        "Max Drawdown %",
        "Sharpe Ratio",
        "Sector",
        "Factor Exposures",
        "Last Updated (Riyadh)",
        "Maturity Date",
        "Notes",
        # v6.6.0: investability-gate snapshot columns -- APPENDED at the END
        # so existing 27-column rows keep their positions (1-27) and load
        # unchanged (old rows read these four as blank). NEVER insert mid-list:
        # doing so would re-map every historical row when read back by name.
        "Entry Data Quality",
        "Entry Forecast Reliability",
        "Entry Investability",
        "Entry Final Action",
    ]

    def __init__(self, spreadsheet_id: str, sheet_name: str):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name or self.SHEET_DEFAULT
        self.backoff = FullJitterBackoff()
        self.cache: Dict[str, PerformanceRecord] = {}
        self.cache_lock = Lock()

        self.gc = None
        self.sheet = None
        self.ws = None
        self._init_sheet()

    def _load_sa_credentials_best_effort(self) -> Optional[Any]:
        # Accept GOOGLE_SHEETS_CREDENTIALS as JSON or base64(JSON)
        raw = (
            os.getenv("GOOGLE_SHEETS_CREDENTIALS")
            or os.getenv("GOOGLE_CREDENTIALS")
            or ""
        ).strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(
                    obj,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"],
                )
        except Exception:
            return None
        return None

    def _init_sheet(self) -> None:
        if not GSPREAD_AVAILABLE or gspread is None:
            return
        try:
            creds = self._load_sa_credentials_best_effort()
            if creds:
                self.gc = gspread.authorize(creds)
            else:
                # relies on local file/service account default
                self.gc = gspread.service_account()

            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            try:
                self.ws = self.sheet.worksheet(self.sheet_name)
            except Exception:
                self.ws = self.sheet.add_worksheet(
                    title=self.sheet_name, rows=2000, cols=80
                )

            self.backoff.execute_sync(self._ensure_headers)
        except Exception as e:
            logger.error("PerformanceStore init failed: %s", e)
            self.gc = None
            self.sheet = None
            self.ws = None

    def _ensure_headers(self) -> None:
        if not self.ws:
            return
        try:
            existing = self.ws.row_values(self.START_ROW)
        except Exception:
            existing = []
        existing_norm = [str(h).strip() for h in (existing or [])]
        header_missing = not existing_norm
        # v6.6.0: widen/repair the header row whenever it differs from the
        # canonical HEADERS by length OR content -- not only when cell A5
        # changes. v6.5.0 compared existing[0] != HEADERS[0] ("Record ID",
        # which never changes), so an existing 27-column Performance_Log would
        # NEVER gain the 4 v6.6.0 gate columns, and the new values written by
        # _record_to_row at positions 28-31 would be re-read under a stale
        # 27-name header map and dropped on the next load.
        header_mismatch = existing_norm != list(self.HEADERS)
        if header_missing or header_mismatch:
            end_col = len(self.HEADERS)
            rng = _a1_range(1, self.START_ROW, end_col, self.START_ROW)
            self.ws.update(values=[self.HEADERS], range_name=rng)

        # Seed the zero summary block + freeze ONLY on a brand-new sheet. On a
        # header WIDENING of an existing populated sheet we must NOT touch the
        # A1:E7 region or re-freeze -- that would wipe live KPIs. Widening the
        # header row (row 5) is safe: data lives at row 6+, columns 1-27 keep
        # their meaning, and the 4 appended columns are blank for old rows.
        if header_missing:
            summary = [
                ["Performance Summary", "", "", f"Generated: {RiyadhTime.format()}", ""],
                ["Total Records", "0", "", "", ""],
                ["Active Records", "0", "", "", ""],
                ["Matured Records", "0", "", "", ""],
                ["Win Rate", "0%", "", "", ""],
                ["Avg ROI", "0%", "", "", ""],
                ["Sharpe Ratio", "0", "", "", ""],
            ]
            try:
                self.ws.update(values=summary, range_name=_summary_block_range(7))
                self.ws.freeze(rows=self.START_ROW)
            except Exception:
                pass

    def is_available(self) -> bool:
        return bool(self.ws)

    def load_records(self, max_records: int = 10000) -> List[PerformanceRecord]:
        if not self.ws:
            return []
        end_row = self.DATA_ROW0 + max(0, int(max_records)) - 1
        end_col = len(self.HEADERS)
        rng = _a1_range(1, self.DATA_ROW0, end_col, end_row)

        def _load() -> List[List[Any]]:
            return self.ws.get(rng)  # type: ignore

        try:
            rows = self.backoff.execute_sync(_load)
            records: List[PerformanceRecord] = []
            with self.cache_lock:
                self.cache.clear()
                for r in rows or []:
                    if not r or not _safe_str(r[0]):
                        continue
                    padded = list(r) + [""] * max(0, len(self.HEADERS) - len(r))
                    rec = PerformanceRecord.from_sheet_row(padded, self.HEADERS)
                    if rec.symbol:
                        records.append(rec)
                        self.cache[rec.key] = rec
            return records
        except Exception as e:
            logger.error("Failed to load records: %s", e)
            return []

    def _record_to_row(self, r: PerformanceRecord) -> List[Any]:
        return [
            r.record_id,
            r.key,
            r.symbol,
            r.horizon.value,
            RiyadhTime.format(r.date_recorded),
            r.entry_price,
            r.entry_recommendation.value,
            r.entry_score,
            r.entry_risk_bucket,
            r.entry_confidence,
            r.origin_tab,
            r.target_price,
            r.target_roi,
            RiyadhTime.format(r.target_date),
            r.status.value,
            r.current_price,
            r.unrealized_roi,
            "" if r.realized_roi is None else r.realized_roi,
            r.outcome or "",
            r.volatility,
            r.max_drawdown,
            r.sharpe_ratio,
            r.sector or "",
            json_dumps(r.factor_exposures) if r.factor_exposures else "{}",
            RiyadhTime.format(r.last_updated.astimezone(_RIYADH_TZ)),
            RiyadhTime.format(r.maturity_date) if r.maturity_date else "",
            r.notes or "",
            # v6.6.0: investability-gate snapshot (positions 28-31; same order
            # as the four appended HEADERS entries).
            r.entry_data_quality,
            r.entry_forecast_reliability,
            r.entry_investability_status or "",
            r.entry_final_action or "",
        ]

    def save_records(self, records: List[PerformanceRecord]) -> bool:
        if not self.ws:
            return False
        end_col = len(self.HEADERS)

        data: List[List[Any]] = [self._record_to_row(r) for r in records]

        def _save() -> None:
            clear_rng = _a1_range(1, self.DATA_ROW0, end_col, 10000)
            self.ws.batch_clear([clear_rng])  # type: ignore
            if data:
                write_rng = _a1_range(
                    1, self.DATA_ROW0, end_col, self.DATA_ROW0 + len(data) - 1
                )
                self.ws.update(values=data, range_name=write_rng)  # type: ignore

        try:
            self.backoff.execute_sync(_save)
            with self.cache_lock:
                self.cache = {r.key: r for r in records}
            return True
        except Exception as e:
            logger.error("Failed to save records: %s", e)
            return False

    def append_records(self, records: List[PerformanceRecord]) -> bool:
        if not self.ws:
            return False
        with self.cache_lock:
            existing = set(self.cache.keys())
        new = [r for r in records if r.key not in existing]
        if not new:
            return True

        rows: List[List[Any]] = [self._record_to_row(r) for r in new]

        try:
            self.backoff.execute_sync(
                lambda: self.ws.append_rows(rows, value_input_option="RAW")  # type: ignore
            )
            with self.cache_lock:
                for r in new:
                    self.cache[r.key] = r
            return True
        except Exception as e:
            logger.error("Failed to append records: %s", e)
            return False

    def update_summary(self, summary: PerformanceSummary) -> bool:
        if not self.ws:
            return False
        block = [
            ["Performance Summary", "", "", f"Updated: {RiyadhTime.format()}", ""],
            ["Total Records", summary.total_records, "", "", ""],
            ["Active Records", summary.active_records, "", "", ""],
            ["Matured Records", summary.matured_records, "", "", ""],
            ["Wins", summary.wins, "", "", ""],
            ["Losses", summary.losses, "", "", ""],
            ["Win Rate", f"{summary.win_rate:.1f}%", "", "", ""],
            ["Avg ROI", f"{summary.avg_roi:.2f}%", "", "", ""],
            ["Median ROI", f"{summary.median_roi:.2f}%", "", "", ""],
            ["Best ROI", f"{summary.best_roi:.2f}%", "", "", ""],
            ["Worst ROI", f"{summary.worst_roi:.2f}%", "", "", ""],
            ["Sharpe Ratio", f"{summary.sharpe_ratio:.2f}", "", "", ""],
            ["Sortino Ratio", f"{summary.sortino_ratio:.2f}", "", "", ""],
        ]
        try:
            self.backoff.execute_sync(lambda: self.ws.update(values=block, range_name=_summary_block_range(13)))  # type: ignore
            return True
        except Exception:
            return False


# =============================================================================
# v6.7.0 — Signal_History store (Google Sheets) — best-effort, gspread-based
# =============================================================================
class SignalHistoryStore:
    """
    Best-effort gspread store for the day-to-day action-trend log. Mirrors
    PerformanceStore's auth + hardened-header pattern but uses a simple
    row-1 header / row-2 data layout (no KPI summary block). Self-contained:
    duplicates the small credential loader so this class adds NO change to
    PerformanceStore. Idempotent per symbol-per-day on append.
    """
    SHEET_DEFAULT = "Signal_History"
    START_ROW = 1   # headers at row 1, data from row 2
    DATA_ROW0 = 2

    HEADERS = [
        "Snapshot ID",
        "Key",
        "Symbol",
        "Date (Riyadh)",
        "Recorded At (Riyadh)",
        "Recommendation",
        "Final Action",
        "Investability",
        "Overall Score",
        "Forecast Reliability",
        "Data Quality",
        "Risk Score",
        "Price",
        "Origin Tab",
        # v6.14.0 (F4) event-context columns — header shape is permanent;
        # value population is gated by TRACK_EVENT_CONTEXT.
        "Analyst Rating",
        "Target Price",
        "Days To Earnings",
        "Days To ExDiv",
    ]

    def __init__(self, spreadsheet_id: str, sheet_name: str):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name or self.SHEET_DEFAULT
        self.backoff = FullJitterBackoff()
        self.cache_keys: set = set()
        self.cache_lock = Lock()

        self.gc = None
        self.sheet = None
        self.ws = None
        self._init_sheet()

    def _load_sa_credentials_best_effort(self) -> Optional[Any]:
        raw = (
            os.getenv("GOOGLE_SHEETS_CREDENTIALS")
            or os.getenv("GOOGLE_CREDENTIALS")
            or ""
        ).strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(
                    obj,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"],
                )
        except Exception:
            return None
        return None

    def _init_sheet(self) -> None:
        if not GSPREAD_AVAILABLE or gspread is None:
            return
        try:
            creds = self._load_sa_credentials_best_effort()
            if creds:
                self.gc = gspread.authorize(creds)
            else:
                self.gc = gspread.service_account()
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            try:
                self.ws = self.sheet.worksheet(self.sheet_name)
            except Exception:
                self.ws = self.sheet.add_worksheet(
                    title=self.sheet_name, rows=4000, cols=40
                )
            self.backoff.execute_sync(self._ensure_headers)
        except Exception as e:
            logger.error("SignalHistoryStore init failed: %s", e)
            self.gc = None
            self.sheet = None
            self.ws = None

    def _ensure_headers(self) -> None:
        if not self.ws:
            return
        try:
            existing = self.ws.row_values(self.START_ROW)
        except Exception:
            existing = []
        existing_norm = [str(h).strip() for h in (existing or [])]
        # Widen/repair the header row whenever it differs from canonical
        # HEADERS by length OR content (same discipline as PerformanceStore).
        if existing_norm != list(self.HEADERS):
            end_col = len(self.HEADERS)
            rng = _a1_range(1, self.START_ROW, end_col, self.START_ROW)
            self.ws.update(values=[self.HEADERS], range_name=rng)
            try:
                self.ws.freeze(rows=self.START_ROW)
            except Exception:
                pass

    def is_available(self) -> bool:
        return bool(self.ws)

    def load_snapshots(self, max_records: int = 20000) -> List[SignalSnapshot]:
        if not self.ws:
            return []
        end_row = self.DATA_ROW0 + max(0, int(max_records)) - 1
        end_col = len(self.HEADERS)
        rng = _a1_range(1, self.DATA_ROW0, end_col, end_row)

        def _load() -> List[List[Any]]:
            return self.ws.get(rng)  # type: ignore

        try:
            rows = self.backoff.execute_sync(_load)
        except Exception as e:
            logger.error("Failed to load signal snapshots: %s", e)
            return []
        out: List[SignalSnapshot] = []
        with self.cache_lock:
            self.cache_keys.clear()
            for r in rows or []:
                if not r or not _safe_str(r[0]):
                    continue
                padded = list(r) + [""] * max(0, len(self.HEADERS) - len(r))
                snap = SignalSnapshot.from_sheet_row(padded, self.HEADERS)
                if snap.symbol:
                    out.append(snap)
                    self.cache_keys.add(snap.key)
        return out

    def _snapshot_to_row(self, s: SignalSnapshot) -> List[Any]:
        return [
            s.snapshot_id,
            s.key,
            s.symbol,
            RiyadhTime.format(s.date_recorded, fmt="%Y-%m-%d"),
            RiyadhTime.format(s.recorded_at.astimezone(_RIYADH_TZ)),
            s.recommendation.value,
            s.final_action or "",
            s.investability_status or "",
            s.overall_score,
            s.forecast_reliability,
            s.data_quality,
            s.risk_score,
            s.price,
            s.origin_tab or "",
            s.analyst_rating or "",
            s.target_price if s.target_price else "",
            s.days_to_earnings if s.days_to_earnings is not None else "",
            s.days_to_exdiv if s.days_to_exdiv is not None else "",
        ]

    def append_snapshots(self, snaps: List[SignalSnapshot]) -> int:
        """Append only snapshots whose key isn't already present (idempotent
        per symbol-per-day). Returns the count actually written."""
        if not self.ws:
            return 0
        with self.cache_lock:
            existing = set(self.cache_keys)
        seen_now: set = set()
        new: List[SignalSnapshot] = []
        for s in snaps:
            if s.key in existing or s.key in seen_now:
                continue
            seen_now.add(s.key)
            new.append(s)
        if not new:
            return 0
        rows = [self._snapshot_to_row(s) for s in new]
        try:
            self.backoff.execute_sync(
                lambda: self.ws.append_rows(rows, value_input_option="RAW")  # type: ignore
            )
            with self.cache_lock:
                for s in new:
                    self.cache_keys.add(s.key)
            return len(new)
        except Exception as e:
            logger.error("Failed to append signal snapshots: %s", e)
            return 0


def _trend_day_keyed_enabled() -> bool:
    """v6.19.0 (Fix TR-1): day-keyed trend windows + intraday-aware flips.
    Default ON; TFB_TREND_DAY_KEYED=0/false/off restores the v6.18.0
    snapshot-window analyzer byte-identically."""
    return (os.getenv("TFB_TREND_DAY_KEYED") or "1").strip().lower() in (
        "1", "true", "yes", "on")


class SignalTrendStore:
    """
    v6.13.0 — Best-effort gspread store that SURFACES the day-to-day
    action-trend trajectory (already computed by SignalTrendAnalyzer via
    _compute_signal_trends) to a user-visible Signal_Trends tab. Mirrors
    SignalHistoryStore's auth + hardened-header pattern (row-1 header /
    row-2 data) and, like it, duplicates the small credential loader so
    this class adds NO change to PerformanceStore or SignalHistoryStore.

    WHY THIS EXISTS: the trajectory the engine computes every cycle
    (action_direction STRENGTHENING/WEAKENING/STABLE/FLIPPED, days in the
    current action, score slope, flip count, stability) was only ever
    written to an EPHEMERAL local <base>_signal_trends.json that the
    GitHub Actions runner discards at job end -- and only under --export,
    which the daily cron does not pass. So the trajectory never reached
    the user. This store persists that SAME, already-computed trajectory
    -- no new computation, NO recommendation/score/gate/reliability change
    -- to a tab the user can read.

    Unlike SignalHistoryStore (append-only daily snapshots, idempotent
    per symbol-per-day), trends are a CURRENT-STATE view: one row per
    decision symbol, recomputed each cycle. write_trends is therefore a
    bounded REPLACE-ALL (overwrite the data region, clear any trailing
    stale rows) so the tab always shows each symbol's latest trajectory.

    Fail-open: any Sheets hiccup logs and returns 0; it must NEVER break
    --record / --audit / --analyze.
    """
    SHEET_DEFAULT = "Signal_Trends"
    START_ROW = 1   # headers at row 1, data from row 2
    DATA_ROW0 = 2
    SCAN_ROWS = 2000  # bounded existing-row probe for trailing-tail clear

    HEADERS = [
        "Symbol",
        "Recorded At (Riyadh)",
        "Current Action",
        "Direction",                 # STRENGTHENING/WEAKENING/STABLE/FLIPPED/NEW
        "Days In Action",
        ("Score Slope (pts/day)" if _trend_day_keyed_enabled()
         else "Score Slope (pts/snap)"),
        "Stability",                 # STABLE/DRIFTING/CHOPPY/NEW
        "Latest Score",
        "Investability",
        "Flip Count",
        "Distinct Actions",
        "Observations",
        "Window",
        "Latest Date",
        "Summary",
    ]

    def __init__(self, spreadsheet_id: str, sheet_name: str):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name or self.SHEET_DEFAULT
        self.backoff = FullJitterBackoff()
        self.gc = None
        self.sheet = None
        self.ws = None
        self._init_sheet()

    def _load_sa_credentials_best_effort(self) -> Optional[Any]:
        raw = (
            os.getenv("GOOGLE_SHEETS_CREDENTIALS")
            or os.getenv("GOOGLE_CREDENTIALS")
            or ""
        ).strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(
                    obj,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"],
                )
        except Exception:
            return None
        return None

    def _init_sheet(self) -> None:
        if not GSPREAD_AVAILABLE or gspread is None:
            return
        try:
            creds = self._load_sa_credentials_best_effort()
            if creds:
                self.gc = gspread.authorize(creds)
            else:
                self.gc = gspread.service_account()
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            try:
                self.ws = self.sheet.worksheet(self.sheet_name)
            except Exception:
                self.ws = self.sheet.add_worksheet(
                    title=self.sheet_name, rows=4000, cols=40
                )
            self.backoff.execute_sync(self._ensure_headers)
        except Exception as e:
            logger.error("SignalTrendStore init failed: %s", e)
            self.gc = None
            self.sheet = None
            self.ws = None

    def _ensure_headers(self) -> None:
        if not self.ws:
            return
        try:
            existing = self.ws.row_values(self.START_ROW)
        except Exception:
            existing = []
        existing_norm = [str(h).strip() for h in (existing or [])]
        if existing_norm != list(self.HEADERS):
            end_col = len(self.HEADERS)
            rng = _a1_range(1, self.START_ROW, end_col, self.START_ROW)
            self.ws.update(values=[self.HEADERS], range_name=rng)
            try:
                self.ws.freeze(rows=self.START_ROW)
            except Exception:
                pass

    def is_available(self) -> bool:
        return bool(self.ws)

    @staticmethod
    def _num2(v: Any) -> Any:
        try:
            return round(float(v), 2)
        except Exception:
            return v

    @staticmethod
    def _int0(v: Any) -> int:
        try:
            return int(v or 0)
        except Exception:
            return 0

    def _trend_to_row(self, t: "SignalTrend") -> List[Any]:
        return [
            t.symbol,
            RiyadhTime.format(),
            t.current_action or "",
            t.action_direction or "",
            self._int0(t.days_in_current_action),
            self._num2(t.score_slope),
            t.stability or "",
            self._num2(t.latest_score),
            t.current_investability or "",
            self._int0(t.flip_count),
            self._int0(t.distinct_actions),
            self._int0(t.n_observations),
            self._int0(t.window),
            t.latest_date or "",
            t.summary_label or "",
        ]

    def write_trends(self, trends: List["SignalTrend"]) -> int:
        """Bounded replace-all: overwrite the data region with the current
        per-symbol trajectories and clear any trailing stale rows. Returns
        the number of trajectory rows written. Fail-open."""
        if not self.ws:
            return 0
        try:
            rows = [
                self._trend_to_row(t)
                for t in (trends or [])
                if getattr(t, "symbol", "")
            ]
        except Exception as e:
            logger.error("SignalTrendStore row build failed: %s", e)
            return 0
        end_col = len(self.HEADERS)
        # best-effort count of existing data rows so a SMALLER new set does
        # not leave a stale tail behind (replace-all semantics).
        old_n = 0
        try:
            col_a = self.backoff.execute_sync(
                lambda: self.ws.get(
                    _a1_range(1, self.DATA_ROW0, 1,
                              self.DATA_ROW0 + self.SCAN_ROWS - 1)
                )
            )
            old_n = len([r for r in (col_a or []) if r and _safe_str(r[0])])
        except Exception:
            old_n = 0
        try:
            if rows:
                rng = _a1_range(1, self.DATA_ROW0, end_col,
                                self.DATA_ROW0 + len(rows) - 1)
                self.backoff.execute_sync(
                    lambda: self.ws.update(
                        values=rows, range_name=rng, value_input_option="RAW"
                    )
                )
            if old_n > len(rows):
                clr = _a1_range(1, self.DATA_ROW0 + len(rows), end_col,
                                self.DATA_ROW0 + old_n - 1)
                try:
                    self.backoff.execute_sync(lambda: self.ws.batch_clear([clr]))
                except Exception:
                    pass
            return len(rows)
        except Exception as e:
            logger.error("Failed to write signal trends: %s", e)
            return 0


# =============================================================================
# Analytics
# =============================================================================
class RiskCalculator:
    def __init__(self, risk_free_rate: float = 0.02):
        self.rfr = float(risk_free_rate)

    def sharpe_ratio(self, returns: List[float]) -> float:
        if not returns:
            return 0.0
        if not NUMPY_AVAILABLE or np is None:
            return 0.0
        arr = np.asarray(returns, dtype=float)
        if arr.size < 2:
            return 0.0
        if float(np.std(arr)) == 0.0:
            return 0.0
        excess = arr - (self.rfr / 252.0)
        return float(np.mean(excess) / np.std(arr) * math.sqrt(252))

    def sortino_ratio(self, returns: List[float]) -> float:
        if not returns:
            return 0.0
        if not NUMPY_AVAILABLE or np is None:
            return 0.0
        arr = np.asarray(returns, dtype=float)
        excess = arr - (self.rfr / 252.0)
        downside = arr[arr < 0]
        if downside.size < 2:
            return 0.0
        if float(np.std(downside)) == 0.0:
            return 0.0
        return float(np.mean(excess) / np.std(downside) * math.sqrt(252))


class MonteCarloSimulator:
    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        if NUMPY_AVAILABLE and np is not None and seed is not None:
            np.random.seed(seed)

    def simulate_win_rate(
        self, outcomes: List[bool], iterations: int = 10000, confidence: float = 0.95
    ) -> Dict[str, float]:
        if not outcomes or not NUMPY_AVAILABLE or np is None:
            return {
                "mean": 0.0, "median": 0.0, "std": 0.0,
                "ci_lower": 0.0, "ci_upper": 0.0, "observed": 0.0,
            }
        n = len(outcomes)
        observed = sum(1 for x in outcomes if x) / n
        sims = np.array(
            [
                np.mean(np.random.choice(outcomes, size=n, replace=True))
                for _ in range(max(100, int(iterations)))
            ],
            dtype=float,
        )
        lo = float(np.percentile(sims, ((1 - confidence) / 2) * 100))
        hi = float(np.percentile(sims, ((1 + confidence) / 2) * 100))
        return {
            "mean": float(np.mean(sims)),
            "median": float(np.median(sims)),
            "std": float(np.std(sims)),
            "ci_lower": lo,
            "ci_upper": hi,
            "observed": float(observed),
        }


class AttributionAnalyzer:
    def by_sector(
        self, records: List[PerformanceRecord]
    ) -> Dict[str, Dict[str, float]]:
        bucket: Dict[str, Dict[str, float]] = {}
        for r in records:
            if not r.sector or r.realized_roi is None:
                continue
            sec = r.sector
            if sec not in bucket:
                bucket[sec] = {"wins": 0.0, "losses": 0.0, "total_roi": 0.0, "count": 0.0}
            bucket[sec]["count"] += 1.0
            bucket[sec]["total_roi"] += float(r.realized_roi)
            if (r.realized_roi or 0.0) > 0:
                bucket[sec]["wins"] += 1.0
            elif (r.realized_roi or 0.0) < 0:
                bucket[sec]["losses"] += 1.0
        out: Dict[str, Dict[str, float]] = {}
        for sec, d in bucket.items():
            cnt = max(1.0, d["count"])
            wins = d["wins"]
            losses = d["losses"]
            out[sec] = {
                "win_rate": (wins / max(1.0, wins + losses)) * 100.0,
                "avg_roi": d["total_roi"] / cnt,
                "wins": wins,
                "losses": losses,
                "count": cnt,
            }
        return out


class PerformanceAnalyzer:
    def __init__(self):
        self.risk = RiskCalculator()
        self.sim = MonteCarloSimulator(seed=42)
        self.attr = AttributionAnalyzer()

    def analyze(self, records: List[PerformanceRecord]) -> PerformanceSummary:
        s = PerformanceSummary(total_records=len(records))
        s.active_records = sum(
            1 for r in records if r.status == PerformanceStatus.ACTIVE
        )
        matured = [
            r
            for r in records
            if r.status == PerformanceStatus.MATURED and r.realized_roi is not None
        ]
        s.matured_records = len(matured)

        if matured:
            rois = [float(r.realized_roi or 0.0) for r in matured]
            wins = sum(1 for r in matured if (r.realized_roi or 0.0) > 0)
            losses = sum(1 for r in matured if (r.realized_roi or 0.0) < 0)
            breakeven = len(matured) - wins - losses
            s.wins, s.losses, s.breakeven = wins, losses, breakeven
            total_outcomes = wins + losses
            s.win_rate = (wins / total_outcomes * 100.0) if total_outcomes > 0 else 0.0

            if NUMPY_AVAILABLE and np is not None:
                arr = np.asarray(rois, dtype=float)
                s.avg_roi = float(np.mean(arr))
                s.median_roi = float(np.median(arr))
                s.roi_std = float(np.std(arr))
                s.best_roi = float(np.max(arr))
                s.worst_roi = float(np.min(arr))
            else:
                rois_sorted = sorted(rois)
                s.avg_roi = sum(rois) / max(1, len(rois))
                s.median_roi = rois_sorted[len(rois_sorted) // 2]
                s.best_roi = max(rois_sorted)
                s.worst_roi = min(rois_sorted)

            if len(rois) > 2:
                s.sharpe_ratio = self.risk.sharpe_ratio(rois)
                s.sortino_ratio = self.risk.sortino_ratio(rois)

            groups: Dict[str, List[PerformanceRecord]] = defaultdict(list)
            for r in matured:
                groups[r.horizon.value].append(r)
            for h, grp in groups.items():
                w = sum(1 for r in grp if (r.realized_roi or 0.0) > 0)
                t = len(grp)
                s.hit_rate_by_horizon[h] = (w / t * 100.0) if t else 0.0

            s.performance_by_sector = self.attr.by_sector(matured)

            # v6.6.0: segment matured outcomes by the ENTRY investability
            # verdict so the operator can answer the real question behind
            # "95%": does an INVESTABLE pick actually outperform a WATCHLIST
            # one? Buckets are the gate's own vocabulary (INVESTABLE /
            # WATCHLIST / BLOCKED); records recorded before v6.6.0 (blank gate
            # field) fall under "UNSPECIFIED" and are kept separate so they
            # never dilute a gate bucket. win_rate is decided-only
            # (wins / (wins + losses)), matching the overall win_rate.
            inv_groups: Dict[str, List[PerformanceRecord]] = defaultdict(list)
            for r in matured:
                bucket = (r.entry_investability_status or "UNSPECIFIED").upper()
                inv_groups[bucket].append(r)
            for bucket, grp in inv_groups.items():
                gw = sum(1 for r in grp if (r.realized_roi or 0.0) > 0)
                gl = sum(1 for r in grp if (r.realized_roi or 0.0) < 0)
                decided = gw + gl
                grp_rois = [float(r.realized_roi or 0.0) for r in grp]
                wr = (gw / decided * 100.0) if decided else 0.0
                s.hit_rate_by_investability[bucket] = wr
                s.performance_by_investability[bucket] = {
                    "win_rate": wr,
                    "avg_roi": (sum(grp_rois) / len(grp_rois)) if grp_rois else 0.0,
                    "wins": float(gw),
                    "losses": float(gl),
                    "count": float(len(grp)),
                }

            try:
                perf_win_rate.set(float(s.win_rate))
            except Exception:
                pass

        return s


# =============================================================================
# v6.7.0 — Action-trend analyzer (day-to-day verdict trajectory)
# =============================================================================
class SignalTrendAnalyzer:
    """
    Turns a flat SignalSnapshot history into per-symbol day-to-day
    trajectories. DESCRIPTIVE ONLY -- characterises how the engine's verdict
    moved; predicts nothing. Tier ranks follow the canonical ordering
    STRONG_BUY > BUY > ACCUMULATE > HOLD > REDUCE > SELL > STRONG_SELL >
    AVOID; HOLD is the neutral midline used for side-flip detection.
    """
    _RANK: Dict[str, int] = {
        RecommendationType.STRONG_BUY.value: 8,
        RecommendationType.BUY.value: 7,
        RecommendationType.ACCUMULATE.value: 6,
        RecommendationType.HOLD.value: 5,
        RecommendationType.REDUCE.value: 4,
        RecommendationType.SELL.value: 3,
        RecommendationType.STRONG_SELL.value: 2,
        RecommendationType.AVOID.value: 1,
    }
    _NEUTRAL = 5  # HOLD rank

    def _rank(self, rec_value: str) -> int:
        return self._RANK.get(rec_value, self._NEUTRAL)

    def _side(self, rec_value: str) -> int:
        r = self._rank(rec_value) - self._NEUTRAL
        return 1 if r > 0 else (-1 if r < 0 else 0)

    def analyze(
        self, snapshots: List[SignalSnapshot], window: int = 5
    ) -> List[SignalTrend]:
        window = max(2, int(window))
        by_sym: Dict[str, List[SignalSnapshot]] = defaultdict(list)
        for s in snapshots:
            if s.symbol:
                by_sym[s.symbol].append(s)

        trends: List[SignalTrend] = []
        _day_keyed = _trend_day_keyed_enabled()
        for sym, snaps in by_sym.items():
            snaps_sorted = sorted(snaps, key=lambda x: x.date_recorded)
            n = len(snaps_sorted)
            latest = snaps_sorted[-1]

            if _day_keyed:
                # v6.19.0 (Fix TR-1): Signal_History carries multiple
                # same-day rows per key on the live sheet, so a
                # snapshot-count window spans HOURS, not days, and hides
                # multi-hour flips (NMM.US 2026-07-13: Flip Count 0 on a
                # BUY->SELL whipsaw). Collapse to ONE representative per
                # Riyadh day -- the day's LAST snapshot, the standing
                # end-of-day verdict -- so `window` means DAYS.
                by_day: Dict[str, SignalSnapshot] = {}
                day_order: List[str] = []
                for s in snaps_sorted:
                    dk = RiyadhTime.format(s.date_recorded, fmt="%Y-%m-%d")
                    if dk not in by_day:
                        day_order.append(dk)
                    by_day[dk] = s  # last write wins = last snapshot of day
                day_reps = [by_day[d] for d in day_order]
                win = day_reps[-window:]
                win_days = set(
                    RiyadhTime.format(s.date_recorded, fmt="%Y-%m-%d")
                    for s in win
                )
                recs = [s.recommendation.value for s in win]
                scores = [float(s.overall_score) for s in win]

                # consecutive trailing DAYS the latest action has held.
                days_in = 1
                for prev in reversed(day_reps[:-1]):
                    if prev.recommendation.value == latest.recommendation.value:
                        days_in += 1
                    else:
                        break

                distinct = len(set(recs))
                # Intraday-aware flip count: EVERY snapshot inside the
                # window's day span, chronological, so a same-day
                # bull->bear crossing is counted, never hidden by the
                # day collapse.
                _win_snaps = [
                    s for s in snaps_sorted
                    if RiyadhTime.format(s.date_recorded, fmt="%Y-%m-%d")
                    in win_days
                ]
                sides = [self._side(s.recommendation.value)
                         for s in _win_snaps]
                nonzero = [x for x in sides if x != 0]
                flip_count = sum(
                    1 for i in range(1, len(nonzero))
                    if nonzero[i] != nonzero[i - 1]
                )
            else:
                win = snaps_sorted[-window:]
                recs = [s.recommendation.value for s in win]
                scores = [float(s.overall_score) for s in win]

                # snapshots (days, at the daily cadence) the latest action has
                # held, counting back from the most recent.
                days_in = 1
                for prev in reversed(snaps_sorted[:-1]):
                    if prev.recommendation.value == latest.recommendation.value:
                        days_in += 1
                    else:
                        break

                distinct = len(set(recs))
                sides = [self._side(r) for r in recs]
                nonzero = [x for x in sides if x != 0]
                flip_count = sum(
                    1 for i in range(1, len(nonzero)) if nonzero[i] != nonzero[i - 1]
                )

            # slope of overall_score over the window (least squares if numpy,
            # else endpoint slope) -- points per snapshot.
            slope = 0.0
            if len(scores) >= 2:
                if NUMPY_AVAILABLE and np is not None:
                    xs = np.arange(len(scores), dtype=float)
                    ys = np.asarray(scores, dtype=float)
                    try:
                        slope = float(np.polyfit(xs, ys, 1)[0])
                    except Exception:
                        slope = (scores[-1] - scores[0]) / (len(scores) - 1)
                else:
                    slope = (scores[-1] - scores[0]) / (len(scores) - 1)

            if n < 2:
                direction = "NEW"
            elif distinct == 1:
                direction = "STABLE"
            elif flip_count > 0:
                direction = "FLIPPED"
            else:
                net = self._rank(recs[-1]) - self._rank(recs[0])
                direction = (
                    "STRENGTHENING" if net > 0
                    else "WEAKENING" if net < 0
                    else "STABLE"
                )

            if n < 2:
                stability = "NEW"
            elif flip_count >= 1:
                stability = "CHOPPY"
            elif distinct == 1:
                stability = "STABLE"
            else:
                stability = "DRIFTING"

            arrow = "↑" if slope > 0.05 else ("↓" if slope < -0.05 else "→")
            label = (
                f"{latest.recommendation.value} · "
                f"{direction.lower()} {days_in}d · score {arrow}"
            )

            trends.append(
                SignalTrend(
                    symbol=sym,
                    n_observations=n,
                    window=min(window, len(day_reps)) if _day_keyed else min(window, n),
                    current_action=latest.recommendation.value,
                    current_investability=latest.investability_status,
                    latest_score=float(latest.overall_score),
                    latest_date=RiyadhTime.format(latest.date_recorded, fmt="%Y-%m-%d"),
                    days_in_current_action=days_in,
                    action_direction=direction,
                    score_slope=round(slope, 4),
                    distinct_actions=distinct,
                    flip_count=flip_count,
                    stability=stability,
                    summary_label=label,
                )
            )

        # newest activity first, then symbol
        trends.sort(key=lambda t: (t.latest_date, t.symbol), reverse=True)
        return trends


# =============================================================================
# v6.8.0 — Reliability calibrator (Measure step of the self-correction loop)
# =============================================================================
class ReliabilityCalibrator:
    """
    Measures whether the engine's STATED reliability / investability verdict
    actually tracks REALIZED outcomes, with honest small-sample uncertainty.
    Pure compute over matured PerformanceRecords (reused from the cycle's
    already-loaded rows). DISCLOSES the gap; never auto-tunes anything.
    """
    # (lo_inclusive, hi_exclusive, label) over stated reliability 0..100
    RELIABILITY_BANDS: List[Tuple[float, float, str]] = [
        (0.0, 50.0, "0-50"),
        (50.0, 70.0, "50-70"),
        (70.0, 85.0, "70-85"),
        (85.0, 100.01, "85-100"),
    ]

    _ADVISORY = (
        "Calibration factors are ADVISORY and NOT applied to any "
        "recommendation, score, gate, or stated reliability. They treat mean "
        "stated reliability as an implied win-probability (the engine does not "
        "strictly define it that way) and are shrunk toward 1.0 by sample size. "
        "Read them as 'how optimistic was the stated confidence vs outcomes,' "
        "not a precise correction. Applying a haircut is a separate, default-OFF, "
        "engine-side phase."
    )

    def __init__(self, min_sample: int = 10, shrink_k: int = 20):
        self.min_sample = max(1, int(min_sample))
        self.shrink_k = max(1, int(shrink_k))

    def _bucket(self, name: str, recs: List[PerformanceRecord]) -> CalibrationBucket:
        n = len(recs)
        rois = [
            _safe_float(r.realized_roi, default=0.0)
            for r in recs if r.realized_roi is not None
        ]
        wins = sum(1 for v in rois if v > 0.0)
        losses = sum(1 for v in rois if v < 0.0)
        breakeven = len(rois) - wins - losses
        decided = wins + losses
        wr = (wins / decided * 100.0) if decided else 0.0
        lo, hi = wilson_interval(wins, decided)
        mean_roi = (sum(rois) / len(rois)) if rois else 0.0
        rels = [_safe_float(r.entry_forecast_reliability, default=0.0) for r in recs]
        mean_rel = (sum(rels) / len(rels)) if rels else 0.0
        claimed_wr = mean_rel  # stated reliability is already on a 0..100 scale

        if decided >= 1 and claimed_wr > 0.0:
            raw = max(0.1, min(2.0, wr / claimed_wr))
        else:
            raw = 1.0
        shrunk = round(shrink_factor(raw, decided, self.shrink_k), 3)

        sufficient = decided >= self.min_sample
        note = "" if sufficient else f"insufficient (decided={decided} < {self.min_sample})"
        return CalibrationBucket(
            name=name, n=n, decided=decided, wins=wins, losses=losses,
            breakeven=breakeven, realized_win_rate=round(wr, 1),
            ci_low=lo, ci_high=hi, mean_realized_roi=round(mean_roi, 2),
            mean_claimed_reliability=round(mean_rel, 1),
            claimed_win_rate=round(claimed_wr, 1),
            calibration_factor_raw=round(raw, 3),
            calibration_factor_shrunk=shrunk,
            sufficient=sufficient, note=note,
        )

    def _brier(self, matured: List[PerformanceRecord]) -> Optional[float]:
        pts: List[float] = []
        for r in matured:
            v = r.realized_roi
            if v is None or v == 0.0:
                continue  # decided-only
            p = max(0.0, min(1.0, _safe_float(r.entry_forecast_reliability, 0.0) / 100.0))
            o = 1.0 if v > 0.0 else 0.0
            pts.append((p - o) ** 2)
        return round(sum(pts) / len(pts), 4) if pts else None

    def _discrimination(self, inv: Optional[CalibrationBucket],
                        watch: Optional[CalibrationBucket]) -> str:
        if inv is None or watch is None:
            return "INVESTABLE/WATCHLIST discrimination: one or both buckets absent."
        if not inv.sufficient or not watch.sufficient:
            return (
                "INVESTABLE/WATCHLIST discrimination: insufficient sample "
                f"(INVESTABLE decided={inv.decided}, WATCHLIST decided={watch.decided})."
            )
        if inv.ci_low > watch.ci_high:
            return (
                f"INVESTABLE significantly outperforms WATCHLIST "
                f"(INVESTABLE {inv.realized_win_rate}% CI[{inv.ci_low},{inv.ci_high}] "
                f"vs WATCHLIST {watch.realized_win_rate}% CI[{watch.ci_low},{watch.ci_high}])."
            )
        if watch.ci_low > inv.ci_high:
            return (
                "WARNING: WATCHLIST outperforms INVESTABLE in this sample -- the "
                "gate is inverted here; investigate before trusting the split."
            )
        return (
            f"INVESTABLE vs WATCHLIST not yet statistically distinguishable "
            f"(CIs overlap: INVESTABLE[{inv.ci_low},{inv.ci_high}] "
            f"WATCHLIST[{watch.ci_low},{watch.ci_high}])."
        )

    def calibrate(self, records: List[PerformanceRecord],
                  signal_mix: Optional[Dict[str, int]] = None) -> CalibrationReport:
        matured = [
            r for r in records
            if r.status == PerformanceStatus.MATURED and r.realized_roi is not None
        ]
        total = len(records)
        decided_total = sum(1 for r in matured if (r.realized_roi or 0.0) != 0.0)
        global_sufficient = decided_total >= self.min_sample

        inv_groups: Dict[str, List[PerformanceRecord]] = defaultdict(list)
        for r in matured:
            inv_groups[(r.entry_investability_status or "UNSPECIFIED").upper()].append(r)
        by_inv = [self._bucket(k, v) for k, v in sorted(inv_groups.items())]

        by_band: List[CalibrationBucket] = []
        for lo, hi, label in self.RELIABILITY_BANDS:
            grp = [
                r for r in matured
                if lo <= _safe_float(r.entry_forecast_reliability, 0.0) < hi
            ]
            by_band.append(self._bucket(label, grp))

        rec_groups: Dict[str, List[PerformanceRecord]] = defaultdict(list)
        for r in matured:
            rec_groups[r.entry_recommendation.value].append(r)
        by_rec = [self._bucket(k, v) for k, v in sorted(rec_groups.items())]

        hz_groups: Dict[str, List[PerformanceRecord]] = defaultdict(list)
        for r in matured:
            hz_groups[r.horizon.value].append(r)
        by_hz = [self._bucket(k, v) for k, v in sorted(hz_groups.items())]

        ordered = [b for b in by_band if b.decided > 0]
        monotonic: Optional[bool] = None
        if len(ordered) >= 2:
            monotonic = all(
                ordered[i].realized_win_rate <= ordered[i + 1].realized_win_rate + 1e-9
                for i in range(len(ordered) - 1)
            )

        inv_map = {b.name: b for b in by_inv}
        disc = self._discrimination(inv_map.get("INVESTABLE"), inv_map.get("WATCHLIST"))
        brier = self._brier(matured)

        # v6.12.0: Information Coefficient (disclosure-only, env-gated default OFF).
        # Rank-correlation of the continuous entry_score against realized_roi over
        # DECIDED records -- a more data-efficient "does the score carry signal?"
        # measure than the binary band-monotonicity flag, using every decided pick
        # rather than only STRONG-labelled ones. Never feeds any recommendation,
        # score, gate, or reliability number.
        ic_val: Optional[float] = None
        ic_t: Optional[float] = None
        ic_p: Optional[float] = None
        ic_n: int = 0
        if (_IC_AVAILABLE and _information_coefficient is not None
                and _env_bool("TFB_HARNESS_IC", False)):
            _ic_scores: List[float] = []
            _ic_rets: List[float] = []
            for r in matured:
                rr = r.realized_roi
                if rr is None or rr == 0.0:
                    continue  # decided-only, mirroring _brier
                _ic_scores.append(_safe_float(r.entry_score, 0.0))
                _ic_rets.append(_safe_float(rr, 0.0))
            if len(_ic_scores) >= 3:
                try:
                    _ic_res = _information_coefficient(_ic_scores, _ic_rets)
                    _raw = _ic_res.get("ic")
                    if isinstance(_raw, (int, float)) and _raw == _raw:  # finite, NaN-safe
                        ic_val = round(float(_raw), 4)
                        _t = _ic_res.get("t_stat")
                        _p = _ic_res.get("p_value")
                        ic_t = (round(float(_t), 3)
                                if isinstance(_t, (int, float)) and _t == _t else None)
                        ic_p = (round(float(_p), 4)
                                if isinstance(_p, (int, float)) and _p == _p else None)
                        ic_n = int(_ic_res.get("n", len(_ic_scores)) or 0)
                except Exception:
                    ic_val = ic_t = ic_p = None
                    ic_n = 0

        if not matured:
            headline = (
                "INSUFFICIENT EVIDENCE -- 0 matured records. The engine's stated "
                "reliability remains an unbacktested estimate; start the clock "
                "(run --record daily, let picks mature)."
            )
        elif not global_sufficient:
            headline = (
                f"INSUFFICIENT EVIDENCE -- {decided_total} decided outcome(s) "
                f"(need >= {self.min_sample}). Point estimates suppressed where "
                f"thin; intervals reflect the (wide) uncertainty."
            )
        else:
            headline = (
                f"Calibration on {decided_total} decided outcome(s) across "
                f"{len(matured)} matured record(s)."
            )

        return CalibrationReport(
            generated_riyadh=RiyadhTime.format(),
            total_records=total,
            matured_records=len(matured),
            decided_records=decided_total,
            min_sample=self.min_sample,
            shrinkage_k=self.shrink_k,
            global_sufficient=global_sufficient,
            headline=headline,
            brier_score=brier,
            brier_baseline=0.25,
            by_investability=by_inv,
            by_reliability_band=by_band,
            by_recommendation=by_rec,
            by_horizon=by_hz,
            monotonic_reliability=monotonic,
            discrimination_note=disc,
            information_coefficient=ic_val,
            ic_t_stat=ic_t,
            ic_p_value=ic_p,
            ic_n=ic_n,
            signal_trend_mix=dict(signal_mix or {}),
            advisory_note=self._ADVISORY,
        )


def _fmt_cal_bucket(b: CalibrationBucket) -> str:
    if b.decided == 0:
        return f"  {b.name:<14} n={b.n:<3} no decided outcomes yet"
    point = f"{b.realized_win_rate:5.1f}%" if b.sufficient else "  (thin)"
    star = "" if b.sufficient else "  <-- insufficient"
    return (
        f"  {b.name:<14} n={b.n:<3} decided={b.decided:<3} "
        f"win={point} CI[{b.ci_low:.0f},{b.ci_high:.0f}] "
        f"claim~{b.claimed_win_rate:4.0f}% roi={b.mean_realized_roi:+6.2f}% "
        f"factor*{b.calibration_factor_shrunk:.2f}{star}"
    )


def render_calibration_report(rep: CalibrationReport, verbose: bool = False) -> None:
    _out("=" * 66)
    _out(f"RELIABILITY CALIBRATION   {rep.generated_riyadh}")
    _out("=" * 66)
    _out(rep.headline)
    _out(
        f"records: total={rep.total_records}  matured={rep.matured_records}  "
        f"decided={rep.decided_records}  min_sample={rep.min_sample}  "
        f"shrink_k={rep.shrinkage_k}"
    )
    if rep.brier_score is not None:
        verdict = ("better than coin-flip" if rep.brier_score < rep.brier_baseline
                   else "no better than coin-flip")
        _out(
            f"Brier (if reliability read as P(win)): {rep.brier_score} "
            f"vs {rep.brier_baseline} baseline -> {verdict}"
        )
    _out("")
    _out("By investability bucket:")
    for b in rep.by_investability:
        _out(_fmt_cal_bucket(b))
    _out(f"  {rep.discrimination_note}")
    _out("")
    _out("By stated-reliability band:")
    for b in rep.by_reliability_band:
        _out(_fmt_cal_bucket(b))
    if rep.monotonic_reliability is True:
        _out("  monotonic: higher stated reliability -> higher realized win-rate (good).")
    elif rep.monotonic_reliability is False:
        _out("  NOT monotonic: stated reliability does not cleanly separate winners here.")
    else:
        _out("  monotonicity: insufficient populated bands to assess.")
    if rep.information_coefficient is not None:
        _sig = ""
        if rep.ic_p_value is not None:
            _sig = " (p<0.05, significant)" if rep.ic_p_value < 0.05 else " (n.s.)"
        _out(
            f"  Information Coefficient (entry_score vs realized_roi): "
            f"{rep.information_coefficient:+.4f} over n={rep.ic_n}{_sig} -- "
            f"rank-correlation; >0 means higher scores ranked higher returns."
        )
    if verbose:
        _out("")
        _out("By recommendation tier:")
        for b in rep.by_recommendation:
            _out(_fmt_cal_bucket(b))
        _out("")
        _out("By horizon:")
        for b in rep.by_horizon:
            _out(_fmt_cal_bucket(b))
    if rep.signal_trend_mix:
        mix = ", ".join(f"{k}:{v}" for k, v in sorted(rep.signal_trend_mix.items()))
        _out("")
        _out(f"Current verdict-stability mix (Signal_History): {mix}")
    _out("")
    _out("NOTE: " + rep.advisory_note)
    _out("=" * 66)


# =============================================================================
# Reporting
# =============================================================================
class ReportGenerator:
    def __init__(self, analyzer: PerformanceAnalyzer):
        self.analyzer = analyzer

    def generate_json(
        self, records: List[PerformanceRecord], summary: PerformanceSummary
    ) -> str:
        matured = [
            r
            for r in records
            if r.status == PerformanceStatus.MATURED and r.realized_roi is not None
        ]
        outcomes = [(r.realized_roi or 0.0) > 0 for r in matured]
        ci = (
            self.analyzer.sim.simulate_win_rate(
                outcomes, iterations=10000, confidence=0.95
            )
            if outcomes
            else {}
        )
        report = {
            "generated_at_riyadh": RiyadhTime.format(),
            "version": SCRIPT_VERSION,
            "summary": summary.to_dict(),
            "confidence_intervals": {"win_rate": ci} if ci else {},
            "records": [r.to_dict() for r in records],
        }
        return json_dumps(report, indent=2)

    def generate_csv(
        self, records: List[PerformanceRecord], filepath: str
    ) -> None:
        if not records:
            return
        rows = [r.to_dict() for r in records]
        fieldnames = list(rows[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    def _plot_png_base64(self, fig: "Figure") -> str:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
        return base64.b64encode(buf.getvalue()).decode("utf-8")

    def generate_html(
        self,
        records: List[PerformanceRecord],
        summary: PerformanceSummary,
        filepath: str,
    ) -> None:
        plots: Dict[str, str] = {}
        if PLOT_AVAILABLE and Figure is not None and NUMPY_AVAILABLE and np is not None:
            try:
                matured = [
                    r
                    for r in records
                    if r.status == PerformanceStatus.MATURED
                    and r.realized_roi is not None
                ]
                matured.sort(key=lambda x: (x.maturity_date or x.date_recorded))

                if matured:
                    fig1 = Figure(figsize=(10, 5))
                    ax1 = fig1.add_subplot(111)
                    dates = [
                        (r.maturity_date or r.date_recorded).astimezone(_RIYADH_TZ)
                        for r in matured
                    ]
                    wins = np.cumsum(
                        [1 if (r.realized_roi or 0.0) > 0 else 0 for r in matured]
                    )
                    denom = np.arange(1, len(matured) + 1, dtype=float)
                    win_rate = (wins / denom) * 100.0
                    ax1.plot(dates, win_rate, linewidth=2)
                    ax1.set_title("Cumulative Win Rate (%)")
                    ax1.set_xlabel("Date")
                    ax1.set_ylabel("Win Rate %")
                    ax1.grid(True, alpha=0.25)
                    plots["win_rate"] = self._plot_png_base64(fig1)

                    roi = np.asarray(
                        [float(r.realized_roi or 0.0) for r in matured], dtype=float
                    )
                    fig2 = Figure(figsize=(10, 5))
                    ax2 = fig2.add_subplot(111)
                    ax2.hist(roi, bins=30)
                    ax2.axvline(0.0, linestyle="--", linewidth=1.5)
                    ax2.set_title("ROI Distribution (%)")
                    ax2.set_xlabel("ROI %")
                    ax2.set_ylabel("Frequency")
                    ax2.grid(True, alpha=0.2)
                    plots["roi_hist"] = self._plot_png_base64(fig2)
            except Exception as e:
                logger.warning("Plot generation failed: %s", e)

        html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Performance Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; background: #f5f5f5; margin: 20px; }}
    .container {{ max-width: 1200px; margin: auto; background: #fff; padding: 18px; box-shadow: 0 1px 6px rgba(0,0,0,0.08); }}
    h1 {{ margin: 0 0 10px 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }}
    .card {{ background: #f8f9fa; padding: 12px; border-radius: 8px; border-left: 4px solid #2b6cb0; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 14px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 10px; text-align: left; }}
    th {{ background: #111827; color: #fff; }}
    .pos {{ color: #16a34a; font-weight: bold; }}
    .neg {{ color: #dc2626; font-weight: bold; }}
    .plot {{ margin: 16px 0; text-align: center; }}
  </style>
</head>
<body>
<div class="container">
  <h1>📊 Performance Report</h1>
  <div>Generated (Riyadh): {RiyadhTime.format()}</div>

  <div class="grid" style="margin-top:12px;">
    <div class="card"><div>Total Records</div><div style="font-size:22px;font-weight:bold;">{summary.total_records}</div></div>
    <div class="card"><div>Win Rate</div><div style="font-size:22px;font-weight:bold;">{summary.win_rate:.1f}%</div></div>
    <div class="card"><div>Avg ROI</div><div style="font-size:22px;font-weight:bold;">{summary.avg_roi:.2f}%</div></div>
    <div class="card"><div>Sharpe</div><div style="font-size:22px;font-weight:bold;">{summary.sharpe_ratio:.2f}</div></div>
  </div>

  {"<div class='plot'><img style='max-width:100%;' src='data:image/png;base64," + plots.get("win_rate","") + "'/></div>" if plots.get("win_rate") else ""}
  {"<div class='plot'><img style='max-width:100%;' src='data:image/png;base64," + plots.get("roi_hist","") + "'/></div>" if plots.get("roi_hist") else ""}

  <h2>Recent Records</h2>
  <table>
    <tr><th>Symbol</th><th>Horizon</th><th>Entry Date</th><th>Status</th><th>ROI</th><th>Outcome</th></tr>
"""
        recs = sorted(records, key=lambda r: r.date_recorded, reverse=True)[:30]
        for r in recs:
            # v6.4.0: coerce to 0.0 BEFORE format spec to avoid TypeError
            roi_raw = (
                r.realized_roi if r.realized_roi is not None else r.unrealized_roi
            )
            roi = float(roi_raw) if roi_raw is not None else 0.0
            cls = "pos" if roi > 0 else "neg" if roi < 0 else ""
            html += (
                f"<tr><td><b>{r.symbol}</b></td>"
                f"<td>{r.horizon.value}</td>"
                f"<td>{RiyadhTime.format(r.date_recorded, '%Y-%m-%d')}</td>"
                f"<td>{r.status.value}</td>"
                f"<td class='{cls}'>{roi:.2f}%</td>"
                f"<td>{r.outcome or ''}</td></tr>\n"
            )

        html += """  </table>
</div>
</body>
</html>
"""
        Path(filepath).write_text(html, encoding="utf-8")


# =============================================================================
# Orchestrator
# =============================================================================
# =============================================================================
# BACKTEST HARNESS (Strategy: evidence-gating). v6.10.0
# -----------------------------------------------------------------------------
# A registered-hypothesis + forward-return backtest framework -- the GATE the
# Strategy requires: a signal may only influence a recommendation once it is
# registered as a hypothesis AND a backtest shows it had edge on historical data.
#
# WHAT IT TESTS NOW: the PRICE_STRUCTURE factors already shipped (flow / candle
# structure). It re-runs the LIVE engine reads (analyze_flow /
# analyze_candle_structure) on rolling historical OHLCV windows -- validating the
# ACTUAL production logic, not a reimplementation -- and measures whether the
# trigger (e.g. flow == STRONG_ACCUMULATION) preceded forward returns in the
# hypothesised direction, vs the non-trigger baseline (the conditional lift).
#
# NO LOOKAHEAD: the trigger at day D is computed from bars[:D+1] only; the
# forward return is measured D -> D+horizon. HONEST: a backtest measures PAST
# edge, never a future guarantee. Results carry sample size + a Wilson hit-rate
# CI + a Welch t-stat; a thin sample yields INSUFFICIENT_DATA, never a verdict.
#
# RESERVED: EVENT_SECTOR / THEME_ROTATION hypotheses are defined but NOT yet
# testable -- they need a historical event/theme archive that does not exist
# (news_intelligence is real-time, no archive). They plug into the SAME
# run_backtest once such an archive accumulates.
# =============================================================================


class HypothesisType(str, Enum):
    PRICE_STRUCTURE = "PRICE_STRUCTURE"
    EVENT_SECTOR = "EVENT_SECTOR"        # reserved -- needs historical event archive
    THEME_ROTATION = "THEME_ROTATION"    # reserved -- needs historical theme archive


class HypothesisStatus(str, Enum):
    REGISTERED = "REGISTERED"                 # never backtested
    ACCEPTED = "ACCEPTED"                     # edge confirmed -> MAY influence a recommendation
    REJECTED = "REJECTED"                     # backtested, no edge in the hypothesised direction
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"   # too few trigger events to decide
    ERROR = "ERROR"                           # backtest could not run (e.g. no engine / no history)


class HypothesisDirection(str, Enum):
    BULLISH = "BULLISH"     # trigger should precede POSITIVE forward return
    BEARISH = "BEARISH"     # trigger should precede NEGATIVE forward return


# Default acceptance thresholds (env-overridable; per-run override via the CLI).
_BT_MIN_SAMPLE = _env_int("BACKTEST_MIN_SAMPLE", 30, lo=5)
_BT_MIN_EFFECT_BPS = _env_float("BACKTEST_MIN_EFFECT_BPS", 50.0)   # signal must beat baseline by >= this, signed by direction
_BT_MIN_TSTAT = _env_float("BACKTEST_MIN_TSTAT", 2.0)             # ~95% one-sided significance
_BT_DEFAULT_DAYS = _env_int("BACKTEST_HISTORY_DAYS", 730, lo=120, hi=5000)


@dataclass
class Hypothesis:
    hypothesis_id: str
    name: str
    hypothesis_type: HypothesisType
    source_field: str          # INTERNAL engine field the trigger reads (e.g. "flow", "candle_structure")
    trigger_value: str         # value that fires the trigger (e.g. "STRONG_ACCUMULATION")
    direction: HypothesisDirection
    horizon_days: int = 20
    min_sample: int = _BT_MIN_SAMPLE
    min_effect_bps: float = _BT_MIN_EFFECT_BPS
    min_tstat: float = _BT_MIN_TSTAT
    # --- result fields (filled by run_backtest) ---
    status: HypothesisStatus = HypothesisStatus.REGISTERED
    sample_size: int = 0
    hit_count: int = 0
    hit_rate: float = 0.0
    hit_rate_lo: float = 0.0
    hit_rate_hi: float = 0.0
    mean_signal_roi: float = 0.0
    mean_baseline_roi: float = 0.0
    effect_bps: float = 0.0
    t_stat: float = 0.0
    universe_size: int = 0
    history_days: int = 0
    last_backtest_utc: str = ""
    notes: str = ""

    @property
    def key(self) -> str:
        return self.hypothesis_id

    def to_row(self) -> List[Any]:
        return [
            self.hypothesis_id, self.name, self.hypothesis_type.value,
            self.source_field, self.trigger_value, self.direction.value,
            self.horizon_days, self.min_sample, round(self.min_effect_bps, 1),
            round(self.min_tstat, 2), self.status.value,
            self.sample_size, self.hit_count, round(self.hit_rate, 1),
            round(self.hit_rate_lo, 1), round(self.hit_rate_hi, 1),
            round(self.mean_signal_roi, 3), round(self.mean_baseline_roi, 3),
            round(self.effect_bps, 1), round(self.t_stat, 2),
            self.universe_size, self.history_days, self.last_backtest_utc,
            self.notes,
        ]

    @classmethod
    def from_row(cls, row: List[Any], headers: List[str]) -> "Hypothesis":
        hmap = {str(h).strip(): i for i, h in enumerate(headers)}

        def g(name: str, default: Any = "") -> Any:
            idx = hmap.get(name)
            if idx is None or idx < 0 or idx >= len(row):
                return default
            return row[idx]

        htype = _parse_enum_value(HypothesisType, g("Type"), HypothesisType.PRICE_STRUCTURE)
        hdir = _parse_enum_value(HypothesisDirection, g("Direction"), HypothesisDirection.BULLISH)
        hstat = _parse_enum_value(HypothesisStatus, g("Status"), HypothesisStatus.REGISTERED)
        return cls(
            hypothesis_id=_safe_str(g("Hypothesis ID")),
            name=_safe_str(g("Name")),
            hypothesis_type=htype,
            source_field=_safe_str(g("Source Field")),
            trigger_value=_safe_str(g("Trigger Value")),
            direction=hdir,
            horizon_days=int(_safe_float(g("Horizon (days)"), 20)),
            min_sample=int(_safe_float(g("Min Sample"), _BT_MIN_SAMPLE)),
            min_effect_bps=_safe_float(g("Min Effect (bps)"), _BT_MIN_EFFECT_BPS),
            min_tstat=_safe_float(g("Min t-stat"), _BT_MIN_TSTAT),
            status=hstat,
            sample_size=int(_safe_float(g("Sample Size"), 0)),
            hit_count=int(_safe_float(g("Hit Count"), 0)),
            hit_rate=_safe_float(g("Hit Rate %"), 0.0),
            hit_rate_lo=_safe_float(g("Hit Rate Lo %"), 0.0),
            hit_rate_hi=_safe_float(g("Hit Rate Hi %"), 0.0),
            mean_signal_roi=_safe_float(g("Mean Signal ROI %"), 0.0),
            mean_baseline_roi=_safe_float(g("Mean Baseline ROI %"), 0.0),
            effect_bps=_safe_float(g("Effect (bps)"), 0.0),
            t_stat=_safe_float(g("t-stat"), 0.0),
            universe_size=int(_safe_float(g("Universe Size"), 0)),
            history_days=int(_safe_float(g("History Days"), 0)),
            last_backtest_utc=_safe_str(g("Last Backtest (UTC)")),
            notes=_safe_str(g("Notes")),
        )


def default_hypotheses() -> List[Hypothesis]:
    """The flow + candle-structure factors already shipped, as registered
    hypotheses to validate against history. STRONG reads only -- the high-
    conviction tails where edge, if any, should be clearest."""
    h = 20
    return [
        Hypothesis("FLOW_STRONG_ACCUM", "Flow strong accumulation precedes upside",
                   HypothesisType.PRICE_STRUCTURE, "flow", "STRONG_ACCUMULATION",
                   HypothesisDirection.BULLISH, horizon_days=h),
        Hypothesis("FLOW_STRONG_DISTR", "Flow strong distribution precedes downside",
                   HypothesisType.PRICE_STRUCTURE, "flow", "STRONG_DISTRIBUTION",
                   HypothesisDirection.BEARISH, horizon_days=h),
        Hypothesis("CANDLE_STRONG_BULL", "Structure strong-bullish precedes upside",
                   HypothesisType.PRICE_STRUCTURE, "candle_structure", "STRONG_BULLISH",
                   HypothesisDirection.BULLISH, horizon_days=h),
        Hypothesis("CANDLE_STRONG_BEAR", "Structure strong-bearish precedes downside",
                   HypothesisType.PRICE_STRUCTURE, "candle_structure", "STRONG_BEARISH",
                   HypothesisDirection.BEARISH, horizon_days=h),
    ]


def _yf_deep_history(sym: str, period: str = "2y") -> List[Dict[str, Any]]:
    """v6.11.0: direct yfinance pull at an explicit multi-year depth. Used ONLY
    when TFB_BACKTEST_KSA_YF is enabled and a symbol came back shallow from the
    normal provider chain. yfinance understands Tadawul '.SR' tickers and returns
    ~505 daily bars at period='2y'; this bypasses the yahoo_chart shim's 1mo
    default that capped KSA history at ~22 bars. '.US' tickers fail here (yfinance
    wants bare US tickers) which is fine -- EODHD already serves US at full depth.
    Returns [] on any failure (fail-soft); bars are lowercase OHLCV dicts the same
    shape _bt_bar_close() and make_trigger_fn() already consume."""
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return []
    try:
        # v6.18.0 (BT-ADJ): adjusted by default — unadjusted bars fed every
        # hypothesis backtest a fake +/-N00% bar at each split/bonus issue
        # (the same split-brain fixed provider-side in yahoo_chart v8.8.0).
        # TFB_BT_ADJUSTED=0 restores the raw v6.17.0 pull.
        _adj = (os.getenv("TFB_BT_ADJUSTED") or "1").strip().lower() not in {"0", "false", "off", "no"}
        df = yf.Ticker(sym).history(period=period, interval="1d", auto_adjust=_adj)
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []
    rows: List[Dict[str, Any]] = []
    for idx, r in df.iterrows():
        try:
            rows.append({
                "date": idx.strftime("%Y-%m-%d"),
                "open": float(r["Open"]), "high": float(r["High"]),
                "low": float(r["Low"]), "close": float(r["Close"]),
                # v6.18.0 (BT-ADJ): with auto_adjust=True, Close IS the
                # adjusted close — surfaced under the key _bt_bar_close
                # prefers, so the adjusted-first pick actually engages.
                "adjclose": float(r["Close"]),
                "volume": float(r.get("Volume", 0) or 0),
            })
        except Exception:
            continue
    return rows


def _bt_bar_close(bar: Dict[str, Any]) -> Optional[float]:
    """Adjusted close preferred (EODHD EOD), else close. Positive floats only."""
    for k in ("adjusted_close", "adjclose", "close", "c"):
        if isinstance(bar, dict) and bar.get(k) is not None:
            v = _safe_float(bar.get(k), default=float("nan"))
            if v == v and v > 0:
                return v
    return None


def make_trigger_fn(hypothesis: Hypothesis) -> Optional[Callable[[List[Dict[str, Any]]], bool]]:
    """Build the trigger evaluator that re-runs the LIVE engine read on a window
    of bars and returns True when the engine field == the hypothesis trigger.
    Imports the engine lazily; returns None if the engine is unavailable (caller
    marks the hypothesis ERROR rather than crashing)."""
    field = hypothesis.source_field
    want = (hypothesis.trigger_value or "").strip().upper()
    try:
        if field == "flow":
            from core.data_engine_v2 import analyze_flow as _read
        elif field == "candle_structure":
            from core.data_engine_v2 import analyze_candle_structure as _read
        else:
            return None
    except Exception:
        try:
            if field == "flow":
                from data_engine_v2 import analyze_flow as _read  # type: ignore
            elif field == "candle_structure":
                from data_engine_v2 import analyze_candle_structure as _read  # type: ignore
            else:
                return None
        except Exception:
            return None

    def _trigger(bars: List[Dict[str, Any]]) -> bool:
        try:
            out = _read(bars)
            return _safe_str(out.get(field)).upper() == want
        except Exception:
            return False

    return _trigger


def _welch_t(a: List[float], b: List[float]) -> float:
    """Welch's t-statistic for mean(a) - mean(b). 0.0 when undefined."""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return 0.0
    ma = sum(a) / na
    mb = sum(b) / nb
    va = sum((x - ma) ** 2 for x in a) / (na - 1)
    vb = sum((x - mb) ** 2 for x in b) / (nb - 1)
    denom = math.sqrt(va / na + vb / nb)
    if denom <= 0:
        return 0.0
    return (ma - mb) / denom


def run_backtest(
    hypothesis: Hypothesis,
    history_by_symbol: Dict[str, List[Dict[str, Any]]],
    trigger_fn: Optional[Callable[[List[Dict[str, Any]]], bool]] = None,
    min_window: int = 12,
    *,
    nonoverlap: Optional[bool] = None,
    tstat_floor: Optional[float] = None,
) -> Hypothesis:
    """Event-study backtest of a single hypothesis. PURE: takes per-symbol bar
    history (date-ASCENDING EOD dicts) + a trigger_fn, returns a COPY of the
    hypothesis with result fields filled. trigger_fn(bars[:D+1]) -> bool is
    evaluated with NO lookahead; the forward horizon-day return is the outcome.
    Signal days are compared against NON-trigger days (the conditional lift).

    v6.11.0: when `nonoverlap` (env TFB_BACKTEST_NONOVERLAP) is set, the verdict
    is re-decided on INDEPENDENT (non-overlapping) windows so long-horizon
    t-stats are not inflated by autocorrelated overlapping returns; the optional
    `tstat_floor` (env TFB_BACKTEST_TSTAT_FLOOR) raises the |t| bar for
    multiple-testing. When False, behaviour is byte-identical to v6.10.1."""
    import copy as _copy
    hyp = _copy.copy(hypothesis)
    hyp.last_backtest_utc = _utc_now().isoformat()

    # v6.11.0: resolve hardening toggles. Default to env so the weekly cron picks
    # them up; pass explicitly in tests. nonoverlap=False => block below is fully
    # skipped and the verdict is identical to v6.10.1.
    if nonoverlap is None:
        nonoverlap = _env_bool("TFB_BACKTEST_NONOVERLAP", False)
    if tstat_floor is None:
        tstat_floor = _env_float("TFB_BACKTEST_TSTAT_FLOOR", 0.0)

    if trigger_fn is None:
        trigger_fn = make_trigger_fn(hypothesis)
    if trigger_fn is None:
        hyp.status = HypothesisStatus.ERROR
        hyp.notes = "engine trigger unavailable (no analyze_%s)" % hypothesis.source_field
        return hyp

    h = max(1, int(hypothesis.horizon_days))
    bullish = hypothesis.direction == HypothesisDirection.BULLISH
    signal_rois: List[float] = []
    baseline_rois: List[float] = []
    signal_indep: List[float] = []      # v6.11.0: independent (non-overlapping) windows
    baseline_indep: List[float] = []
    universe = 0
    max_hist = 0

    for _sym, bars in (history_by_symbol or {}).items():
        if not isinstance(bars, list) or len(bars) < (min_window + h + 1):
            continue
        max_hist = max(max_hist, len(bars))
        closes = [_bt_bar_close(b) for b in bars]
        universe += 1
        for d in range(min_window, len(bars) - h):
            c0 = closes[d]
            ch = closes[d + h]
            if c0 is None or ch is None or c0 <= 0:
                continue
            roi = (ch - c0) / c0 * 100.0   # percent
            if trigger_fn(bars[: d + 1]):
                signal_rois.append(roi)
            else:
                baseline_rois.append(roi)
        if nonoverlap:
            # v6.11.0: independent windows -- advance the cursor by h whenever a
            # usable window is consumed so no two outcomes share return periods.
            i = min_window
            limit = len(bars) - h
            while i < limit:
                c0 = closes[i]
                ch = closes[i + h]
                if c0 is None or ch is None or c0 <= 0:
                    i += 1
                    continue
                roi = (ch - c0) / c0 * 100.0
                if trigger_fn(bars[: i + 1]):
                    signal_indep.append(roi)
                else:
                    baseline_indep.append(roi)
                i += h

    hyp.universe_size = universe
    hyp.history_days = max_hist
    n = len(signal_rois)
    hyp.sample_size = n

    if n > 0:
        hyp.mean_signal_roi = sum(signal_rois) / n
    if baseline_rois:
        hyp.mean_baseline_roi = sum(baseline_rois) / len(baseline_rois)

    if n < max(1, hypothesis.min_sample):
        hyp.status = HypothesisStatus.INSUFFICIENT_DATA
        hyp.notes = "%d trigger events (< min %d); no verdict" % (n, hypothesis.min_sample)
        return hyp

    hyp.effect_bps = (hyp.mean_signal_roi - hyp.mean_baseline_roi) * 100.0   # 1% ROI = 100 bps
    hits = sum(1 for r in signal_rois if (r > 0 if bullish else r < 0))
    hyp.hit_count = hits
    hyp.hit_rate = hits / n * 100.0
    lo, hi = wilson_interval(hits, n)
    hyp.hit_rate_lo, hyp.hit_rate_hi = lo, hi
    hyp.t_stat = _welch_t(signal_rois, baseline_rois)

    eff_ok = (hyp.effect_bps >= hypothesis.min_effect_bps) if bullish else (hyp.effect_bps <= -hypothesis.min_effect_bps)
    t_ok = (hyp.t_stat >= hypothesis.min_tstat) if bullish else (hyp.t_stat <= -hypothesis.min_tstat)
    if eff_ok and t_ok:
        hyp.status = HypothesisStatus.ACCEPTED
        hyp.notes = "edge confirmed: effect %+.0f bps, t=%+.2f, hit %.0f%% [%.0f-%.0f]" % (
            hyp.effect_bps, hyp.t_stat, hyp.hit_rate, lo, hi)
    else:
        hyp.status = HypothesisStatus.REJECTED
        hyp.notes = "no edge in direction: effect %+.0f bps, t=%+.2f (need %s%.0f bps & |t|>=%.1f)" % (
            hyp.effect_bps, hyp.t_stat, "" if bullish else "-", hypothesis.min_effect_bps, hypothesis.min_tstat)

    if nonoverlap:
        # v6.11.0: HONEST re-verdict on independent windows. The overlapping
        # t-stat above is kept only for disclosure; the decision AND the stored
        # 't-stat' field switch to the non-overlapping statistic, which is not
        # inflated by autocorrelation at long horizons. Reached only when the
        # overlapping sample already cleared min_sample (so lo/hi are defined).
        overlap_t = hyp.t_stat
        n_indep = len(signal_indep)
        t_indep = _welch_t(signal_indep, baseline_indep) if n_indep >= 2 else 0.0
        eff_min_t = max(float(hypothesis.min_tstat), float(tstat_floor or 0.0))
        hyp.t_stat = t_indep   # column now carries the decision-relevant statistic
        t_ok_i = (t_indep >= eff_min_t) if bullish else (t_indep <= -eff_min_t)
        bar_txt = "%s%.0f bps & |t_indep|>=%.2f" % (
            "" if bullish else "-", hypothesis.min_effect_bps, eff_min_t)
        if n_indep < max(1, hypothesis.min_sample):
            hyp.status = HypothesisStatus.INSUFFICIENT_DATA
            hyp.notes = ("indep-window thin: n_indep=%d (< min %d) -- overlapping "
                         "t=%+.2f NOT trusted (autocorrelated)"
                         % (n_indep, hypothesis.min_sample, overlap_t))
        elif eff_ok and t_ok_i:
            hyp.status = HypothesisStatus.ACCEPTED
            hyp.notes = ("edge confirmed (independent windows): effect %+.0f bps, "
                         "t_indep=%+.2f, n_indep=%d, hit %.0f%% [%.0f-%.0f] [overlap t=%+.2f]"
                         % (hyp.effect_bps, t_indep, n_indep, hyp.hit_rate, lo, hi, overlap_t))
        else:
            hyp.status = HypothesisStatus.REJECTED
            hyp.notes = ("no robust edge (independent windows): effect %+.0f bps, "
                         "t_indep=%+.2f, n_indep=%d (need %s) [overlap t=%+.2f inflated]"
                         % (hyp.effect_bps, t_indep, n_indep, bar_txt, overlap_t))

    return hyp


class HypothesisRegistry:
    """Sheets-backed store for hypotheses + their latest backtest result.
    Mirrors PerformanceStore's IO pattern (own tab Hypothesis_Registry).
    Best-effort: the backtest still runs in-memory when gspread is unavailable."""
    SHEET_DEFAULT = "Hypothesis_Registry"
    START_ROW = 5
    DATA_ROW0 = 6
    HEADERS = [
        "Hypothesis ID", "Name", "Type", "Source Field", "Trigger Value",
        "Direction", "Horizon (days)", "Min Sample", "Min Effect (bps)",
        "Min t-stat", "Status", "Sample Size", "Hit Count", "Hit Rate %",
        "Hit Rate Lo %", "Hit Rate Hi %", "Mean Signal ROI %",
        "Mean Baseline ROI %", "Effect (bps)", "t-stat", "Universe Size",
        "History Days", "Last Backtest (UTC)", "Notes",
    ]

    def __init__(self, spreadsheet_id: str, sheet_name: str = ""):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name or self.SHEET_DEFAULT
        self.backoff = FullJitterBackoff()
        self.gc = None
        self.sheet = None
        self.ws = None
        self._init_sheet()

    def _load_sa_credentials_best_effort(self) -> Optional[Any]:
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(
                    obj, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        except Exception:
            return None
        return None

    def _init_sheet(self) -> None:
        if not GSPREAD_AVAILABLE or gspread is None:
            return
        try:
            creds = self._load_sa_credentials_best_effort()
            self.gc = gspread.authorize(creds) if creds else gspread.service_account()
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            try:
                self.ws = self.sheet.worksheet(self.sheet_name)
            except Exception:
                self.ws = self.sheet.add_worksheet(title=self.sheet_name, rows=200, cols=40)
            self.backoff.execute_sync(self._ensure_headers)
        except Exception as e:
            logger.error("HypothesisRegistry init failed: %s", e)
            self.gc = None
            self.sheet = None
            self.ws = None

    def _ensure_headers(self) -> None:
        if not self.ws:
            return
        try:
            existing = self.ws.row_values(self.START_ROW)
        except Exception:
            existing = []
        if [str(x).strip() for x in existing[: len(self.HEADERS)]] != self.HEADERS:
            rng = _a1_range(1, self.START_ROW, len(self.HEADERS), self.START_ROW)
            self.backoff.execute_sync(
                lambda: self.ws.update(values=[self.HEADERS], range_name=rng))

    def is_available(self) -> bool:
        return self.ws is not None

    def load(self) -> List["Hypothesis"]:
        if not self.ws:
            return []
        try:
            rows = self.ws.get_all_values()
        except Exception:
            return []
        if len(rows) < self.DATA_ROW0:
            return []
        headers = rows[self.START_ROW - 1] if len(rows) >= self.START_ROW else self.HEADERS
        out: List[Hypothesis] = []
        for row in rows[self.DATA_ROW0 - 1:]:
            if not any(str(c).strip() for c in row):
                continue
            try:
                out.append(Hypothesis.from_row(row, headers))
            except Exception:
                continue
        return out

    def save(self, hypotheses: List["Hypothesis"]) -> bool:
        if not self.ws:
            return False
        end_col = len(self.HEADERS)
        data = [h.to_row() for h in hypotheses]

        def _save() -> None:
            self.ws.batch_clear([_a1_range(1, self.DATA_ROW0, end_col, 10000)])
            if data:
                self.ws.update(
                    values=data,
                    range_name=_a1_range(1, self.DATA_ROW0, end_col, self.DATA_ROW0 + len(data) - 1))

        try:
            self.backoff.execute_sync(_save)
            return True
        except Exception as e:
            logger.error("Failed to save hypotheses: %s", e)
            return False

    def register_defaults(self, existing: Optional[List["Hypothesis"]] = None) -> List["Hypothesis"]:
        """Merge default hypotheses in, preserving any already-present (by id)."""
        cur = {h.hypothesis_id: h for h in (existing or [])}
        for d in default_hypotheses():
            if d.hypothesis_id not in cur:
                cur[d.hypothesis_id] = d
        return list(cur.values())


class PerformanceTrackerApp:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        # v6.4.0: don't raise here; let main() check and return 1
        self.spreadsheet_id = self._resolve_spreadsheet_id()
        self.store = PerformanceStore(
            self.spreadsheet_id, args.sheet_name or PerformanceStore.SHEET_DEFAULT
        )
        self.analyzer = PerformanceAnalyzer()
        self.reporter = ReportGenerator(self.analyzer)

        self.backend = BackendClient(
            base_url=self._backend_base_url(),
            token=self._backend_token(),
        )

        # v6.7.0: day-to-day action-trend (Signal_History). Env-gated,
        # default ON (kill-switch TRACK_SIGNAL_HISTORY=0). Reuses the same
        # spreadsheet; a separate tab. The trend window is snapshot-based
        # (one snapshot per symbol per day at the intended daily cadence).
        self.signal_history_enabled = _env_bool("TRACK_SIGNAL_HISTORY", True)
        self.trend_window = _env_int("TRACK_TREND_WINDOW", 5, lo=2, hi=60)
        self.signal_store: Optional[SignalHistoryStore] = None
        if self.signal_history_enabled:
            try:
                self.signal_store = SignalHistoryStore(
                    self.spreadsheet_id,
                    os.getenv("TRACK_SIGNAL_SHEET", SignalHistoryStore.SHEET_DEFAULT),
                )
            except Exception as e:
                logger.error("SignalHistoryStore unavailable: %s", e)
                self.signal_store = None
        self.trend_analyzer = SignalTrendAnalyzer()

        # v6.13.0: SURFACE the action-trend trajectory to a user-visible
        # Signal_Trends tab (the already-computed SignalTrend, which prior
        # versions wrote only to an ephemeral local JSON under --export).
        # Separate kill-switch, default OFF -> with TFB_SIGNAL_TRENDS_TAB
        # unset the store is never constructed and the analyze path is
        # byte-identical to v6.12.0. Requires the snapshot history (the
        # trend source), so it is additionally gated on signal_history.
        self.signal_trends_enabled = _env_bool("TFB_SIGNAL_TRENDS_TAB", False)
        self.signal_trend_store: Optional[SignalTrendStore] = None
        if self.signal_trends_enabled and self.signal_history_enabled:
            try:
                self.signal_trend_store = SignalTrendStore(
                    self.spreadsheet_id,
                    os.getenv(
                        "TFB_SIGNAL_TRENDS_SHEET",
                        SignalTrendStore.SHEET_DEFAULT,
                    ),
                )
            except Exception as e:
                logger.error("SignalTrendStore unavailable: %s", e)
                self.signal_trend_store = None

        # v6.8.0: reliability calibration (Measure step). Reuses the cycle's
        # already-loaded records -- no extra I/O. Gated, default ON.
        self.calibration_enabled = _env_bool("TRACK_CALIBRATION", True)
        self.cal_min_sample = max(1, int(
            getattr(self.args, "min_sample", None)
            or _env_int("CAL_MIN_SAMPLE", 10, lo=1)
        ))
        self.cal_shrink_k = _env_int("CAL_SHRINKAGE_K", 20, lo=1)
        self.calibrator = ReliabilityCalibrator(self.cal_min_sample, self.cal_shrink_k)

        self.stop_event = Event()

    def _resolve_spreadsheet_id(self) -> str:
        if self.args.sheet_id and str(self.args.sheet_id).strip():
            return str(self.args.sheet_id).strip()

        sid = (os.getenv("TRACK_SHEET_ID") or "").strip()
        if sid:
            return sid

        sid = (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
        if sid:
            return sid

        sid = (
            (getattr(settings, "default_spreadsheet_id", None) or "").strip()
            if settings
            else ""
        )
        if sid:
            return sid

        return ""

    def _backend_base_url(self) -> str:
        env_url = (os.getenv("BACKEND_BASE_URL") or "").strip()
        if env_url:
            return env_url.rstrip("/")
        cfg_url = (
            (getattr(settings, "backend_base_url", None) or "").strip()
            if settings
            else ""
        )
        if cfg_url:
            return cfg_url.rstrip("/")
        return (os.getenv("TFB_BASE_URL") or "").strip().rstrip("/")

    def _backend_token(self) -> str:
        for k in ("TFB_TOKEN", "APP_TOKEN", "BACKEND_TOKEN", "X_APP_TOKEN"):
            v = (os.getenv(k) or "").strip()
            if v:
                return v
        return ""

    def _horizons(self) -> List[HorizonType]:
        out: List[HorizonType] = []
        for raw in (self.args.horizons or ["1M", "3M"]):
            out.append(_parse_enum_value(HorizonType, raw, HorizonType.MONTH_1))
        seen = set()
        final = []
        for h in out:
            if h.value not in seen:
                seen.add(h.value)
                final.append(h)
        return final

    def _derive_target(
        self, row: Dict[str, Any], horizon: HorizonType, entry_price: float
    ) -> Tuple[float, float]:
        suffix = (
            "1m"
            if horizon.value == "1M"
            else "3m"
            if horizon.value == "3M"
            else "12m"
            if horizon.value == "1Y"
            else ""
        )
        fkey = f"forecast_price_{suffix}" if suffix else ""
        rkey = f"expected_roi_{suffix}" if suffix else ""

        tgt_price = _safe_float(row.get(fkey), default=0.0) if fkey else 0.0
        tgt_roi = _safe_float(row.get(rkey), default=0.0) if rkey else 0.0

        if tgt_price <= 0.0 and tgt_roi != 0.0 and entry_price > 0.0:
            tgt_price = entry_price * (1.0 + (tgt_roi / 100.0))
        if tgt_roi == 0.0 and tgt_price > 0.0 and entry_price > 0.0:
            tgt_roi = (tgt_price / entry_price - 1.0) * 100.0

        return tgt_price, tgt_roi

    def _recommendation_from_row(self, row: Dict[str, Any]) -> RecommendationType:
        """
        v6.5.0: classify a Top10 row's `recommendation` field into the
        canonical 8-tier `RecommendationType`. Substring-match-based with
        a precedence-ordered table -- longer / more specific tokens are
        checked first to prevent the v6.4.0 silent-collapse bug where
        ACCUMULATE / OUTPERFORM / REDUCE / AVOID all fell through to HOLD.

        Vocabulary table (order matters):
          "STRONG BUY" / "STRONG_BUY" / "STRONGBUY"          -> STRONG_BUY
          "STRONG SELL" / "STRONG_SELL" / "STRONGSELL"       -> STRONG_SELL
          "CONVICTION SELL" / "DEEP SELL"                    -> STRONG_SELL
          "ACCUMULATE" / "SCALE IN" / "SCALE_IN"             -> ACCUMULATE
          "MODERATE BUY" / "MOD BUY"                         -> ACCUMULATE
          "AVOID" / "HARD PASS" / "DO NOT BUY" / "DNB"       -> AVOID
          "UNINVESTABLE"                                     -> AVOID
          "OUTPERFORM" / "OVERWEIGHT" / "ADD"                -> BUY
          "BUY"                                              -> BUY
          "MARKET PERFORM" / "NEUTRAL" / "EQUAL WEIGHT"      -> HOLD
          "HOLD"                                             -> HOLD
          "UNDERPERFORM" / "UNDERWEIGHT" / "REDUCE" / "TRIM" -> REDUCE
          "EXIT"                                             -> SELL
          "SELL"                                             -> SELL
          unknown / blank                                    -> HOLD

        Aligned byte-for-byte with yahoo_fundamentals_provider v6.3.0's
        `map_recommendation` precedence so historical rows ingested
        from either source classify identically.
        """
        raw = _safe_str(row.get("recommendation")).upper()
        if not raw:
            return RecommendationType.HOLD

        # Normalize separators so "STRONG_BUY" / "STRONG-BUY" / "STRONGBUY"
        # all reduce to a form that matches the substring checks below.
        s = raw.replace("_", " ").replace("-", " ").replace("/", " ")
        # Collapse internal runs of whitespace
        while "  " in s:
            s = s.replace("  ", " ")
        s = s.strip()

        # No-separator form for tokens like "STRONGBUY" / "STRONGSELL".
        s_nospace = s.replace(" ", "")

        # --- Tier 1: STRONG_BUY / STRONG_SELL (longest tokens first) ---
        if ("STRONG" in s and "BUY" in s) or "STRONGBUY" in s_nospace:
            return RecommendationType.STRONG_BUY
        if (
            ("STRONG" in s and "SELL" in s)
            or "STRONGSELL" in s_nospace
            or "CONVICTION SELL" in s
            or "DEEP SELL" in s
        ):
            return RecommendationType.STRONG_SELL

        # --- Tier 2: ACCUMULATE (broker-vocab variants) ---
        # Checked BEFORE plain BUY so "MODERATE BUY" routes to ACCUMULATE,
        # not BUY. Also covers "SCALE IN" which has no "BUY" substring.
        if (
            "ACCUMULATE" in s
            or "SCALE IN" in s
            or "MODERATE BUY" in s
            or "MOD BUY" in s
        ):
            return RecommendationType.ACCUMULATE

        # --- Tier 3: AVOID (no substring conflict, but place high so a
        # row labelled "AVOID THIS STOCK" doesn't accidentally hit a
        # later branch). ---
        if (
            "AVOID" in s
            or "HARD PASS" in s
            or "DO NOT BUY" in s
            or s == "DNB"
            or "UNINVESTABLE" in s
        ):
            return RecommendationType.AVOID

        # --- Tier 4: BUY (with broker-vocab aliases) ---
        # OUTPERFORM / OVERWEIGHT / ADD all route to BUY -- v6.4.0
        # collapsed them silently to HOLD because none contain "BUY".
        if "OUTPERFORM" in s or "OVERWEIGHT" in s:
            return RecommendationType.BUY
        if s == "ADD" or " ADD " in (" " + s + " "):
            # Word-boundary check for "ADD" to avoid false positives on
            # tokens like "ADDITIONAL".
            return RecommendationType.BUY
        if "BUY" in s:
            return RecommendationType.BUY

        # --- Tier 5: REDUCE (with broker-vocab aliases). Checked BEFORE
        # plain SELL so "UNDERPERFORM" routes to REDUCE, not the
        # fallthrough. ---
        if (
            "UNDERPERFORM" in s
            or "UNDERWEIGHT" in s
            or "REDUCE" in s
            or "TRIM" in s
        ):
            return RecommendationType.REDUCE

        # --- Tier 6: SELL (the catch-all for any remaining sell variant) ---
        if "EXIT" in s or "SELL" in s:
            return RecommendationType.SELL

        # --- Tier 7: HOLD aliases ---
        if (
            "HOLD" in s
            or "MARKET PERFORM" in s
            or "NEUTRAL" in s
            or "EQUAL WEIGHT" in s
        ):
            return RecommendationType.HOLD

        # Unknown vocabulary -- preserves v6.4.0's HOLD default.
        return RecommendationType.HOLD

    async def record_from_top10(
        self,
        existing: List[PerformanceRecord],
        rows: Optional[List[Dict[str, Any]]] = None,
    ) -> List[PerformanceRecord]:
        # v6.7.0: accept pre-fetched rows so run_once can fetch the Top10
        # universe ONCE and feed both the pick recorder and the daily
        # signal-snapshot logger -- no second backend call, no added edge
        # pressure. rows=None preserves the original self-fetch behavior.
        if rows is None:
            if not self.backend.base_url:
                return []
            rows, meta = await self.backend.get_top10_rows(criteria_overrides=None)
            if not rows:
                if meta.get("error"):
                    logger.warning(
                        "record_from_top10: no rows (%s)", meta.get("error")
                    )
                return []
            logger.info(
                "record_from_top10: %d rows from %s",
                len(rows),
                meta.get("endpoint") or "unknown",
            )
        else:
            if not rows:
                return []
            logger.info("record_from_top10: %d rows (pre-fetched)", len(rows))

        existing_keys = set(r.key for r in existing)
        now = RiyadhTime.now()
        horizons = self._horizons()

        new_records: List[PerformanceRecord] = []
        for row in rows:
            sym = _safe_str(row.get("symbol")).upper()
            if not sym:
                continue
            entry_price = _safe_float(
                row.get("current_price") or row.get("price"), default=0.0
            )
            if entry_price <= 0.0:
                continue

            score = _safe_float(row.get("overall_score"), default=0.0)
            risk_score = row.get("risk_score")
            conf = row.get("forecast_confidence")
            risk_bucket = _risk_bucket_from_score(
                _safe_float(risk_score, default=0.0) if risk_score is not None else None
            )
            conf_bucket = _confidence_bucket(
                _safe_float(conf, default=0.0) if conf is not None else None
            )
            rec = self._recommendation_from_row(row)

            # v6.6.0: capture the investability-gate snapshot at entry. The
            # engine (v5.78+) emits these as canonical snake_case keys on every
            # Top10 row. On a pre-gate deploy they are absent -> safe zeros /
            # blanks, so the record is still written, just without a gate tag.
            entry_dq = _safe_float(row.get("data_quality_score"), default=0.0)
            entry_frel = _safe_float(
                row.get("forecast_reliability_score"), default=0.0
            )
            entry_inv_status = _safe_str(row.get("investability_status"))
            entry_final_action = _safe_str(row.get("final_action"))

            origin = (
                _safe_str(
                    row.get("source_page") or row.get("origin") or "Top_10_Investments"
                )
                or "Top_10_Investments"
            )

            for h in horizons:
                tgt_price, tgt_roi = self._derive_target(row, h, entry_price)
                if tgt_price <= 0.0:
                    tgt_price = entry_price
                    tgt_roi = 0.0

                target_date = now + timedelta(days=h.days)
                r = PerformanceRecord(
                    record_id=str(uuid.uuid4()),
                    symbol=sym,
                    horizon=h,
                    date_recorded=now,
                    entry_price=entry_price,
                    entry_recommendation=rec,
                    entry_score=score,
                    entry_risk_bucket=risk_bucket,
                    entry_confidence=conf_bucket,
                    origin_tab=origin,
                    target_price=tgt_price,
                    target_roi=tgt_roi,
                    target_date=target_date,
                    status=PerformanceStatus.ACTIVE,
                    current_price=entry_price,
                    unrealized_roi=0.0,
                    last_updated=_utc_now(),
                    # v6.6.0: investability-gate snapshot at entry.
                    entry_data_quality=entry_dq,
                    entry_forecast_reliability=entry_frel,
                    entry_investability_status=entry_inv_status,
                    entry_final_action=entry_final_action,
                )
                if r.key not in existing_keys:
                    existing_keys.add(r.key)
                    new_records.append(r)

        return new_records

    def _build_signal_snapshots(
        self, rows: List[Dict[str, Any]]
    ) -> List[SignalSnapshot]:
        now = RiyadhTime.now()
        out: List[SignalSnapshot] = []
        seen: set = set()
        for row in rows or []:
            sym = _safe_str(row.get("symbol")).upper()
            if not sym:
                continue
            price = _safe_float(
                row.get("current_price") or row.get("price"), default=0.0
            )
            rec = self._recommendation_from_row(row)
            origin = (
                _safe_str(
                    row.get("source_page") or row.get("origin") or "Top_10_Investments"
                )
                or "Top_10_Investments"
            )
            # v6.14.0 (F4): event-context capture, gated (default ON).
            # Best-effort header-keyed reads — blank when the backend row
            # doesn't carry the key; days_to_* stay None until the F1
            # calendar layer supplies next_earnings_date / next_ex_div_date.
            if _env_bool("TRACK_EVENT_CONTEXT", True):
                _arating = _safe_str(
                    row.get("analyst_rating") or row.get("Analyst Rating")
                )
                _tprice = _safe_float(
                    row.get("target_price") or row.get("Target Price"), default=0.0
                )
                _dte = _days_until(
                    row.get("next_earnings_date") or row.get("Next Earnings Date")
                )
                _dtx = _days_until(
                    row.get("next_ex_div_date") or row.get("Next Ex-Div Date")
                )
            else:
                _arating, _tprice, _dte, _dtx = "", 0.0, None, None
            snap = SignalSnapshot(
                snapshot_id=str(uuid.uuid4()),
                symbol=sym,
                date_recorded=now,
                recommendation=rec,
                final_action=_safe_str(row.get("final_action")),
                investability_status=_safe_str(row.get("investability_status")),
                overall_score=_safe_float(row.get("overall_score"), default=0.0),
                forecast_reliability=_safe_float(
                    row.get("forecast_reliability_score"), default=0.0
                ),
                data_quality=_safe_float(row.get("data_quality_score"), default=0.0),
                risk_score=_safe_float(row.get("risk_score"), default=0.0),
                price=price,
                origin_tab=origin,
                analyst_rating=_arating,
                target_price=_tprice,
                days_to_earnings=_dte,
                days_to_exdiv=_dtx,
            )
            if snap.key in seen:
                continue
            seen.add(snap.key)
            out.append(snap)
        return out

    def _load_calendar_context(self) -> Dict[str, Dict[str, Any]]:
        """v6.15.0 (F1 wiring): read the Calendar_Events tab written daily by
        scripts/run_calendar_sync.py. Best-effort: piggybacks the already-open
        Signal_History spreadsheet handle; ANY problem returns {} silently
        (one debug line) — snapshots then behave exactly as v6.14.0."""
        if not _env_bool("TRACK_EVENT_CONTEXT", True):
            return {}
        store = self.signal_store
        if store is None or getattr(store, "sheet", None) is None:
            return {}
        tab = os.getenv("TFB_CALENDAR_SHEET", "Calendar_Events").strip() or "Calendar_Events"
        try:
            ws = store.sheet.worksheet(tab)
            values = ws.get("A1:E2000") or []
        except Exception as e:
            logger.debug("calendar context unavailable (%s): %s", tab, e)
            return {}
        if not values:
            return {}
        hdr = [_safe_str(h) for h in values[0]]
        hmap = {h: i for i, h in enumerate(hdr)}
        ci_s = hmap.get("Symbol")
        ci_e = hmap.get("Next Earnings Date")
        ci_x = hmap.get("Next Ex-Div Date")
        if ci_s is None:
            return {}

        def _cell_at(row: List[Any], idx: Optional[int]) -> str:
            if idx is None or idx >= len(row):
                return ""
            return _safe_str(row[idx])

        out: Dict[str, Dict[str, Any]] = {}
        for r in values[1:]:
            sym = _cell_at(r, ci_s).upper()
            if not sym:
                continue
            e, x = _cell_at(r, ci_e), _cell_at(r, ci_x)
            if e or x:
                out[sym] = {"next_earnings_date": e or None,
                            "next_ex_div_date": x or None}
        return out

    async def _augment_with_decision_symbols(
        self, prefetched: Optional[List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """v6.16.0 (Fix #5): guarantee every decision symbol (cost-basis
        holdings + pinned extras) is present in the row set that feeds BOTH
        Performance_Log recording and Signal_History snapshots. Returns a
        LIST even when prefetched is None, so a total Top_10 fetch failure
        no longer silently loses the snapshot day for held names. Only
        called when the master switch is ON (run_once gates it)."""
        rows: List[Dict[str, Any]] = list(prefetched or [])
        covered = {
            _safe_str(r.get("symbol")).strip().upper()
            for r in rows if _safe_str(r.get("symbol")).strip()
        }
        priority: set = set(_force_extra_symbols())
        if self.spreadsheet_id:
            loop = asyncio.get_running_loop()
            try:
                held = await loop.run_in_executor(
                    _get_executor(), _load_costbasis_symbols, self.spreadsheet_id
                )
                priority |= set(held or [])
            except Exception as e:
                logger.warning("cost-basis holdings read failed: %s", e)
        missing = sorted(s for s in priority if s and s not in covered)
        if len(missing) > _force_max():
            logger.warning(
                "[v6.16.0 COVERAGE] %d missing decision symbols exceed cap %d; "
                "forcing the first %d (raise TFB_TRACK_FORCE_MAX if intended)",
                len(missing), _force_max(), _force_max(),
            )
            missing = missing[: _force_max()]
        if not missing:
            logger.info(
                "[v6.16.0 COVERAGE] full: %d priority symbol(s) already in the "
                "%d prefetched row(s)", len(priority), len(rows),
            )
            return rows
        if not self.backend.base_url:
            logger.warning(
                "[v6.16.0 COVERAGE] %d decision symbol(s) missing but no "
                "backend configured: %s", len(missing), ", ".join(missing),
            )
            return rows
        fetched, meta = await self.backend.get_rows_for_symbols(
            missing, _force_rows_page()
        )
        got: set = set()
        for r in fetched:
            sym = _safe_str(r.get("symbol")).strip().upper()
            if not sym or sym in covered:
                continue
            r.setdefault("origin", "Decision_Coverage")
            rows.append(r)
            covered.add(sym)
            got.add(sym)
        unresolved = [s for s in missing if s not in got]
        logger.info(
            "[v6.16.0 COVERAGE] priority=%d prefetched=%d forced=%d fetched=%d "
            "unresolved=%s", len(priority), len(rows) - len(got), len(missing),
            len(got), ",".join(unresolved) if unresolved else "-",
        )
        return rows

    async def record_signal_snapshots(self, rows: List[Dict[str, Any]]) -> int:
        # v6.7.0: log one daily verdict snapshot per decision symbol from the
        # ALREADY-FETCHED Top10 rows. Idempotent per symbol-per-day. Best
        # effort: a Sheets hiccup here must never break --record/--audit.
        if not self.signal_history_enabled or self.signal_store is None:
            return 0
        if not self.signal_store.is_available():
            return 0
        # v6.15.0 (F1 wiring): merge Calendar_Events dates into the rows so
        # the v6.14.0 day columns fill. Best-effort; {} -> no-op.
        try:
            merged = _merge_calendar_context(rows, self._load_calendar_context())
            if merged:
                logger.info("calendar context merged onto %d row(s)", merged)
        except Exception as e:
            logger.debug("calendar context merge skipped: %s", e)
        snaps = self._build_signal_snapshots(rows)
        if not snaps:
            return 0
        loop = asyncio.get_running_loop()
        try:
            written = await loop.run_in_executor(
                _get_executor(), self.signal_store.append_snapshots, snaps
            )
        except Exception as e:
            logger.error("record_signal_snapshots failed: %s", e)
            return 0
        if written:
            logger.info(
                "record_signal_snapshots: wrote %d new daily snapshot(s)", written
            )
        return int(written or 0)

    async def _compute_signal_trends(self) -> List[SignalTrend]:
        if not self.signal_history_enabled or self.signal_store is None:
            return []
        if not self.signal_store.is_available():
            return []
        loop = asyncio.get_running_loop()
        try:
            snaps = await loop.run_in_executor(
                _get_executor(),
                self.signal_store.load_snapshots,
                int(self.args.max_records or 20000),
            )
        except Exception as e:
            logger.error("load_snapshots failed: %s", e)
            return []
        if not snaps:
            return []
        return self.trend_analyzer.analyze(snaps, window=self.trend_window)

    def _compute_calibration(
        self,
        records: List[PerformanceRecord],
        signal_trends: List[SignalTrend],
    ) -> CalibrationReport:
        # v6.8.0: derive the verdict-stability mix from the trends already
        # computed this cycle (no extra load), then calibrate the in-memory
        # matured records.
        mix: Dict[str, int] = {}
        for t in signal_trends or []:
            d = str(getattr(t, "action_direction", "") or "?")
            mix[d] = mix.get(d, 0) + 1
        return self.calibrator.calibrate(records, signal_mix=mix)

    async def audit_active_records(
        self, records: List[PerformanceRecord]
    ) -> List[PerformanceRecord]:
        active = [
            r for r in records if r.status == PerformanceStatus.ACTIVE and r.symbol
        ]
        if not active:
            return records

        syms = list(dict.fromkeys([r.symbol for r in active]))
        if not self.backend.base_url:
            # v6.18.0 (PRICE-FEED-LOUD): this exact silent {} froze all 2,264
            # records at entry price on 2026-07-12 — and would have expired the
            # whole first cohort UNPRICED. Never silent again.
            price_map: Dict[str, float] = {}
            logger.error(
                "%s backend base_url is EMPTY — %d active symbols cannot be "
                "priced; fresh-only maturation will hold/expire the cohort. "
                "Set the backend URL env for this job.",
                _PRICE_FEED_TAG, len(syms),
            )
        else:
            price_map = await self.backend.fetch_prices(syms)
            _dead = _price_feed_dead(syms, price_map)
            if _dead:
                logger.error("%s %s — fresh-only maturation will hold/expire "
                             "affected records.", _PRICE_FEED_TAG, _dead)
        now_r = RiyadhTime.now()

        _fresh_only = _mature_fresh_only_enabled()
        _grace_d = _mature_grace_days()
        _n_matured = 0
        _pending_syms: List[str] = []
        _expired_syms: List[str] = []

        for r in active:
            px = float(price_map.get(r.symbol, 0.0))
            _fresh = px > 0.0
            if _fresh:
                r.current_price = px
                if r.entry_price > 0:
                    r.unrealized_roi = (px / r.entry_price - 1.0) * 100.0

            if r.target_date and now_r >= r.target_date:
                if not _fresh_only:
                    # v6.16.0 VERBATIM (kill-switch path): mature on whatever
                    # unrealized_roi is stored — stale or default-0.0 included.
                    r.status = PerformanceStatus.MATURED
                    r.maturity_date = now_r
                    r.realized_roi = float(r.unrealized_roi)
                    if (r.realized_roi or 0.0) > 0:
                        r.outcome = "WIN"
                    elif (r.realized_roi or 0.0) < 0:
                        r.outcome = "LOSS"
                    else:
                        r.outcome = "BREAKEVEN"
                elif _fresh and r.entry_price > 0:
                    # v6.17.0 (A): fresh positive price THIS run -> honest
                    # maturation on today's realized number.
                    # v6.18.0 (CA-GUARD): a big print-ROI is verified against
                    # the ADJUSTED series before it becomes a WIN/LOSS — a
                    # bonus issue / split between entry and maturity fakes the
                    # print ratio (1:1 bonus => -50%; 1-for-10 reverse =>
                    # +900%). Verified-agree matures as-is; disagree matures on
                    # the adjusted ROI; unverifiable expires CORP_ACTION_SUSPECT
                    # (excluded like UNPRICED). TFB_TRACK_CA_GUARD=0 -> v6.17.0.
                    _roi_print = float(r.unrealized_roi or 0.0)
                    _verdict, _roi_use = "ok", _roi_print
                    if _ca_guard_enabled() and abs(_roi_print) >= _ca_verify_pct():
                        _adj = _ca_adjusted_roi(r.symbol, r.date_recorded)
                        _verdict, _roi_use = _ca_decide(_roi_print, _adj)
                    if _verdict == "suspect":
                        r.status = PerformanceStatus.EXPIRED
                        r.maturity_date = now_r
                        r.realized_roi = None
                        r.outcome = "CORP_ACTION_SUSPECT"
                        _note = (f"{_CA_TAG} |print ROI| {_roi_print:+.1f}% >= "
                                 f"{_ca_verify_pct():.0f}% but adjusted history "
                                 f"unavailable — excluded, never a fake outcome")
                        r.notes = f"{r.notes} | {_note}" if r.notes else _note
                        _expired_syms.append(r.symbol)
                        r.last_updated = _utc_now()
                        continue
                    if _verdict == "adjust":
                        _note = (f"[v6.18.0 CA-ADJUST] print {_roi_print:+.1f}% "
                                 f"vs adjusted {_roi_use:+.1f}% — corporate "
                                 f"action re-based the prints; matured on the "
                                 f"adjusted series")
                        r.notes = f"{r.notes} | {_note}" if r.notes else _note
                        r.unrealized_roi = float(_roi_use)
                    r.status = PerformanceStatus.MATURED
                    r.maturity_date = now_r
                    r.realized_roi = float(_roi_use)
                    if (r.realized_roi or 0.0) > 0:
                        r.outcome = "WIN"
                    elif (r.realized_roi or 0.0) < 0:
                        r.outcome = "LOSS"
                    else:
                        r.outcome = "BREAKEVEN"
                    _n_matured += 1
                else:
                    # v6.17.0 (B): past due but no fresh price (or no usable
                    # entry price) -> hold ACTIVE and retry, then expire
                    # UNPRICED after the grace window. EXPIRED + realized None
                    # is structurally excluded from every statistic.
                    _overdue = (now_r - r.target_date).days
                    if _overdue > _grace_d:
                        r.status = PerformanceStatus.EXPIRED
                        r.maturity_date = now_r
                        r.realized_roi = None
                        r.outcome = "UNPRICED"
                        _note = (
                            f"{_MATURE_FRESH_TAG} expired unpriced {_overdue}d "
                            f"past target (no fresh price this run)"
                        )
                        r.notes = f"{r.notes} | {_note}" if r.notes else _note
                        _expired_syms.append(r.symbol)
                    else:
                        _pending_syms.append(r.symbol)

            r.last_updated = _utc_now()

        if _fresh_only and (_n_matured or _pending_syms or _expired_syms):
            logger.info(
                "%s matured-fresh=%d | pending-price=%d%s | expired-unpriced=%d%s",
                _MATURE_FRESH_TAG, _n_matured,
                len(_pending_syms),
                f" ({', '.join(_pending_syms[:8])}{'…' if len(_pending_syms) > 8 else ''})" if _pending_syms else "",
                len(_expired_syms),
                f" ({', '.join(_expired_syms[:8])}{'…' if len(_expired_syms) > 8 else ''})" if _expired_syms else "",
            )

        return records

    async def run_once(self) -> int:
        loop = asyncio.get_running_loop()
        records: List[PerformanceRecord] = []

        if self.store.is_available():
            records = await loop.run_in_executor(
                _get_executor(),
                self.store.load_records,
                int(self.args.max_records or 10000),
            )
        else:
            records = []

        if self.args.record:
            # v6.7.0: fetch the Top10 universe once, feed both pipelines.
            prefetched: Optional[List[Dict[str, Any]]] = None
            if (
                self.signal_history_enabled
                and self.signal_store is not None
                and self.backend.base_url
            ):
                try:
                    prefetched, _pf_meta = await self.backend.get_top10_rows(
                        criteria_overrides=None
                    )
                except Exception as e:
                    logger.warning("Top10 prefetch failed: %s", e)
                    prefetched = None

            # v6.16.0 (Fix #5): guarantee decision-symbol coverage for BOTH
            # consumers below. Fail-open; kill switch skips the call entirely
            # (v6.15.0 None-on-failure semantics preserved byte-identically).
            if _force_decision_enabled():
                try:
                    prefetched = await self._augment_with_decision_symbols(prefetched)
                except Exception as e:
                    logger.warning("decision-symbol coverage failed: %s", e)

            new = await self.record_from_top10(records, rows=prefetched)
            if new and self.store.is_available():
                ok = await loop.run_in_executor(
                    _get_executor(), self.store.append_records, new
                )
                if ok:
                    records.extend(new)

            # v6.7.0: log today's verdict snapshot per decision symbol.
            if prefetched is not None:
                await self.record_signal_snapshots(prefetched)

        if self.args.audit:
            records = await self.audit_active_records(records)
            if self.store.is_available():
                await loop.run_in_executor(
                    _get_executor(), self.store.save_records, records
                )

        summary = (
            self.analyzer.analyze(records)
            if self.args.analyze or self.args.export
            else PerformanceSummary(total_records=len(records))
        )
        try:
            perf_records_processed.inc(len(records))
        except Exception:
            pass

        # v6.7.0: compute day-to-day action-trends once (reused by --analyze
        # stdout, --calibrate mix, and --export json). Empty unless
        # Signal_History has data.
        signal_trends: List[SignalTrend] = []
        if (
            (self.args.analyze or self.args.export or self.args.calibrate)
            and self.signal_history_enabled
            and self.signal_store is not None
        ):
            signal_trends = await self._compute_signal_trends()
            # v6.13.0: persist the just-computed trajectory to the
            # user-visible Signal_Trends tab. Off the request path (cron),
            # gated, fail-open -- a Sheets hiccup here must never break
            # --analyze. No-op unless TFB_SIGNAL_TRENDS_TAB is set.
            if (
                self.signal_trends_enabled
                and signal_trends
                and self.signal_trend_store is not None
                and self.signal_trend_store.is_available()
            ):
                try:
                    _tl = asyncio.get_running_loop()
                    _tw = await _tl.run_in_executor(
                        _get_executor(),
                        self.signal_trend_store.write_trends,
                        signal_trends,
                    )
                    logger.info(
                        "signal-trends tab: wrote %d trajectory row(s)",
                        int(_tw or 0),
                    )
                except Exception as e:
                    logger.error("signal-trends tab write failed: %s", e)

        # v6.8.0: reliability calibration on the records already loaded this
        # cycle (no extra I/O). Built when a consumer asks for it.
        calibration: Optional[CalibrationReport] = None
        if (
            self.calibration_enabled
            and (self.args.analyze or self.args.calibrate or self.args.export)
        ):
            calibration = self._compute_calibration(records, signal_trends)

        if self.args.analyze:
            if self.store.is_available():
                await loop.run_in_executor(
                    _get_executor(), self.store.update_summary, summary
                )
            _out("=" * 66)
            _out("📊 PERFORMANCE SUMMARY")
            _out("=" * 66)
            _out(
                f"Total: {summary.total_records} | Active: {summary.active_records} "
                f"| Matured: {summary.matured_records}"
            )
            _out(
                f"Wins: {summary.wins} | Losses: {summary.losses} "
                f"| WinRate: {summary.win_rate:.1f}%"
            )
            _out(
                f"Avg ROI: {summary.avg_roi:.2f}% "
                f"| Sharpe: {summary.sharpe_ratio:.2f} "
                f"| Sortino: {summary.sortino_ratio:.2f}"
            )
            # v6.6.0: surface the investability segmentation -- the gate KPI
            # that the whole "95%" question reduces to. Decided-only win-rate
            # per bucket, with the matured sample size in parentheses.
            if summary.hit_rate_by_investability:
                inv_bits = ", ".join(
                    "%s=%.0f%% (n=%d)"
                    % (
                        k,
                        summary.hit_rate_by_investability[k],
                        int(
                            summary.performance_by_investability.get(k, {}).get(
                                "count", 0
                            )
                        ),
                    )
                    for k in sorted(summary.hit_rate_by_investability.keys())
                )
                _out(f"Win-rate by investability: {inv_bits}")

            # v6.7.0: day-to-day action-trend across decision symbols. Reads
            # the Signal_History accumulated by --record. Needs >=2 days of
            # history to characterise direction; day 1 shows NEW.
            if signal_trends:
                _out("-" * 66)
                _out(
                    f"ACTION-TREND (day-to-day, window={self.trend_window}) "
                    f"— {len(signal_trends)} symbol(s)"
                )
                latest_day = signal_trends[0].latest_date
                shown = [t for t in signal_trends if t.latest_date == latest_day][:20]
                for t in shown:
                    _out(
                        f"  {t.symbol:<12} {t.summary_label}  "
                        f"[{t.stability}, {t.n_observations} obs, flips={t.flip_count}]"
                    )

        # v6.8.0: render reliability calibration once (under --analyze or
        # --calibrate), from the report built above.
        if calibration is not None and (self.args.analyze or self.args.calibrate):
            _out("")
            render_calibration_report(calibration, verbose=bool(self.args.verbose))

        if self.args.simulate:
            matured = [
                r
                for r in records
                if r.status == PerformanceStatus.MATURED
                and r.realized_roi is not None
            ]
            outcomes = [(r.realized_roi or 0.0) > 0 for r in matured]
            if outcomes:
                ci = self.analyzer.sim.simulate_win_rate(
                    outcomes,
                    iterations=int(self.args.iterations or 10000),
                    confidence=float(self.args.confidence or 0.95),
                )
                _out(
                    f"Win-Rate Simulation (mean): {ci['mean']*100:.1f}% "
                    f"| CI: [{ci['ci_lower']*100:.1f}%, {ci['ci_upper']*100:.1f}%] "
                    f"| observed: {ci['observed']*100:.1f}%"
                )
            else:
                _out("Simulation skipped: no matured outcomes.")

        if self.args.export:
            out_base = (
                self.args.output
                or f"performance_report_{RiyadhTime.format(fmt='%Y%m%d_%H%M%S')}"
            ).strip()
            fmt = (self.args.format or "html").lower()
            if fmt in {"json", "all"}:
                Path(out_base + ".json").write_text(
                    self.reporter.generate_json(records, summary), encoding="utf-8"
                )
            if fmt in {"csv", "all"}:
                self.reporter.generate_csv(records, out_base + ".csv")
            if fmt in {"html", "all"}:
                self.reporter.generate_html(records, summary, out_base + ".html")
            if signal_trends:
                try:
                    Path(out_base + "_signal_trends.json").write_text(
                        json_dumps([t.to_dict() for t in signal_trends], indent=2),
                        encoding="utf-8",
                    )
                    _out(f"Signal-trend export: {out_base}_signal_trends.json")
                except Exception as e:
                    logger.warning("signal-trend export failed: %s", e)
            if calibration is not None:
                try:
                    Path(out_base + "_calibration.json").write_text(
                        json_dumps(calibration.to_dict(), indent=2),
                        encoding="utf-8",
                    )
                    _out(f"Calibration export: {out_base}_calibration.json")
                except Exception as e:
                    logger.warning("calibration export failed: %s", e)
            _out(f"Export complete: {out_base}.* ({fmt})")

        return 0

    # =========================================================================
    # v6.10.0 BACKTEST HARNESS MODE (distinct from the forward-tracking path).
    # Reads historical OHLCV (EODHD) + the LIVE engine reads; writes
    # Hypothesis_Registry. Touches NEITHER Performance_Log NOR the record/audit
    # flow -- this is the evidence gate the Strategy requires before any
    # event/theme factor may influence a recommendation.
    # =========================================================================
    async def run_backtest_mode(self) -> int:
        registry = HypothesisRegistry(
            self.spreadsheet_id,
            os.getenv("TRACK_HYPOTHESIS_SHEET", "Hypothesis_Registry"),
        )
        existing = registry.load() if registry.is_available() else []

        if getattr(self.args, "register_hypotheses", False):
            merged = registry.register_defaults(existing)
            if registry.is_available():
                registry.save(merged)
            _out("[backtest] registered %d hypotheses (%d new)\n" % (
                len(merged), max(0, len(merged) - len(existing))))
            existing = merged

        if not existing:
            existing = default_hypotheses()
            _out("[backtest] registry empty/unavailable -> %d in-memory defaults\n" % len(existing))

        if getattr(self.args, "list_hypotheses", False) and not self.args.backtest:
            for h in existing:
                _out("  %-20s %-16s %-22s %-8s %s\n" % (
                    h.hypothesis_id, h.hypothesis_type.value, h.trigger_value,
                    h.direction.value, h.status.value))
            return 0

        if not self.args.backtest:
            return 0

        want_id = (getattr(self.args, "hypothesis", "") or "").strip().upper()
        selected = [h for h in existing if (not want_id or h.hypothesis_id.upper() == want_id)]
        testable = [h for h in selected if h.hypothesis_type == HypothesisType.PRICE_STRUCTURE]
        for h in (h for h in selected if h.hypothesis_type != HypothesisType.PRICE_STRUCTURE):
            _out("[backtest] SKIP %s (%s not yet testable -- needs a historical archive)\n" % (
                h.hypothesis_id, h.hypothesis_type.value))
        if not testable:
            _out("[backtest] no testable (PRICE_STRUCTURE) hypotheses selected\n")
            return 0

        days = int(getattr(self.args, "backtest_days", 0) or _BT_DEFAULT_DAYS)
        universe = self._backtest_universe()
        _out("[backtest] fetching %d days history for %d symbols...\n" % (days, len(universe)))
        history = await self._fetch_backtest_history(universe, days)
        got = sum(1 for v in history.values() if v)
        _out("[backtest] history fetched for %d/%d symbols\n" % (got, len(universe)))
        if got == 0:
            _out("[backtest] no history -> aborting (EODHD unavailable?)\n")
            return 1

        loop = asyncio.get_running_loop()
        results: List[Hypothesis] = []
        for h in testable:
            res = await loop.run_in_executor(_get_executor(), run_backtest, h, history)
            results.append(res)
            _out("  %-20s %-18s n=%-5d hit=%.0f%% [%.0f-%.0f] eff=%+.0fbps t=%+.2f\n" % (
                res.hypothesis_id, res.status.value, res.sample_size,
                res.hit_rate, res.hit_rate_lo, res.hit_rate_hi,
                res.effect_bps, res.t_stat))

        if registry.is_available():
            by_id = {h.hypothesis_id: h for h in existing}
            for r in results:
                by_id[r.hypothesis_id] = r
            registry.save(list(by_id.values()))
            _out("[backtest] results written to %s\n" % registry.sheet_name)

        accepted = [r for r in results if r.status == HypothesisStatus.ACCEPTED]
        _out("[backtest] ACCEPTED %d/%d (only ACCEPTED may influence a recommendation)\n" % (
            len(accepted), len(results)))
        return 0

    def _backtest_universe(self) -> List[str]:
        """Symbols to backtest over. Env TRACK_BACKTEST_UNIVERSE (CSV) or a
        liquid KSA+US default for statistical power."""
        env = _env_csv("TRACK_BACKTEST_UNIVERSE", None)
        if env:
            return [s.strip().upper() for s in env if s.strip()]
        return [
            "1120.SR", "2222.SR", "1180.SR", "2010.SR", "7010.SR", "1211.SR",
            "4013.SR", "1010.SR", "2350.SR", "4002.SR",
            "AAPL.US", "MSFT.US", "NVDA.US", "JPM.US", "INTC.US", "T.US",
        ]

    async def _fetch_backtest_history(
        self, symbols: List[str], days: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch long OHLCV history per symbol using the SAME provider preference
        the live engine uses: yahoo_chart FIRST (serves Tadawul .SR with volume),
        EODHD as fallback. The backend exposes no history endpoint.

        v6.10.1: was EODHD-only -- EODHD returns 404 for every .SR ticker (no
        Tadawul EOD coverage on this plan), which silently dropped the ENTIRE KSA
        book from the backtest (only US names survived). Routing yahoo-first means
        the backtest reads the same bars the live flow/candle path already sees,
        so a verdict actually transfers to production. Each symbol's bars are
        date-sorted ASCENDING so the no-lookahead walk is correct regardless of
        provider ordering. Concurrency-limited; fail-soft per symbol and provider.
        """
        out: Dict[str, List[Dict[str, Any]]] = {}

        # provider modules in engine-preference order; import lazily, tolerate
        # absence of either.
        provider_specs = [
            ("core.providers.yahoo_chart_provider", "yahoo_chart_provider"),
            ("core.providers.eodhd_provider", "eodhd_provider"),
        ]
        providers: List[Any] = []
        for _dotted, _bare in provider_specs:
            _mod = None
            for _path in (_dotted, _bare):
                try:
                    _mod = __import__(_path, fromlist=["fetch_history"])
                    break
                except Exception:
                    continue
            if _mod is not None:
                providers.append(_mod)
        if not providers:
            logger.error("no history provider importable -- cannot fetch history")
            return out

        _hist_fns = ("fetch_history", "fetch_price_history", "fetch_ohlc_history")

        def _sort_bars(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            # ISO 'YYYY-MM-DD' date strings sort lexically == chronologically.
            try:
                return sorted(
                    bars,
                    key=lambda r: str(
                        (r or {}).get("date")
                        or (r or {}).get("datetime")
                        or (r or {}).get("timestamp")
                        or ""
                    ),
                )
            except Exception:
                return bars

        async def _fetch_from(mod: Any, sym: str) -> Optional[List[Dict[str, Any]]]:
            for _fn_name in _hist_fns:
                fn = getattr(mod, _fn_name, None)
                if fn is None:
                    continue
                try:
                    bars = await fn(sym, days=days)
                except TypeError:
                    try:
                        bars = await fn(sym)
                    except Exception:
                        continue
                except Exception:
                    continue
                if isinstance(bars, list) and bars:
                    return bars
            return None

        # v6.11.0: deep-KSA fallback config (default OFF -> routing identical to
        # v6.10.1). When on, shallow/missing symbols are re-pulled from yfinance.
        ksa_yf = _env_bool("TFB_BACKTEST_KSA_YF", False)
        ksa_min_bars = _env_int("TFB_BACKTEST_KSA_MIN_BARS", 60, lo=20, hi=400)
        ksa_period = (os.getenv("TFB_BACKTEST_KSA_YF_PERIOD", "2y") or "2y").strip() or "2y"

        sem = asyncio.Semaphore(max(1, _env_int("BACKTEST_CONCURRENCY", 6, lo=1, hi=16)))

        async def _one(sym: str) -> None:
            async with sem:
                chosen: Optional[List[Dict[str, Any]]] = None
                for mod in providers:
                    try:
                        bars = await _fetch_from(mod, sym)
                    except Exception as e:
                        logger.warning(
                            "history fetch error %s via %s: %s",
                            sym, getattr(mod, "__name__", mod), e,
                        )
                        bars = None
                    if bars:
                        chosen = bars
                        break
                # v6.11.0: shallow/missing (chiefly .SR capped at ~22 bars by the
                # yahoo_chart shim) -> pull deep from yfinance at period=2y and keep
                # whichever source is deeper. Skipped entirely when ksa_yf is OFF.
                if ksa_yf and (chosen is None or len(chosen) < ksa_min_bars):
                    try:
                        yb = await asyncio.get_event_loop().run_in_executor(
                            None, _yf_deep_history, sym, ksa_period)
                    except Exception:
                        yb = None
                    if yb and (chosen is None or len(yb) > len(chosen)):
                        chosen = yb
                if chosen:
                    out[sym] = _sort_bars(chosen)
                else:
                    logger.warning("no history for %s from any provider", sym)

        await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=True)
        return out

    async def run_daemon(self) -> int:
        interval = max(30, int(self.args.interval or 3600))
        _out(f"Daemon mode ON (interval={interval}s). Press Ctrl+C to stop.")
        while not self.stop_event.is_set():
            try:
                try:
                    perf_daemon_cycles.inc()
                except Exception:
                    pass
                await self.run_once()
            except Exception as e:
                logger.exception("Daemon cycle failed: %s", e)
            # sleep in small steps for fast stop
            for _ in range(interval):
                if self.stop_event.is_set():
                    break
                await asyncio.sleep(1.0)
        return 0

    def stop(self) -> None:
        self.stop_event.set()


# =============================================================================
# CLI
# =============================================================================
def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=f"Tadawul Fast Bridge - Performance Tracker v{SCRIPT_VERSION}"
    )
    p.add_argument(
        "--sheet-id",
        default=os.getenv("TRACK_SHEET_ID") or None,
        help="Spreadsheet ID (also TRACK_SHEET_ID / DEFAULT_SPREADSHEET_ID env).",
    )
    p.add_argument(
        "--sheet-name",
        default=os.getenv("TRACK_SHEET_NAME", "Performance_Log"),
        help="Performance log tab name (also TRACK_SHEET_NAME env; default Performance_Log).",
    )
    p.add_argument(
        "--record",
        action="store_true",
        default=_env_bool("TRACK_RECORD", False),
        help=(
            "Record new recommendations from /v1/advanced/top10-investments "
            "(also TRACK_RECORD env)."
        ),
    )
    p.add_argument(
        "--audit",
        action="store_true",
        default=_env_bool("TRACK_AUDIT", False),
        help="Audit/update active records (also TRACK_AUDIT env).",
    )
    p.add_argument(
        "--analyze",
        action="store_true",
        default=_env_bool("TRACK_ANALYZE", False),
        help="Analyze performance + write summary block in sheet (also TRACK_ANALYZE env).",
    )
    p.add_argument(
        "--simulate",
        action="store_true",
        default=_env_bool("TRACK_SIMULATE", False),
        help="Monte Carlo win-rate simulation (requires numpy; also TRACK_SIMULATE env).",
    )
    p.add_argument(
        "--export",
        action="store_true",
        default=_env_bool("TRACK_EXPORT", False),
        help="Export performance report (also TRACK_EXPORT env).",
    )
    p.add_argument(
        "--calibrate",
        action="store_true",
        default=_env_bool("TRACK_CALIBRATE", False),
        help=(
            "Print the reliability-calibration report (realized vs stated "
            "confidence, Wilson intervals, sample-gated). Also runs under "
            "--analyze unless TRACK_CALIBRATION=0. Env: TRACK_CALIBRATE."
        ),
    )
    p.add_argument(
        "--min-sample",
        type=int,
        default=_env_int("CAL_MIN_SAMPLE", 10, lo=1),
        help=(
            "Min decided outcomes before a calibration point estimate is "
            "shown (default 10; env CAL_MIN_SAMPLE)."
        ),
    )
    p.add_argument(
        "--daemon",
        action="store_true",
        default=_env_bool("TRACK_DAEMON", False),
        help="Run continuously (daemon; also TRACK_DAEMON env).",
    )
    p.add_argument(
        "--horizons",
        nargs="+",
        default=_env_csv("TRACK_HORIZONS", ["1M", "3M"]),
        help=(
            "Horizons to track, space- or comma-separated "
            "(e.g., 1W 1M 3M 6M 1Y; also TRACK_HORIZONS env as CSV)."
        ),
    )
    p.add_argument(
        "--max-records",
        type=int,
        default=_env_int("TRACK_MAX_RECORDS", 10000, lo=1),
        help="Max records to load from sheet (also TRACK_MAX_RECORDS env).",
    )
    p.add_argument(
        "--confidence",
        type=float,
        default=_env_float("TRACK_CONFIDENCE", 0.95),
        help="Confidence level for simulation (also TRACK_CONFIDENCE env).",
    )
    p.add_argument(
        "--iterations",
        type=int,
        default=_env_int("TRACK_ITERATIONS", 10000, lo=100),
        help="Monte Carlo iterations (also TRACK_ITERATIONS env).",
    )
    p.add_argument(
        "--format",
        choices=["json", "csv", "html", "all"],
        default=(os.getenv("TRACK_FORMAT", "html") or "html").lower(),
        help="Export format (also TRACK_FORMAT env).",
    )
    p.add_argument(
        "--output",
        default=os.getenv("TRACK_OUTPUT") or None,
        help="Output file base path without extension (also TRACK_OUTPUT env).",
    )
    p.add_argument(
        "--interval",
        type=int,
        default=_env_int("TRACK_INTERVAL", 3600, lo=30),
        help="Daemon interval seconds (also TRACK_INTERVAL env).",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=_env_bool("TRACK_VERBOSE", False),
        help="Verbose logs (also TRACK_VERBOSE env).",
    )
    p.add_argument(
        "--backtest",
        action="store_true",
        default=_env_bool("TRACK_BACKTEST", False),
        help=(
            "Run the hypothesis backtest harness: a forward-return event study "
            "on historical OHLCV that re-runs the live engine reads (flow / "
            "candle structure) with no lookahead. Env TRACK_BACKTEST."
        ),
    )
    p.add_argument(
        "--register-hypotheses",
        action="store_true",
        default=_env_bool("TRACK_REGISTER_HYPOTHESES", False),
        help="Seed the default flow/candle hypotheses into Hypothesis_Registry (idempotent).",
    )
    p.add_argument(
        "--list-hypotheses",
        action="store_true",
        default=_env_bool("TRACK_LIST_HYPOTHESES", False),
        help="List registered hypotheses + their latest status, then stop.",
    )
    p.add_argument(
        "--hypothesis",
        default=os.getenv("TRACK_HYPOTHESIS", "") or "",
        help="Backtest only this hypothesis id (default: all testable).",
    )
    p.add_argument(
        "--backtest-days",
        type=int,
        default=_env_int("BACKTEST_HISTORY_DAYS", 730, lo=120, hi=5000),
        help="History window (days) for the backtest (default 730; env BACKTEST_HISTORY_DAYS).",
    )
    return p


async def async_main() -> int:
    args = create_parser().parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # If nothing selected, show help
    if not any(
        [args.record, args.audit, args.analyze, args.simulate, args.export,
         args.calibrate, args.daemon, args.backtest, args.register_hypotheses,
         args.list_hypotheses]
    ):
        _out(create_parser().format_help())
        return 0

    try:
        app = PerformanceTrackerApp(args)
    except Exception as e:
        logger.error("Failed to initialize PerformanceTrackerApp: %s", e)
        return 1

    # v6.4.0: clean diagnostic instead of raised exception
    if not app.spreadsheet_id:
        logger.error(
            "No spreadsheet ID provided. Use --sheet-id or set "
            "TRACK_SHEET_ID / DEFAULT_SPREADSHEET_ID."
        )
        return 1

    def _sig_handler(_signum, _frame):
        app.stop()

    try:
        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)
    except Exception:
        pass

    try:
        if args.backtest or args.register_hypotheses or args.list_hypotheses:
            return await app.run_backtest_mode()
        if args.daemon:
            return await app.run_daemon()
        return await app.run_once()
    finally:
        _shutdown_executor()


def main() -> int:
    if sys.platform == "win32":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 1
    finally:
        _shutdown_executor()


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "SCRIPT_NAME",
    "PerformanceStatus",
    "HorizonType",
    "RecommendationType",
    "PerformanceRecord",
    "PerformanceSummary",
    "BackendClient",
    "PerformanceStore",
    "PerformanceAnalyzer",
    "ReportGenerator",
    "SignalSnapshot",
    "SignalHistoryStore",
    "SignalTrend",
    "SignalTrendAnalyzer",
    "wilson_interval",
    "shrink_factor",
    "CalibrationBucket",
    "CalibrationReport",
    "ReliabilityCalibrator",
    "render_calibration_report",
    "HypothesisType",
    "HypothesisStatus",
    "HypothesisDirection",
    "Hypothesis",
    "HypothesisRegistry",
    "default_hypotheses",
    "make_trigger_fn",
    "run_backtest",
    "PerformanceTrackerApp",
    "create_parser",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
