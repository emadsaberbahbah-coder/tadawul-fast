/**
 * 02_Core.gs
 *
 * v2.11.1 (2026-07-30) — RE-ENTRANT DOCUMENT LOCK FOR MANUAL PRIORITY
 * --------------------------------------------------------------------------
 * INTEGRATION CORRECTION: menu-level manual priority owns DocumentLock before
 * calling the existing page wrappers, and those wrappers call withScriptLock_
 * again. v2.11.1 makes the shared lock re-entrant WITHIN ONE execution:
 * nested withScriptLock_ calls reuse the outer ownership and never release it.
 * Cross-execution exclusion is unchanged. No writer or HTTP logic changed.
 *
 * OPERATOR ISSUE: a time-driven auto-refresh or auto-resume execution can hold
 * the shared DocumentLock long enough that a menu-triggered manual refresh
 * reaches the legacy 30-second lock timeout and fails. The automatic path then
 * keeps retrying every five minutes, but there is no durable signal telling it
 * that an operator is waiting.
 *
 * FIX (coordination layer only; no page writer or investment logic changed):
 *   1. A TTL-backed Script Property records one manual refresh request before
 *      it waits for the DocumentLock. New automatic executions can see the
 *      request and skip; an automatic batch already running can see it at the
 *      next safe checkpoint and yield.
 *   2. withManualRefreshPriority_ waits for the DocumentLock in short polling
 *      slices, marks the request ACTIVE only after the lock is obtained, runs
 *      the supplied refresh body, flushes pending Sheet writes, and clears the
 *      request in finally on success, error, or timeout.
 *   3. tfbAutomaticYieldPoint_ is a side-effect-free cooperative checkpoint for
 *      05_Refresh.gs. It never interrupts a write; callers invoke it only after
 *      a completed page/batch and a persisted checkpoint.
 *   4. withAutomaticRefreshPriorityLock_ gives lockless cockpit refreshers a
 *      safe automatic wrapper: manual priority is checked both before and
 *      immediately after DocumentLock acquisition.
 *   5. Duplicate manual requests are rejected while one non-expired request is
 *      pending/active. Token ownership prevents an older execution from clearing
 *      a newer request. TTL expiry prevents a crash from pausing automation
 *      permanently.
 *
 * SCRIPT PROPERTIES (all optional):
 *   TFB_MANUAL_REFRESH_PRIORITY_ENABLED   default 1; 0/off disables coordinator
 *   TFB_MANUAL_REFRESH_PRIORITY_TTL_SEC   default 1200; clamp 120..3600
 *   TFB_MANUAL_REFRESH_LOCK_WAIT_SEC      default 240; clamp 30..300
 *   TFB_MANUAL_REFRESH_LOCK_POLL_MS       default 3000; clamp 500..10000
 *
 * DEPLOYMENT BOUNDARY: this file only provides the shared primitives. Live
 * behavior changes only after 01_Menu.gs, 05_Refresh.gs and 13_AutoRefresh.gs
 * are revised to call them. Existing withScriptLock_ semantics are preserved.
 *
 * v2.10.0 (2026-07-16) — WRITER SLIM-READ (universe-expansion quota fix)
 * --------------------------------------------------------------------------
 * WHY: writeRowsBySymbol_ read the ENTIRE data block
 * (lastRow x lastCol getValues) on EVERY call to build its symbol->row map
 * and to source preserved-column values — and the batch refresh calls the
 * writer once per 10-symbol chunk. At the 2026-07 expanded Global_Markets
 * size (6,512 rows x 115 cols ~= 749k cells) that is ~749k cells read per
 * chunk x ~652 chunks: the read, not the write, becomes the execution-time
 * budget, stretching a manual full-page refresh to multiple hours of
 * trigger fires.
 * FIX: the map is now built from a SYMBOL-COLUMN-ONLY read (~6.5k cells),
 * and the existing-row values needed for manual / preserve-on-empty /
 * unmapped-header preservation are fetched LAZILY — one single-row read per
 * matched incoming symbol (<= chunk size per call), cached for the call.
 * Net per-chunk read cost drops ~100x; write mechanics, PLACE-GUARD
 * (v2.8.1), APPEND-GUARD + second-chance (v2.9.0), status-bleed
 * sanitization (v2.5.4) and every result counter are UNCHANGED — the
 * guards always did their own live reads and are untouched.
 * KILL-SWITCH: Script Property TFB_CORE_WRITER_SLIM_READ = 0/false/off/no
 * restores the v2.9.1 full-block read byte-for-byte (the legacy code path
 * is preserved verbatim, not emulated).
 *
 * Tadawul Tracker — Core Utilities, HTTP, Locking, Logging, Payload Handling
 * Version: 2.11.1
 *
 * v2.9.1 (2026-07-12) — SELF-VERIFY INTEGRITY + ALIGNMENT REFRESH
 * (zero write-path changes)
 * --------------------------------------------------------------------------------
 * WHY: verifyCoreVersion_ had been asserting expected='2.8.0' against
 * CORE_SCRIPT_VERSION_='2.8.0' since the v2.8.0 release — the constant
 * was never bumped through v2.8.1 (place-guard) OR v2.9.0 (append-
 * guard), so for two releases the file's banner said one version while
 * its self-verify green-lit another and checked NONE of the concurrency
 * guards those releases exist for. A self-test that cannot fail is
 * worse than no self-test: it manufactures false confidence exactly
 * where the operator looks for assurance. Found in the 2026-07-12
 * full-file review.
 * FIX (verification + documentation only — the write paths are
 * byte-identical to v2.9.0):
 *   1. CORE_SCRIPT_VERSION_ 2.8.0 -> 2.9.1; verifyCoreVersion_
 *      expected 2.9.1 with a version-equality check that now moves
 *      with the constant.
 *   2. verifyCoreVersion_ extended to cover the concurrency layer:
 *      _corePlaceGuardEnabled_ (v2.8.1), _coreAppendGuardEnabled_
 *      (v2.9.0), and writeRowsBySymbol_ existence join the v2.8.0
 *      gate checks. Log tag [v2.9.1 CORE VERIFY].
 *   3. 'Aligned with' refreshed to the 2026-07-12 family:
 *      05_Refresh.gs v1.14.0 (keep-last-good batch caller; consumes
 *      placementAborted / appendAborted / secondChanceUpdated /
 *      deferredSymbols per the v1.13.0 lossless-deferral contract),
 *      01_Menu.gs v2.15.0, 13_AutoRefresh.gs v1.9.1 (file renamed
 *      from 09_AutoRefresh).
 * TAG STABILITY: [v2.5.1 VERDICT], [v2.8.1 PLACE-GUARD], and
 * [v2.9.0 APPEND-GUARD] log tags are carried verbatim — cross-file
 * consumers grep for them. Only the CORE VERIFY log tag (no external
 * consumers) moves to v2.9.1.
 *
 * v2.9.0 (2026-07-08) — APPEND PLACE-GUARD + LOSSLESS DEFERRAL
 * (concurrent-writer hardening, part 2)
 * --------------------------------------------------------------------------------
 * WHY: the v2.8.1 place-guard protects UPDATE blocks, but the APPEND
 * path stayed blind. writeRowsBySymbol_ routes a symbol to append when
 * the symbolToRow map — built from ONE read at call start — does not
 * contain it. With a concurrent full-table writer (the GitHub
 * daily_sync) rewriting the page in the read->write gap, two live
 * failure modes follow, both measured on the 2026-07-08 v36 export:
 *   (a) DUPLICATE MINTING — a symbol that exists on the sheet but is
 *       missing from the stale map is appended AGAIN (AAPL twice at
 *       rows 590/1223, MET.US twice), and
 *   (b) APPEND-WINDOW COLLISION — two writers computing the same
 *       getLastRow()+1 anchor overwrite each other's freshly appended
 *       rows, silently erasing symbols (one mechanism behind the
 *       Global_Markets -75 symbol loss that day).
 * FIX (master Script Property TFB_CORE_APPEND_GUARD, default ON;
 * 0/false/off restores the exact v2.8.1 blind append):
 *   1. FRESH RE-READ: immediately before flushing appends, the symbol
 *      column is re-read; a routed-to-append symbol that exists NOW is
 *      written IN PLACE instead (per-row live verify first) — counter
 *      secondChanceUpdated. Duplicates can no longer be minted by map
 *      staleness.
 *   2. ANCHOR VERIFY: true appends require the target rows under
 *      getLastRow()+1 to read BLANK symbols immediately before the
 *      write (capacity ensured via insertRowsAfter); one re-anchor
 *      retry, then the whole append set is withheld fail-safe —
 *      counter appendAborted. A post-write witness read logs any
 *      overlap that still lands.
 *   3. LOSSLESS DEFERRAL: every row withheld by EITHER guard (the
 *      v2.8.1 placement aborts included) now reports its symbol in
 *      result.deferredSymbols so the batch caller (05_Refresh v1.13.0)
 *      re-queues it into the checkpoint — fresh data is retried, never
 *      dropped. New result fields: appendAborted, secondChanceUpdated,
 *      deferredSymbols. New '[v2.9.0 APPEND-GUARD]' log line. All
 *      v2.8.1 semantics, tags and guards carried verbatim.
 *
 * v2.8.1 (2026-07-07) — WRITE-PLACEMENT GUARD (concurrent-writer hardening)
 * -------------------------------------------------------------------------
 * WHY: writeRowsBySymbol_ builds its symbolToRow map from one read, then
 * flushes contiguous blocks with setValues. Google Sheets gives no
 * transaction between that read and those writes: a concurrent writer (the
 * GitHub daily_sync full-table write, a manual paste, a sort) can shift rows
 * in the gap, landing a block's full content under DIFFERENT symbols — the
 * exact whole-block transposition signature measured on 2026-07-07
 * (94.7% of Global_Markets names foreign at constant 25-multiple offsets).
 * FIX: immediately before each block flush, re-read ONLY the Symbol column
 * for the block's target span and require every live cell to still match the
 * symbol that block intends to write. Any mismatch aborts THAT block (rows
 * counted in result.placementAborted, WARN logged); matching blocks flush
 * unchanged. Cost: one narrow read per contiguous block. Gate: script
 * property TFB_CORE_PLACE_GUARD (default ON; set 0/off/false/no to restore
 * v2.8.0 byte-identical flushing).
 *
 *
 * Aligned with — June 2026 / current 115-118 investment-gate family
 * - 00_Config.gs v1.12.1    (HEADER_TO_KEY map: 209 entries
 *                            including v5.74 SCORING_V574 group [7]
 *                            — provider_rating,
 *                            scoring_recommendation_source,
 *                            scoring_schema_version, scoring_errors,
 *                            opportunity_source, overall_score_raw,
 *                            overall_penalty_factor; expectedColumnCount
 *                            106 canonical / 109 Top10; AUDIT-15
 *                            [11 sub-checks] guards SCORING_V574
 *                            including distinct-fields invariant
 *                            15e: 'scoring_recommendation_source'
 *                            MUST NOT alias to 'recommendation_source';
 *                            DECIMAL_FRACTION_FIELDS=15 UNCHANGED —
 *                            overall_penalty_factor is a fractional
 *                            multiplier (0.0–1.0), NOT a percent —
 *                            guarded by AUDIT-15f/f'/f''; AUDIT-9b
 *                            'Priority Band' vs 'Reco Priority' guard
 *                            preserved)
 * - 01_Menu.gs v2.15.0      (v5.74 cascade-aware menu wrappers;
 *                            generalized version display in
 *                            menuVerifyConfigVersion_ /
 *                            menuRunPhase1Verification_; Tests menu
 *                            4 items including Test Provider Health)
 * - 03_Schema.gs v2.4.0     (v5.74 backend alignment;
 *                            _V28_COLUMN_GROUPS_ extended with
 *                            SCORING_V574_V574 group [7];
 *                            debugV28SchemaPropagation_ classifies
 *                            6 groups including SCORING_V574 = 24
 *                            tracked keys total)
 * - 04_Format.gs v2.12.0     (v5.74 cascade-aware; Decision Matrix +
 *                            Candlestick + Priority Band CF
 *                            preserved from v2.7.1; NEW
 *                            _FRACTIONAL_MULTIPLIER_COLUMNS_ list +
 *                            applyFractionalMultiplierFormat_
 *                            guards overall_penalty_factor from
 *                            percent corruption; format pass order
 *                            PRICE → INTEGER → RATIO → PERCENT →
 *                            SIGNED_PERCENT → SIGNED_NUMBER → DATE →
 *                            FRACTIONAL_MULTIPLIER →
 *                            NULL_DISPLAY_OVERRIDES;
 *                            verifyFormatVersion_ has 5 explicit
 *                            correctness invariants)
 * - 05_Refresh.gs v1.14.0    (strict header-mapped path + per-batch
 *                            critical-field verification with
 *                            BACKEND_NOT_SENDING / WRITER_DROPPED /
 *                            MIXED diagnosis; [v1.9.2 *] tags
 *                            preserved verbatim per Tag Stability
 *                            policy; 106-col rows pass through
 *                            header-mapped writer transparently;
 *                            v1.13.0 lossless-deferral consumer —
 *                            re-queues result.deferredSymbols into
 *                            the checkpoint and reads
 *                            placementAborted / appendAborted /
 *                            secondChanceUpdated; v1.14.0 adds the
 *                            keep-last-good row withhold UPSTREAM
 *                            of this writer, so rowObjects arriving
 *                            here are already identity-vetted)
 * - 07_Global_Markets.gs v1.6.7 (verification triplet host;
 *                            Layout A criticals = 5 view fields;
 *                            v5.74 SCORING_V574 N/A on Layout A;
 *                            v5.3.0 provenance N/A on Layout A)
 * - 13_AutoRefresh.gs v1.9.1 (post-refresh audit reads verifyIssues
 *                              + verifyDiagnoses; cockpit dispatch
 *                              includes Portfolio_Ledger)
 * - 11_SpecialPages.gs v1.3.0 (v3.1.0 criteria + v7.0.0 Data Quality
 *                              Alerts diagnostics)
 * - 99_Diag.gs v2.3.0       (verifyAllColumnsComplete +
 *                            manualRefreshAndVerify cooperate with
 *                            writeRowsBySymbol_ VERDICT line;
 *                            3-way classification handles SCORING_V574
 *                            blanks like Decision Matrix / Canonical
 *                            Provenance — 5 of 7 fields are "real"
 *                            failures, provider_rating + scoring_errors
 *                            are best-effort)
 * - Backend rollout v5.74.0 / May 2026 family + scoring v5.4.2:
 *     core/sheets/schema_registry      v2.10.0 (97→106 instrument
 *                                                + 3 Top10 = 106/109
 *                                                — adds SCORING_V574
 *                                                column group at
 *                                                positions 100-106
 *                                                with "0.0000"
 *                                                format-string
 *                                                warning docstring
 *                                                on overall_penalty_factor;
 *                                                Top10 extras shift
 *                                                to 107-109)
 *     core/schemas                     v6.3.0  (_FALLBACK_STANDARD_KEYS
 *                                                rebuilt to EXACTLY
 *                                                mirror v2.10.0's
 *                                                106 keys;
 *                                                overall_penalty_factor
 *                                                REMOVED from
 *                                                _percent_fields_smart
 *                                                validator —
 *                                                fractional multiplier
 *                                                guard layer 1)
 *     core/data_engine_v2              v5.74.0 (106-key
 *                                                INSTRUMENT_CANONICAL_KEYS
 *                                                schema; engine
 *                                                identity overall_score
 *                                                = overall_score_raw *
 *                                                overall_penalty_factor;
 *                                                v5.71.0 Phase H/I/P
 *                                                engine drops + Phase
 *                                                B forecast unavail +
 *                                                Phase Q last_error_class
 *                                                + Phase N "; "-joined
 *                                                warnings + _classify_8tier
 *                                                8-tier verdict preserved;
 *                                                v5.61.0 provider_
 *                                                unhealthy_markers
 *                                                registry preserved)
 *     core/scoring                     v5.4.2  (CANONICAL RECOMMENDATION
 *                                                SOURCE OF TRUTH;
 *                                                v5.3.0 emissions
 *                                                preserved AND new
 *                                                v5.74 SCORING_V574
 *                                                group emitted on
 *                                                every row —
 *                                                provider_rating from
 *                                                external analyst
 *                                                rating, scoring_
 *                                                recommendation_source
 *                                                as scoring.py
 *                                                provenance tag,
 *                                                scoring_schema_version
 *                                                constant per deploy,
 *                                                scoring_errors
 *                                                "; "-joined,
 *                                                opportunity_source
 *                                                canonical/local/
 *                                                fallback,
 *                                                overall_score_raw
 *                                                pre-penalty composite,
 *                                                overall_penalty_factor
 *                                                FRACTIONAL MULTIPLIER
 *                                                0.0-1.0)
 *     core/providers/eodhd_provider    v4.8.0  (circuit breaker,
 *                                                circuit_open warning,
 *                                                provider_unhealthy:eodhd
 *                                                cross-provider marker)
 *     routes/advanced_analysis         v4.1.0  (v2 engine binding;
 *                                                /v1/schema/provider-health
 *                                                endpoint; meta.provider_health
 *                                                on every /sheet-rows
 *                                                response)
 *     core/reco_normalize              v7.2.0  (rule-id-aware, env floors,
 *                                                AVOID->SELL fix,
 *                                                conviction-badge de-dup)
 *     core/scoring_engine              v3.4.4  (compatibility bridge,
 *                                                cascade-bridge aware)
 *     core/insights_builder            v7.0.0  (Data Quality Alerts section)
 *     core/analysis/criteria_model     v3.1.0  (min_conviction_score +
 *                                                3 exclusion flags)
 *     core/investment_advisor_engine   v4.4.2  (97-col schema; pending
 *                                                v4.5.0 canonical-delegation)
 *     core/investment_advisor          v5.4.0  (canonical-delegated via
 *                                                core.scoring._recommendation)
 *     core/analysis/top10_selector     v4.13.0 (criteria_model v3.1.0
 *                                                hard-filter consumer)
 *     core/candlesticks                v1.0.0  (5 candlestick fields via engine)
 *     core/buckets                     v1.0.0  (canonical bucket helpers,
 *                                                all 5 consumers routed)
 *
 * Why v2.8.0 (vs v2.7.0) — 115/118 INVESTABILITY-GATE WRITER ALIGNMENT
 * ─────────────────────────────────────────────────────────────────────
 *
 * The current dashboard contract extends the v5.74 106-column core to
 * the approved 115-column main-page schema and 118-column Top10 schema.
 * The extra columns are the investability gate used by 05_Refresh.gs
 * v1.11.2, 07_Global_Markets.gs v1.7.1, 11_SpecialPages.gs v1.6.0,
 * 99_Data_Audit.gs v1.3.0, and 99_Diag.gs v2.3.0.
 *
 * v2.8.0 adds writer/extractor awareness for the 9 gate fields:
 *   forecast_source, data_quality_score, forecast_reliability_score,
 *   provider_engine_conflict, conflict_type, final_decision_basis,
 *   investability_status, final_action, block_reason.
 *
 * The change is intentionally non-invasive: HTTP, locking, retry,
 * payload normalization, manual-column preservation, status sheets,
 * and existing v2.5.1 VERDICT tag semantics are preserved. The writer
 * now has a Path 4g alias table for gate fields and a log-only
 * investability_gate_populated=N/M VERDICT segment so propagation gaps
 * are visible during refresh.
 *
 * Important blank semantics:
 *   - Conflict Type and Block Reason are allowed to be blank on clean
 *     INVEST rows.
 *   - Data Quality Score / Forecast Reliability Score / Final Action /
 *     Investability Status should be populated on investment pages.
 *   - Provider/Engine Conflict is a boolean-like gate field and false is
 *     a populated value; do not treat false as blank.
 *
 * Why v2.7.0 (vs v2.6.1) — v5.74 SCORING_V574 OBSERVABILITY
 * ─────────────────────────────────────────────────────────────────────
 *
 * The May 2026 v5.74.0 backend rollout introduced a NEW canonical
 * column group beyond v2.6.1's Canonical Provenance — at schema
 * positions 100-106:
 *
 *   SCORING_V574 (7 cols, v5.74):
 *     provider_rating               — float 0.00, external analyst
 *                                     rating sourced by scoring.py
 *                                     v5.4.2 (best-effort — external
 *                                     data dependent; null when no
 *                                     analyst coverage)
 *     scoring_recommendation_source — str, scoring.py provenance
 *                                     tag identifying which scoring
 *                                     pipeline produced the row's
 *                                     recommendation. CRITICAL:
 *                                     SEMANTICALLY DISTINCT from
 *                                     CANONICAL_PROVENANCE's
 *                                     recommendation_source (engine
 *                                     vocab provenance) — BOTH are
 *                                     on every row by design and
 *                                     MUST NOT be aliased to each
 *                                     other. Config v1.10.0 AUDIT-15e
 *                                     guards this at the HEADER_TO_KEY
 *                                     map level.
 *     scoring_schema_version        — str, constant per-deploy tag
 *                                     ("v5.74"). Always populated
 *                                     when scoring ran.
 *     scoring_errors                — str, "; "-joined error tokens
 *                                     ("provider_error:NetworkError;
 *                                     engine_dropped_valuation").
 *                                     INVERSE signal: empty means
 *                                     "no errors" (the success case);
 *                                     populated means errors captured.
 *     opportunity_source            — str enum (canonical/local/
 *                                     fallback). Always populated
 *                                     when scoring ran.
 *     overall_score_raw             — float 0.00, pre-penalty
 *                                     composite. CRITICAL: SEMANTICALLY
 *                                     DISTINCT from canonical
 *                                     overall_score field — MUST NOT
 *                                     be aliased. Engine identity:
 *                                     overall_score = overall_score_raw
 *                                     * overall_penalty_factor.
 *     overall_penalty_factor        — float 0.0000, FRACTIONAL
 *                                     MULTIPLIER (0.0-1.0, NOT a
 *                                     percent). 1.0 = no penalty,
 *                                     0.95 = 5% penalty. Multiple
 *                                     defense-in-depth layers guard
 *                                     this from percent corruption:
 *                                     schemas.py v6.3.0 validator
 *                                     exclusion, schema_registry
 *                                     v2.10.0 "0.0000" format string,
 *                                     04_Format.gs v2.12.0
 *                                     _FRACTIONAL_MULTIPLIER_COLUMNS_
 *                                     list, Config v1.10.0 AUDIT-15f
 *                                     check.
 *
 * v2.7.0 adds matching alias safety net, extraction path, sanitization
 * exemption for the text field, and per-batch observability — fully
 * symmetric with the v2.5.1 view-field treatment, v2.5.2 Insights
 * treatment, v2.6.0 Decision Matrix + Candlestick treatment, and
 * v2.6.1 Canonical Provenance treatment.
 *
 * IMPORTANT — TWO DISTINCT-FIELD INVARIANTS
 * ──────────────────────────────────────────
 * The SCORING_V574 group contains two fields whose names look like
 * they could overlap with existing canonical fields. Both pairs are
 * SEMANTICALLY DISTINCT and MUST NOT be aliased to each other:
 *
 *   1. scoring_recommendation_source (v5.74, SCORING_V574 group,
 *      'Scoring Reco Source' column) — scoring.py provenance tag
 *      VS.
 *      recommendation_source (v5.3.0, CANONICAL_PROVENANCE group,
 *      'Reco Source' column) — engine vocab provenance
 *
 *      These are complementary: BOTH are written on every
 *      canonically-scored row. The Apps Script side carries each
 *      to its own column without crossover.
 *
 *   2. overall_score_raw (v5.74, SCORING_V574 group, 'Overall Score
 *      (Raw)' column) — pre-penalty composite
 *      VS.
 *      overall_score (canonical, 'Overall Score' column) — post-
 *      penalty final score
 *
 *      Engine identity: overall_score = overall_score_raw *
 *      overall_penalty_factor. Both fields are first-class canonical
 *      columns and MUST NOT be aliased — the operator needs visibility
 *      into BOTH to verify penalty arithmetic.
 *
 * Config v1.10.0 AUDIT-15e explicitly guards (1) at the HEADER_TO_KEY
 * map level. v2.7.0's _SCORING_V574_FIELD_ALIASES_ preserves the
 * distinction at the writer-extraction level by deliberately
 * OMITTING the would-be-confusable fallback from each alias list.
 *
 * IMPORTANT — OVERALL_PENALTY_FACTOR FRACTIONAL MULTIPLIER GUARD
 * ──────────────────────────────────────────────────────────────
 * The overall_penalty_factor field is a fractional multiplier
 * (0.0-1.0 range). It is NOT a percent. Formatting it as a percent
 * would corrupt 0.95 → "95.00%" and visually break the engine
 * identity overall_score = overall_score_raw * overall_penalty_factor.
 * No code in 02_Core.gs depends on this — Core only carries the
 * field value through unchanged — but the docstring captures the
 * semantics so future revisions don't introduce well-meaning but
 * wrong "fixes" to percent formatting.
 *
 * The defense-in-depth layers are:
 *   - schemas.py v6.3.0: overall_penalty_factor REMOVED from
 *     _percent_fields_smart validator
 *   - schema_registry.py v2.10.0: format string "0.0000" with
 *     docstring warning
 *   - 04_Format.gs v2.12.0: dedicated _FRACTIONAL_MULTIPLIER_COLUMNS_
 *     list + applyFractionalMultiplierFormat_ function
 *   - 00_Config.gs v1.12.1: AUDIT-15f/f'/f'' check
 *     overall_penalty_factor NOT in DECIMAL_FRACTION_FIELDS
 *
 * [NEW] _SCORING_V574_FIELD_ALIASES_ — parallel alias table for
 *       the 7 v5.74 SCORING_V574 fields. Same rationale as prior
 *       alias tables: forward-compat insurance against future
 *       backend casing changes. The current v5.74 backend sends
 *       snake_case, so Path 1 of extractApiValue_ resolves these
 *       directly. Path 4f is the safety net.
 *
 *       Display labels match 04_Format.gs v2.12.0 / 00_Config.gs
 *       v1.10.0 entries:
 *         'Provider Rating', 'Scoring Reco Source',
 *         'Scoring Schema Version', 'Scoring Errors',
 *         'Opportunity Source', 'Overall Score (Raw)',
 *         'Overall Penalty Factor'
 *
 *       Distinct-fields guards (see above):
 *         - 'scoring_recommendation_source' aliases do NOT include
 *           'recommendation_source' (the v5.3.0 canonical provenance
 *           field — separate column, separate group, separate
 *           semantics)
 *         - 'overall_score_raw' aliases do NOT include 'overall_score'
 *           (the canonical post-penalty score — separate column)
 *         - 'provider_rating' aliases do NOT include bare 'rating'
 *           (too generic — would collide with future rating fields)
 *
 * [NEW] extractApiValue_ Path 4f — checks _SCORING_V574_FIELD_ALIASES_
 *       if Paths 1-3 + Paths 4 / 4b / 4c / 4d / 4e didn't match.
 *       Same pattern, same semantics, same return-as-is behavior.
 *
 * [NEW] _countPopulatedScoringV574Fields_(rowObjects, sampleSize)
 *       Page-aware counter. Returns { total, populated, empty,
 *       byField } where total = 7 * (rows where at least one
 *       SCORING_V574 field is present). On Layout A pages
 *       (Global_Markets / Insights_Analysis at 46 cols), SCORING_V574
 *       isn't in the payload — total=0/populated=0 lets the VERDICT
 *       line skip the metric. On canonical 106-col pages,
 *       total = 7 * sampleSize.
 *
 *       SEMANTIC NOTE — MIXED INTERPRETATION
 *       ─────────────────────────────────────
 *       Unlike the prior column groups (where blank semantics are
 *       uniform), SCORING_V574 has fields with three different
 *       interpretations:
 *
 *         "Always populated when scoring ran" (5 fields — propagation
 *          gap signal):
 *           - scoring_recommendation_source
 *           - scoring_schema_version (constant per deploy)
 *           - opportunity_source (enum, always set)
 *           - overall_score_raw (deterministic composite)
 *           - overall_penalty_factor (1.0 if no penalty)
 *
 *         "Best-effort, external-data dependent" (1 field):
 *           - provider_rating — null when no analyst coverage
 *             (legitimate state, not a propagation gap)
 *
 *         "INVERSE signal — empty is success" (1 field):
 *           - scoring_errors — empty means "no errors";
 *             populated means errors were captured
 *
 *       When interpreting the canonical_provenance_blanks list:
 *         - provider_rating appearing: NORMAL for under-covered
 *           symbols, not a bug
 *         - scoring_errors appearing: NORMAL on healthy batches,
 *           expected most of the time (empty = success)
 *         - Any of the 5 "always populated" fields appearing:
 *           REAL PROPAGATION GAP — investigate via
 *           debugV28SchemaPropagation_('<page>') from Schema v2.4.0
 *
 * [NEW] _STATUS_TEXT_HEADERS_ extended — 'Scoring Errors' added
 *       to the allowlist. scoring_errors legitimately carries tokens
 *       like "provider_error:NetworkError; engine_dropped_valuation".
 *       Without the allowlist exemption, a single bare token alone
 *       in the field (e.g. just "provider_error") WOULD trigger
 *       sanitization (the token IS in _KNOWN_STATUS_TOKENS_) and
 *       get incorrectly clobbered + routed to Warnings, leaving
 *       Scoring Errors blank. The allowlist preserves the field's
 *       semantic role as a multi-token error log.
 *
 *       NOTE: 'Scoring Reco Source' is NOT added to the allowlist.
 *       Same rationale as v2.6.1's 'Reco Source' exemption: the
 *       column intentionally carries a constant per-deploy tag
 *       ("scoring.py v5.74"). The tag is never a known status
 *       token, so the sanitization check is a no-op for that column
 *       and no special exemption is needed. If a future scoring.py
 *       revision starts emitting the source field as one of the
 *       status tokens — which would be a serious semantic
 *       regression — we'd want sanitization to FIRE rather than
 *       be skipped.
 *
 * [CHANGE] writeRowsBySymbol_ VERDICT line — extended to include
 *          v5.74 group coverage on canonical pages:
 *
 *            [v2.5.1 VERDICT][Market_Leaders] OK updated=10 appended=0
 *              view_fields_populated=50/50
 *              insights_populated=50/50
 *              decision_matrix_populated=20/20
 *              candlestick_populated=37/50 candlestick_blanks=[...]
 *              canonical_provenance_populated=20/20
 *              scoring_v574_populated=68/70 scoring_v574_blanks=[provider_rating(8/10)]
 *
 *          The new metric uses the same total>0 suppression rule
 *          and same per-field blanks-detail rule as prior groups:
 *
 *            - Metric is suppressed when its total = 0 (Layout A
 *              pages where the payload doesn't carry these cols)
 *            - When populated < total, a per-field blank list is
 *              appended as scoring_v574_blanks=[...] naming which
 *              specific fields were blank in the sampled rows
 *
 *          The VERDICT tag REMAINS "[v2.5.1 VERDICT]" per the
 *          v2.5.2/v2.5.3/v2.5.4/v2.6.0/v2.6.1 precedent —
 *          05_Refresh.gs v1.11.2, 07_Global_Markets.gs v1.6.7, and
 *          99_Diag.gs v2.3.0 cite this exact tag in their
 *          verification triplet docs. Bumping the tag would break
 *          cross-file consumers that grep for it; the Tag Stability
 *          policy explicitly forbids this even when adding new
 *          segments.
 *
 * [DOC] Header docstring fully synced for the v5.74 era. Backend
 *       tracking bumped: scoring v5.3.0 → v5.4.2, data_engine_v2
 *       v5.71.0 → v5.74.0; new entries for schema_registry v2.10.0
 *       and schemas v6.3.0 surface the defense-in-depth invariants
 *       guarding overall_penalty_factor. Aligned-with refs bumped:
 *       Config v1.8.0 → v1.10.0, Menu v2.4.0 → v2.8.0,
 *       Schema v2.3.1 → v2.4.0, Format v2.7.1 → v2.8.0.
 *
 * [PRESERVED — strictly] All v2.6.1 functions, signatures, and
 *       return shapes are byte-identical for clean inputs. The new
 *       v2.7.0 additions:
 *         - 1 new frozen alias table (_SCORING_V574_FIELD_ALIASES_)
 *         - 1 new extractApiValue_ path (4f)
 *         - 1 new entry in _STATUS_TEXT_HEADERS_ ('Scoring Errors')
 *         - 1 new page-aware counter helper
 *           (_countPopulatedScoringV574Fields_)
 *         - 1 new optional VERDICT line segment (suppressed when
 *           the column group isn't on the page) + matching per-
 *           field blanks detail segment
 *
 *       Specifically untouched (byte-identical to v2.6.1):
 *         - All HTTP layer (apiGet_, apiPost_, apiFetchJson_,
 *           buildUrl_, buildAuthHeaders_)
 *         - All retry/lock/cache helpers
 *         - All JSON / payload helpers
 *         - All logging / status sheets
 *         - All sheet helpers
 *         - All header-mapped writer machinery
 *         - Paths 1, 2, 3, 4, 4b, 4c, 4d, 4e of extractApiValue_
 *         - snakeToCamel_ / isEmptyValue_ / _coreRequireConfig13_
 *         - v2.5.4 _isStatusTokenValue_ helper
 *         - v2.5.4 _KNOWN_STATUS_TOKENS_ vocabulary (v5.74 emits
 *           tokens already in v2.6.0's vocabulary via scoring_errors
 *           — provider_error, engine_dropped_valuation — both
 *           already covered; opportunity_source values
 *           'canonical'/'local'/'fallback' don't match vocabulary
 *           keys; no new entries needed)
 *         - v2.6.1 _CANONICAL_PROVENANCE_FIELD_ALIASES_ + Path 4e +
 *           _countPopulatedCanonicalProvenanceFields_ counter
 *         - All v2.6.0 / v2.5.x mechanics
 *         - All utility helpers
 *
 *       writeRowsBySymbol_ + writeSingleRowByHeaderMap_ retain all
 *       v2.5.4 + v2.6.0 + v2.6.1 write semantics — the sanitization
 *       layer, manual / onEmpty / null handling, header resolution,
 *       and per-row Warnings flushing are byte-identical. Only the
 *       VERDICT line construction in writeRowsBySymbol_ gains
 *       one optional segment (+ matching blanks detail).
 *
 *       writeSingleRowByHeaderMap_ has NO functional change in v2.7.0
 *       — it writes one row at a time and doesn't run VERDICT
 *       counters, but it benefits automatically from the new Path 4f
 *       via its existing extractApiValue_ call. Symmetric extraction
 *       coverage with no code change needed.
 *
 * Why v2.6.1 (vs v2.6.0) — SCORING v5.3.0 CANONICAL PROVENANCE (PRESERVED HISTORY)
 * ─────────────────────────────────────────────────────────────────────
 *
 * The Phase 6 / May 2026 scoring v5.3.0 rollout introduced a third
 * canonical-only column group beyond v2.6.0's Decision Matrix +
 * Candlestick:
 *
 *   CANONICAL PROVENANCE (2 cols, v5.3.0):
 *     recommendation_priority_band  — str P1..P5 from
 *                                     scoring.py._compute_priority
 *                                     (CRITICAL: NOT the same field
 *                                     as recommendation_priority,
 *                                     which is the int 1-8 verdict
 *                                     from data_engine_v2's
 *                                     _classify_8tier — see 00_Config
 *                                     v1.8.0 AUDIT-9b distinct-fields
 *                                     invariant)
 *     recommendation_source         — constant per-deploy tag
 *                                     "scoring.py v5.3.0" (the
 *                                     idempotency check key for
 *                                     apply_canonical_recommendation)
 *
 * v2.6.1 adds matching alias safety net, extraction path, and per-
 * batch observability for the new group — symmetric with the v2.5.1
 * view-field treatment, v2.5.2 Insights treatment, and v2.6.0
 * Decision Matrix + Candlestick treatment.
 *
 * IMPORTANT — NON-INTUITIVE PRIORITY BAND MAPPING
 * ────────────────────────────────────────────────
 * The P1..P5 vocabulary scoring.py emits is aligned with
 * audit_data_quality.AlertPriority, NOT with naive "P1=top pick"
 * intuition:
 *
 *     P1 = Critical action: STRONG_SELL or broken-row REDUCE
 *     P2 = STRONG_BUY or high-conviction BUY
 *     P3 = BUY (normal conviction)
 *     P4 = HOLD (default)
 *     P5 = SELL or low-priority REDUCE
 *
 * Both P1 and P5 are red (different urgencies of sell). 04_Format.gs
 * v2.7.1 implements this color mapping; consult the
 * PRIORITY_BAND_VOCAB_ comment block in that file when in doubt.
 * No code in 02_Core.gs depends on this mapping — Core only carries
 * the field values through unchanged — but the docstring captures
 * the semantics so future revisions don't introduce well-meaning
 * but wrong "fixes" to the color rules.
 *
 * [NEW] _CANONICAL_PROVENANCE_FIELD_ALIASES_ — parallel alias table
 *       for the 2 v5.3.0 canonical-provenance fields. Same rationale
 *       as _DECISION_MATRIX_FIELD_ALIASES_ and other alias tables:
 *       forward-compat insurance against future backend casing
 *       changes. The current v5.3.0 backend sends snake_case, so
 *       Path 1 of extractApiValue_ resolves these directly. Path 4e
 *       is the safety net.
 *
 *       Display labels match 04_Format.gs v2.7.1 / 00_Config.gs
 *       v1.8.0 entries: 'Priority Band' and 'Reco Source'.
 *
 *       Aliases include the legacy "recommendation_priority"
 *       NEVER as a fallback for "recommendation_priority_band" —
 *       the two are SEMANTICALLY DISTINCT (int 1-8 verdict vs str
 *       P1..P5 band) and must not be aliased even defensively.
 *       Mis-aliasing them is a regression class explicitly guarded
 *       against by Config v1.8.0 AUDIT-9b.
 *
 * [NEW] extractApiValue_ Path 4e — checks
 *       _CANONICAL_PROVENANCE_FIELD_ALIASES_ if Paths 1-3 + Path 4
 *       (view) + Path 4b (insights) + Path 4c (decision matrix) +
 *       Path 4d (candlestick) didn't match. Same pattern, same
 *       semantics, same return-as-is behavior.
 *
 * [NEW] _countPopulatedCanonicalProvenanceFields_(rowObjects, sampleSize)
 *       Page-aware counter. Returns { total, populated, empty,
 *       byField } where total = 2 * (rows where at least one
 *       Canonical Provenance field is present). On Layout A pages
 *       (Global_Markets / Insights_Analysis at 46 cols), Canonical
 *       Provenance isn't in the payload — total=0/populated=0 lets
 *       the VERDICT line skip the metric. On canonical 99-col pages,
 *       total = 2 * sampleSize.
 *
 *       SEMANTIC NOTE: Canonical Provenance blanks are "REAL" failures,
 *       not best-effort. Both fields are deterministic:
 *         - recommendation_priority_band is computed centrally by
 *           scoring.py._compute_priority given upstream overall /
 *           risk / conviction scores. Deterministic — same inputs
 *           give same band.
 *         - recommendation_source is a constant per-deploy tag
 *           ("scoring.py v5.3.0") that scoring.py
 *           apply_canonical_recommendation writes on every row it
 *           touches. Blank means apply_canonical_recommendation
 *           was never called for that row.
 *       If a canonically-scored row shows either field blank,
 *       that's a real propagation gap — investigate via
 *       debugV28SchemaPropagation_ from Schema v2.3.1.
 *       Contrast with Candlestick where partial coverage is normal.
 *
 * [CHANGE] writeRowsBySymbol_ VERDICT line — extended to include
 *          v5.3.0 group coverage on canonical pages:
 *
 *            [v2.5.1 VERDICT][Market_Leaders] OK updated=10 appended=0
 *              view_fields_populated=50/50
 *              insights_populated=50/50
 *              decision_matrix_populated=20/20
 *              candlestick_populated=37/50 candlestick_blanks=[...]
 *              canonical_provenance_populated=20/20
 *
 *          The new metric uses the same total>0 suppression rule
 *          and same per-field blanks-detail rule as v2.5.3 /
 *          v2.6.0:
 *
 *            - Metric is suppressed when its total = 0 (Layout A
 *              pages where the payload doesn't carry these cols)
 *            - When populated < total, a per-field blank list is
 *              appended as canonical_provenance_blanks=[...]
 *              naming which specific fields were blank in the
 *              sampled rows
 *
 *          The VERDICT tag REMAINS "[v2.5.1 VERDICT]" per the
 *          v2.5.2/v2.5.3/v2.5.4/v2.6.0 precedent — 05_Refresh.gs
 *          v1.9.3, 07_Global_Markets.gs v1.6.7, and 99_Diag.gs
 *          v2.1.0 cite this exact tag in their verification triplet
 *          docs. Bumping the tag would break cross-file consumers
 *          that grep for it; the Tag Stability policy explicitly
 *          forbids this even when adding new segments.
 *
 * [DOC] Header docstring fully synced for the v5.3.0 era. Backend
 *       tracking bumped: scoring v5.2.5 → v5.3.0, data_engine_v2
 *       v5.60.0 → v5.71.0, scoring_engine v3.4.2 → v3.4.4, advisor
 *       v5.3.0 → v5.4.0, advisor_engine v4.4.0 → v4.4.2,
 *       top10_selector v4.12.0 → v4.13.0; new core/buckets v1.0.0
 *       added. Aligned-with refs bumped: Config v1.6.0+ → v1.8.0,
 *       Schema v2.3.0 → v2.3.1, Format v2.7.0 → v2.7.1, Refresh
 *       v1.9.1 → v1.9.3, Global_Markets v1.6.5 → v1.6.7,
 *       AutoRefresh v1.2.0 → v1.4.0, 99_Diag v2.0.0 → v2.1.0.
 *
 * [PRESERVED — strictly] All v2.6.0 functions, signatures, and
 *       return shapes are byte-identical for clean inputs. The new
 *       v2.6.1 additions:
 *         - 1 new frozen alias table (Canonical Provenance)
 *         - 1 new extractApiValue_ path (4e)
 *         - 1 new page-aware counter helper
 *         - 1 new optional VERDICT line segment (suppressed when
 *           the column group isn't on the page) + matching per-
 *           field blanks detail segment
 *
 *       Specifically untouched (byte-identical to v2.6.0):
 *         - All HTTP layer (apiGet_, apiPost_, apiFetchJson_,
 *           buildUrl_, buildAuthHeaders_)
 *         - All retry/lock/cache helpers
 *         - All JSON / payload helpers (safeJsonParse_,
 *           unwrapPayload_, isTableShapedPayload_, collectWarnings_,
 *           extractHeaders_, extractKeys_, extractRows_, to2DArray_,
 *           normalizeTablePayload_)
 *         - All logging / status sheets (logRun_, writePageStatus_,
 *           writeGlobalStatus_)
 *         - All sheet helpers (getOrCreateSheet_, ensureHeaderRow_,
 *           clearDataArea_, writeTableToSheet_, etc.)
 *         - All header-mapped writer machinery (_normalizeHeaderText_,
 *           coreResolveHeaderKey_, getSheetHeaders_, findColumnIndex_,
 *           buildHeaderIndex_)
 *         - Paths 1, 2, 3, 4, 4b, 4c, 4d of extractApiValue_
 *         - snakeToCamel_ / isEmptyValue_ / _coreRequireConfig13_
 *         - v2.5.4 _isStatusTokenValue_ helper
 *         - v2.5.4 _KNOWN_STATUS_TOKENS_ vocabulary (v5.3.0
 *           emits 'provider_error:<class>' and
 *           'engine_dropped_valuation' via scoring_errors; both
 *           are ALREADY in v2.6.0's vocabulary so no new entries
 *           needed)
 *         - v2.5.4 _STATUS_TEXT_HEADERS_ vocabulary
 *         - v2.5.4 status-bleed sanitization in both writers
 *         - v2.5.4 Warnings column flush logic
 *         - v2.5.4 statusBleedSanitized result field
 *         - v2.5.3 byField extension on view/insights counters
 *         - v2.5.3 per-field blank detail in VERDICT reason
 *         - v2.5.2 _INSIGHTS_FIELD_ALIASES_ + Path 4b
 *         - v2.5.1 VERDICT logging framework
 *         - v2.6.0 _DECISION_MATRIX_FIELD_ALIASES_ + Path 4c +
 *           _countPopulatedDecisionMatrixFields_ counter
 *         - v2.6.0 _CANDLESTICK_FIELD_ALIASES_ + Path 4d +
 *           _countPopulatedCandlestickFields_ counter
 *         - All utility helpers (buildQueryString_, normalizeCellValue_,
 *           etc.)
 *
 *       writeRowsBySymbol_ + writeSingleRowByHeaderMap_ retain all
 *       v2.5.4 + v2.6.0 write semantics — the sanitization layer,
 *       manual / onEmpty / null handling, header resolution, and
 *       per-row Warnings flushing are byte-identical. Only the
 *       VERDICT line construction in writeRowsBySymbol_ gains
 *       one optional segment (+ matching blanks detail).
 *
 *       writeSingleRowByHeaderMap_ has NO change in v2.6.1 — it
 *       writes one row at a time and doesn't run VERDICT counters,
 *       but it benefits automatically from the new Path 4e via
 *       its existing extractApiValue_ call. Symmetric extraction
 *       coverage with no code change needed.
 *
 * Why v2.6.0 (vs v2.5.4) — v2.8.0 BACKEND ALIGNMENT (PRESERVED in v2.6.1)
 * ─────────────────────────────────────────────────────────────────────
 *
 * Added _DECISION_MATRIX_FIELD_ALIASES_ + _CANDLESTICK_FIELD_ALIASES_
 * + Paths 4c/4d + two new counter helpers + four new optional
 * VERDICT line segments + 9 new _KNOWN_STATUS_TOKENS_ entries for
 * v5.60.0 era engine-drop and intrinsic-unit-detection markers.
 * v2.6.1 builds on this foundation symmetrically.
 *
 * Why v2.5.4 (vs v2.5.3) — STATUS-BLEED SANITIZATION (PRESERVED in v2.6.1)
 * ─────────────────────────────────────────────────────────────────────
 *
 * Defensive fix for the audit finding (May 8, 2026 Global_Markets
 * dump) that STNE.US, EXE.US, ML.PA, and ALL.US showed status
 * strings like "stale_data" / "fallback_provider" / "fetch_failed"
 * embedded in numeric cells. Writer-side defense: detect known
 * status tokens, drop them from the target cell, route the token
 * text to the Warnings column. _KNOWN_STATUS_TOKENS_ +
 * _STATUS_TEXT_HEADERS_ + _isStatusTokenValue_ + the per-row
 * rowBleeds accumulator implement this. v2.6.0 extended the
 * vocabulary with v5.60.0 era tokens. v2.6.1 needs no further
 * vocabulary entries — scoring.py v5.3.0 emits
 * 'provider_error:<class>' and 'engine_dropped_valuation' via
 * scoring_errors, and both are already covered by v2.6.0.
 *
 * Why v2.5.3 (vs v2.5.2) — PER-FIELD VERDICT DETAIL (PRESERVED in v2.6.1)
 * ───────────────────────────────────────────────────────────────────
 *
 * Refresh v1.9.3's sampled-row verification reads only the first
 * row of each batch. v2.5.3's byField extension on
 * _countPopulatedViewFields_ / _countPopulatedInsightsFields_
 * surfaces per-field blank counts in the VERDICT line so partial
 * WRITER_DROPPED ("field N fails on a few rows but row 0 is fine")
 * becomes visible. v2.6.0 mirrored the byField pattern in Decision
 * Matrix + Candlestick counters. v2.6.1 mirrors it again in the
 * Canonical Provenance counter.
 *
 * Why v2.5.2 (vs v2.5.1) — Wave 3 / v2.6.0 INSIGHTS ALIGNMENT (PRESERVED in v2.6.1)
 * ─────────────────────────────────────────────────────────────────────────
 *
 * Added _INSIGHTS_FIELD_ALIASES_ + Path 4b in extractApiValue_ +
 * _countPopulatedInsightsFields_ + VERDICT line metric for the 5
 * Wave 3 Insights fields. v2.6.0 followed the same pattern for the
 * 7 new v2.8.0 fields. v2.6.1 follows the same pattern again for
 * the 2 new v5.3.0 fields.
 *
 * Why v2.5.0 → v2.5.1 — SURGICAL FIXES + FAIL-LOUD VERDICT (PRESERVED in v2.6.1)
 * ──────────────────────────────────────────────────────────
 *
 * extractApiValue_ Path 4 (_VIEW_FIELD_ALIASES_) safety net for
 * the 5 multi-method view fields. _countPopulatedViewFields_ +
 * single concise [v2.5.1 VERDICT] line per writeRowsBySymbol_
 * call summarizing OK / EMPTY_FIELDS / UNMAPPED / WRITER_SKIPPED
 * / WRITER_THREW outcomes.
 *
 * INVESTIGATION CONTEXT
 * ─────────────────────
 *
 * The blank-cells bug for the 5 Layout A view fields was resolved
 * post-v1.8.1 / v1.9.0 Refresh. The status-bleed bug was resolved
 * post-v2.5.4. The v2.8.0 structural-coverage gap was closed
 * post-v2.6.0. v2.6.1 closes the analogous gap for the v5.3.0
 * canonical-provenance group — the writer can now extract
 * recommendation_priority_band + recommendation_source under all
 * reasonable backend casing conventions (snake_case is canonical,
 * Path 4e is the safety net), and the VERDICT line surfaces per-
 * batch coverage for both fields so any propagation gap is visible
 * at the same log entry that already shows view + insights +
 * decision matrix + candlestick coverage.
 *
 * Post-v5.3.0 + v2.6.1 verification (run after deploying Config
 * v1.8.0 with v5.3.0 HEADER_TO_KEY entries + Schema v2.3.1 +
 * Format v2.7.1 + Refresh v1.9.3 + this file + Global_Markets
 * v1.6.7):
 *
 *   1. Run debugV28SchemaPropagation_('Market_Leaders') (03_Schema.gs
 *      v2.3.1 helper) — confirms Config's HEADER_TO_KEY map
 *      includes the 2 new v5.3.0 keys AND the 7 v2.8.0 keys, and
 *      that the CANONICAL_PROVENANCE_V53 group classifies cleanly.
 *   2. Run a refresh and check the Executions log for the full
 *      VERDICT line. On Market_Leaders / Top_10_Investments expect:
 *        [v2.5.1 VERDICT][Market_Leaders] OK updated=N appended=N
 *          view_fields_populated=A/B
 *          insights_populated=C/D
 *          decision_matrix_populated=E/F
 *          candlestick_populated=G/H
 *          canonical_provenance_populated=I/J
 *      Layout A pages (Global_Markets, Insights_Analysis at 46
 *      cols) will show only view_fields_populated; the other four
 *      metrics will be suppressed because their group counters
 *      return total=0.
 *   3. If canonical_provenance_populated < total with a non-empty
 *      canonical_provenance_blanks list, the propagation gap is
 *      between scoring.py v5.3.0 and the writer. Check:
 *        - Is data_engine_v2 v5.71.0 actually calling
 *          apply_canonical_recommendation? (Look for non-empty
 *          recommendation_source in raw payload.)
 *        - Is Config v1.8.0 HEADER_TO_KEY mapping the headers?
 *          (Run debugVerifyConfigVersion_ AUDIT-9.)
 *        - Is the sheet showing the 99-col layout? (Run
 *          "Update Current Headers" on the canonical sheet.)
 *   4. If any v2.8.0 group shows populated < total, follow the
 *      v2.6.0 troubleshooting chain.
 *   5. If a row shows status_bleed_sanitized > 0, open the
 *      affected row's Warnings cell to see which provider/path
 *      leaked which token into which numeric column.
 *
 * Depends on:
 * - 00_Config.gs v1.12.1  (provides getHeaderToKey [v5.74 SCORING_V574
 *                          map AND v5.3.0 canonical provenance map
 *                          AND v2.8.0 Decision Matrix + Candlestick
 *                          map AND Wave 3 Insights map],
 *                          getDecimalFractionFields,
 *                          getPreserveManualColumns,
 *                          getPreserveOnEmptyColumns)
 */

'use strict';

/* =========================================================
 * Core constants
 * ========================================================= */

var CORE_SCRIPT_VERSION_ = '2.11.1';
var CORE_CACHE_PREFIX_ = 'TFB_CORE::';
var CORE_LOCK_TIMEOUT_MS_ = 30000;
// v2.11.1: execution-local re-entrant scope for the shared DocumentLock.
var CORE_DOCUMENT_LOCK_DEPTH_ = 0;
var CORE_DOCUMENT_LOCK_LABEL_ = '';
var CORE_RETRY_MAX_ = 3;
var CORE_RETRY_BASE_MS_ = 1000;


// v2.11.0 — manual-refresh priority coordination. The state property contains
// a JSON object with a unique owner token, phase, timestamps and expiry. It is
// deliberately separate from refresh checkpoints and trigger configuration.
var CORE_MANUAL_PRIORITY_STATE_PROP_ = 'TFB_MANUAL_REFRESH_PRIORITY_STATE_V1';
var CORE_MANUAL_PRIORITY_ENABLED_PROP_ = 'TFB_MANUAL_REFRESH_PRIORITY_ENABLED';
var CORE_MANUAL_PRIORITY_TTL_SEC_PROP_ = 'TFB_MANUAL_REFRESH_PRIORITY_TTL_SEC';
var CORE_MANUAL_LOCK_WAIT_SEC_PROP_ = 'TFB_MANUAL_REFRESH_LOCK_WAIT_SEC';
var CORE_MANUAL_LOCK_POLL_MS_PROP_ = 'TFB_MANUAL_REFRESH_LOCK_POLL_MS';
var CORE_MANUAL_PRIORITY_SCHEMA_VERSION_ = 1;
var CORE_MANUAL_PRIORITY_DEFAULT_TTL_SEC_ = 1200;
var CORE_MANUAL_LOCK_WAIT_DEFAULT_SEC_ = 240;
var CORE_MANUAL_LOCK_POLL_DEFAULT_MS_ = 3000;
var CORE_MANUAL_PRIORITY_STATE_LOCK_MS_ = 5000;

// v2.5.1: read from Script Property if present, default true
var _CORE_DEBUG_HEADER_MATCHING_ = (function () {
  try {
    var props = PropertiesService.getScriptProperties();
    var v = props.getProperty('TFB_DEBUG_HEADER_MATCHING');
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    return ['1', 'true', 'yes', 'y', 'on'].indexOf(s) !== -1;
  } catch (e) {
    return true;
  }
})();

var ROW_LIST_ALIASES_ = Object.freeze([
  'rows',
  'row_objects',
  'items',
  'records',
  'data',
  'quotes',
  'results',
  'rows_matrix'
]);

// v2.5.1: aliases for the 5 critical view fields. Order: snake_case
// (canonical, what the backend sends), camelCase, kebab-case,
// space-separated lowercase, capitalized.
var _VIEW_FIELD_ALIASES_ = Object.freeze({
  'fundamental_view': ['fundamentalView', 'fundamental-view', 'fundamental view', 'FundamentalView', 'Fundamental View', 'fundamentalview'],
  'technical_view':   ['technicalView',   'technical-view',   'technical view',   'TechnicalView',   'Technical View',   'technicalview'],
  'risk_view':        ['riskView',        'risk-view',        'risk view',        'RiskView',        'Risk View',        'riskview'],
  'value_view':       ['valueView',       'value-view',       'value view',       'ValueView',       'Value View',       'valueview'],
  'upside_pct':       ['upsidePct',       'upside-pct',       'upside pct',       'UpsidePct',       'Upside %',         'upsidepct', 'upside_percent']
});

// v2.5.2: aliases for the 5 Insights fields (Wave 3 / v2.6.0).
// Parallel structure to _VIEW_FIELD_ALIASES_; same rationale —
// forward-compat insurance against future backend casing changes.
// The current backend (v2.6.0+) sends snake_case, so Path 1 of
// extractApiValue_ resolves these directly. Path 4b is the safety
// net.
var _INSIGHTS_FIELD_ALIASES_ = Object.freeze({
  'sector_relative_score': ['sectorRelativeScore', 'sector-relative-score', 'sector relative score', 'SectorRelativeScore', 'Sector-Adj Score', 'sectorrelativescore', 'sector_adj_score', 'sectorAdjScore', 'sector_adjusted_score'],
  'conviction_score':      ['convictionScore',     'conviction-score',      'conviction score',      'ConvictionScore',     'Conviction Score', 'convictionscore'],
  'top_factors':           ['topFactors',          'top-factors',           'top factors',           'TopFactors',          'Top Factors',      'topfactors'],
  'top_risks':             ['topRisks',            'top-risks',             'top risks',             'TopRisks',            'Top Risks',        'toprisks'],
  'position_size_hint':    ['positionSizeHint',    'position-size-hint',    'position size hint',    'PositionSizeHint',    'Position Size Hint', 'positionsizehint', 'position_size']
});

// v2.6.0: aliases for the 2 Decision Matrix fields (v2.8.0,
// positions 91-92). Parallel structure to the view + insights
// alias tables. The current backend (v2.8.0) sends snake_case,
// so Path 1 of extractApiValue_ resolves these directly. Path 4c
// is the safety net for future casing changes.
//
// Aliases cover camelCase, kebab-case, space-separated lowercase,
// PascalCase, display label, and collapsed lowercase variants.
// Display labels match the 04_Format.gs v2.7.0 _WIDE_COLUMNS_ /
// _NARROW_COLUMNS_ entries: 'Recommendation Detail' / 'Reco Priority'.
var _DECISION_MATRIX_FIELD_ALIASES_ = Object.freeze({
  'recommendation_detailed': ['recommendationDetailed', 'recommendation-detailed', 'recommendation detailed', 'RecommendationDetailed', 'Recommendation Detail', 'recommendationdetailed', 'recommendation_detail', 'recommendationDetail', 'reco_detailed'],
  'recommendation_priority': ['recommendationPriority', 'recommendation-priority', 'recommendation priority', 'RecommendationPriority', 'Reco Priority',         'recommendationpriority', 'recommendation_prio', 'recoPriority', 'reco_priority']
});

// v2.6.0: aliases for the 5 Candlestick fields (v2.8.0, positions
// 93-97). Same structure, same safety-net rationale. Display
// labels match 04_Format.gs v2.7.0:
//   'Candle Pattern', 'Candle Signal', 'Candle Strength',
//   'Candle Confidence', 'Recent Patterns (5D)'.
//
// Note on candlestick_patterns_recent: the display label "Recent
// Patterns (5D)" contains parens, which are tolerated by
// extractApiValue_ Path 3 (alphanumeric-lower normalization).
// The alias entry is included for symmetry but the canonical
// snake_case path is what the backend emits.
var _CANDLESTICK_FIELD_ALIASES_ = Object.freeze({
  'candlestick_pattern':         ['candlestickPattern',         'candlestick-pattern',         'candlestick pattern',         'CandlestickPattern',         'Candle Pattern',      'candlestickpattern', 'candle_pattern'],
  'candlestick_signal':          ['candlestickSignal',          'candlestick-signal',          'candlestick signal',          'CandlestickSignal',          'Candle Signal',       'candlesticksignal',  'candle_signal'],
  'candlestick_strength':        ['candlestickStrength',        'candlestick-strength',        'candlestick strength',        'CandlestickStrength',        'Candle Strength',     'candlestickstrength','candle_strength'],
  'candlestick_confidence':      ['candlestickConfidence',      'candlestick-confidence',      'candlestick confidence',      'CandlestickConfidence',      'Candle Confidence',   'candlestickconfidence','candle_confidence'],
  'candlestick_patterns_recent': ['candlestickPatternsRecent',  'candlestick-patterns-recent', 'candlestick patterns recent', 'CandlestickPatternsRecent',  'Recent Patterns (5D)', 'candlestickpatternsrecent', 'recent_patterns_5d', 'candle_patterns_recent', 'recentPatterns5D']
});

// v2.6.1: aliases for the 2 Canonical Provenance fields (scoring
// v5.3.0). Parallel structure to the v2.6.0 alias tables; same
// safety-net rationale. The current v5.3.0 backend sends snake_case
// directly (RECOMMENDATION_SOURCE_TAG = "scoring.py v5.3.0" with
// recommendation_priority_band as the field name on the AssetScores
// dataclass), so Path 1 of extractApiValue_ resolves these
// directly. Path 4e is the safety net.
//
// Display labels match 04_Format.gs v2.7.1 / 00_Config.gs v1.8.0:
//   'Priority Band'  (90px narrow column, P1..P5 chip CF)
//   'Reco Source'    (140px medium column, no CF — constant per-
//                     deploy provenance tag)
//
// CRITICAL — DISTINCT FIELDS GUARD
// ────────────────────────────────
// The alias list for 'recommendation_priority_band' DOES NOT
// include 'recommendation_priority' (without "_band"), even
// defensively. The two are semantically distinct fields:
//   - recommendation_priority      (int 1-8 verdict from
//                                    data_engine_v2._classify_8tier,
//                                    aliased by
//                                    _DECISION_MATRIX_FIELD_ALIASES_
//                                    above as the 'Reco Priority'
//                                    column)
//   - recommendation_priority_band (str P1..P5 band from
//                                    scoring.py._compute_priority,
//                                    aliased here as the
//                                    'Priority Band' column)
// Mis-aliasing them — having either field's lookup fall through
// to the other — would silently regress one column to show the
// other's values. Config v1.8.0 AUDIT-9b explicitly guards
// against this at the HEADER_TO_KEY map level. This alias table
// preserves the distinction at the writer-extraction level.
//
// NON-INTUITIVE PRIORITY BAND MAPPING REMINDER
// ────────────────────────────────────────────
// P1 = STRONG_SELL / broken-row REDUCE (red),
// P2 = STRONG_BUY (green),
// P3 = BUY (green),
// P4 = HOLD (yellow, default),
// P5 = SELL / low-priority REDUCE (red).
// Aligned with audit_data_quality.AlertPriority. See
// scoring.py._compute_priority and 04_Format.gs v2.7.1's
// PRIORITY_BAND_VOCAB_ comment block.
var _CANONICAL_PROVENANCE_FIELD_ALIASES_ = Object.freeze({
  'recommendation_priority_band': ['recommendationPriorityBand', 'recommendation-priority-band', 'recommendation priority band', 'RecommendationPriorityBand', 'Priority Band',  'recommendationpriorityband', 'priority_band',        'priorityBand', 'reco_priority_band', 'reco_band'],
  'recommendation_source':        ['recommendationSource',        'recommendation-source',        'recommendation source',        'RecommendationSource',        'Reco Source',    'recommendationsource',        'reco_source',          'recoSource',   'scoring_source',     'recommendation_provenance']
});

// v2.7.0: aliases for the 7 SCORING_V574 fields (v5.74 cascade,
// schema positions 100-106). Parallel structure to the v2.6.x alias
// tables; same safety-net rationale. The current v5.74 backend
// sends snake_case directly (data_engine_v2 v5.74.0 + scoring v5.4.2
// pipeline), so Path 1 of extractApiValue_ resolves these directly.
// Path 4f is the safety net.
//
// Display labels match 04_Format.gs v2.12.0 / 00_Config.gs v1.12.1:
//   'Provider Rating'         (numeric, 0.00 format)
//   'Scoring Reco Source'     (text, no CF — constant per-deploy
//                              scoring.py provenance tag; NOT the
//                              same as 'Reco Source')
//   'Scoring Schema Version'  (text, constant per deploy)
//   'Scoring Errors'          (text, "; "-joined error tokens,
//                              status-bleed-allowlisted)
//   'Opportunity Source'      (text enum: canonical/local/fallback)
//   'Overall Score (Raw)'     (numeric, 0.00 format —
//                              pre-penalty composite, NOT the same
//                              as 'Overall Score')
//   'Overall Penalty Factor'  (numeric, 0.0000 format — FRACTIONAL
//                              MULTIPLIER 0.0-1.0, NOT a percent)
//
// CRITICAL — THREE DISTINCT-FIELD GUARDS
// ──────────────────────────────────────
// The alias lists deliberately OMIT three would-be-confusable
// fallbacks. Each pair is semantically distinct:
//
//   (1) 'scoring_recommendation_source' alias list does NOT include
//       'recommendation_source'. The latter is the v5.3.0 Canonical
//       Provenance field (engine vocab provenance, 'Reco Source'
//       column, _CANONICAL_PROVENANCE_FIELD_ALIASES_ above). The
//       former is the v5.74 scoring.py provenance tag ('Scoring Reco
//       Source' column). BOTH are written on every canonically-
//       scored row by design; they must NOT cross-extract.
//       Config v1.10.0 AUDIT-15e guards this at the HEADER_TO_KEY
//       map level.
//
//   (2) 'overall_score_raw' alias list does NOT include
//       'overall_score'. The latter is the canonical post-penalty
//       final score; the former is the pre-penalty composite.
//       Engine identity: overall_score = overall_score_raw *
//       overall_penalty_factor. The operator needs visibility into
//       BOTH to verify penalty arithmetic; aliasing one to the
//       other would silently regress one column.
//
//   (3) 'provider_rating' alias list does NOT include bare 'rating'.
//       'rating' is too generic and would collide with any future
//       rating field. The explicit 'providerRating' / 'Provider
//       Rating' forms are the canonical aliases.
//
// OVERALL_PENALTY_FACTOR FRACTIONAL MULTIPLIER REMINDER
// ─────────────────────────────────────────────────────
// The overall_penalty_factor field is a fractional multiplier
// (0.0-1.0 range). It is NOT a percent. 1.0 = no penalty,
// 0.95 = 5% penalty. Multiple defense-in-depth layers guard this:
//   - schemas.py v6.3.0: REMOVED from _percent_fields_smart validator
//   - schema_registry.py v2.10.0: format string "0.0000"
//   - 04_Format.gs v2.12.0: _FRACTIONAL_MULTIPLIER_COLUMNS_ list +
//     applyFractionalMultiplierFormat_
//   - 00_Config.gs v1.12.1: AUDIT-15f/f'/f'' check
// 02_Core.gs only carries the value through unchanged; this
// docstring captures the semantics so future revisions don't
// introduce well-meaning but wrong "fixes" to percent formatting.
var _SCORING_V574_FIELD_ALIASES_ = Object.freeze({
  'provider_rating':                ['providerRating',               'provider-rating',                 'provider rating',                 'ProviderRating',               'Provider Rating',         'providerrating'],
  'scoring_recommendation_source':  ['scoringRecommendationSource',  'scoring-recommendation-source',   'scoring recommendation source',   'ScoringRecommendationSource',  'Scoring Reco Source',     'scoringrecommendationsource', 'scoring_reco_source', 'scoringRecoSource'],
  'scoring_schema_version':         ['scoringSchemaVersion',         'scoring-schema-version',          'scoring schema version',          'ScoringSchemaVersion',         'Scoring Schema Version',  'scoringschemaversion'],
  'scoring_errors':                 ['scoringErrors',                'scoring-errors',                  'scoring errors',                  'ScoringErrors',                'Scoring Errors',          'scoringerrors'],
  'opportunity_source':             ['opportunitySource',            'opportunity-source',              'opportunity source',              'OpportunitySource',            'Opportunity Source',      'opportunitysource'],
  'overall_score_raw':              ['overallScoreRaw',              'overall-score-raw',               'overall score raw',               'OverallScoreRaw',              'Overall Score (Raw)',     'overallscoreraw'],
  'overall_penalty_factor':         ['overallPenaltyFactor',         'overall-penalty-factor',          'overall penalty factor',          'OverallPenaltyFactor',         'Overall Penalty Factor',  'overallpenaltyfactor']
});


// v2.8.0: aliases for the 9 INVESTABILITY_GATE_V578 fields (current
// 115-column main-page / 118-column Top10 contract). These are the
// gate fields consumed by 05_Refresh.gs v1.11.2, 99_Data_Audit.gs
// v1.3.0, and 99_Diag.gs v2.3.0.
//
// The current backend/config mapping uses snake_case keys. The alias
// table protects the writer if a future payload uses camelCase,
// kebab-case, display labels, or compact variants.
//
// Distinct-field guard:
//   - forecast_source is NOT the same as opportunity_source.
//   - final_action is NOT the same as recommendation.
//   - investability_status is NOT the same as recommendation_detail.
//   - provider_engine_conflict is a boolean-like field; false is a
//     meaningful populated value and must not be treated as blank.
var _INVESTABILITY_GATE_FIELD_ALIASES_ = Object.freeze({
  'forecast_source':              ['forecastSource',             'forecast-source',              'forecast source',              'ForecastSource',             'Forecast Source',              'forecastsource'],
  'data_quality_score':           ['dataQualityScore',           'data-quality-score',           'data quality score',           'DataQualityScore',           'Data Quality Score',           'dataqualityscore', 'dq_score', 'dqScore'],
  'forecast_reliability_score':   ['forecastReliabilityScore',   'forecast-reliability-score',   'forecast reliability score',   'ForecastReliabilityScore',   'Forecast Reliability Score',   'forecastreliabilityscore', 'reliability_score', 'reliabilityScore'],
  'provider_engine_conflict':     ['providerEngineConflict',     'provider-engine-conflict',     'provider engine conflict',     'ProviderEngineConflict',     'Provider/Engine Conflict',     'providerengineconflict', 'provider_conflict', 'engine_conflict'],
  'conflict_type':                ['conflictType',               'conflict-type',                'conflict type',                'ConflictType',               'Conflict Type',                'conflicttype'],
  'final_decision_basis':         ['finalDecisionBasis',         'final-decision-basis',         'final decision basis',         'FinalDecisionBasis',         'Final Decision Basis',         'finaldecisionbasis', 'decision_basis', 'decisionBasis'],
  'investability_status':         ['investabilityStatus',        'investability-status',         'investability status',         'InvestabilityStatus',        'Investability Status',         'investabilitystatus'],
  'final_action':                 ['finalAction',                'final-action',                 'final action',                 'FinalAction',                'Final Action',                 'finalaction'],
  'block_reason':                 ['blockReason',                'block-reason',                 'block reason',                 'BlockReason',                'Block Reason',                 'blockreason']
});

// v2.5.4 — Status-bleed sanitization vocabulary.
//
// Lowercase snake_case status tokens that should NEVER appear in
// a value field. When detected as the incoming value for a non-
// text column, the writer drops the value (writes blank) and
// routes the token text to the Warnings column instead.
//
// Membership rules:
//   - Trimmed lowercase EXACT match required to trigger sanitization.
//   - Case-sensitive: "Stale Data" or "STALE_DATA" won't trigger
//     (legitimate data may use those casings).
//   - When adding a new token, prefer the form the backend actually
//     emits. Legitimate enum values (BULLISH, N/A, Technology) must
//     not be added — they belong to vocabulary lists, not status.
//
// v2.6.0 extends the vocabulary with v5.60.0 era engine-drop +
// provider-error-classification tokens. v2.6.1 needs NO further
// vocabulary entries — scoring.py v5.3.0 emits two tokens via
// scoring_errors:
//   - 'provider_error:<class>' (e.g. 'provider_error:NetworkError')
//   - 'engine_dropped_valuation'
// Both are covered by v2.6.0's vocabulary already
// ('provider_error' exact match + 'engine_dropped_valuation'
// exact match), and the 'provider_error:NetworkError' form's
// colon-prefixed class label is preserved in the warnings array
// as intended (carries useful classification info).
//
// Source provenance:
//   - "stale_data", "rate_limited", "fallback_provider",
//     "fetch_failed", "fetch_timeout", "provider_error",
//     "missing_data", "data_unavailable" — observed in May 2026
//     audit on STNE.US, EXE.US, ML.PA, ALL.US in numeric cells.
//   - "fallback_used", "fetch_error", "provider_timeout",
//     "timeout", "no_data", "error", "failed", "unavailable",
//     "unknown_error" — defensive coverage for adjacent failure
//     modes (v2.5.4 baseline).
//   - "forecast_unavailable", "confidence_unavailable",
//     "intrinsic_outlier" — data_engine_v2 v5.54.1+ forecast
//     pipeline tokens (v2.5.4 baseline).
//   - "engine_dropped_valuation", "engine_drop_valuation" —
//     data_engine_v2 v5.60.0 Phase H/I/P engine drop reasons.
//     Routed to scoring_errors in the canonical contract but
//     defensively sanitized here. ALSO emitted by scoring.py
//     v5.3.0 via _row_engine_dropped_valuation().
//   - "intrinsic_unit_mismatch_suspected", "intrinsic_unit_too_high",
//     "intrinsic_unit_too_low" — data_engine_v2 v5.60.0 Phase Q
//     intrinsic-value unit-detection error classes.
//   - "provider_error_class", "last_error_class" — Phase Q error
//     classification markers; should appear as values inside the
//     warnings array, not as values for numeric columns.
var _KNOWN_STATUS_TOKENS_ = Object.freeze({
  // Provider / fetch failures (v2.5.4 baseline)
  'stale_data': true,
  'rate_limited': true,
  'fallback_provider': true,
  'fallback_used': true,
  'fetch_failed': true,
  'fetch_timeout': true,
  'fetch_error': true,
  'provider_error': true,
  'provider_timeout': true,
  'timeout': true,
  // Data completeness (v2.5.4 baseline)
  'missing_data': true,
  'data_unavailable': true,
  'no_data': true,
  // v5.54.1 forecast pipeline (v2.5.4 baseline)
  'forecast_unavailable': true,
  'confidence_unavailable': true,
  'intrinsic_outlier': true,
  // Generic catch-alls (v2.5.4 baseline)
  'error': true,
  'failed': true,
  'unavailable': true,
  'unknown_error': true,
  // v2.6.0 / v5.60.0 — Phase H/I/P engine drop reasons
  // (also emitted by scoring.py v5.3.0 via scoring_errors)
  'engine_dropped_valuation': true,
  'engine_drop_valuation': true,
  // v2.6.0 / v5.60.0 — Phase Q intrinsic-value unit-detection
  'intrinsic_unit_mismatch_suspected': true,
  'intrinsic_unit_too_high': true,
  'intrinsic_unit_too_low': true,
  // v2.6.0 / v5.60.0 — Phase Q error classification markers
  'provider_error_class': true,
  'last_error_class': true
});

// v2.5.4 — Headers that legitimately carry text status / warnings.
// The status-token sanitization is SKIPPED for these columns so a
// row's Warnings cell can hold "stale_data | rate_limited" without
// triggering double-sanitization, and Recommendation Reason can
// quote a token in its narrative without being clobbered.
//
// v2.6.0 NOTE: the 04_Format.gs v2.7.0+ status-column CF chain
// recognizes "Engine:" and "unavailable" as warning tokens in
// these columns. The text columns intentionally carry these
// strings as their content — sanitization is correctly skipped
// here.
//
// v2.6.1 NOTE: 'Reco Source' is NOT added to this set. The Reco
// Source column intentionally carries a constant per-deploy
// provenance tag ("scoring.py v5.3.0"). The tag is never a known
// status token (it's not in _KNOWN_STATUS_TOKENS_), so the
// sanitization check is a no-op for that column and no special
// exemption is needed. If a future scoring.py revision starts
// emitting the source field as one of the status tokens — which
// would be a serious semantic regression — we'd want sanitization
// to FIRE rather than be skipped, so 'Reco Source' must stay out
// of _STATUS_TEXT_HEADERS_.
//
// v2.7.0 NOTE: 'Scoring Errors' IS added to this set. Unlike
// 'Reco Source', the Scoring Errors column legitimately carries
// status-token-shaped strings: scoring.py v5.4.2 emits "; "-joined
// error lists like "provider_error:NetworkError;
// engine_dropped_valuation". Without the allowlist exemption, a
// single bare token alone in the field (e.g. just "provider_error")
// WOULD trigger sanitization (the token IS in
// _KNOWN_STATUS_TOKENS_) and get clobbered + routed to Warnings,
// leaving Scoring Errors blank — that would defeat the field's
// purpose as a multi-token error log. The allowlist preserves the
// semantic role. Multi-token values like "provider_error:NetworkError;
// engine_dropped_valuation" don't trigger sanitization anyway
// (the exact-match check requires the whole string match a vocab
// key, which a "; "-joined list never does); the allowlist is
// specifically for the single-token edge case.
//
// v2.7.0 NOTE: 'Scoring Reco Source' is NOT added to this set.
// Same rationale as v2.6.1's 'Reco Source' exemption: the column
// intentionally carries a constant per-deploy scoring.py
// provenance tag ("scoring.py v5.74"). Never a known status token.
var _STATUS_TEXT_HEADERS_ = Object.freeze({
  'Warnings': true,
  'Notes': true,
  'User Notes': true,
  'Recommendation Reason': true,
  'Recommendation Detail': true,
  'Reason': true,
  'selection_reason': true,
  'criteria_snapshot': true,
  'Scoring Errors': true,
  // v2.8.0 — investability-gate narrative / enum fields. These
  // columns may legitimately carry status-like words inside text.
  'Forecast Source': true,
  'Conflict Type': true,
  'Final Decision Basis': true,
  'Investability Status': true,
  'Final Action': true,
  'Block Reason': true
});

/* =========================================================
 * Public HTTP helpers
 * ========================================================= */

function apiGet_(path, params, options) {
  return apiFetchJson_('GET', path, null, params || null, options || {});
}

function apiPost_(path, payload, options) {
  return apiFetchJson_('POST', path, payload || {}, null, options || {});
}

function apiFetchJson_(method, path, payload, params, options) {
  validateRequiredConfig_();

  method = sanitizeString_(method || 'GET').toUpperCase();
  path = sanitizeString_(path);
  options = options || {};

  var url = buildUrl_(path, params);
  var requestId = buildRequestId_();
  var headers = buildAuthHeaders_(requestId, options.extraHeaders || {});
  var startedAt = new Date().getTime();

  var fetchOptions = {
    method: method,
    headers: headers,
    muteHttpExceptions: true,
    followRedirects: true
  };

  if (['POST', 'PUT', 'PATCH'].indexOf(method) !== -1) {
    fetchOptions.contentType = 'application/json';
    fetchOptions.payload = JSON.stringify(payload || {});
  }

  var response = retryWithBackoff_(function () {
    return UrlFetchApp.fetch(url, fetchOptions);
  }, {
    maxRetries: options.maxRetries || CORE_RETRY_MAX_,
    baseDelayMs: options.baseDelayMs || CORE_RETRY_BASE_MS_,
    actionName: 'apiFetchJson_',
    path: path
  });

  var endedAt = new Date().getTime();
  var durationMs = endedAt - startedAt;

  var statusCode = response.getResponseCode();
  var responseHeaders = response.getAllHeaders ? response.getAllHeaders() : {};
  var rawText = '';

  try {
    rawText = response.getContentText();
  } catch (e) {
    rawText = '';
  }

  var json = safeJsonParse_(rawText);
  var payloadObj = unwrapPayload_(json);
  var warnings = collectWarnings_(json, payloadObj);

  return {
    ok: statusCode >= 200 && statusCode < 300,
    statusCode: statusCode,
    path: path,
    url: url,
    method: method,
    headers: responseHeaders || {},
    rawText: rawText,
    json: json,
    payload: payloadObj,
    warnings: warnings,
    durationMs: durationMs,
    requestId: requestId,
    message: deriveApiMessage_(statusCode, json, rawText)
  };
}

function buildUrl_(path, params) {
  var baseUrl = getBaseUrl();
  var cleanPath = sanitizeString_(path || '');

  if (!cleanPath) {
    throw new Error('API path is required.');
  }

  if (cleanPath.charAt(0) !== '/') {
    cleanPath = '/' + cleanPath;
  }

  var url = baseUrl + cleanPath;
  var query = buildQueryString_(params || {});

  if (query) {
    url += '?' + query;
  }

  return url;
}

function buildAuthHeaders_(requestId, extraHeaders) {
  var token = getApiToken();
  var headers = {
    Accept: 'application/json',
    'X-APP-TOKEN': token,
    'X-API-Key': token,
    Authorization: 'Bearer ' + token,
    'X-Request-ID': requestId || buildRequestId_()
  };

  if (extraHeaders) {
    var keys = Object.keys(extraHeaders);
    for (var i = 0; i < keys.length; i++) {
      headers[keys[i]] = extraHeaders[keys[i]];
    }
  }

  return headers;
}

/* =========================================================
 * Retry, lock, cache
 * ========================================================= */

function withScriptLock_(fn, timeoutMs) {
  if (typeof fn !== 'function') {
    throw new Error('withScriptLock_ requires a function.');
  }

  // v2.11.1: reuse an outer coordinator-owned DocumentLock in this SAME
  // Apps Script execution. Other executions have isolated globals and still
  // block on the real DocumentLock, so cross-execution exclusion is unchanged.
  if (CORE_DOCUMENT_LOCK_DEPTH_ > 0) {
    CORE_DOCUMENT_LOCK_DEPTH_ += 1;
    try {
      return fn();
    } finally {
      CORE_DOCUMENT_LOCK_DEPTH_ = Math.max(0, CORE_DOCUMENT_LOCK_DEPTH_ - 1);
    }
  }

  var lock = LockService.getDocumentLock();
  lock.waitLock(timeoutMs || CORE_LOCK_TIMEOUT_MS_);
  CORE_DOCUMENT_LOCK_DEPTH_ = 1;
  CORE_DOCUMENT_LOCK_LABEL_ = 'withScriptLock_';

  try {
    return fn();
  } finally {
    CORE_DOCUMENT_LOCK_DEPTH_ = 0;
    CORE_DOCUMENT_LOCK_LABEL_ = '';
    try {
      lock.releaseLock();
    } catch (e) {}
  }
}

/* =========================================================
 * v2.11.0 — Manual refresh priority coordinator
 * ========================================================= */

function _coreManualPriorityNowMs_() {
  return new Date().getTime();
}

function _coreManualPriorityCleanText_(value) {
  if (value === null || value === undefined) return '';
  return String(value).replace(/^\s+|\s+$/g, '');
}

function _coreManualPriorityClampInt_(value, minValue, maxValue, fallback) {
  var n = parseInt(String(value === null || value === undefined ? '' : value), 10);
  if (!isFinite(n)) n = fallback;
  if (n < minValue) n = minValue;
  if (n > maxValue) n = maxValue;
  return n;
}

function _coreManualPriorityProperty_(name) {
  try {
    return PropertiesService.getScriptProperties().getProperty(name);
  } catch (e) {
    return null;
  }
}

function _coreManualPriorityEnabled_() {
  var raw = _coreManualPriorityProperty_(CORE_MANUAL_PRIORITY_ENABLED_PROP_);
  if (raw === null || raw === undefined || String(raw).trim() === '') return true;
  var s = String(raw).trim().toLowerCase();
  return ['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) === -1;
}

function _coreManualPriorityTtlMs_() {
  var sec = _coreManualPriorityClampInt_(
    _coreManualPriorityProperty_(CORE_MANUAL_PRIORITY_TTL_SEC_PROP_),
    120,
    3600,
    CORE_MANUAL_PRIORITY_DEFAULT_TTL_SEC_
  );
  return sec * 1000;
}

function _coreManualPriorityLockWaitMs_() {
  var sec = _coreManualPriorityClampInt_(
    _coreManualPriorityProperty_(CORE_MANUAL_LOCK_WAIT_SEC_PROP_),
    30,
    300,
    CORE_MANUAL_LOCK_WAIT_DEFAULT_SEC_
  );
  return sec * 1000;
}

function _coreManualPriorityPollMs_() {
  return _coreManualPriorityClampInt_(
    _coreManualPriorityProperty_(CORE_MANUAL_LOCK_POLL_MS_PROP_),
    500,
    10000,
    CORE_MANUAL_LOCK_POLL_DEFAULT_MS_
  );
}

function _coreManualPriorityParseState_(raw) {
  if (!raw) return null;
  var parsed;
  try {
    parsed = JSON.parse(String(raw));
  } catch (e) {
    return null;
  }
  if (!parsed || typeof parsed !== 'object') return null;
  if (Number(parsed.schemaVersion || 0) !== CORE_MANUAL_PRIORITY_SCHEMA_VERSION_) return null;
  if (!_coreManualPriorityCleanText_(parsed.token || '')) return null;
  return parsed;
}

function _coreManualPriorityReadState_() {
  var raw = _coreManualPriorityProperty_(CORE_MANUAL_PRIORITY_STATE_PROP_);
  var state = _coreManualPriorityParseState_(raw);
  if (!state) return null;

  var expiresAtMs = Number(state.expiresAtMs || 0);
  if (!isFinite(expiresAtMs) || expiresAtMs <= _coreManualPriorityNowMs_()) {
    return null;
  }
  return state;
}

function _coreManualPriorityWithStateLock_(fn) {
  var lock = LockService.getScriptLock();
  lock.waitLock(CORE_MANUAL_PRIORITY_STATE_LOCK_MS_);
  try {
    return fn();
  } finally {
    try {
      lock.releaseLock();
    } catch (e) {}
  }
}

function _coreManualPriorityWriteState_(state) {
  PropertiesService.getScriptProperties().setProperty(
    CORE_MANUAL_PRIORITY_STATE_PROP_,
    JSON.stringify(state)
  );
}

function _coreManualPriorityDeleteState_() {
  PropertiesService.getScriptProperties().deleteProperty(CORE_MANUAL_PRIORITY_STATE_PROP_);
}

function _coreManualPriorityNewToken_() {
  try {
    return Utilities.getUuid();
  } catch (e) {
    return 'manual-' + _coreManualPriorityNowMs_() + '-' + Math.floor(Math.random() * 1000000);
  }
}

/**
 * Record manual intent BEFORE waiting on the DocumentLock. Only one live
 * request is accepted; repeated clicks return busy rather than queueing the
 * same expensive refresh multiple times.
 */
function requestManualRefreshPriority_(label, options) {
  options = options || {};
  var cleanLabel = _coreManualPriorityCleanText_(label || 'Manual Refresh') || 'Manual Refresh';

  if (!_coreManualPriorityEnabled_()) {
    return { ok: true, enabled: false, token: '', action: 'priority-disabled' };
  }

  return _coreManualPriorityWithStateLock_(function () {
    var props = PropertiesService.getScriptProperties();
    var raw = props.getProperty(CORE_MANUAL_PRIORITY_STATE_PROP_);
    var current = _coreManualPriorityParseState_(raw);
    var nowMs = _coreManualPriorityNowMs_();

    if (current && Number(current.expiresAtMs || 0) > nowMs) {
      return {
        ok: false,
        busy: true,
        action: 'manual-already-pending',
        state: current,
        message: 'Another manual refresh is already pending or active: ' +
          _coreManualPriorityCleanText_(current.label || 'Manual Refresh')
      };
    }

    // Remove malformed/expired residue before establishing the new owner.
    if (raw) props.deleteProperty(CORE_MANUAL_PRIORITY_STATE_PROP_);

    var token = _coreManualPriorityNewToken_();
    var ttlMs = options.ttlMs
      ? _coreManualPriorityClampInt_(options.ttlMs, 120000, 3600000, _coreManualPriorityTtlMs_())
      : _coreManualPriorityTtlMs_();
    var state = {
      schemaVersion: CORE_MANUAL_PRIORITY_SCHEMA_VERSION_,
      token: token,
      phase: 'requested',
      label: cleanLabel,
      requestedAtMs: nowMs,
      requestedAt: new Date(nowMs).toISOString(),
      activeAtMs: 0,
      activeAt: '',
      expiresAtMs: nowMs + ttlMs,
      expiresAt: new Date(nowMs + ttlMs).toISOString()
    };
    _coreManualPriorityWriteState_(state);

    return { ok: true, enabled: true, token: token, action: 'manual-requested', state: state };
  });
}

function _coreManualPriorityRefreshOwnedState_(token, phase) {
  if (!token || !_coreManualPriorityEnabled_()) return null;

  return _coreManualPriorityWithStateLock_(function () {
    var props = PropertiesService.getScriptProperties();
    var current = _coreManualPriorityParseState_(
      props.getProperty(CORE_MANUAL_PRIORITY_STATE_PROP_)
    );
    if (!current || String(current.token) !== String(token)) return null;

    var nowMs = _coreManualPriorityNowMs_();
    current.expiresAtMs = nowMs + _coreManualPriorityTtlMs_();
    current.expiresAt = new Date(current.expiresAtMs).toISOString();
    if (phase) current.phase = phase;
    if (phase === 'active' && !Number(current.activeAtMs || 0)) {
      current.activeAtMs = nowMs;
      current.activeAt = new Date(nowMs).toISOString();
    }
    _coreManualPriorityWriteState_(current);
    return current;
  });
}

function clearManualRefreshPriority_(token, reason, force) {
  if (!_coreManualPriorityEnabled_() && !force) {
    return { ok: true, cleared: false, action: 'priority-disabled' };
  }

  return _coreManualPriorityWithStateLock_(function () {
    var props = PropertiesService.getScriptProperties();
    var current = _coreManualPriorityParseState_(
      props.getProperty(CORE_MANUAL_PRIORITY_STATE_PROP_)
    );
    if (!current) {
      props.deleteProperty(CORE_MANUAL_PRIORITY_STATE_PROP_);
      return { ok: true, cleared: false, action: 'no-priority-state' };
    }
    if (!force && String(current.token) !== String(token || '')) {
      return {
        ok: false,
        cleared: false,
        action: 'owner-token-mismatch',
        state: current
      };
    }
    props.deleteProperty(CORE_MANUAL_PRIORITY_STATE_PROP_);
    return {
      ok: true,
      cleared: true,
      action: 'manual-priority-cleared',
      reason: _coreManualPriorityCleanText_(reason || '')
    };
  });
}

function getManualRefreshPriorityStatus_() {
  var state = _coreManualPriorityReadState_();
  return {
    ok: true,
    enabled: _coreManualPriorityEnabled_(),
    pending: !!state,
    phase: state ? _coreManualPriorityCleanText_(state.phase || 'requested') : 'idle',
    label: state ? _coreManualPriorityCleanText_(state.label || '') : '',
    requestedAt: state ? _coreManualPriorityCleanText_(state.requestedAt || '') : '',
    activeAt: state ? _coreManualPriorityCleanText_(state.activeAt || '') : '',
    expiresAt: state ? _coreManualPriorityCleanText_(state.expiresAt || '') : '',
    token: state ? _coreManualPriorityCleanText_(state.token || '') : ''
  };
}

function getManualRefreshPriorityStatus() {
  return getManualRefreshPriorityStatus_();
}

function clearManualRefreshPriorityEmergency() {
  return clearManualRefreshPriority_('', 'operator-emergency-clear', true);
}

function isManualRefreshPriorityRequested_() {
  if (!_coreManualPriorityEnabled_()) return false;
  return !!_coreManualPriorityReadState_();
}

/**
 * Cooperative checkpoint for AUTOMATIC loops. This function never changes
 * state and never releases a lock. Callers must return normally so their
 * existing finally block releases the DocumentLock.
 */
function tfbAutomaticYieldPoint_(context) {
  var state = _coreManualPriorityReadState_();
  if (!state || !_coreManualPriorityEnabled_()) {
    return { yield: false, action: 'continue' };
  }
  return {
    yield: true,
    action: 'yield-manual-priority',
    reason: 'manual_refresh_requested',
    context: _coreManualPriorityCleanText_(context || ''),
    label: _coreManualPriorityCleanText_(state.label || 'Manual Refresh'),
    phase: _coreManualPriorityCleanText_(state.phase || 'requested'),
    requestedAt: _coreManualPriorityCleanText_(state.requestedAt || ''),
    expiresAt: _coreManualPriorityCleanText_(state.expiresAt || '')
  };
}

/**
 * Manual wrapper used by menu entrypoints. It announces intent, waits for the
 * current automatic owner to yield, runs alone under DocumentLock, and always
 * clears the priority state. Existing writer functions remain unchanged.
 */
function withManualRefreshPriority_(label, fn, options) {
  if (typeof fn !== 'function') {
    throw new Error('withManualRefreshPriority_ requires a function.');
  }
  options = options || {};

  if (!_coreManualPriorityEnabled_()) {
    return withScriptLock_(fn, options.lockWaitMs || CORE_LOCK_TIMEOUT_MS_);
  }

  var claim = requestManualRefreshPriority_(label, options);
  if (!claim.ok) {
    throw new Error(claim.message || 'Another manual refresh is already pending.');
  }

  var token = claim.token;
  var lock = LockService.getDocumentLock();
  var acquired = false;
  var startedAt = _coreManualPriorityNowMs_();
  var waitMs = options.lockWaitMs
    ? _coreManualPriorityClampInt_(options.lockWaitMs, 30000, 300000, _coreManualPriorityLockWaitMs_())
    : _coreManualPriorityLockWaitMs_();
  var pollMs = options.pollMs
    ? _coreManualPriorityClampInt_(options.pollMs, 500, 10000, _coreManualPriorityPollMs_())
    : _coreManualPriorityPollMs_();

  try {
    while (!acquired) {
      var elapsed = _coreManualPriorityNowMs_() - startedAt;
      var remaining = waitMs - elapsed;
      if (remaining <= 0) break;

      var owned = _coreManualPriorityReadState_();
      if (!owned || String(owned.token) !== String(token)) {
        throw new Error('Manual refresh priority ownership was lost before lock acquisition.');
      }

      acquired = lock.tryLock(Math.min(pollMs, remaining));
      if (!acquired) _coreManualPriorityRefreshOwnedState_(token, 'requested');
    }

    if (!acquired) {
      throw new Error(
        'Manual refresh could not obtain the document lock within ' +
        Math.round(waitMs / 1000) + ' seconds. The priority request was cleared safely.'
      );
    }

    var activeState = _coreManualPriorityRefreshOwnedState_(token, 'active');
    if (!activeState) {
      throw new Error('Manual refresh priority ownership was lost after lock acquisition.');
    }

    // v2.11.1: nested refreshPage_/refreshPageInBatches_ wrappers reuse this
    // already-owned DocumentLock instead of trying to acquire it again.
    CORE_DOCUMENT_LOCK_DEPTH_ = 1;
    CORE_DOCUMENT_LOCK_LABEL_ = 'manual:' + String(label || 'Manual Refresh');
    return fn();
  } finally {
    if (acquired) {
      CORE_DOCUMENT_LOCK_DEPTH_ = 0;
      CORE_DOCUMENT_LOCK_LABEL_ = '';
      try {
        SpreadsheetApp.flush();
      } catch (flushErr) {}
      try {
        lock.releaseLock();
      } catch (releaseErr) {}
    }
    try {
      clearManualRefreshPriority_(token, 'manual-wrapper-finally', false);
    } catch (clearErr) {}
  }
}

/**
 * Automatic wrapper for refreshers that do NOT already take DocumentLock
 * (for example decision cockpits). It checks manual priority on both sides of
 * lock acquisition to close the check-then-lock race.
 */
function withAutomaticRefreshPriorityLock_(label, fn, options) {
  if (typeof fn !== 'function') {
    throw new Error('withAutomaticRefreshPriorityLock_ requires a function.');
  }
  options = options || {};

  var pre = tfbAutomaticYieldPoint_(label || 'automatic-refresh');
  if (pre.yield) {
    return {
      ok: true,
      skipped: true,
      action: 'skipped-manual-priority',
      reason: pre.reason,
      priority: pre
    };
  }

  var lock = LockService.getDocumentLock();
  var waitMs = _coreManualPriorityClampInt_(options.lockWaitMs, 0, 30000, 1000);
  var acquired = lock.tryLock(waitMs);
  if (!acquired) {
    return {
      ok: true,
      skipped: true,
      action: 'skipped-lock-busy',
      reason: 'document_lock_busy'
    };
  }

  try {
    var post = tfbAutomaticYieldPoint_(label || 'automatic-refresh');
    if (post.yield) {
      return {
        ok: true,
        skipped: true,
        action: 'skipped-manual-priority',
        reason: post.reason,
        priority: post
      };
    }
    CORE_DOCUMENT_LOCK_DEPTH_ = 1;
    CORE_DOCUMENT_LOCK_LABEL_ = 'automatic:' + String(label || 'automatic-refresh');
    return fn();
  } finally {
    CORE_DOCUMENT_LOCK_DEPTH_ = 0;
    CORE_DOCUMENT_LOCK_LABEL_ = '';
    try {
      lock.releaseLock();
    } catch (e) {}
  }
}

function retryWithBackoff_(fn, options) {
  options = options || {};

  var maxRetries = Math.max(1, options.maxRetries || CORE_RETRY_MAX_);
  var baseDelayMs = Math.max(250, options.baseDelayMs || CORE_RETRY_BASE_MS_);
  var actionName = sanitizeString_(options.actionName || 'operation');
  var path = sanitizeString_(options.path || '');

  var attempt = 0;
  var lastError = null;

  while (attempt < maxRetries) {
    try {
      return fn();
    } catch (err) {
      lastError = err;
      attempt++;

      if (attempt >= maxRetries) {
        throw err;
      }

      var responseCode = tryExtractResponseCode_(err);
      var retryable = isRetryableStatus_(responseCode);

      if (!retryable && responseCode !== null) {
        throw err;
      }

      var sleepMs = Math.min(8000, baseDelayMs * Math.pow(2, attempt - 1));
      Utilities.sleep(sleepMs);
    }
  }

  throw lastError || new Error('Retry failed: ' + actionName + ' ' + path);
}

function cacheGetJson_(key) {
  var cache = CacheService.getScriptCache();
  var raw = cache.get(CORE_CACHE_PREFIX_ + key);
  return raw ? safeJsonParse_(raw) : null;
}

function cachePutJson_(key, value, ttlSec) {
  var cache = CacheService.getScriptCache();
  cache.put(CORE_CACHE_PREFIX_ + key, JSON.stringify(value), ttlSec || DEFAULTS.CACHE_TTL_SEC);
}

function cacheRemove_(key) {
  var cache = CacheService.getScriptCache();
  cache.remove(CORE_CACHE_PREFIX_ + key);
}

/* =========================================================
 * JSON / payload helpers
 * ========================================================= */

function safeJsonParse_(text) {
  if (text === null || text === undefined) return null;

  var s = String(text).trim();
  if (!s) return null;

  try {
    return JSON.parse(s);
  } catch (e) {
    return null;
  }
}

/**
 * v2.5.0 (preserved through v2.6.1)
 * Stop unwrapping as soon as the current object is already table-shaped.
 * This prevents losing row_objects when the backend sends both wrapper
 * fields and table fields in the same object.
 */
function unwrapPayload_(obj) {
  if (!isPlainObject_(obj)) return obj;

  var NEST_KEYS = ['data', 'result', 'payload', 'body', 'payload_obj'];
  var current = obj;
  var depth = 0;

  while (isPlainObject_(current) && depth < 6) {
    if (isTableShapedPayload_(current)) {
      return current;
    }

    var advanced = false;

    for (var i = 0; i < NEST_KEYS.length; i++) {
      var key = NEST_KEYS[i];

      if (isPlainObject_(current[key])) {
        current = current[key];
        depth++;
        advanced = true;
        break;
      }
    }

    if (!advanced) break;
  }

  return current;
}

function isTableShapedPayload_(obj) {
  if (!isPlainObject_(obj)) return false;

  if (Array.isArray(obj.headers)) return true;
  if (Array.isArray(obj.display_headers)) return true;
  if (Array.isArray(obj.sheet_headers)) return true;
  if (Array.isArray(obj.column_headers)) return true;

  if (Array.isArray(obj.keys)) return true;
  if (Array.isArray(obj.fields)) return true;
  if (Array.isArray(obj.columns)) return true;
  if (Array.isArray(obj.schema_keys)) return true;

  for (var i = 0; i < ROW_LIST_ALIASES_.length; i++) {
    var key = ROW_LIST_ALIASES_[i];
    if (Array.isArray(obj[key])) return true;
  }

  return false;
}

function collectWarnings_(root, payload) {
  var warnings = [];

  if (isPlainObject_(root) && Array.isArray(root.warnings)) {
    warnings = warnings.concat(root.warnings);
  }

  if (isPlainObject_(root) && isPlainObject_(root.meta) && Array.isArray(root.meta.warnings)) {
    warnings = warnings.concat(root.meta.warnings);
  }

  if (isPlainObject_(payload) && Array.isArray(payload.warnings)) {
    warnings = warnings.concat(payload.warnings);
  }

  if (isPlainObject_(payload) && isPlainObject_(payload.meta) && Array.isArray(payload.meta.warnings)) {
    warnings = warnings.concat(payload.meta.warnings);
  }

  return uniqueStrings_(warnings.map(String));
}

function extractHeaders_(payload) {
  payload = payload || {};

  var headers =
    payload.headers ||
    payload.display_headers ||
    payload.sheet_headers ||
    payload.column_headers ||
    [];

  return Array.isArray(headers) ? headers.map(String) : [];
}

function extractKeys_(payload) {
  payload = payload || {};

  var keys =
    payload.keys ||
    payload.fields ||
    payload.columns ||
    payload.schema_keys ||
    [];

  return Array.isArray(keys) ? keys.map(String) : [];
}

/**
 * v2.5.0 (preserved through v2.6.1)
 * Prefer object containers before matrix rows.
 * row_objects usually contains the complete API-field object needed
 * by writeRowsBySymbol_ — including the 5 multi-method view fields,
 * the 5 Wave 3 Insights fields, the 2 Decision Matrix + 5 Candlestick
 * fields (under v2.8.0), and the 2 Canonical Provenance fields
 * (under v5.3.0).
 */
function extractRows_(payload) {
  payload = payload || {};

  var objectPreferred = [
    { key: 'row_objects', type: 'row_objects' },
    { key: 'records', type: 'records' },
    { key: 'items', type: 'items' },
    { key: 'quotes', type: 'quotes' },
    { key: 'results', type: 'results' }
  ];

  for (var i = 0; i < objectPreferred.length; i++) {
    var oc = objectPreferred[i];
    if (Array.isArray(payload[oc.key]) && payload[oc.key].length > 0 && isPlainObject_(payload[oc.key][0])) {
      return { type: oc.type, rows: payload[oc.key] };
    }
  }

  var generic = [
    { key: 'rows', type: 'rows' },
    { key: 'data', type: 'data' },
    { key: 'rows_matrix', type: 'rows_matrix' }
  ];

  for (var j = 0; j < generic.length; j++) {
    var gc = generic[j];
    if (Array.isArray(payload[gc.key])) {
      return { type: gc.type, rows: payload[gc.key] };
    }
  }

  for (var k = 0; k < objectPreferred.length; k++) {
    var fallback = objectPreferred[k];
    if (Array.isArray(payload[fallback.key])) {
      return { type: fallback.type, rows: payload[fallback.key] };
    }
  }

  return { type: 'none', rows: [] };
}

function to2DArray_(rows, keys) {
  rows = Array.isArray(rows) ? rows : [];
  keys = Array.isArray(keys) ? keys : [];

  if (!rows.length) return [];

  if (Array.isArray(rows[0])) {
    return rows.map(function (row) {
      return Array.isArray(row) ? row.slice() : [];
    });
  }

  if (isPlainObject_(rows[0])) {
    return rows.map(function (row) {
      return keys.map(function (key) {
        var v = extractApiValue_(row, key);
        return normalizeCellValue_(v !== undefined && v !== null ? v : '');
      });
    });
  }

  return rows.map(function (row) {
    return [normalizeCellValue_(row)];
  });
}

/**
 * v2.5.0 (preserved through v2.6.1)
 * rowObjects are preserved from object containers and not overwritten
 * by legacy matrix rows.
 */
function normalizeTablePayload_(payload) {
  payload = payload || {};

  var headers = extractHeaders_(payload);
  var keys = extractKeys_(payload);
  var extracted = extractRows_(payload);
  var rowType = extracted.type;
  var rows = extracted.rows || [];

  if (!keys.length && headers.length) {
    keys = headers.slice();
  }

  var rows2d = to2DArray_(rows, keys.length ? keys : headers);

  var rowObjects = [];

  if (rows.length > 0) {
    if (isPlainObject_(rows[0])) {
      rowObjects = rows.slice();
    } else if (Array.isArray(rows[0]) && keys.length > 0) {
      rowObjects = rows.map(function (arr) {
        var o = {};

        for (var i = 0; i < keys.length; i++) {
          var k = keys[i];
          o[k] = arr && i < arr.length ? arr[i] : null;
        }

        return o;
      });
    }
  }

  // Authoritative override: if payload top-level row_objects is an
  // array of objects, ALWAYS prefer it over synthesized rowObjects.
  if (Array.isArray(payload.row_objects) && payload.row_objects.length > 0 && isPlainObject_(payload.row_objects[0])) {
    rowObjects = payload.row_objects.slice();
  }

  return {
    headers: headers,
    keys: keys,
    rows2d: rows2d,
    rowObjects: rowObjects,
    rowType: rowType,
    rowCount: rows.length
  };
}

/* =========================================================
 * Logging / status sheets
 * ========================================================= */

function logRun_(level, action, page, message, details) {
  level = level || 'INFO';
  action = action || '';
  page = page || '';
  message = message || '';
  details = details || {};

  var sheet = getLogSheet_();
  var headers = [
    'Timestamp',
    'Level',
    'Action',
    'Page',
    'Status',
    'Message',
    'Endpoint',
    'HTTP Code',
    'Duration ms',
    'Details JSON'
  ];

  ensureHeaderRow_(sheet, headers);

  var row = [
    new Date(),
    sanitizeString_(level).toUpperCase(),
    sanitizeString_(action),
    sanitizeString_(page),
    sanitizeString_(details.status),
    sanitizeString_(message),
    sanitizeString_(details.endpoint),
    details.httpCode || '',
    details.durationMs || '',
    truncateText_(safeStringify_(details), 50000)
  ];

  sheet.appendRow(row);
  hideSheetIfNeeded_(sheet);
}

function getLogSheet_() {
  return getOrCreateSheet_(getLogSheetName(), true);
}

/* v-STAMP-GUARD (2026-08-28) - PRESERVE BACKEND [STATUS-STAMP] ROWS.
 * FORENSIC WHY: writePageStatus_ overwrites a page's _Status row A..J
 * unconditionally. Three consecutive nights the backend stamp on
 * Global_Markets ([STATUS-STAMP v6.4x] with data=/guard=/sha=/run=) was
 * destroyed within hours by routine GAS batch-progress writes, leaving the
 * one page without a machine-readable freshness contract. Guard: when the
 * EXISTING row's Message begins with '[STATUS-STAMP v' and Script Property
 * TFB_GAS_STAMP_GUARD is not '0' (default ON), the GAS write is suppressed
 * (Logger note only). Backend stamps supersede each other via the API path,
 * which this function never handles, so nothing can wedge. GAS progress
 * remains fully visible in _Run_Log. Kill switch: TFB_GAS_STAMP_GUARD='0'.
 * Functions added: 0. Removed: 0. */
function writePageStatus_(page, statusObj) {
  page = sanitizeString_(page || '');
  statusObj = statusObj || {};

  var sheet = getStatusSheet_();
  var headers = [
    'Page',
    'Last Updated',
    'Status',
    'Message',
    'Endpoint',
    'HTTP Code',
    'Rows',
    'Columns',
    'Duration ms',
    'Warnings'
  ];

  ensureHeaderRow_(sheet, headers);

  var values = sheet.getDataRange().getValues();
  var targetRow = -1;

  for (var i = 1; i < values.length; i++) {
    if (String(values[i][0] || '') === page) {
      targetRow = i + 1;
      break;
    }
  }

  // v-STAMP-GUARD: preserve backend stamps (see block above).
  try {
    if (targetRow > 0) {
      var sgOn = String((PropertiesService.getScriptProperties()
          .getProperty('TFB_GAS_STAMP_GUARD') || '1')).trim() !== '0';
      var sgMsg = String(values[targetRow - 1][3] || '');
      if (sgOn && sgMsg.indexOf('[STATUS-STAMP v') === 0) {
        Logger.log('[CORE STAMP-GUARD] ' + page + ': backend stamp preserved; ' +
            'GAS status suppressed (status=' +
            sanitizeString_(statusObj.status) + ')');
        return;
      }
    }
  } catch (sgErr) {}

  var warningsStr = Array.isArray(statusObj.warnings)
    ? statusObj.warnings.join(' | ')
    : sanitizeString_(statusObj.warnings);

  var rowValues = [[
    page,
    new Date(),
    sanitizeString_(statusObj.status),
    sanitizeString_(statusObj.message),
    sanitizeString_(statusObj.endpoint),
    statusObj.httpCode || '',
    statusObj.rows || '',
    statusObj.columns || '',
    statusObj.durationMs || '',
    warningsStr
  ]];

  var rowToUpdate = targetRow === -1 ? sheet.getLastRow() + 1 : targetRow;
  sheet.getRange(rowToUpdate, 1, 1, rowValues[0].length).setValues(rowValues);

  hideSheetIfNeeded_(sheet);
}

function writeGlobalStatus_(statusObj) {
  statusObj = statusObj || {};

  var sheet = getStatusSheet_();
  var startRow = 1;
  var startCol = 12;

  var rows = [
    ['Global Key', 'Value'],
    ['Last Global Update', new Date()],
    ['Backend URL', getBaseUrl()],
    ['Token Loaded', getApiToken() ? 'Yes' : 'No'],
    ['Last Health Status', sanitizeString_(statusObj.healthStatus)],
    ['Last Meta Version', sanitizeString_(statusObj.version)],
    ['Last Message', sanitizeString_(statusObj.message)]
  ];

  sheet.getRange(startRow, startCol, rows.length, 2).setValues(rows);
  hideSheetIfNeeded_(sheet);
}

function getStatusSheet_() {
  return getOrCreateSheet_(getStatusSheetName(), true);
}

/* =========================================================
 * Sheet helpers
 * ========================================================= */

function getOrCreateSheet_(name, hidden) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(name);

  if (!sheet) {
    sheet = ss.insertSheet(name);
  }

  if (hidden) {
    hideSheetIfNeeded_(sheet);
  }

  return sheet;
}

function hideSheetIfNeeded_(sheet) {
  try {
    if (!sheet.isSheetHidden()) {
      sheet.hideSheet();
    }
  } catch (e) {}
}

function ensureHeaderRow_(sheet, headers) {
  if (!Array.isArray(headers) || !headers.length) return;

  var existingLastCol = Math.max(1, sheet.getLastColumn());
  var width = Math.max(existingLastCol, headers.length);
  var existing = sheet.getRange(1, 1, 1, width).getValues()[0];

  var same = headers.every(function (h, i) {
    return String(existing[i] || '') === String(h || '');
  });

  if (!same) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}

function findHeaderRow_(sheet) {
  if (!sheet) return 1;
  return 1;
}

function clearDataArea_(sheet, preserveHeaderRows, preserveManualColumns) {
  preserveHeaderRows = preserveHeaderRows || DEFAULTS.PRESERVE_HEADER_ROWS;
  preserveManualColumns = preserveManualColumns || [];

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();

  if (lastRow <= preserveHeaderRows || lastCol < 1) return;

  if (!preserveManualColumns.length) {
    sheet.getRange(preserveHeaderRows + 1, 1, lastRow - preserveHeaderRows, lastCol).clearContent();
    return;
  }

  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  var preserveSet = {};

  for (var i = 0; i < preserveManualColumns.length; i++) {
    preserveSet[String(preserveManualColumns[i])] = true;
  }

  var dataRange = sheet.getRange(preserveHeaderRows + 1, 1, lastRow - preserveHeaderRows, lastCol);
  var data = dataRange.getValues();

  for (var r = 0; r < data.length; r++) {
    for (var c = 0; c < headers.length; c++) {
      if (!preserveSet[headers[c]]) {
        data[r][c] = '';
      }
    }
  }

  dataRange.setValues(data);
}

function writeTableToSheet_(sheet, headers, rows2d, options) {
  headers = headers || [];
  rows2d = rows2d || [];
  options = options || {};

  var preserveHeaderRows = options.preserveHeaderRows || 1;
  var preserveManualColumns = options.preserveManualColumns || [];

  if (!headers.length) {
    throw new Error('Cannot write table without headers.');
  }

  ensureHeaderRow_(sheet, headers);
  clearDataArea_(sheet, preserveHeaderRows, preserveManualColumns);

  if (!rows2d.length) return;

  var width = headers.length;

  var normalizedRows = rows2d.map(function (row) {
    var out = Array.isArray(row) ? row.slice(0, width) : [];

    while (out.length < width) {
      out.push('');
    }

    return out;
  });

  sheet.getRange(preserveHeaderRows + 1, 1, normalizedRows.length, width).setValues(normalizedRows);
}

function getSheetDataAsObjects_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();

  if (lastRow < 2 || lastCol < 1) return [];

  var values = sheet.getRange(1, 1, lastRow, lastCol).getValues();
  var headers = values[0].map(String);
  var rows = [];

  for (var i = 1; i < values.length; i++) {
    var obj = {};

    for (var j = 0; j < headers.length; j++) {
      obj[headers[j]] = values[i][j];
    }

    rows.push(obj);
  }

  return rows;
}

function getSelectedRowContext_() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  var range = sheet.getActiveRange();

  if (!range) {
    throw new Error('No active selection found.');
  }

  var row = range.getRow();

  if (row <= 1) {
    throw new Error('Please select a data row below the header.');
  }

  var lastCol = Math.max(1, sheet.getLastColumn());
  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  var values = sheet.getRange(row, 1, 1, lastCol).getValues()[0];

  var record = {};

  for (var i = 0; i < headers.length; i++) {
    record[headers[i]] = values[i];
  }

  return {
    sheet: sheet,
    row: row,
    headers: headers,
    values: values,
    record: record
  };
}

/* =========================================================
 * Header-mapped sheet writers
 * ========================================================= */

function _normalizeHeaderText_(text) {
  if (text === null || text === undefined) return '';

  var s = String(text);

  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');

  return s.trim();
}

/**
 * v2.5.1 — performance: short-circuit at first match.
 * Behavior unchanged through v2.6.1.
 *
 * Resolution order:
 *   1. exact raw match (handles maps with literal-keyed entries)
 *   2. trimmed match (drops leading/trailing whitespace only)
 *   3. fully-normalized match (zero-width chars stripped, all
 *      whitespace collapsed to single space, trimmed)
 *   4. fully-normalized + lowercased match (final case-insensitive
 *      fallback)
 */
function coreResolveHeaderKey_(headerText, headerToKeyMap) {
  if (headerText === null || headerText === undefined) return null;
  if (!isPlainObject_(headerToKeyMap)) return null;

  var raw = String(headerText);

  // Path 1: exact raw match
  if (Object.prototype.hasOwnProperty.call(headerToKeyMap, raw)) {
    return headerToKeyMap[raw];
  }

  // Path 2: trimmed match
  var trimmed = raw.trim();
  if (trimmed && trimmed !== raw && Object.prototype.hasOwnProperty.call(headerToKeyMap, trimmed)) {
    return headerToKeyMap[trimmed];
  }

  // Path 3 & 4: normalized match (single map walk does both)
  var normalized = _normalizeHeaderText_(raw);
  if (!normalized) return null;

  var lower = normalized.toLowerCase();
  var mapKeys = Object.keys(headerToKeyMap);
  var lowerHit = null;  // remember case-insensitive hit but prefer exact-normalized

  for (var i = 0; i < mapKeys.length; i++) {
    var k = mapKeys[i];
    var kn = _normalizeHeaderText_(k);

    if (kn === normalized) {
      return headerToKeyMap[k];   // prefer exact-normalized match
    }

    if (lowerHit === null && kn.toLowerCase() === lower) {
      lowerHit = headerToKeyMap[k];   // case-insensitive fallback
    }
  }

  return lowerHit;
}

function getSheetHeaders_(sheet) {
  if (!sheet) return [];

  var lastCol = sheet.getLastColumn();
  if (lastCol < 1) return [];

  return sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(function (h) {
    return _normalizeHeaderText_(h);
  });
}

function findColumnIndex_(headers, headerName, options) {
  options = options || {};

  if (!Array.isArray(headers) || !headerName) return -1;

  var target = _normalizeHeaderText_(headerName);

  for (var i = 0; i < headers.length; i++) {
    if (_normalizeHeaderText_(headers[i]) === target) return i + 1;
  }

  if (options.caseInsensitive) {
    var lower = target.toLowerCase();

    for (var j = 0; j < headers.length; j++) {
      if (_normalizeHeaderText_(headers[j]).toLowerCase() === lower) return j + 1;
    }
  }

  return -1;
}

function buildHeaderIndex_(headers, headerToKeyMap) {
  var out = {};

  if (!Array.isArray(headers)) return out;

  for (var i = 0; i < headers.length; i++) {
    var key = coreResolveHeaderKey_(headers[i], headerToKeyMap || {});

    if (key && out[key] === undefined) {
      out[key] = i;
    }
  }

  return out;
}

/**
 * v2.5.1 — extraction order:
 *   1. exact apiKey
 *   2. snake_case → camelCase equivalent
 *   3. normalized (alphanumeric-only, lowercased) key comparison
 *   4. explicit alias table for the 5 multi-method view fields
 *      (_VIEW_FIELD_ALIASES_)
 *
 * v2.5.2 — extension:
 *   4b. explicit alias table for the 5 Wave 3 Insights fields
 *       (_INSIGHTS_FIELD_ALIASES_)
 *
 * v2.6.0 — extension:
 *   4c. explicit alias table for the 2 Decision Matrix fields
 *       (_DECISION_MATRIX_FIELD_ALIASES_)
 *   4d. explicit alias table for the 5 Candlestick fields
 *       (_CANDLESTICK_FIELD_ALIASES_)
 *
 * v2.6.1 — extension:
 *   4e. explicit alias table for the 2 Canonical Provenance
 *       fields from scoring.py v5.3.0
 *       (_CANONICAL_PROVENANCE_FIELD_ALIASES_)
 *
 * v2.7.0 — extension:
 *   4f. explicit alias table for the 7 SCORING_V574 fields from
 *       v5.74 backend cascade — provider_rating,
 *       scoring_recommendation_source, scoring_schema_version,
 *       scoring_errors, opportunity_source, overall_score_raw,
 *       overall_penalty_factor
 *       (_SCORING_V574_FIELD_ALIASES_)
 *
 * All paths are safety nets against future backend casing changes.
 * The current backend (v5.74.0 + scoring v5.4.2) uses snake_case so
 * Paths 1 or 3 will succeed for every canonical field. Paths 4 /
 * 4b / 4c / 4d / 4e / 4f exist to keep the writer correct if a
 * future backend revision switches casing conventions.
 *
 * Always returns the value as-is — null/undefined/0/false are NOT
 * coerced. The caller (writeRowsBySymbol_) decides what to do with
 * null/undefined via isEmptyValue_.
 */
function extractApiValue_(rowObject, apiKey) {
  if (!isPlainObject_(rowObject) || !apiKey) return null;

  // Path 1: exact apiKey
  if (Object.prototype.hasOwnProperty.call(rowObject, apiKey)) {
    return rowObject[apiKey];
  }

  // Path 2: snake_case → camelCase
  var camel = snakeToCamel_(apiKey);

  if (camel && Object.prototype.hasOwnProperty.call(rowObject, camel)) {
    return rowObject[camel];
  }

  // Path 3: normalized alphanumeric, lowercase
  var target = String(apiKey).toLowerCase().replace(/[^a-z0-9]/g, '');
  var keys = Object.keys(rowObject);

  for (var i = 0; i < keys.length; i++) {
    var normalized = String(keys[i]).toLowerCase().replace(/[^a-z0-9]/g, '');

    if (normalized === target) {
      return rowObject[keys[i]];
    }
  }

  // Path 4 (v2.5.1): multi-method view field alias table
  if (Object.prototype.hasOwnProperty.call(_VIEW_FIELD_ALIASES_, apiKey)) {
    var aliases = _VIEW_FIELD_ALIASES_[apiKey];

    for (var a = 0; a < aliases.length; a++) {
      var alias = aliases[a];

      if (Object.prototype.hasOwnProperty.call(rowObject, alias)) {
        return rowObject[alias];
      }
    }
  }

  // Path 4b (v2.5.2): Wave 3 Insights field alias table
  if (Object.prototype.hasOwnProperty.call(_INSIGHTS_FIELD_ALIASES_, apiKey)) {
    var insightsAliases = _INSIGHTS_FIELD_ALIASES_[apiKey];

    for (var ia = 0; ia < insightsAliases.length; ia++) {
      var iAlias = insightsAliases[ia];

      if (Object.prototype.hasOwnProperty.call(rowObject, iAlias)) {
        return rowObject[iAlias];
      }
    }
  }

  // Path 4c (v2.6.0): v2.8.0 Decision Matrix field alias table
  if (Object.prototype.hasOwnProperty.call(_DECISION_MATRIX_FIELD_ALIASES_, apiKey)) {
    var dmAliases = _DECISION_MATRIX_FIELD_ALIASES_[apiKey];

    for (var dma = 0; dma < dmAliases.length; dma++) {
      var dmAlias = dmAliases[dma];

      if (Object.prototype.hasOwnProperty.call(rowObject, dmAlias)) {
        return rowObject[dmAlias];
      }
    }
  }

  // Path 4d (v2.6.0): v2.8.0 Candlestick field alias table
  if (Object.prototype.hasOwnProperty.call(_CANDLESTICK_FIELD_ALIASES_, apiKey)) {
    var csAliases = _CANDLESTICK_FIELD_ALIASES_[apiKey];

    for (var csa = 0; csa < csAliases.length; csa++) {
      var csAlias = csAliases[csa];

      if (Object.prototype.hasOwnProperty.call(rowObject, csAlias)) {
        return rowObject[csAlias];
      }
    }
  }

  // Path 4e (v2.6.1): v5.3.0 Canonical Provenance field alias table
  if (Object.prototype.hasOwnProperty.call(_CANONICAL_PROVENANCE_FIELD_ALIASES_, apiKey)) {
    var cpAliases = _CANONICAL_PROVENANCE_FIELD_ALIASES_[apiKey];

    for (var cpa = 0; cpa < cpAliases.length; cpa++) {
      var cpAlias = cpAliases[cpa];

      if (Object.prototype.hasOwnProperty.call(rowObject, cpAlias)) {
        return rowObject[cpAlias];
      }
    }
  }

  // Path 4f (v2.7.0): v5.74 SCORING_V574 field alias table
  if (Object.prototype.hasOwnProperty.call(_SCORING_V574_FIELD_ALIASES_, apiKey)) {
    var svAliases = _SCORING_V574_FIELD_ALIASES_[apiKey];

    for (var sva = 0; sva < svAliases.length; sva++) {
      var svAlias = svAliases[sva];

      if (Object.prototype.hasOwnProperty.call(rowObject, svAlias)) {
        return rowObject[svAlias];
      }
    }
  }

  // Path 4g (v2.8.0): current investability-gate field alias table.
  if (Object.prototype.hasOwnProperty.call(_INVESTABILITY_GATE_FIELD_ALIASES_, apiKey)) {
    var gateAliases = _INVESTABILITY_GATE_FIELD_ALIASES_[apiKey];

    for (var ga = 0; ga < gateAliases.length; ga++) {
      var gateAlias = gateAliases[ga];

      if (Object.prototype.hasOwnProperty.call(rowObject, gateAlias)) {
        return rowObject[gateAlias];
      }
    }
  }

  return null;
}

function snakeToCamel_(text) {
  var s = String(text || '');
  if (s.indexOf('_') === -1) return '';

  return s.replace(/_([a-zA-Z0-9])/g, function (_, ch) {
    return String(ch).toUpperCase();
  });
}

function isEmptyValue_(value) {
  if (value === null || value === undefined) return true;

  if (typeof value === 'number') {
    return !isFinite(value);
  }

  if (typeof value === 'string') {
    var t = value.trim();
    if (t === '' || t === 'null' || t === 'undefined' || t === 'NaN') return true;
    return false;
  }

  if (Array.isArray(value)) {
    return value.length === 0;
  }

  return false;
}

function _coreRequireConfig13_(callerName) {
  if (typeof getHeaderToKey !== 'function') {
    throw new Error(
      callerName + ' requires 00_Config.gs v1.3.0+ (getHeaderToKey is missing). ' +
      'Deploy 00_Config.gs v1.3.0 before this file.'
    );
  }
}

/**
 * v2.5.4 helper — return true when the value is a known status
 * token AND the column is not a designated text column.
 *
 * Detection rules:
 *   - typeof value === 'string'
 *   - trimmed lowercase form is an exact key in _KNOWN_STATUS_TOKENS_
 *   - headerNorm is not in _STATUS_TEXT_HEADERS_
 *
 * Numeric, boolean, null/undefined, array, and object values are
 * never treated as status tokens. Empty / whitespace-only strings
 * are not tokens either.
 *
 * v2.6.0 NOTE: function body UNCHANGED. The vocabulary grew
 * (_KNOWN_STATUS_TOKENS_ has 9 new v5.60.0 era entries) but the
 * lookup mechanism is identical to v2.5.4.
 *
 * v2.6.1 NOTE: function body UNCHANGED. scoring.py v5.3.0 emits
 * 'provider_error:<class>' and 'engine_dropped_valuation' via
 * scoring_errors — both already covered by v2.6.0's vocabulary
 * ('provider_error' exact match catches the bare token; the
 * colon-prefixed form 'provider_error:NetworkError' doesn't match
 * any vocabulary key and is preserved verbatim in the warnings
 * array as intended).
 */
function _isStatusTokenValue_(value, headerNorm) {
  if (typeof value !== 'string') return false;

  var t = value.trim().toLowerCase();
  if (!t) return false;

  if (!_KNOWN_STATUS_TOKENS_[t]) return false;

  // The lookup uses the same normalized header form the writer
  // already computes for manualSet / onEmptySet membership.
  if (_STATUS_TEXT_HEADERS_[headerNorm]) return false;

  return true;
}

/**
 * v2.5.1 internal helper — count how many of the 5 critical view
 * fields are populated in a sample of row objects. Returns object
 * { total, populated, empty } where total = 5 * sampleSize.
 *
 * v2.5.3 EXTENSION: return shape now also includes a `byField`
 * property — an object keyed by field name with per-field
 * { total, populated } counts. Existing total/populated/empty
 * properties are byte-identical to v2.5.1. Callers that ignore
 * byField get v2.5.1 semantics.
 *
 * Used solely for VERDICT logging in writeRowsBySymbol_. Does NOT
 * affect what gets written to the sheet.
 *
 * v2.6.0: function body UNCHANGED.
 * v2.6.1: function body UNCHANGED.
 */
function _countPopulatedViewFields_(rowObjects, sampleSize) {
  // v2.5.3: build empty byField up front so the empty-input case
  // returns a consistently-shaped result (callers can safely look
  // up byField[fieldName] without checking presence).
  var fields = ['fundamental_view', 'technical_view', 'risk_view', 'value_view', 'upside_pct'];
  var byField = {};
  for (var bf = 0; bf < fields.length; bf++) {
    byField[fields[bf]] = { total: 0, populated: 0 };
  }

  if (!Array.isArray(rowObjects) || rowObjects.length === 0) {
    return { total: 0, populated: 0, empty: 0, byField: byField };
  }

  var n = Math.min(sampleSize || 5, rowObjects.length);
  var total = 0;
  var populated = 0;

  for (var i = 0; i < n; i++) {
    var row = rowObjects[i];

    if (!isPlainObject_(row)) continue;

    for (var f = 0; f < fields.length; f++) {
      total++;
      byField[fields[f]].total++;

      var v = extractApiValue_(row, fields[f]);

      if (!isEmptyValue_(v)) {
        populated++;
        byField[fields[f]].populated++;
      }
    }
  }

  return {
    total: total,
    populated: populated,
    empty: total - populated,
    byField: byField
  };
}

/**
 * v2.5.2 internal helper — count how many of the 5 Wave 3 Insights
 * fields are populated in a sample of row objects. Page-aware: rows
 * that contain NO Insights field (i.e. Layout A pages where the
 * payload doesn't carry these cols at all) are skipped, so total=0
 * for those callers and the metric can be omitted from the VERDICT
 * line. For canonical 90+/97-col pages, total = 5 * (rows with at
 * least one Insights field).
 *
 * v2.5.3 EXTENSION: byField extension same as
 * _countPopulatedViewFields_.
 *
 * Used solely for VERDICT logging in writeRowsBySymbol_. Does NOT
 * affect what gets written to the sheet.
 *
 * v2.6.0: function body UNCHANGED.
 * v2.6.1: function body UNCHANGED.
 */
function _countPopulatedInsightsFields_(rowObjects, sampleSize) {
  // v2.5.3: byField pre-initialized so empty-input case returns a
  // consistently-shaped result.
  var fields = [
    'sector_relative_score',
    'conviction_score',
    'top_factors',
    'top_risks',
    'position_size_hint'
  ];
  var byField = {};
  for (var bf = 0; bf < fields.length; bf++) {
    byField[fields[bf]] = { total: 0, populated: 0 };
  }

  if (!Array.isArray(rowObjects) || rowObjects.length === 0) {
    return { total: 0, populated: 0, empty: 0, byField: byField };
  }

  var n = Math.min(sampleSize || 5, rowObjects.length);
  var total = 0;
  var populated = 0;

  for (var i = 0; i < n; i++) {
    var row = rowObjects[i];

    if (!isPlainObject_(row)) continue;

    // Page-aware: skip the row if it carries NONE of the 5
    // Insights fields (Layout A / non-Wave-3 page). v2.5.3
    // preserves this — byField counts only accumulate for rows
    // with at least one Insights field, so Layout A pages still
    // produce byField totals of 0/0 across all 5 fields and the
    // VERDICT-line metric is suppressed by the existing
    // `if (insightsStats.total > 0)` guard in writeRowsBySymbol_.
    var hasAny = false;

    for (var p = 0; p < fields.length; p++) {
      if (extractApiValue_(row, fields[p]) !== null) {
        hasAny = true;
        break;
      }
    }

    if (!hasAny) continue;

    for (var f = 0; f < fields.length; f++) {
      total++;
      byField[fields[f]].total++;

      var v = extractApiValue_(row, fields[f]);

      if (!isEmptyValue_(v)) {
        populated++;
        byField[fields[f]].populated++;
      }
    }
  }

  return {
    total: total,
    populated: populated,
    empty: total - populated,
    byField: byField
  };
}

/**
 * v2.6.0 internal helper — count how many of the 2 v2.8.0
 * Decision Matrix fields are populated in a sample of row objects.
 *
 * Page-aware: rows that carry NEITHER Decision Matrix field are
 * skipped, so total=0 on Layout A pages where these cols aren't
 * in the payload. On canonical 97-col pages, total = 2 * (rows
 * with at least one Decision Matrix field).
 *
 * Returns { total, populated, empty, byField } with the same
 * shape as the v2.5.3 view + insights counters.
 *
 * Used solely for VERDICT logging in writeRowsBySymbol_. Does NOT
 * affect what gets written to the sheet.
 *
 * Provenance: data_engine_v2 v5.71.0 _classify_8tier() produces
 * recommendation_detailed (free-form 8-tier verdict) and
 * recommendation_priority (integer 1-8). Both fields are
 * deterministic given the upstream scoring inputs; partial
 * coverage in the sample indicates either a routing gap or a
 * stale row from before the engine bump.
 *
 * v2.6.1: function body UNCHANGED.
 */
function _countPopulatedDecisionMatrixFields_(rowObjects, sampleSize) {
  var fields = ['recommendation_detailed', 'recommendation_priority'];
  var byField = {};
  for (var bf = 0; bf < fields.length; bf++) {
    byField[fields[bf]] = { total: 0, populated: 0 };
  }

  if (!Array.isArray(rowObjects) || rowObjects.length === 0) {
    return { total: 0, populated: 0, empty: 0, byField: byField };
  }

  var n = Math.min(sampleSize || 5, rowObjects.length);
  var total = 0;
  var populated = 0;

  for (var i = 0; i < n; i++) {
    var row = rowObjects[i];

    if (!isPlainObject_(row)) continue;

    // Page-aware: skip rows that carry NEITHER Decision Matrix
    // field. Mirrors the _countPopulatedInsightsFields_ pattern
    // so Layout A pages produce total=0 and the VERDICT line
    // suppresses the metric.
    var hasAny = false;

    for (var p = 0; p < fields.length; p++) {
      if (extractApiValue_(row, fields[p]) !== null) {
        hasAny = true;
        break;
      }
    }

    if (!hasAny) continue;

    for (var f = 0; f < fields.length; f++) {
      total++;
      byField[fields[f]].total++;

      var v = extractApiValue_(row, fields[f]);

      if (!isEmptyValue_(v)) {
        populated++;
        byField[fields[f]].populated++;
      }
    }
  }

  return {
    total: total,
    populated: populated,
    empty: total - populated,
    byField: byField
  };
}

/**
 * v2.6.0 internal helper — count how many of the 5 v2.8.0
 * Candlestick fields are populated in a sample of row objects.
 *
 * Page-aware: rows that carry NONE of the 5 Candlestick fields
 * are skipped (Layout A pages). On canonical 97-col pages,
 * total = 5 * (rows with at least one Candlestick field).
 *
 * Returns { total, populated, empty, byField } with the same
 * shape as the v2.5.3 view + insights counters and the v2.6.0
 * Decision Matrix counter.
 *
 * Used solely for VERDICT logging in writeRowsBySymbol_. Does NOT
 * affect what gets written to the sheet.
 *
 * Provenance: core.candlesticks v1.0.0 produces these via
 * data_engine_v2 v5.71.0. Best-effort by design — when OHLC
 * history is insufficient or the signal is too weak to classify,
 * the engine returns null/N/A. A batch with populated < total is
 * NORMAL behavior for symbols with poor history coverage; it is
 * NOT necessarily a bug. The metric exists for visibility, not
 * for alarm.
 *
 * Operators interpret a partial-coverage Candlestick reading as
 * "engine had nothing to say for those symbols" rather than
 * "writer dropped fields", in contrast to the view + insights +
 * decision-matrix + canonical-provenance groups where partial
 * coverage typically indicates a propagation gap.
 *
 * v2.6.1: function body UNCHANGED.
 */
function _countPopulatedCandlestickFields_(rowObjects, sampleSize) {
  var fields = [
    'candlestick_pattern',
    'candlestick_signal',
    'candlestick_strength',
    'candlestick_confidence',
    'candlestick_patterns_recent'
  ];
  var byField = {};
  for (var bf = 0; bf < fields.length; bf++) {
    byField[fields[bf]] = { total: 0, populated: 0 };
  }

  if (!Array.isArray(rowObjects) || rowObjects.length === 0) {
    return { total: 0, populated: 0, empty: 0, byField: byField };
  }

  var n = Math.min(sampleSize || 5, rowObjects.length);
  var total = 0;
  var populated = 0;

  for (var i = 0; i < n; i++) {
    var row = rowObjects[i];

    if (!isPlainObject_(row)) continue;

    // Page-aware: skip rows that carry NONE of the 5 Candlestick
    // fields. Mirrors the v2.5.2 Insights pattern.
    var hasAny = false;

    for (var p = 0; p < fields.length; p++) {
      if (extractApiValue_(row, fields[p]) !== null) {
        hasAny = true;
        break;
      }
    }

    if (!hasAny) continue;

    for (var f = 0; f < fields.length; f++) {
      total++;
      byField[fields[f]].total++;

      var v = extractApiValue_(row, fields[f]);

      if (!isEmptyValue_(v)) {
        populated++;
        byField[fields[f]].populated++;
      }
    }
  }

  return {
    total: total,
    populated: populated,
    empty: total - populated,
    byField: byField
  };
}

/**
 * v2.6.1 internal helper — count how many of the 2 v5.3.0
 * Canonical Provenance fields are populated in a sample of row
 * objects.
 *
 * Page-aware: rows that carry NEITHER Canonical Provenance field
 * are skipped, so total=0 on Layout A pages where these cols
 * aren't in the payload. On canonical 99-col pages, total = 2 *
 * (rows with at least one Canonical Provenance field).
 *
 * Returns { total, populated, empty, byField } with the same
 * shape as the v2.5.3 view + insights counters and the v2.6.0
 * Decision Matrix + Candlestick counters.
 *
 * Used solely for VERDICT logging in writeRowsBySymbol_. Does NOT
 * affect what gets written to the sheet.
 *
 * Provenance: scoring.py v5.3.0 produces these via:
 *   - recommendation_priority_band: str P1..P5 from
 *     _compute_priority() — deterministic given upstream overall,
 *     risk, and conviction scores.
 *   - recommendation_source: constant string
 *     RECOMMENDATION_SOURCE_TAG = "scoring.py v5.3.0" written by
 *     apply_canonical_recommendation() on every row it processes.
 *
 * SEMANTICS — REAL FAILURES, NOT BEST-EFFORT
 * ──────────────────────────────────────────
 * Unlike Candlestick (where partial coverage is normal because
 * the engine returns null when OHLC history is insufficient),
 * Canonical Provenance blanks indicate REAL propagation gaps:
 *
 *   - A blank recommendation_source on a canonically-scored row
 *     means apply_canonical_recommendation() was never called for
 *     that row. Likely causes: the row bypassed scoring.py
 *     entirely (e.g. came from a stale cache), or the writer
 *     dropped the field, or HEADER_TO_KEY in Config v1.8.0
 *     doesn't map the column.
 *   - A blank recommendation_priority_band on a row that has a
 *     recommendation_source means apply_canonical_recommendation
 *     ran but _compute_priority returned None — investigate
 *     upstream score availability.
 *
 * Operators should treat canonical_provenance_blanks=[...] in the
 * VERDICT line as actionable, not informational. Run
 * debugV28SchemaPropagation_('<page>') from Schema v2.3.1 to
 * confirm whether the gap is at the schema-registry / Config
 * mapping level or further upstream.
 */
function _countPopulatedCanonicalProvenanceFields_(rowObjects, sampleSize) {
  var fields = ['recommendation_priority_band', 'recommendation_source'];
  var byField = {};
  for (var bf = 0; bf < fields.length; bf++) {
    byField[fields[bf]] = { total: 0, populated: 0 };
  }

  if (!Array.isArray(rowObjects) || rowObjects.length === 0) {
    return { total: 0, populated: 0, empty: 0, byField: byField };
  }

  var n = Math.min(sampleSize || 5, rowObjects.length);
  var total = 0;
  var populated = 0;

  for (var i = 0; i < n; i++) {
    var row = rowObjects[i];

    if (!isPlainObject_(row)) continue;

    // Page-aware: skip rows that carry NEITHER Canonical Provenance
    // field. Mirrors the _countPopulatedDecisionMatrixFields_
    // pattern so Layout A pages produce total=0 and the VERDICT
    // line suppresses the metric.
    var hasAny = false;

    for (var p = 0; p < fields.length; p++) {
      if (extractApiValue_(row, fields[p]) !== null) {
        hasAny = true;
        break;
      }
    }

    if (!hasAny) continue;

    for (var f = 0; f < fields.length; f++) {
      total++;
      byField[fields[f]].total++;

      var v = extractApiValue_(row, fields[f]);

      if (!isEmptyValue_(v)) {
        populated++;
        byField[fields[f]].populated++;
      }
    }
  }

  return {
    total: total,
    populated: populated,
    empty: total - populated,
    byField: byField
  };
}

/**
 * v2.7.0 internal helper — count how many of the 7 v5.74
 * SCORING_V574 fields are populated in a sample of row objects.
 *
 * Page-aware: rows that carry NONE of the 7 SCORING_V574 fields
 * are skipped, so total=0 on Layout A pages where these cols
 * aren't in the payload. On canonical 106-col pages,
 * total = 7 * (rows with at least one SCORING_V574 field).
 *
 * Returns { total, populated, empty, byField } with the same
 * shape as the v2.5.3 view + insights counters and the v2.6.x
 * Decision Matrix + Candlestick + Canonical Provenance counters.
 *
 * Used solely for VERDICT logging in writeRowsBySymbol_. Does NOT
 * affect what gets written to the sheet.
 *
 * Provenance: scoring.py v5.4.2 produces all 7 fields in the
 * v5.74 cascade. The fields are emitted in three distinct
 * "blank semantics" categories:
 *
 *   "Always populated when scoring ran" (5 fields):
 *     - scoring_recommendation_source — scoring.py provenance tag
 *     - scoring_schema_version        — constant per-deploy tag
 *     - opportunity_source            — enum, always set
 *     - overall_score_raw             — deterministic composite
 *     - overall_penalty_factor        — 1.0 if no penalty
 *
 *   "Best-effort, external-data dependent" (1 field):
 *     - provider_rating — null when no analyst coverage (legitimate
 *                         state, not a propagation gap; behaves
 *                         like Candlestick fields)
 *
 *   "INVERSE signal — empty is the success case" (1 field):
 *     - scoring_errors — empty means "no errors detected";
 *                        populated means errors were captured
 *
 * SEMANTICS — MIXED INTERPRETATION
 * ─────────────────────────────────
 * Unlike Canonical Provenance (where partial coverage is uniformly
 * a propagation gap signal) or Candlestick (where partial coverage
 * is uniformly best-effort), SCORING_V574 has fields in all three
 * categories. The blanks list is informational, not uniformly
 * actionable. Operators interpret each blanks entry by category:
 *
 *   - provider_rating in blanks list: NORMAL for under-covered
 *     symbols (small caps, illiquid names). Not a bug.
 *   - scoring_errors in blanks list: NORMAL on healthy batches.
 *     Empty = success. Expected most of the time.
 *   - Any of the 5 "always populated" fields in blanks list:
 *     REAL PROPAGATION GAP. Investigate via
 *     debugV28SchemaPropagation_('<page>') from Schema v2.4.0.
 *
 * The counter includes all 7 fields rather than excluding
 * provider_rating / scoring_errors because the operator-facing
 * value of seeing per-field detail outweighs the noise of
 * routinely-blank entries. Consistent with the v2.6.0 Candlestick
 * counter precedent: include all fields, document interpretation.
 *
 * NOTE on overall_penalty_factor: numeric 0 is NOT empty per
 * isEmptyValue_ (0 is finite). A value of 0.0 would count as
 * populated. This is correct — the engine never emits exactly
 * 0.0 for this field (range is 0.0 < x <= 1.0 in practice), but
 * if it did, that would still represent a real engine decision
 * rather than a propagation gap.
 */
function _countPopulatedScoringV574Fields_(rowObjects, sampleSize) {
  var fields = [
    'provider_rating',
    'scoring_recommendation_source',
    'scoring_schema_version',
    'scoring_errors',
    'opportunity_source',
    'overall_score_raw',
    'overall_penalty_factor'
  ];
  var byField = {};
  for (var bf = 0; bf < fields.length; bf++) {
    byField[fields[bf]] = { total: 0, populated: 0 };
  }

  if (!Array.isArray(rowObjects) || rowObjects.length === 0) {
    return { total: 0, populated: 0, empty: 0, byField: byField };
  }

  var n = Math.min(sampleSize || 5, rowObjects.length);
  var total = 0;
  var populated = 0;

  for (var i = 0; i < n; i++) {
    var row = rowObjects[i];

    if (!isPlainObject_(row)) continue;

    // Page-aware: skip rows that carry NONE of the 7 SCORING_V574
    // fields. Mirrors the prior page-aware counter pattern so
    // Layout A pages produce total=0 and the VERDICT line
    // suppresses the metric.
    var hasAny = false;

    for (var p = 0; p < fields.length; p++) {
      if (extractApiValue_(row, fields[p]) !== null) {
        hasAny = true;
        break;
      }
    }

    if (!hasAny) continue;

    for (var f = 0; f < fields.length; f++) {
      total++;
      byField[fields[f]].total++;

      var v = extractApiValue_(row, fields[f]);

      if (!isEmptyValue_(v)) {
        populated++;
        byField[fields[f]].populated++;
      }
    }
  }

  return {
    total: total,
    populated: populated,
    empty: total - populated,
    byField: byField
  };
}



/**
 * v2.8.0 internal helper — count how many of the 9 current
 * investability-gate fields are populated in a sample of row objects.
 *
 * Page-aware: rows that carry NONE of the gate fields are skipped, so
 * older/narrow payloads produce total=0 and the VERDICT segment is
 * suppressed.
 *
 * Mixed blank semantics:
 *   - provider_engine_conflict: false is populated and meaningful.
 *   - conflict_type: may be blank when there is no conflict.
 *   - block_reason: may be blank for valid INVEST rows.
 *   - the remaining fields should normally populate on investment
 *     pages; blanks are useful propagation diagnostics.
 *
 * This helper is log-only. It never changes what is written to the
 * sheet.
 */
function _countPopulatedInvestabilityGateFields_(rowObjects, sampleSize) {
  var fields = [
    'forecast_source',
    'data_quality_score',
    'forecast_reliability_score',
    'provider_engine_conflict',
    'conflict_type',
    'final_decision_basis',
    'investability_status',
    'final_action',
    'block_reason'
  ];
  var byField = {};
  for (var bf = 0; bf < fields.length; bf++) {
    byField[fields[bf]] = { total: 0, populated: 0 };
  }

  if (!Array.isArray(rowObjects) || rowObjects.length === 0) {
    return { total: 0, populated: 0, empty: 0, byField: byField };
  }

  var n = Math.min(sampleSize || 5, rowObjects.length);
  var total = 0;
  var populated = 0;

  for (var i = 0; i < n; i++) {
    var row = rowObjects[i];

    if (!isPlainObject_(row)) continue;

    var hasAny = false;
    for (var p = 0; p < fields.length; p++) {
      if (extractApiValue_(row, fields[p]) !== null) {
        hasAny = true;
        break;
      }
    }

    if (!hasAny) continue;

    for (var f = 0; f < fields.length; f++) {
      total++;
      byField[fields[f]].total++;

      var v = extractApiValue_(row, fields[f]);

      if (!isEmptyValue_(v)) {
        populated++;
        byField[fields[f]].populated++;
      }
    }
  }

  return {
    total: total,
    populated: populated,
    empty: total - populated,
    byField: byField
  };
}
/**
 * v2.5.1 — adds VERDICT logging without changing write behavior.
 * v2.5.4 — adds defensive status-bleed sanitization in the main
 *          per-row loop.
 * v2.6.0 — extends VERDICT line with decision_matrix_populated and
 *          candlestick_populated metrics (suppressed on Layout A
 *          pages where the corresponding column groups aren't
 *          carried in the payload).
 * v2.6.1 — extends VERDICT line with canonical_provenance_populated
 *          metric (suppressed on Layout A pages with the same
 *          total>0 rule as v2.6.0 metrics).
 * v2.7.0 — extends VERDICT line with scoring_v574_populated metric
 *          for the 7 v5.74 SCORING_V574 fields (suppressed on
 *          Layout A pages with the same total>0 rule).
 *
 * Behavior in v2.7.0 vs v2.6.1:
 *   - Wraps body in try/catch only to emit WRITER_THREW verdict
 *     before re-throwing. Caller still receives the original error.
 *   - Emits exactly one [v2.5.1 VERDICT] line per call summarizing
 *     which path was taken and what was written.
 *   - When the incoming value for a non-text column matches a
 *     known status token (see _KNOWN_STATUS_TOKENS_), the value
 *     is dropped from the target cell and routed to the row's
 *     Warnings column with a "status_bleed: {header}={token}"
 *     marker. The cell receives '' (blank).
 *   - 'Scoring Errors' is now in _STATUS_TEXT_HEADERS_ allowlist
 *     (v2.7.0) — single-token values in that column are NOT
 *     sanitized, preserving the column's role as a multi-token
 *     error log.
 *   - The VERDICT line gains:
 *       - status_bleed_sanitized=N when N > 0 (v2.5.4)
 *       - decision_matrix_populated=N/M when on a v2.8.0 canonical
 *         page (v2.6.0)
 *       - candlestick_populated=N/M when on a v2.8.0 canonical
 *         page (v2.6.0)
 *       - decision_matrix_blanks=[...] / candlestick_blanks=[...]
 *         when populated < total (v2.6.0)
 *       - canonical_provenance_populated=N/M when on a v5.3.0
 *         canonical page (v2.6.1)
 *       - canonical_provenance_blanks=[...] when populated < total
 *         (v2.6.1)
 *       - scoring_v574_populated=N/M when on a v5.74 canonical
 *         page (v2.7.0)
 *       - scoring_v574_blanks=[...] when populated < total (v2.7.0)
 *
 * The VERDICT tag REMAINS "[v2.5.1 VERDICT]" per project
 * convention (v2.5.2 / v2.5.3 / v2.5.4 / v2.6.0 / v2.6.1 / v2.7.0
 * precedent). 05_Refresh.gs v1.11.2, 07_Global_Markets.gs v1.6.7,
 * and 99_Diag.gs v2.3.0 cite this exact tag.
 *
 * Sheet writes for non-bleed cells are byte-identical to v2.5.4 /
 * v2.6.0 / v2.6.1. The new VERDICT segments are log-only and have
 * no effect on what gets written to the sheet.
 */
function _coreWriterSlimReadEnabled_() {
  /* v2.10.0: writer slim-read master switch. Default ON. OFF restores the
   * v2.9.1 full-block existing-row read exactly. */
  try {
    var v = PropertiesService.getScriptProperties().getProperty('TFB_CORE_WRITER_SLIM_READ');
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    if (['0', 'false', 'off', 'no', 'disabled', 'disable'].indexOf(s) >= 0) return false;
    return true;
  } catch (e) {
    return true;
  }
}

function _corePlaceGuardEnabled_() {
  /* v2.8.1: write-placement guard master switch. Default ON. */
  try {
    var v = PropertiesService.getScriptProperties().getProperty('TFB_CORE_PLACE_GUARD');
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    if (['0', 'false', 'off', 'no', 'disabled', 'disable'].indexOf(s) >= 0) return false;
    return true;
  } catch (e) {
    return true;
  }
}

function _coreAppendGuardEnabled_() {
  /* v2.9.0: append-placement guard master switch. Default ON.
   * The v2.8.1 place-guard protects UPDATE blocks; the APPEND path was
   * unguarded — a stale symbolToRow map (built before a concurrent
   * writer rewrote the table) routes an EXISTING symbol to append,
   * minting a duplicate physical row (live 2026-07-08: AAPL x2,
   * MET.US x2), and two writers appending against the same
   * getLastRow()+1 anchor overwrite each other's fresh rows — one
   * mechanism behind the Global_Markets symbol-loss ratchet.
   * Set Script Property TFB_CORE_APPEND_GUARD to 0/false/off/no/
   * disabled to restore the exact v2.8.1 blind-append behavior. */
  try {
    var v = PropertiesService.getScriptProperties().getProperty('TFB_CORE_APPEND_GUARD');
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    if (['0', 'false', 'off', 'no', 'disabled', 'disable'].indexOf(s) >= 0) return false;
    return true;
  } catch (e) {
    return true;
  }
}

function writeRowsBySymbol_(sheet, pageName, rowObjects, options) {
  var pageNameSafe = pageName || '<unknown>';
  var verdictBase = '[v2.5.1 VERDICT][' + pageNameSafe + ']';

  try {
    _coreRequireConfig13_('writeRowsBySymbol_');

    options = options || {};

    if (!sheet) throw new Error('writeRowsBySymbol_: sheet is required');

    if (!Array.isArray(rowObjects)) {
      throw new Error('writeRowsBySymbol_: rowObjects must be an array');
    }

    var result = {
      updated: 0,
      appended: 0,
      skipped: 0,
      unmappedHeaders: [],
      headersUsed: 0,
      // v2.5.4: count of cells where a status token was sanitized
      // and routed to the Warnings column.
      statusBleedSanitized: 0,
      // v2.8.1: rows NOT written because the live Symbol cell no longer
      // matched the intended symbol at flush time (concurrent shift).
      placementAborted: 0,
      // v2.9.0: appends withheld by the append-guard (anchor dirty after
      // retry, or per-row second-chance verify failed).
      appendAborted: 0,
      // v2.9.0: rows the stale map routed to APPEND that the fresh
      // pre-append re-read proved already exist — written in place
      // instead (duplicate averted).
      secondChanceUpdated: 0,
      // v2.9.0: symbols whose fresh data was withheld by either guard;
      // the caller re-queues them so the data is retried, not lost.
      deferredSymbols: []
    };

    if (rowObjects.length === 0) {
      try {
        Logger.log(verdictBase + ' WRITER_SKIPPED reason=empty_input rowObjects.length=0');
      } catch (e0) {}
      return result;
    }

    var preserveHeaderRows = options.preserveHeaderRows || 1;
    var symbolApiKey = options.symbolApiKey || 'symbol';
    var symbolHeaderName = options.symbolHeaderName || 'Symbol';
    var allowAppend = options.allowAppend !== false;

    var headerToKey = getHeaderToKey(pageName);

    var manualSet = {};
    var manualCols = options.preserveManualColumns ||
      (typeof getPreserveManualColumns === 'function' ? getPreserveManualColumns(pageName) : []);

    for (var mi = 0; mi < manualCols.length; mi++) {
      manualSet[_normalizeHeaderText_(manualCols[mi])] = true;
    }

    var onEmptySet = {};
    var onEmptyCols = options.preserveOnEmptyColumns ||
      (typeof getPreserveOnEmptyColumns === 'function' ? getPreserveOnEmptyColumns(pageName) : []);

    for (var oi = 0; oi < onEmptyCols.length; oi++) {
      onEmptySet[_normalizeHeaderText_(onEmptyCols[oi])] = true;
    }

    var headers = getSheetHeaders_(sheet);

    if (headers.length === 0) {
      throw new Error('Sheet "' + sheet.getName() + '" has no header row.');
    }

    result.headersUsed = headers.length;

    var resolved = headers.map(function (h) {
      var k = coreResolveHeaderKey_(h, headerToKey);

      if (!k && h) {
        result.unmappedHeaders.push(h);
      }

      return k;
    });

    var symbolColNumber = findColumnIndex_(headers, symbolHeaderName, { caseInsensitive: true });

    if (symbolColNumber === -1) {
      throw new Error('Sheet "' + sheet.getName() + '" has no "' + symbolHeaderName + '" header column.');
    }

    var symbolColIdx = symbolColNumber - 1;

    // v2.5.4: locate the Warnings column once per call. -1 if absent.
    var warningsColIdx = -1;
    for (var wcInit = 0; wcInit < headers.length; wcInit++) {
      if (_normalizeHeaderText_(headers[wcInit]) === 'Warnings') {
        warningsColIdx = wcInit;
        break;
      }
    }

    var dataStartRow = preserveHeaderRows + 1;
    var lastRow = sheet.getLastRow();
    var lastCol = headers.length;

    var existingBlock = [];
    var symbolToRow = {};

    /* v2.10.0 SLIM-READ: build the routing map from the symbol column only
     * (~1/115th of the old read); existing-row values are fetched lazily,
     * one single-row read per matched incoming symbol, cached per call.
     * Kill-switch OFF -> the v2.9.1 full-block read below runs verbatim. */
    var writerSlimRead = _coreWriterSlimReadEnabled_();
    var lazyRowCache = {};

    if (lastRow >= dataStartRow) {
      if (writerSlimRead) {
        var symColVals = sheet.getRange(dataStartRow, symbolColNumber, lastRow - dataStartRow + 1, 1).getValues();
        for (var sc = 0; sc < symColVals.length; sc++) {
          var scSym = String(symColVals[sc][0] || '').trim();
          if (scSym) {
            symbolToRow[scSym.toUpperCase()] = dataStartRow + sc;
          }
        }
      } else {
        existingBlock = sheet.getRange(dataStartRow, 1, lastRow - dataStartRow + 1, lastCol).getValues();

        for (var bi = 0; bi < existingBlock.length; bi++) {
          var existingSymbol = String(existingBlock[bi][symbolColIdx] || '').trim();
          if (existingSymbol) {
            symbolToRow[existingSymbol.toUpperCase()] = dataStartRow + bi;
          }
        }
      }
    }

    function _existingRowFor_(rowNum) {
      if (!rowNum) return null;
      if (!writerSlimRead) {
        var legIdx = rowNum - dataStartRow;
        return legIdx >= 0 && legIdx < existingBlock.length ? existingBlock[legIdx] : null;
      }
      if (Object.prototype.hasOwnProperty.call(lazyRowCache, rowNum)) {
        return lazyRowCache[rowNum];
      }
      var fetched = null;
      try {
        fetched = sheet.getRange(rowNum, 1, 1, lastCol).getValues()[0];
      } catch (lrErr) {
        fetched = null;
      }
      lazyRowCache[rowNum] = fetched;
      return fetched;
    }

    var inPlace = [];
    var appends = [];

    for (var rIdx = 0; rIdx < rowObjects.length; rIdx++) {
      var rowObj = rowObjects[rIdx];

      if (!isPlainObject_(rowObj)) {
        result.skipped++;
        continue;
      }

      var symbolRaw =
        extractApiValue_(rowObj, symbolApiKey) ||
        rowObj.Symbol ||
        rowObj.symbol ||
        '';

      var symbol = String(symbolRaw || '').trim();

      if (!symbol) {
        result.skipped++;
        continue;
      }

      var symbolLookup = symbol.toUpperCase();
      var existingRowNum = symbolToRow[symbolLookup];
      /* v2.10.0: unified sourcing — slim mode lazy-fetches + caches the row;
       * legacy mode indexes the full block exactly as v2.9.1 did. */
      var existingRow = existingRowNum ? _existingRowFor_(existingRowNum) : null;

      var values = new Array(lastCol);
      // v2.5.4: per-row accumulator for status tokens sanitized
      // out of value cells. Flushed to values[warningsColIdx] after
      // the column loop completes.
      var rowBleeds = [];

      for (var c = 0; c < lastCol; c++) {
        var header = headers[c];
        var headerNorm = _normalizeHeaderText_(header);

        if (manualSet[headerNorm]) {
          values[c] = existingRow ? existingRow[c] : '';
          continue;
        }

        var apiKey = resolved[c];

        if (!apiKey) {
          values[c] = existingRow ? existingRow[c] : '';
          continue;
        }

        var incoming = extractApiValue_(rowObj, apiKey);

        if (onEmptySet[headerNorm] && isEmptyValue_(incoming) && existingRow && !isEmptyValue_(existingRow[c])) {
          values[c] = existingRow[c];
          continue;
        }

        // v2.5.4: status-bleed sanitization. Detect and route
        // before normalizeCellValue_ stringifies the bleed into
        // the cell. Only triggers for known tokens in non-text
        // columns; legitimate text columns and unknown tokens
        // pass through unchanged.
        if (_isStatusTokenValue_(incoming, headerNorm)) {
          rowBleeds.push(header + '=' + String(incoming).trim().toLowerCase());
          values[c] = '';
          result.statusBleedSanitized++;
          continue;
        }

        values[c] = normalizeCellValue_(incoming);
      }

      // v2.5.4: append accumulated bleeds to the Warnings cell for
      // this row, preserving any warnings the writer just put there
      // (e.g. forecast_unavailable from row_object.warnings field
      // routed via the legitimate Warnings column path).
      if (rowBleeds.length > 0 && warningsColIdx >= 0) {
        var existingWarn = String(values[warningsColIdx] || '').trim();
        var bleedSegment = 'status_bleed: ' + rowBleeds.join(', ');

        values[warningsColIdx] = existingWarn
          ? existingWarn + ' | ' + bleedSegment
          : bleedSegment;
      }
      // If warningsColIdx < 0 (no Warnings column on the sheet),
      // bleeds are silently dropped from this row but still counted
      // in result.statusBleedSanitized so the VERDICT line surfaces
      // them. Pages without a Warnings column are degenerate under
      // the canonical schema; this branch is defensive.

      if (existingRowNum) {
        inPlace.push({
          rowNum: existingRowNum,
          symbol: symbolLookup,   // v2.8.1: carried for the placement guard
          values: values
        });
      } else if (allowAppend) {
        // v2.9.0: symbol carried so the append-guard can second-chance
        // and post-verify each row.
        appends.push({ symbol: symbolLookup, values: values });
      } else {
        result.skipped++;
      }
    }

    inPlace.sort(function (a, b) {
      return a.rowNum - b.rowNum;
    });

    var i2 = 0;

    while (i2 < inPlace.length) {
      var end = i2;

      while (end + 1 < inPlace.length && inPlace[end + 1].rowNum === inPlace[end].rowNum + 1) {
        end++;
      }

      var block = inPlace.slice(i2, end + 1).map(function (x) {
        return x.values;
      });

      /* v2.8.1 PLACE-GUARD: the map was built from an earlier read; verify
       * the block's target rows still carry the symbols this block intends
       * to write. A concurrent writer shifting rows in the read->write gap
       * otherwise lands this ENTIRE block under foreign symbols. */
      var placeOk = true;
      if (_corePlaceGuardEnabled_()) {
        try {
          var liveSyms = sheet.getRange(inPlace[i2].rowNum, symbolColNumber, block.length, 1).getValues();
          for (var pg = 0; pg < block.length; pg++) {
            var liveSym = String(liveSyms[pg][0] || '').trim().toUpperCase();
            var wantSym = String(inPlace[i2 + pg].symbol || '').trim().toUpperCase();
            if (liveSym !== wantSym) {
              placeOk = false;
              try {
                Logger.log('[v2.8.1 PLACE-GUARD][' + pageNameSafe + '] BLOCK ABORTED at row ' +
                  inPlace[i2].rowNum + ' (' + block.length + ' rows): live symbol "' + liveSym +
                  '" != intended "' + wantSym + '" at offset ' + pg +
                  ' — sheet shifted between read and write; block skipped.');
              } catch (pgLog) {}
              break;
            }
          }
        } catch (pgErr) {
          /* verification unavailable -> fail SAFE: skip the block rather
           * than risk a transposed write. */
          placeOk = false;
          try {
            Logger.log('[v2.8.1 PLACE-GUARD][' + pageNameSafe + '] verify read failed (' + pgErr + '); block at row ' +
              inPlace[i2].rowNum + ' skipped fail-safe.');
          } catch (pgLog2) {}
        }
      }

      if (placeOk) {
        sheet.getRange(inPlace[i2].rowNum, 1, block.length, lastCol).setValues(block);
      } else {
        result.placementAborted += block.length;
        /* v2.9.0: the fresh data is deferred, not lost — the caller
         * re-queues these symbols into its checkpoint. */
        for (var pgd = 0; pgd < block.length; pgd++) {
          result.deferredSymbols.push(inPlace[i2 + pgd].symbol);
        }
      }
      i2 = end + 1;
    }

    result.updated = inPlace.length - result.placementAborted;
    if (result.placementAborted > 0) {
      try {
        Logger.log('[v2.8.1 PLACE-GUARD][' + pageNameSafe + '] total rows withheld this call: ' +
          result.placementAborted + ' (re-run refresh; if persistent, a concurrent writer is active).');
      } catch (paLog) {}
    }

    if (appends.length > 0) {
      if (!_coreAppendGuardEnabled_()) {
        /* v2.9.0 guard OFF: exact v2.8.1 blind-append behavior. */
        var legacyVals = [];
        for (var la = 0; la < appends.length; la++) legacyVals.push(appends[la].values);
        var legacyStart = sheet.getLastRow() + 1;
        sheet.getRange(legacyStart, 1, legacyVals.length, lastCol).setValues(legacyVals);
        result.appended = legacyVals.length;
      } else {
        /* v2.9.0 APPEND-GUARD.
         * (1) FRESH RE-READ of the symbol column: the routing map above is
         *     stale by construction; a symbol it missed may exist NOW
         *     (concurrent full-table writer). Such rows go SECOND-CHANCE
         *     in-place (per-row live verify) instead of minting duplicates.
         * (2) ANCHOR VERIFY for true appends: the target rows under
         *     getLastRow()+1 must read BLANK symbols immediately before the
         *     write; one re-anchor retry, then fail-safe defer. Two writers
         *     appending against the same anchor can no longer erase each
         *     other's fresh rows. */
        var freshRowBySym = {};
        var lastRowNow = sheet.getLastRow();
        if (lastRowNow >= dataStartRow) {
          var freshSyms = sheet.getRange(dataStartRow, symbolColNumber, lastRowNow - dataStartRow + 1, 1).getValues();
          for (var fsI = 0; fsI < freshSyms.length; fsI++) {
            var fsV = String(freshSyms[fsI][0] || '').trim().toUpperCase();
            if (fsV) freshRowBySym[fsV] = dataStartRow + fsI; /* last-wins, mirrors the map */
          }
        }

        var trueAppends = [];
        for (var apI = 0; apI < appends.length; apI++) {
          var apEntry = appends[apI];
          var scRow = freshRowBySym[apEntry.symbol];
          if (scRow) {
            /* SECOND CHANCE: exists now — write in place with live verify. */
            var scOk = false;
            try {
              var scLive = String(sheet.getRange(scRow, symbolColNumber).getValue() || '').trim().toUpperCase();
              scOk = (scLive === apEntry.symbol);
            } catch (scErr) {
              scOk = false;
            }
            if (scOk) {
              sheet.getRange(scRow, 1, 1, lastCol).setValues([apEntry.values]);
              result.secondChanceUpdated++;
            } else {
              result.appendAborted++;
              result.deferredSymbols.push(apEntry.symbol);
            }
          } else {
            trueAppends.push(apEntry);
          }
        }

        if (trueAppends.length > 0) {
          var apVals = [];
          for (var tvI = 0; tvI < trueAppends.length; tvI++) apVals.push(trueAppends[tvI].values);

          var anchorOk = false;
          var startRow = 0;
          for (var anchorTry = 0; anchorTry < 2 && !anchorOk; anchorTry++) {
            startRow = sheet.getLastRow() + 1;
            var needMax = startRow + apVals.length - 1;
            if (sheet.getMaxRows() < needMax) {
              sheet.insertRowsAfter(sheet.getMaxRows(), needMax - sheet.getMaxRows());
            }
            var anchorSyms = sheet.getRange(startRow, symbolColNumber, apVals.length, 1).getValues();
            anchorOk = true;
            for (var anI = 0; anI < anchorSyms.length; anI++) {
              if (String(anchorSyms[anI][0] || '').trim() !== '') {
                anchorOk = false;
                break;
              }
            }
          }

          if (anchorOk) {
            sheet.getRange(startRow, 1, apVals.length, lastCol).setValues(apVals);
            result.appended = apVals.length;
            /* Post-write witness: the anchor rows must now carry exactly
             * the symbols we appended; a mismatch means a concurrent
             * writer landed in the same window — observable, not silent. */
            try {
              var postSyms = sheet.getRange(startRow, symbolColNumber, apVals.length, 1).getValues();
              var postBad = 0;
              for (var poI = 0; poI < postSyms.length; poI++) {
                if (String(postSyms[poI][0] || '').trim().toUpperCase() !== trueAppends[poI].symbol) postBad++;
              }
              if (postBad > 0) {
                Logger.log('[v2.9.0 APPEND-GUARD][' + pageNameSafe + '] POST-WRITE MISMATCH on ' + postBad +
                  ' of ' + apVals.length + ' appended row(s) at anchor ' + startRow +
                  ' — a concurrent writer overlapped the append window.');
              }
            } catch (poErr) {}
          } else {
            result.appendAborted += apVals.length;
            for (var dfI = 0; dfI < trueAppends.length; dfI++) {
              result.deferredSymbols.push(trueAppends[dfI].symbol);
            }
            try {
              Logger.log('[v2.9.0 APPEND-GUARD][' + pageNameSafe + '] append anchor dirty after retry — ' +
                apVals.length + ' row(s) deferred fail-safe (concurrent appender active).');
            } catch (aaLog) {}
          }
        }
      }

      if (result.appendAborted > 0 || result.secondChanceUpdated > 0) {
        try {
          Logger.log('[v2.9.0 APPEND-GUARD][' + pageNameSafe + '] second_chance=' + result.secondChanceUpdated +
            ' append_aborted=' + result.appendAborted +
            ' deferred=' + result.deferredSymbols.length);
        } catch (agLog) {}
      }
    }

    if (result.unmappedHeaders.length > 0) {
      var seen = {};
      var unique = [];

      for (var ui = 0; ui < result.unmappedHeaders.length; ui++) {
        var u = result.unmappedHeaders[ui];

        if (!seen[u]) {
          seen[u] = true;
          unique.push(u);
        }
      }

      result.unmappedHeaders = unique;

      if (_CORE_DEBUG_HEADER_MATCHING_) {
        try {
          var sample = unique.slice(0, 3).map(function (h) {
            var codes = [];

            for (var ci = 0; ci < h.length; ci++) {
              codes.push(h.charCodeAt(ci));
            }

            return '"' + h + '" charCodes=[' + codes.join(',') + ']';
          });

          Logger.log(
            'writeRowsBySymbol_ [' + pageNameSafe + '] ' +
            unique.length + ' unmapped header(s). Sample: ' + sample.join(' | ')
          );
        } catch (logErr) {}
      }
    }

    // v2.5.1: emit single VERDICT line summarizing what happened.
    // v2.5.2: conditionally append insights_populated=N/M for
    //         Wave 3 canonical pages (Layout A pages skip it).
    // v2.5.3: append per-field blank detail when populated < total.
    // v2.5.4: append status_bleed_sanitized=N when N > 0.
    // v2.6.0: append decision_matrix_populated=N/M and
    //         candlestick_populated=N/M for v2.8.0 canonical pages
    //         (Layout A pages skip both). Per-field blank detail
    //         follows the v2.5.3 pattern.
    // v2.6.1: append canonical_provenance_populated=N/M for v5.3.0
    //         canonical pages (Layout A pages skip it). Per-field
    //         blank detail follows the v2.5.3 / v2.6.0 pattern.
    // v2.7.0: append scoring_v574_populated=N/M for v5.74
    //         canonical pages (Layout A pages skip it). Per-field
    //         blank detail follows the v2.5.3 / v2.6.x pattern.
    try {
      var viewStats = _countPopulatedViewFields_(rowObjects, 5);
      var insightsStats = _countPopulatedInsightsFields_(rowObjects, 5);
      var decisionStats = _countPopulatedDecisionMatrixFields_(rowObjects, 5);
      var candleStats = _countPopulatedCandlestickFields_(rowObjects, 5);
      var provenanceStats = _countPopulatedCanonicalProvenanceFields_(rowObjects, 5);
      var scoringStats = _countPopulatedScoringV574Fields_(rowObjects, 5);
      var gateStats = _countPopulatedInvestabilityGateFields_(rowObjects, 5);
      var totalWritten = result.updated + result.appended;

      var verdict;
      var reason;

      if (totalWritten === 0) {
        verdict = 'WRITER_SKIPPED';
        reason = 'no_writes updated=0 appended=0 skipped=' + result.skipped;
      } else if (result.unmappedHeaders.length >= 5) {
        verdict = 'UNMAPPED';
        reason = 'unmapped_count=' + result.unmappedHeaders.length +
                 ' first=' + JSON.stringify(result.unmappedHeaders.slice(0, 3));
      } else if (viewStats.total > 0 && viewStats.populated === 0) {
        verdict = 'EMPTY_FIELDS';
        reason = 'all_5_view_fields_empty_in_first_' + (viewStats.total / 5) + '_rows ' +
                 'sample_keys=' + JSON.stringify(Object.keys(rowObjects[0] || {}).slice(0, 8));
      } else {
        verdict = 'OK';
        reason = 'updated=' + result.updated + ' appended=' + result.appended +
                 ' view_fields_populated=' + viewStats.populated + '/' + viewStats.total;

        // v2.5.3: when populated < total in OK case, append per-field
        // blank list so partial WRITER_DROPPED is visible. v1.9.1's
        // sampled-row VERIFY only reads the first row of each batch;
        // this catches "field N fails on a few rows but row 0 is
        // fine" cases. Only fields blank in at least one sampled
        // row appear in the list.
        if (viewStats.populated < viewStats.total && viewStats.byField) {
          var viewBlankList = [];
          var viewKeys = Object.keys(viewStats.byField);
          for (var vk = 0; vk < viewKeys.length; vk++) {
            var vfName = viewKeys[vk];
            var vfCounts = viewStats.byField[vfName];
            if (vfCounts && vfCounts.populated < vfCounts.total) {
              viewBlankList.push(
                vfName + '(' + vfCounts.populated + '/' + vfCounts.total + ')'
              );
            }
          }
          if (viewBlankList.length) {
            reason += ' view_blanks=[' + viewBlankList.join(',') + ']';
          }
        }

        // v2.5.2: append Insights metric only when the page actually
        // carries Insights cols. Layout A pages (Global_Markets,
        // Insights_Analysis at 46 cols) have insightsStats.total = 0
        // and the metric is suppressed to avoid confusing 0/0 lines.
        if (insightsStats.total > 0) {
          reason += ' insights_populated=' + insightsStats.populated +
                    '/' + insightsStats.total;

          // v2.5.3: same per-field blank detail for Insights when
          // partial blanks detected in the sample.
          if (insightsStats.populated < insightsStats.total && insightsStats.byField) {
            var insBlankList = [];
            var insKeys = Object.keys(insightsStats.byField);
            for (var ik = 0; ik < insKeys.length; ik++) {
              var ifName = insKeys[ik];
              var ifCounts = insightsStats.byField[ifName];
              // Only count fields that were actually sampled in
              // the rows that carry Insights — total>0 guard.
              if (ifCounts && ifCounts.total > 0 &&
                  ifCounts.populated < ifCounts.total) {
                insBlankList.push(
                  ifName + '(' + ifCounts.populated + '/' + ifCounts.total + ')'
                );
              }
            }
            if (insBlankList.length) {
              reason += ' insights_blanks=[' + insBlankList.join(',') + ']';
            }
          }
        }

        // v2.6.0: append Decision Matrix metric only when the page
        // carries the v2.8.0 Decision Matrix group. Layout A pages
        // produce decisionStats.total = 0; the metric is suppressed
        // to avoid noise on narrow layouts.
        if (decisionStats.total > 0) {
          reason += ' decision_matrix_populated=' + decisionStats.populated +
                    '/' + decisionStats.total;

          if (decisionStats.populated < decisionStats.total && decisionStats.byField) {
            var dmBlankList = [];
            var dmKeys = Object.keys(decisionStats.byField);
            for (var dk = 0; dk < dmKeys.length; dk++) {
              var dmName = dmKeys[dk];
              var dmCounts = decisionStats.byField[dmName];
              if (dmCounts && dmCounts.total > 0 &&
                  dmCounts.populated < dmCounts.total) {
                dmBlankList.push(
                  dmName + '(' + dmCounts.populated + '/' + dmCounts.total + ')'
                );
              }
            }
            if (dmBlankList.length) {
              reason += ' decision_matrix_blanks=[' + dmBlankList.join(',') + ']';
            }
          }
        }

        // v2.6.0: append Candlestick metric only when the page
        // carries the v2.8.0 Candlestick group. Layout A pages
        // produce candleStats.total = 0; the metric is suppressed.
        // NOTE: partial coverage is NORMAL for Candlestick (engine
        // returns null when OHLC history is insufficient or signal
        // too weak to classify). The blanks list is informational,
        // not a failure indicator.
        if (candleStats.total > 0) {
          reason += ' candlestick_populated=' + candleStats.populated +
                    '/' + candleStats.total;

          if (candleStats.populated < candleStats.total && candleStats.byField) {
            var csBlankList = [];
            var csKeys = Object.keys(candleStats.byField);
            for (var ck = 0; ck < csKeys.length; ck++) {
              var csName = csKeys[ck];
              var csCounts = candleStats.byField[csName];
              if (csCounts && csCounts.total > 0 &&
                  csCounts.populated < csCounts.total) {
                csBlankList.push(
                  csName + '(' + csCounts.populated + '/' + csCounts.total + ')'
                );
              }
            }
            if (csBlankList.length) {
              reason += ' candlestick_blanks=[' + csBlankList.join(',') + ']';
            }
          }
        }

        // v2.6.1: append Canonical Provenance metric only when the
        // page carries the v5.3.0 Canonical Provenance group.
        // Layout A pages produce provenanceStats.total = 0; the
        // metric is suppressed.
        // NOTE: unlike Candlestick, partial coverage for Canonical
        // Provenance is a REAL FAILURE indicator. Both fields are
        // deterministic (priority_band from _compute_priority,
        // source from RECOMMENDATION_SOURCE_TAG constant), so any
        // blank on a canonically-scored row means
        // apply_canonical_recommendation was never called for that
        // row — a real propagation gap to investigate via
        // debugV28SchemaPropagation_ from Schema v2.3.1.
        if (provenanceStats.total > 0) {
          reason += ' canonical_provenance_populated=' + provenanceStats.populated +
                    '/' + provenanceStats.total;

          if (provenanceStats.populated < provenanceStats.total && provenanceStats.byField) {
            var cpBlankList = [];
            var cpKeys = Object.keys(provenanceStats.byField);
            for (var pk = 0; pk < cpKeys.length; pk++) {
              var cpName = cpKeys[pk];
              var cpCounts = provenanceStats.byField[cpName];
              if (cpCounts && cpCounts.total > 0 &&
                  cpCounts.populated < cpCounts.total) {
                cpBlankList.push(
                  cpName + '(' + cpCounts.populated + '/' + cpCounts.total + ')'
                );
              }
            }
            if (cpBlankList.length) {
              reason += ' canonical_provenance_blanks=[' + cpBlankList.join(',') + ']';
            }
          }
        }

        // v2.7.0: append SCORING_V574 metric only when the page
        // carries the v5.74 SCORING_V574 group. Layout A pages
        // produce scoringStats.total = 0; the metric is suppressed.
        // NOTE on interpretation: SCORING_V574 has MIXED blank
        // semantics across its 7 fields. Operators interpret the
        // scoring_v574_blanks list by field:
        //   - provider_rating: best-effort, null when no analyst
        //     coverage. NORMAL for under-covered symbols.
        //   - scoring_errors: INVERSE signal — empty = success.
        //     NORMAL on healthy batches.
        //   - scoring_recommendation_source / scoring_schema_version
        //     / opportunity_source / overall_score_raw /
        //     overall_penalty_factor: REAL propagation gap if blank
        //     on canonically-scored rows. Investigate via
        //     debugV28SchemaPropagation_('<page>') from Schema v2.4.0.
        if (scoringStats.total > 0) {
          reason += ' scoring_v574_populated=' + scoringStats.populated +
                    '/' + scoringStats.total;

          if (scoringStats.populated < scoringStats.total && scoringStats.byField) {
            var svBlankList = [];
            var svKeys = Object.keys(scoringStats.byField);
            for (var sk = 0; sk < svKeys.length; sk++) {
              var svName = svKeys[sk];
              var svCounts = scoringStats.byField[svName];
              if (svCounts && svCounts.total > 0 &&
                  svCounts.populated < svCounts.total) {
                svBlankList.push(
                  svName + '(' + svCounts.populated + '/' + svCounts.total + ')'
                );
              }
            }
            if (svBlankList.length) {
              reason += ' scoring_v574_blanks=[' + svBlankList.join(',') + ']';
            }
          }
        }

        // v2.8.0: append investability-gate metric only when
        // the payload carries the current 115/118 gate group. Blank
        // semantics are mixed: Conflict Type and Block Reason can be
        // legitimately blank, while the core gate status/action fields
        // should usually populate. The metric is observability only.
        if (gateStats.total > 0) {
          reason += ' investability_gate_populated=' + gateStats.populated +
                    '/' + gateStats.total;

          if (gateStats.populated < gateStats.total && gateStats.byField) {
            var gateBlankList = [];
            var gateKeys = Object.keys(gateStats.byField);
            for (var gk = 0; gk < gateKeys.length; gk++) {
              var gateName = gateKeys[gk];
              var gateCounts = gateStats.byField[gateName];
              if (gateCounts && gateCounts.total > 0 &&
                  gateCounts.populated < gateCounts.total) {
                gateBlankList.push(
                  gateName + '(' + gateCounts.populated + '/' + gateCounts.total + ')'
                );
              }
            }
            if (gateBlankList.length) {
              reason += ' investability_gate_blanks=[' + gateBlankList.join(',') + ']';
            }
          }
        }

        // v2.5.4: append status_bleed_sanitized when the writer
        // routed any tokens away from numeric cells. Operators
        // can then check the affected rows' Warnings cells for
        // "status_bleed: {header}={token}" entries.
        if (result.statusBleedSanitized > 0) {
          reason += ' status_bleed_sanitized=' + result.statusBleedSanitized;
        }
      }

      Logger.log(verdictBase + ' ' + verdict + ' ' + reason);
    } catch (verdictErr) {
      try {
        Logger.log(verdictBase + ' VERDICT_LOG_FAILED ' + verdictErr.message);
      } catch (e2) {}
    }

    return result;

  } catch (writerErr) {
    try {
      Logger.log(verdictBase + ' WRITER_THREW ' +
        (writerErr && writerErr.message ? writerErr.message : String(writerErr)));
    } catch (e3) {}

    throw writerErr;
  }
}

/**
 * v2.5.4: writeSingleRowByHeaderMap_ now applies the same status-
 * bleed sanitization as writeRowsBySymbol_. When the incoming
 * value for a non-text column is a known status token, the value
 * is dropped, the cell is set to blank, and the token is appended
 * to the row's Warnings column with a "status_bleed: {header}={token}"
 * marker.
 *
 * Symmetric with writeRowsBySymbol_ — the single-row path used by
 * refreshSelectedRow_ and the v1.6.4 view-token override gets the
 * same defense.
 *
 * v2.6.0: function body UNCHANGED. The v2.6.0 vocabulary extension
 * (engine drop reasons + intrinsic-unit detection markers) and the
 * v2.8.0 alias paths in extractApiValue_ both flow through
 * automatically because this function consults the same helpers
 * (extractApiValue_ + _isStatusTokenValue_) as the multi-row writer.
 *
 * v2.6.1: function body UNCHANGED. The new Path 4e canonical-
 * provenance alias resolution flows through automatically via
 * extractApiValue_, so single-row refresh of canonical pages gains
 * correct extraction of recommendation_priority_band and
 * recommendation_source without code change here. Single-row
 * refreshes don't emit VERDICT lines (no batch context), so the
 * new counter helper is not invoked from this path.
 *
 * v2.7.0: function body UNCHANGED. The new Path 4f SCORING_V574
 * alias resolution flows through automatically via extractApiValue_,
 * so single-row refresh of canonical pages gains correct extraction
 * of all 7 v5.74 fields (provider_rating,
 * scoring_recommendation_source, scoring_schema_version,
 * scoring_errors, opportunity_source, overall_score_raw,
 * overall_penalty_factor) without code change here. The new
 * 'Scoring Errors' entry in _STATUS_TEXT_HEADERS_ also takes
 * effect on this path symmetrically, preserving multi-token
 * error log values in single-row refreshes.
 */
function writeSingleRowByHeaderMap_(sheet, rowNum, rowObject, pageName, options) {
  _coreRequireConfig13_('writeSingleRowByHeaderMap_');

  options = options || {};

  if (!sheet) throw new Error('writeSingleRowByHeaderMap_: sheet is required');
  if (!isPlainObject_(rowObject)) return false;

  var preserveHeaderRows = options.preserveHeaderRows || 1;

  if (!rowNum || rowNum <= preserveHeaderRows) {
    throw new Error('writeSingleRowByHeaderMap_: rowNum must be > ' + preserveHeaderRows);
  }

  var headers = getSheetHeaders_(sheet);
  if (headers.length === 0) return false;

  var headerToKey = getHeaderToKey(pageName);

  var manualSet = {};
  var manualCols = options.preserveManualColumns ||
    (typeof getPreserveManualColumns === 'function' ? getPreserveManualColumns(pageName) : []);

  for (var mi = 0; mi < manualCols.length; mi++) {
    manualSet[_normalizeHeaderText_(manualCols[mi])] = true;
  }

  var onEmptySet = {};
  var onEmptyCols = options.preserveOnEmptyColumns ||
    (typeof getPreserveOnEmptyColumns === 'function' ? getPreserveOnEmptyColumns(pageName) : []);

  for (var oi = 0; oi < onEmptyCols.length; oi++) {
    onEmptySet[_normalizeHeaderText_(onEmptyCols[oi])] = true;
  }

  var lastCol = headers.length;
  var existingValues = sheet.getRange(rowNum, 1, 1, lastCol).getValues()[0];
  var values = new Array(lastCol);

  // v2.5.4: locate the Warnings column once. -1 if absent.
  var warningsColIdx = -1;
  for (var wcInit = 0; wcInit < headers.length; wcInit++) {
    if (_normalizeHeaderText_(headers[wcInit]) === 'Warnings') {
      warningsColIdx = wcInit;
      break;
    }
  }

  // v2.5.4: per-row accumulator for sanitized tokens.
  var rowBleeds = [];

  for (var c = 0; c < lastCol; c++) {
    var header = headers[c];
    var headerNorm = _normalizeHeaderText_(header);

    if (manualSet[headerNorm]) {
      values[c] = existingValues[c];
      continue;
    }

    var apiKey = coreResolveHeaderKey_(header, headerToKey);

    if (!apiKey) {
      values[c] = existingValues[c];
      continue;
    }

    var incoming = extractApiValue_(rowObject, apiKey);

    if (onEmptySet[headerNorm] && isEmptyValue_(incoming) && !isEmptyValue_(existingValues[c])) {
      values[c] = existingValues[c];
      continue;
    }

    // v2.5.4: status-bleed sanitization (symmetric with
    // writeRowsBySymbol_).
    if (_isStatusTokenValue_(incoming, headerNorm)) {
      rowBleeds.push(header + '=' + String(incoming).trim().toLowerCase());
      values[c] = '';
      continue;
    }

    values[c] = normalizeCellValue_(incoming);
  }

  // v2.5.4: append accumulated bleeds to the Warnings cell.
  if (rowBleeds.length > 0 && warningsColIdx >= 0) {
    var existingWarn = String(values[warningsColIdx] || '').trim();
    var bleedSegment = 'status_bleed: ' + rowBleeds.join(', ');

    values[warningsColIdx] = existingWarn
      ? existingWarn + ' | ' + bleedSegment
      : bleedSegment;
  }

  sheet.getRange(rowNum, 1, 1, lastCol).setValues([values]);
  return true;
}

/* =========================================================
 * Utility helpers
 * ========================================================= */

function buildQueryString_(params) {
  if (typeof params !== 'object' || !params) return '';

  return Object.keys(params)
    .filter(function (key) {
      var v = params[key];
      return v !== null && v !== undefined && v !== '';
    })
    .map(function (key) {
      var value = params[key];
      var valStr = value;

      if (Array.isArray(value)) {
        valStr = value.join(',');
      } else if (typeof value === 'boolean') {
        valStr = String(value);
      }

      return encodeURIComponent(key) + '=' + encodeURIComponent(valStr);
    })
    .join('&');
}

function buildRequestId_() {
  return 'gs-' + Utilities.getUuid().slice(0, 12);
}

function deriveApiMessage_(statusCode, json, rawText) {
  if (isPlainObject_(json)) {
    if (json.message) return String(json.message);
    if (json.error) return String(json.error);
    if (json.status) return 'status=' + json.status;
  }

  if (statusCode >= 200 && statusCode < 300) {
    return 'OK';
  }

  return truncateText_(String(rawText || ('HTTP ' + statusCode)), 500);
}

function tryExtractResponseCode_(err) {
  try {
    var msg = String((err && err.message) || err);
    var match = msg.match(/\b(\d{3})\b/);

    if (match) {
      return parseInt(match[1], 10);
    }
  } catch (e) {}

  return null;
}

function isRetryableStatus_(statusCode) {
  if (statusCode === null || statusCode === undefined) return true;
  return statusCode === 408 || statusCode === 429 || (statusCode >= 500 && statusCode < 600);
}

function isPlainObject_(value) {
  return !!value && Object.prototype.toString.call(value) === '[object Object]';
}

function safeStringify_(value) {
  try {
    return JSON.stringify(value);
  } catch (e) {
    return String(value);
  }
}

function truncateText_(text, maxLen) {
  var str = String(text || '');
  return str.length <= maxLen ? str : str.slice(0, maxLen) + '...';
}

function normalizeCellValue_(value) {
  if (value === null || value === undefined) return '';
  if (Array.isArray(value)) return value.join(', ');
  if (isPlainObject_(value)) return safeStringify_(value);
  return value;
}

function toBooleanSafe_(value) {
  if (typeof value === 'boolean') return value;

  var s = String(value || '').trim().toLowerCase();

  return ['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) !== -1;
}

function toNumberSafe_(value, fallback) {
  if (fallback === undefined) fallback = 0;

  var n = Number(value);

  return isNaN(n) || !isFinite(n) ? fallback : n;
}


function verifyCoreVersion_() {
  /* v2.9.1: the expected version now MOVES with CORE_SCRIPT_VERSION_
   * bumps (the v2.8.0 body hard-coded '2.8.0' in both the constant
   * and the check, so it silently passed through v2.8.1 and v2.9.0
   * while verifying a version the file no longer was). The v2.8.0
   * gate checks are kept verbatim; the concurrency-guard layer the
   * stale verify never covered is added. */
  var result = {
    ok: true,
    version: CORE_SCRIPT_VERSION_,
    expected: '2.11.1',
    checks: []
  };

  function check(name, ok) {
    result.checks.push({ name: name, ok: !!ok });
    if (!ok) result.ok = false;
  }

  check('CORE_SCRIPT_VERSION_ matches expected', CORE_SCRIPT_VERSION_ === result.expected);
  /* v2.11.1 MANUAL-PRIORITY + RE-ENTRANT LOCK assertions */
  check('v2.11.1: manual-priority state property is versioned',
    CORE_MANUAL_PRIORITY_STATE_PROP_ === 'TFB_MANUAL_REFRESH_PRIORITY_STATE_V1');
  check('v2.11.1: manual wrapper exists', typeof withManualRefreshPriority_ === 'function');
  check('v2.11.1: automatic yield point exists', typeof tfbAutomaticYieldPoint_ === 'function');
  check('v2.11.1: lockless automatic wrapper exists',
    typeof withAutomaticRefreshPriorityLock_ === 'function');
  check('v2.11.1: manual wrapper announces intent before DocumentLock polling',
    String(withManualRefreshPriority_).indexOf('requestManualRefreshPriority_') >= 0 &&
    String(withManualRefreshPriority_).indexOf('lock.tryLock') >= 0);
  check('v2.11.1: manual wrapper has guaranteed cleanup',
    String(withManualRefreshPriority_).indexOf('clearManualRefreshPriority_') >= 0 &&
    String(withManualRefreshPriority_).indexOf('SpreadsheetApp.flush') >= 0);
  check('v2.11.1: document lock depth state exists',
    typeof CORE_DOCUMENT_LOCK_DEPTH_ === 'number');
  check('v2.11.1: withScriptLock is re-entrant',
    String(withScriptLock_).indexOf('CORE_DOCUMENT_LOCK_DEPTH_ > 0') >= 0);
  check('v2.11.1: manual wrapper enters shared lock scope',
    String(withManualRefreshPriority_).indexOf('CORE_DOCUMENT_LOCK_DEPTH_ = 1') >= 0);
  /* v2.10.0 WRITER SLIM-READ assertions */
  check('v2.10.0: _coreWriterSlimReadEnabled_ helper exists',
    typeof _coreWriterSlimReadEnabled_ === 'function');
  check('v2.10.0: writer builds map from symbol column (slim path present)',
    String(writeRowsBySymbol_).indexOf('symColVals') >= 0);
  check('v2.10.0: writer retains verbatim legacy full-block path (kill-switch)',
    String(writeRowsBySymbol_).indexOf('existingBlock = sheet.getRange(dataStartRow, 1, lastRow - dataStartRow + 1, lastCol)') >= 0);
  check('v2.10.0: lazy existing-row fetch is cached',
    String(writeRowsBySymbol_).indexOf('lazyRowCache') >= 0);
  check('Investability gate alias table exists', typeof _INVESTABILITY_GATE_FIELD_ALIASES_ === 'object');
  check('forecast_source alias exists', !!_INVESTABILITY_GATE_FIELD_ALIASES_['forecast_source']);
  check('provider_engine_conflict alias exists', !!_INVESTABILITY_GATE_FIELD_ALIASES_['provider_engine_conflict']);
  check('block_reason alias exists', !!_INVESTABILITY_GATE_FIELD_ALIASES_['block_reason']);
  check('Block Reason is status-text allowlisted', _STATUS_TEXT_HEADERS_['Block Reason'] === true);
  check('Final Decision Basis is status-text allowlisted', _STATUS_TEXT_HEADERS_['Final Decision Basis'] === true);
  check('Gate counter helper exists', typeof _countPopulatedInvestabilityGateFields_ === 'function');
  /* v2.9.1: concurrency-guard coverage (v2.8.1 + v2.9.0 layers). */
  check('Place-guard toggle exists (v2.8.1)', typeof _corePlaceGuardEnabled_ === 'function');
  check('Append-guard toggle exists (v2.9.0)', typeof _coreAppendGuardEnabled_ === 'function');
  check('Header-mapped batch writer exists', typeof writeRowsBySymbol_ === 'function');
  check('Header-mapped single-row writer exists', typeof writeSingleRowByHeaderMap_ === 'function');

  Logger.log('[v2.11.1 CORE VERIFY] ok=' + result.ok + ' checks=' + result.checks.length);
  return result;
}

function verifyCoreVersion() {
  return verifyCoreVersion_();
}

function todayRiyadhString_() {
  return Utilities.formatDate(new Date(), APP_INFO.TIMEZONE, 'yyyy-MM-dd HH:mm:ss');
}
