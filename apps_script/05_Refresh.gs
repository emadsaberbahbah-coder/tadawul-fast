/**
 * 05_Refresh.gs
 * Tadawul Tracker — Refresh, Build, Validation, Sorting, Batch Refresh
 * Version: 1.20.0
 *
 * v1.20.0 (2026-11-25) - FAILED-BATCH LEDGER, ONE BOUNDED RETRY, PARTIAL STATUS
 * --------------------------------------------------------------------------
 * WHY (week plan, package 3, refresh reliability): a market page refreshes in
 * chunks of symbols. When a chunk's fetch failed (backend error, timeout, an
 * error thrown while writing), its symbols were counted once in failedBatches,
 * the checkpoint moved past them and nothing ever came back for them in that
 * run; the page then closed as WARN with 'completed with N failed batch(es)'
 * and no record of WHICH symbols kept yesterday's row. A pause (timeout,
 * manual priority, tripwire) lost even that count, and a resumed run could
 * not know the chunk had failed.
 * WHAT:
 *  (1) LEDGER (always on, observe): every symbol of a failed chunk is kept in
 *      checkpoint.failed {SYMBOL: attempts} - saved with the checkpoint, so it
 *      survives a pause and a resume. A symbol whose chunk later succeeds is
 *      taken off it. The run's log carries the count and the warnings name the
 *      symbols still on it.
 *  (2) ONE BOUNDED RETRY (Script Property REFRESH_FAILED_RETRY = on): each
 *      failed symbol is put once more at the tail of the run (checkpoint
 *      .retried keeps it to one retry), at most REFRESH_FAILED_RETRY_MAX_
 *      symbols a run. The retry goes through the same fetch / guard / write
 *      path as any chunk.
 *  (3) PARTIAL STATUS (same property): a run that ends with symbols still on
 *      the ledger closes as PARTIAL, naming them, instead of WARN or SUCCESS;
 *      a failure that the retry recovered no longer turns the run WARN. Their
 *      rows are never written, so the previous (last-good) values stay: the
 *      upsert never deletes.
 *  (4) A checkpoint now records baseCount, the length of the requested list;
 *      isCheckpointCompatible_ compares that prefix, so a run paused after a
 *      retry or a v1.13.0 requeue resumes where it stopped instead of starting
 *      the page again, and a chunk stops at baseCount, so a first try never
 *      shares a request with a symbol that already failed. A checkpoint
 *      without baseCount is compared and cut as before.
 * REFRESH_FAILED_RETRY unset / off / anything but on = observe: no retry, the
 * status as in 1.19.0, one '[v1.20.0 FAILED-LEDGER observe]' warning line
 * naming what the property would retry. Rollback: leave the property unset.
 * ES5. ZERO functions removed; FOUR functions added (_refreshFailedRetryMode_,
 * _refreshNoteFailedChunk_, _refreshClearFailedChunk_, _refreshFailedSymbols_).
 * --------------------------------------------------------------------------
 * v1.18.1 — COCKPIT GUARD FAILS CLOSED + RUNTIME VERSION CONSTANT
 * --------------------------------------------------------------------------
 * (2026-08-11 suite review.)
 * (1) FAIL-CLOSED GUARD. _refreshIsDecisionOwnedPage_ delegated to
 *     00_Config v1.12.6 isDecisionOwnedPage_ and returned FALSE when that
 *     authority was absent or threw — i.e., with 00_Config missing, every
 *     legacy writer (refresh / trim / sort / header repair, "the single
 *     most destructive legacy action") would have written straight over
 *     the Top_10 cockpit. The identity-war doctrine of this project is
 *     that a mandatory guard NEVER fails open; the fallback now protects
 *     'Top_10_Investments' by local case-insensitive constant, keeping
 *     the interlock alive independently of any other file. Zero behavior
 *     change while 00_Config is present (the live state) — the delegate
 *     path is byte-identical.
 * (2) var REFRESH_VERSION = '1.18.1' — the suite-standard runtime
 *     constant; this was one of two files with none. The per-feature log
 *     tags ([v1.14.0 KEEP_LAST_GOOD], [v1.12.1 MP_ECONOMICS], ...) are
 *     DELIBERATELY untouched: they stamp each subsystem's introduction
 *     version by convention, which is coherent and stays.
 * ES5. ZERO functions removed; ONE constant added; ONE fallback line
 * changed inside _refreshIsDecisionOwnedPage_.
 * --------------------------------------------------------------------------
 * v1.18.0 — MANUAL REFRESH PRIORITY + SAFE COOPERATIVE YIELD
 * --------------------------------------------------------------------------
 * OPERATOR ISSUE: a long automatic batch can hold the shared DocumentLock for
 * several minutes. A menu-triggered manual refresh announces intent in
 * 02_Core.gs v2.11.0, but v1.17.0 had no in-loop checkpoint where the active
 * automatic execution could see that request and return cleanly. The manual
 * caller could therefore time out even though the automatic run was healthy.
 *
 * FIX (coordination only; no data, scoring, recommendation or writer rule is
 * changed):
 *   1. NEW _refreshManualPriorityProbe_ reads the Core coordinator state and
 *      yields ONLY while phase='requested'. phase='active' belongs to the
 *      manual owner after it acquires the lock, so a manual refresh can never
 *      stop itself.
 *   2. refreshPageInBatchesCore_ checks at three safe boundaries:
 *        - immediately after lock acquisition and before any page mutation;
 *        - before each new chunk;
 *        - immediately AFTER advancing and saving the checkpoint for a
 *          completed/failed chunk.
 *      The automatic run never yields after clearing a range and before its
 *      corresponding write. The saved checkpoint remains authoritative.
 *      If the last chunk just finished, final dedupe/format/validation completes
 *      before releasing the lock; a logically complete checkpoint is never
 *      stranded without its finalization pass.
 *   3. _refreshPauseForManualPriority_ flushes completed Sheet writes, writes
 *      an honest PARTIAL status only when a chunk actually completed, logs an
 *      INFO event, and returns action='paused-manual-priority' with
 *      reason='manual_refresh_requested'. 13_AutoRefresh.gs v1.10.0 treats
 *      this as a healthy deferral: no fail counter, no checkpoint deletion,
 *      no trigger recreation.
 *   4. refreshAllPages_ and refreshAnalysisAndInsights_ gain between-page
 *      cooperative checks. Under the aligned 01_Menu coordinator these
 *      functions run with phase='active' and continue normally; the check only
 *      protects an older/uncoordinated long execution when a newer manual
 *      request is waiting.
 *   5. Direct/manual single-page logic is unchanged. The normal five-minute
 *      automatic trigger remains installed and resumes the saved checkpoint
 *      after the manual owner clears its TTL-backed priority state.
 *
 * ALIGNMENT: 02_Core.gs v2.11.0 + 13_AutoRefresh.gs v1.10.0. The matching
 * 01_Menu.gs revision supplies the manual wrapper. All v1.17.0 HTTP honesty,
 * v1.16.0 slim reads, v1.15.0 identity firewall, v1.14.0 keep-last-good,
 * v1.13.0 tripwire/lossless deferral and prior safeguards are preserved.
 *
 * v1.17.0 (B-5) NON-2xx NEVER LOGS AS OK
 * OPERATOR EVIDENCE (_Run_Log 2026-07-27 05:05:55): "refreshPage |
 * Insights_Analysis | OK | Page refreshed successfully. |
 * /v1/advanced/sheet-rows | 401" — an AUTH FAILURE recorded at level INFO
 * with status OK and a success message. The 09:10 run then succeeded with
 * 200, so the page self-healed and nobody looked; but a guard-health gate
 * that reads status can never see a 401 that files itself as OK, and the
 * morning audit had to spot it by eye. Cause: every logRun_/writePageStatus_
 * site derives status from the VALIDATION result only, and validation runs
 * on whatever body came back — a non-2xx with a parseable body validates
 * clean.
 * FIX: rfHttpOk_(code) + rfStatusForHttp_/rfLevelForHttp_ downgrade the
 * status to WARN and the level to WARN whenever a REAL http code is present
 * and outside 200-299. A blank/absent code (the many internal call sites)
 * is untouched, so only the four fetch-backed sites can change, and only
 * when the transport actually failed. Messages gain "(http NNN)" so the
 * line is self-explaining.
 * NOT TOUCHED: retry/backoff, refresh flow, batching, validation logic,
 * writers, or any success path — a 2xx run is byte-identical.
 * KILL-SWITCH: Script Property TT_HTTP_STATUS_HONESTY = 'off' restores
 * v1.16.0 exactly. Default ON (protective guard doctrine). ES5.

 * v1.16.0 (2026-07-16) — VERIFY SLIM-READ + BATCH-SIZE PROPERTY
 * (universe-expansion quota fix, twin of 02_Core v2.10.0)
 * --------------------------------------------------------------------
 * WHY (1/2): _verifyCriticalFieldsAfterWrite_ read the ENTIRE data grid
 * (lastRow x lastCol getValues) once PER CHUNK — only to locate ONE
 * sampled row (the chunk's first symbol) and inspect ~a dozen cells on
 * it. At the 2026-07 expanded Global_Markets size (6,512 x 115 ~= 749k
 * cells) that read alone dominated each chunk's runtime. FIX: locate
 * the row via a SYMBOL-COLUMN-ONLY read, then read that single row —
 * ~6.6k cells instead of ~749k, identical semantics, identical
 * diagnosis strings. KILL-SWITCH: Script Property
 * TFB_REFRESH_VERIFY_SLIM_READ = 0/false/off/no restores the v1.15.0
 * full-grid read verbatim.
 * WHY (2/2): REFRESH_BATCH_SIZE_DEFAULT_ (10) was compile-time only.
 * At 6,511 GM symbols that is 652 chunks per full manual refresh; the
 * server-side sync has run batch=25 safely for weeks. NEW Script
 * Property TFB_REFRESH_BATCH_SIZE (clamped 1..50, default 10 — NO
 * behavior change until set) now feeds both the batch planner and the
 * auto-batch threshold, so the two can never disagree. Setting 25
 * roughly halves the chunk count and the trigger fires needed.
 * All v1.15.0 mechanics (ID-FIREWALL KLG gate, tripwire, checkpoints,
 * lossless deferral) carried verbatim.
 *
 * v1.15.0 (2026-07-15) — ID-FIREWALL: KLG IDENTITY GATE (GAS twin of
 * run_dashboard_sync v6.24.0 FW-1)
 * --------------------------------------------------------------------
 * EVIDENCE (v32 export forensics, 2026-07-14 audit): Market_Leaders rows
 * stamped 21:07-21:23 on 2026-07-13 still carried poisoned identities
 * (2010.SR named "NiSource Inc.", 1140.SR named "Oracle Corporation",
 * DD.US and HSBA.L both named "Microsoft Corporation") AFTER the engine
 * guards were armed. MECHANISM: the armed backend guards correctly
 * DISCARD poisoned enrichment, so the incoming row arrives as an
 * empty-partial stub; v1.14.0 KEEP-LAST-GOOD then withholds the stub
 * (or, on the positional path, substitutes the existing sheet row) —
 * and the existing row IS the poisoned one. The guard and the cache
 * cooperate to keep the damage alive; the page can never self-heal.
 *
 * FIX — the keep decision now runs an IDENTITY test on the EXISTING row
 * before it may be protected. An existing row is IDENTITY-SUSPECT when:
 *   (a) its Name is blank while it carries a price (the NMM.US
 *       blank-name damage class), or
 *   (b) its stated P/E disagrees with Price / EPS beyond 5% relative
 *       tolerance, excluding the GBX/GBP pence unit band (ratio 50-200)
 *       — the transposition signature (the GOOG-named-Gulfport class),
 *       mirroring run_dashboard_sync's L3b per-row rules exactly so the
 *       two layers can never disagree about the same row.
 * A suspect existing row is NOT protected: the incoming stub is written
 * (upsert paths) / kept in the outgoing table (positional path), which
 * blanks the poison honestly and lets the next healthy backend fetch
 * repopulate the row — self-healing instead of self-preserving. Healthy
 * existing rows keep the full v1.14.0 protection byte-identically.
 *
 * TELEMETRY: each keep-last-good pass now also reports
 * '[ID-FIREWALL v1.15.0] suspect_healed=N: SYM,SYM,...' via Logger and
 * a companion warnings line beside the existing v1.14.0 lines (visible
 * in _Status like all page warnings), so the standing six-gate audit
 * sees the firewall firing (or silent) every run. The filter/merge
 * results additionally expose .suspects (additive field; existing
 * callers are unaffected).
 *
 * KILL-SWITCH: Script Property TFB_REFRESH_KLG_IDENTITY_GATE, default
 * ON; set 0/false/off/no/disabled to restore exact v1.14.0 keep
 * behavior (suspect rows protected again). Scope note: the single-row
 * selected-refresh keep path is deliberately NOT gated in this version
 * (manual, operator-visible, one symbol); it inherits the gate in a
 * later pass if ever needed.
 *
 * New functions (4): _refreshKlgIdentityGateEnabled_,
 * _refreshNumForIdentity_, _refreshRowIdentityBroken_,
 * _refreshOldRecordIsSuspect_ (+ global REFRESH_KLG_ID_SUSPECTS_).
 * ZERO functions removed; every prior WHY-block carried verbatim.
 *
 * v1.14.0 (2026-07-12) — KEEP-LAST-GOOD EMPTY-PARTIAL WRITE GUARD
 * --------------------------------------------------------------------------------
 * WHY: the 2026-07-12 morning audit (v38 export) caught this writer
 * destroying data during a backend quote-provider outage: the manual
 * Market_Leaders batch stamped 37 rows at 06:11/06:42 UTC whose Name AND
 * Current Price were BLANK (MSFT.US, PG.US, ADBE.US, HSBA.L, ...) —
 * MSFT.US had only 58/115 fields populated (forecasts present, identity
 * and price gone). The backend returned EMPTY PARTIALS (quote failed,
 * derived fields partially present) and this file wrote them straight
 * through: the symbol-keyed upsert overwrites every mapped field, blank
 * or not, so a good yesterday-price row became a timestamped shell. The
 * GitHub-side writer gained exactly this protection in
 * run_dashboard_sync v6.22.3 (L4c: error stubs must never overwrite
 * valid rows); this closes the SECOND writer so no path can downgrade a
 * row that already has a price.
 *
 * RULE (row-level, honest — no field-chimera merging): an incoming row
 * is WITHHELD when its current_price is blank AND the sheet's existing
 * row for that symbol has a non-blank Current Price; additionally, on a
 * page without a price column, when incoming name+price are both blank
 * over an existing Name. Withheld rows are NOT written at all — the
 * previous row survives verbatim WITH ITS OWN HONEST TIMESTAMP. We
 * deliberately do NOT backfill old name/price into the fresh row: that
 * would stamp NOW onto a stale price (the exact dishonesty v1.12.6
 * eliminated) and mint rows mixing two runs (the Top_10 C1 defect
 * class). New symbols with no existing identity pass through unchanged
 * (nothing to protect); a row with a FRESH price but blank name passes
 * (price is the protected ground truth; the 5023.SR-class name gap is
 * the backend's static-name-fallback fix, not a write-guard concern).
 *
 * WIRE SITES (all five write paths):
 *   1. refreshPageInBatchesCore_ — per-chunk filter of rowObjects AND
 *      legacy rows2d BEFORE provenance injection, against a ONE-READ
 *      identity map (SYMBOL -> has-name/has-price) captured at batch
 *      start (each symbol is written at most once per run, so the map
 *      stays accurate; requeued guard-deferrals were never written).
 *      Dropping a row here only skips that symbol's upsert — the upsert
 *      never deletes. Cumulative '[v1.14.0 KEEP_LAST_GOOD]' telemetry
 *      at completion + keptLastGood counters in logRun/result. (The
 *      paused/tripwire PARTIAL early-returns do not carry the cumulative
 *      line; the per-chunk Logger lines cover those runs.)
 *   2. refreshPageCore_ header-mapped single-shot — same filter; if
 *      every row is withheld the write is skipped entirely and a
 *      synthetic zero-write result is used (covers My_Portfolio, which
 *      at 8 symbols takes the single-shot path).
 *   3. refreshPageCore_ positional full-table — writeTableToSheet_
 *      CLEARS the data area first, so a withheld row is SUBSTITUTED with
 *      the existing sheet row rebuilt verbatim (dropping it would DELETE
 *      the symbol).
 *   4./5. refreshSelectedRowCore_ (header-mapped + positional) — an
 *      empty-partial single-row refresh returns a friendly
 *      ROW_KEPT_LAST_GOOD result instead of wiping the selected row.
 *
 * KILL-SWITCH: Script Property TFB_REFRESH_KEEP_LAST_GOOD, default ON;
 * set 0/false/off/no/disabled to restore exact v1.13.0 write behavior
 * (mirrors the TFB_REFRESH_TRIPWIRE pattern). All helpers never throw;
 * on any internal error the filter degrades to pass-through. NOTE this
 * guard prevents RECURRENCE only — the 37 rows already wiped on
 * 2026-07-12 are repaired by the next successful backend write (the
 * v6.22.4 Actions dispatch / universe restore), not by this file.
 * Everything else byte-identical to v1.13.0; every prior WHY-block and
 * guard carried verbatim. Internal logger tags preserved per the Tag
 * Stability policy.
 *
 * v1.13.0 (2026-07-08) — CONCURRENT-WRITER TRIPWIRE + LOSSLESS DEFERRAL
 * --------------------------------------------------------------------------------
 * WHY: the v36 audit caught the read->write race in the act. The GitHub
 * daily_sync full-table writer and this file's symbol-keyed upsert share
 * the four instrument pages with NO cross-process coordination (the
 * withScriptLock_ DocumentLock only serializes Apps Script executions).
 * When the sync rewrites a page inside our map->flush gap, the stale map
 * mints duplicates via the unguarded append path (AAPL x2, MET.US x2 on
 * 2026-07-08) and colliding getLastRow()+1 anchors erase fresh rows
 * (part of the GM -75 symbol loss). 02_Core v2.9.0 closes the write
 * seams (append place-guard + second-chance + deferredSymbols); THIS
 * file adds the page-level defenses:
 *   1. TRIPWIRE (_refreshTripwireEnabled_, Script Property
 *      TFB_REFRESH_TRIPWIRE, default ON): a cheap structural fingerprint
 *      (last row + 12 head + 12 tail symbols) is captured at batch start,
 *      RE-BASELINED after each of our own chunk writes (so we never trip
 *      on ourselves), and re-checked every 3rd iteration. Drift our
 *      writer cannot explain (row delta > 25, or >=10% / >=3 sampled
 *      symbols disagreeing — thresholds echoing the v1.12.7 mass-drop
 *      guard) ABORTS the batch CLEANLY: checkpoint kept, PARTIAL status
 *      with reason 'concurrent_writer', paused:true so 13_AutoRefresh's
 *      existing resume machinery retries after the foreign writer
 *      finishes. Writing blind into a moving table is never acceptable.
 *   2. TRIPWIRE BELT ON DEDUPE: the v1.12.4 compaction (whole-data-area
 *      rewrite from one read — the precise v1.12.7 concern) is skipped
 *      when drift is detected at completion.
 *   3. LOSSLESS DEFERRAL: symbols withheld by the 02_Core v2.9.0 guards
 *      (placementAborted blocks + appendAborted rows) are re-queued ONCE
 *      into the live checkpoint tail (checkpoint.requeued ledger caps
 *      each symbol to a single retry per run), so guard aborts defer
 *      fresh data instead of dropping it. New '[v1.13.0 GUARDS]'
 *      completion telemetry line (place_aborted / append_aborted /
 *      second_chance / requeued). Aligned with 02_Core.gs v2.9.0.
 * Everything else byte-identical to v1.12.7; every prior WHY-block and
 * guard carried verbatim. Internal logger tags preserved per the Tag
 * Stability policy.
 *
 * v1.12.7 (2026-07-07) — DEDUPE COMPACTION SAFETIES (concurrent-writer hardening)
 * --------------------------------------------------------------------------------
 * WHY: _refreshDedupeInstrumentSheetBySymbol_ rewrites the ENTIRE data area
 * from a single read (clearDataArea_ + setValues + deleteRows of the
 * surplus tail). With any concurrent writer (the GitHub daily_sync
 * full-table write) or a stale read, that authoritative rewrite can
 * resurrect old content over fresh rows and its deleteRows tail-trim
 * permanently removes symbol rows — one plausible contributor to the
 * whole-block symbol losses measured on 2026-07-05..07. Two changes:
 *   1. _refreshDedupeEnabled_ default flips ON -> OFF. The pass now runs
 *      only when script property TFB_REFRESH_DEDUPE is explicitly 1/on.
 *      (The duplicate problem it solved is real but rare; the blast radius
 *      of a bad compaction is the whole page.)
 *   2. MASS-DROP GUARD inside the compaction: if the rewrite would remove
 *      more than 25 rows AND more than 10% of the data rows, it ABORTS with
 *      a WARN instead of rewriting — mass removal is treated as evidence of
 *      a stale read / concurrent writer, never of genuine duplicates.
 * Everything else byte-identical to v1.12.6.
 *
 *
 * Aligned with:
 * - 00_Config.gs v1.12.6 (isDecisionOwnedPage_ authority)
 * - 01_Menu.gs v2.12.0 (decision-owned page-action REDIRECT)
 * - 02_Core.gs v2.8.0
 * - 03_Schema.gs v2.7.0 (decision-wire)
 * - 04_Format.gs v2.16.0 (decision-wire + proposal palette)
 * - 11_SpecialPages.gs v1.8.3
 * - 16_Decision_Top10.gs v1.1.0 (cockpit owner of Top_10_Investments)
 * - 19_Portfolio_Economics.gs v1.0.0 (My_Portfolio derived economics post-pass)
 * - Backend: core/analysis/top10_selector v4.18.0 (Fix W), data_engine_v2 v5.103.0 (Fix AQ)
 * - Backend sync: scripts/run_dashboard_sync.py v6.19.2 (UNIVERSE JUNK FILTER + STRICT MEMBERSHIP + CAP twin)
 * - Main investment pages: 115 columns
 * - My_Portfolio: 122 columns (115 + 7 engine-native decision columns at 116-122)
 * - Top_10_Investments: DECISION-OWNED cockpit (engine refresh must never touch it)
 * - Global_Markets: full 115-column investment page, NOT old 46-column Layout A
 *
 * v1.12.6 PROVENANCE HONESTY FOR UNVERIFIED-PRICE ROWS (lockstep with
 * backend data_engine_v2 v5.103.0 Fix AQ)
 * ----------------------------------------------------------------------
 * ROOT CAUSE the backend now serves a row whose price could NOT be
 * verified live (Yahoo throttle -> history/snapshot fallback) with HONEST
 * timestamps — the fallback source's original stamp, or BLANK — plus a
 * warnings tag "price_unverified_live:<history|snapshot>". But this
 * file's _injectProvenanceFields_ fills EVERY blank last_updated_utc /
 * last_updated_riyadh with NOW before the write, so a manual sheet-menu
 * refresh would re-mint a fresh timestamp onto exactly the rows the
 * backend just declared unverified — partially undoing the honesty on
 * the one write path the GitHub sync does not own.
 *
 * FIX _injectProvenanceFields_ now SKIPS the timestamp fill for any row
 * whose warnings carry the price_unverified_live tag (new helper
 * _provenanceIsUnverifiedPrice_, string- and array-tolerant, never
 * throws). The data_provider fill is unchanged (naming the source is
 * still correct). A "[v1.12.6 PROVENANCE_HONESTY]" log line counts the
 * rows left honest. KILL-SWITCH: none needed here — the tag only exists
 * while the backend gate TFB_GATE_PRICE_STALENESS is ON; with it OFF no
 * row carries the tag and this file behaves byte-identically to v1.12.5.
 * Everything else, including the entire v1.12.5 deny filter, is carried
 * verbatim. Internal logger tags preserved per the Tag Stability policy.
 *
 * v1.12.5 SHEET-ECHO UNIVERSE DENY FILTER (lockstep with backend
 * run_dashboard_sync v6.19.0 UNIVERSE JUNK FILTER)
 * ----------------------------------------------------------------------
 * ROOT CAUSE the four instrument pages SELF-SOURCE their request universe
 * from the sheet's own Symbol column (getManualSymbolsFromSheet_, reached
 * via getWorkingSymbolsForPage_ / resolveRefreshSymbols_ /
 * getRequestedSymbolsFromOptions_). Any junk symbol that ever lands on a
 * page (the TICK000..TICK021 placeholder family found contaminating
 * Global_Markets and reaching the Top_10 candidate pool on 2026-07-03) is
 * therefore RE-REQUESTED and re-written on EVERY refresh, forever — the
 * upsert writer never deletes, so sheet-echoed junk is immortal. The
 * GitHub-side sync gained the equivalent guard in run_dashboard_sync
 * v6.19.0 (TFB_SYNC_UNIVERSE_DENY); this closes the SECOND writer (the
 * Apps Script batch refresh) so junk cannot re-enter through either path.
 *
 * FIX one filter at the single sheet-echo chokepoint
 * (getManualSymbolsFromSheet_): symbols matching a deny pattern are
 * dropped from the echoed universe BEFORE any request is built, with a
 * "[v1.12.5 UNIVERSE_FILTER]" log line counting the drops. Properties:
 *   - DEFAULT deny list: ^TICK\d+ (the placeholder-ticker family).
 *   - CONFIGURABLE via Script Property TFB_UNIVERSE_DENY — comma-separated
 *     case-insensitive regexes matched against the normalized (trimmed,
 *     uppercased) symbol; set 0/off/no/false/disabled/- to disable the
 *     filter entirely (byte-identical v1.12.4 behavior).
 *   - SCOPE every caller that echoes the sheet universe (all four
 *     instrument pages plus any page falling through to the sheet echo).
 *     EXPLICIT operator-passed symbols (options / payload overrides) are
 *     deliberately NOT filtered — an explicit request is operator intent.
 *   - NON-FATAL a Properties read failure degrades to the default list; a
 *     malformed pattern is skipped with a log line; the helpers never
 *     throw and can never alter or fail a refresh result.
 *   - NOTE this filter deliberately does NOT delete junk rows already on
 *     the sheet (the upsert never deletes; the one-off 2026-07-03 cleanup
 *     removed the existing 26). It guarantees junk is never REQUESTED
 *     again, so it can never be re-stamped or re-enriched, and the GitHub
 *     sync's guarded write path (v6.19.0 deny + persistence) keeps it out
 *     of that writer too.
 * New helpers: _refreshUniverseDenyPatterns_, _refreshIsDeniedSymbol_.
 * New constants: REFRESH_UNIVERSE_DENY_PROPERTY_,
 * REFRESH_UNIVERSE_DENY_DEFAULT_. Everything else is byte-identical to
 * v1.12.4. Every prior WHY-block and every guard is carried verbatim.
 * Internal logger tags ([v1.11.2 *], [v1.9.2 *], [v1.12.1 MP_ECONOMICS])
 * are preserved verbatim per the Tag Stability policy.
 *
 * v1.12.4 INSTRUMENT-SHEET DUPLICATE-ROW SELF-HEAL (no contract change)
 * --------------------------------------------------------------------
 * ROOT CAUSE the four pure-instrument pages (Market_Leaders,
 * Global_Markets, Commodities_FX, Mutual_Funds) accumulate duplicate
 * physical rows — e.g. 4030.SR present twice: a stale 2026-06-24
 * "Saudi Market" row beside a fresh 2026-06-25 GICS-sector row. The
 * Top_10 candidate-pool dedup then keeps the OLDER copy and surfaces
 * the stale "Saudi Market" sector even though both engine fixes
 * (TFB_KW_FILS_NORMALIZE, TFB_KSA_SECTOR_MAP) are confirmed live and
 * correct on the freshly-written copy.
 *
 * WHY THE UPSERT CAN NEVER SELF-CLEAN. These four pages self-source
 * their symbol list from the sheet (getManualSymbolsFromSheet_) and
 * grow past REFRESH_BATCH_SIZE_DEFAULT_, so refreshPageCore_ always
 * routes them through refreshPageInBatchesCore_ -> upsertRowsBySymbol_
 * -> 02_Core writeRowsBySymbol_. That writer is a SYMBOL-KEYED UPSERT:
 * it builds a symbol->row map from the existing sheet and either
 * UPDATES-in-place or APPENDS. It NEVER DELETES. Worse, the map is
 * LAST-WINS: when a symbol is already present twice, the map points at
 * the LATER copy, so the upsert refreshes that copy and leaves the
 * EARLIER copy ORPHANED. Orphans are unreachable by every subsequent
 * refresh and persist indefinitely (the interleaved 06-20/06-24/06-25
 * dates on the live sheet are the fingerprint of this accretion). The
 * positional writer (writeTableToSheet_) clears first and is immune;
 * the dup problem is specific to the upsert paths.
 *
 * FIX a scoped, idempotent post-write dedupe pass
 * (_refreshDedupeInstrumentSheetBySymbol_) that runs AFTER the write
 * on those four pages only. It groups data rows by Symbol
 * (case/space-insensitive), keeps exactly ONE row per symbol — the
 * freshest by "Last Updated (Riyadh)" (later physical occurrence wins
 * a timestamp tie, i.e. the copy the upsert just refreshed) — drops
 * blank-symbol padding rows, then rewrites via 02_Core clearDataArea_
 * + setValues and removes the surplus trailing rows. Properties:
 *   - SELF-HEALING the existing 406->~203 collapse happens on the next
 *     Market_Leaders refresh with no manual step.
 *   - ZERO empty-sheet risk it operates ONLY on rows already on the
 *     sheet (no backend dependency); on a clean sheet it is a NO-OP
 *     (no clear, no write) and behavior is byte-identical to v1.12.3.
 *   - SCOPED true only for the four instrument pages
 *     (_refreshIsInstrumentDedupePage_); it NEVER runs on My_Portfolio
 *     (manual cost-basis columns — a blind rewrite would risk them),
 *     on the decision-owned Top_10 cockpit, or on the positional
 *     key/value pages. A defensive guard also bails on any page that
 *     declares preserveManualColumns.
 *   - REVERSIBLE kill-switch Script Property
 *     TFB_DEDUPE_INSTRUMENT_SHEETS (default ON; set 0/false/off/no/
 *     disabled to restore exact v1.12.3 behavior).
 *   - NON-FATAL the whole pass is wrapped so it can never alter or
 *     fail a refresh result; on any error it logs and returns a skip.
 *
 * It is fired at two sites: refreshPageInBatchesCore_ AFTER the batch
 * loop completes and the checkpoint is cleared (the paused/partial
 * branch returns earlier, so dedupe never runs on an incomplete
 * refresh — no half-batch data loss), and refreshPageCore_'s
 * single-shot header-mapped path after writeRowsBySymbol_ (covers a
 * future sub-batch-size instrument sheet). 02_Core.gs is intentionally
 * NOT changed — its upsert throw/return semantics are correct; the fix
 * is to prune orphans after the write, not to rewrite the writer.
 *
 * Everything else is byte-identical to v1.12.3. Every prior WHY-block
 * and every v1.12.x decision-owned guard is carried verbatim. Internal
 * logger tags ([v1.11.2 *], [v1.9.2 *], [v1.12.1 MP_ECONOMICS]) are
 * preserved verbatim per the Tag Stability policy.
 *
 * v1.12.3 DOCUMENTATION ALIGNMENT (no logic change; byte-identical behavior)
 * --------------------------------------------------------------------------
 * This revision changes only this header block — every function body is
 * carried verbatim from v1.12.2 and runtime behavior is byte-identical. It
 * re-aligns the stale "Aligned with" cross-file references to the contract
 * now shipped: 01_Menu v2.12.0 (the decision-owned page action is now a
 * REDIRECT, not a hard guard), 04_Format v2.16.0 (proposal-palette harmonized),
 * and 16_Decision_Top10 v1.1.0 (the cockpit owner).
 *
 * WHY THIS FILE NEEDS NO CODE CHANGE FOR THE v2.12.0 MENU REDIRECT. The menu
 * fix and this file's decision-owned skip are deliberate COMPLEMENTS that
 * together make every route to Top_10_Investments safe:
 *   - 01_Menu v2.12.0 handles the SINGLE-PAGE menu actions. Clicking a legacy
 *     engine action on the Top_10 submenu (Build / Refresh All Data / Apply
 *     Format / Sort, etc.) no longer throws "Action failed" — menuPageAction_
 *     now REDIRECTS those to the cockpit's own builder/refresh
 *     (buildDecisionTop10Layout / refreshDecisionTop10 in 16_Decision_Top10),
 *     and gracefully skips the actions with no cockpit equivalent. That path
 *     never enters this file.
 *   - THIS FILE handles the MULTI-PAGE loops. "Refresh All Pages"
 *     (refreshAllPages_) and "Refresh Analysis & Insights"
 *     (refreshAnalysisAndInsights_) iterate getSupportedPages /
 *     [Insights, Top_10] and call refreshPageCore_(Top_10) directly; the
 *     v1.12.0 MASTER GUARD in refreshPageCore_ — plus the five entry-point
 *     guards (refreshSelectedRowCore_, sortAndRankPageCore_,
 *     _repairCanonicalHeadersCore_, trimSheetToExpectedColumns_, validatePage_)
 *     — SKIP-not-throw on the cockpit so the loop keeps running and refreshes
 *     the remaining pages. The cockpit is therefore only ever refreshed through
 *     its dedicated path (the Decision menu / the menu redirect), never by an
 *     engine table write from a bulk refresh. No body change is required or
 *     desirable; adding a redirect here would double-refresh the cockpit inside
 *     a bulk loop and risk the Render edge timeout.
 *
 * Internal logger tags ([v1.11.2 *], [v1.9.2 *], [v1.12.1 MP_ECONOMICS]) are
 * preserved verbatim per the Tag Stability policy.
 *
 * v1.12.2 MY_PORTFOLIO WIDTH-VALIDATION FALSE-ALARM FIX
 * -----------------------------------------------------
 * ROOT CAUSE validateExpectedRefreshWidth_ classified My_Portfolio as a
 * "main instrument page" (via _isMainInstrumentPageForRefresh_) and then
 * asserted that EVERY main instrument page must be exactly 115 columns:
 *
 *     if (_isMainInstrumentPageForRefresh_(page.pageName) && expected !== 115)
 *
 * Under the current contract My_Portfolio is intentionally 122 columns
 * (115 canonical + 7 engine-native decision columns at 116-122: Buy Date,
 * Target Weight %, Actual Weight %, Weight Gap, Rebalance Action, Investor
 * Decision, User Notes — see 00_Config v1.12.3 / 04_Format v2.14.0 FIX B/C/D).
 * A CORRECT 122-column My_Portfolio therefore failed this assertion on EVERY
 * refresh: validateExpectedRefreshWidth_ returned ok=false with a spurious
 * "My_Portfolio expectedColumnCount should be 115 but Config says 122"
 * warning, which propagated through validatePostRefreshIntegrity_ and forced
 * the page status to WARN every single time — a permanent false alarm that
 * buried any genuine warning, exactly the failure class the v1.11.4 Top_10
 * realignment set out to eliminate.
 *
 * FIX (two surgical changes; everything else byte-identical to v1.12.1):
 *   1. New helper _isStrict115PageForRefresh_(pageName) — the FOUR pages that
 *      are genuinely 115 columns (Market_Leaders, Global_Markets,
 *      Commodities_FX, Mutual_Funds). My_Portfolio is deliberately NOT in it.
 *   2. validateExpectedRefreshWidth_ now asserts 115 only for those four
 *      pages, and asserts My_Portfolio against 122 separately. Real
 *      misconfigurations are still caught (a 120-wide My_Portfolio or a
 *      114-wide Market_Leaders still warns), but a correct 122-column
 *      My_Portfolio validates clean.
 *   The page-final-header check is UNCHANGED and still runs for all main
 *   instrument pages including My_Portfolio: it already derives the correct
 *   trailing header per page from getFallbackHeaders (Warnings for the 115
 *   pages, User Notes for My_Portfolio), so it never misfired and is left as
 *   is. The Top_10 118-width assertion is likewise untouched.
 *
 * Everything else is byte-identical to v1.12.1. Every v1.12.x decision-owned
 * guard, the My_Portfolio economics post-pass, and every WHY-block are carried
 * verbatim. Internal logger tags ([v1.11.2 *], [v1.9.2 *], [v1.12.1
 * MP_ECONOMICS]) are preserved verbatim per the Tag Stability policy.
 *
 * v1.12.1 MY_PORTFOLIO ECONOMICS POST-PASS (lockstep with 19_Portfolio_Economics.gs v1.0.0)
 * ------------------------------------------------------------------------------------------
 * ROOT CAUSE Position Cost / Position Value / Unrealized P/L / Unrealized P/L %
 * render blank on My_Portfolio. The engine derives them only when position_qty
 * AND avg_cost are present on the row, but those two are MANUAL sheet columns
 * (00_Config preserveManualColumns) that GAS preserves locally and never sends
 * to the backend, and there is no server-side holdings/cost-basis book — so the
 * engine receives qty=None/avg_cost=None and skips the computation. The four
 * derived columns are NOT in preserveOnEmpty, so every refresh (header-mapped,
 * positional, or batch) writes them blank. They must be computed CLIENT-SIDE
 * from the sheet's own Qty / Avg Cost / Current Price AFTER each write.
 *
 * FIX one new typeof-guarded helper _refreshRunMyPortfolioEconomics_(pageName)
 * that calls pecoFillMyPortfolioEconomics_() (19_Portfolio_Economics.gs) only
 * when pageName is My_Portfolio. It is fired from every PUBLIC entry point that
 * completes a My_Portfolio refresh — refreshPage_ (Refresh Current Page),
 * buildPage_ (Build Current Page), refreshAllPages_ (Refresh All Pages),
 * refreshSelectedRow_ (selected-row refresh), and refreshPageInBatches_ (manual
 * batch) — immediately after the core write returns, INSIDE the existing
 * withScriptLock_ scope. Each path fires exactly once: refreshPageCore_'s
 * internal auto-batch delegates to refreshPageInBatchesCore_ (the CORE, not the
 * public wrapper), and no path chains two public wrappers, so there is no
 * double-fire. refreshAnalysisAndInsights_ touches only Insights/Top_10 and is
 * deliberately NOT wired. The helper:
 *   - is a NO-OP for every page except My_Portfolio (cheap early return);
 *   - degrades cleanly (typeof guard) when 19_Portfolio_Economics.gs is not yet
 *     installed — refresh behaves exactly as v1.12.0;
 *   - swallows ALL errors in a try/catch and logs a "[v1.12.1 MP_ECONOMICS]"
 *     line, so the economics pass can never alter or fail a refresh result.
 * The economics pass itself is stateless and idempotent (it recomputes the four
 * columns from current inputs and blanks rows with insufficient inputs), so a
 * single call per refresh fully reconciles them.
 *
 * Everything else is byte-identical to v1.12.0. Every v1.12.0 decision-owned
 * guard and WHY-block is carried verbatim. Internal logger tags
 * ([v1.11.2 *], [v1.9.2 *]) are preserved verbatim per the Tag Stability policy.
 *
 * v1.12.0 DECISION-LAYER WIRING (closes the KNOWN GAP declared in 01_Menu v2.10.0)
 * --------------------------------------------------------------------------------
 * ROOT CAUSE Top_10_Investments is now rendered by the Decision cockpit
 * (16_Decision_Top10.gs): row 1 is a title banner, not a 'Symbol' header row.
 * Every legacy refresh entry point still funnels Top_10 into refreshPageCore_
 * (refreshAllPages_, refreshAnalysisAndInsights_, buildPage_, direct
 * refreshPage_, and the v1.11.5 batch redirect). On the cockpit,
 * _sheetHasSymbolColumnRefresh_ is false, so refreshPageCore_ takes the
 * POSITIONAL path: writeTableToSheet_ writes the 118 engine headers into
 * row 1, clears the data area, and dumps engine rows — TOTAL COCKPIT
 * DESTRUCTION from a single "Refresh All Pages" click. Additional destroyers:
 * repairCanonicalHeaders_Top10 (would insert all 118 engine headers as
 * columns), refreshSelectedRowCore_ (row overwrite), sortAndRankPageCore_
 * (re-orders cockpit zones), trimSheetToExpectedColumns_ (expected=118
 * width logic is meaningless for the cockpit), and validatePage_ (engine-
 * layout checks guarantee false WARN spam on the cockpit).
 *
 * FIX one MASTER guard plus five entry-point guards, all consulting
 * 00_Config v1.12.6 isDecisionOwnedPage_ (typeof-guarded; degrades to legacy
 * behavior if that Config is not yet installed):
 *   1. refreshPageCore_ — the single chokepoint every refresh path crosses
 *      (refreshAllPages_/refreshAnalysisAndInsights_/buildPageCore_/
 *      refreshPage_/batch redirect). Skip-not-throw: returns
 *      {ok:true, action:'skipped_decision_owned'} so multi-page loops keep
 *      running and refresh the remaining pages normally.
 *   2. refreshSelectedRowCore_ — independent entry; same skip.
 *   3. sortAndRankPageCore_ — same skip (also shields direct script calls).
 *   4. _repairCanonicalHeadersCore_ — skip with a friendly dialog; covers
 *      repairCanonicalHeaders_Top10 and the generic repair entry.
 *   5. trimSheetToExpectedColumns_ — skip result (covers
 *      trimAllSheetsToExpectedColumns_ and the internal cleanup callers).
 *   6. validatePage_ — returns a decision-owned result WITHOUT logging WARN
 *      or writing a WARN page status (no false-alarm spam for the cockpit).
 * New helpers: _refreshIsDecisionOwnedPage_ / _refreshSkipDecisionOwned_
 * (names unique to this file per the guard-helper convention). Explicit
 * 'force' options do NOT override the guard. reformatAllSheets_ needs no
 * change — 04_Format v2.14.0 guards applySheetFormatting_ itself.
 *
 * Everything else is byte-identical to v1.11.5. Internal logger tags
 * ([v1.11.2 *], [v1.9.2 *]) are preserved verbatim per the Tag Stability
 * policy.
 *
 * v1.11.5 TOP10 BATCH-PATH SYMBOL ECHO (closes the gap v1.11.4 left open)
 * ------------------------------------------------------------------------
 * ROOT CAUSE the v1.11.4 empty-symbol default landed in
 * resolveRefreshSymbols_ — the SINGLE-SHOT payload path — while the batch
 * path was "intentionally unchanged (batch mode is not the supported refresh
 * path for Top_10)". But Top_10 reaches batch mode AUTOMATICALLY:
 *
 *   refreshPageCore_(Top_10)
 *     -> shouldAutoBatchRefresh_
 *          -> getRequestedSymbolsFromOptions_   (no explicit overrides:
 *               11_SpecialPages v1.8.2+ omits symbol keys)
 *               -> getManualSymbolsFromSheet_   ECHOES the sheet's CURRENT
 *                  Symbol column
 *     -> sheet had 17 residual symbols (union of runs) -> 17 > batch size 10
 *     -> AUTO-BATCH -> refreshPageInBatchesCore_ re-derives the same 17 sheet
 *        symbols and sends them as symbols/tickers/direct_symbols in chunks
 *        of REFRESH_BATCH_SIZE_DEFAULT_=10.
 *
 * LIVE EVIDENCE (2026-06-11 08:30 UTC run): the two Criteria Snapshots on the
 * sheet are exactly the sheet's 17 symbols split 10+7 —
 *   ["GOVT.US","GOOGL","HE=F","META","1120.SR","2222.SR","NVDA","2220.SR","MSFT","IWM"]
 *   ["GC=F","AMZN","AAPL","CPI.JSE","BZ=F","CL=F","VOO"]
 * Each chunk got the selector's +140 direct-symbol priority bump (correct for
 * explicit user requests), so last run's winners crowded out the organic
 * candidate universe — the self-reinforcing list v1.11.4 set out to kill,
 * reborn one code path over. Two batches also means two independent selector
 * runs, each emitting its own ranks 1..N (two rank-1 rows observed).
 *
 * FIX (three surgical changes; everything else byte-identical to v1.11.4):
 *   1. getRequestedSymbolsFromOptions_ now returns [] for Top_10_Investments
 *      when no explicit symbols are given — mirroring the v1.11.4
 *      resolveRefreshSymbols_ rule at the second derivation site. With zero
 *      requested symbols, shouldAutoBatchRefresh_ can never trip for Top_10.
 *   2. refreshPageInBatchesCore_ routes Top_10_Investments to the single-shot
 *      refreshPageCore_ path (exactly like Data_Dictionary and
 *      Insights_Analysis already are). Top_10 is ONE backend-computed POST;
 *      chunked per-symbol batching is meaningless for it and harmful when the
 *      chunks carry direct_symbols. This guards every entry point, including
 *      a manual "batch refresh" menu invocation.
 *   3. shouldAutoBatchRefresh_ returns false for Top_10_Investments
 *      explicitly (defense in depth alongside 1 and 2).
 *   Explicit overrides still win everywhere: selected-row refresh passes its
 *   one symbol and keeps working; options.symbols on the Top_10 refresh is
 *   still honored verbatim.
 *
 * Internal logger tags ([v1.11.2 *], [v1.9.2 *]) are preserved verbatim per
 * the Tag Stability policy.
 *
 * v1.11.4 TOP10 CONTRACT REALIGNMENT (lockstep with selector v4.17.0 / engine v5.84.0)
 * ------------------------------------------------------------------------------------
 * ROOT CAUSE two pieces of this file still encoded the pre-Fix-T Top_10 contract:
 *
 *   1. validateTop10RefreshIntegrity_ required EVERY Top_10 row to be
 *      Final Action=INVEST / Investability Status=INVESTABLE and to pass the
 *      strict DQ/reliability thresholds unconditionally. The deployed backend
 *      (engine v5.84.0 Fix T conservative floor + selector v4.17.0 Fix U/V)
 *      deliberately ranks the BEST candidates and labels them honestly — a
 *      fully-WATCHLIST Top_10 is correct system behavior (verified live
 *      2026-06-10: all 10 rows WATCHLIST/WATCH with coherent Block Reasons,
 *      fresh gate verdicts on the wire). The old validator therefore flagged
 *      every legitimate row on every refresh, set page status to WARN forever,
 *      and buried real problems in guaranteed false alarms.
 *
 *      The validator now checks the ACTUAL deployed contract:
 *        - completeness/staleness (the validator's original purpose, retained):
 *          Top 10 Rank, Current Price, Investability Status, Final Action,
 *          Data Quality Score and Forecast Reliability Score must be populated;
 *          blank gate fields = stale pre-gate row;
 *        - coherence: Investability Status must pair with Final Action
 *          (INVESTABLE<->INVEST, WATCHLIST<->WATCH, BLOCKED<->DO_NOT_INVEST),
 *          and WATCHLIST/BLOCKED rows must carry a non-blank Block Reason;
 *        - duplicate-rank detection;
 *        - the strict quality bar (HIGH risk, ROI<=0, DQ/reliability minimums,
 *          Provider/Engine Conflict, nonblank Block Reason) now applies to
 *          INVEST rows ONLY — exactly the rows where those conditions are
 *          genuine contradictions.
 *
 *   2. resolveRefreshSymbols_ echoed the sheet's CURRENT 10 symbols back to the
 *      backend as symbols/tickers/direct_symbols when refreshing Top_10.
 *      Top_10 is a backend-computed selection page; echoing last run's winners
 *      would pin the selector's candidate pool to itself (a self-reinforcing
 *      list — a displaced symbol could never be replaced) on any backend
 *      revision that honors direct_symbols. The verified-correct caller sends
 *      EMPTY symbol lists (live criteria snapshots show direct_symbols: []).
 *      resolveRefreshSymbols_ now returns [] for Top_10_Investments when no
 *      explicit override is given. Explicit overrides (e.g. selected-row
 *      refresh) still win; the batch path is intentionally unchanged (batch
 *      mode is not the supported refresh path for Top_10).
 *      (v1.11.5: the batch path turned out to be REACHABLE via auto-batch —
 *      see the v1.11.5 block above for the closure.)
 *
 * Everything else is byte-identical to v1.11.2/v1.11.3. Internal logger tags
 * ([v1.11.2 *], [v1.9.2 *]) are preserved verbatim per the Tag Stability policy.
 *
 * v1.11.3 INSIGHTS WRITER-PATH FIX (lockstep with the v2.14.0 Insights contract)
 * -----------------------------------------------------------------------------
 * ROOT CAUSE after Insights_Analysis adopted the live backend contract
 * (Section | Item | Metric | Value | Notes | Source | Sort Order), refreshing it
 * threw: 'Sheet "Insights_Analysis" has no "Symbol" header column.'
 *
 * The throw originates in 02_Core.gs writeRowsBySymbol_, which is the
 * HEADER-MAPPED, SYMBOL-KEYED writer. It locates a 'Symbol' column to upsert
 * rows by symbol and (correctly) throws when there is none. refreshPageCore_
 * routed Insights_Analysis to that writer because its decision was:
 *
 *     useHeaderMappedPath = useHeaderMap && rowObjects.length > 0
 *                           && sheetHasHeaders && !legacyPathRequested;
 *
 * The OLD Insights contract carried a 'Symbol' column, so it slipped through.
 * The NEW v2.14.0 contract is a key/value report with NO 'Symbol' column, so the
 * symbol-keyed writer can no longer be used for it. Insights is not a per-symbol
 * instrument table; it must use the POSITIONAL writer (writeTableToSheet_).
 *
 * FIX add a single routing guard at the one chokepoint every refresh entry point
 * passes through (refreshPageCore_): if the sheet has no 'Symbol' column, never
 * take the header-mapped path. This covers ALL callers at once —
 * refreshInsightsAnalysis_, refreshAllPages_, refreshAnalysisAndInsights_, and
 * any direct refreshPage_ call — rather than patching a single page entry (which
 * would leave Refresh All Pages / Refresh Analysis & Insights still throwing,
 * since those call refreshPageCore_('Insights_Analysis') directly).
 *
 * The guard only NARROWS when the header-mapped path is taken; every instrument
 * page (Market_Leaders / Global_Markets / Commodities_FX / Mutual_Funds /
 * My_Portfolio / Top_10_Investments) has a 'Symbol' column and is unaffected.
 * Data_Dictionary already returned empty row_objects (positional anyway); the
 * guard makes that explicit and future-proofs any new key/value page.
 *
 * CHANGES (surgical; everything else byte-identical to v1.11.2):
 *   1. New helper _sheetHasSymbolColumnRefresh_(sheet) — true when the sheet's
 *      header row resolves a 'Symbol' column (case/space-insensitive).
 *   2. refreshPageCore_ useHeaderMappedPath now also requires
 *      sheetHasSymbolColumn; the POSITIONAL diagnostic reason gains a
 *      POSITIONAL_NO_SYMBOL_COLUMN case so the chosen path is visible in logs.
 *   3. 02_Core.gs is intentionally NOT changed — its throw is correct behavior
 *      for a symbol-keyed writer; the fix is to not route symbol-less pages to
 *      it.
 *
 * v1.11.0 MAIN FIXES
 * ------------------
 * 1. Adds post-refresh expected-width validation:
 *      - Main pages must be 115 columns
 *      - Top_10_Investments must be 118 columns
 *
 * 2. Adds post-refresh investment-gate validation:
 *      - No INVEST with blank Current Price
 *      - No INVEST with negative Expected ROI 3M
 *      - No INVEST with negative Expected ROI 12M
 *      - No INVEST with HIGH Risk Bucket
 *      - When strict gate is enabled, no INVEST with:
 *          Data Quality Score < threshold
 *          Forecast Reliability Score < threshold
 *          Provider/Engine Conflict = TRUE
 *          Block Reason not blank
 *
 * 3. Adds Global_Markets refresh-completeness validation:
 *      - Detects PARTIAL / mixed old-new timestamp situation
 *      - Warns if row count is lower than requested symbol count
 *      - Warns if many rows lack Last Updated (Riyadh)
 *
 * 4. Adds Top_10_Investments integrity validation:
 *      - (v1.11.4: realigned to the deployed Fix T/U/V contract — see above)
 *
 * 5. Keeps the existing strong mechanics:
 *      - Header-mapped writer path
 *      - Checkpointed batch refresh
 *      - Status panel cleanup / residue hunt
 *      - Provenance injection
 *      - Header drift detection / repair
 *      - Fallback warning audit trail
 * v1.11.1 FIX
 * -----------
 * - validateExpectedRefreshWidth_ now derives the expected final column
 *   from getFallbackHeaders(pageName). This avoids false WARN results because
 *   the canonical 115-column main-page layout ends with Warnings, not Block Reason.
 *
 * v1.11.2 FIX
 * -----------
 * - writePageStatusPanel_ is now non-mutating for data sheets. It no longer
 *   writes Status/Message/Warnings/Updated cells into row 1 beyond the canonical
 *   schema, which previously caused sheet-ahead widths such as 117/120.
 * - Adds trimSheetToExpectedColumns_ and trimAllSheetsToExpectedColumns_ to
 *   delete old status-panel / stale layout columns after the approved schema
 *   width: 115 main pages, 118 Top10, 9 Data_Dictionary, 7 Insights.
 */
'use strict';
/* =========================================================
 * Runtime constants
 * ========================================================= */
/* v1.18.1: suite-standard runtime version (header kept in lockstep). */
var REFRESH_VERSION = '1.20.0';
/* -----------------------------------------------------------------------------
 * v1.19.0 (2026-08-29) - BACKEND-HOLD PRE-WRITE TRIPWIRE (SPANNING-CHUNK FIX)
 * -----------------------------------------------------------------------------
 * FIELD PROOF (run 33255802873): a chunk's fetch takes 40-90s while the
 * backend's whole Sheets burst is ~30s - the burst started AND finished
 * entirely inside one fetch (backend GM write 17:47:22-17:47:50 sat inside
 * the chunk in flight 17:45->17:50; the stale flush landed 17:50:42). Every
 * existing probe fires BEFORE the fetch or AFTER the checkpoint, so the one
 * moment that decides contamination - just before the write - was unguarded,
 * and the 45s post-write grace had already expired by the next boundary.
 * ONE insertion point, TWO detectors, both fail-open, both inert unless the
 * coordinator gate TFB_GAS_BACKEND_HOLD=1 (script property):
 *   (1) LIVE HOLD - tfbBackendHoldProbe_ (coordinator v1.2.0) says the hold
 *       is active at write time.
 *   (2) HOLD ACTIVITY - the _Sync_Control backend-hold row (B or C) changed
 *       between fetch-start and write time: a backend publish/clear happened
 *       while our request was in flight, so the fetched rows are stale even
 *       if the hold has ALREADY expired. This detector needs no grace
 *       tuning and pairs with backend v6.49's owners= ledger (any publish
 *       or clear rewrites C).
 * On either: the fetched chunk is discarded UNWRITTEN, the checkpoint stays
 * at the chunk start, and the run exits through the TRIPWIRE result contract
 * (ok/partial/paused + aborted:'backend_hold', action:'yield-backend-hold'
 * for the 13_AutoRefresh v1.10.1 classifier). DELIBERATE deviation from the
 * TRIPWIRE template: no writePageStatusPanel_/writePageStatus_ calls inside
 * this yield - those are themselves sheet writes into the exact window we
 * just declined to touch; the yield is logRun_ + checkpoint + return only.
 * HEADER DRIFT CORRECTED: the file header said 1.18.1 while the runtime
 * constant said 1.18.2; both now read 1.19.0.
 * Functions added: 3 (_refreshBackendHoldGateOn_, _refreshBackendHoldSnapshot_,
 * _refreshBackendHoldPreWriteCheck_). Functions removed: 0. Gate OFF or any
 * read error: byte-identical behavior to v1.18.2.
 * -------------------------------------------------------------------------- */
/* -----------------------------------------------------------------------------
 * v1.18.2 (2026-08-27) — BACKEND HOLD CHECKED ON THE NO-MANUAL PATH
 * -----------------------------------------------------------------------------
 * FORENSIC WHY: _refreshManualPriorityProbe_ returned {yield:false} the
 * moment Core manual priority was absent — the ONLY early exit every batch
 * boundary takes on a normal day — so the batch writer never consulted the
 * TFB_GAS_BACKEND_HOLD coordinator at all. Runtime evidence 2026-08-27:
 * 63 GAS actions inside the backend sync window (5 PARTIAL row-writes on
 * Global_Markets 02:10–02:41 Riyadh), and OHLC-READBACK divergence
 * +392/+102/+63 payload→sheet on the same run. ONE anchored edit: on the
 * no-manual path, probe tfbBackendHoldProbe_ (coordinator v1.2.0,
 * typeof-guarded, fail-open) before returning continue. A hold-yield reuses
 * the existing pause machinery — checkpoint preserved, PARTIAL result,
 * trigger resumes after the hold TTL (180s) expires. Functions added: 0.
 * Functions removed: 0. Behavior when the coordinator is absent or the gate
 * is OFF: byte-identical to v1.18.1.
 * -------------------------------------------------------------------------- */

var REFRESH_BATCH_SIZE_DEFAULT_ = 10;
var REFRESH_MAX_RUNTIME_MS_ = 240000;
var REFRESH_CHECKPOINT_PREFIX_ = 'TFB_BATCH_CHECKPOINT::';
/* v1.20.0 [FAILED-LEDGER]: one bounded retry of failed-batch symbols (property 'on'), else observe. */
var REFRESH_FAILED_RETRY_PROPERTY_ = 'REFRESH_FAILED_RETRY';
var REFRESH_FAILED_RETRY_MAX_ = 60;
var REFRESH_STATUS_PANEL_BUFFER_COLS_ = 5;
var REFRESH_STATUS_PANEL_SWEEP_ROWS_ = 10;
var REFRESH_PANEL_LABEL_TOKENS_ = ['Status', 'Message', 'Warnings', 'Updated'];
var REFRESH_PANEL_RESIDUE_SCAN_WIDTH_ = 30;
var REFRESH_STRICT_GATE_PROPERTY_ = 'TFB_STRICT_INVEST_GATE';
var REFRESH_STRICT_DQ_MIN_PROPERTY_ = 'TFB_STRICT_INVEST_DQ_MIN';
var REFRESH_STRICT_RELIABILITY_MIN_PROPERTY_ = 'TFB_STRICT_INVEST_RELIABILITY_MIN';
var REFRESH_DEFAULT_STRICT_DQ_MIN_ = 80;
var REFRESH_DEFAULT_STRICT_RELIABILITY_MIN_ = 70;
// v1.12.4: kill-switch for the instrument-sheet duplicate-row dedupe pass.
// Default ON; set this Script Property to 0/false/off/no/disabled to restore
// exact v1.12.3 behavior (no post-write dedupe).
var REFRESH_DEDUPE_PROPERTY_ = 'TFB_DEDUPE_INSTRUMENT_SHEETS';
// v1.12.5: deny-pattern filter for the sheet-echoed symbol universe. The
// Script Property holds comma-separated case-insensitive regexes matched
// against the normalized symbol; absent/blank -> the default placeholder
// family below; 0/off/no/false/disabled/- -> filter disabled entirely.
var REFRESH_UNIVERSE_DENY_PROPERTY_ = 'TFB_UNIVERSE_DENY';
var REFRESH_UNIVERSE_DENY_DEFAULT_ = '^TICK\\d+';
// v1.14.0: kill-switch for the keep-last-good empty-partial write guard.
// Default ON; set this Script Property to 0/false/off/no/disabled to restore
// exact v1.13.0 write behavior (empty partials written through).
var REFRESH_KEEP_LAST_GOOD_PROPERTY_ = 'TFB_REFRESH_KEEP_LAST_GOOD';
/* v1.15.0 ID-FIREWALL kill-switch property + per-run suspects accumulator. */
var REFRESH_KLG_IDENTITY_GATE_PROPERTY_ = 'TFB_REFRESH_KLG_IDENTITY_GATE';
var REFRESH_KLG_ID_SUSPECTS_ = [];
/* =========================================================
 * Feature detection
 * ========================================================= */
/**
 * v1.17.0 (B-5) — HTTP-aware status/level. ES5, no dependencies.
 * A blank/absent code means "no transport involved" and is always OK.
 */
function rfHttpHonestyOn_() {
  try {
    var v = PropertiesService.getScriptProperties()
        .getProperty('TT_HTTP_STATUS_HONESTY');
    if (v === null || v === undefined || v === '') return true;
    v = String(v).toLowerCase();
    return !(v === 'off' || v === '0' || v === 'false' || v === 'no');
  } catch (e) {
    return true;   // unreadable properties must not disable a guard
  }
}
function rfHttpOk_(code) {
  if (code === null || code === undefined || code === '') return true;
  var n = parseInt(code, 10);
  if (isNaN(n)) return true;
  return n >= 200 && n <= 299;
}
function rfStatusForHttp_(status, code) {
  if (!rfHttpHonestyOn_() || rfHttpOk_(code)) return status;
  return 'WARN';
}
function rfLevelForHttp_(level, code) {
  if (!rfHttpHonestyOn_() || rfHttpOk_(code)) return level;
  return 'WARN';
}
function rfMsgForHttp_(msg, code) {
  if (!rfHttpHonestyOn_() || rfHttpOk_(code)) return msg;
  return String(msg) + ' (http ' + code + ')';
}

function _refreshCanUseHeaderMap_() {
  return typeof writeRowsBySymbol_ === 'function'
    && typeof writeSingleRowByHeaderMap_ === 'function'
    && typeof getHeaderToKey === 'function';
}
/* v1.11.3: the header-mapped writer (02_Core.gs writeRowsBySymbol_) is
 * symbol-keyed and throws on a sheet with no 'Symbol' header column. Pages that
 * are key/value reports (Insights_Analysis under the v2.14.0 contract,
 * Data_Dictionary) have no 'Symbol' column and must use the positional writer.
 * This helper lets refreshPageCore_ decide the path correctly for every caller. */
function _sheetHasSymbolColumnRefresh_(sheet) {
  try {
    if (!sheet) return false;
    var headers = readCurrentSheetHeaders_(sheet);
    if (!headers || !headers.length) return false;
    return findHeaderIndexRefresh_(headers, 'Symbol') >= 0;
  } catch (e) {
    return false;
  }
}
/* =========================================================
 * v1.12.0 — decision-owned page guard
 * ========================================================= */
/* Top_10_Investments is rendered by the Decision cockpit
 * (16_Decision_Top10.gs). Engine refresh/sort/repair/trim must never touch
 * it. Authority is 00_Config v1.12.6 isDecisionOwnedPage_; typeof-guarded so
 * this file degrades to legacy behavior when that Config is absent. */
function _refreshIsDecisionOwnedPage_(pageName) {
  try {
    if (typeof isDecisionOwnedPage_ === 'function') {
      return isDecisionOwnedPage_(pageName) === true;
    }
  } catch (ownErr) {}
  /* v1.18.1: FAIL-CLOSED fallback — if the 00_Config authority is absent
   * or throws, the cockpit page stays protected by a local constant. A
   * mandatory guard never fails open (identity-war doctrine). */
  return String(pageName || '').toLowerCase() === 'top_10_investments';
}
/* Standard skip result + audit log. Skip-not-throw so batch loops
 * (refreshAllPages_, refreshAnalysisAndInsights_, trimAll) keep running. */
function _refreshSkipDecisionOwned_(actionName, pageName) {
  var msg = '"' + pageName + '" is rendered by the Decision cockpit ' +
    '(16_Decision_Top10.gs). ' + actionName + ' skipped to protect the cockpit. ' +
    'Use the Decision menu (Refresh Decision Top 10) instead.';
  try {
    logRun_('WARN', actionName, pageName, msg, {
      status: 'SKIPPED_DECISION_OWNED'
    });
  } catch (logErr) {}
  return {
    ok: true,
    pageName: pageName,
    action: 'skipped_decision_owned',
    skipped: true,
    message: msg
  };
}
/* =========================================================
 * v1.12.1 — My_Portfolio economics post-pass
 * ========================================================= */
/* My_Portfolio's Position Cost / Position Value / Unrealized P/L /
 * Unrealized P/L % are emitted blank by the engine (the manual Qty / Avg Cost
 * are never sent to the backend; see 19_Portfolio_Economics.gs for the full
 * root-cause note) and are NOT preserve-on-empty, so every refresh rewrites
 * them blank. They are computed client-side from the sheet's own Qty /
 * Avg Cost / Current Price AFTER the write. This helper is fired from every
 * public entry point that completes a My_Portfolio refresh. It is a no-op for
 * any other page, degrades cleanly when 19_Portfolio_Economics.gs is absent
 * (typeof guard), and swallows ALL errors so the refresh result is never
 * affected. The economics pass is stateless + idempotent. */
function _refreshRunMyPortfolioEconomics_(pageName) {
  try {
    if (pageName !== SHEET_NAMES.MY_PORTFOLIO) return;
    if (typeof pecoFillMyPortfolioEconomics_ !== 'function') return;
    var econ = pecoFillMyPortfolioEconomics_();
    try {
      Logger.log('[v1.12.1 MP_ECONOMICS] ok=' + (econ && econ.ok) +
        ' filledCost=' + (econ && econ.filledCost) +
        ' filledValue=' + (econ && econ.filledValue) +
        ' filledPL=' + (econ && econ.filledPL) +
        ' filledPLpct=' + (econ && econ.filledPLpct) +
        ' skipped=' + (econ && econ.skipped));
    } catch (logErr) {}
  } catch (e) {
    try {
      Logger.log('[v1.12.1 MP_ECONOMICS] THREW error=' +
        ((e && e.message) ? e.message : String(e)));
    } catch (logErr2) {}
  }
}
/* =========================================================
 * v1.12.5 — sheet-echo universe deny filter
 * ========================================================= */
/* Compile the deny-pattern list for the sheet-echoed symbol universe. The
 * Script Property TFB_UNIVERSE_DENY holds comma-separated case-insensitive
 * regexes; absent/blank falls back to the default placeholder family
 * (^TICK\d+); an explicit 0/off/no/false/disabled/- disables the filter
 * (returns []). A Properties read failure degrades to the default list; a
 * malformed pattern is skipped with a log line. Never throws. */
function _refreshUniverseDenyPatterns_() {
  var raw = null;
  try {
    var props = PropertiesService.getScriptProperties();
    raw = props.getProperty(REFRESH_UNIVERSE_DENY_PROPERTY_);
  } catch (propErr) {
    raw = null;
  }
  if (raw === null || raw === undefined || String(raw).trim() === '') {
    raw = REFRESH_UNIVERSE_DENY_DEFAULT_;
  }
  var s = String(raw).trim();
  var sLower = s.toLowerCase();
  if (['0', 'off', 'no', 'false', 'disabled', 'disable', '-'].indexOf(sLower) >= 0) {
    return [];
  }
  var parts = s.split(',');
  var out = [];
  for (var i = 0; i < parts.length; i++) {
    var p = String(parts[i]).trim();
    if (!p) continue;
    try {
      out.push(new RegExp(p, 'i'));
    } catch (reErr) {
      try {
        Logger.log('[v1.12.5 UNIVERSE_FILTER] bad deny pattern "' + p + '" skipped: ' +
          ((reErr && reErr.message) ? reErr.message : String(reErr)));
      } catch (logErr) {}
    }
  }
  return out;
}
/* True when the (normalized) symbol matches any deny pattern. Junk is never
 * REQUESTED, so it can never be re-stamped or re-enriched by this writer.
 * Explicit operator-passed symbols do NOT pass through this helper — an
 * explicit request is operator intent. Never throws. */
function _refreshIsDeniedSymbol_(symbol, patterns) {
  var t = String(symbol === null || symbol === undefined ? '' : symbol).trim().toUpperCase();
  if (!t) return false;
  var pats = patterns || _refreshUniverseDenyPatterns_();
  for (var i = 0; i < pats.length; i++) {
    try {
      if (pats[i].test(t)) return true;
    } catch (testErr) {}
  }
  return false;
}
/* =========================================================
 * v1.12.4 — instrument-sheet duplicate-row self-heal
 * ========================================================= */
/* Kill-switch reader for the dedupe pass. Default ON. Only an explicit
 * 0/false/off/no/disabled Script Property turns it off; any other value
 * (or absence, or a read error) leaves it ON. */
function _refreshDedupeEnabled_() {
  /* v1.12.7: default flipped OFF. The authoritative compaction rewrite is
   * unsafe while any concurrent writer can touch the page; enable explicitly
   * with script property TFB_REFRESH_DEDUPE = 1/on/true when needed. */
  try {
    var props = PropertiesService.getScriptProperties();
    var v = props.getProperty(REFRESH_DEDUPE_PROPERTY_);
    if (v === null || v === undefined) return false;
    var s = String(v).trim().toLowerCase();
    if (['1', 'true', 'on', 'yes', 'enabled', 'enable'].indexOf(s) >= 0) return true;
    return false;
  } catch (e) {
    return false;
  }
}
/* The four PURE-INSTRUMENT pages that are safe to dedupe by Symbol. These are
 * backend-computed instrument tables with one row per symbol and NO manual
 * columns, so collapsing duplicate-symbol rows to the freshest copy is always
 * correct. My_Portfolio is deliberately EXCLUDED (it shares the canonical
 * schema but carries manual cost-basis columns); the decision-owned Top_10
 * cockpit and the positional key/value pages are excluded too. */
function _refreshIsInstrumentDedupePage_(pageName) {
  return [
    SHEET_NAMES.MARKET_LEADERS,
    SHEET_NAMES.GLOBAL_MARKETS,
    SHEET_NAMES.COMMODITIES_FX,
    SHEET_NAMES.MUTUAL_FUNDS
  ].indexOf(pageName) >= 0;
}
/* Collapse duplicate-symbol rows on an instrument sheet to ONE row per symbol,
 * keeping the freshest copy. See the v1.12.4 header block for the full root
 * cause (writeRowsBySymbol_ is a last-wins symbol-keyed upsert that orphans the
 * earlier copy of any already-duplicated symbol and never deletes).
 *
 * Self-guards (every one returns a skip result, never throws):
 *   - kill-switch disabled
 *   - page not one of the four pure-instrument pages
 *   - page declares preserveManualColumns (defensive — should never be true for
 *     the four, but bail rather than risk manual data if that ever changes)
 *   - sheet has < 2 data rows (no duplicate possible)
 *   - sheet has no resolvable Symbol column
 *
 * Freshness rule: keep the row with the newest "Last Updated (Riyadh)"
 * timestamp (yyyy-MM-dd HH:mm:ss strings sort lexicographically); ties and a
 * missing timestamp column both fall back to "later physical occurrence wins",
 * which is exactly the copy the last-wins upsert just refreshed. Blank-symbol
 * padding rows are dropped. On a clean sheet (no dups, no blanks) this is a
 * NO-OP — no clear, no write — so behavior is byte-identical to v1.12.3.
 *
 * Rewrite: 02_Core clearDataArea_(sheet, 1, []) + setValues of the kept rows
 * (first-seen order; a sort pass runs immediately after in the caller), then a
 * best-effort deleteRows of the now-surplus trailing rows. The content rewrite
 * is the authoritative step; the trailing-row delete is pure cleanup and is
 * wrapped so a failure can never corrupt the already-correct data. */
function _refreshDedupeInstrumentSheetBySymbol_(sheet, pageName) {
  try {
    if (!_refreshDedupeEnabled_()) {
      return { ok: true, skipped: true, reason: 'disabled' };
    }
    if (!sheet) {
      return { ok: true, skipped: true, reason: 'no_sheet' };
    }
    if (!_refreshIsInstrumentDedupePage_(pageName)) {
      return { ok: true, skipped: true, reason: 'not_instrument_page' };
    }
    // Defensive: never rewrite a page that declares manual columns.
    var page = null;
    try {
      page = getPageConfig(pageName);
    } catch (cfgErr) {
      page = null;
    }
    if (page && Array.isArray(page.preserveManualColumns) && page.preserveManualColumns.length) {
      return { ok: true, skipped: true, reason: 'preserve_manual_columns' };
    }
    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    if (lastRow < 3 || lastCol < 1) {
      return { ok: true, skipped: true, reason: 'too_few_rows' };
    }
    var headers = readCurrentSheetHeaders_(sheet);
    if (!headers || !headers.length) {
      return { ok: true, skipped: true, reason: 'no_headers' };
    }
    var symbolColName = getSymbolColumnNameFromHeaders_(headers);
    if (!symbolColName) {
      return { ok: true, skipped: true, reason: 'no_symbol_column' };
    }
    var symbolIdx = findHeaderIndexRefresh_(headers, symbolColName);
    if (symbolIdx < 0) {
      return { ok: true, skipped: true, reason: 'no_symbol_index' };
    }
    // Optional freshness column. -1 is fine: the physical-position tiebreak
    // alone still keeps the copy the upsert just refreshed (the later row).
    var tsIdx = findHeaderIndexRefresh_(headers, 'Last Updated (Riyadh)');
    var width = headers.length;
    var numDataRows = lastRow - 1;
    var readCols = Math.max(lastCol, width);
    var values = sheet.getRange(2, 1, numDataRows, readCols).getValues();
    var bestBySymbol = {};   // symbolUC -> { ts, idx, row }
    var order = [];          // first-seen order of kept symbols
    var dupDropped = 0;
    var blankDropped = 0;
    for (var i = 0; i < values.length; i++) {
      var row = values[i];
      var symRaw = (symbolIdx < row.length) ? row[symbolIdx] : '';
      var sym = String(symRaw == null ? '' : symRaw).trim();
      if (!sym) {
        blankDropped++;
        continue;
      }
      var symUC = sym.toUpperCase();
      var ts = (tsIdx >= 0 && tsIdx < row.length)
        ? String(row[tsIdx] == null ? '' : row[tsIdx]).trim()
        : '';
      if (!Object.prototype.hasOwnProperty.call(bestBySymbol, symUC)) {
        bestBySymbol[symUC] = { ts: ts, idx: i, row: row };
        order.push(symUC);
      } else {
        var cur = bestBySymbol[symUC];
        var replace;
        if (ts > cur.ts) {
          replace = true;
        } else if (ts === cur.ts) {
          replace = i > cur.idx;   // later physical row wins a tie
        } else {
          replace = false;
        }
        if (replace) {
          bestBySymbol[symUC] = { ts: ts, idx: i, row: row };
        }
        dupDropped++;
      }
    }
    // No-op on a clean sheet: preserve byte-identical v1.12.3 behavior.
    if (dupDropped === 0 && blankDropped === 0) {
      return { ok: true, skipped: true, reason: 'already_clean', keptSymbols: order.length };
    }
    var outRows = [];
    for (var o = 0; o < order.length; o++) {
      var best = bestBySymbol[order[o]].row;
      var outRow = best.slice(0, width);
      while (outRow.length < width) {
        outRow.push('');
      }
      outRows.push(outRow);
    }
    // v1.12.7 MASS-DROP GUARD: a compaction that would remove a large block
    // of rows is evidence of a stale read or a concurrent writer, not of
    // duplicates. Refuse to rewrite the page in that state.
    var plannedDrop = numDataRows - outRows.length;
    if (plannedDrop > 25 && plannedDrop > Math.floor(numDataRows * 0.10)) {
      try {
        Logger.log('[v1.12.7 DEDUPE_GUARD][' + pageName + '] ABORTED: rewrite would drop ' +
          plannedDrop + ' of ' + numDataRows + ' data rows — mass compaction refused.');
      } catch (gLog) {}
      return { ok: false, skipped: true, reason: 'mass_drop_guard', plannedDrop: plannedDrop, beforeDataRows: numDataRows };
    }
    // Authoritative rewrite: clear data rows, write the deduped block.
    clearDataArea_(sheet, 1, []);
    if (outRows.length) {
      sheet.getRange(2, 1, outRows.length, width).setValues(outRows);
    }
    // Best-effort cleanup: delete the now-surplus trailing physical rows that
    // previously held data. Non-fatal — the content above is already correct.
    var newLastDataRow = 1 + outRows.length;
    var surplusStart = newLastDataRow + 1;
    var surplusEnd = 1 + numDataRows;
    try {
      surplusEnd = Math.min(surplusEnd, sheet.getMaxRows());
    } catch (maxErr) {}
    if (surplusEnd >= surplusStart) {
      try {
        sheet.deleteRows(surplusStart, surplusEnd - surplusStart + 1);
      } catch (delErr) {}
    }
    try {
      Logger.log('[v1.12.4 DEDUPE_SYMBOL][' + pageName + '] keptSymbols=' + order.length +
        ' dupDropped=' + dupDropped + ' blankDropped=' + blankDropped +
        ' beforeDataRows=' + numDataRows + ' afterDataRows=' + outRows.length);
    } catch (logErr) {}
    return {
      ok: true,
      pageName: pageName,
      keptSymbols: order.length,
      dupDropped: dupDropped,
      blankDropped: blankDropped,
      beforeDataRows: numDataRows,
      afterDataRows: outRows.length
    };
  } catch (e) {
    try {
      Logger.log('[v1.12.4 DEDUPE_SYMBOL][' + (pageName || '<unknown>') + '] THREW error=' +
        ((e && e.message) ? e.message : String(e)));
    } catch (logErr2) {}
    return { ok: true, skipped: true, reason: 'threw', error: (e && e.message) ? e.message : String(e) };
  }
}
/* =========================================================
 * v1.14.0 — keep-last-good empty-partial write guard
 * ========================================================= */
/* Kill-switch reader. Default ON. Only an explicit 0/false/off/no/disabled
 * Script Property turns it off; any other value (or absence, or a read
 * error) leaves it ON — the guard exists to stop data destruction. */
function _refreshKeepLastGoodEnabled_() {
  try {
    var v = PropertiesService.getScriptProperties().getProperty(REFRESH_KEEP_LAST_GOOD_PROPERTY_);
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    if (['0', 'false', 'off', 'no', 'disabled', 'disable'].indexOf(s) >= 0) return false;
    return true;
  } catch (e) {
    return true;
  }
}
/* v1.15.0 ID-FIREWALL kill-switch reader. Default ON. Only an explicit
 * 0/false/off/no/disabled Script Property turns it off; any other value
 * (or absence, or a read error) leaves it ON — the live failure class
 * (poison preserved by the keep, 2026-07-13 21:07-21:23 stamps)
 * violates it. */
function _refreshKlgIdentityGateEnabled_() {
  try {
    var v = PropertiesService.getScriptProperties().getProperty(REFRESH_KLG_IDENTITY_GATE_PROPERTY_);
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    if (['0', 'false', 'off', 'no', 'disabled', 'disable'].indexOf(s) >= 0) return false;
    return true;
  } catch (e) {
    return true;
  }
}
/* v1.15.0: tolerant numeric read for the identity test. Returns null on
 * blank / non-numeric / NaN / Infinity; strips thousands separators, %,
 * and unicode minus. Never throws. */
function _refreshNumForIdentity_(v) {
  try {
    if (v === null || v === undefined) return null;
    if (typeof v === 'boolean') return null;
    if (typeof v === 'number') {
      return (isFinite(v) ? v : null);
    }
    var s = String(v).trim();
    if (!s) return null;
    s = s.replace(/,/g, '').replace(/%/g, '').replace(/\u2212/g, '-').trim();
    if (s === '-' || s === '--' || s === 'N/A' || s === 'n/a' || s === 'NA' ||
        s === 'null' || s === 'None' || s === '\u2014') return null;
    var f = parseFloat(s);
    return (isFinite(f) ? f : null);
  } catch (e) {
    return null;
  }
}
/* v1.15.0: single-row P/E == Price / EPS identity test — the GAS mirror
 * of run_dashboard_sync's L3b per-row rules (5% relative tolerance;
 * GBX/GBP pence unit band ratio 50-200 excluded; a row missing any of
 * the triple, |EPS| < 0.01, or a non-positive stated P/E is UNTESTABLE
 * and therefore never condemned). TRUE = transposition signature. */
function _refreshRowIdentityBroken_(px, eps, pe) {
  var p = _refreshNumForIdentity_(px);
  var e = _refreshNumForIdentity_(eps);
  var q = _refreshNumForIdentity_(pe);
  if (p === null || e === null || q === null) return false;
  if (Math.abs(e) < 0.01 || q <= 0 || p <= 0) return false;
  var implied = p / e;
  if (implied <= 0) return false;
  var rel = Math.abs(implied - q) / Math.abs(q);
  if (rel < 0.05) return false;
  var ratio = implied / q;
  if (ratio >= 50 && ratio <= 200) return false;
  return true;
}
/* v1.15.0: is an EXISTING header-keyed record identity-suspect?
 * Suspect = (Name blank while a price is present) OR the P/E identity is
 * broken. Untestable rows (funds/FX with no EPS) are never suspect. */
function _refreshOldRecordIsSuspect_(record, headers) {
  try {
    if (!record || !headers || !headers.length) return false;
    var nameIdx = findHeaderIndexRefresh_(headers, 'Name');
    var priceIdx = findHeaderIndexRefresh_(headers, 'Current Price');
    var epsIdx = findHeaderIndexRefresh_(headers, 'EPS (TTM)');
    if (epsIdx < 0) epsIdx = findHeaderIndexRefresh_(headers, 'EPS');
    var peIdx = findHeaderIndexRefresh_(headers, 'P/E (TTM)');
    if (peIdx < 0) peIdx = findHeaderIndexRefresh_(headers, 'P/E');
    var nameBlank = nameIdx >= 0 ? _isBlankRefresh_(record[headers[nameIdx]]) : false;
    var priceVal = priceIdx >= 0 ? record[headers[priceIdx]] : '';
    var pricePresent = priceIdx >= 0 ? !_isBlankRefresh_(priceVal) : false;
    if (nameBlank && pricePresent) return true;
    if (priceIdx >= 0 && epsIdx >= 0 && peIdx >= 0) {
      return _refreshRowIdentityBroken_(
        priceVal,
        record[headers[epsIdx]],
        record[headers[peIdx]]
      );
    }
    return false;
  } catch (e) {
    return false;
  }
}
/* ONE-READ identity map of the sheet: SYMBOL -> { name: bool, price: bool }
 * (true = the existing row has a non-blank value in that column). Returns an
 * empty map — which makes every downstream check a pass-through — when the
 * sheet has no Symbol column or neither a Name nor a Current Price column
 * (key/value pages auto-scope themselves out). Never throws. */
function _refreshBuildIdentityMap_(sheet) {
  var map = {};
  try {
    if (!sheet) return map;
    var headers = readCurrentSheetHeaders_(sheet);
    if (!headers || !headers.length) return map;
    var symIdx = findHeaderIndexRefresh_(headers, 'Symbol');
    if (symIdx < 0) symIdx = findHeaderIndexRefresh_(headers, 'Ticker');
    if (symIdx < 0) return map;
    var nameIdx = findHeaderIndexRefresh_(headers, 'Name');
    var priceIdx = findHeaderIndexRefresh_(headers, 'Current Price');
    if (nameIdx < 0 && priceIdx < 0) return map;
    /* v1.15.0: identity-test columns for the suspect flag (fail-open:
     * a page without the P/E triple simply never marks a row suspect
     * on the ratio leg). */
    var epsIdx = findHeaderIndexRefresh_(headers, 'EPS (TTM)');
    if (epsIdx < 0) epsIdx = findHeaderIndexRefresh_(headers, 'EPS');
    var peIdx = findHeaderIndexRefresh_(headers, 'P/E (TTM)');
    if (peIdx < 0) peIdx = findHeaderIndexRefresh_(headers, 'P/E');
    var gateOn = _refreshKlgIdentityGateEnabled_();
    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    if (lastRow <= 1 || lastCol < 1) return map;
    var values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
    for (var i = 0; i < values.length; i++) {
      var sym = String(values[i][symIdx] || '').trim().toUpperCase();
      if (!sym) continue;
      var hasName = nameIdx >= 0 ? !_isBlankRefresh_(values[i][nameIdx]) : false;
      var hasPrice = priceIdx >= 0 ? !_isBlankRefresh_(values[i][priceIdx]) : false;
      var suspect = false;
      if (gateOn) {
        if (!hasName && hasPrice) {
          suspect = true;
        } else if (priceIdx >= 0 && epsIdx >= 0 && peIdx >= 0) {
          suspect = _refreshRowIdentityBroken_(
            values[i][priceIdx], values[i][epsIdx], values[i][peIdx]
          );
        }
      }
      map[sym] = {
        name: hasName,
        price: hasPrice,
        suspect: suspect
      };
    }
  } catch (e) {}
  return map;
}
/* Identity flags for ONE existing row given as a header-keyed record (the
 * selected-row context / getSheetDataAsObjects_ shape). hasCols=false when
 * the page has neither a Name nor a Current Price column, which makes the
 * downgrade check a pass-through. Never throws. */
function _refreshRecordIdentityFlags_(record, headers) {
  var flags = { name: false, price: false, hasCols: false };
  try {
    if (!record || !headers || !headers.length) return flags;
    var nameIdx = findHeaderIndexRefresh_(headers, 'Name');
    var priceIdx = findHeaderIndexRefresh_(headers, 'Current Price');
    if (nameIdx < 0 && priceIdx < 0) return flags;
    flags.hasCols = true;
    if (nameIdx >= 0) flags.name = !_isBlankRefresh_(record[headers[nameIdx]]);
    if (priceIdx >= 0) flags.price = !_isBlankRefresh_(record[headers[priceIdx]]);
  } catch (e) {}
  return flags;
}
/* THE keep-last-good predicate. TRUE = writing the incoming row would
 * DOWNGRADE the sheet:
 *   - incoming price blank over an existing price (the MSFT.US 06:11 UTC
 *     wipe class), or
 *   - incoming name AND price both blank over an existing name (full
 *     identity wipe on a page whose existing row has no price).
 * A fresh price with a blank name PASSES (price is the protected ground
 * truth; the name gap is the backend static-name-fallback concern). */
function _refreshIncomingIsDowngradeVsFlags_(nameBlank, priceBlank, flags) {
  if (!flags || !flags.hasCols) return false;
  if (priceBlank && flags.price) return true;
  if (nameBlank && priceBlank && flags.name) return true;
  return false;
}
/* Downgrade check for a backend row OBJECT (snake_case engine keys: name,
 * current_price) against the batch identity map. Never throws. */
function _refreshRowObjIsIdentityDowngrade_(rowObj, identityMap) {
  try {
    if (!rowObj || !identityMap) return false;
    var sym = extractRowSymbol_(rowObj);
    if (!sym) return false;
    var existing = identityMap[sym];
    if (!existing) return false;
    /* v1.15.0 ID-FIREWALL: an identity-suspect existing row forfeits
     * keep-last-good protection — the incoming stub is written so the
     * poison blanks out and the next healthy fetch can heal the row. */
    if (existing.suspect) {
      try { REFRESH_KLG_ID_SUSPECTS_.push(sym); } catch (fwErr) {}
      return false;
    }
    return _refreshIncomingIsDowngradeVsFlags_(
      _isBlankRefresh_(rowObj.name),
      _isBlankRefresh_(rowObj.current_price),
      { hasCols: true, name: existing.name, price: existing.price }
    );
  } catch (e) {
    return false;
  }
}
/* Downgrade check for a positional 2d row against ONE existing header-keyed
 * record. Never throws. */
function _refreshRow2dIsIdentityDowngrade_(row2d, headers, oldRecord) {
  try {
    if (!row2d || !headers || !headers.length || !oldRecord) return false;
    var nameIdx = findHeaderIndexRefresh_(headers, 'Name');
    var priceIdx = findHeaderIndexRefresh_(headers, 'Current Price');
    if (nameIdx < 0 && priceIdx < 0) return false;
    var nameBlank = nameIdx >= 0 ? _isBlankRefresh_(row2d[nameIdx]) : true;
    var priceBlank = priceIdx >= 0 ? _isBlankRefresh_(row2d[priceIdx]) : true;
    return _refreshIncomingIsDowngradeVsFlags_(
      nameBlank,
      priceBlank,
      _refreshRecordIdentityFlags_(oldRecord, headers)
    );
  } catch (e) {
    return false;
  }
}
/* Filter a chunk of backend row OBJECTS: rows that would downgrade an
 * existing row are WITHHELD (not written; the upsert never deletes, so the
 * previous row — and its honest timestamp — survives untouched). Returns
 * { kept: [...], withheld: [symbols] }. Pass-through when the guard is
 * disabled or the map is empty. Never throws. */
function _refreshFilterEmptyPartialRowObjects_(pageName, rowObjects, identityMap) {
  var out = { kept: rowObjects || [], withheld: [], suspects: [] };
  REFRESH_KLG_ID_SUSPECTS_ = [];
  try {
    if (!Array.isArray(rowObjects) || !rowObjects.length) return out;
    if (!identityMap) return out;
    if (!_refreshKeepLastGoodEnabled_()) return out;
    var kept = [];
    var withheld = [];
    for (var i = 0; i < rowObjects.length; i++) {
      if (_refreshRowObjIsIdentityDowngrade_(rowObjects[i], identityMap)) {
        withheld.push(extractRowSymbol_(rowObjects[i]));
      } else {
        kept.push(rowObjects[i]);
      }
    }
    out.kept = kept;
    out.withheld = withheld;
    out.suspects = REFRESH_KLG_ID_SUSPECTS_.slice(0);
    if (out.suspects.length) {
      try {
        Logger.log('[ID-FIREWALL v1.15.0][' + (pageName || '<unknown>') + '] suspect_healed=' +
          out.suspects.length + ' identity-suspect previous row(s) NOT protected (stub written to heal): ' +
          out.suspects.slice(0, 15).join(',') +
          (out.suspects.length > 15 ? ',...(+' + (out.suspects.length - 15) + ')' : ''));
      } catch (fwLogErr) {}
    }
    if (withheld.length) {
      try {
        Logger.log('[v1.14.0 KEEP_LAST_GOOD][' + (pageName || '<unknown>') + '] withheld=' +
          withheld.length + ' of ' + rowObjects.length +
          ' empty-partial row(s) (blank Price over existing data): ' +
          withheld.slice(0, 15).join(',') +
          (withheld.length > 15 ? ',...(+' + (withheld.length - 15) + ')' : ''));
      } catch (logErr) {}
    }
  } catch (e) {
    out.kept = rowObjects || [];
    out.withheld = [];
  }
  return out;
}
/* Same filter for legacy positional 2d rows on the BATCH/UPSERT path only:
 * dropping a row here merely skips that symbol's in-place update (the upsert
 * never deletes). NEVER use this on the full-table positional path — use
 * _refreshKeepLastGoodPositionalMerge_ there, because writeTableToSheet_
 * clears the data area and a dropped row would DELETE the symbol. */
function _refreshFilterEmptyPartialRows2d_(pageName, headers, rows2d, identityMap) {
  var out = { kept: rows2d || [], withheld: [], suspects: [] };
  try {
    if (!Array.isArray(rows2d) || !rows2d.length) return out;
    if (!identityMap || !headers || !headers.length) return out;
    if (!_refreshKeepLastGoodEnabled_()) return out;
    var nameIdx = findHeaderIndexRefresh_(headers, 'Name');
    var priceIdx = findHeaderIndexRefresh_(headers, 'Current Price');
    if (nameIdx < 0 && priceIdx < 0) return out;
    var symbolColumn = getSymbolColumnNameFromHeaders_(headers);
    if (!symbolColumn) return out;
    var symbolIndex = findHeaderIndexRefresh_(headers, symbolColumn);
    if (symbolIndex < 0) return out;
    var kept = [];
    var withheld = [];
    REFRESH_KLG_ID_SUSPECTS_ = [];
    for (var i = 0; i < rows2d.length; i++) {
      var row = rows2d[i];
      var sym = String((row && row[symbolIndex]) || '').trim().toUpperCase();
      var existing = sym ? identityMap[sym] : null;
      /* v1.15.0 ID-FIREWALL: suspect previous row -> never withhold. */
      if (existing && existing.suspect) {
        try { REFRESH_KLG_ID_SUSPECTS_.push(sym); } catch (fwErr) {}
        kept.push(row);
        continue;
      }
      var isDowngrade = false;
      if (existing) {
        var nameBlank = nameIdx >= 0 ? _isBlankRefresh_(row[nameIdx]) : true;
        var priceBlank = priceIdx >= 0 ? _isBlankRefresh_(row[priceIdx]) : true;
        isDowngrade = _refreshIncomingIsDowngradeVsFlags_(
          nameBlank, priceBlank,
          { hasCols: true, name: existing.name, price: existing.price }
        );
      }
      if (isDowngrade) {
        withheld.push(sym);
      } else {
        kept.push(row);
      }
    }
    out.kept = kept;
    out.withheld = withheld;
    out.suspects = REFRESH_KLG_ID_SUSPECTS_.slice(0);
    if (out.suspects.length) {
      try {
        Logger.log('[ID-FIREWALL v1.15.0][' + (pageName || '<unknown>') + '] legacy-2d: suspect_healed=' +
          out.suspects.length + ': ' + out.suspects.slice(0, 15).join(',') +
          (out.suspects.length > 15 ? ',...(+' + (out.suspects.length - 15) + ')' : ''));
      } catch (fwLogErr) {}
    }
    if (withheld.length) {
      try {
        Logger.log('[v1.14.0 KEEP_LAST_GOOD][' + (pageName || '<unknown>') + '] legacy-2d withheld=' +
          withheld.length + ' of ' + rows2d.length + ' empty-partial row(s): ' +
          withheld.slice(0, 15).join(',') +
          (withheld.length > 15 ? ',...(+' + (withheld.length - 15) + ')' : ''));
      } catch (logErr) {}
    }
  } catch (e) {
    out.kept = rows2d || [];
    out.withheld = [];
  }
  return out;
}
/* Keep-last-good for the POSITIONAL FULL-TABLE path (writeTableToSheet_
 * clears the data area first): a downgrading incoming row is SUBSTITUTED
 * with the existing sheet row rebuilt verbatim from its header-keyed record
 * — never dropped, or the symbol would be deleted from the page. Row count
 * is always preserved. Returns { rows: [...], withheld: [symbols] }.
 * Pass-through when disabled / nothing to protect. Never throws. */
function _refreshKeepLastGoodPositionalMerge_(pageName, headers, rows2d, existingRowsAsObjects) {
  var out = { rows: rows2d || [], withheld: [], suspects: [] };
  try {
    if (!Array.isArray(rows2d) || !rows2d.length) return out;
    if (!Array.isArray(existingRowsAsObjects) || !existingRowsAsObjects.length) return out;
    if (!headers || !headers.length) return out;
    if (!_refreshKeepLastGoodEnabled_()) return out;
    var nameIdx = findHeaderIndexRefresh_(headers, 'Name');
    var priceIdx = findHeaderIndexRefresh_(headers, 'Current Price');
    if (nameIdx < 0 && priceIdx < 0) return out;
    var symbolColumn = getSymbolColumnNameFromHeaders_(headers);
    if (!symbolColumn) return out;
    var symbolIndex = findHeaderIndexRefresh_(headers, symbolColumn);
    if (symbolIndex < 0) return out;
    var oldMap = mapExistingRowsBySymbol_(existingRowsAsObjects);
    var merged = [];
    var withheld = [];
    REFRESH_KLG_ID_SUSPECTS_ = [];
    var gateOn = _refreshKlgIdentityGateEnabled_();
    for (var i = 0; i < rows2d.length; i++) {
      var row = rows2d[i];
      var sym = String((row && row[symbolIndex]) || '').trim().toUpperCase();
      var oldRecord = sym ? oldMap[sym] : null;
      /* v1.15.0 ID-FIREWALL: a suspect previous row is never substituted
       * back over the incoming row — the incoming (possibly stub) row is
       * written so the poison blanks out honestly. */
      if (oldRecord && gateOn && _refreshOldRecordIsSuspect_(oldRecord, headers)) {
        try { REFRESH_KLG_ID_SUSPECTS_.push(sym); } catch (fwErr) {}
        merged.push(row);
        continue;
      }
      if (oldRecord && _refreshRow2dIsIdentityDowngrade_(row, headers, oldRecord)) {
        var oldRow = [];
        for (var h = 0; h < headers.length; h++) {
          var v = oldRecord[headers[h]];
          oldRow.push(v === null || v === undefined ? '' : v);
        }
        merged.push(oldRow);
        withheld.push(sym);
      } else {
        merged.push(row);
      }
    }
    out.rows = merged;
    out.withheld = withheld;
    out.suspects = REFRESH_KLG_ID_SUSPECTS_.slice(0);
    if (out.suspects.length) {
      try {
        Logger.log('[ID-FIREWALL v1.15.0][' + (pageName || '<unknown>') + '] positional: suspect_healed=' +
          out.suspects.length + ' identity-suspect previous row(s) NOT substituted (incoming row kept to heal): ' +
          out.suspects.slice(0, 15).join(',') +
          (out.suspects.length > 15 ? ',...(+' + (out.suspects.length - 15) + ')' : ''));
      } catch (fwLogErr) {}
    }
    if (withheld.length) {
      try {
        Logger.log('[v1.14.0 KEEP_LAST_GOOD][' + (pageName || '<unknown>') + '] positional: substituted ' +
          withheld.length + ' of ' + rows2d.length +
          ' empty-partial row(s) with the existing sheet row: ' +
          withheld.slice(0, 15).join(',') +
          (withheld.length > 15 ? ',...(+' + (withheld.length - 15) + ')' : ''));
      } catch (logErr) {}
    }
  } catch (e) {
    out.rows = rows2d || [];
    out.withheld = [];
  }
  return out;
}
/* Friendly terminal result for a WITHHELD selected-row refresh: the backend
 * returned an empty partial for the one requested symbol, so the existing
 * row is kept verbatim and the operator is told why. Never throws. */
function _refreshSelectedRowKeptLastGood_(pageName, sheet, selected, selectedSymbol, fetchResult) {
  var msg = 'Selected-row refresh withheld for ' + selectedSymbol +
    ': the backend returned no live price (empty partial). The existing row was kept unchanged — try again after the data provider recovers.';
  try {
    Logger.log('[v1.14.0 KEEP_LAST_GOOD][' + (pageName || '<unknown>') + '] selected-row withheld: ' + selectedSymbol);
  } catch (logErr) {}
  try {
    logRun_('WARN', 'refreshSelectedRow', pageName, msg, {
      status: 'KEPT_LAST_GOOD',
      endpoint: (fetchResult && fetchResult.endpointUsed) || '',
      httpCode: (fetchResult && fetchResult.statusCode) || '',
      durationMs: (fetchResult && fetchResult.durationMs) || '',
      symbol: selectedSymbol,
      row: selected ? selected.row : ''
    });
  } catch (lrErr) {}
  try {
    writePageStatus_(pageName, {
      status: 'ROW_KEPT_LAST_GOOD',
      message: msg,
      endpoint: (fetchResult && fetchResult.endpointUsed) || '',
      httpCode: (fetchResult && fetchResult.statusCode) || '',
      rows: 1,
      columns: readCurrentSheetHeaders_(sheet).length,
      durationMs: (fetchResult && fetchResult.durationMs) || '',
      warnings: [msg]
    });
  } catch (wpErr) {}
  return {
    ok: true,
    pageName: pageName,
    symbol: selectedSymbol,
    rowNumber: selected ? selected.row : 0,
    keptLastGood: true,
    endpointUsed: (fetchResult && fetchResult.endpointUsed) || '',
    message: msg
  };
}
/* =========================================================
 * Expected width helpers
 * ========================================================= */
function _findExpectedColumnCountForPage_(pageName) {
  if (!pageName) return 0;
  try {
    var page = getPageConfig(pageName);
    if (page && typeof page.expectedColumnCount === 'number' && page.expectedColumnCount > 0) {
      return page.expectedColumnCount;
    }
  } catch (e) {}
  return 0;
}
function _isInvestmentPageForRefresh_(pageName) {
  return [
    SHEET_NAMES.MARKET_LEADERS,
    SHEET_NAMES.GLOBAL_MARKETS,
    SHEET_NAMES.COMMODITIES_FX,
    SHEET_NAMES.MUTUAL_FUNDS,
    SHEET_NAMES.MY_PORTFOLIO,
    SHEET_NAMES.TOP_10_INVESTMENTS
  ].indexOf(pageName) >= 0;
}
function _isMainInstrumentPageForRefresh_(pageName) {
  return [
    SHEET_NAMES.MARKET_LEADERS,
    SHEET_NAMES.GLOBAL_MARKETS,
    SHEET_NAMES.COMMODITIES_FX,
    SHEET_NAMES.MUTUAL_FUNDS,
    SHEET_NAMES.MY_PORTFOLIO
  ].indexOf(pageName) >= 0;
}
/* v1.12.2: the pages that are GENUINELY 115 columns. My_Portfolio is a main
 * instrument page (it shares the 115-column canonical investment schema as its
 * first 115 columns) but is intentionally 122 wide because the engine appends
 * 7 native decision columns at 116-122. The 115-width assertion in
 * validateExpectedRefreshWidth_ must therefore use THIS list, not
 * _isMainInstrumentPageForRefresh_, so a correct 122-column My_Portfolio is not
 * flagged. My_Portfolio is asserted against 122 separately. */
function _isStrict115PageForRefresh_(pageName) {
  return [
    SHEET_NAMES.MARKET_LEADERS,
    SHEET_NAMES.GLOBAL_MARKETS,
    SHEET_NAMES.COMMODITIES_FX,
    SHEET_NAMES.MUTUAL_FUNDS
  ].indexOf(pageName) >= 0;
}
function validateExpectedRefreshWidth_(pageName, sheet) {
  var page = getPageConfig(pageName);
  sheet = sheet || ensureSheetExists_(page.pageName);
  var expected = Number(page.expectedColumnCount || 0);
  var headers = readCurrentSheetHeaders_(sheet);
  var actual = headers.length;
  var ok = expected <= 0 || actual === expected;
  var warnings = [];
  if (!ok) {
    warnings.push('Width mismatch: expected ' + expected + ' columns but sheet has ' + actual + '.');
  }
  // v1.11.1:
  // Do not hardcode Block Reason as the final main-page column.
  // In the canonical 115-column schema, Block Reason is part of the gate,
  // but the final column is Warnings. Derive the expected final header from
  // 00_Config fallback headers to keep this validator aligned with Config.
  // (This check is correct for My_Portfolio too: getFallbackHeaders returns the
  //  122-column fallback whose final header is User Notes.)
  if (_isMainInstrumentPageForRefresh_(page.pageName)) {
    var expectedFinalHeader = '';
    try {
      var fallbackHeaders = getFallbackHeaders(page.pageName);
      expectedFinalHeader = fallbackHeaders && fallbackHeaders.length
        ? String(fallbackHeaders[fallbackHeaders.length - 1])
        : '';
    } catch (e) {
      expectedFinalHeader = '';
    }
    if (expectedFinalHeader && headers[actual - 1] !== expectedFinalHeader) {
      ok = false;
      warnings.push(
        'Main-page final column should be ' + expectedFinalHeader +
        ' but found: ' + (headers[actual - 1] || '<blank>') + '.'
      );
    }
  }
  if (page.pageName === SHEET_NAMES.TOP_10_INVESTMENTS && expected !== 118) {
    ok = false;
    warnings.push('Top_10_Investments expectedColumnCount should be 118 but Config says ' + expected + '.');
  }
  // v1.12.2: assert 115 only for the four genuinely-115 pages. My_Portfolio is
  // intentionally 122 (115 + 7 engine-native decision columns at 116-122), so
  // the previous unconditional "_isMainInstrumentPageForRefresh_ && expected
  // !== 115" assertion fired on a CORRECT My_Portfolio and produced a permanent
  // false WARN. Assert My_Portfolio against 122 separately so real
  // misconfigurations are still caught.
  if (_isStrict115PageForRefresh_(page.pageName) && expected !== 115) {
    ok = false;
    warnings.push(page.pageName + ' expectedColumnCount should be 115 but Config says ' + expected + '.');
  }
  if (page.pageName === SHEET_NAMES.MY_PORTFOLIO && expected !== 122) {
    ok = false;
    warnings.push('My_Portfolio expectedColumnCount should be 122 but Config says ' + expected + '.');
  }
  return {
    ok: ok,
    pageName: page.pageName,
    expected: expected,
    actual: actual,
    finalHeader: headers[actual - 1] || '',
    warnings: warnings
  };
}
/* =========================================================
 * Post-refresh investment-gate validation
 * ========================================================= */
function _getStrictGateSettingsRefresh_() {
  var props = PropertiesService.getScriptProperties();
  var gateRaw = props.getProperty(REFRESH_STRICT_GATE_PROPERTY_);
  var dqRaw = props.getProperty(REFRESH_STRICT_DQ_MIN_PROPERTY_);
  var relRaw = props.getProperty(REFRESH_STRICT_RELIABILITY_MIN_PROPERTY_);
  return {
    enabled: _toBooleanRefresh_(gateRaw, false),
    dqMin: _toNumberRefresh_(dqRaw, REFRESH_DEFAULT_STRICT_DQ_MIN_),
    reliabilityMin: _toNumberRefresh_(relRaw, REFRESH_DEFAULT_STRICT_RELIABILITY_MIN_)
  };
}
function validateInvestmentGateAfterRefresh_(pageName, sheet) {
  var page = getPageConfig(pageName);
  sheet = sheet || ensureSheetExists_(page.pageName);
  var result = {
    ok: true,
    pageName: page.pageName,
    checkedRows: 0,
    investRows: 0,
    blankPriceInvest: [],
    negativeRoi3mInvest: [],
    negativeRoi12mInvest: [],
    highRiskInvest: [],
    lowDataQualityInvest: [],
    lowForecastReliabilityInvest: [],
    providerConflictInvest: [],
    blockReasonInvest: [],
    reduceSellInvest: [],
    warnings: []
  };
  if (!_isInvestmentPageForRefresh_(page.pageName)) {
    result.skipped = true;
    result.reason = 'not_investment_page';
    return result;
  }
  var headers = readCurrentSheetHeaders_(sheet);
  if (!headers.length || sheet.getLastRow() < 2) {
    result.skipped = true;
    result.reason = 'empty_sheet';
    return result;
  }
  var idx = _buildHeaderIndexRefresh_(headers);
  var required = [
    'Symbol',
    'Current Price',
    'Recommendation',
    'Risk Bucket',
    'Expected ROI 3M',
    'Expected ROI 12M',
    'Data Quality Score',
    'Forecast Reliability Score',
    'Provider/Engine Conflict',
    'Investability Status',
    'Final Action',
    'Block Reason'
  ];
  var missingRequired = required.filter(function (h) {
    return idx[h] === undefined;
  });
  if (missingRequired.length) {
    result.ok = false;
    result.warnings.push('Investment-gate validation skipped/incomplete: missing columns = ' + missingRequired.join(', '));
    result.missingRequired = missingRequired;
    return result;
  }
  var strict = _getStrictGateSettingsRefresh_();
  var lastRow = sheet.getLastRow();
  var lastCol = Math.max(sheet.getLastColumn(), headers.length);
  var values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  for (var r = 0; r < values.length; r++) {
    var row = values[r];
    var rowNum = r + 2;
    var symbol = _cellStringRefresh_(row, idx['Symbol']);
    if (!symbol) continue;
    result.checkedRows++;
    var finalAction = _cellStringRefresh_(row, idx['Final Action']).toUpperCase();
    var reco = _cellStringRefresh_(row, idx['Recommendation']).toUpperCase();
    var risk = _cellStringRefresh_(row, idx['Risk Bucket']).toUpperCase();
    var currentPrice = row[idx['Current Price']];
    var roi3m = _toNumberRefresh_(row[idx['Expected ROI 3M']], null);
    var roi12m = _toNumberRefresh_(row[idx['Expected ROI 12M']], null);
    var dq = _toNumberRefresh_(row[idx['Data Quality Score']], null);
    var rel = _toNumberRefresh_(row[idx['Forecast Reliability Score']], null);
    var conflict = _toBooleanRefresh_(row[idx['Provider/Engine Conflict']], false);
    var blockReason = _cellStringRefresh_(row, idx['Block Reason']);
    if (finalAction !== 'INVEST') continue;
    result.investRows++;
    var tag = symbol + '@R' + rowNum;
    if (_isBlankRefresh_(currentPrice)) {
      result.blankPriceInvest.push(tag);
    }
    if (roi3m !== null && roi3m <= 0) {
      result.negativeRoi3mInvest.push(tag + '=' + roi3m);
    }
    if (roi12m !== null && roi12m <= 0) {
      result.negativeRoi12mInvest.push(tag + '=' + roi12m);
    }
    if (risk === 'HIGH') {
      result.highRiskInvest.push(tag);
    }
    if (['REDUCE', 'SELL', 'AVOID'].indexOf(reco) >= 0) {
      result.reduceSellInvest.push(tag + '=' + reco);
    }
    if (strict.enabled) {
      if (dq !== null && dq < strict.dqMin) {
        result.lowDataQualityInvest.push(tag + '=' + dq);
      }
      if (rel !== null && rel < strict.reliabilityMin) {
        result.lowForecastReliabilityInvest.push(tag + '=' + rel);
      }
      if (conflict) {
        result.providerConflictInvest.push(tag);
      }
      if (blockReason) {
        result.blockReasonInvest.push(tag + '=' + blockReason);
      }
    }
  }
  _pushValidationWarningRefresh_(result.warnings, 'INVEST with blank Current Price', result.blankPriceInvest);
  _pushValidationWarningRefresh_(result.warnings, 'INVEST with Expected ROI 3M <= 0', result.negativeRoi3mInvest);
  _pushValidationWarningRefresh_(result.warnings, 'INVEST with Expected ROI 12M <= 0', result.negativeRoi12mInvest);
  _pushValidationWarningRefresh_(result.warnings, 'INVEST with HIGH risk', result.highRiskInvest);
  _pushValidationWarningRefresh_(result.warnings, 'REDUCE/SELL/AVOID with Final Action INVEST', result.reduceSellInvest);
  if (strict.enabled) {
    _pushValidationWarningRefresh_(result.warnings, 'STRICT: INVEST with low Data Quality Score < ' + strict.dqMin, result.lowDataQualityInvest);
    _pushValidationWarningRefresh_(result.warnings, 'STRICT: INVEST with low Forecast Reliability Score < ' + strict.reliabilityMin, result.lowForecastReliabilityInvest);
    _pushValidationWarningRefresh_(result.warnings, 'STRICT: INVEST with Provider/Engine Conflict = TRUE', result.providerConflictInvest);
    _pushValidationWarningRefresh_(result.warnings, 'STRICT: INVEST with nonblank Block Reason', result.blockReasonInvest);
  }
  result.strictGate = strict;
  result.ok = result.warnings.length === 0;
  return result;
}
function validateGlobalMarketsRefreshCompleteness_(pageName, sheet, expectedSymbolCount) {
  var page = getPageConfig(pageName);
  var result = {
    ok: true,
    pageName: page.pageName,
    skipped: page.pageName !== SHEET_NAMES.GLOBAL_MARKETS,
    rowCount: 0,
    expectedSymbolCount: expectedSymbolCount || 0,
    missingTimestampRows: 0,
    distinctRiyadhDates: [],
    warnings: []
  };
  if (page.pageName !== SHEET_NAMES.GLOBAL_MARKETS) {
    return result;
  }
  sheet = sheet || ensureSheetExists_(page.pageName);
  var headers = readCurrentSheetHeaders_(sheet);
  if (!headers.length || sheet.getLastRow() < 2) {
    result.ok = false;
    result.warnings.push('Global_Markets has no data rows after refresh.');
    return result;
  }
  var idx = _buildHeaderIndexRefresh_(headers);
  var symbolIdx = idx['Symbol'];
  var riyadhIdx = idx['Last Updated (Riyadh)'];
  var lastRow = sheet.getLastRow();
  var values = sheet.getRange(2, 1, lastRow - 1, Math.max(sheet.getLastColumn(), headers.length)).getValues();
  var symbolCount = 0;
  var missingTimestamp = 0;
  var dateCounts = {};
  values.forEach(function (row) {
    var symbol = symbolIdx !== undefined ? _cellStringRefresh_(row, symbolIdx) : '';
    if (!symbol) return;
    symbolCount++;
    if (riyadhIdx === undefined) {
      missingTimestamp++;
      return;
    }
    var ts = _cellStringRefresh_(row, riyadhIdx);
    if (!ts) {
      missingTimestamp++;
      return;
    }
    var day = ts.length >= 10 ? ts.substring(0, 10) : ts;
    dateCounts[day] = (dateCounts[day] || 0) + 1;
  });
  result.rowCount = symbolCount;
  result.missingTimestampRows = missingTimestamp;
  result.distinctRiyadhDates = Object.keys(dateCounts).sort();
  result.dateCounts = dateCounts;
  if (expectedSymbolCount && symbolCount < expectedSymbolCount) {
    result.warnings.push('Global_Markets row count lower than requested symbols: rows=' + symbolCount + ', requested=' + expectedSymbolCount + '.');
  }
  if (missingTimestamp > 0) {
    result.warnings.push('Global_Markets rows missing Last Updated (Riyadh): ' + missingTimestamp + '.');
  }
  if (result.distinctRiyadhDates.length > 1) {
    result.warnings.push('Global_Markets has mixed refresh dates: ' + JSON.stringify(dateCounts) + '. Complete batch refresh until all rows are current.');
  }
  result.ok = result.warnings.length === 0;
  return result;
}
/**
 * v1.11.4 — Top_10 integrity validator, realigned to the DEPLOYED contract
 * (engine v5.84.0 Fix T conservative floor + selector v4.17.0 Fix U/V).
 *
 * WHY (carried from the v1.11.4 header block):
 *   The previous version required every Top_10 row to be Final Action=INVEST /
 *   Investability Status=INVESTABLE and to pass strict DQ/reliability minimums
 *   unconditionally. Under the deployed backend that is WRONG by design: the
 *   selector ranks the best candidates and the gate labels them honestly, so a
 *   fully-WATCHLIST Top_10 (verified live 2026-06-10) is correct output. The
 *   old checks flagged every legitimate row on every refresh — guaranteed
 *   false alarms that bury real problems.
 *
 * WHAT THIS VALIDATOR GUARDS NOW:
 *   1. STALENESS / COMPLETENESS (original purpose, retained): rank, price and
 *      the four core gate fields must be populated. A blank Investability
 *      Status / Final Action / Data Quality Score / Forecast Reliability Score
 *      means a stale pre-gate row survived the refresh.
 *   2. COHERENCE: Investability Status must pair with Final Action
 *      (INVESTABLE<->INVEST, WATCHLIST<->WATCH, BLOCKED<->DO_NOT_INVEST) and
 *      WATCHLIST/BLOCKED rows must carry a Block Reason. A mismatch means the
 *      row mixes values from two different runs (the original C1 defect class).
 *   3. DUPLICATE RANKS: two rows claiming the same Top 10 Rank.
 *   4. STRICT QUALITY BAR — INVEST ROWS ONLY: HIGH risk, ROI<=0, DQ/reliability
 *      below minimums, Provider/Engine Conflict TRUE, or a nonblank Block
 *      Reason on an INVEST row are genuine contradictions and are flagged.
 *      On WATCH/DO_NOT_INVEST rows these conditions are the EXPECTED,
 *      already-accounted-for reasons the gate withheld INVEST — not defects.
 */
function validateTop10RefreshIntegrity_(pageName, sheet) {
  var page = getPageConfig(pageName);
  var result = {
    ok: true,
    pageName: page.pageName,
    skipped: page.pageName !== SHEET_NAMES.TOP_10_INVESTMENTS,
    rowCount: 0,
    invalidRows: [],
    duplicateRanks: [],
    warnings: []
  };
  if (page.pageName !== SHEET_NAMES.TOP_10_INVESTMENTS) {
    return result;
  }
  sheet = sheet || ensureSheetExists_(page.pageName);
  var widthValidation = validateExpectedRefreshWidth_(page.pageName, sheet);
  if (!widthValidation.ok) {
    result.warnings = result.warnings.concat(widthValidation.warnings);
  }
  var headers = readCurrentSheetHeaders_(sheet);
  if (!headers.length || sheet.getLastRow() < 2) {
    result.ok = false;
    result.warnings.push('Top_10_Investments has no refreshed rows.');
    return result;
  }
  var idx = _buildHeaderIndexRefresh_(headers);
  var required = [
    'Top 10 Rank',
    'Symbol',
    'Current Price',
    'Risk Bucket',
    'Expected ROI 3M',
    'Expected ROI 12M',
    'Data Quality Score',
    'Forecast Reliability Score',
    'Provider/Engine Conflict',
    'Investability Status',
    'Final Action',
    'Block Reason'
  ];
  var missing = required.filter(function (h) {
    return idx[h] === undefined;
  });
  if (missing.length) {
    result.ok = false;
    result.warnings.push('Top10 missing required columns: ' + missing.join(', '));
    result.missingRequired = missing;
    return result;
  }
  var strict = _getStrictGateSettingsRefresh_();
  var dqMin = strict.dqMin;
  var relMin = strict.reliabilityMin;
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(sheet.getLastColumn(), headers.length)).getValues();
  var seenRanks = {};
  for (var r = 0; r < values.length; r++) {
    var row = values[r];
    var rowNum = r + 2;
    var symbol = _cellStringRefresh_(row, idx['Symbol']);
    var rank = _cellStringRefresh_(row, idx['Top 10 Rank']);
    if (!symbol && !rank) continue;
    result.rowCount++;
    var reasons = [];
    var finalAction = _cellStringRefresh_(row, idx['Final Action']).toUpperCase();
    var investability = _cellStringRefresh_(row, idx['Investability Status']).toUpperCase();
    var risk = _cellStringRefresh_(row, idx['Risk Bucket']).toUpperCase();
    var price = row[idx['Current Price']];
    var roi3m = _toNumberRefresh_(row[idx['Expected ROI 3M']], null);
    var roi12m = _toNumberRefresh_(row[idx['Expected ROI 12M']], null);
    var dq = _toNumberRefresh_(row[idx['Data Quality Score']], null);
    var rel = _toNumberRefresh_(row[idx['Forecast Reliability Score']], null);
    var conflict = _toBooleanRefresh_(row[idx['Provider/Engine Conflict']], false);
    var blockReason = _cellStringRefresh_(row, idx['Block Reason']);
    // 1. Staleness / completeness — blank gate fields mean a stale pre-gate
    //    row survived the refresh (the original purpose of this validator).
    if (!rank) {
      reasons.push('blank Top 10 Rank');
    } else if (seenRanks[rank]) {
      result.duplicateRanks.push(rank);
    } else {
      seenRanks[rank] = true;
    }
    if (_isBlankRefresh_(price)) reasons.push('blank Current Price');
    if (!investability) reasons.push('blank Investability Status');
    if (!finalAction) reasons.push('blank Final Action');
    if (dq === null) reasons.push('blank Data Quality Score');
    if (rel === null) reasons.push('blank Forecast Reliability Score');
    // 2. Status/action coherence — a mismatch means the row mixes values from
    //    two different runs (the stale-merge defect class Fix U/V closed).
    if (investability && finalAction) {
      var pairOk =
        (investability === 'INVESTABLE' && finalAction === 'INVEST') ||
        (investability === 'WATCHLIST' && finalAction === 'WATCH') ||
        (investability === 'BLOCKED' && finalAction === 'DO_NOT_INVEST');
      if (!pairOk) {
        reasons.push('Investability/Final Action mismatch: ' + investability + '/' + finalAction);
      }
    }
    if ((investability === 'WATCHLIST' || investability === 'BLOCKED') && !blockReason) {
      reasons.push(investability + ' with blank Block Reason');
    }
    // 3. Strict quality bar — INVEST rows ONLY. These conditions on an INVEST
    //    row are genuine contradictions; on WATCH/DO_NOT_INVEST rows they are
    //    the expected reasons the gate withheld INVEST.
    if (finalAction === 'INVEST') {
      if (risk === 'HIGH') reasons.push('INVEST with HIGH Risk Bucket');
      if (roi3m !== null && roi3m <= 0) reasons.push('INVEST with Expected ROI 3M <= 0');
      if (roi12m !== null && roi12m <= 0) reasons.push('INVEST with Expected ROI 12M <= 0');
      if (dq !== null && dq < dqMin) reasons.push('INVEST with Data Quality Score < ' + dqMin);
      if (rel !== null && rel < relMin) reasons.push('INVEST with Forecast Reliability Score < ' + relMin);
      if (conflict) reasons.push('INVEST with Provider/Engine Conflict TRUE');
      if (blockReason) reasons.push('INVEST with nonblank Block Reason');
    }
    if (reasons.length) {
      result.invalidRows.push({
        row: rowNum,
        symbol: symbol,
        reasons: reasons
      });
    }
  }
  if (result.duplicateRanks.length) {
    result.warnings.push('Top10 duplicate ranks: ' + uniqueStrings_(result.duplicateRanks).join(', '));
  }
  if (result.invalidRows.length) {
    result.warnings.push(
      'Top10 integrity issues: ' +
      result.invalidRows.slice(0, 10).map(function (x) {
        return x.symbol + '@R' + x.row + ' [' + x.reasons.join('; ') + ']';
      }).join(' | ') +
      (result.invalidRows.length > 10 ? ' ...(+' + (result.invalidRows.length - 10) + ')' : '')
    );
  }
  result.ok = result.warnings.length === 0;
  return result;
}
function validatePostRefreshIntegrity_(pageName, sheet, context) {
  context = context || {};
  var page = getPageConfig(pageName);
  sheet = sheet || ensureSheetExists_(page.pageName);
  var validations = [];
  var widthValidation = validateExpectedRefreshWidth_(page.pageName, sheet);
  validations.push(widthValidation);
  if (_isInvestmentPageForRefresh_(page.pageName)) {
    validations.push(validateInvestmentGateAfterRefresh_(page.pageName, sheet));
  }
  if (page.pageName === SHEET_NAMES.GLOBAL_MARKETS) {
    validations.push(validateGlobalMarketsRefreshCompleteness_(page.pageName, sheet, context.expectedSymbolCount || 0));
  }
  if (page.pageName === SHEET_NAMES.TOP_10_INVESTMENTS) {
    validations.push(validateTop10RefreshIntegrity_(page.pageName, sheet));
  }
  var warnings = [];
  var ok = true;
  validations.forEach(function (v) {
    if (!v) return;
    if (v.ok === false) ok = false;
    if (Array.isArray(v.warnings) && v.warnings.length) {
      warnings = warnings.concat(v.warnings);
    }
  });
  warnings = uniqueStrings_(warnings);
  try {
    Logger.log('[v1.11.2 POST_REFRESH_VALIDATION][' + page.pageName + '] ' +
      'ok=' + ok +
      ' warnings=' + warnings.length +
      ' expectedWidth=' + widthValidation.expected +
      ' actualWidth=' + widthValidation.actual);
  } catch (e) {}
  return {
    ok: ok,
    pageName: page.pageName,
    warnings: warnings,
    validations: validations
  };
}
function _pushValidationWarningRefresh_(warnings, label, items) {
  if (!items || !items.length) return;
  warnings.push(label + ': ' + items.slice(0, 15).join(', ') + (items.length > 15 ? ' ...(+' + (items.length - 15) + ')' : ''));
}
/* =========================================================
 * Header drift detection
 * ========================================================= */
function _detectMissingCanonicalHeaders_(sheet, pageName) {
  if (!sheet || !pageName) return [];
  var expectedHeaders;
  try {
    expectedHeaders = getExpectedHeaders_(pageName) || [];
  } catch (e) {
    return [];
  }
  if (!expectedHeaders.length) return [];
  var actualHeaders;
  try {
    actualHeaders = readCurrentSheetHeaders_(sheet) || [];
  } catch (e2) {
    return [];
  }
  var actualSet = {};
  for (var i = 0; i < actualHeaders.length; i++) {
    var raw = actualHeaders[i];
    if (raw === null || raw === undefined) continue;
    var normalized = normalizeHeaderTextRefresh_(raw).toLowerCase();
    if (normalized) actualSet[normalized] = true;
  }
  var missing = [];
  for (var j = 0; j < expectedHeaders.length; j++) {
    var canonical = expectedHeaders[j];
    if (!canonical) continue;
    var canonicalNormalized = normalizeHeaderTextRefresh_(canonical).toLowerCase();
    if (!canonicalNormalized) continue;
    if (!actualSet[canonicalNormalized]) {
      missing.push(String(canonical));
    }
  }
  return missing;
}
function _logCanonicalHeaderDrift_(pageName, missing) {
  if (!missing || !missing.length) return;
  try {
    var preview = missing.slice(0, 10).join(',');
    var truncated = missing.length > 10 ? ',...(+' + (missing.length - 10) + ')' : '';
    Logger.log('[v1.9.2 HEADER_DRIFT][' + (pageName || '<unknown>') + '] ' +
      'missing=' + missing.length + ' headers=[' + preview + truncated + ']');
  } catch (e) {}
}
function _logCanonicalHeaderRepair_(pageName, inserted, skipped) {
  try {
    var preview = (inserted || []).slice(0, 10).map(function (it) {
      return (it && it.header) ? (it.header + '@' + it.col) : '?';
    }).join(',');
    var truncated = (inserted && inserted.length > 10) ? ',...(+' + (inserted.length - 10) + ')' : '';
    Logger.log('[v1.9.2 HEADER_REPAIR][' + (pageName || '<unknown>') + '] ' +
      'inserted=' + ((inserted && inserted.length) || 0) + ' ' +
      'skipped=' + (skipped || 0) + ' cols=[' + preview + truncated + ']');
  } catch (e) {}
}
/* =========================================================
 * Diagnostic logging helpers
 * ========================================================= */
function _logBatchPathChoice_(pageName, choice, reason) {
  try {
    Logger.log('[v1.9.2 PATH][' + (pageName || '<unknown>') + '] ' +
      (choice || '<unknown>') + ' ' + (reason || ''));
  } catch (e) {}
}
function _logActualWriterPath_(pageName, writer, ctx) {
  try {
    Logger.log('[v1.9.2 WRITER][' + (pageName || '<unknown>') + '] ' +
      (writer || '<unknown>') + ' ' + (ctx || ''));
  } catch (e) {}
}
function _logEmptyRowObjectsWarning_(pageName, batchSize, chunk) {
  try {
    Logger.log('[v1.9.2 WRITER][' + (pageName || '<unknown>') + '] ' +
      'EMPTY_ROW_OBJECTS_IN_HEADER_MAPPED_BATCH batchSize=' + batchSize +
      ' chunkSymbols=' + (Array.isArray(chunk) ? chunk.join(',') : ''));
  } catch (e) {}
}
function _logProvenanceInjection_(pageName, summary) {
  try {
    Logger.log('[v1.9.2 PROVENANCE][' + (pageName || '<unknown>') + '] ' +
      'INJECTED rows=' + (summary.totalRows || 0) + ' ' +
      'filled_utc=' + (summary.filledUtc || 0) + ' ' +
      'filled_riyadh=' + (summary.filledRiyadh || 0) + ' ' +
      'filled_provider=' + (summary.filledProvider || 0) + ' ' +
      'provider=' + (summary.providerTag || '<empty>'));
  } catch (e) {}
}
function _logCanonicalWidth_(pageName, expected, actual) {
  try {
    var verdict = (expected === actual) ? 'OK' : (actual > expected ? 'OVERSHOOT' : 'UNDERSHOOT');
    Logger.log('[v1.9.2 CANONICAL_WIDTH][' + (pageName || '<unknown>') + '] ' +
      verdict + ' expected=' + expected + ' actual=' + actual);
  } catch (e) {}
}
function _logResidueHunt_(pageName, removedColumns) {
  if (!removedColumns || !removedColumns.length) return;
  try {
    Logger.log('[v1.9.2 RESIDUE_HUNT][' + (pageName || '<unknown>') + '] ' +
      'removed=' + removedColumns.length + ' cols=[' + removedColumns.join(',') + ']');
  } catch (e) {}
}
function _logResidueHuntData_(pageName, removedCells) {
  if (!removedCells || !removedCells.length) return;
  try {
    var preview = removedCells.slice(0, 20).join(',');
    var truncated = removedCells.length > 20 ? ',...' : '';
    Logger.log('[v1.9.2 RESIDUE_HUNT_DATA][' + (pageName || '<unknown>') + '] ' +
      'cleared=' + removedCells.length + ' cells=[' + preview + truncated + ']');
  } catch (e) {}
}
/* =========================================================
 * Status panel placement and cleanup
 * ========================================================= */
function _normalizeStatusPanelStartCol_(sheet, headersLength, pageName) {
  var minByHeaders = (headersLength || 0) + 2;
  var minByPageConfig = 0;
  if (pageName) {
    try {
      var page = getPageConfig(pageName);
      if (page && page.expectedColumnCount) {
        minByPageConfig = page.expectedColumnCount + REFRESH_STATUS_PANEL_BUFFER_COLS_;
      }
    } catch (e) {
      minByPageConfig = 0;
    }
  }
  return Math.max(minByHeaders, minByPageConfig, 1);
}
function _clearStalePanelHeaderResidue_(sheet, pageName) {
  var removed = [];
  try {
    if (!sheet) return removed;
    var expectedColumnCount = _findExpectedColumnCountForPage_(pageName);
    if (!expectedColumnCount) return removed;
    var maxCol = sheet.getMaxColumns();
    var maxRow = Math.min(REFRESH_STATUS_PANEL_SWEEP_ROWS_, sheet.getMaxRows());
    if (maxCol <= expectedColumnCount) return removed;
    var scanStartCol = expectedColumnCount + 1;
    var scanEndCol = Math.min(expectedColumnCount + REFRESH_PANEL_RESIDUE_SCAN_WIDTH_, maxCol);
    var scanWidth = scanEndCol - scanStartCol + 1;
    if (scanWidth <= 0) return removed;
    var row1Values = sheet.getRange(1, scanStartCol, 1, scanWidth).getValues()[0];
    var tokenSet = {};
    REFRESH_PANEL_LABEL_TOKENS_.forEach(function (t) {
      tokenSet[t] = true;
    });
    for (var i = 0; i < row1Values.length; i++) {
      var raw = row1Values[i];
      if (raw === null || raw === undefined) continue;
      var asString = String(raw).trim();
      if (!asString) continue;
      if (tokenSet[asString]) {
        removed.push(scanStartCol + i);
      }
    }
    if (!removed.length) return removed;
    removed.forEach(function (col) {
      var clearWidth = Math.min(2, maxCol - col + 1);
      if (clearWidth < 1) return;
      try {
        sheet.getRange(1, col, maxRow, clearWidth).clearContent();
      } catch (clearErr) {}
    });
    _logResidueHunt_(pageName, removed);
  } catch (e) {
    try {
      Logger.log('[v1.9.2 RESIDUE_HUNT][' + (pageName || '<unknown>') + '] HUNT_THREW error=' +
        ((e && e.message) ? e.message : String(e)));
    } catch (logErr) {}
  }
  return removed;
}
function _clearPanelTokensInsideDataArea_(sheet, pageName) {
  var cleared = [];
  try {
    if (!sheet) return cleared;
    var expectedColumnCount = _findExpectedColumnCountForPage_(pageName);
    if (!expectedColumnCount) return cleared;
    var lastRow = sheet.getLastRow();
    if (lastRow < 2) return cleared;
    var lastCol = sheet.getLastColumn();
    if (lastCol < 1) return cleared;
    var scanLastCol = Math.min(expectedColumnCount, lastCol);
    if (scanLastCol < 1) return cleared;
    var tokenSet = {};
    REFRESH_PANEL_LABEL_TOKENS_.forEach(function (t) {
      tokenSet[t] = true;
    });
    var headerValues = sheet.getRange(1, 1, 1, scanLastCol).getValues()[0];
    var skipCols = {};
    for (var hc = 0; hc < headerValues.length; hc++) {
      var hvStr = String(headerValues[hc] || '').trim();
      if (tokenSet[hvStr]) {
        skipCols[hc + 1] = true;
      }
    }
    var dataRowCount = lastRow - 1;
    var dataValues = sheet.getRange(2, 1, dataRowCount, scanLastCol).getValues();
    var toClear = [];
    for (var r = 0; r < dataValues.length; r++) {
      for (var c = 0; c < scanLastCol; c++) {
        var colNum = c + 1;
        if (skipCols[colNum]) continue;
        var cellVal = dataValues[r][c];
        if (typeof cellVal !== 'string') continue;
        var cellStr = cellVal.trim();
        if (!cellStr) continue;
        if (tokenSet[cellStr]) {
          var rowNum = r + 2;
          toClear.push({ row: rowNum, col: colNum });
          cleared.push(rowNum + ':' + colNum);
        }
      }
    }
    if (!toClear.length) return cleared;
    toClear.forEach(function (x) {
      try {
        sheet.getRange(x.row, x.col).clearContent();
      } catch (clearErr) {}
    });
    _logResidueHuntData_(pageName, cleared);
  } catch (e) {
    try {
      Logger.log('[v1.9.2 RESIDUE_HUNT_DATA][' + (pageName || '<unknown>') + '] HUNT_THREW error=' +
        ((e && e.message) ? e.message : String(e)));
    } catch (logErr) {}
  }
  return cleared;
}
function _clearStatusPanelZone_(sheet, headersLength, pageName) {
  try {
    if (!sheet || typeof sheet.getMaxColumns !== 'function') return;
    var expectedColumnCount = _findExpectedColumnCountForPage_(pageName);
    var clearStartCol = expectedColumnCount > 0
      ? expectedColumnCount + 1
      : ((headersLength || 0) + 1);
    if (clearStartCol < 1) clearStartCol = 1;
    var maxCol = sheet.getMaxColumns();
    var maxRow = Math.min(REFRESH_STATUS_PANEL_SWEEP_ROWS_, sheet.getMaxRows());
    if (maxCol >= clearStartCol) {
      var clearWidth = maxCol - clearStartCol + 1;
      if (clearWidth > 0 && maxRow > 0) {
        var totalCells = clearWidth * maxRow;
        if (totalCells <= 20000) {
          try {
            sheet.getRange(1, clearStartCol, maxRow, clearWidth).clearContent();
          } catch (e1) {}
        }
      }
    }
    _clearStalePanelHeaderResidue_(sheet, pageName);
    _clearPanelTokensInsideDataArea_(sheet, pageName);
    if (expectedColumnCount > 0) {
      trimSheetToExpectedColumns_(pageName, {
        sheet: sheet,
        reason: 'status_panel_cleanup',
        silent: true
      });
    }
  } catch (e) {}
}
function trimSheetToExpectedColumns_(pageName, options) {
  options = options || {};
  var page;
  try {
    page = getPageConfig(pageName);
  } catch (e) {
    return {
      ok: false,
      pageName: pageName || '',
      message: 'Unknown page: ' + (pageName || ''),
      deletedColumns: 0
    };
  }
  /* v1.12.0: the 118-column width contract is meaningless for the cockpit;
   * trimming/clearing beyond "expected" width must never run on it. Silent
   * skip (no logRun) — this is called from hot internal cleanup paths. */
  if (_refreshIsDecisionOwnedPage_(page.pageName)) {
    return {
      ok: true,
      pageName: page.pageName,
      skipped: true,
      action: 'skipped_decision_owned',
      reason: 'decision_owned_page',
      deletedColumns: 0
    };
  }
  var sheet = options.sheet || ensureSheetExists_(page.pageName);
  var expected = Number(page.expectedColumnCount || 0);
  if (!expected || expected < 1) {
    return {
      ok: true,
      pageName: page.pageName,
      skipped: true,
      reason: 'no_expected_width',
      deletedColumns: 0
    };
  }
  var lastCol = sheet.getLastColumn();
  var maxCol = sheet.getMaxColumns();
  var before = {
    lastColumn: lastCol,
    maxColumns: maxCol
  };
  try {
    if (maxCol > expected) {
      var extraWidth = maxCol - expected;
      var sweepRows = Math.min(REFRESH_STATUS_PANEL_SWEEP_ROWS_, sheet.getMaxRows());
      if (sweepRows > 0 && extraWidth > 0) {
        try {
          sheet.getRange(1, expected + 1, sweepRows, extraWidth).clearContent();
        } catch (clearErr) {}
      }
      try {
        sheet.deleteColumns(expected + 1, extraWidth);
      } catch (deleteErr) {
        return {
          ok: false,
          pageName: page.pageName,
          expected: expected,
          before: before,
          after: {
            lastColumn: sheet.getLastColumn(),
            maxColumns: sheet.getMaxColumns()
          },
          deletedColumns: 0,
          message: 'Could not delete extra columns: ' + (deleteErr && deleteErr.message ? deleteErr.message : String(deleteErr))
        };
      }
    }
  } catch (e2) {
    return {
      ok: false,
      pageName: page.pageName,
      expected: expected,
      before: before,
      after: {
        lastColumn: sheet.getLastColumn(),
        maxColumns: sheet.getMaxColumns()
      },
      deletedColumns: 0,
      message: 'Trim failed: ' + (e2 && e2.message ? e2.message : String(e2))
    };
  }
  var afterLastCol = sheet.getLastColumn();
  var afterMaxCol = sheet.getMaxColumns();
  var deleted = Math.max(0, before.maxColumns - afterMaxCol);
  if (!options.silent) {
    logRun_(
      'INFO',
      'trimSheetToExpectedColumns',
      page.pageName,
      deleted ? 'Trimmed sheet to expected schema width.' : 'No extra physical columns to trim.',
      {
        status: 'OK',
        expected: expected,
        beforeLastColumn: before.lastColumn,
        beforeMaxColumns: before.maxColumns,
        afterLastColumn: afterLastCol,
        afterMaxColumns: afterMaxCol,
        deletedColumns: deleted,
        reason: options.reason || ''
      }
    );
  } else if (deleted) {
    try {
      Logger.log('[v1.11.2 WIDTH_TRIM][' + page.pageName + '] deletedColumns=' + deleted +
        ' expected=' + expected +
        ' beforeMax=' + before.maxColumns +
        ' afterMax=' + afterMaxCol +
        ' reason=' + (options.reason || ''));
    } catch (logErr) {}
  }
  return {
    ok: true,
    pageName: page.pageName,
    expected: expected,
    before: before,
    after: {
      lastColumn: afterLastCol,
      maxColumns: afterMaxCol
    },
    deletedColumns: deleted
  };
}
function trimAllSheetsToExpectedColumns_() {
  return withScriptLock_(function () {
    var pages = getSupportedPages();
    var results = [];
    var ok = true;
    for (var i = 0; i < pages.length; i++) {
      var result = trimSheetToExpectedColumns_(pages[i], {
        reason: 'manual_trim_all',
        silent: false
      });
      if (!result.ok) ok = false;
      results.push(result);
    }
    return {
      ok: ok,
      action: 'trimAllSheetsToExpectedColumns',
      results: results
    };
  });
}
function trimCurrentSheetToExpectedColumns_() {
  var pageName = getActivePageName_();
  return trimSheetToExpectedColumns_(pageName, {
    reason: 'manual_trim_current',
    silent: false
  });
}
function trimAllSheetsToExpectedColumns() {
  return trimAllSheetsToExpectedColumns_();
}
function trimCurrentSheetToExpectedColumns() {
  return trimCurrentSheetToExpectedColumns_();
}
/* =========================================================
 * Provenance injection
 * ========================================================= */
function _inferProviderFromEndpoint_(endpointUrl) {
  if (!endpointUrl) return '';
  var u = String(endpointUrl).toLowerCase();
  if (u.indexOf('argaam') > -1) return 'argaam';
  if (u.indexOf('eodhd') > -1) return 'eodhd';
  if (u.indexOf('finnhub') > -1) return 'finnhub';
  if (u.indexOf('yahoo_chart') > -1) return 'yahoo_chart';
  if (u.indexOf('yahoo') > -1) return 'yahoo_chart';
  if (u.indexOf('tadawul-fast-bridge') > -1) return 'tadawul-fast-bridge';
  if (u.indexOf('onrender.com') > -1) return 'tadawul-fast-bridge';
  if (u.indexOf('tadawul') > -1) return 'tadawul';
  return 'tfb-api';
}
function _utcTimestampNow_() {
  try {
    return Utilities.formatDate(new Date(), 'UTC', 'yyyy-MM-dd HH:mm:ss');
  } catch (e) {
    return new Date().toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  }
}
function _riyadhTimestampNow_() {
  if (typeof todayRiyadhString_ === 'function') {
    try {
      return todayRiyadhString_();
    } catch (e) {}
  }
  try {
    return Utilities.formatDate(new Date(), 'Asia/Riyadh', 'yyyy-MM-dd HH:mm:ss');
  } catch (e2) {
    return _utcTimestampNow_();
  }
}
/* v1.12.6: true when the backend tagged this row's price as not verified
 * live this run (data_engine_v2 v5.103.0 Fix AQ). Tolerates warnings as a
 * string or an array; never throws. */
function _provenanceIsUnverifiedPrice_(rowObj) {
  try {
    if (!rowObj) return false;
    var w = rowObj.warnings;
    if (w === null || w === undefined) return false;
    if (Object.prototype.toString.call(w) === '[object Array]') {
      for (var i = 0; i < w.length; i++) {
        if (String(w[i]).indexOf('price_unverified_live') >= 0) return true;
      }
      return false;
    }
    return String(w).indexOf('price_unverified_live') >= 0;
  } catch (e) {
    return false;
  }
}
function _isProvenanceFieldEmpty_(val) {
  if (val === null || val === undefined) return true;
  if (typeof val === 'string' && val.trim() === '') return true;
  return false;
}
function _injectProvenanceFields_(rowObjects, fetchResult, pageName) {
  var summary = {
    totalRows: 0,
    filledUtc: 0,
    filledRiyadh: 0,
    filledProvider: 0,
    providerTag: ''
  };
  try {
    if (!Array.isArray(rowObjects) || !rowObjects.length) return summary;
    var nowUtc = _utcTimestampNow_();
    var nowRiyadh = _riyadhTimestampNow_();
    var endpointUsed = (fetchResult && fetchResult.endpointUsed) || '';
    var providerTag = _inferProviderFromEndpoint_(endpointUsed);
    summary.providerTag = providerTag;
    summary.totalRows = rowObjects.length;
    var unverifiedSkipped = 0;
    for (var i = 0; i < rowObjects.length; i++) {
      var ro = rowObjects[i];
      if (!ro || typeof ro !== 'object') continue;
      // v1.12.6: the backend declared this row's price NOT verified live and
      // shipped honest (old/blank) stamps on purpose — do not re-mint NOW.
      var unverifiedPrice = _provenanceIsUnverifiedPrice_(ro);
      if (unverifiedPrice) {
        unverifiedSkipped++;
      }
      if (!unverifiedPrice && _isProvenanceFieldEmpty_(ro.last_updated_utc)) {
        ro.last_updated_utc = nowUtc;
        summary.filledUtc++;
      }
      if (!unverifiedPrice && _isProvenanceFieldEmpty_(ro.last_updated_riyadh)) {
        ro.last_updated_riyadh = nowRiyadh;
        summary.filledRiyadh++;
      }
      if (_isProvenanceFieldEmpty_(ro.data_provider) && providerTag) {
        ro.data_provider = providerTag;
        summary.filledProvider++;
      }
    }
    if (unverifiedSkipped > 0) {
      try {
        Logger.log('[v1.12.6 PROVENANCE_HONESTY][' + (pageName || '<unknown>') + '] left ' +
          unverifiedSkipped + ' price_unverified_live row(s) with their honest backend timestamps (no NOW re-mint).');
      } catch (phLogErr) {}
    }
    _logProvenanceInjection_(pageName, summary);
  } catch (e) {
    try {
      Logger.log('[v1.9.2 PROVENANCE][' + (pageName || '<unknown>') + '] INJECT_THREW error=' +
        ((e && e.message) ? e.message : String(e)));
    } catch (logErr) {}
  }
  return summary;
}
/* =========================================================
 * Critical field verification
 * ========================================================= */
function _getCriticalViewFieldsForPage_(sheet, pageName) {
  var layoutA = [
    'Fundamental View',
    'Technical View',
    'Risk View',
    'Value View',
    'Recommendation'
  ];
  var canonical = [
    'Sector-Adj Score',
    'Conviction Score',
    'Top Factors',
    'Top Risks',
    'Position Size Hint',
    'Recommendation Detail',
    'Reco Priority',
    'Data Quality Score',
    'Forecast Reliability Score',
    'Investability Status',
    'Final Action'
  ];
  var headers;
  try {
    headers = readCurrentSheetHeaders_(sheet);
  } catch (e) {
    return [];
  }
  if (!Array.isArray(headers) || !headers.length) return [];
  var headerSet = {};
  for (var i = 0; i < headers.length; i++) {
    var h = String(headers[i] == null ? '' : headers[i]).trim();
    if (h) headerSet[h] = true;
  }
  var present = [];
  var candidates = layoutA.concat(canonical);
  for (var c = 0; c < candidates.length; c++) {
    if (headerSet[candidates[c]]) {
      present.push(candidates[c]);
    }
  }
  return present;
}
function _verifyCriticalFieldsAfterWrite_(sheet, pageName, rowObjectsWritten) {
  try {
    var fields = _getCriticalViewFieldsForPage_(sheet, pageName);
    if (!fields.length) {
      return { ok: true, skipped: true, reason: 'no_critical_fields_on_sheet' };
    }
    if (!Array.isArray(rowObjectsWritten) || !rowObjectsWritten.length) {
      return { ok: true, skipped: true, reason: 'no_row_objects' };
    }
    var firstRowObj = rowObjectsWritten[0];
    var firstSymbol = '';
    if (firstRowObj) {
      firstSymbol = String(
        firstRowObj.symbol || firstRowObj.Symbol ||
        firstRowObj.ticker || firstRowObj.Ticker || ''
      ).trim().toUpperCase();
    }
    if (!firstSymbol) {
      return { ok: true, skipped: true, reason: 'no_symbol_on_first_row_obj' };
    }
    var headers = readCurrentSheetHeaders_(sheet);
    if (!Array.isArray(headers) || !headers.length) {
      return { ok: true, skipped: true, reason: 'no_headers' };
    }
    var symbolColIdx = findHeaderIndexRefresh_(headers, 'Symbol');
    if (symbolColIdx < 0) symbolColIdx = findHeaderIndexRefresh_(headers, 'Ticker');
    if (symbolColIdx < 0) {
      return { ok: true, skipped: true, reason: 'no_symbol_column' };
    }
    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    if (lastRow < 2 || lastCol < 1) {
      return { ok: true, skipped: true, reason: 'empty_sheet' };
    }
    /* v1.16.0 SLIM-READ: this function samples exactly ONE row. Locate it
     * via the symbol column, then read only that row (~6.6k cells vs the
     * v1.15.0 full-grid ~749k at expanded Global_Markets). Kill-switch OFF
     * runs the v1.15.0 full-grid path verbatim in the else-branch. */
    var rowValues = null;
    if (_refreshVerifySlimReadEnabled_()) {
      var symOnly = sheet.getRange(2, symbolColIdx + 1, lastRow - 1, 1).getValues();
      var slimOffset = -1;
      for (var si = 0; si < symOnly.length; si++) {
        var ss = String(symOnly[si][0] || '').trim().toUpperCase();
        if (ss === firstSymbol) {
          slimOffset = si;
          break;
        }
      }
      if (slimOffset < 0) {
        return { ok: true, skipped: true, reason: 'symbol_not_found_on_sheet' };
      }
      rowValues = sheet.getRange(2 + slimOffset, 1, 1, lastCol).getValues()[0];
    } else {
      var allValues = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
      var sheetRowOffset = -1;
      for (var i = 0; i < allValues.length; i++) {
        var rs = String(allValues[i][symbolColIdx] || '').trim().toUpperCase();
        if (rs === firstSymbol) {
          sheetRowOffset = i;
          break;
        }
      }
      if (sheetRowOffset < 0) {
        return { ok: true, skipped: true, reason: 'symbol_not_found_on_sheet' };
      }
      rowValues = allValues[sheetRowOffset];
    }
    var headerToKey = {};
    if (typeof getHeaderToKey === 'function') {
      try {
        headerToKey = getHeaderToKey(pageName) || {};
      } catch (htkErr) {
        headerToKey = {};
      }
    }
    var blanks = [];
    var rowObjMissing = [];
    for (var f = 0; f < fields.length; f++) {
      var fieldHeader = fields[f];
      var fieldIdx = findHeaderIndexRefresh_(headers, fieldHeader);
      if (fieldIdx < 0) continue;
      var sheetValue = rowValues[fieldIdx];
      var sheetBlank = _isBlankRefresh_(sheetValue);
      var apiKey = headerToKey[fieldHeader] || headerToKey[String(fieldHeader).trim()] || '';
      var rowObjValue = '';
      var rowObjHasField = false;
      if (apiKey && Object.prototype.hasOwnProperty.call(firstRowObj, apiKey)) {
        rowObjValue = firstRowObj[apiKey];
        rowObjHasField = true;
      }
      var rowObjBlank = !rowObjHasField || _isBlankRefresh_(rowObjValue);
      if (sheetBlank) {
        blanks.push({
          field: fieldHeader,
          apiKey: apiKey,
          rowObjBlank: rowObjBlank,
          rowObjHasField: rowObjHasField
        });
        if (rowObjBlank) rowObjMissing.push(fieldHeader);
      }
    }
    if (!blanks.length) {
      try {
        Logger.log('[v1.9.2 VERIFY][' + pageName + '] OK_ALL_POPULATED sampled=' + firstSymbol + ' fieldsChecked=' + fields.length);
      } catch (logErr) {}
      return {
        ok: true,
        sampled: firstSymbol,
        fieldsChecked: fields.length
      };
    }
    var diagnosis;
    if (rowObjMissing.length === blanks.length) {
      diagnosis = 'BACKEND_NOT_SENDING';
    } else if (rowObjMissing.length === 0) {
      diagnosis = 'WRITER_DROPPED';
    } else {
      diagnosis = 'MIXED';
    }
    try {
      var blankNames = blanks.map(function (b) {
        return b.field;
      }).join(',');
      Logger.log('[v1.9.2 VERIFY][' + pageName + '] BLANK_VIEW_FIELDS sampled=' +
        firstSymbol + ' blanks=' + blankNames + ' diagnosis=' + diagnosis);
    } catch (logErr2) {}
    return {
      ok: false,
      sampled: firstSymbol,
      blanks: blanks,
      diagnosis: diagnosis,
      fieldsChecked: fields.length
    };
  } catch (verifyErr) {
    try {
      Logger.log('[v1.9.2 VERIFY][' + pageName + '] VERIFY_THREW error=' +
        ((verifyErr && verifyErr.message) ? verifyErr.message : String(verifyErr)));
    } catch (logErr3) {}
    return {
      ok: true,
      skipped: true,
      reason: 'verify_threw'
    };
  }
}
/* =========================================================
 * v1.18.0 — Manual refresh priority / cooperative yield
 * ========================================================= */

/**
 * Read the Core manual-priority state without mutating it.
 *
 * Yield ONLY while phase='requested'. Once the manual owner acquires the
 * DocumentLock, 02_Core switches the phase to 'active'; the manual refresh is
 * then allowed to execute the same refresh functions without stopping itself.
 */
function _refreshManualPriorityProbe_(context) {
  var status = null;
  try {
    if (typeof getManualRefreshPriorityStatus_ === 'function') {
      status = getManualRefreshPriorityStatus_();
    } else if (typeof getManualRefreshPriorityStatus === 'function') {
      status = getManualRefreshPriorityStatus();
    }
  } catch (statusErr) {
    status = null;
  }

  if (!status || status.pending !== true) {
    // v1.18.2: no manual intent pending — this is the path every automatic
    // batch boundary takes on a normal day. Consult the backend write hold
    // here (coordinator v1.2.0), typeof-guarded and fail-open, so backend
    // and GAS writers stop overlapping on the same page.
    try {
      if (typeof tfbBackendHoldProbe_ === 'function') {
        var backendHold = tfbBackendHoldProbe_(String(context || ''));
        if (backendHold && backendHold.yield === true) {
          return backendHold;
        }
      }
    } catch (backendHoldErr) {
      // Fail-open: a broken hold probe must never stop refreshes.
    }
    return { yield: false, action: 'continue', context: String(context || '') };
  }

  var phase = String(status.phase || '').toLowerCase();
  if (phase !== 'requested') {
    return {
      yield: false,
      action: 'continue-manual-owner-active',
      context: String(context || ''),
      phase: phase || 'active'
    };
  }

  var probe = null;
  try {
    if (typeof tfbAutomaticYieldPoint_ === 'function') {
      probe = tfbAutomaticYieldPoint_(String(context || 'refresh'));
    }
  } catch (probeErr) {
    probe = null;
  }

  if (!probe || typeof probe !== 'object') probe = {};
  probe.yield = true;
  probe.action = 'yield-manual-priority';
  probe.reason = 'manual_refresh_requested';
  probe.context = String(context || '');
  probe.phase = 'requested';
  probe.label = String(status.label || probe.label || 'Manual Refresh');
  probe.requestedAt = String(status.requestedAt || probe.requestedAt || '');
  probe.expiresAt = String(status.expiresAt || probe.expiresAt || '');
  return probe;
}

function _refreshUniqueWarnings_(warnings) {
  var source = Array.isArray(warnings) ? warnings : [];
  try {
    if (typeof uniqueStrings_ === 'function') return uniqueStrings_(source);
  } catch (e) {}

  var out = [];
  var seen = {};
  for (var i = 0; i < source.length; i++) {
    var value = String(source[i] || '').trim();
    if (!value || seen[value]) continue;
    seen[value] = true;
    out.push(value);
  }
  return out;
}

/**
 * Return cleanly from an automatic/uncoordinated refresh at a safe boundary.
 * No checkpoint is cleared. Sheet writes already completed are flushed before
 * the caller's finally block releases the DocumentLock.
 */
function _refreshBackendHoldGateOn_() {
  /* v1.19.0: same master gate the coordinator honors. FAIL-OPEN=OFF. */
  try {
    if (typeof PropertiesService === 'undefined') return false;
    var v = PropertiesService.getScriptProperties()
      .getProperty('TFB_GAS_BACKEND_HOLD');
    return String(v || '').trim() === '1';
  } catch (gateErr) {
    return false;
  }
}

function _refreshBackendHoldSnapshot_() {
  /* v1.19.0: fingerprint the _Sync_Control backend-hold row (key matched
   * exactly like backend _sync_hold_find_row: lowercase, non-alphanumerics
   * stripped == 'backendsyncholduntil'). Returns {found,b,c}. Any error or
   * gate OFF -> {found:false,'',''} so the comparator sees no change. */
  var out = { found: false, b: '', c: '' };
  try {
    if (!_refreshBackendHoldGateOn_()) return out;
    var ss = SpreadsheetApp.getActive();
    var sh = ss ? ss.getSheetByName('_Sync_Control') : null;
    if (!sh) return out;
    var rows = Math.min(12, Math.max(1, sh.getLastRow()));
    var vals = sh.getRange(1, 1, rows, 3).getValues();
    for (var i = 0; i < vals.length; i++) {
      var key = String(vals[i][0] || '').toLowerCase()
        .replace(/[^a-z0-9]+/g, '');
      if (key === 'backendsyncholduntil') {
        out.found = true;
        out.b = String(vals[i][1] || '');
        out.c = String(vals[i][2] || '');
        break;
      }
    }
  } catch (snapErr) {}
  return out;
}

function _refreshBackendHoldPreWriteCheck_(snapBefore, context) {
  /* v1.19.0: decide, at the last safe instant before a chunk write,
   * whether the backend owned or touched the page while our fetch was in
   * flight. Detector (1): live hold via the coordinator probe. Detector
   * (2): hold-row activity - B or C differs from the fetch-start
   * snapshot (publish, clear, grace rewrite, row creation or blanking
   * all count). Returns {yield,reason,detail}. Every path FAIL-OPEN. */
  var out = { yield: false, reason: '', detail: '' };
  try {
    if (!_refreshBackendHoldGateOn_()) return out;
    try {
      if (typeof tfbBackendHoldProbe_ === 'function') {
        var live = tfbBackendHoldProbe_(String(context || ''));
        if (live && live.yield === true) {
          out.yield = true;
          out.reason = 'backend_hold_live';
          out.detail = String(live.reason || live.action || 'hold-live');
          return out;
        }
      }
    } catch (probeErr) {}
    var nowSnap = _refreshBackendHoldSnapshot_();
    var before = (snapBefore && typeof snapBefore === 'object')
      ? snapBefore
      : { found: false, b: '', c: '' };
    if (String(nowSnap.b) !== String(before.b) ||
        String(nowSnap.c) !== String(before.c)) {
      out.yield = true;
      out.reason = 'backend_hold_activity';
      out.detail = 'hold row changed during fetch (B ' +
        (String(nowSnap.b) === String(before.b) ? 'same' : 'changed') +
        ', C ' +
        (String(nowSnap.c) === String(before.c) ? 'same' : 'changed') + ')';
    }
  } catch (checkErr) {
    out.yield = false;
  }
  return out;
}

function _refreshPauseForManualPriority_(details) {
  details = details || {};
  var pageName = String(details.pageName || '');
  var checkpoint = details.checkpoint || null;
  var priority = details.priority || {
    yield: true,
    action: 'yield-manual-priority',
    reason: 'manual_refresh_requested'
  };
  var nextIndex = checkpoint && typeof checkpoint.nextIndex === 'number'
    ? checkpoint.nextIndex
    : Number(details.nextIndex || 0);
  var totalSymbols = checkpoint && typeof checkpoint.totalSymbols === 'number'
    ? checkpoint.totalSymbols
    : Number(details.totalSymbols || 0);
  var message = 'Automatic refresh yielded safely for a waiting manual refresh.';
  if (totalSymbols > 0) {
    message += ' Checkpoint kept at ' + nextIndex + ' / ' + totalSymbols + '.';
  }

  var warnings = _refreshUniqueWarnings_((details.warnings || []).concat([
    '[v1.18.0 MANUAL-PRIORITY] ' + message
  ]));

  // Make completed writes visible before the lock is released to the manual
  // owner. This is intentionally done only at a safe boundary.
  try {
    if (typeof SpreadsheetApp !== 'undefined' && SpreadsheetApp.flush) {
      SpreadsheetApp.flush();
    }
  } catch (flushErr) {}

  if (details.writeStatus !== false && details.sheet) {
    try {
      writePageStatusPanel_(details.sheet, {
        status: 'PARTIAL',
        message: message,
        warnings: warnings
      }, Math.max(readCurrentSheetHeaders_(details.sheet).length, 1), pageName);
    } catch (panelErr) {}

    try {
      writePageStatus_(pageName, {
        status: 'PARTIAL',
        message: message,
        endpoint: String(details.endpointUsed || ''),
        httpCode: '',
        rows: Math.max(0, details.sheet.getLastRow() - 1),
        columns: readCurrentSheetHeaders_(details.sheet).length,
        durationMs: Number(details.durationMs || 0),
        warnings: warnings
      });
    } catch (statusErr) {}
  }

  try {
    logRun_('INFO', 'refreshPageInBatches', pageName, message, {
      status: 'PARTIAL',
      reason: 'manual_refresh_requested',
      endpoint: String(details.endpointUsed || ''),
      durationMs: Number(details.durationMs || 0),
      nextIndex: nextIndex,
      totalSymbols: totalSymbols,
      successfulBatches: Number(details.successfulBatches || 0),
      failedBatches: Number(details.failedBatches || 0),
      warnings: warnings
    });
  } catch (logErr) {}

  return {
    ok: true,
    action: 'paused-manual-priority',
    reason: 'manual_refresh_requested',
    pageName: pageName,
    partial: true,
    paused: true,
    nextIndex: nextIndex,
    totalSymbols: totalSymbols,
    endpointUsed: String(details.endpointUsed || ''),
    successfulBatches: Number(details.successfulBatches || 0),
    failedBatches: Number(details.failedBatches || 0),
    verifyIssues: Number(details.verifyIssues || 0),
    warnings: warnings,
    canonicalDrift: Number(details.canonicalDrift || 0),
    priority: priority
  };
}

function _refreshPauseMultiPageForManualPriority_(actionName, pages, nextIndex, results, startedAt, priority) {
  try {
    if (typeof SpreadsheetApp !== 'undefined' && SpreadsheetApp.flush) {
      SpreadsheetApp.flush();
    }
  } catch (flushErr) {}

  var nextPage = pages && nextIndex < pages.length ? String(pages[nextIndex] || '') : '';
  var message = actionName + ' yielded between completed pages for a waiting manual refresh.';
  if (nextPage) message += ' Next page: ' + nextPage + '.';

  try {
    writeGlobalStatus_({
      healthStatus: 'REFRESH_PAUSED_MANUAL_PRIORITY',
      version: '',
      message: message
    });
  } catch (statusErr) {}

  try {
    logRun_('INFO', actionName, nextPage, message, {
      status: 'PARTIAL',
      reason: 'manual_refresh_requested',
      completedPages: results ? results.length : 0,
      totalPages: pages ? pages.length : 0,
      nextPageIndex: nextIndex,
      durationMs: new Date().getTime() - Number(startedAt || new Date().getTime())
    });
  } catch (logErr) {}

  return {
    ok: true,
    action: 'paused-manual-priority',
    reason: 'manual_refresh_requested',
    partial: true,
    paused: true,
    operation: actionName,
    results: results || [],
    completedPages: results ? results.length : 0,
    totalPages: pages ? pages.length : 0,
    nextPageIndex: nextIndex,
    nextPage: nextPage,
    durationMs: new Date().getTime() - Number(startedAt || new Date().getTime()),
    priority: priority || null
  };
}

/* =========================================================
 * Public entry points
 * ========================================================= */
function buildPage_(pageName) {
  return withScriptLock_(function () {
    var _r = buildPageCore_(pageName);
    _refreshRunMyPortfolioEconomics_(pageName);
    return _r;
  });
}
function refreshPage_(pageName, options) {
  options = options || {};
  return withScriptLock_(function () {
    var _r = refreshPageCore_(pageName, options);
    _refreshRunMyPortfolioEconomics_(pageName);
    return _r;
  });
}
function refreshAllPages_() {
  return withScriptLock_(function () {
    var pages = getSupportedPages();
    var results = [];
    var startedAt = new Date().getTime();
    for (var i = 0; i < pages.length; i++) {
      // v1.18.0: an aligned manual owner is phase='active' and proceeds.
      // Only an older/uncoordinated execution yields to a newer request.
      var manualPriority = _refreshManualPriorityProbe_(
        'refreshAllPages_:before-page:' + pages[i]
      );
      if (manualPriority.yield) {
        return _refreshPauseMultiPageForManualPriority_(
          'refreshAllPages', pages, i, results, startedAt, manualPriority
        );
      }

      try {
        results.push(refreshPageCore_(pages[i], {
          skipLock: true
        }));
        _refreshRunMyPortfolioEconomics_(pages[i]);
      } catch (err) {
        var message = getRefreshErrorMessage_(err);
        logRun_('ERROR', 'refreshAllPages', pages[i], message, {
          status: 'FAIL',
          endpoint: '',
          httpCode: '',
          durationMs: '',
          error: message
        });
        writePageStatus_(pages[i], {
          status: 'FAIL',
          message: message,
          endpoint: '',
          httpCode: '',
          rows: '',
          columns: '',
          durationMs: '',
          warnings: []
        });
        results.push({
          ok: false,
          pageName: pages[i],
          message: message
        });
      }
    }
    var endedAt = new Date().getTime();
    writeGlobalStatus_({
      healthStatus: 'REFRESH_ALL_DONE',
      version: '',
      message: 'Refresh all pages completed in ' + (endedAt - startedAt) + ' ms'
    });
    return {
      ok: true,
      action: 'refreshAllPages',
      results: results,
      durationMs: endedAt - startedAt
    };
  });
}
function refreshAnalysisAndInsights_() {
  return withScriptLock_(function () {
    var pages = [
      SHEET_NAMES.INSIGHTS_ANALYSIS,
      SHEET_NAMES.TOP_10_INVESTMENTS
    ];
    var results = [];
    var startedAt = new Date().getTime();
    for (var i = 0; i < pages.length; i++) {
      var manualPriority = _refreshManualPriorityProbe_(
        'refreshAnalysisAndInsights_:before-page:' + pages[i]
      );
      if (manualPriority.yield) {
        return _refreshPauseMultiPageForManualPriority_(
          'refreshAnalysisAndInsights', pages, i, results, startedAt, manualPriority
        );
      }
      results.push(refreshPageCore_(pages[i], {
        skipLock: true
      }));
    }
    return {
      ok: true,
      action: 'refreshAnalysisAndInsights',
      results: results,
      durationMs: new Date().getTime() - startedAt
    };
  });
}
function refreshSelectedRow_(pageName, options) {
  options = options || {};
  return withScriptLock_(function () {
    var _r = refreshSelectedRowCore_(pageName, options);
    _refreshRunMyPortfolioEconomics_(pageName);
    return _r;
  });
}
function refreshPageInBatches_(pageName, options) {
  options = options || {};
  return withScriptLock_(function () {
    var _r = refreshPageInBatchesCore_(pageName, options);
    _refreshRunMyPortfolioEconomics_(pageName);
    return _r;
  });
}
function validatePage_(pageName) {
  var page = getPageConfig(pageName);
  /* v1.12.0: engine-layout validation on the cockpit guarantees false WARN
   * spam (width 118, Symbol column, gate fields — none apply). Return a
   * friendly decision-owned result without logging WARN or writing a WARN
   * page status. */
  if (_refreshIsDecisionOwnedPage_(page.pageName)) {
    var ownedMsg = '"' + page.pageName + '" is rendered by the Decision cockpit ' +
      '(16_Decision_Top10.gs); engine-layout validation does not apply. ' +
      'Use the Decision menu (Decision Diagnostics) to check cockpit health.';
    try {
      logRun_('INFO', 'validatePage', page.pageName, ownedMsg, {
        status: 'SKIPPED_DECISION_OWNED'
      });
    } catch (logErr) {}
    return {
      ok: true,
      pageName: page.pageName,
      action: 'skipped_decision_owned',
      skipped: true,
      message: ownedMsg
    };
  }
  var sheet = ensureSheetExists_(page.pageName);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var expectedHeaders = [];
  var actualHeaders = readCurrentSheetHeaders_(sheet);
  var missingHeaders = [];
  var extraHeaders = [];
  var orderMatches = false;
  var validationOk = false;
  var mappingModeUsed = false;
  var unmappedHeaders = [];
  if (_refreshCanUseHeaderMap_() && typeof validateHeaderMapping_ === 'function') {
    try {
      var mappingValidation = validateHeaderMapping_(page.pageName);
      mappingModeUsed = true;
      validationOk = !!mappingValidation.ok;
      unmappedHeaders = mappingValidation.unmapped || [];
      expectedHeaders = actualHeaders.slice();
      missingHeaders = unmappedHeaders.slice();
      extraHeaders = [];
      orderMatches = true;
    } catch (mapErr) {
      mappingModeUsed = false;
    }
  }
  if (!mappingModeUsed) {
    var headerValidation = validateHeaderOrder_(page.pageName);
    expectedHeaders = headerValidation.expectedHeaders || [];
    actualHeaders = headerValidation.actualHeaders || [];
    missingHeaders = headerValidation.missingHeaders || [];
    extraHeaders = headerValidation.extraHeaders || [];
    orderMatches = !!headerValidation.orderMatches;
    validationOk = !!headerValidation.ok;
  }
  var requiredKeys = getRequiredKeys(page.pageName);
  var missingRequiredKeys = requiredKeys.filter(function (key) {
    return actualHeaders.indexOf(key) === -1 && expectedHeaders.indexOf(key) === -1;
  });
  var widthValidation = validateExpectedRefreshWidth_(page.pageName, sheet);
  var gateValidation = validatePostRefreshIntegrity_(page.pageName, sheet, {
    expectedSymbolCount: getRequestedSymbolsFromOptions_(page.pageName, {})
  });
  var result = {
    ok: validationOk && missingRequiredKeys.length === 0 && widthValidation.ok && gateValidation.ok,
    pageName: page.pageName,
    expectedColumnCount: page.expectedColumnCount,
    expectedHeadersCount: expectedHeaders.length,
    actualHeadersCount: actualHeaders.length,
    rowCount: rowCount,
    missingHeaders: missingHeaders,
    extraHeaders: extraHeaders,
    orderMatches: orderMatches,
    missingRequiredKeys: missingRequiredKeys,
    mappingModeUsed: mappingModeUsed,
    unmappedHeaders: unmappedHeaders,
    widthValidation: widthValidation,
    gateValidation: gateValidation
  };
  var warnings = []
    .concat(result.missingHeaders || [])
    .concat(result.extraHeaders || [])
    .concat(result.missingRequiredKeys || [])
    .concat(widthValidation.warnings || [])
    .concat(gateValidation.warnings || []);
  logRun_(
    result.ok ? 'INFO' : 'WARN',
    'validatePage',
    page.pageName,
    result.ok ? 'Page validation passed.' : 'Page validation found differences.',
    {
      status: result.ok ? 'PASS' : 'WARN',
      endpoint: '',
      httpCode: '',
      durationMs: '',
      details: result
    }
  );
  writePageStatus_(page.pageName, {
    status: result.ok ? 'VALID' : 'WARN',
    message: result.ok ? 'Validation passed' : 'Validation differences found',
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: actualHeaders.length,
    durationMs: '',
    warnings: uniqueStrings_(warnings)
  });
  return result;
}
function sortAndRankPage_(pageName) {
  return withScriptLock_(function () {
    return sortAndRankPageCore_(pageName);
  });
}
/* =========================================================
 * Canonical header repair
 * ========================================================= */
function repairCanonicalHeaders(pageName) {
  return withScriptLock_(function () {
    return _repairCanonicalHeadersCore_(pageName);
  });
}
function repairCanonicalHeaders_MarketLeaders() {
  return repairCanonicalHeaders(SHEET_NAMES.MARKET_LEADERS);
}
function repairCanonicalHeaders_GlobalMarkets() {
  return repairCanonicalHeaders(SHEET_NAMES.GLOBAL_MARKETS);
}
function repairCanonicalHeaders_CommoditiesFx() {
  return repairCanonicalHeaders(SHEET_NAMES.COMMODITIES_FX);
}
function repairCanonicalHeaders_MutualFunds() {
  return repairCanonicalHeaders(SHEET_NAMES.MUTUAL_FUNDS);
}
function repairCanonicalHeaders_MyPortfolio() {
  return repairCanonicalHeaders(SHEET_NAMES.MY_PORTFOLIO);
}
function repairCanonicalHeaders_Top10() {
  return repairCanonicalHeaders(SHEET_NAMES.TOP_10_INVESTMENTS);
}
function _repairCanonicalHeadersCore_(pageName) {
  var normalized = normalizePageNameRefresh_(pageName || getActivePageName_());
  var page = getPageConfig(normalized);
  /* v1.12.0: header repair would insert all 118 engine headers as columns
   * into the cockpit — the single most destructive legacy action. */
  if (_refreshIsDecisionOwnedPage_(page.pageName)) {
    var ownedResult = _refreshSkipDecisionOwned_('repairCanonicalHeaders', page.pageName);
    showRepairDialog_('Canonical Header Repair', [
      'Page: ' + page.pageName,
      'Status: SKIPPED_DECISION_OWNED',
      '',
      ownedResult.message
    ]);
    return ownedResult;
  }
  var sheet = ensureSheetExists_(page.pageName);
  var lastCol = sheet.getLastColumn();
  if (lastCol < 1) {
    var emptyMsg = 'Cannot repair an empty sheet. Run a normal refresh first to lay down initial headers.';
    showRepairDialog_('Canonical Header Repair', [
      'Page: ' + page.pageName,
      'Status: SKIPPED',
      '',
      emptyMsg
    ]);
    return {
      ok: false,
      pageName: page.pageName,
      inserted: [],
      skipped: 0,
      message: emptyMsg
    };
  }
  var expectedColumnCount = _findExpectedColumnCountForPage_(page.pageName);
  if (!expectedColumnCount) {
    var unknownMsg = 'Cannot determine canonical column count for this page. No repair possible.';
    showRepairDialog_('Canonical Header Repair', [
      'Page: ' + page.pageName,
      'Status: SKIPPED',
      '',
      unknownMsg
    ]);
    return {
      ok: false,
      pageName: page.pageName,
      inserted: [],
      skipped: 0,
      message: unknownMsg
    };
  }
  var missing = _detectMissingCanonicalHeaders_(sheet, page.pageName);
  if (!missing.length) {
    var cleanMsg = 'No canonical header drift detected. All ' + expectedColumnCount + ' canonical headers are present.';
    showRepairDialog_('Canonical Header Repair', [
      'Page: ' + page.pageName,
      'Status: NO_DRIFT',
      'Expected: ' + expectedColumnCount + ' canonical headers',
      'Missing: 0',
      '',
      cleanMsg
    ]);
    return {
      ok: true,
      pageName: page.pageName,
      inserted: [],
      skipped: 0,
      message: cleanMsg
    };
  }
  var expectedHeaders;
  try {
    expectedHeaders = getExpectedHeaders_(page.pageName) || [];
  } catch (e) {
    var schemaErrMsg = 'Could not load schema headers: ' + (e && e.message ? e.message : String(e));
    showRepairDialog_('Canonical Header Repair', [
      'Page: ' + page.pageName,
      'Status: FAIL',
      '',
      schemaErrMsg
    ]);
    return {
      ok: false,
      pageName: page.pageName,
      inserted: [],
      skipped: 0,
      message: schemaErrMsg
    };
  }
  var plan = [];
  for (var i = 0; i < missing.length; i++) {
    var header = missing[i];
    var schemaIdx = -1;
    for (var j = 0; j < expectedHeaders.length; j++) {
      if (
        normalizeHeaderTextRefresh_(expectedHeaders[j]).toLowerCase() ===
        normalizeHeaderTextRefresh_(header).toLowerCase()
      ) {
        schemaIdx = j;
        break;
      }
    }
    if (schemaIdx < 0) continue;
    plan.push({
      header: header,
      targetCol: schemaIdx + 1
    });
  }
  plan.sort(function (a, b) {
    return a.targetCol - b.targetCol;
  });
  if (!plan.length) {
    var noPlanMsg = 'Detected ' + missing.length + ' missing headers but could not compute insertion positions.';
    showRepairDialog_('Canonical Header Repair', [
      'Page: ' + page.pageName,
      'Status: FAIL',
      '',
      noPlanMsg
    ]);
    return {
      ok: false,
      pageName: page.pageName,
      inserted: [],
      skipped: 0,
      message: noPlanMsg
    };
  }
  var ui = SpreadsheetApp.getUi();
  var confirmLines = [
    'Page: ' + page.pageName,
    'Status: DRIFT_DETECTED',
    '',
    missing.length + ' canonical headers are missing from row 1.',
    'Existing data columns will shift right as each one is inserted.',
    '',
    'Insertion plan:'
  ];
  for (var p = 0; p < Math.min(plan.length, 25); p++) {
    confirmLines.push('  ' + (p + 1) + '. col ' + plan[p].targetCol + ': "' + plan[p].header + '"');
  }
  if (plan.length > 25) {
    confirmLines.push('  ...(+' + (plan.length - 25) + ' more)');
  }
  confirmLines.push('');
  confirmLines.push('After repair, run a normal batch refresh to populate the new columns.');
  confirmLines.push('');
  confirmLines.push('Proceed?');
  var confirmResponse;
  try {
    confirmResponse = ui.alert(
      'Canonical Header Repair — ' + page.pageName,
      confirmLines.join('\n'),
      ui.ButtonSet.YES_NO
    );
  } catch (uiErr) {
    return {
      ok: false,
      pageName: page.pageName,
      inserted: [],
      skipped: 0,
      message: 'Cannot show confirmation dialog in this execution context. Repair aborted.'
    };
  }
  if (confirmResponse !== ui.Button.YES) {
    return {
      ok: false,
      pageName: page.pageName,
      inserted: [],
      skipped: 0,
      message: 'Repair aborted by user.'
    };
  }
  var inserted = [];
  var skipped = 0;
  for (var k = 0; k < plan.length; k++) {
    var item = plan[k];
    try {
      sheet.insertColumnBefore(item.targetCol);
      sheet.getRange(1, item.targetCol).setValue(item.header);
      inserted.push({
        header: item.header,
        col: item.targetCol
      });
    } catch (insertErr) {
      skipped++;
      try {
        Logger.log('[v1.9.2 HEADER_REPAIR][' + page.pageName + '] INSERT_FAILED header=' +
          item.header + ' col=' + item.targetCol + ' err=' +
          (insertErr && insertErr.message ? insertErr.message : String(insertErr)));
      } catch (logErr) {}
    }
  }
  _logCanonicalHeaderRepair_(page.pageName, inserted, skipped);
  try {
    var postRepairHeaders = readCurrentSheetHeaders_(sheet);
    _clearStatusPanelZone_(sheet, postRepairHeaders.length, page.pageName);
  } catch (cleanupErr) {}
  var resultMsg = 'Inserted ' + inserted.length + ' canonical headers (' + skipped + ' skipped). Run a batch refresh to populate the new columns.';
  logRun_(
    inserted.length > 0 ? 'INFO' : 'WARN',
    'repairCanonicalHeaders',
    page.pageName,
    resultMsg,
    {
      status: inserted.length > 0 ? 'OK' : 'WARN',
      inserted: inserted.length,
      skipped: skipped,
      details: {
        insertedColumns: inserted,
        skippedCount: skipped
      }
    }
  );
  var completionLines = [
    'Page: ' + page.pageName,
    'Status: ' + (inserted.length > 0 ? 'OK' : 'WARN'),
    'Inserted: ' + inserted.length,
    'Skipped: ' + skipped,
    '',
    resultMsg
  ];
  showRepairDialog_('Canonical Header Repair — ' + page.pageName, completionLines);
  return {
    ok: inserted.length > 0,
    pageName: page.pageName,
    inserted: inserted,
    skipped: skipped,
    message: resultMsg
  };
}
function showRepairDialog_(title, lines) {
  try {
    var ui = SpreadsheetApp.getUi();
    ui.alert(title, (lines || []).join('\n'), ui.ButtonSet.OK);
  } catch (e) {
    try {
      Logger.log('[v1.10.0 REPAIR_DIALOG] ' + title);
      for (var i = 0; i < (lines || []).length; i++) {
        Logger.log('  ' + lines[i]);
      }
    } catch (logErr) {}
  }
}
/* =========================================================
 * Reformat all
 * ========================================================= */
function reformatAllSheets_() {
  return withScriptLock_(function () {
    var pages = getSupportedPages();
    var results = [];
    var startedAt = new Date().getTime();
    var successCount = 0;
    var failCount = 0;
    for (var i = 0; i < pages.length; i++) {
      var pageName = pages[i];
      var pageStartedAt = new Date().getTime();
      try {
        applySheetFormatting_(pageName);
        var pageDuration = new Date().getTime() - pageStartedAt;
        successCount++;
        results.push({
          ok: true,
          pageName: pageName,
          durationMs: pageDuration
        });
      } catch (err) {
        failCount++;
        var message = getRefreshErrorMessage_(err);
        results.push({
          ok: false,
          pageName: pageName,
          message: message
        });
        logRun_(
          'WARN',
          'reformatAllSheets',
          pageName,
          'Reformat failed for ' + pageName + ': ' + message,
          {
            status: 'WARN',
            error: message
          }
        );
      }
    }
    var totalDuration = new Date().getTime() - startedAt;
    logRun_(
      failCount ? 'WARN' : 'INFO',
      'reformatAllSheets',
      '',
      'Reformat all sheets completed: ' + successCount + ' ok, ' + failCount + ' failed in ' + totalDuration + ' ms.',
      {
        status: failCount ? 'WARN' : 'OK',
        successCount: successCount,
        failCount: failCount,
        totalSheets: pages.length,
        durationMs: totalDuration
      }
    );
    writeGlobalStatus_({
      healthStatus: 'REFORMAT_ALL_DONE',
      version: '',
      message: 'Reformat all sheets: ' + successCount + ' ok, ' + failCount + ' failed in ' + totalDuration + ' ms.'
    });
    return {
      ok: failCount === 0,
      action: 'reformatAllSheets',
      successCount: successCount,
      failCount: failCount,
      totalSheets: pages.length,
      durationMs: totalDuration,
      results: results
    };
  });
}
/* =========================================================
 * Core page operations
 * ========================================================= */
function buildPageCore_(pageName) {
  var page = getPageConfig(pageName);
  ensureSheetExists_(page.pageName);
  updateHeadersOnly_(page.pageName);
  return refreshPageCore_(page.pageName, {
    forceHeaders: false,
    applyFormat: true,
    applySort: true,
    isBuild: true,
    skipLock: true
  });
}
function refreshPageCore_(pageName, options) {
  options = options || {};
  var page = getPageConfig(pageName);
  /* v1.12.0 MASTER GUARD: every refresh entry point (refreshAllPages_,
   * refreshAnalysisAndInsights_, buildPageCore_, refreshPage_, and the
   * v1.11.5 batch redirect) crosses this chokepoint. The cockpit must never
   * receive an engine table write. Skip-not-throw so loops continue. */
  if (_refreshIsDecisionOwnedPage_(page.pageName)) {
    return _refreshSkipDecisionOwned_('refreshPage', page.pageName);
  }
  var sheet = ensureSheetExists_(page.pageName);
  var startedAt = new Date().getTime();
  var useHeaderMap = _refreshCanUseHeaderMap_();
  var sheetHasHeaders = readCurrentSheetHeaders_(sheet).length > 0;
  var forceHeaders = options.forceHeaders === true;
  var legacyPathRequested = options.legacyPath === true;
  var shouldRewriteHeaders;
  if (forceHeaders) {
    shouldRewriteHeaders = true;
  } else if (!sheetHasHeaders) {
    shouldRewriteHeaders = true;
  } else if (!useHeaderMap) {
    shouldRewriteHeaders = options.forceHeaders !== false;
  } else {
    shouldRewriteHeaders = false;
  }
  if (shouldRewriteHeaders) {
    applyHeadersFromSchema_(page.pageName);
  }
  var driftMissing = _detectMissingCanonicalHeaders_(sheet, page.pageName);
  if (driftMissing.length) {
    _logCanonicalHeaderDrift_(page.pageName, driftMissing);
  }
  var preSweepHeaders = readCurrentSheetHeaders_(sheet);
  _clearStatusPanelZone_(sheet, preSweepHeaders.length, page.pageName);
  if (shouldAutoBatchRefresh_(page.pageName, options)) {
    return refreshPageInBatchesCore_(page.pageName, mergeObjects_({}, options, {
      autoBatch: true,
      skipLock: true
    }));
  }
  var currentHeaders = readCurrentSheetHeaders_(sheet);
  if (!currentHeaders.length) {
    currentHeaders = getExpectedHeaders_(page.pageName);
  }
  var existingRowsAsObjects = getSheetDataAsObjects_(sheet);
  var fetchResult = fetchPageData_(page.pageName, options);
  if (!fetchResult || !fetchResult.ok) {
    throw new Error((fetchResult && fetchResult.message) || ('Failed to fetch data for page: ' + page.pageName));
  }
  var normalized = fetchResult.normalized || {
    headers: [],
    keys: [],
    rows2d: [],
    rowObjects: [],
    rowCount: 0,
    rowType: 'none'
  };
  var headers = normalized.headers && normalized.headers.length ? normalized.headers.slice() : currentHeaders.slice();
  var rows2d = Array.isArray(normalized.rows2d) ? normalized.rows2d.slice() : [];
  var rowObjects = Array.isArray(normalized.rowObjects) ? normalized.rowObjects.slice() : [];
  var warnings = Array.isArray(fetchResult.warnings) ? fetchResult.warnings.slice() : [];
  var payloadStatus = fetchResult.payloadStatus || '';
  var isInsightsPartialTimeout = isInsightsTimeoutPartial_(page.pageName, fetchResult);
  if (driftMissing.length) {
    warnings.push('Canonical header drift: ' + driftMissing.length + ' missing — run repairCanonicalHeaders(\'' + page.pageName + '\') to fix.');
  }
  if (!headers.length) {
    throw new Error('No headers available for page: ' + page.pageName);
  }
  if (
    isInsightsPartialTimeout &&
    shouldKeepLastGoodInsightsData() &&
    rows2d.length === 0 &&
    rowObjects.length === 0
  ) {
    writePageStatusPanel_(sheet, {
      status: 'PARTIAL',
      message: 'Insights timeout - kept previous good data',
      warnings: warnings
    }, headers.length, page.pageName);
    logRun_(
      'WARN',
      'refreshPage',
      page.pageName,
      'Insights returned partial timeout. Previous rows preserved.',
      {
        status: 'WARN',
        endpoint: fetchResult.endpointUsed,
        httpCode: fetchResult.statusCode,
        durationMs: fetchResult.durationMs,
        warnings: warnings
      }
    );
    writePageStatus_(page.pageName, {
      status: 'PARTIAL',
      message: 'Kept previous good rows due to timeout',
      endpoint: fetchResult.endpointUsed,
      httpCode: fetchResult.statusCode,
      rows: Math.max(0, sheet.getLastRow() - 1),
      columns: headers.length,
      durationMs: fetchResult.durationMs,
      warnings: warnings
    });
    return {
      ok: true,
      pageName: page.pageName,
      partial: true,
      keptPreviousRows: true,
      endpointUsed: fetchResult.endpointUsed,
      headers: headers,
      rowCount: Math.max(0, sheet.getLastRow() - 1),
      warnings: warnings,
      canonicalDrift: driftMissing.length
    };
  }
  var hasAnyRows = rows2d.length > 0 || rowObjects.length > 0;
  if (!hasAnyRows && !page.allowEmptyRows) {
    throw new Error('No rows returned for page: ' + page.pageName);
  }
  // v1.11.3: the header-mapped writer (writeRowsBySymbol_) is symbol-keyed and
  // throws on a sheet with no "Symbol" column. Insights_Analysis under the
  // v2.14.0 contract (Section/Item/Metric/Value/Notes/Source/Sort Order) and
  // Data_Dictionary are key/value report pages with no Symbol column, so they
  // must use the positional writer. Guarding here covers EVERY entry point
  // (refreshInsightsAnalysis_, refreshAllPages_, refreshAnalysisAndInsights_,
  // and direct refreshPage_) at the single path-decision chokepoint.
  var sheetHasSymbolColumn = _sheetHasSymbolColumnRefresh_(sheet);
  var useHeaderMappedPath = useHeaderMap && rowObjects.length > 0 && sheetHasHeaders && sheetHasSymbolColumn && !legacyPathRequested;
  if (useHeaderMappedPath) {
    _logBatchPathChoice_(page.pageName, 'HEADER_MAPPED', 'rowObjects=' + rowObjects.length + ' sheetHasHeaders=true');
    /* v1.14.0 KEEP-LAST-GOOD (single-shot header-mapped): withhold
     * empty-partial rows before provenance stamping and the write. Covers
     * My_Portfolio, which at <= 10 symbols takes this path. If EVERY row
     * is withheld the write is skipped entirely (a synthetic zero-write
     * result keeps the downstream accounting intact). */
    var klgSingle = _refreshFilterEmptyPartialRowObjects_(
      page.pageName,
      rowObjects,
      _refreshKeepLastGoodEnabled_() ? _refreshBuildIdentityMap_(sheet) : null
    );
    var klgSingleWithheld = klgSingle.withheld;
    rowObjects = klgSingle.kept;
    if (klgSingleWithheld.length) {
      warnings.push('[v1.14.0 KEEP_LAST_GOOD] withheld=' + klgSingleWithheld.length +
        ' empty-partial row(s) (blank Price over existing data); previous values kept: ' +
        klgSingleWithheld.slice(0, 15).join(',') +
        (klgSingleWithheld.length > 15 ? ',...(+' + (klgSingleWithheld.length - 15) + ')' : ''));
    }
    if (klgSingle.suspects && klgSingle.suspects.length) {
      warnings.push('[ID-FIREWALL v1.15.0] suspect_healed=' + klgSingle.suspects.length +
        ' identity-suspect previous row(s) not protected (stub written to heal): ' +
        klgSingle.suspects.slice(0, 15).join(',') +
        (klgSingle.suspects.length > 15 ? ',...(+' + (klgSingle.suspects.length - 15) + ')' : ''));
    }
    var hmResult;
    if (rowObjects.length) {
      _injectProvenanceFields_(rowObjects, fetchResult, page.pageName);
      hmResult = writeRowsBySymbol_(sheet, page.pageName, rowObjects, {
        preserveHeaderRows: 1,
        symbolApiKey: 'symbol',
        symbolHeaderName: 'Symbol',
        preserveManualColumns: page.preserveManualColumns || [],
        preserveOnEmptyColumns: getPreserveOnEmptyColumns(page.pageName) || [],
        allowAppend: true
      });
    } else {
      hmResult = {
        updated: 0,
        appended: 0,
        skipped: 0,
        unmappedHeaders: [],
        headersUsed: readCurrentSheetHeaders_(sheet).length
      };
    }
    /* v1.12.4: scoped post-write symbol dedupe (single-shot path). The
     * header-mapped upsert (02_Core writeRowsBySymbol_) is LAST-WINS: when a
     * symbol already exists twice on the sheet (a fresh GICS-sector row beside
     * a stale "Saudi Market" row from an earlier run), its internal symbolToRow
     * map points at the later physical occurrence, refreshes THAT row, and
     * ORPHANS the earlier copy permanently — the writer only updates-in-place
     * or appends, never deletes. That orphaning is the Market_Leaders
     * 406-data-row (~203x2) duplication, which in turn makes the Top_10 pool
     * dedup keep the older "Saudi Market" copy. This pass collapses the sheet
     * back to one row per symbol (keeping the freshest copy), AFTER the write,
     * on the four pure-instrument pages ONLY (My_Portfolio and the cockpit are
     * excluded). It is a NO-OP on a clean sheet — byte-identical to v1.12.3 —
     * self-guards on every precondition, and never throws. This call site is
     * defense-in-depth: the four market sheets normally auto-batch (>10 symbols)
     * and dedupe via the refreshPageInBatchesCore_ call site instead. */
    _refreshDedupeInstrumentSheetBySymbol_(sheet, page.pageName);
    var hmVerify = _verifyCriticalFieldsAfterWrite_(sheet, page.pageName, rowObjects);
    if (hmVerify && hmVerify.ok === false) {
      var hmBlankFields = (hmVerify.blanks || []).map(function (b) { return b.field; }).join(', ');
      warnings = uniqueStrings_(warnings.concat([
        'Post-write verify [' + hmVerify.diagnosis + ']: blank fields on ' + hmVerify.sampled + ' = ' + hmBlankFields
      ]));
    }
    var expectedW = _findExpectedColumnCountForPage_(page.pageName);
    if (expectedW) {
      var actualW = readCurrentSheetHeaders_(sheet).length;
      _logCanonicalWidth_(page.pageName, expectedW, actualW);
    }
    if (options.applyFormat !== false) {
      applySheetFormatting_(page.pageName);
    }
    if (options.applySort !== false) {
      sortAndRankPageCore_(page.pageName);
    }
    if (Array.isArray(hmResult.unmappedHeaders) && hmResult.unmappedHeaders.length) {
      warnings = uniqueStrings_(warnings.concat([
        'Unmapped sheet columns (left blank): ' + hmResult.unmappedHeaders.join(', ')
      ]));
    }
    var postValidation = validatePostRefreshIntegrity_(page.pageName, sheet, {
      expectedSymbolCount: rowObjects.length
    });
    warnings = uniqueStrings_(warnings.concat(postValidation.warnings || []));
    var hmEndedAt = new Date().getTime();
    var finalStatus = postValidation.ok ? (payloadStatus || 'SUCCESS') : 'WARN';
    writePageStatusPanel_(sheet, {
      status: finalStatus,
      message: postValidation.ok ? 'Page refreshed (header-mapped)' : 'Page refreshed with validation warnings',
      warnings: warnings
    }, readCurrentSheetHeaders_(sheet).length, page.pageName);
    logRun_(
      rfLevelForHttp_(postValidation.ok ? 'INFO' : 'WARN', fetchResult.statusCode),
      'refreshPage',
      page.pageName,
      postValidation.ok ? 'Page refreshed via header-mapped writer.' : 'Page refreshed but post-refresh validation found warnings.',
      {
        status: rfStatusForHttp_(postValidation.ok ? 'OK' : 'WARN', fetchResult.statusCode),
        endpoint: fetchResult.endpointUsed,
        httpCode: fetchResult.statusCode,
        durationMs: hmEndedAt - startedAt,
        rows: rowObjects.length,
        updated: hmResult.updated,
        appended: hmResult.appended,
        skipped: hmResult.skipped,
        keptLastGood: klgSingleWithheld.length,
        headersUsed: hmResult.headersUsed,
        unmappedCount: (hmResult.unmappedHeaders || []).length,
        columns: readCurrentSheetHeaders_(sheet).length,
        canonicalDrift: driftMissing.length,
        warnings: warnings,
        postValidationOk: postValidation.ok,
        path: 'header-mapped'
      }
    );
    writePageStatus_(page.pageName, {
      status: finalStatus,
      message: postValidation.ok ? 'Refresh completed (header-mapped)' : 'Refresh completed with validation warnings',
      endpoint: fetchResult.endpointUsed,
      httpCode: fetchResult.statusCode,
      rows: hmResult.updated + hmResult.appended,
      columns: readCurrentSheetHeaders_(sheet).length,
      durationMs: hmEndedAt - startedAt,
      warnings: warnings
    });
    return {
      ok: true,
      pageName: page.pageName,
      endpointUsed: fetchResult.endpointUsed,
      headers: readCurrentSheetHeaders_(sheet),
      keys: normalized.keys || [],
      rowCount: hmResult.updated + hmResult.appended,
      warnings: warnings,
      partial: payloadStatus === 'partial',
      path: 'header-mapped',
      updated: hmResult.updated,
      appended: hmResult.appended,
      keptLastGood: klgSingleWithheld.length,
      keptLastGoodSymbols: klgSingleWithheld,
      unmappedHeaders: hmResult.unmappedHeaders || [],
      canonicalDrift: driftMissing.length,
      postValidation: postValidation
    };
  }
  _logBatchPathChoice_(page.pageName,
    legacyPathRequested ? 'POSITIONAL_FORCED' : (!sheetHasSymbolColumn ? 'POSITIONAL_NO_SYMBOL_COLUMN' : 'POSITIONAL_LEGACY'),
    'useHeaderMap=' + useHeaderMap +
    ' rowObjects=' + rowObjects.length +
    ' sheetHasHeaders=' + sheetHasHeaders +
    ' sheetHasSymbolColumn=' + sheetHasSymbolColumn +
    (legacyPathRequested ? ' legacyPath=true' : ''));
  var v192SingleLegacyReason;
  if (legacyPathRequested) {
    v192SingleLegacyReason = 'LEGACY_FORCED';
  } else if (!useHeaderMap) {
    v192SingleLegacyReason = 'LEGACY_NO_HEADER_MAP';
  } else if (rowObjects.length === 0) {
    v192SingleLegacyReason = 'LEGACY_NO_ROW_OBJECTS';
  } else if (!sheetHasHeaders) {
    v192SingleLegacyReason = 'LEGACY_NO_SHEET_HEADERS';
  } else if (!sheetHasSymbolColumn) {
    v192SingleLegacyReason = 'POSITIONAL_NO_SYMBOL_COLUMN';
  } else {
    v192SingleLegacyReason = 'LEGACY_UNKNOWN';
  }
  _logActualWriterPath_(page.pageName, v192SingleLegacyReason, 'rows2d=' + rows2d.length + ' rowObjects=' + rowObjects.length);
  rows2d = dedupeRows2dBySymbol_(rows2d, headers);
  var preserveOnEmptyList = getPreserveOnEmptyColumns(page.pageName);
  if ((page.preserveManualColumns && page.preserveManualColumns.length) || preserveOnEmptyList.length) {
    if (rows2d.length) {
      rows2d = mergePreservedColumns_(
        headers,
        rows2d,
        existingRowsAsObjects,
        page.preserveManualColumns || [],
        preserveOnEmptyList
      );
    }
  }
  /* v1.14.0 KEEP-LAST-GOOD (positional full-table): writeTableToSheet_
   * clears the data area first, so a downgrading incoming row is
   * SUBSTITUTED with the existing sheet row verbatim — never dropped,
   * or the symbol would be deleted from the page. */
  var klgPositional = _refreshKeepLastGoodPositionalMerge_(page.pageName, headers, rows2d, existingRowsAsObjects);
  rows2d = klgPositional.rows;
  if (klgPositional.withheld.length) {
    warnings.push('[v1.14.0 KEEP_LAST_GOOD] positional: kept the previous row for ' +
      klgPositional.withheld.length + ' empty-partial symbol(s): ' +
      klgPositional.withheld.slice(0, 15).join(',') +
      (klgPositional.withheld.length > 15 ? ',...(+' + (klgPositional.withheld.length - 15) + ')' : ''));
  }
  if (klgPositional.suspects && klgPositional.suspects.length) {
    warnings.push('[ID-FIREWALL v1.15.0] positional: suspect_healed=' + klgPositional.suspects.length +
      ' identity-suspect previous row(s) not substituted (incoming row kept to heal): ' +
      klgPositional.suspects.slice(0, 15).join(',') +
      (klgPositional.suspects.length > 15 ? ',...(+' + (klgPositional.suspects.length - 15) + ')' : ''));
  }
  writeTableToSheet_(sheet, headers, rows2d, {
    preserveHeaderRows: 1,
    preserveManualColumns: []
  });
  var expectedWLegacy = _findExpectedColumnCountForPage_(page.pageName);
  if (expectedWLegacy) {
    var actualWLegacy = readCurrentSheetHeaders_(sheet).length;
    _logCanonicalWidth_(page.pageName, expectedWLegacy, actualWLegacy);
  }
  if (options.applyFormat !== false) {
    applySheetFormatting_(page.pageName);
  }
  if (options.applySort !== false) {
    sortAndRankPageCore_(page.pageName);
  }
  var legacyValidation = validatePostRefreshIntegrity_(page.pageName, sheet, {
    expectedSymbolCount: rows2d.length
  });
  warnings = uniqueStrings_(warnings.concat(legacyValidation.warnings || []));
  var endedAt = new Date().getTime();
  var legacyStatus = legacyValidation.ok ? (payloadStatus || 'SUCCESS') : 'WARN';
  writePageStatusPanel_(sheet, {
    status: legacyStatus,
    message: legacyValidation.ok ? 'Page refreshed successfully' : 'Page refreshed with validation warnings',
    warnings: warnings
  }, readCurrentSheetHeaders_(sheet).length, page.pageName);
  logRun_(
    rfLevelForHttp_(legacyValidation.ok ? 'INFO' : 'WARN', fetchResult.statusCode),
    'refreshPage',
    page.pageName,
    legacyValidation.ok ? 'Page refreshed successfully.' : 'Page refreshed but post-refresh validation found warnings.',
    {
      status: rfStatusForHttp_(legacyValidation.ok ? 'OK' : 'WARN', fetchResult.statusCode),
      endpoint: fetchResult.endpointUsed,
      httpCode: fetchResult.statusCode,
      durationMs: endedAt - startedAt,
      rows: rows2d.length,
      columns: readCurrentSheetHeaders_(sheet).length,
      canonicalDrift: driftMissing.length,
      warnings: warnings,
      postValidationOk: legacyValidation.ok,
      path: legacyPathRequested ? 'positional-forced' : 'positional'
    }
  );
  writePageStatus_(page.pageName, {
    status: legacyStatus,
    message: legacyValidation.ok ? 'Refresh completed' : 'Refresh completed with validation warnings',
    endpoint: fetchResult.endpointUsed,
    httpCode: fetchResult.statusCode,
    rows: rows2d.length,
    columns: readCurrentSheetHeaders_(sheet).length,
    durationMs: endedAt - startedAt,
    warnings: warnings
  });
  return {
    ok: true,
    pageName: page.pageName,
    endpointUsed: fetchResult.endpointUsed,
    headers: readCurrentSheetHeaders_(sheet),
    keys: normalized.keys || [],
    rowCount: rows2d.length,
    warnings: warnings,
    partial: payloadStatus === 'partial',
    path: legacyPathRequested ? 'positional-forced' : 'positional',
    canonicalDrift: driftMissing.length,
    postValidation: legacyValidation
  };
}
function refreshSelectedRowCore_(pageName, options) {
  options = options || {};
  var page = getPageConfig(pageName);
  /* v1.12.0: selected-row refresh would overwrite a cockpit row in place. */
  if (_refreshIsDecisionOwnedPage_(page.pageName)) {
    return _refreshSkipDecisionOwned_('refreshSelectedRow', page.pageName);
  }
  var sheet = ensureSheetExists_(page.pageName);
  if (readCurrentSheetHeaders_(sheet).length === 0) {
    applyHeadersFromSchema_(page.pageName);
  }
  var srrHeaders = readCurrentSheetHeaders_(sheet);
  _clearStatusPanelZone_(sheet, srrHeaders.length, page.pageName);
  var selected = getTargetRowContextForPage_(page.pageName, options);
  var selectedSymbol = extractRowSymbol_(selected.record);
  if (!selectedSymbol) {
    throw new Error('Selected row does not contain a usable Symbol/Ticker value.');
  }
  var endpointCandidates = options.endpointCandidates || getEndpointCandidates(page.pageName);
  var fetchResult = fetchPageData_(page.pageName, {
    endpointCandidates: endpointCandidates,
    payloadOverrides: mergeObjects_({}, options.payloadOverrides || {}, {
      symbols: [selectedSymbol],
      tickers: [selectedSymbol],
      direct_symbols: [selectedSymbol],
      limit: 50
    })
  });
  if (!fetchResult || !fetchResult.ok) {
    throw new Error((fetchResult && fetchResult.message) || 'Failed to refresh selected row.');
  }
  var normalized = fetchResult.normalized || {
    headers: [],
    keys: [],
    rows2d: [],
    rowObjects: [],
    rowCount: 0
  };
  var rowObjects = Array.isArray(normalized.rowObjects) ? normalized.rowObjects.slice() : [];
  var legacyPathRequested = options.legacyPath === true;
  if (!legacyPathRequested && _refreshCanUseHeaderMap_() && rowObjects.length > 0) {
    var matchedRowObj = null;
    var targetSymbolUC = String(selectedSymbol).toUpperCase();
    for (var ri = 0; ri < rowObjects.length; ri++) {
      var ro = rowObjects[ri];
      var rs = String((ro && (ro.symbol || ro.Symbol || ro.ticker || ro.Ticker)) || '').trim().toUpperCase();
      if (rs === targetSymbolUC) {
        matchedRowObj = ro;
        break;
      }
    }
    if (!matchedRowObj && rowObjects.length === 1) {
      matchedRowObj = rowObjects[0];
    }
    if (matchedRowObj) {
      /* v1.14.0 KEEP-LAST-GOOD (selected row, header-mapped): an
       * empty-partial single-row refresh must not wipe the selected row.
       * Uses the row's own record (no full-sheet read needed). */
      if (_refreshKeepLastGoodEnabled_() &&
          _refreshIncomingIsDowngradeVsFlags_(
            _isBlankRefresh_(matchedRowObj.name),
            _isBlankRefresh_(matchedRowObj.current_price),
            _refreshRecordIdentityFlags_(selected.record, selected.headers))) {
        return _refreshSelectedRowKeptLastGood_(page.pageName, sheet, selected, selectedSymbol, fetchResult);
      }
      _injectProvenanceFields_([matchedRowObj], fetchResult, page.pageName);
      var hmOk = writeSingleRowByHeaderMap_(sheet, selected.row, matchedRowObj, page.pageName, {
        preserveHeaderRows: 1,
        preserveManualColumns: page.preserveManualColumns || [],
        preserveOnEmptyColumns: getPreserveOnEmptyColumns(page.pageName) || []
      });
      if (hmOk) {
        var hmHeaders = readCurrentSheetHeaders_(sheet);
        var postValidation = validatePostRefreshIntegrity_(page.pageName, sheet, { expectedSymbolCount: 1 });
        var warnings = uniqueStrings_((fetchResult.warnings || []).concat(postValidation.warnings || []));
        if (sheet.getName() !== SpreadsheetApp.getActiveSpreadsheet().getActiveSheet().getName()) {
          SpreadsheetApp.getActiveSpreadsheet().setActiveSheet(sheet);
        }
        try {
          sheet.getRange(selected.row, 1, 1, Math.max(1, hmHeaders.length)).activate();
        } catch (e2) {}
        writePageStatusPanel_(sheet, {
          status: postValidation.ok ? 'ROW_REFRESH_OK' : 'WARN',
          message: postValidation.ok ? 'Selected row refreshed (header-mapped): ' + selectedSymbol : 'Selected row refreshed with validation warnings: ' + selectedSymbol,
          warnings: warnings
        }, hmHeaders.length, page.pageName);
        logRun_(
          rfLevelForHttp_(postValidation.ok ? 'INFO' : 'WARN', fetchResult.statusCode),
          'refreshSelectedRow',
          page.pageName,
          'Selected row refreshed via header-mapped writer for: ' + selectedSymbol,
          {
            status: rfStatusForHttp_(postValidation.ok ? 'OK' : 'WARN', fetchResult.statusCode),
            endpoint: fetchResult.endpointUsed,
            httpCode: fetchResult.statusCode,
            durationMs: fetchResult.durationMs,
            symbol: selectedSymbol,
            row: selected.row,
            warnings: warnings,
            path: 'header-mapped'
          }
        );
        writePageStatus_(page.pageName, {
          status: postValidation.ok ? 'ROW_REFRESH_OK' : 'WARN',
          message: postValidation.ok ? 'Selected row refreshed: ' + selectedSymbol : 'Selected row refreshed with validation warnings: ' + selectedSymbol,
          endpoint: fetchResult.endpointUsed,
          httpCode: fetchResult.statusCode,
          rows: 1,
          columns: hmHeaders.length,
          durationMs: fetchResult.durationMs,
          warnings: warnings
        });
        return {
          ok: true,
          pageName: page.pageName,
          symbol: selectedSymbol,
          rowNumber: selected.row,
          endpointUsed: fetchResult.endpointUsed,
          path: 'header-mapped',
          postValidation: postValidation
        };
      }
    }
  }
  var currentHeaders = readCurrentSheetHeaders_(sheet);
  var responseHeaders = normalized.headers && normalized.headers.length ? normalized.headers.slice() : currentHeaders.slice();
  if (!currentHeaders.length && responseHeaders.length) {
    writeHeaderRowExact_(sheet, responseHeaders);
    currentHeaders = responseHeaders.slice();
  }
  if (responseHeaders.length && !arraysEqual_(responseHeaders, currentHeaders)) {
    writeHeaderRowExact_(sheet, responseHeaders);
    currentHeaders = responseHeaders.slice();
  }
  if (!currentHeaders.length) {
    throw new Error('No usable headers found for selected-row refresh.');
  }
  var matchedRow = selectBestMatching2dRow_(normalized.rows2d || [], currentHeaders, selectedSymbol);
  if (!matchedRow) {
    throw new Error('Backend returned no matching row for selected symbol: ' + selectedSymbol);
  }
  var preserveManual = page.preserveManualColumns || [];
  var preserveOnEmpty = getPreserveOnEmptyColumns(page.pageName);
  if (preserveManual.length || preserveOnEmpty.length) {
    matchedRow = mergeSingleRowPreservedColumns_(
      currentHeaders,
      matchedRow,
      selected.record,
      preserveManual,
      preserveOnEmpty
    );
  }
  /* v1.14.0 KEEP-LAST-GOOD (selected row, positional). */
  if (_refreshKeepLastGoodEnabled_() &&
      _refreshRow2dIsIdentityDowngrade_(matchedRow, currentHeaders, selected.record)) {
    return _refreshSelectedRowKeptLastGood_(page.pageName, sheet, selected, selectedSymbol, fetchResult);
  }
  while (matchedRow.length < currentHeaders.length) {
    matchedRow.push('');
  }
  var targetWidth = Math.max(sheet.getLastColumn(), currentHeaders.length);
  if (sheet.getMaxColumns() < targetWidth) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), targetWidth - sheet.getMaxColumns());
  }
  sheet.getRange(selected.row, 1, 1, targetWidth).clearContent();
  sheet.getRange(selected.row, 1, 1, currentHeaders.length).setValues([matchedRow.slice(0, currentHeaders.length)]);
  if (sheet.getName() !== SpreadsheetApp.getActiveSpreadsheet().getActiveSheet().getName()) {
    SpreadsheetApp.getActiveSpreadsheet().setActiveSheet(sheet);
  }
  try {
    sheet.getRange(selected.row, 1, 1, Math.max(1, currentHeaders.length)).activate();
  } catch (e3) {}
  var selectedValidation = validatePostRefreshIntegrity_(page.pageName, sheet, { expectedSymbolCount: 1 });
  var selectedWarnings = uniqueStrings_((fetchResult.warnings || []).concat(selectedValidation.warnings || []));
  writePageStatusPanel_(sheet, {
    status: selectedValidation.ok ? 'ROW_REFRESH_OK' : 'WARN',
    message: selectedValidation.ok ? 'Selected row refreshed: ' + selectedSymbol : 'Selected row refreshed with validation warnings: ' + selectedSymbol,
    warnings: selectedWarnings
  }, currentHeaders.length, page.pageName);
  logRun_(
    rfLevelForHttp_(selectedValidation.ok ? 'INFO' : 'WARN', fetchResult.statusCode),
    'refreshSelectedRow',
    page.pageName,
    'Selected row refreshed for symbol: ' + selectedSymbol,
    {
      status: rfStatusForHttp_(selectedValidation.ok ? 'OK' : 'WARN', fetchResult.statusCode),
      endpoint: fetchResult.endpointUsed,
      httpCode: fetchResult.statusCode,
      durationMs: fetchResult.durationMs,
      symbol: selectedSymbol,
      row: selected.row,
      warnings: selectedWarnings,
      path: 'positional'
    }
  );
  writePageStatus_(page.pageName, {
    status: selectedValidation.ok ? 'ROW_REFRESH_OK' : 'WARN',
    message: selectedValidation.ok ? 'Selected row refreshed: ' + selectedSymbol : 'Selected row refreshed with validation warnings: ' + selectedSymbol,
    endpoint: fetchResult.endpointUsed,
    httpCode: fetchResult.statusCode,
    rows: 1,
    columns: currentHeaders.length,
    durationMs: fetchResult.durationMs,
    warnings: selectedWarnings
  });
  return {
    ok: true,
    pageName: page.pageName,
    symbol: selectedSymbol,
    rowNumber: selected.row,
    endpointUsed: fetchResult.endpointUsed,
    path: 'positional',
    postValidation: selectedValidation
  };
}
function _refreshTripwireEnabled_() {
  /* v1.13.0: concurrent-writer tripwire master switch. Default ON.
   * Script Property TFB_REFRESH_TRIPWIRE = 0/false/off/no/disabled
   * restores exact v1.12.7 loop behavior (no fingerprint checks). */
  try {
    var v = PropertiesService.getScriptProperties().getProperty('TFB_REFRESH_TRIPWIRE');
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    if (['0', 'false', 'off', 'no', 'disabled', 'disable'].indexOf(s) >= 0) return false;
    return true;
  } catch (e) {
    return true;
  }
}
function _refreshVerifySlimReadEnabled_() {
  /* v1.16.0: post-write verify slim-read master switch. Default ON. OFF
   * restores the v1.15.0 full-grid read exactly. */
  try {
    var v = PropertiesService.getScriptProperties().getProperty('TFB_REFRESH_VERIFY_SLIM_READ');
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    if (['0', 'false', 'off', 'no', 'disabled', 'disable'].indexOf(s) >= 0) return false;
    return true;
  } catch (e) {
    return true;
  }
}
function _refreshBatchSizeDefault_() {
  /* v1.16.0: effective default chunk size for the batch planner AND the
   * auto-batch threshold. Script Property TFB_REFRESH_BATCH_SIZE overrides
   * the compile-time REFRESH_BATCH_SIZE_DEFAULT_ (10); clamped 1..50.
   * Unset/blank/unparsable -> 10 (no behavior change). */
  try {
    var v = PropertiesService.getScriptProperties().getProperty('TFB_REFRESH_BATCH_SIZE');
    if (v === null || v === undefined || String(v).trim() === '') {
      return REFRESH_BATCH_SIZE_DEFAULT_;
    }
    var n = parseInt(String(v).trim(), 10);
    if (isNaN(n)) return REFRESH_BATCH_SIZE_DEFAULT_;
    return Math.max(1, Math.min(n, 50));
  } catch (e) {
    return REFRESH_BATCH_SIZE_DEFAULT_;
  }
}
function _refreshCaptureFingerprint_(sheet, symbolColNumber) {
  /* v1.13.0: cheap structural fingerprint of the page — last row plus up
   * to 12 head and 12 tail symbols. Two tiny ranged reads; never throws. */
  var fp = { rows: 0, head: [], tail: [] };
  try {
    var lastRow = sheet.getLastRow();
    fp.rows = lastRow;
    var dataStart = 2;
    var n = lastRow - dataStart + 1;
    if (n <= 0 || !symbolColNumber || symbolColNumber < 1) return fp;
    var headN = Math.min(12, n);
    var headVals = sheet.getRange(dataStart, symbolColNumber, headN, 1).getValues();
    for (var hI = 0; hI < headVals.length; hI++) {
      fp.head.push(String(headVals[hI][0] || '').trim().toUpperCase());
    }
    if (n > headN) {
      var tailN = Math.min(12, n - headN);
      var tailVals = sheet.getRange(lastRow - tailN + 1, symbolColNumber, tailN, 1).getValues();
      for (var tI = 0; tI < tailVals.length; tI++) {
        fp.tail.push(String(tailVals[tI][0] || '').trim().toUpperCase());
      }
    }
  } catch (e) {}
  return fp;
}
function _refreshFingerprintDrift_(fpA, fpB) {
  /* v1.13.0: TRUE when the page changed in a way our own writer cannot
   * explain between the two captures. Thresholds echo the v1.12.7
   * mass-drop guard: row delta > 25, or > 10% of the sampled symbols
   * (min 3) disagree. HEAD moves only under a foreign full-table
   * rewrite; TAIL naturally grows under our own appends, so tail
   * mismatches are counted only when the row count did NOT grow. */
  try {
    if (!fpA || !fpB) return false;
    var rowDelta = Math.abs((fpB.rows || 0) - (fpA.rows || 0));
    if (rowDelta > 25) return true;
    var mism = 0;
    var sampled = 0;
    var hLen = Math.min(fpA.head.length, fpB.head.length);
    for (var i = 0; i < hLen; i++) {
      sampled++;
      if (fpA.head[i] !== fpB.head[i]) mism++;
    }
    var grew = (fpB.rows || 0) > (fpA.rows || 0);
    if (!grew) {
      var tLen = Math.min(fpA.tail.length, fpB.tail.length);
      for (var j = 0; j < tLen; j++) {
        sampled++;
        if (fpA.tail[j] !== fpB.tail[j]) mism++;
      }
    }
    if (!sampled) return false;
    return mism >= Math.max(3, Math.ceil(sampled * 0.10));
  } catch (e) {
    return false;
  }
}
function refreshPageInBatchesCore_(pageName, options) {
  options = options || {};
  var page = getPageConfig(pageName);

  // v1.18.0: final race-closing check after the caller acquired the shared
  // DocumentLock and before this function mutates the page. A manual owner is
  // phase='active' and is deliberately not stopped by this probe.
  var manualPriorityPreflight = _refreshManualPriorityProbe_(
    'refreshPageInBatchesCore_:preflight:' + page.pageName
  );
  if (manualPriorityPreflight.yield) {
    return _refreshPauseForManualPriority_({
      pageName: page.pageName,
      priority: manualPriorityPreflight,
      writeStatus: false
    });
  }

  var sheet = ensureSheetExists_(page.pageName);
  // v1.11.5: Top_10_Investments joins Data_Dictionary and Insights_Analysis
  // on the single-shot path. Top_10 is ONE backend-computed POST — the
  // selector (top10_selector v4.18.0) scans its own candidate universe and
  // returns the ranked shortlist. Chunked per-symbol batching is meaningless
  // for it, and actively harmful: each chunk previously carried the sheet's
  // CURRENT symbols as symbols/tickers/direct_symbols, which earned the
  // selector's +140 direct-symbol priority bump and pinned last run's winners
  // over the organic universe (live evidence 2026-06-11 08:30 UTC: two
  // Criteria Snapshots = the sheet's 17 symbols split 10+7). This guard
  // covers every entry point, including a manual batch-refresh invocation.
  if (
    page.pageName === SHEET_NAMES.DATA_DICTIONARY ||
    page.pageName === SHEET_NAMES.INSIGHTS_ANALYSIS ||
    page.pageName === SHEET_NAMES.TOP_10_INVESTMENTS
  ) {
    return refreshPageCore_(page.pageName, {
      skipLock: true,
      endpointCandidates: options.endpointCandidates,
      payloadOverrides: options.payloadOverrides || {},
      applyFormat: options.applyFormat !== false,
      applySort: options.applySort !== false,
      allowAutoBatch: false,
      legacyPath: options.legacyPath === true
    });
  }
  if (readCurrentSheetHeaders_(sheet).length === 0) {
    applyHeadersFromSchema_(page.pageName);
  }
  var driftMissing = _detectMissingCanonicalHeaders_(sheet, page.pageName);
  if (driftMissing.length) {
    _logCanonicalHeaderDrift_(page.pageName, driftMissing);
  }
  var preBatchHeaders = readCurrentSheetHeaders_(sheet);
  _clearStatusPanelZone_(sheet, preBatchHeaders.length, page.pageName);
  var headers = readCurrentSheetHeaders_(sheet);
  var symbols = getRequestedSymbolsFromOptions_(page.pageName, options);
  var batchSize = Math.max(1, parseInt(options.batchSize || _refreshBatchSizeDefault_(), 10));
  var endpointCandidates = options.endpointCandidates || getEndpointCandidates(page.pageName);
  var preserveColumns = page.preserveManualColumns || [];
  var preserveOnEmpty = getPreserveOnEmptyColumns(page.pageName);
  var runtimeStart = new Date().getTime();
  var legacyPathRequested = options.legacyPath === true;
  var useHeaderMappedBatch = _refreshCanUseHeaderMap_() && !legacyPathRequested;
  _logBatchPathChoice_(page.pageName,
    useHeaderMappedBatch ? 'HEADER_MAPPED' : (legacyPathRequested ? 'POSITIONAL_FORCED' : 'POSITIONAL_LEGACY'),
    'batch=' + batchSize + ' symbols=' + symbols.length);
  if (!symbols.length) {
    throw new Error('No symbols available to refresh for page: ' + page.pageName);
  }
  var checkpoint = loadBatchCheckpoint_(page.pageName);
  if (!isCheckpointCompatible_(checkpoint, symbols, batchSize, page.pageName)) {
    checkpoint = {
      pageName: page.pageName,
      batchSize: batchSize,
      symbols: symbols.slice(),
      nextIndex: 0,
      totalSymbols: symbols.length,
      baseCount: symbols.length,   /* v1.20.0 */
      failed: {},
      retried: {}
    };
    saveBatchCheckpoint_(page.pageName, checkpoint);
  }
  var successfulBatches = 0;
  var failedBatches = 0;
  var allWarnings = [];
  var endpointUsed = '';
  var totalUpdated = 0;
  var totalAppended = 0;
  var totalSkipped = 0;
  var verifyIssues = 0;
  var verifyDiagnoses = {};
  var totalProvenanceFilled = 0;
  /* v1.13.0 CONCURRENT-WRITER TRIPWIRE + LOSSLESS DEFERRAL state. */
  var symbolColNumberTw = findColumnIndex_(headers, 'Symbol', { caseInsensitive: true });
  var tripwireOn = _refreshTripwireEnabled_() && symbolColNumberTw > 0;
  var fpBaseline = tripwireOn ? _refreshCaptureFingerprint_(sheet, symbolColNumberTw) : null;
  var iterCount = 0;
  var totalDeferred = 0;
  var totalAppendAborted = 0;
  var totalSecondChance = 0;
  var totalPlacementAborted = 0;
  checkpoint.requeued = checkpoint.requeued || {};
  /* v1.20.0 [FAILED-LEDGER] state: the ledger rides on the checkpoint. */
  checkpoint.failed = checkpoint.failed || {};
  checkpoint.retried = checkpoint.retried || {};
  var failedRetryMode = _refreshFailedRetryMode_();
  var totalRetried = 0;
  var totalRecovered = 0;
  /* v1.14.0 KEEP-LAST-GOOD state: one identity read at batch start (each
   * symbol is written at most once per run, so the map stays accurate for
   * the whole loop; requeued guard-deferrals were never written). */
  var klgIdentityMap = _refreshKeepLastGoodEnabled_() ? _refreshBuildIdentityMap_(sheet) : null;
  var totalKeptLastGood = 0;
  var klgWithheldAll = [];
  if (driftMissing.length) {
    allWarnings.push('Canonical header drift: ' + driftMissing.length + ' missing — run repairCanonicalHeaders(\'' + page.pageName + '\') to fix.');
  }
  while (checkpoint.nextIndex < checkpoint.totalSymbols) {
    // v1.18.0 safe boundary: no chunk mutation has started in this iteration.
    var manualPriorityBeforeChunk = _refreshManualPriorityProbe_(
      'refreshPageInBatchesCore_:before-chunk:' + page.pageName + ':' + checkpoint.nextIndex
    );
    if (manualPriorityBeforeChunk.yield) {
      saveBatchCheckpoint_(page.pageName, checkpoint);
      return _refreshPauseForManualPriority_({
        pageName: page.pageName,
        sheet: sheet,
        checkpoint: checkpoint,
        priority: manualPriorityBeforeChunk,
        endpointUsed: endpointUsed,
        durationMs: new Date().getTime() - runtimeStart,
        warnings: allWarnings,
        successfulBatches: successfulBatches,
        failedBatches: failedBatches,
        verifyIssues: verifyIssues,
        canonicalDrift: driftMissing.length,
        writeStatus: checkpoint.nextIndex > 0
      });
    }

    /* v1.13.0 TRIPWIRE: every 3rd iteration, re-fingerprint the page.
     * Drift our own writer cannot explain (see _refreshFingerprintDrift_)
     * means a FOREIGN writer (GitHub daily_sync full-table rewrite) is
     * active in our read->write gap — the exact condition that minted
     * transposed blocks, duplicates and symbol losses. Writing blind
     * into a moving table is never acceptable: abort CLEANLY, keep the
     * checkpoint, surface the reason, let the operator (or the next
     * trigger fire) resume after the other writer finishes. */
    iterCount++;
    if (tripwireOn && (iterCount % 3) === 1 && iterCount > 1) {
      var fpNow = _refreshCaptureFingerprint_(sheet, symbolColNumberTw);
      if (_refreshFingerprintDrift_(fpBaseline, fpNow)) {
        saveBatchCheckpoint_(page.pageName, checkpoint);
        var twMsg = 'Batch refresh aborted: concurrent writer detected (sheet changed mid-batch: rows ' +
          (fpBaseline ? fpBaseline.rows : '?') + ' -> ' + fpNow.rows +
          '). Checkpoint kept at ' + checkpoint.nextIndex + ' / ' + checkpoint.totalSymbols + '; run again to continue.';
        allWarnings = uniqueStrings_(allWarnings.concat(['[v1.13.0 TRIPWIRE] ' + twMsg]));
        writePageStatusPanel_(sheet, {
          status: 'PARTIAL',
          message: twMsg,
          warnings: allWarnings
        }, Math.max(readCurrentSheetHeaders_(sheet).length, 1), page.pageName);
        writePageStatus_(page.pageName, {
          status: 'PARTIAL',
          message: 'Batch aborted at ' + checkpoint.nextIndex + ' of ' + checkpoint.totalSymbols + ' (concurrent writer)',
          endpoint: endpointUsed,
          httpCode: '',
          rows: Math.max(0, sheet.getLastRow() - 1),
          columns: readCurrentSheetHeaders_(sheet).length,
          durationMs: new Date().getTime() - runtimeStart,
          warnings: allWarnings
        });
        logRun_(
          'WARN',
          'refreshPageInBatches',
          page.pageName,
          twMsg,
          {
            status: 'PARTIAL',
            endpoint: endpointUsed,
            httpCode: '',
            durationMs: new Date().getTime() - runtimeStart,
            nextIndex: checkpoint.nextIndex,
            totalSymbols: checkpoint.totalSymbols,
            reason: 'concurrent_writer',
            rowsBefore: fpBaseline ? fpBaseline.rows : null,
            rowsNow: fpNow.rows,
            warnings: allWarnings
          }
        );
        return {
          ok: true,
          pageName: page.pageName,
          partial: true,
          paused: true,
          aborted: 'concurrent_writer',
          nextIndex: checkpoint.nextIndex,
          totalSymbols: checkpoint.totalSymbols,
          endpointUsed: endpointUsed,
          warnings: allWarnings,
          canonicalDrift: driftMissing.length
        };
      }
    }
    if ((new Date().getTime() - runtimeStart) >= REFRESH_MAX_RUNTIME_MS_) {
      saveBatchCheckpoint_(page.pageName, checkpoint);
      var pausedWidthValidation = validateExpectedRefreshWidth_(page.pageName, sheet);
      allWarnings = uniqueStrings_(allWarnings.concat(pausedWidthValidation.warnings || []));
      writePageStatusPanel_(sheet, {
        status: 'PARTIAL',
        message: 'Batch refresh paused to avoid execution timeout. Run again to continue.',
        warnings: uniqueStrings_(['Progress: ' + checkpoint.nextIndex + ' / ' + checkpoint.totalSymbols].concat(allWarnings))
      }, Math.max(readCurrentSheetHeaders_(sheet).length, 1), page.pageName);
      writePageStatus_(page.pageName, {
        status: 'PARTIAL',
        message: 'Batch refresh paused at ' + checkpoint.nextIndex + ' of ' + checkpoint.totalSymbols,
        endpoint: endpointUsed,
        httpCode: '',
        rows: Math.max(0, sheet.getLastRow() - 1),
        columns: readCurrentSheetHeaders_(sheet).length,
        durationMs: new Date().getTime() - runtimeStart,
        warnings: allWarnings
      });
      logRun_(
        'WARN',
        'refreshPageInBatches',
        page.pageName,
        'Batch refresh paused before timeout. Run again to continue.',
        {
          status: 'PARTIAL',
          endpoint: endpointUsed,
          httpCode: '',
          durationMs: new Date().getTime() - runtimeStart,
          nextIndex: checkpoint.nextIndex,
          totalSymbols: checkpoint.totalSymbols,
          warnings: allWarnings
        }
      );
      return {
        ok: true,
        pageName: page.pageName,
        partial: true,
        paused: true,
        nextIndex: checkpoint.nextIndex,
        totalSymbols: checkpoint.totalSymbols,
        endpointUsed: endpointUsed,
        warnings: allWarnings,
        canonicalDrift: driftMissing.length
      };
    }
    /* v1.20.0: a chunk never mixes the requested list with the retry/requeue tail (a first try never
     * shares a request with a symbol that already failed). A checkpoint without baseCount is cut as before. */
    var chunkEnd = checkpoint.nextIndex + batchSize;
    if (typeof checkpoint.baseCount === 'number' && checkpoint.nextIndex < checkpoint.baseCount &&
        chunkEnd > checkpoint.baseCount) {
      chunkEnd = checkpoint.baseCount;
    }
    var chunk = checkpoint.symbols.slice(checkpoint.nextIndex, chunkEnd);
    /* v1.19.0: fingerprint the backend-hold row before the slow fetch. */
    var backendHoldSnap = _refreshBackendHoldSnapshot_();
    try {
      var fetchResult = fetchPageData_(page.pageName, {
        endpointCandidates: endpointCandidates,
        payloadOverrides: mergeObjects_({}, options.payloadOverrides || {}, {
          symbols: chunk,
          tickers: chunk,
          direct_symbols: chunk,
          limit: Math.max(chunk.length, batchSize)
        })
      });
      if (!fetchResult || !fetchResult.ok) {
        failedBatches++;
        allWarnings.push('Batch failed: ' + chunk.join(', '));
        totalRetried += _refreshNoteFailedChunk_(checkpoint, chunk, failedRetryMode, REFRESH_FAILED_RETRY_MAX_);   // v1.20.0
        checkpoint.nextIndex += chunk.length;
        saveBatchCheckpoint_(page.pageName, checkpoint);

        // v1.18.0: the failed chunk has been accounted for and checkpointed;
        // this is a safe place to yield before the next request.
        var manualPriorityAfterFailedChunk = _refreshManualPriorityProbe_(
          'refreshPageInBatchesCore_:after-failed-chunk:' + page.pageName + ':' + checkpoint.nextIndex
        );
        if (manualPriorityAfterFailedChunk.yield &&
            checkpoint.nextIndex < checkpoint.totalSymbols) {
          return _refreshPauseForManualPriority_({
            pageName: page.pageName,
            sheet: sheet,
            checkpoint: checkpoint,
            priority: manualPriorityAfterFailedChunk,
            endpointUsed: endpointUsed,
            durationMs: new Date().getTime() - runtimeStart,
            warnings: allWarnings,
            successfulBatches: successfulBatches,
            failedBatches: failedBatches,
            verifyIssues: verifyIssues,
            canonicalDrift: driftMissing.length,
            writeStatus: true
          });
        }
        continue;
      }
      /* v1.19.0 BACKEND-HOLD PRE-WRITE TRIPWIRE: last safe instant before
       * this chunk mutates the sheet (writeHeaderRowExact_/upsert below).
       * See the v1.19.0 header block for the field proof and contract. */
      var backendHoldPre = _refreshBackendHoldPreWriteCheck_(
        backendHoldSnap,
        'refreshPageInBatchesCore_:pre-write:' + page.pageName + ':' +
          checkpoint.nextIndex
      );
      if (backendHoldPre.yield) {
        saveBatchCheckpoint_(page.pageName, checkpoint);
        var bhMsg = 'Batch write yielded to backend sync hold (' +
          backendHoldPre.reason + '). Fetched chunk discarded unwritten; ' +
          'checkpoint kept at ' + checkpoint.nextIndex + ' / ' +
          checkpoint.totalSymbols + '; will resume after the hold clears.';
        allWarnings = uniqueStrings_(allWarnings.concat(
          ['[v1.19.0 BACKEND-HOLD] ' + bhMsg]));
        logRun_(
          'WARN',
          'refreshPageInBatches',
          page.pageName,
          bhMsg,
          {
            status: 'PARTIAL',
            action: 'yield-backend-hold',
            reason: backendHoldPre.reason,
            holdDetail: backendHoldPre.detail,
            endpoint: endpointUsed,
            httpCode: '',
            durationMs: new Date().getTime() - runtimeStart,
            nextIndex: checkpoint.nextIndex,
            totalSymbols: checkpoint.totalSymbols,
            warnings: allWarnings
          }
        );
        return {
          ok: true,
          pageName: page.pageName,
          partial: true,
          paused: true,
          aborted: 'backend_hold',
          action: 'yield-backend-hold',
          nextIndex: checkpoint.nextIndex,
          totalSymbols: checkpoint.totalSymbols,
          endpointUsed: endpointUsed,
          warnings: allWarnings,
          canonicalDrift: driftMissing.length
        };
      }
      if (!endpointUsed) {
        endpointUsed = fetchResult.endpointUsed || '';
      }
      var normalized = fetchResult.normalized || {
        headers: [],
        keys: [],
        rows2d: [],
        rowObjects: []
      };
      if (!useHeaderMappedBatch) {
        if (normalized.headers && normalized.headers.length && !arraysEqual_(normalized.headers, headers)) {
          headers = normalized.headers.slice();
          writeHeaderRowExact_(sheet, headers);
        }
      }
      var rows2d = dedupeRows2dBySymbol_(normalized.rows2d || [], headers);
      var rowObjects = Array.isArray(normalized.rowObjects) ? normalized.rowObjects.slice() : [];
      /* v1.14.0 KEEP-LAST-GOOD: withhold empty-partial rows BEFORE the
       * provenance stamp and the write — a row whose price the backend
       * could not produce must never overwrite a row that has one. The
       * upsert never deletes, so dropping only skips that symbol's update
       * this run; the next scheduled fire retries it naturally via the
       * sheet echo. */
      if (klgIdentityMap) {
        var klgObjRes = _refreshFilterEmptyPartialRowObjects_(page.pageName, rowObjects, klgIdentityMap);
        if (klgObjRes.withheld.length) {
          rowObjects = klgObjRes.kept;
          totalKeptLastGood += klgObjRes.withheld.length;
          for (var kwI = 0; kwI < klgObjRes.withheld.length; kwI++) {
            klgWithheldAll.push(klgObjRes.withheld[kwI]);
          }
        }
        var klg2dRes = _refreshFilterEmptyPartialRows2d_(page.pageName, headers, rows2d, klgIdentityMap);
        if (klg2dRes.withheld.length) {
          rows2d = klg2dRes.kept;
          totalKeptLastGood += klg2dRes.withheld.length;
          for (var kw2 = 0; kw2 < klg2dRes.withheld.length; kw2++) {
            klgWithheldAll.push(klg2dRes.withheld[kw2]);
          }
        }
      }
      if (useHeaderMappedBatch && rowObjects.length) {
        var provSummary = _injectProvenanceFields_(rowObjects, fetchResult, page.pageName);
        totalProvenanceFilled += (provSummary.filledUtc || 0);
      }
      if (rows2d.length || rowObjects.length) {
        if (useHeaderMappedBatch && rowObjects.length === 0 && rows2d.length > 0) {
          _logEmptyRowObjectsWarning_(page.pageName, batchSize, chunk);
          allWarnings.push('Header-mapped batch fell to legacy (empty rowObjects): ' + chunk.join(','));
        }
        var upsertResult = upsertRowsBySymbol_(
          sheet,
          headers,
          rows2d,
          preserveColumns,
          preserveOnEmpty,
          {
            rowObjects: rowObjects,
            pageName: page.pageName,
            legacyPath: legacyPathRequested
          }
        );
        if (upsertResult && typeof upsertResult === 'object') {
          totalUpdated += upsertResult.updated || 0;
          totalAppended += upsertResult.appended || 0;
          totalSkipped += upsertResult.skipped || 0;
          totalPlacementAborted += upsertResult.placementAborted || 0;
          totalAppendAborted += upsertResult.appendAborted || 0;
          totalSecondChance += upsertResult.secondChanceUpdated || 0;
          /* v1.13.0 LOSSLESS DEFERRAL: symbols the 02_Core guards withheld
           * (place-guard block aborts + append-guard aborts) are re-queued
           * ONCE into this run's checkpoint tail so their fresh data is
           * retried after the concurrent window passes — never dropped. */
          var defSyms = upsertResult.deferredSymbols || [];
          for (var dqI = 0; dqI < defSyms.length; dqI++) {
            var dqSym = String(defSyms[dqI] || '').trim().toUpperCase();
            if (!dqSym || checkpoint.requeued[dqSym]) continue;
            checkpoint.requeued[dqSym] = 1;
            checkpoint.symbols.push(dqSym);
            checkpoint.totalSymbols++;
            totalDeferred++;
          }
          if (Array.isArray(upsertResult.unmappedHeaders) && upsertResult.unmappedHeaders.length) {
            allWarnings.push('Unmapped sheet columns (left blank): ' + upsertResult.unmappedHeaders.join(', '));
          }
        }
        /* v1.13.0: our own writes moved the tail — re-baseline so the
         * tripwire trips only on FOREIGN changes, never on ourselves. */
        if (tripwireOn) {
          fpBaseline = _refreshCaptureFingerprint_(sheet, symbolColNumberTw);
        }
        if (useHeaderMappedBatch && rowObjects.length > 0) {
          var batchVerify = _verifyCriticalFieldsAfterWrite_(sheet, page.pageName, rowObjects);
          if (batchVerify && batchVerify.ok === false) {
            verifyIssues++;
            var diagKey = batchVerify.diagnosis || 'UNKNOWN';
            verifyDiagnoses[diagKey] = (verifyDiagnoses[diagKey] || 0) + 1;
            var batchBlankFields = (batchVerify.blanks || []).map(function (b) { return b.field; }).join(',');
            allWarnings.push(
              'Batch verify [' + batchVerify.diagnosis + ']: blank ' +
              batchBlankFields + ' on ' + batchVerify.sampled +
              ' (chunk=' + chunk.join(',') + ')'
            );
          }
        }
      }
      if (fetchResult.warnings && fetchResult.warnings.length) {
        allWarnings = allWarnings.concat(fetchResult.warnings);
      }
      successfulBatches++;
      totalRecovered += _refreshClearFailedChunk_(checkpoint, chunk);   // v1.20.0
    } catch (err) {
      failedBatches++;
      allWarnings.push(getRefreshErrorMessage_(err));
      totalRetried += _refreshNoteFailedChunk_(checkpoint, chunk, failedRetryMode, REFRESH_FAILED_RETRY_MAX_);   // v1.20.0
    }
    checkpoint.nextIndex += chunk.length;
    saveBatchCheckpoint_(page.pageName, checkpoint);

    // v1.18.0 critical cooperative boundary: the chunk's fetch, write,
    // verification and checkpoint save are complete. Yielding here cannot
    // leave a cleared range or a half-written row block.
    var manualPriorityAfterCheckpoint = _refreshManualPriorityProbe_(
      'refreshPageInBatchesCore_:after-checkpoint:' + page.pageName + ':' + checkpoint.nextIndex
    );
    if (manualPriorityAfterCheckpoint.yield &&
        checkpoint.nextIndex < checkpoint.totalSymbols) {
      return _refreshPauseForManualPriority_({
        pageName: page.pageName,
        sheet: sheet,
        checkpoint: checkpoint,
        priority: manualPriorityAfterCheckpoint,
        endpointUsed: endpointUsed,
        durationMs: new Date().getTime() - runtimeStart,
        warnings: allWarnings,
        successfulBatches: successfulBatches,
        failedBatches: failedBatches,
        verifyIssues: verifyIssues,
        canonicalDrift: driftMissing.length,
        writeStatus: true
      });
    }
  }
  allWarnings = uniqueStrings_(allWarnings);
  /* v1.20.0 [FAILED-LEDGER]: read the ledger before the checkpoint goes. */
  var unwrittenSymbols = _refreshFailedSymbols_(checkpoint);
  clearBatchCheckpoint_(page.pageName);
  if (unwrittenSymbols.length || totalRetried) {
    var ledgerList = unwrittenSymbols.slice(0, 20).join(',') +
      (unwrittenSymbols.length > 20 ? ',...(+' + (unwrittenSymbols.length - 20) + ')' : '');
    if (failedRetryMode === 'on') {
      allWarnings.push('[v1.20.0 FAILED-LEDGER] retried=' + totalRetried + ' recovered=' + totalRecovered +
        ' unwritten=' + unwrittenSymbols.length + (unwrittenSymbols.length ? ' (previous rows kept): ' + ledgerList : ''));
    } else {
      allWarnings.push('[v1.20.0 FAILED-LEDGER observe] ' + unwrittenSymbols.length +
        ' symbol(s) in failed batches kept their previous rows: ' + ledgerList +
        '; with REFRESH_FAILED_RETRY=on each would be retried once and the run would close PARTIAL if any stayed unwritten.');
    }
  }
  /* v1.13.0: surface the run's guard telemetry once. */
  if (totalDeferred || totalAppendAborted || totalSecondChance || totalPlacementAborted) {
    allWarnings.push('[v1.13.0 GUARDS] place_aborted=' + totalPlacementAborted +
      ' append_aborted=' + totalAppendAborted +
      ' second_chance=' + totalSecondChance +
      ' requeued=' + totalDeferred);
  }
  /* v1.14.0: surface the run's keep-last-good telemetry once. */
  if (totalKeptLastGood) {
    allWarnings.push('[v1.14.0 KEEP_LAST_GOOD] withheld=' + totalKeptLastGood +
      ' empty-partial row(s) (blank Price over existing data); previous values kept: ' +
      klgWithheldAll.slice(0, 15).join(',') +
      (klgWithheldAll.length > 15 ? ',...(+' + (klgWithheldAll.length - 15) + ')' : ''));
  }
  /* v1.13.0 TRIPWIRE BELT: the v1.12.4 dedupe compaction rewrites the
   * whole data area from one read — the single most dangerous op against
   * a concurrent writer (the exact v1.12.7 concern). Skip it on drift. */
  var skipDedupeForDrift = false;
  if (tripwireOn) {
    var fpPre = _refreshCaptureFingerprint_(sheet, symbolColNumberTw);
    if (_refreshFingerprintDrift_(fpBaseline, fpPre)) {
      skipDedupeForDrift = true;
      allWarnings.push('[v1.13.0 TRIPWIRE] dedupe compaction skipped: concurrent writer detected at completion.');
    }
  }
  /* v1.12.4: scoped post-write symbol dedupe — THE call site that matters for
   * the four market sheets. Market_Leaders / Global_Markets / Commodities_FX /
   * Mutual_Funds self-source ~203 unique symbols (getManualSymbolsFromSheet_),
   * exceed REFRESH_BATCH_SIZE_DEFAULT_=10, and AUTO-BATCH through here. Each
   * per-chunk upsertRowsBySymbol_ -> 02_Core writeRowsBySymbol_ builds a
   * LAST-WINS symbolToRow map: a symbol that already exists twice on the sheet
   * (fresh GICS row + stale "Saudi Market" row) refreshes the later physical
   * copy and ORPHANS the earlier one forever (the writer never deletes). That
   * is the 406-data-row (~203x2) duplication. This pass collapses the sheet to
   * one row per symbol (freshest copy) AFTER the loop fully completes. The
   * paused/partial timeout branch returns earlier (inside the while loop above),
   * so this NEVER runs on a half-finished sheet. It runs BEFORE the sort/format/
   * validation below so the sort operates on deduped rows and the Global_Markets
   * "mixed refresh dates" validator sees the cleaned sheet. Scoped to the four
   * pure-instrument pages only; NO-OP on a clean sheet (byte-identical to
   * v1.12.3); self-guards on every precondition; never throws. Self-heals the
   * existing 406 -> ~203 on the next full Market_Leaders refresh. */
  if (!skipDedupeForDrift) {
    _refreshDedupeInstrumentSheetBySymbol_(sheet, page.pageName);
  }
  var expectedWBatch = _findExpectedColumnCountForPage_(page.pageName);
  if (expectedWBatch) {
    var actualWBatch = readCurrentSheetHeaders_(sheet).length;
    _logCanonicalWidth_(page.pageName, expectedWBatch, actualWBatch);
  }
  if (options.applyFormat !== false) {
    applySheetFormatting_(page.pageName);
  }
  if (options.applySort !== false) {
    sortAndRankPageCore_(page.pageName);
  }
  var postValidation = validatePostRefreshIntegrity_(page.pageName, sheet, {
    expectedSymbolCount: symbols.length
  });
  allWarnings = uniqueStrings_(allWarnings.concat(postValidation.warnings || []));
  var batchSeverity = failedBatches || verifyIssues || !postValidation.ok ? 'WARN' : 'INFO';
  var batchStatus = failedBatches || verifyIssues || !postValidation.ok ? 'WARN' : 'OK';
  var batchMsg;
  /* v1.20.0 [FAILED-LEDGER] mode 'on': the ledger, not the failed-batch count, decides. */
  var ledgerOn = failedRetryMode === 'on';
  if (ledgerOn) {
    batchStatus = unwrittenSymbols.length ? 'PARTIAL' : (verifyIssues || !postValidation.ok ? 'WARN' : 'OK');
    batchSeverity = batchStatus === 'OK' ? 'INFO' : 'WARN';
  }
  if (ledgerOn && unwrittenSymbols.length) {
    batchMsg = 'Batch refresh partial: ' + unwrittenSymbols.length + ' symbol(s) stayed unwritten after one retry and keep their previous rows' +
      (verifyIssues ? '; ' + verifyIssues + ' verify issue(s)' : '') + '.';
  } else if (ledgerOn && !verifyIssues && postValidation.ok) {
    batchMsg = totalRecovered ? 'Batch refresh completed successfully; the retry recovered ' + totalRecovered + ' symbol(s) of failed batches.'
      : 'Batch refresh completed successfully.';
  } else if (ledgerOn && verifyIssues) {
    batchMsg = 'Batch refresh completed with ' + verifyIssues + ' verify issue(s).';
  } else if (ledgerOn) {
    batchMsg = 'Batch refresh completed but post-refresh validation found warnings.';
  } else if (failedBatches && verifyIssues) {
    batchMsg = 'Batch refresh completed with ' + failedBatches + ' failed batch(es), ' + verifyIssues + ' verify issue(s), and validation status=' + postValidation.ok + '.';
  } else if (failedBatches) {
    batchMsg = 'Batch refresh completed with ' + failedBatches + ' failed batch(es).';
  } else if (verifyIssues) {
    batchMsg = 'Batch refresh completed with ' + verifyIssues + ' verify issue(s).';
  } else if (!postValidation.ok) {
    batchMsg = 'Batch refresh completed but post-refresh validation found warnings.';
  } else {
    batchMsg = 'Batch refresh completed successfully.';
  }
  var batchPanelStatus = batchStatus === 'OK' ? 'SUCCESS' : (batchStatus === 'PARTIAL' ? 'PARTIAL' : 'WARN');   // v1.20.0
  writePageStatusPanel_(sheet, {
    status: batchPanelStatus,
    message: batchMsg,
    warnings: allWarnings
  }, Math.max(readCurrentSheetHeaders_(sheet).length, 1), page.pageName);
  logRun_(
    batchSeverity,
    'refreshPageInBatches',
    page.pageName,
    batchMsg,
    {
      status: batchStatus,
      endpoint: endpointUsed,
      httpCode: '',
      durationMs: new Date().getTime() - runtimeStart,
      rows: Math.max(0, sheet.getLastRow() - 1),
      columns: readCurrentSheetHeaders_(sheet).length,
      canonicalDrift: driftMissing.length,
      warnings: allWarnings,
      successfulBatches: successfulBatches,
      failedBatches: failedBatches,
      verifyIssues: verifyIssues,
      verifyDiagnoses: verifyDiagnoses,
      provenanceFilled: totalProvenanceFilled,
      symbolsProcessed: symbols.length,
      updated: totalUpdated,
      appended: totalAppended,
      skipped: totalSkipped,
      keptLastGood: totalKeptLastGood,
      failedLedger: unwrittenSymbols.length,
      failedRetried: totalRetried,
      failedRecovered: totalRecovered,
      failedRetryMode: failedRetryMode,
      postValidationOk: postValidation.ok,
      path: useHeaderMappedBatch ? 'header-mapped' : (legacyPathRequested ? 'positional-forced' : 'positional')
    }
  );
  writePageStatus_(page.pageName, {
    status: batchPanelStatus,
    message: batchMsg,
    endpoint: endpointUsed,
    httpCode: '',
    rows: Math.max(0, sheet.getLastRow() - 1),
    columns: readCurrentSheetHeaders_(sheet).length,
    durationMs: new Date().getTime() - runtimeStart,
    warnings: allWarnings
  });
  return {
    ok: true,
    pageName: page.pageName,
    batchSize: batchSize,
    symbolsProcessed: symbols.length,
    rowsWritten: Math.max(0, sheet.getLastRow() - 1),
    successfulBatches: successfulBatches,
    failedBatches: failedBatches,
    verifyIssues: verifyIssues,
    verifyDiagnoses: verifyDiagnoses,
    provenanceFilled: totalProvenanceFilled,
    warnings: allWarnings,
    endpointUsed: endpointUsed,
    updated: totalUpdated,
    appended: totalAppended,
    skipped: totalSkipped,
    keptLastGood: totalKeptLastGood,
    keptLastGoodSymbols: klgWithheldAll,
    partial: batchStatus === 'PARTIAL',
    unwrittenSymbols: unwrittenSymbols,
    failedRetried: totalRetried,
    failedRecovered: totalRecovered,
    failedRetryMode: failedRetryMode,
    path: useHeaderMappedBatch ? 'header-mapped' : (legacyPathRequested ? 'positional-forced' : 'positional'),
    canonicalDrift: driftMissing.length,
    postValidation: postValidation
  };
}
function sortAndRankPageCore_(pageName) {
  var page = getPageConfig(pageName);
  /* v1.12.0: sorting rows 2+ would re-order the cockpit's layout zones. */
  if (_refreshIsDecisionOwnedPage_(page.pageName)) {
    return _refreshSkipDecisionOwned_('sortAndRankPage', page.pageName);
  }
  var sheet = ensureSheetExists_(page.pageName);
  var headers = readCurrentSheetHeaders_(sheet);
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow <= 2 || !headers.length || !page.sort || !page.sort.enabled) {
    return {
      ok: true,
      pageName: page.pageName,
      skipped: true
    };
  }
  var sortField = page.sort.activeField || page.sort.fallbackField || '';
  var headerMap = buildHeaderMapRefresh_(headers);
  var sortColumn = sortField ? headerMap[sortField] : null;
  if (!sortColumn) {
    if (headerMap['Top 10 Rank']) {
      sortColumn = headerMap['Top 10 Rank'];
      page.sort.ascending = true;
    } else if (headerMap['Rank (Overall)']) {
      sortColumn = headerMap['Rank (Overall)'];
      page.sort.ascending = true;
    } else if (headerMap['Opportunity Score']) {
      sortColumn = headerMap['Opportunity Score'];
      page.sort.ascending = false;
    } else if (headerMap['Conviction Score']) {
      sortColumn = headerMap['Conviction Score'];
      page.sort.ascending = false;
    } else if (headerMap['Sector-Adj Score']) {
      sortColumn = headerMap['Sector-Adj Score'];
      page.sort.ascending = false;
    } else if (headerMap['Overall Score']) {
      sortColumn = headerMap['Overall Score'];
      page.sort.ascending = false;
    }
  }
  if (!sortColumn) {
    return {
      ok: true,
      pageName: page.pageName,
      skipped: true,
      message: 'No sort field available.'
    };
  }
  var expectedColumnCount = _findExpectedColumnCountForPage_(page.pageName);
  var sortRangeCols;
  if (expectedColumnCount > 0) {
    sortRangeCols = Math.min(expectedColumnCount, headers.length, lastCol);
  } else {
    sortRangeCols = Math.min(headers.length, lastCol);
  }
  if (sortRangeCols < 1) sortRangeCols = 1;
  if (sortColumn > sortRangeCols) {
    sortRangeCols = Math.min(sortColumn, lastCol);
  }
  var range = sheet.getRange(2, 1, lastRow - 1, sortRangeCols);
  range.sort({
    column: sortColumn,
    ascending: !!page.sort.ascending
  });
  return {
    ok: true,
    pageName: page.pageName,
    sortedBy: headers[sortColumn - 1],
    ascending: !!page.sort.ascending,
    sortRangeCols: sortRangeCols,
    expectedColumnCount: expectedColumnCount
  };
}
/* =========================================================
 * Fetch orchestration
 * ========================================================= */
function fetchPageData_(pageName, options) {
  options = options || {};
  var page = getPageConfig(pageName);
  var endpointCandidates = options.endpointCandidates || getEndpointCandidates(page.pageName);
  if (page.pageName === SHEET_NAMES.DATA_DICTIONARY) {
    return fetchDataDictionaryData_(page.pageName, options);
  }
  var best = null;
  for (var i = 0; i < endpointCandidates.length; i++) {
    var endpoint = endpointCandidates[i];
    var request = buildEndpointRequest_(page, endpoint, options);
    var response = executeEndpointRequest_(request);
    var parsed = normalizeEndpointResponse_(page, endpoint, response);
    if (!best) {
      best = parsed;
    }
    var hasRows =
      parsed &&
      parsed.normalized &&
      (
        (parsed.normalized.rows2d || []).length > 0 ||
        (parsed.normalized.rowObjects || []).length > 0
      );
    if (parsed.ok && hasRows) {
      return parsed;
    }
    if (parsed.ok && parsed.allowEmptyRows && parsed.normalized.headers.length > 0) {
      best = parsed;
      return parsed;
    }
    if (parsed.ok && (!best || !best.ok)) {
      best = parsed;
    }
  }
  return best || {
    ok: false,
    pageName: page.pageName,
    message: 'No usable endpoint response.',
    normalized: {
      headers: [],
      keys: [],
      rows2d: [],
      rowObjects: [],
      rowCount: 0,
      rowType: 'none'
    },
    warnings: [],
    endpointUsed: '',
    statusCode: 0,
    durationMs: 0,
    payloadStatus: ''
  };
}
function fetchDataDictionaryData_(pageName, options) {
  options = options || {};
  var page = getPageConfig(pageName);
  var primaryResp = apiGet_(ENDPOINTS.SCHEMA_DATA_DICTIONARY, {}, {
    maxRetries: 2
  });
  var primaryParsed = normalizeEndpointResponse_(page, ENDPOINTS.SCHEMA_DATA_DICTIONARY, primaryResp);
  if (primaryParsed.ok && (primaryParsed.normalized.headers.length || primaryParsed.allowEmptyRows)) {
    return primaryParsed;
  }
  var schema = fetchDataDictionarySchema_();
  if (schema.ok) {
    return {
      ok: true,
      pageName: page.pageName,
      message: 'Data dictionary schema fallback used.',
      normalized: {
        headers: schema.headers || [],
        keys: schema.keys || [],
        rows2d: [],
        rowObjects: [],
        rowCount: 0,
        rowType: 'none'
      },
      warnings: ['Used schema fallback for Data_Dictionary'],
      endpointUsed: schema.source,
      statusCode: schema.response ? schema.response.statusCode : 200,
      durationMs: schema.response ? schema.response.durationMs : 0,
      payloadStatus: 'success',
      allowEmptyRows: true
    };
  }
  return primaryParsed;
}
function buildEndpointRequest_(page, endpoint, options) {
  options = options || {};
  var method = page.defaultMethod || 'POST';
  var endpointOverrides = options.payloadOverrides || {};
  if (page.pageName === SHEET_NAMES.DATA_DICTIONARY) {
    if (endpoint === ENDPOINTS.SCHEMA_DATA_DICTIONARY) {
      return {
        method: 'GET',
        endpoint: endpoint,
        params: {},
        payload: null
      };
    }
    return {
      method: 'GET',
      endpoint: endpoint,
      params: {
        page: page.pageName
      },
      payload: null
    };
  }
  if (endpoint.indexOf('sheet-spec') > -1) {
    return {
      method: 'GET',
      endpoint: endpoint,
      params: {
        page: page.pageName
      },
      payload: null
    };
  }
  var payload = buildRefreshPayload_(page.pageName, endpointOverrides);
  return {
    method: method,
    endpoint: endpoint,
    params: method === 'GET' ? payload : {},
    payload: method === 'GET' ? null : payload
  };
}
function buildRefreshPayload_(pageName, endpointOverrides) {
  endpointOverrides = endpointOverrides || {};
  var payload = buildBasePayload(pageName, {});
  var symbols = resolveRefreshSymbols_(pageName, endpointOverrides);
  payload.symbols = symbols;
  payload.tickers = symbols.slice();
  payload.direct_symbols = symbols.slice();
  Object.keys(endpointOverrides).forEach(function (key) {
    payload[key] = endpointOverrides[key];
  });
  if (endpointOverrides.symbols || endpointOverrides.tickers || endpointOverrides.direct_symbols) {
    var overrideSymbols = resolveRefreshSymbols_(pageName, endpointOverrides);
    payload.symbols = overrideSymbols;
    payload.tickers = overrideSymbols.slice();
    payload.direct_symbols = overrideSymbols.slice();
  }
  return payload;
}
function resolveRefreshSymbols_(pageName, endpointOverrides) {
  endpointOverrides = endpointOverrides || {};
  var explicit = []
    .concat(endpointOverrides.symbols || [])
    .concat(endpointOverrides.tickers || [])
    .concat(endpointOverrides.direct_symbols || []);
  explicit = uniqueStrings_(
    explicit
      .map(function (x) {
        return String(x || '').trim().toUpperCase();
      })
      .filter(function (x) {
        return !!x;
      })
  );
  if (explicit.length) {
    return explicit;
  }
  // v1.11.4: Top_10_Investments is a BACKEND-COMPUTED selection page (selector
  // v4.17.0 scans the full candidate universe). Echoing the sheet's CURRENT 10
  // symbols back as symbols/tickers/direct_symbols would pin the selector's
  // candidate pool to last run's winners — a self-reinforcing list where a
  // displaced symbol could never be replaced — on any backend revision that
  // honors direct_symbols. The verified-correct caller sends EMPTY symbol
  // lists (live criteria snapshots show direct_symbols: []), so the default
  // full-refresh payload now matches it. Explicit overrides above (e.g.
  // selected-row refresh, which passes the one symbol) still win.
  if (pageName === SHEET_NAMES.TOP_10_INVESTMENTS) {
    return [];
  }
  var manualSheetSymbols = getManualSymbolsFromSheet_(pageName);
  if (manualSheetSymbols.length) {
    return manualSheetSymbols;
  }
  return getDefaultSymbols(pageName);
}
function executeEndpointRequest_(request) {
  if (String(request.method).toUpperCase() === 'GET') {
    return apiGet_(request.endpoint, request.params || {}, {
      maxRetries: 2
    });
  }
  return apiPost_(request.endpoint, request.payload || {}, {
    maxRetries: 2
  });
}
function normalizeEndpointResponse_(page, endpoint, response) {
  response = response || {};
  var json = response.json || null;
  var payload = response.payload || unwrapPayload_(json) || json || {};
  var normalized = normalizeTablePayload_(payload);
  var warnings = collectWarnings_(json, payload);
  var payloadStatus = derivePayloadStatus_(json, payload);
  var allowEmptyRows = !!page.allowEmptyRows;
  if (!normalized.headers.length) {
    normalized.headers = getExpectedHeaders_(page.pageName);
  }
  if (!normalized.keys.length) {
    normalized.keys = getExpectedKeys_(page.pageName);
  }
  if (!Array.isArray(normalized.rowObjects)) {
    normalized.rowObjects = [];
  }
  if (!Array.isArray(normalized.rows2d)) {
    normalized.rows2d = [];
  }
  normalized.rowCount = Math.max(
    normalized.rows2d.length,
    normalized.rowObjects.length
  );
  var ok = !!response.ok;
  if (!ok && response.statusCode === 200) {
    ok = true;
  }
  if (payloadStatus === 'partial' && normalized.headers.length) {
    ok = true;
  }
  if (allowEmptyRows && normalized.headers.length) {
    ok = true;
  }
  return {
    ok: ok,
    pageName: page.pageName,
    message: response.message || '',
    normalized: normalized,
    warnings: warnings,
    endpointUsed: endpoint,
    statusCode: response.statusCode || 0,
    durationMs: response.durationMs || 0,
    payloadStatus: payloadStatus,
    allowEmptyRows: allowEmptyRows,
    rawResponse: response
  };
}
function derivePayloadStatus_(json, payload) {
  if (payload && payload.status) return String(payload.status);
  if (json && json.status) return String(json.status);
  return '';
}
function isInsightsTimeoutPartial_(pageName, fetchResult) {
  if (pageName !== SHEET_NAMES.INSIGHTS_ANALYSIS) return false;
  if (!fetchResult) return false;
  var status = String(fetchResult.payloadStatus || '').toLowerCase();
  var warnings = fetchResult.warnings || [];
  var hasTimeout = warnings.join(' | ').toLowerCase().indexOf('timeout') > -1;
  return status === 'partial' && hasTimeout;
}
/* =========================================================
 * Preserve columns — legacy positional path
 * ========================================================= */
function mergePreservedColumns_(headers, rows2d, existingRowsAsObjects, preserveColumns, preserveOnEmptyColumns) {
  headers = headers || [];
  rows2d = rows2d || [];
  existingRowsAsObjects = existingRowsAsObjects || [];
  preserveColumns = preserveColumns || [];
  preserveOnEmptyColumns = preserveOnEmptyColumns || [];
  if (!rows2d.length || (!preserveColumns.length && !preserveOnEmptyColumns.length)) {
    return rows2d;
  }
  var symbolColumn = getSymbolColumnNameFromHeaders_(headers);
  if (!symbolColumn) return rows2d;
  var symbolIndex = findHeaderIndexRefresh_(headers, symbolColumn);
  if (symbolIndex < 0) return rows2d;
  var oldMap = mapExistingRowsBySymbol_(existingRowsAsObjects);
  if (!Object.keys(oldMap).length) return rows2d;
  return rows2d.map(function (row) {
    var out = row.slice();
    var symbol = String(out[symbolIndex] || '').trim().toUpperCase();
    if (!symbol || !oldMap[symbol]) return out;
    var oldRecord = oldMap[symbol];
    preserveColumns.forEach(function (colName) {
      var idx = findHeaderIndexRefresh_(headers, colName);
      if (idx === -1) return;
      var oldValue = oldRecord[colName];
      if (oldValue !== null && oldValue !== undefined && String(oldValue) !== '') {
        out[idx] = oldValue;
      }
    });
    preserveOnEmptyColumns.forEach(function (colName) {
      if (preserveColumns.indexOf(colName) !== -1) return;
      var idx = findHeaderIndexRefresh_(headers, colName);
      if (idx === -1) return;
      var incomingValue = out[idx];
      var incomingIsEmpty = _isBlankRefresh_(incomingValue);
      if (!incomingIsEmpty) return;
      var oldValue = oldRecord[colName];
      if (oldValue !== null && oldValue !== undefined && String(oldValue) !== '') {
        out[idx] = oldValue;
      }
    });
    return out;
  });
}
function mergeSingleRowPreservedColumns_(headers, row2d, oldRecord, preserveColumns, preserveOnEmptyColumns) {
  var out = row2d.slice();
  preserveColumns = preserveColumns || [];
  preserveOnEmptyColumns = preserveOnEmptyColumns || [];
  oldRecord = oldRecord || {};
  preserveColumns.forEach(function (colName) {
    var idx = findHeaderIndexRefresh_(headers, colName);
    if (idx === -1) return;
    var oldValue = oldRecord[colName];
    if (oldValue !== null && oldValue !== undefined && String(oldValue) !== '') {
      out[idx] = oldValue;
    }
  });
  preserveOnEmptyColumns.forEach(function (colName) {
    if (preserveColumns.indexOf(colName) !== -1) return;
    var idx = findHeaderIndexRefresh_(headers, colName);
    if (idx === -1) return;
    var incomingValue = out[idx];
    var incomingIsEmpty = _isBlankRefresh_(incomingValue);
    if (!incomingIsEmpty) return;
    var oldValue = oldRecord[colName];
    if (oldValue !== null && oldValue !== undefined && String(oldValue) !== '') {
      out[idx] = oldValue;
    }
  });
  return out;
}
function mapExistingRowsBySymbol_(rows) {
  var map = {};
  rows = rows || [];
  rows.forEach(function (row) {
    var symbol = extractRowSymbol_(row);
    if (!symbol) return;
    map[symbol] = row;
  });
  return map;
}
function extractRowSymbol_(row) {
  if (!row) return '';
  var candidates = [
    row.Symbol,
    row.symbol,
    row.Ticker,
    row.ticker
  ];
  for (var i = 0; i < candidates.length; i++) {
    var s = String(candidates[i] || '').trim().toUpperCase();
    if (s) return s;
  }
  return '';
}
function getSymbolColumnNameFromHeaders_(headers) {
  var candidates = ['Symbol', 'symbol', 'Ticker', 'ticker'];
  for (var i = 0; i < candidates.length; i++) {
    var idx = findHeaderIndexRefresh_(headers, candidates[i]);
    if (idx > -1) {
      return headers[idx];
    }
  }
  return '';
}
/* =========================================================
 * Symbol working set
 * ========================================================= */
function getWorkingSymbolsForPage_(pageName) {
  var manualSymbols = getManualSymbolsFromSheet_(pageName);
  if (manualSymbols.length) return manualSymbols;
  return getDefaultSymbols(pageName);
}
function getManualSymbolsFromSheet_(pageName) {
  var page = getPageConfig(pageName);
  var sheet = ensureSheetExists_(page.pageName);
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow <= 1 || lastCol < 1) return [];
  var headers = readCurrentSheetHeaders_(sheet);
  var symbolColumnName = getSymbolColumnNameFromHeaders_(headers);
  if (!symbolColumnName) return [];
  var symbolIndex = findHeaderIndexRefresh_(headers, symbolColumnName);
  if (symbolIndex < 0) return [];
  var values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  var symbols = [];
  for (var i = 0; i < values.length; i++) {
    var symbol = String(values[i][symbolIndex] || '').trim().toUpperCase();
    if (symbol) {
      symbols.push(symbol);
    }
  }
  // v1.12.5 UNIVERSE FILTER: drop deny-listed junk from the sheet-echoed
  // universe at the SINGLE chokepoint every echo path crosses
  // (getWorkingSymbolsForPage_, resolveRefreshSymbols_,
  // getRequestedSymbolsFromOptions_ all land here). A placeholder like
  // TICK014.US that ever reaches a sheet would otherwise be re-requested
  // and re-written on every refresh forever, because the upsert writer
  // never deletes. Explicit operator-passed symbols do NOT pass through
  // this function and are deliberately unfiltered. Default deny: ^TICK\d+;
  // Script Property TFB_UNIVERSE_DENY overrides; 'off' disables (see the
  // v1.12.5 header block).
  var denyPatterns = _refreshUniverseDenyPatterns_();
  if (denyPatterns.length && symbols.length) {
    var keptSymbols = [];
    var deniedCount = 0;
    for (var d = 0; d < symbols.length; d++) {
      if (_refreshIsDeniedSymbol_(symbols[d], denyPatterns)) {
        deniedCount++;
      } else {
        keptSymbols.push(symbols[d]);
      }
    }
    if (deniedCount > 0) {
      try {
        Logger.log('[v1.12.5 UNIVERSE_FILTER][' + page.pageName + '] dropped=' + deniedCount +
          ' of ' + symbols.length + ' sheet-echoed symbol(s) matching deny pattern(s); kept=' + keptSymbols.length);
      } catch (ufLogErr) {}
    }
    symbols = keptSymbols;
  }
  return uniqueStrings_(symbols);
}
/* =========================================================
 * Visible status panel
 * ========================================================= */
function writePageStatusPanel_(sheet, statusObj, headersLength, pageName) {
  statusObj = statusObj || {};
  // v1.11.2:
  // Do NOT write a visible panel into row 1 of data sheets. The previous
  // 4x2 Status/Message/Warnings/Updated panel was written beyond the schema
  // width and therefore made correct sheets look like they had extra headers.
  // Status details are already persisted through writePageStatus_ and logRun_.
  try {
    if (pageName && sheet) {
      trimSheetToExpectedColumns_(pageName, {
        sheet: sheet,
        reason: 'write_status_panel_suppressed',
        silent: true
      });
    }
    Logger.log('[v1.11.2 STATUS_PANEL_SUPPRESSED][' + (pageName || '<unknown>') + '] ' +
      'status=' + sanitizeStringRefresh_(statusObj.status || '') +
      ' message=' + sanitizeStringRefresh_(statusObj.message || '') +
      ' warnings=' + (Array.isArray(statusObj.warnings) ? statusObj.warnings.length : (statusObj.warnings ? 1 : 0)));
  } catch (e) {}
}
/* =========================================================
 * Shared fallback-warning helper
 * ========================================================= */
function appendGenericFallbackWarning_(pageName, result) {
  if (!result || !result.endpointUsed) return result;
  var page = getPageConfig(pageName);
  var primaryEndpoint = page.primaryEndpoint;
  if (result.endpointUsed === primaryEndpoint) return result;
  var sheet = ensureSheetExists_(pageName);
  var headers = readCurrentSheetHeaders_(sheet);
  var fallbackWarnings = uniqueStrings_(
    (result.warnings || []).concat(['Fallback endpoint used: ' + result.endpointUsed])
  );
  writePageStatusPanel_(sheet, {
    status: result.partial ? 'PARTIAL' : 'SUCCESS',
    message: pageName + ' refreshed via fallback: ' + result.endpointUsed,
    warnings: fallbackWarnings
  }, Math.max(headers.length, 1), pageName);
  writePageStatus_(pageName, {
    status: result.partial ? 'PARTIAL' : 'SUCCESS',
    message: 'Fallback endpoint used for ' + pageName + ': ' + result.endpointUsed,
    endpoint: result.endpointUsed,
    httpCode: '',
    rows: result.rowCount || '',
    columns: headers.length || '',
    durationMs: '',
    warnings: fallbackWarnings
  });
  logRun_(
    'WARN',
    'refresh' + pageName.replace(/_/g, '') + 'Fallback',
    pageName,
    'Fallback endpoint was used for ' + pageName + ': ' + result.endpointUsed,
    {
      status: 'WARN',
      endpoint: result.endpointUsed,
      httpCode: '',
      durationMs: '',
      warnings: fallbackWarnings,
      primaryEndpoint: primaryEndpoint
    }
  );
  result.warnings = fallbackWarnings;
  result.usedFallback = true;
  result.fallbackEndpoint = result.endpointUsed;
  result.primaryEndpoint = primaryEndpoint;
  return result;
}
/* =========================================================
 * Batch / timeout helpers
 * ========================================================= */
function shouldAutoBatchRefresh_(pageName, options) {
  options = options || {};
  if (options.allowAutoBatch === false) return false;
  if (options.autoBatch) return false;
  // v1.11.5: Top_10_Investments joins the never-auto-batch list. It is a
  // single-call BACKEND-COMPUTED page — there is nothing to chunk. Letting it
  // auto-batch made getRequestedSymbolsFromOptions_ echo the sheet's current
  // symbols (17 stale rows > batch size 10 on 2026-06-11), engaging the batch
  // planner which sent those symbols back as direct_symbols and pinned the
  // selector's candidate pool to itself.
  if (
    pageName === SHEET_NAMES.DATA_DICTIONARY ||
    pageName === SHEET_NAMES.INSIGHTS_ANALYSIS ||
    pageName === SHEET_NAMES.TOP_10_INVESTMENTS
  ) {
    return false;
  }
  var symbols = getRequestedSymbolsFromOptions_(pageName, options);
  return symbols.length > _refreshBatchSizeDefault_();
}
function getBatchCheckpointKey_(pageName) {
  return REFRESH_CHECKPOINT_PREFIX_ + pageName;
}
function loadBatchCheckpoint_(pageName) {
  var props = PropertiesService.getScriptProperties();
  var raw = props.getProperty(getBatchCheckpointKey_(pageName));
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}
function saveBatchCheckpoint_(pageName, checkpoint) {
  var props = PropertiesService.getScriptProperties();
  props.setProperty(getBatchCheckpointKey_(pageName), JSON.stringify(checkpoint));
}
function clearBatchCheckpoint_(pageName) {
  var props = PropertiesService.getScriptProperties();
  props.deleteProperty(getBatchCheckpointKey_(pageName));
}
function isCheckpointCompatible_(checkpoint, symbols, batchSize, pageName) {
  if (!checkpoint) return false;
  if (checkpoint.pageName !== pageName) return false;
  if (checkpoint.batchSize !== batchSize) return false;
  if (!Array.isArray(checkpoint.symbols)) return false;
  /* v1.20.0: a retry or a requeue lengthens the list; the requested prefix decides. */
  if (typeof checkpoint.baseCount === 'number' && checkpoint.baseCount === symbols.length &&
      checkpoint.symbols.length >= symbols.length) {
    return arraysEqual_(checkpoint.symbols.slice(0, symbols.length), symbols);
  }
  if (checkpoint.symbols.length !== symbols.length) return false;
  return arraysEqual_(checkpoint.symbols, symbols);
}
/* v1.20.0 [FAILED-LEDGER]: 'on' = retry once and close PARTIAL; anything else = 'observe'. */
function _refreshFailedRetryMode_() {
  try {
    var v = PropertiesService.getScriptProperties().getProperty(REFRESH_FAILED_RETRY_PROPERTY_);
    return String(v === null || v === undefined ? '' : v).trim().toLowerCase() === 'on' ? 'on' : 'observe';
  } catch (e) {
    return 'observe';
  }
}
/* v1.20.0 PURE (on the checkpoint object): the symbols of a failed chunk go on checkpoint.failed
 * (attempts counted); in mode 'on' each one not yet retried is put once at the tail of the run,
 * at most cap symbols a run. -> the number put back. */
function _refreshNoteFailedChunk_(checkpoint, chunk, mode, cap) {
  checkpoint.failed = checkpoint.failed || {};
  checkpoint.retried = checkpoint.retried || {};
  var added = 0;
  var used = 0;
  for (var k in checkpoint.retried) {
    if (Object.prototype.hasOwnProperty.call(checkpoint.retried, k)) used++;
  }
  for (var i = 0; i < (chunk || []).length; i++) {
    var sym = String(chunk[i] || '').trim().toUpperCase();
    if (!sym) continue;
    checkpoint.failed[sym] = (checkpoint.failed[sym] || 0) + 1;
    if (mode !== 'on' || checkpoint.retried[sym] || used >= cap) continue;
    checkpoint.retried[sym] = 1;
    used++;
    checkpoint.symbols.push(chunk[i]);
    checkpoint.totalSymbols++;
    added++;
  }
  return added;
}
/* v1.20.0 PURE: a chunk that was fetched and written takes its symbols off the ledger. */
function _refreshClearFailedChunk_(checkpoint, chunk) {
  if (!checkpoint.failed) return 0;
  var cleared = 0;
  for (var i = 0; i < (chunk || []).length; i++) {
    var sym = String(chunk[i] || '').trim().toUpperCase();
    if (sym && Object.prototype.hasOwnProperty.call(checkpoint.failed, sym)) {
      delete checkpoint.failed[sym];
      cleared++;
    }
  }
  return cleared;
}
/* v1.20.0 PURE: the symbols still on the ledger, sorted. */
function _refreshFailedSymbols_(checkpoint) {
  var out = [];
  var f = (checkpoint && checkpoint.failed) || {};
  for (var k in f) {
    if (Object.prototype.hasOwnProperty.call(f, k)) out.push(k);
  }
  return out.sort();
}
function upsertRowsBySymbol_(sheet, headers, rows2d, preserveColumns, preserveOnEmptyColumns, options) {
  headers = headers || [];
  rows2d = rows2d || [];
  preserveColumns = preserveColumns || [];
  preserveOnEmptyColumns = preserveOnEmptyColumns || [];
  options = options || {};
  if (!headers.length || (!rows2d.length && !(options.rowObjects || []).length)) {
    return;
  }
  var legacyPathRequested = options.legacyPath === true;
  if (
    !legacyPathRequested &&
    _refreshCanUseHeaderMap_() &&
    options.pageName &&
    Array.isArray(options.rowObjects) &&
    options.rowObjects.length
  ) {
    _logActualWriterPath_(options.pageName, 'HEADER_MAPPED', 'rowObjects=' + options.rowObjects.length);
    return writeRowsBySymbol_(sheet, options.pageName, options.rowObjects, {
      preserveHeaderRows: 1,
      symbolApiKey: 'symbol',
      symbolHeaderName: 'Symbol',
      preserveManualColumns: preserveColumns,
      preserveOnEmptyColumns: preserveOnEmptyColumns,
      allowAppend: true
    });
  }
  var v192LegacyReason;
  if (legacyPathRequested) {
    v192LegacyReason = 'LEGACY_FORCED';
  } else if (!_refreshCanUseHeaderMap_()) {
    v192LegacyReason = 'LEGACY_NO_HEADER_MAP';
  } else if (!options.pageName) {
    v192LegacyReason = 'LEGACY_NO_PAGE_NAME';
  } else if (!Array.isArray(options.rowObjects) || !options.rowObjects.length) {
    v192LegacyReason = 'LEGACY_NO_ROW_OBJECTS';
  } else {
    v192LegacyReason = 'LEGACY_UNKNOWN';
  }
  _logActualWriterPath_(options.pageName || '<unknown>', v192LegacyReason,
    'rows2d=' + rows2d.length + ' rowObjects=' + ((options.rowObjects || []).length || 0));
  if (!rows2d.length) return;
  var currentHeaders = readCurrentSheetHeaders_(sheet);
  if (!currentHeaders.length || !arraysEqual_(currentHeaders, headers)) {
    writeHeaderRowExact_(sheet, headers);
    currentHeaders = headers.slice();
  }
  var lastCol = Math.max(sheet.getLastColumn(), headers.length);
  if (sheet.getMaxColumns() < lastCol) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), lastCol - sheet.getMaxColumns());
  }
  var rowObjectsExisting = getSheetDataAsObjects_(sheet);
  var symbolRowMap = buildSheetSymbolRowMap_(sheet);
  var rowObjectMap = mapExistingRowsBySymbol_(rowObjectsExisting);
  var legacyUpdated = 0;
  var legacyAppended = 0;
  for (var i = 0; i < rows2d.length; i++) {
    var row = rows2d[i].slice();
    while (row.length < headers.length) {
      row.push('');
    }
    var symbol = extractSymbolFrom2dRow_(row, headers);
    var existingRowNumber = symbol ? symbolRowMap[symbol] : null;
    if (existingRowNumber) {
      var oldRecord = rowObjectMap[symbol] || {};
      if (preserveColumns.length || preserveOnEmptyColumns.length) {
        row = mergeSingleRowPreservedColumns_(
          headers,
          row,
          oldRecord,
          preserveColumns,
          preserveOnEmptyColumns
        );
      }
      sheet.getRange(existingRowNumber, 1, 1, lastCol).clearContent();
      sheet.getRange(existingRowNumber, 1, 1, headers.length).setValues([row.slice(0, headers.length)]);
      legacyUpdated++;
    } else {
      var appendRow = row.slice(0, headers.length);
      sheet.appendRow(appendRow);
      var newRowNumber = sheet.getLastRow();
      if (symbol) {
        symbolRowMap[symbol] = newRowNumber;
      }
      legacyAppended++;
    }
  }
  return {
    updated: legacyUpdated,
    appended: legacyAppended,
    skipped: 0,
    unmappedHeaders: [],
    headersUsed: headers.length
  };
}
function buildSheetSymbolRowMap_(sheet) {
  var map = {};
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow <= 1 || lastCol < 1) return map;
  var headers = readCurrentSheetHeaders_(sheet);
  var symbolColumnName = getSymbolColumnNameFromHeaders_(headers);
  if (!symbolColumnName) return map;
  var symbolIndex = findHeaderIndexRefresh_(headers, symbolColumnName);
  if (symbolIndex < 0) return map;
  var values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  for (var i = 0; i < values.length; i++) {
    var symbol = String(values[i][symbolIndex] || '').trim().toUpperCase();
    if (symbol) {
      map[symbol] = i + 2;
    }
  }
  return map;
}
function extractSymbolFrom2dRow_(row, headers) {
  var symbolColumnName = getSymbolColumnNameFromHeaders_(headers);
  if (!symbolColumnName) return '';
  var idx = findHeaderIndexRefresh_(headers, symbolColumnName);
  if (idx < 0) return '';
  return String(row[idx] || '').trim().toUpperCase();
}
/* =========================================================
 * Selected row helpers
 * ========================================================= */
function getTargetRowContextForPage_(pageName, options) {
  options = options || {};
  var sheet = ensureSheetExists_(pageName);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var activeSheet = ss.getActiveSheet();
  var row = 0;
  if (options.row && parseInt(options.row, 10) > 1) {
    row = parseInt(options.row, 10);
  } else if (activeSheet && activeSheet.getName() === sheet.getName()) {
    try {
      var activeRange = activeSheet.getActiveRange();
      if (activeRange) row = activeRange.getRow();
    } catch (e) {}
  }
  if (row <= 1) {
    row = findFirstDataRowWithSymbol_(sheet);
  }
  if (row <= 1) {
    throw new Error('No usable data row found. Select a data row or make sure the page has at least one populated row.');
  }
  return buildRowContextFromSheet_(sheet, row);
}
function findFirstDataRowWithSymbol_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow <= 1 || lastCol < 1) return 0;
  var headers = readCurrentSheetHeaders_(sheet);
  var symbolColumnName = getSymbolColumnNameFromHeaders_(headers);
  if (!symbolColumnName) return lastRow >= 2 ? 2 : 0;
  var symbolIndex = findHeaderIndexRefresh_(headers, symbolColumnName);
  if (symbolIndex < 0) return lastRow >= 2 ? 2 : 0;
  var values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  for (var i = 0; i < values.length; i++) {
    var symbol = String(values[i][symbolIndex] || '').trim();
    if (symbol) return i + 2;
  }
  return lastRow >= 2 ? 2 : 0;
}
function buildRowContextFromSheet_(sheet, row) {
  var lastCol = sheet.getLastColumn();
  if (lastCol < 1) {
    throw new Error('Sheet has no columns.');
  }
  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(function (h) {
    return String(h || '');
  });
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
function selectBestMatching2dRow_(rows2d, headers, selectedSymbol) {
  rows2d = Array.isArray(rows2d) ? rows2d : [];
  headers = Array.isArray(headers) ? headers : [];
  selectedSymbol = String(selectedSymbol || '').trim().toUpperCase();
  if (!rows2d.length) return null;
  if (!selectedSymbol) return Array.isArray(rows2d[0]) ? rows2d[0].slice() : null;
  var symbolColumnName = getSymbolColumnNameFromHeaders_(headers);
  var symbolIndex = symbolColumnName ? findHeaderIndexRefresh_(headers, symbolColumnName) : -1;
  if (symbolIndex > -1) {
    for (var i = 0; i < rows2d.length; i++) {
      var row = Array.isArray(rows2d[i]) ? rows2d[i] : [];
      var rowSymbol = String(row[symbolIndex] || '').trim().toUpperCase();
      if (rowSymbol === selectedSymbol) {
        return row.slice();
      }
    }
    return null;
  }
  return Array.isArray(rows2d[0]) ? rows2d[0].slice() : null;
}
/* =========================================================
 * Page name helpers
 * ========================================================= */
function normalizePageNameRefresh_(pageName) {
  if (!pageName) {
    throw new Error('Page name is required. Pass one of: ' + getSupportedPages().join(', '));
  }
  var name = String(pageName).trim();
  var supported = getSupportedPages();
  for (var i = 0; i < supported.length; i++) {
    if (supported[i] === name) return name;
  }
  var nameLower = name.toLowerCase();
  for (var j = 0; j < supported.length; j++) {
    if (supported[j].toLowerCase() === nameLower) return supported[j];
  }
  throw new Error('Unknown page: "' + name + '". Supported pages: ' + supported.join(', '));
}
function getActivePageName_() {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) return '';
    var active = ss.getActiveSheet();
    if (!active) return '';
    return active.getName();
  } catch (e) {
    return '';
  }
}
/* =========================================================
 * Utility helpers
 * ========================================================= */
function dedupeRows2dBySymbol_(rows2d, headers) {
  rows2d = Array.isArray(rows2d) ? rows2d : [];
  headers = Array.isArray(headers) ? headers : [];
  if (!rows2d.length) return [];
  var symbolColumnName = getSymbolColumnNameFromHeaders_(headers);
  if (!symbolColumnName) return rows2d;
  var symbolIndex = findHeaderIndexRefresh_(headers, symbolColumnName);
  if (symbolIndex < 0) return rows2d;
  var seen = {};
  var out = [];
  for (var i = 0; i < rows2d.length; i++) {
    var row = Array.isArray(rows2d[i]) ? rows2d[i] : [];
    var symbol = String(row[symbolIndex] || '').trim().toUpperCase();
    if (!symbol) {
      out.push(row.slice());
      continue;
    }
    if (!seen[symbol]) {
      seen[symbol] = true;
      out.push(row.slice());
    }
  }
  return out;
}
function mergeObjects_() {
  var result = {};
  for (var i = 0; i < arguments.length; i++) {
    var obj = arguments[i] || {};
    Object.keys(obj).forEach(function (key) {
      result[key] = obj[key];
    });
  }
  return result;
}
function buildHeaderMapRefresh_(headers) {
  var map = {};
  headers = headers || [];
  for (var i = 0; i < headers.length; i++) {
    var raw = String(headers[i] == null ? '' : headers[i]);
    var trimmed = raw.trim();
    var normalized = normalizeHeaderTextRefresh_(raw);
    if (raw) map[raw] = i + 1;
    if (trimmed) map[trimmed] = i + 1;
    if (normalized) map[normalized] = i + 1;
  }
  return map;
}
function _buildHeaderIndexRefresh_(headers) {
  var map = {};
  headers = headers || [];
  for (var i = 0; i < headers.length; i++) {
    var raw = String(headers[i] == null ? '' : headers[i]).trim();
    if (!raw) continue;
    map[raw] = i;
    map[normalizeHeaderTextRefresh_(raw)] = i;
  }
  return map;
}
function getRequestedSymbolsFromOptions_(pageName, options) {
  options = options || {};
  var overrides = options.payloadOverrides || {};
  var requested = []
    .concat(overrides.symbols || [])
    .concat(overrides.tickers || [])
    .concat(overrides.direct_symbols || [])
    .concat(options.symbols || []);
  requested = uniqueStrings_(
    requested
      .map(function (x) {
        return String(x || '').trim().toUpperCase();
      })
      .filter(function (x) {
        return !!x;
      })
  );
  if (requested.length) return requested;
  // v1.11.5: Top_10_Investments is a BACKEND-COMPUTED selection page. This is
  // the SECOND symbol-derivation site — v1.11.4 fixed resolveRefreshSymbols_
  // (the payload builder) but this function feeds shouldAutoBatchRefresh_ and
  // the batch planner, and its fall-through to getManualSymbolsFromSheet_
  // echoed the sheet's CURRENT symbols: 17 stale rows > batch size 10 engaged
  // auto-batch, which sent those 17 back as symbols/tickers/direct_symbols
  // (live criteria snapshots, 2026-06-11 08:30 UTC — two selector runs whose
  // direct_symbols were exactly the sheet's 17 symbols split 10+7). The
  // correct default for Top_10 is an EMPTY working set; the selector scans the
  // full candidate universe itself. Explicit requested symbols above (e.g.
  // selected-row refresh) still win.
  if (pageName === SHEET_NAMES.TOP_10_INVESTMENTS) {
    return [];
  }
  var manualSheetSymbols = getManualSymbolsFromSheet_(pageName);
  if (manualSheetSymbols.length) return manualSheetSymbols;
  return getDefaultSymbols(pageName);
}
function getRefreshErrorMessage_(err) {
  if (!err) return 'Unknown error.';
  if (err && err.message) return String(err.message);
  return String(err);
}
function normalizeHeaderTextRefresh_(text) {
  if (text === null || text === undefined) return '';
  if (typeof _normalizeHeaderText_ === 'function') {
    return _normalizeHeaderText_(text);
  }
  var s = String(text);
  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}
function findHeaderIndexRefresh_(headers, headerName) {
  headers = Array.isArray(headers) ? headers : [];
  var target = normalizeHeaderTextRefresh_(headerName).toLowerCase();
  for (var i = 0; i < headers.length; i++) {
    var candidate = normalizeHeaderTextRefresh_(headers[i]).toLowerCase();
    if (candidate === target) {
      return i;
    }
  }
  return -1;
}
function _cellStringRefresh_(row, idx) {
  if (!row || idx === undefined || idx < 0 || idx >= row.length) return '';
  var v = row[idx];
  if (v === null || v === undefined) return '';
  return String(v).trim();
}
function _isBlankRefresh_(v) {
  return v === null || v === undefined || String(v).trim() === '';
}
function _toNumberRefresh_(value, fallback) {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'number') {
    if (isNaN(value) || !isFinite(value)) return fallback;
    return value;
  }
  var s = String(value).trim();
  if (!s) return fallback;
  var isPercent = s.indexOf('%') >= 0;
  s = s.replace(/%/g, '').replace(/,/g, '');
  var n = parseFloat(s);
  if (isNaN(n) || !isFinite(n)) return fallback;
  if (isPercent) return n / 100;
  return n;
}
function _toBooleanRefresh_(value, fallback) {
  if (value === null || value === undefined || value === '') return !!fallback;
  if (typeof value === 'boolean') return value;
  var s = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) >= 0) return true;
  if (['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) >= 0) return false;
  return !!fallback;
}
function sanitizeStringRefresh_(value) {
  if (value === null || value === undefined) return '';
  return String(value).trim();
}
