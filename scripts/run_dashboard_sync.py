#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/run_dashboard_sync.py
================================================================================
TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.22.3)
================================================================================
PRODUCTION-HARDENED | ASYNC | NON-BLOCKING | COMPILEALL-SAFE | SCHEMA-FIRST

v6.22.3 fix — KEEP-LAST-GOOD ROWS (L4c STALE-OVER-STUB SUBSTITUTION)
- ROOT CAUSE (2026-07-10 morning + evening workbook audits): Global_Markets
  carries 9 rows with Data Provider = fallback_error and Name/Price EMPTY
  (NVDA, LLY, ADSK, ANET, ETN, KMI, EQT, MO, CHD). The v6.19.0 persistence
  pass protects a symbol the backend OMITS — but a symbol the backend
  answers WITH A DATA-FREE ERROR STUB is "present", passes every membership
  guard, and the stub OVERWRITES the symbol's last good row. On the next
  sync the stub is the baseline: a transient provider failure has become
  permanent data loss, one symbol at a time.
- FIX L4c [KEEP-LAST-GOOD]: on the ranked market pages, AFTER the
  persistence pass and BEFORE the L4b membership verification, pre-scan the
  final matrix for DATA-FREE stub rows — (a) Data Provider normalizing into
  {fallback_error, error, unavailable, none} with no positive price, or
  (b) blank Name AND no positive price. Zero stubs (every healthy sync) =
  ZERO extra reads. Otherwise read the live page ONCE (the same A1:ZZ6000
  read + header-NAME alignment as the persistence pass) and swap each stub
  for the symbol's existing row IFF that row is GOOD (positive price,
  provider not in the error set). A stub whose old row is also stub/absent
  keeps the fresh stub — the guard can only substitute strictly better
  data, never freeze an error in place. Deliberately conservative: an
  error-tagged row that DOES carry a price keeps the fresh price.
  Scope: _RANKED_MARKET_PAGES only (My_Portfolio semantics untouched).
  Kill-switch TFB_SYNC_KEEP_LAST_GOOD=0/false/off/no restores v6.22.2
  exactly. FAIL-SAFE + never-throws: any detection/read error keeps the
  fetched matrix unchanged and appends a warning. Tag
  [v6.22.3 KEEP-LAST-GOOD] in warnings/logs; the per-page warning lists the
  substituted symbols. New helpers: _keep_last_good_enabled,
  _klg_price_ok, _klg_provider_is_error, _keep_last_good_rows (4 added,
  0 removed, 0 signature changes; every other line verbatim v6.22.2).

v6.22.2 fix — SYMBOL-AMPUTATION HARD GUARDS (L4a READBACK-EMPTY-GUARD +
  L4b PERSISTENCE-HARD-GUARD; both default ON with kill-switches; no
  workflow ENV action required to arm them)
- WHY (confirmed live overnight 2026-07-08/09, morning export 2026-07-09):
  this runner rewrote Market_Leaders 1,278 -> 897 rows (-381 symbols,
  including 2222.SR Saudi Aramco and 1010.SR Riyad Bank) and
  Global_Markets 3,818 -> 3,668 (-150; 4030.SR Bahri erased from the
  whole workbook) — the GAS batch engine caught it mid-write at 21:37
  Riyadh: "concurrent writer detected (sheet changed mid-batch: rows
  3818 -> 3668)". Because the sheet Symbol column is the symbol source,
  every dropped row is PERMANENTLY out of the universe. Zero
  [SYMBOL-PERSISTENCE] appends reached the written pages. TWO holes,
  either one sufficient, both proven reachable during exactly that
  window (concurrent GAS batch + Yahoo 401 "Invalid Crumb" storm):
  (1) READBACK HOLE: _read_existing_page_symbols is FAIL-SAFE to [] on a
      Sheets read failure, and all four market TaskSpecs carry
      allow_empty_symbols=True — so one failed read at run start turns
      the task into a PAGE-DRIVEN request (symbols=[]), which bypasses
      EVERY symbol-scoped guard at once: the v6.18.2 shrink floor, the
      v6.19.0 persistence, the v6.19.1 strict membership, and the
      v6.22.0 L3 identity-tripwire scope. Whatever partial page the
      backend returns is then written verbatim and trimmed.
  (2) PERSISTENCE HOLE: _persist_missing_symbol_rows is itself FAIL-SAFE
      — a read_values failure (or unlocatable header) returns the
      SHRUNKEN matrix unchanged with NO exception, so the caller's
      try/except never fires and the 70-99%-coverage write proceeds,
      silently deleting every fetch-missed symbol.
- FIX L4a [READBACK-EMPTY-GUARD]: on the four ranked market pages, when
  the read-back is enabled and yields ZERO usable symbols, retry the
  read ONCE; still zero -> SKIP the task (preserve last-good rows,
  status="skipped") instead of falling through to the unguarded
  page-driven rewrite. On these pages the sheet IS the symbol source, so
  an empty read in production means the read failed or the sheet was
  mid-rewrite — never a legitimate empty page. Bootstrap of a genuinely
  empty page: run once with TFB_SYNC_READBACK_EMPTY_GUARD=0, or build
  via GAS. Kill-switch: TFB_SYNC_READBACK_EMPTY_GUARD=0/false/off/no
  restores the v6.22.1 page-driven fallback byte-identically.
- FIX L4b [PERSISTENCE-HARD-GUARD]: after the persistence pass, VERIFY
  THE OUTCOME on the four ranked market pages — recompute the requested
  symbols still absent from the final matrix (deny-junk excluded, same
  normalization as persistence). Any still-missing symbol means the
  preservation degraded (read failure, header-scan failure, or
  exception) -> SKIP clear+write and PRESERVE last-good rows, exactly
  like the empty/shrink/tripwire guards. Outcome verification (not
  exception-catching) is the point: hole (2) raises nothing. Invariant
  is valid on these pages because every requested symbol came FROM the
  sheet (read-back), so a last-good row exists for it by construction.
  Scoped to _RANKED_MARKET_PAGES only (My_Portfolio keeps v6.22.1
  semantics — a brand-new cost-basis holding legitimately has no
  last-good row; its v6.5.0 manual guard already protects it). Runs only
  while TFB_SYNC_SYMBOL_PERSISTENCE is ON (persistence deliberately OFF
  restores the documented v6.18.2 drop behavior whole). Kill-switch:
  TFB_SYNC_PERSISTENCE_HARD=0/false/off/no restores the v6.22.1
  warn-and-continue byte-identically.
- Availability trade, stated: a page can now SKIP a cycle (stay on
  last-good rows) where v6.22.1 would have written a shrunken table.
  That is the same trade the empty/shrink/identity guards already made:
  stale-but-complete beats fresh-but-amputated, and it self-heals on the
  next healthy run. Everything else byte-identical to v6.22.1.

v6.22.1 hotfix — SAFE CHAIN IS ANALYSIS-ONLY (drops /v1/advanced/* from
  the L1 market chains; same TFB_SYNC_SAFE_GATEWAYS switch, no new ENV)
- WHY (Render log 2026-07-09 01:48 Riyadh, during a Yahoo 401 "Invalid
  Crumb" storm): "POST /v1/advanced/sheet-rows 200" — the v6.22.0 safe
  chain's SECOND candidate is not a harmless 404. main.py canonically
  routes the whole /v1/advanced/* prefix to routes.investment_advisor
  (v2.17.0), a live 2,396-line module with ZERO transposition/identity
  firewall markers (verified against repo HEAD); the unmounted
  routes/advanced_sheet_rows.py file even carries the literal positional
  pattern `{s: r for s, r in zip(symbols, data)}`. One analysis hiccup on
  the first batch pins used_endpoint to advanced for the WHOLE page — an
  unverified funnel exactly where the safe chain promised a verified one.
- FIX: in safe mode, BOTH the "analysis"/"ai" and the "advanced" gateway
  chains now return the analysis endpoints only. An analysis outage yields
  an empty fetch -> the existing empty/shrink guards preserve last-good
  rows (the availability trade v6.22.0 already accepted, now applied
  consistently). L3 IDENTITY-TRIPWIRE remains the last fuse regardless.
  TFB_SYNC_SAFE_GATEWAYS=0 still restores the full v6.21.0 chains
  byte-identically. Everything else byte-identical to v6.22.0.

v6.22.0 fix — SYMBOL↔NAME TRANSPOSITION FIREWALL, WRITER SIDE (three
  independent layers; L1+L2+L3 default ON with kill-switches; no workflow
  ENV action required to arm them)
- WHY (confirmed live 2026-07-08, evening export v37): between 17:30 and
  18:12 UTC this runner rewrote 1,274/1,283 Market_Leaders rows (and then
  Global_Markets) with symbol↔attribute TRANSPOSED payloads — 1010.SR
  carried "AstraZeneca PLC", 1120.SR "Bruker Corporation", GOOGL "Arabia
  Insurance Cooperative Company", 005930.KS "Bharti Airtel"; 8/19 known
  Saudi anchors were foreign on ML and ~7/11 on GM — OVERWRITING a clean
  GAS batch refresh completed 3h earlier. ROOT CAUSE: all four market
  TaskSpecs default gateway="enriched", so the PRIMARY serving route is
  /v1/enriched/sheet-rows, which carries NEITHER the analysis router's
  transposition firewall (v4.7.0+) nor its rank/dedup passes (v6.13.0 WHY
  block already documented "neither pass" for enriched); GAS refreshes go
  through /v1/analysis/sheet-rows and stay clean. The writer then trusted
  the rows verbatim: STRICT-MEMBERSHIP checks the Symbol cell only, so a
  row with the RIGHT symbol and the WRONG attribute payload sails through.
- L1 [SAFE-GATEWAYS] (TFB_SYNC_SAFE_GATEWAYS, default ON): the four
  _RANKED_MARKET_PAGES resolve to the "analysis" gateway REGARDLESS of the
  v6.10.0 boolean and the v6.18.0 override, and the market candidate
  chains lose their unfirewalled tails (/v1/ai/*, /v1/enriched/*) — an
  analysis outage now leaves the page on last-good rows via the existing
  empty/shrink guards instead of accepting unfirewalled rows. Conscious
  availability trade; =0 restores the v6.21.0 routing byte-identically.
  My_Portfolio keeps its enriched gateway this build (122-col schema);
  L3 covers its identity instead.
- L2 [BATCH-IDENTITY] (TFB_SYNC_BATCH_IDENTITY, default ON): the batched
  market fetcher now (a) drops any row whose Symbol is not in THAT
  batch's requested set (cross-batch bleed), (b) collapses duplicate
  symbols (first occurrence wins), (c) drops blank-symbol rows, and
  (d) emits the combined matrix keyed BY SYMBOL in the REQUESTED order —
  no positional concatenation survives. Fail-safe: Symbol column missing
  from the response headers -> legacy extend() path unchanged. =0
  restores v6.21.0 accumulation byte-identically.
- L3 [IDENTITY-TRIPWIRE] (TFB_SYNC_IDENTITY_TRIPWIRE, default ON;
  threshold TFB_SYNC_IDENTITY_MIN_FAILS, default 2; extra pairs via
  TFB_SYNC_IDENTITY_ANCHORS_EXTRA "SYM=sub|sub,SYM2=sub"): before the
  clear/write, verify the built-in Symbol->Name anchor pairs that are
  PRESENT in the fetched matrix (1120.SR must contain "rajhi", 2222.SR
  "aramco"/"saudi arabian oil", AAPL "apple", 005930.KS "samsung", ...).
  >= threshold mismatches => the payload is transposed at the source =>
  SKIP clear+write (status=skipped, last-good rows preserved, loud
  logger.error naming up to 10 offending pairs) — the same preserve
  semantics as the empty/shrink guards. Blank names never count as a
  mismatch; a page with no anchors present is never blocked. This layer
  would have BLOCKED tonight's ML write (>=5 anchor failures observed).
- New helpers: _safe_gateways_enabled, _batch_identity_enabled,
  _identity_tripwire_enabled, _identity_min_fails, _identity_extra_anchors,
  _identity_anchor_map, _identity_anchor_scan, _GUARD_NAME_ALIASES,
  _IDENTITY_ANCHORS. Touched: _effective_gateway,
  _endpoint_candidates_for_gateway, _fetch_market_rows_batched,
  _run_one_task (one inserted guard block). Everything else byte-identical
  to v6.21.0; zero functions removed.

v6.21.0 fix — SMALL-PAGE STARVATION (Fix #6: page-order override +
  bounded empty-row retry; two INDEPENDENT env switches, both inert by
  default: TFB_SYNC_PAGE_ORDER unset, TFB_SYNC_EMPTY_RETRY "0")
- WHY (confirmed live 2026-07-06): the Mutual_Funds page sat at ~44% live
  coverage inside the full sync (485 snapshot/fallback rows, flagship ETFs
  MDY/VUG/IJR/VWO blocked "Missing current price") — yet a SOLO refresh of
  the same page, same symbols, same code priced 870/871 rows (99.9%) in one
  pass. Same signature on Commodities_FX (38% missing in-run). ROOT CAUSE:
  _default_tasks() launches Market_Leaders(2) + Global_Markets(3) first;
  under Semaphore(workers) the ~4,500 ML+GM symbols burn the provider
  budget window (Yahoo datacenter 401 storm + EODHD breaker/backoff), so
  the small pages at priorities 4-5 fetch into exhausted providers and the
  honesty gates correctly write them as blocked. Sequencing problem, not a
  pricing problem.
- FIX 6a — TFB_SYNC_PAGE_ORDER (csv of sheet names or task keys): reorders
  ONLY the enriched market tasks; listed pages take launch positions 1..k
  in the given order, unlisted enriched tasks follow in their original
  relative order, and the analysis/cockpit tasks (Insights, Top_10,
  Data_Dictionary) keep their later priorities REGARDLESS — they must run
  after all universes. Unknown tokens are logged and ignored. Unset ->
  byte-identical v6.20.0 order. Recommended production value puts the
  starved small pages ahead of the big two:
  "My_Portfolio,Mutual_Funds,Commodities_FX,Market_Leaders,Global_Markets".
- FIX 6b — TFB_SYNC_EMPTY_RETRY ("0" default): after a batched market fetch,
  rows whose price cell is empty get ONE bounded re-fetch pass
  (TFB_SYNC_EMPTY_RETRY_MAX, default 120 symbols; optional cool-down
  TFB_SYNC_EMPTY_RETRY_DELAY_SEC, default 0, cap 120) through the SAME
  batched fetcher; healed rows are spliced back BY SYMBOL only when the
  retry returns the identical header row (a mismatch skips the splice with
  a warning — the retry can never make a page worse). Second-line safety
  for breaker-window casualties from ANY cause; arm it AFTER one ordered
  run confirms 6a, so attribution stays clean.
- New helpers: _page_order_override, _apply_page_order, _empty_retry_*,
  _retry_empty_rows. Everything else byte-identical to v6.20.0.

v6.20.0 fix — CROSS-PAGE PRICE-DELTA GUARD (Fix 1b; env-gated
  TFB_XPAGE_PRICE_CHECK, DEFAULT OFF; threshold TFB_XPAGE_PRICE_DELTA_PCT,
  default 2.0; report cap TFB_XPAGE_MAX_REPORT, default 50)
- WHY: the 2026-07-05 workbook audit found the SAME symbol carrying wildly
  different prices on two pages written by the SAME run — 1211.SR at 17.73
  on Market_Leaders vs 58.90 on Global_Markets (market truth that session:
  63.10), and 1120.SR at 43.28 vs 66.00 — with no alarm anywhere. The sync
  runner is the only component that holds every page's final matrix in one
  process, so it is the natural (and cheapest) place to detect intra-run
  disagreement: a >2% same-symbol spread across pages means at least one
  page is serving a stale, contaminated, or mis-mapped price. This is the
  workbook-level complement to the engine's v5.104.0 bar-age gate (which
  judges each row against the exchange calendar; this judges rows against
  EACH OTHER).
- FIX (observe-and-report only — writes, guards, ordering, exit codes all
  byte-identical; OFF by default):
  1. _xpage_collect(): after each task's final headers/matrix are ready
     (immediately before the write step), harvest (page, symbol, price)
     into a run-level map. Pages without a symbol or price column
     (Insights_Analysis etc.) contribute nothing; blank/non-numeric/
     non-positive prices are skipped. Read-only; wrapped so it can never
     affect the write path. Runs in dry-run too (harvest reads the fetched
     matrix, not the sheet).
  2. _xpage_report(): after the task gather completes, for every symbol
     seen on 2+ pages compute the max spread (hi-lo)/lo; spreads above the
     threshold are logged as WARN lines (worst first, capped), plus one
     INFO summary line with counts. The collector is cleared after the
     report (re-entrant for in-process test harnesses).
  3. Detection only, BY DESIGN: the runner cannot know which page is wrong,
     so it does not mutate rows or block writes — it makes the disagreement
     impossible to miss in the run log. Escalation (row tagging / write
     blocking) stays a human decision after observing real-world hit rates.
- New helpers: _xpage_check_enabled, _xpage_delta_threshold_pct,
  _xpage_max_report, _xpage_collect, _xpage_report + module collector
  _XPAGE_PRICES and _XPAGE_PRICE_ALIASES. Everything else is byte-identical
  to v6.19.2.

v6.19.2 fix — MARKET-PAGE SYMBOL CAP ALIGNED TO THE EXPANDED UNIVERSE
  (env-tunable TFB_SYNC_MAX_SYMBOLS_MARKET, default 2500)
- WHY: on 2026-07-03 the owner deliberately expanded the universe by pasting
  the TFB Symbol Expansion Pack (~1,284 Global_Markets additions + 500
  Market_Leaders + CFX/MF additions). The hardcoded market-page caps
  (800/800/400/400) then became the SYMBOL REMOVER: _read_symbols returns at
  most max_symbols from the sheet, so symbols beyond the cap are never
  REQUESTED — which means the v6.19.0 persistence guard cannot protect them
  (it keeps requested-but-missing symbols only) — and the page write drops
  them. Live fingerprint: Global_Markets pinned at EXACTLY 800 rows after
  every run while ~2,050 were on the sheet. This deliberately reverses the
  v6.19.1 decision to hold the cap at 800; that decision's premise ("no
  universe CSV was pasted") no longer holds.
- FIX: the four market pages' TaskSpec caps now come from
  _market_symbol_cap(): TFB_SYNC_MAX_SYMBOLS_MARKET if set (clamped 1..5000),
  else 2500 — sized to the expansion pack's documented ceiling with headroom.
  My_Portfolio stays at 800 (its symbols come from _Portfolio_CostBasis, ~10
  names; the cap is irrelevant there). The CLI --max-symbols override and the
  safe_limit request ceiling flow from the same value automatically.
- RUNTIME NOTE (watch item, not a change): ~2,050 Global_Markets symbols at
  TFB_SYNC_SYMBOL_BATCH_SIZE=25 is ~82 backend requests for that page alone.
  Morning windows absorb this easily; midday Yahoo throttle will yield partial
  coverage — which is now NON-DESTRUCTIVE (shrink guard skips the write below
  the coverage floor; persistence keeps last-good rows above it). If the
  GitHub job starts brushing its 45-minute timeout, raise timeout-minutes in
  daily_sync.yml or the batch size — do not lower this cap back.
- New helper: _market_symbol_cap. Everything else is byte-identical to
  v6.19.1.

v6.19.1 fix — STRICT RESPONSE MEMBERSHIP (unrequested backend rows were
  expanding the page universe)
- WHY: the owner confirmed (2026-07-03) that NO universe CSV was pasted, yet
  Global_Markets grew 749 -> 3,068 rows across GREEN runs and later collapsed
  back to 775. With the manual-paste explanation eliminated, the only remaining
  writer-side cause is the backend returning MORE rows than were requested
  (a gateway/universe endpoint answering with its own symbol set on top of the
  requested one). The sync wrote every returned row verbatim; because the sheet
  IS the symbol source, each foreign row then became a REQUESTED symbol on the
  next run — a one-way universe ratchet, and the direct feeder of the corrupt
  Top_10 candidates. The deny filter (v6.19.0 WHY 2) only blocks the TICK
  placeholder family; real-looking foreign symbols passed straight through.
- FIX (TFB_SYNC_STRICT_MEMBERSHIP, default ON, =0/false/off/no to restore
  v6.19.0 byte-identically): on requested-symbol pages, response rows whose
  Symbol is NOT in the requested set are dropped BEFORE the guards run, with
  the dropped symbols named in a [STRICT-MEMBERSHIP] warning. Ordering matters
  and is deliberate: membership -> empty-guard -> shrink-guard -> persistence,
  so (a) a fully-foreign response degenerates to the empty-guard's
  preserve-last-good skip, (b) coverage %% is measured on REQUESTED rows only,
  and (c) persistence still re-appends any requested symbol the backend missed.
  Rows with a BLANK symbol cell are kept unchanged (never structural loss), and
  pages that request no symbols (backend-computed pages like Top_10) are never
  filtered — the guard is scoped exactly like persistence. The market-page
  max_symbols cap stays at 800 ON PURPOSE: with no CSV paste, the organic
  universe is ~750 and raising the cap would only widen the door this fix
  closes.
- New helpers: _strict_membership_enabled, _filter_rows_to_requested,
  _STRICT_MEMBERSHIP_TAG. Integration is ONE block in _run_one_task, placed
  after the no-credentials early return and before the My_Portfolio write
  guard (so every guard evaluates the rows that will actually be written).
  Everything else is byte-identical to v6.19.0.

v6.19.0 fix — PER-SYMBOL PERSISTENCE + UNIVERSE JUNK FILTER (operator symbols
  were being deleted by GREEN runs)
- WHY 1 (SYMBOL PERSISTENCE): the v6.16.0 read-back guarantees operator-added
  symbols are REQUESTED, and the v6.18.2 shrink guard blocks a write when
  coverage falls below 70%% — but between 70%% and 99%% coverage the page is
  rewritten from the response verbatim, so any requested symbol the backend
  failed to return (Yahoo throttle, gateway universe gap) is silently dropped.
  Because the sheet IS the symbol source, the drop is PERMANENT (observed
  2026-07-03: operator additions vanishing across successful syncs). FIX
  (TFB_SYNC_SYMBOL_PERSISTENCE, default ON, =0 to disable): right before the
  write, every requested-but-missing symbol keeps its existing last-good row
  (read from the live page, re-aligned to the new header order by header NAME);
  the symbol therefore stays in the universe and self-heals on the next healthy
  fetch. A fetch miss can no longer delete a requested symbol — only the
  operator (or the junk filter below) can remove one. Preserved symbols are
  named in a [SYMBOL-PERSISTENCE] warning on every affected run.
- WHY 2 (UNIVERSE JUNK FILTER): persistence makes every sheet symbol immortal —
  including garbage (the TICK000..TICK021 placeholder family that contaminated
  Global_Markets and reached the Top_10 picks before the 2026-07-03 cleanup).
  FIX (TFB_SYNC_UNIVERSE_DENY, default "^TICK\\d+", comma-separated regexes,
  set to off/0/- to disable): deny-pattern symbols are dropped from the
  read-back universe BEFORE the request and are never persisted, with every
  drop counted in a [UNIVERSE-FILTER] warning. Junk cannot self-perpetuate
  again; legitimate operator symbols are untouched.
- New helpers: _symbol_persistence_enabled, _persist_missing_symbol_rows,
  _universe_deny_patterns, _universe_junk, _SYMBOL_PERSISTENCE_TAG,
  _UNIVERSE_FILTER_TAG. Integration is two blocks in _run_one_task (read-back
  filter + pre-write persistence); disabled flags restore v6.18.2 byte-identical.

v6.18.2 fix — PARTIAL-FETCH SHRINK GUARD (the Market_Leaders universe ratchet)
- WHY (diagnosed from the owner's two workbook exports of 2026-07-02): between
  the 13:40 and 16:xx exports, Market_Leaders shrank 288 -> 163 symbols (-125,
  all .SR) with no manual deletion. MECHANISM: the sync reads the page's OWN
  Symbol column as its request universe; under midday Yahoo throttling some
  v6.17.0 symbol-batches fail and only the successful batches' rows are
  accumulated. The v6.9.0 empty-guard protects ONLY the zero-rows case — a
  PARTIAL result (163 of 288) passes it, the shorter table is written, the
  tail is trimmed, and because the sheet IS the symbol source the failed
  symbols are gone PERMANENTLY. Each throttled cycle can ratchet the universe
  smaller. That is a silent, compounding data loss.
- FIX: a MIN-COVERAGE guard beside the empty-guard: when a page EXPECTS rows,
  was requested with a concrete symbol list, and the fetch returned fewer than
  TFB_SYNC_MIN_COVERAGE_PCT percent of the requested symbols (default 70),
  the write is SKIPPED (status="skipped", neither write nor trim), the
  last-good rows — including every symbol the throttled batches missed — are
  preserved, and a warning names the coverage ratio. Self-heals on the next
  healthy cycle exactly like the empty-guard. Legitimate small shrinks
  (delistings, curation) pass untouched below the threshold. Page-driven
  requests (no symbol list) and non-expects_rows pages are exempt (no
  denominator / already covered). Set TFB_SYNC_MIN_COVERAGE_PCT=0 to disable
  and restore v6.18.1 byte-identical behavior. New helper:
  _min_coverage_pct(). RECOVERY of the already-lost 125 symbols is a one-time
  sheet paste (the owner holds the extracted list); this guard prevents
  recurrence, it cannot resurrect rows already trimmed.

v6.18.1 fix — TRANSIENT-WRITE RETRY + GRID-LIMIT-SILENT TRIM (from the
              2026-07-02 09:02 run-28568344788 log: 4/5 pages green, exit 2)
- WHY 1: GLOBAL_MARKETS failed with "Write failed: EOF occurred in violation of
  protocol (_ssl.c:2437)" — a transient SSL drop during the single ~3,000-row
  values.update. write_table() had NO retry, so one dropped connection failed
  the whole page (v6.18.0's write-then-trim correctly preserved the old rows —
  under the old clear-then-write this same failure would have EMPTIED the tab).
  FIX: write_table() retries the values.update up to TFB_SYNC_WRITE_RETRIES
  times (default 3 attempts total) with 2s/5s backoff, ONLY when the error
  matches a known-transient marker (SSL EOF, connection reset/aborted, timeout,
  HTTP 429/500/502/503, Broken pipe). values.update is idempotent (same block,
  same range) so a retry after an ambiguous EOF is safe. Non-transient errors
  raise immediately, exactly as before. TFB_SYNC_WRITE_RETRIES=1 restores the
  v6.18.0 single-attempt behavior byte-identically.
- WHY 2: the v6.18.0 trim-right warned every run on exactly-115-column sheets:
  "Range (Commodities_FX!DL1:ZZ) exceeds grid limits. Max columns: 115".
  Trimming from column 116 of a 115-column grid is a NO-OP by definition —
  nothing can be stale beyond the grid — but the Sheets API answers 400 instead
  of succeeding quietly. FIX: _trim_after_write treats "exceeds grid limits"
  as silent success for BOTH trims (below + right); every other trim failure
  still surfaces as a warning. New helper: _is_transient_write_error.

v6.18.0 fix — MARKET-GATEWAY OVERRIDE + CANCELLATION-SAFE WRITE-THEN-TRIM
              (fixes the 2026-07-02 02:47 run: job cancelled at the 25-min
              ceiling mid-run, leaving Mutual_Funds + Commodities_FX EMPTY and
              Market_Leaders degraded)
- WHY (diagnosed from the run-28554325006 sync log): with the Render env
  TFB_ANALYSIS_ENGINE_TIMEOUT_SEC now deleted (the correct fix for the
  placeholder-wipe), a BIG market-page request to /v1/analysis/sheet-rows runs
  unbounded and dies as a gateway 502 (the documented pre-FIX-3 symptom).
  Because TFB_SYNC_MARKET_ANALYSIS_GATEWAY=1 routes the four market pages
  ANALYSIS-FIRST, every page paid 502s + a 404 candidate walk before landing on
  /v1/advanced/sheet-rows 200 — the run blew past timeout-minutes:25 and GitHub
  CANCELLED it. And the write path was clear_from() THEN write_table(): a
  cancellation landing between the two leaves a CLEARED, NEVER-REWRITTEN page.
  That is exactly how Mutual_Funds and Commodities_FX went empty.
- FIX 1 (TFB_SYNC_MARKET_GATEWAY, default unset): a GENERIC market-page gateway
  override consulted by _effective_gateway BEFORE the v6.10.0 boolean. Value
  "advanced" routes the four ranked market pages /v1/advanced/sheet-rows-FIRST —
  the endpoint that answered 200 on EVERY attempt in the failed run's log and
  the same route the user's manual "Refresh" uses (his correctness reference).
  "analysis" / "enriched" / "argaam" select those chains; unset/blank falls
  through to the v6.10.0 boolean then the TaskSpec default (byte-identical
  routing). Honest trade-off, stated: the analysis router's global-rank + dedup
  passes do not run on the sync copy while "advanced" is selected; the analysis
  endpoints remain fallback candidates in the advanced chain.
- FIX 2 (TFB_SYNC_WRITE_THEN_TRIM, default ON): the clear-before-write pair is
  reordered to WRITE-then-TRIM. write_table() (one atomic values.update)
  overwrites the block in place FIRST; only then are the leftovers trimmed with
  two targeted clears — the tail BELOW the new block (full width) and the tail
  RIGHT of the header width (full depth). A cancellation now leaves either the
  OLD page or the NEW page (worst case: new page + a stale tail that self-heals
  on the next run) — NEVER an empty page. Set 0/false/off/no to restore the
  exact v6.17.0 clear-then-write order. New helpers: _market_gateway_override,
  _write_then_trim_enabled, _a1_col_to_idx, _idx_to_a1_col, _trim_after_write.
  No schema / payload-key / endpoint-list / guard change.

v6.17.0 fix — market-page SYMBOL BATCHING (fixes empty market pages + 502s under
              Yahoo rate-limiting)
- WHY (diagnosed + confirmed from the 2026-07-01 13:05 sync logs + a code trace
  of data_engine_v2 v5.101.0): each market page was fetched in ONE request
  carrying its ENTIRE symbol set (Market_Leaders ~388, Global_Markets ~3000).
  That single burst makes the backend fan out hundreds of Yahoo history calls at
  once, which (a) trips Yahoo's datacenter-IP rate limit (HTTP 429) so the
  symbols return no price and the route hands back a 200 with ZERO data rows ->
  the v6.9.0 empty-guard skips the write and the page shows STALE data, and (b)
  exceeds Render's ~100s edge timeout on the analysis route -> HTTP 502. NOTE
  the engine's trust gate does NOT delete rows (_apply_rank_overall only
  WITHHOLDS a Rank (Overall) from a LOW-trust row; the row stays), so the empty
  page is a fetch/throttle problem, and THIS (the sync request shape) is the
  correct layer to fix — not a rewrite of the 12k-line engine.
- FIX: when enabled, split a market page's symbol set into small SEQUENTIAL
  batches and fetch each on its own request, accumulating the data rows. Each
  request is light enough to finish inside the timeout (kills the 502) and the
  calls are spread out so they are far less likely to 429 (recovers rows). The
  combined (headers, rows) then flow into the SAME guards + single clear/write
  as before. Default OFF -> byte-identical to v6.16.0. New env:
  TFB_SYNC_SYMBOL_BATCH_SIZE (positive int enables; e.g. 100) and optional
  TFB_SYNC_BATCH_DELAY_MS (default 0). Scope: the four _RANKED_MARKET_PAGES only,
  and only when the page has MORE symbols than the batch size — My_Portfolio,
  Top_10, meta pages, and empty-symbol page-driven requests are untouched.
- KNOWN TRADE-OFF (documented, honest): the analysis route's page-level Global
  Rank / Global Dedup passes run PER REQUEST, so with batching the Rank (Overall)
  column is ranked WITHIN each batch, and duplicate symbols split across batches
  are not collapsed. Per-symbol data (price / score / recommendation /
  final_action) is correct regardless. This is a deliberate exchange: reliably
  POPULATED pages with per-batch ranking, versus whole-page ranking that
  currently 502s / returns empty. A client-side re-rank of the combined set is a
  clean follow-up if whole-page Rank (Overall) is required.
- New helpers: _symbol_batch_size(), _batch_delay_ms(), _should_batch_market_page(),
  _fetch_market_rows_batched(). All v6.16.0 functions carried verbatim (none
  removed); 4 added. No schema / payload-key / endpoint / guard change.

v6.16.0 fix — market-page symbol read-back (fixes user-added symbols being wiped)
- WHY (diagnosed + confirmed live 2026-06-29): the four market DATA pages
  (Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds) had NO working
  symbol source, so the backend served hardcoded _DEFAULT_SHEET_SYMBOLS
  placeholders and the sync OVERWROTE any user-added symbols every ~2h cycle.
  The live pages held an EXACT match to those placeholder sets, and the cause is
  in _read_symbols(): the sync runs from the repo root, so `import symbols_reader`
  binds the ROOT utility module, and getattr(mod, "get_page_symbols") /
  getattr(mod, "get_universe") both return None there (neither name exists) ->
  _read_symbols() returns [] on EVERY run.
- FIX: read the symbols the user actually has on each market page (its Symbol
  column) from the live sheet via the writer's own read service (the same proven
  path as the My_Portfolio cost-basis rebuild) and refresh THAT list instead of
  sending empty. User symbols persist; pages populate with the real universe;
  Top_10 (which pools from these pages) is no longer starved.
- SAFETY: fail-safe + env-gated. Any read failure / missing Symbol column / zero
  symbols -> [] -> existing page-driven flow (defaults seed a genuinely empty
  page; the v6.9.0 empty-rows guard still preserves the last-good page). The
  read-back can only ADD the user's symbols; it never blanks a page. Default ON;
  kill-switch TFB_MARKET_SYMBOL_READBACK=0. New helpers: _read_existing_page_symbols(),
  _market_symbol_readback_enabled(), _market_readback_pages().
- NOTE: _read_symbols() is left intact (now harmless dead weight for the market
  pages; the read-back supersedes it) to keep the write-path change minimal.

v6.15.1 fix — follow-up to v6.15.0 after the 6->3 sync (run #2123, commit 2d898c9)
- WHY (reconcile didn't catch 1211.SR): v6.15.0's reconciler classified reco
  families with _guard_norm (strips ALL non-alphanumerics), which can disagree
  with the validator's _norm_token (keeps single spaces). 1211.SR's sell-reco +
  Final Action=INVEST therefore slipped past while the validator still flagged
  it. FIX: classify EXACTLY as scripts/validate_dashboard.py does (_norm_token +
  the validator's own _SELL_FAMILY/_BUY_FAMILY), plus a substring fallback for
  decorated values; and ALWAYS log one line per decision page (page, the column
  indices found, rows scanned, rows changed, and the distinct reco/action value
  pairs WITHOUT symbols — safe for the public repo's logs) so the next run is
  fully diagnosable instead of silent.
- WHY (Top_10 still blank): run #2123's Top_10 fetch returned 0 data rows ("No
  symbols found -> page-driven request -> empty fetch"), so the empty-rows guard
  correctly SKIPPED the write to preserve last-good rows — which means v6.15.0's
  header repair never ran, and the blank header from the prior write survived.
  That blank header is self-perpetuating (blank header -> symbol read finds no
  Symbol column -> page-driven request -> 0 rows -> skip -> header stays blank).
  FIX: a Top_10 header SELF-HEAL in the empty-fetch skip path — even when the
  data write is skipped, repair ONLY row 1 from the canonical schema (column
  order taken from the response's own keys) so the 17 existing last-good picks
  (which already carry prices) become correctly labeled and the validator can
  map columns. Data rows are untouched. Gated by TFB_TOP10_HEADER_SELFHEAL
  (default ON; no ENV change needed to activate; set 0 to disable). The flaky
  Top_10 build returning 0 picks is a separate, deeper backend matter (transient
  provider/cold-cache); this makes the dashboard robust to it instead of red.

v6.15.0 fix — Top_10 blank-header repair + decision-row reconciliation (no new features)
- WHY (headers): the analysis route that serves Top_10_Investments returns a
  header row of 118 EMPTY-STRING cells (verified on the live sheet + in
  validate_dashboard.json: contract.header_match logs `extra: , , , ...`).
  Written verbatim by this sync, that blanks every column title, so the
  validator cannot map columns and reports top10.no_missing_price for ALL rows
  even though the data rows ARE populated. FIX: for Top_10 only, rebuild the
  header row from the canonical schema_registry headers, taking column ORDER
  from the response's own `keys` when present (else canonical order when the
  data width matches). FAIL-SAFE: if the schema/keys are unavailable or a safe
  rebuild is impossible, the original headers are returned unchanged — so the
  page can never be made worse than its current (blank) state. Lives entirely
  in the writer; the backend route is NOT touched (cannot be verified from CI
  without live providers). Gemini/DeepSeek/ChatGPT independently reached the
  same diagnosis; this is the verified, fail-safe implementation.
- WHY (reconcile): two integrity gates were failing on genuine cross-field
  contradictions — a sell-family Recommendation still carrying Final
  Action=INVEST (1211.SR), and a buy-family Recommendation carrying a non-empty
  Block Reason (BBD.US, whose block is legitimate). FIX: a NEUTRAL sheet-level
  reconciliation on the two decision pages (My_Portfolio, Top_10) that only
  REMOVES contradictions — sell+INVEST -> Final Action HOLD; buy+block ->
  Recommendation WATCH / Final Action HOLD. It never invents a BUY or SELL call
  and never clears a real block. The engine still emits the raw values; the
  engine-side root fix is a separate follow-up. REJECTED the uploaded
  daily_sync_hotfix YAML: it sets the validator to continue-on-error (green over
  a still-broken page), strips the hardened key/credential logic, and does NOT
  actually repair the headers.

v6.10.0 fix — Rank (Overall) / duplicate-symbol corrections actually reach the sheet
- WHY: routes/analysis_sheet_rows.py already carries two verified page-level
  corrections for the cross-sectional market pages — GLOBAL-RANK (v4.4.0:
  _apply_global_rank_overall re-ranks Rank (Overall) across the WHOLE page in one
  pass, default ON) and GLOBAL-DEDUP (v4.5.0: collapses duplicate-symbol rows,
  default ON). Both run ONLY in the analysis router, "the single funnel where the
  COMPLETE page exists before pagination". But this sync routes Market_Leaders,
  Global_Markets, Commodities_FX and Mutual_Funds through gateway="enriched"
  (/v1/enriched/sheet-rows), which has NEITHER pass — so the daily sheet showed
  the SAME Rank (Overall) value repeated once per upstream fetch batch (a row with
  overall 42 ranked 1 above a row with overall 67 ranked 2) and let duplicate
  symbols survive. The fix was built and on by default; it was simply never on the
  path the sync writes.
- FIX (env-gated, DEFAULT OFF -> byte-identical v6.9.0 routing): a per-task
  _effective_gateway() resolves the four cross-sectional market pages
  (_RANKED_MARKET_PAGES, mirroring the analysis router's scope exactly) to the
  "analysis" gateway when TFB_SYNC_MARKET_ANALYSIS_GATEWAY is enabled, so the
  global rank + dedup passes run on what gets written. My_Portfolio (holding
  order / multi-lot) and the meta pages are excluded. The analysis gateway's
  endpoint-candidate chain ends at the enriched endpoints, so an analysis-route
  outage falls back to the prior path (that page loses the rank/dedup for the
  cycle — never a failed write). Two new helpers added
  (_market_analysis_gateway_enabled, _effective_gateway) + one constant
  (_RANKED_MARKET_PAGES); every v6.9.0 function carried verbatim, none removed.
  Reversible: unset TFB_SYNC_MARKET_ANALYSIS_GATEWAY -> v6.9.0 routing exactly.

v6.9.0 fix — empty-rows wipe guard (silent clear-then-blank on provider outage)
- WHY: the four page-driven data pages (Market_Leaders, Global_Markets,
  Commodities_FX, Mutual_Funds) plus My_Portfolio ALWAYS return rows on a healthy
  run. The fetch loop in _run_one_task guards on HEADERS, not rows
  (`if not headers: failed/return`), and _extract_table_payload has an explicit
  "empty rows, but headers exist -> return headers_list, []" branch. So when the
  backend returns a well-formed envelope with the schema headers but ZERO data
  rows — exactly what a provider/Yahoo outage produces, where every symbol on the
  page fails to fetch yet the header envelope (from the schema registry) is intact
  — `headers` is truthy, the loop "succeeds", and control falls through to
  clear-before-write (default ON). clear_from() wipes {col}{row}:ZZ, write_table()
  writes headers only, and `if not rows_matrix: status="success"` reports the
  BLANKING as a SUCCESS. Result: an unattended daily_sync can clear Market_Leaders
  (or even My_Portfolio, whose manual-cell guard at
  `if rows_matrix and _guard_should_apply(...)` is itself bypassed by empty rows)
  to a single header row, and log it green. Market_Leaders is the worst-exposed
  page: Yahoo is its ONLY Saudi source, so a Yahoo hiccup is the exact trigger.
- FIX: a per-task `expects_rows` flag (TaskSpec, DEFAULT True) marks pages that
  MUST have data rows when healthy. In _run_one_task, placed BEFORE the clear so a
  skip performs NEITHER clear nor write, a page with expects_rows=True that fetched
  0 rows is SKIPPED (status="skipped", rows_written=0) with a warning — its
  last-good rows are preserved and self-heal on the next healthy sync, instead of
  being blanked. Mirrors the script's existing pre-clear protective-skip pattern
  (the My_Portfolio and decision-owned guards).
- SCOPE / SAFETY:
    * The empty-rows skip changes behavior ONLY for expects_rows=True pages that
      return 0 data rows. The five data pages (My_Portfolio, Market_Leaders,
      Global_Markets, Commodities_FX, Mutual_Funds) are explicitly marked
      expects_rows=True; they never legitimately write headers-only via the daily
      sync (first-time header setup is setup_sheet_headers.py's job, not this
      runner's). The default is True, so the meta pages (Insights_Analysis,
      Data_Dictionary) are ALSO protected — on an empty fetch they keep last-good
      rows rather than blank; Top_10_Investments is page-skipped by the
      decision-owned guard before the empty guard is ever reached. The
      "schema-only success" code path is retained intact for any future page that
      sets expects_rows=False deliberately.
    * Healthy runs (>=1 data row) are byte-for-byte unchanged: same fetch, same
      limit policy, same My_Portfolio + decision guards, same clear-before-write,
      same matrix rectification, same write_table, same exit codes.
    * Gated by TFB_SYNC_EMPTY_GUARD (default ON; set 0/false/off/no to restore the
      v6.8.0 behavior EXACTLY — clear-then-blank-and-report-success on empty).
- UNCHANGED: everything in v6.8.0 below.

v6.8.0 fix — non-scalar cell write (list/dict cells 400 the page write)
- WHY: the Google Sheets values API (valueInputOption=RAW) rejects any cell that
  is a list or dict ("Invalid values[r][c]: list_value ..."). The backend emits
  a few STRUCTURED columns for instrument rows — confirmed live: column 96,
  "Scoring Errors", is a Python list (usually empty []). The matrix path
  (_extract_table_payload's rows_matrix branch) returned cells verbatim and
  _rectify_matrix only padded width, so a list cell reached the API untouched.
  This stayed HIDDEN while the v6.6.0 limit:1 bug truncated every page-driven
  page to a single row whose structured cells happened to be benign; once
  v6.7.0 let the FULL pages through, the first row carrying a list cell 400-ed
  the whole write (Market_Leaders / Global_Markets / Commodities_FX failed;
  Mutual_Funds passed only because its rows had no list there). A latent
  data-shape bug surfaced — not caused — by the v6.7.0 fix.
- FIX: a per-cell scalar flatten (_cell_to_scalar) applied in _rectify_matrix —
  the single common choke point both the rows_matrix and rows[dict] paths pass
  through before the write, so one edit covers both. Empty list/dict -> "" (a
  clean empty cell); list of scalars -> "a, b, c"; nested -> compact JSON;
  scalars / None / Enum / datetime handled as _coerce_jsonable handles them.
- SCOPE / SAFETY:
    * Pure correctness: a list/dict cell is NEVER a valid Sheets RAW write, so
      there is no prior behavior to preserve (the prior behavior is a hard 400).
      Deliberately NOT env-gated for that reason. Widths, the limit policy, every
      endpoint/payload key, the My_Portfolio + decision guards, credentials, and
      exit codes are all byte-for-byte unchanged.
- UNCHANGED: everything in v6.7.0 below.

v6.7.0 fix — page-driven limit truncation (single-row pages)
- WHY: the page-driven pages (Market_Leaders, Global_Markets, Commodities_FX,
  Mutual_Funds) have NO symbol source — their symbol list resolves empty every
  run. In _run_one_task the limit was computed as
  `safe_limit = 1 if not symbols else min(5000, max(1, len(symbols)))`, on the
  assumption that empty symbols meant a "schema-only" request (headers only).
  But these pages are served by the enriched endpoint via the `page` field,
  which returns the page's OWN rows and honors `limit` as a row cap — so
  limit:1 silently truncated each page to a SINGLE written row. Confirmed live:
  the same endpoint + body returned 8 Market_Leaders rows at limit:800 but 1 row
  at limit:1; the request/parse/write path was otherwise byte-clean (the
  extractor and matrix rectifier preserve every row). A request-shape bug, not a
  data, parse, or backend bug.
- FIX: split the limit policy. Symbols present -> unchanged (cap at the symbol
  count, ceiling 5000). Symbols empty -> send the task's configured cap
  (max_symbols, e.g. 800/400; a high 5000 ceiling when max_symbols=0 for the
  analysis meta pages) so the full page returns. Still never sends literal 0.
- SCOPE / SAFETY:
    * Only the empty-symbol limit changes; the symbol path, every endpoint,
      payload key, the My_Portfolio + decision guards, matrix rectification, the
      clear-before-write default, credential loading, and exit codes are all
      byte-for-byte unchanged.
    * Gated by TFB_SYNC_PAGE_LIMIT_FIX (default ON; set 0/false/off/no to restore
      the v6.6.0 limit:1 EXACTLY).
- UNCHANGED: everything in v6.6.0 below.

v6.6.0 fix — decision-owned (cockpit) page guard (Top_10 clobber prevention)
- WHY: Top_10_Investments is a DECISION-OWNED page — the user records BUY /
  decision state in its decision columns (the cockpit), and data_engine_v2
  already serves a FRESH Top_10 on demand via the route (advanced_analysis ->
  top10_selector.build_top10_rows). GAS protects the page from refresh-overwrite
  with isDecisionOwnedPage_ (00_Config.gs), but the Python daily sync had a
  TOP_10_INVESTMENTS write task that bypassed that guard: with clear-before-write
  the default (v6.4.0), every cycle CLEARED the sheet and rewrote it WITHOUT the
  user's decision cells — clobbering the cockpit's decisions daily. A cross-layer
  gap: the guard existed in GAS but had no Python-side enforcement.
- FIX: a Python-side mirror of isDecisionOwnedPage_. A decision-owned page is
  SKIPPED in the Hard-filters block — BEFORE the symbol read, the backend fetch
  (the expensive selector build), the clear, and the write — so nothing is
  fetched, cleared, or written for it. The page's last-good rows + the user's
  decisions are left intact, and it refreshes on demand via the route.
- WHY PAGE-LEVEL SKIP (not the column-merge of the v6.5.0 My_Portfolio guard):
  the WHOLE Top_10 page is cockpit-owned and is re-derivable on demand by the
  engine, so the sync has no business writing any of it — unlike My_Portfolio,
  whose manual INPUT columns must be preserved while the rest is refreshed.
- SCOPE / SAFETY:
    * Applies to Top_10_Investments only; every other page is byte-for-byte
      unchanged. status="skipped" (NOT partial), so the daily exit code stays 0.
    * Gated by TFB_SYNC_DECISION_GUARD (default ON; set 0/false/off/no to restore
      the v6.5.0 write-through of decision pages exactly).
    * Pages overridable via TFB_SYNC_DECISION_GUARD_PAGES (comma-separated list).
    * Check the "[v6.6.0 DECISION-GUARD]" log line for the per-page skip reason.
- UNCHANGED: every endpoint, payload, the My_Portfolio guard, other task
  definitions, matrix rectification, credential loading, exit codes, the
  clear-before-write default, and the schema-agnostic write path.

v6.5.0 fix — My_Portfolio manual-cell write guard (irreversible-loss prevention)
- WHY: My_Portfolio carries user-authored ("manual") inputs that live ONLY in
  the sheet and are NEVER re-derivable from a market feed — position quantity
  and average cost (and, downstream, the position math computed from them). The
  backend echoes those cells back in the sync payload after reading them via the
  engine's sheet rows-reader. If that upstream read transiently misses (a Sheets
  API hiccup, a cold reader), the payload returns those manual cells BLANK while
  the live sheet still holds the real values. A normal write then overwrites the
  user's real Qty/Avg Cost with blanks — irreversible data loss.
- FIX: before writing My_Portfolio (and ONLY My_Portfolio), the runner now
  independently re-reads the live sheet and checks whether any symbol that
  currently HAS manual data (Qty / Avg Cost) would be regressed to BLANK by the
  outgoing payload. If so — or if that verification read itself cannot be
  trusted — the write is SKIPPED for this cycle (status=partial + warning).
  Nothing is cleared, nothing is written; the existing row (manual inputs AND
  the computed columns derived from them) is preserved whole and self-heals on
  the next healthy sync.
- WHY WHOLE-ROW SKIP (not per-cell merge): the upstream rows-reader reads the
  grid in a single call — it gets every row or none. On a miss, the manual
  inputs AND their computed columns (position value / unrealized P&L) blank out
  together. A per-cell merge would keep Qty/Avg Cost but still write a BLANK
  position value against a FRESH price — a misleading, internally-inconsistent
  half-row. Skipping the whole write keeps the row consistent and correct.
- SCOPE / SAFETY:
    * Applies to My_Portfolio only; every other page is byte-for-byte unchanged.
    * Gated by TFB_SYNC_MANUAL_GUARD (default ON; set 0/false/off to disable —
      disabling restores pre-v6.5.0 write-through behavior exactly).
    * Pages overridable via TFB_SYNC_MANUAL_GUARD_PAGES (comma-separated list).
    * Fail-safe: any uncertainty (read error, unmappable header/symbol column,
      missing manual columns on the payload) skips the write to protect existing
      data — the guard NEVER writes blind. A persistently-skipping My_Portfolio
      therefore means the guard is protecting data, not losing it; check the
      "[v6.5.0 PORTFOLIO-GUARD]" log line for the specific reason.
    * Robust to layout: the verification read locates the header row by content
      (symbol + manual columns), so a header at row 1 OR at the A5 default with
      title rows above are both handled, and column reorder is tolerated via
      normalized header-name matching.
- UNCHANGED: every endpoint, payload, task definition, matrix rectification,
  credential loading, exit codes, the clear-before-write default, and the
  schema-agnostic write path.

v6.4.0 fix — clear-before-write is now the DEFAULT (ghost/stale-row root cause)
- ROOT CAUSE: write_table() writes via Sheets values.update, which overwrites
  cells IN PLACE and NEVER truncates trailing rows/columns. Clearing was gated
  behind the opt-in --clear flag (default OFF), and the production daily_sync
  workflow never passes it. So whenever a refresh wrote FEWER rows than the
  prior run (e.g. Top_10_Investments returning 3 rows after a previous 8-row
  write) or FEWER columns than a stale wider write, the leftover rows/columns
  survived as "ghosts": stale Top 10 picks (the 5 leftover rows) and the
  trailing ghost "Status" columns observed on Global_Markets.
- FIX: clear-before-write is now the DEFAULT. The per-task clear is driven by a
  new --no-clear opt-OUT (default: clear ON) in place of the old --clear
  opt-IN. clear_from() already clears {col}{row}:ZZ — full column width AND all
  rows to the bottom — so one default-on clear removes BOTH stale rows and
  ghost columns on every page. No other logic changed.
- BACKWARD COMPAT: --clear is still accepted (now redundant/deprecated) so any
  existing cron that passes it keeps working; --no-clear restores the old
  opt-in (append/preserve) behavior for a run that genuinely wants it.
- UNCHANGED: every endpoint, payload, task definition, matrix rectification,
  credential loading, exit codes, and the schema-agnostic write path.

v6.3.0 fixes (targets your recurring ❌ causes)
- ✅ Sheets-safe ALWAYS: backend rows (dicts or lists) -> strict 2D matrix (pads/truncates to header length)
- ✅ JSON-safe value coercion for Google API (datetime/Enum/set/etc -> primitives)
- ✅ Key parsing is robust: --keys supports space, comma, semicolon, JSON array-like tokens
- ✅ Stronger backend compatibility: sends sheet/sheet_name/page/name/tab + tickers/symbols + request_id
- ✅ Health preflight probes /readyz + /health + /livez (best-effort)
- ✅ Credentials loader hardened: supports GOOGLE_APPLICATION_CREDENTIALS file + env JSON + env base64; fixes "\\n" private_key
- ✅ Never runs forbidden legacy keys (KSA_TADAWUL / ADVISOR_CRITERIA)
- ✅ Deterministic exit codes:
    0 = all success
    1 = partial (some partial/skipped) but no hard failures
    2 = one or more failed

Design rules
- No network calls at import-time.
- Conservative: warnings instead of crashes.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Version
# -----------------------------------------------------------------------------
SCRIPT_VERSION = "6.22.3"

# -----------------------------------------------------------------------------
# Logging (Render-safe)
# -----------------------------------------------------------------------------
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DashboardSync")

# -----------------------------------------------------------------------------
# Helpers (safe)
# -----------------------------------------------------------------------------
_A1_CELL_RE = re.compile(r"^\$?[A-Za-z]+\$?\d+$")
_SHEET_SAFE_RE = re.compile(r"^[A-Za-z0-9_]+$")
_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSY = {"0", "false", "no", "n", "off"}

_ALLOWED_KEYS = {
    "MARKET_LEADERS",
    "GLOBAL_MARKETS",
    "COMMODITIES_FX",
    "MUTUAL_FUNDS",
    "MY_PORTFOLIO",
    "INSIGHTS_ANALYSIS",
    "TOP_10_INVESTMENTS",
    "DATA_DICTIONARY",
}
_FORBIDDEN_KEYS = {"KSA_TADAWUL", "ADVISOR_CRITERIA"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _safe_int(v: Any, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(str(v).strip()))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _validate_a1_cell(a1: str) -> str:
    s = (a1 or "").strip()
    if not s:
        return "A5"
    if not _A1_CELL_RE.match(s):
        raise ValueError(f"Invalid A1 start cell: {a1!r}")
    return s


def _canon_key(user_key: str) -> str:
    """
    Normalizes SYNC_KEYS tokens to canonical runner keys.

    Canonical runner keys (March 2026):
      MARKET_LEADERS, GLOBAL_MARKETS, COMMODITIES_FX, MUTUAL_FUNDS,
      MY_PORTFOLIO, INSIGHTS_ANALYSIS, TOP_10_INVESTMENTS, DATA_DICTIONARY
    """
    k = (user_key or "").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "LEADERS": "MARKET_LEADERS",
        "MARKET": "MARKET_LEADERS",
        "GLOBAL": "GLOBAL_MARKETS",
        "FUNDS": "MUTUAL_FUNDS",
        "ETF": "MUTUAL_FUNDS",
        "ETFS": "MUTUAL_FUNDS",
        "FX": "COMMODITIES_FX",
        "COMMODITIES": "COMMODITIES_FX",
        "PORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ANALYSIS": "INSIGHTS_ANALYSIS",
        "TOP10": "TOP_10_INVESTMENTS",
        "TOP_10": "TOP_10_INVESTMENTS",
        "TOP10_INVESTMENTS": "TOP_10_INVESTMENTS",
        "TOP_10_INVESTMENTS": "TOP_10_INVESTMENTS",
        "DATA_DICTIONARY_SHEET": "DATA_DICTIONARY",
        "DICTIONARY": "DATA_DICTIONARY",
    }
    return aliases.get(k, k)


def _is_forbidden_key(k: str) -> bool:
    return _canon_key(k) in _FORBIDDEN_KEYS


def _default_backend_url() -> str:
    return (os.getenv("BACKEND_BASE_URL") or os.getenv("DEFAULT_BACKEND_URL") or "http://127.0.0.1:8000").rstrip("/")


def _default_spreadsheet_id(cli_id: Optional[str]) -> str:
    if cli_id and cli_id.strip():
        return cli_id.strip()
    return (os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()


def _env_token() -> str:
    """
    Best-effort auth token loader.
    Supports:
      - TFB_TOKEN
      - X_APP_TOKEN
      - APP_TOKEN
      - BACKEND_TOKEN
    """
    for name in ("TFB_TOKEN", "X_APP_TOKEN", "APP_TOKEN", "BACKEND_TOKEN"):
        v = (os.getenv(name) or "").strip()
        if v:
            return v
    return ""


def _coerce_jsonable(v: Any) -> Any:
    """Make values safe for JSON/Google Sheets payloads."""
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, (datetime, date)):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    if isinstance(v, dict):
        return {str(k): _coerce_jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_coerce_jsonable(x) for x in v]
    # pydantic-ish
    try:
        if hasattr(v, "model_dump"):
            return _coerce_jsonable(v.model_dump(mode="python"))  # type: ignore
        if hasattr(v, "dict"):
            return _coerce_jsonable(v.dict())  # type: ignore
    except Exception:
        pass
    return str(v)


def _parse_keys_tokens(raw_tokens: Sequence[str]) -> List[str]:
    """
    Accepts:
      --keys A B C
      --keys "A,B,C"
      --keys "A;B;C"
      --keys '["A","B"]'
    """
    flat: List[str] = []
    for t in raw_tokens or []:
        s = str(t or "").strip()
        if not s:
            continue
        # JSON array
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    for x in arr:
                        xs = str(x or "").strip()
                        if xs:
                            flat.append(xs)
                    continue
            except Exception:
                pass
        # split by common separators
        parts = re.split(r"[,\s;|]+", s)
        for p in parts:
            pp = (p or "").strip()
            if pp:
                flat.append(pp)
    # canonicalize + de-dup
    out: List[str] = []
    seen: set[str] = set()
    for k in flat:
        ck = _canon_key(k)
        if not ck or ck in seen:
            continue
        seen.add(ck)
        out.append(ck)
    return out


# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class TaskSpec:
    key: str
    sheet_name: str                   # Google Sheet tab name + backend canonical page
    gateway: str                      # enriched | analysis | advanced | argaam
    priority: int = 5
    max_symbols: int = 500
    allow_empty_symbols: bool = True  # allow schema-only write when symbols list is empty
    expects_rows: bool = True         # v6.9.0: page MUST have data rows when healthy.
                                      # headers + 0 rows => failed fetch => skip clear+write
                                      # (preserve last-good) instead of blanking the tab.
                                      # Default True (protect); set False only for a page that
                                      # legitimately writes headers-only via the daily sync.


@dataclass(slots=True)
class TaskResult:
    key: str
    sheet_name: str
    status: str
    start_utc: str
    end_utc: Optional[str] = None
    duration_ms: float = 0.0
    symbols_requested: int = 0
    symbols_processed: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    gateway_used: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "sheet_name": self.sheet_name,
            "status": self.status,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "duration_ms": self.duration_ms,
            "symbols_requested": self.symbols_requested,
            "symbols_processed": self.symbols_processed,
            "rows_written": self.rows_written,
            "rows_failed": self.rows_failed,
            "gateway_used": self.gateway_used,
            "warnings": self.warnings,
            "error": self.error,
            "request_id": self.request_id,
            "version": SCRIPT_VERSION,
        }


@dataclass(slots=True)
class RunSummary:
    version: str = SCRIPT_VERSION
    start_utc: str = field(default_factory=lambda: _utc_now().isoformat())
    end_utc: Optional[str] = None
    duration_ms: float = 0.0
    total_tasks: int = 0
    success: int = 0
    partial: int = 0
    failed: int = 0
    skipped: int = 0
    total_rows_written: int = 0
    total_rows_failed: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "duration_ms": self.duration_ms,
            "total_tasks": self.total_tasks,
            "success": self.success,
            "partial": self.partial,
            "failed": self.failed,
            "skipped": self.skipped,
            "total_rows_written": self.total_rows_written,
            "total_rows_failed": self.total_rows_failed,
        }


# -----------------------------------------------------------------------------
# Backend client (httpx preferred)
# -----------------------------------------------------------------------------
class BackendClient:
    def __init__(self, base_url: str, timeout_sec: float = 30.0, token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.token = (token or "").strip()
        self._client = None  # lazy

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
            h["X-APP-TOKEN"] = self.token
        return h

    async def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            import httpx
        except Exception as e:
            raise RuntimeError(f"httpx not available: {e}")
        self._client = httpx.AsyncClient(timeout=self.timeout_sec, headers=self._headers())
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def get_json(self, path: str) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        url = f"{self.base_url}{path}"
        try:
            client = await self._get_client()
            r = await client.get(url)
            code = int(r.status_code)
            if code != 200:
                return None, f"HTTP {code}: {r.text[:200]}", code
            try:
                return r.json(), None, code
            except Exception as e:
                return None, f"JSON parse error: {e}", code
        except Exception as e:
            return None, str(e), 0

    async def post_json(self, path: str, payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        url = f"{self.base_url}{path}"
        max_retries = 3
        for attempt in range(max_retries):
            try:
                client = await self._get_client()
                r = await client.post(url, json=payload)
                code = int(r.status_code)

                if code in (429,) or (500 <= code < 600):
                    if attempt == max_retries - 1:
                        return None, f"HTTP {code}: {r.text[:200]}", code
                    await asyncio.sleep(min(10.0, (2**attempt) + random.uniform(0, 1.0)))
                    continue

                if code != 200:
                    return None, f"HTTP {code}: {r.text[:200]}", code

                try:
                    return r.json(), None, code
                except Exception as e:
                    return None, f"JSON parse error: {e}", code

            except Exception as e:
                if attempt == max_retries - 1:
                    return None, str(e), 0
                await asyncio.sleep(min(10.0, (2**attempt) + random.uniform(0, 1.0)))

        return None, "Unknown error", 0


# -----------------------------------------------------------------------------
# Redis distributed lock (optional)
# -----------------------------------------------------------------------------
class RedisLock:
    def __init__(self, lock_name: str, ttl_sec: int = 300):
        self.lock_name = f"tfb:dashboard_sync:{lock_name}"
        self.ttl_sec = int(ttl_sec)
        self.value = str(uuid.uuid4())
        self._redis = None
        self.acquired = False

    async def _get_redis(self):
        if self._redis is not None:
            return self._redis
        url = (os.getenv("REDIS_URL") or "").strip()
        if not url:
            return None
        try:
            import redis.asyncio as redis_async
        except Exception:
            return None
        try:
            self._redis = redis_async.from_url(url, decode_responses=True)
            return self._redis
        except Exception:
            return None

    async def acquire(self) -> bool:
        r = await self._get_redis()
        if r is None:
            self.acquired = True
            return True
        try:
            ok = await r.set(self.lock_name, self.value, nx=True, ex=self.ttl_sec)
            self.acquired = bool(ok)
            return self.acquired
        except Exception:
            self.acquired = False
            return False

    async def release(self) -> bool:
        r = await self._get_redis()
        if r is None:
            return True
        if not self.acquired:
            return True
        lua = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            res = await r.eval(lua, 1, self.lock_name, self.value)
            self.acquired = False
            return bool(res)
        except Exception:
            return False

    async def close(self) -> None:
        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None


# -----------------------------------------------------------------------------
# Google Sheets writer (optional, direct API)
# -----------------------------------------------------------------------------
class SheetsWriter:
    def __init__(self):
        self._service = None  # lazy

    def _fix_private_key(self, d: Dict[str, Any]) -> Dict[str, Any]:
        try:
            pk = d.get("private_key")
            if isinstance(pk, str) and "\\n" in pk:
                d["private_key"] = pk.replace("\\n", "\n")
        except Exception:
            pass
        return d

    def _load_credentials_dict(self) -> Optional[Dict[str, Any]]:
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()

        # Prefer GOOGLE_APPLICATION_CREDENTIALS file path (GitHub Actions pattern)
        path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
        if path and os.path.exists(path):
            try:
                d = json.loads(Path(path).read_text(encoding="utf-8"))
                return self._fix_private_key(d) if isinstance(d, dict) else None
            except Exception:
                return None

        if not raw:
            return None

        try:
            if raw.startswith("{") and raw.endswith("}"):
                d = json.loads(raw)
            else:
                d = json.loads(base64.b64decode(raw).decode("utf-8"))
            return self._fix_private_key(d) if isinstance(d, dict) else None
        except Exception:
            return None

    def _get_service(self):
        if self._service is not None:
            return self._service

        creds_dict = self._load_credentials_dict()
        if not creds_dict:
            return None
        try:
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build
        except Exception:
            return None

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return self._service

    def _safe_sheet_a1(self, sheet_name: str) -> str:
        # Always quote if not safe
        if _SHEET_SAFE_RE.match(sheet_name or ""):
            return sheet_name
        name = (sheet_name or "").replace("'", "''")
        return f"'{name}'"

    def clear_from(self, spreadsheet_id: str, sheet_name: str, start_a1: str) -> None:
        svc = self._get_service()
        if not svc:
            return
        m = re.match(r"^\$?([A-Za-z]+)\$?(\d+)$", start_a1.strip())
        if not m:
            return
        col = m.group(1).upper()
        row = int(m.group(2))
        rng = f"{self._safe_sheet_a1(sheet_name)}!{col}{row}:ZZ"
        svc.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=rng, body={}).execute()

    def write_table(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        start_a1: str,
        headers: List[Any],
        rows: List[List[Any]],
    ) -> int:
        svc = self._get_service()
        if not svc:
            return 0

        # Ensure rectangular rows matching header length (Sheets-friendly)
        hdr = [str(h) for h in (headers or [])]
        width = len(hdr)

        matrix: List[List[Any]] = []
        for r in rows or []:
            rr = list(r) if isinstance(r, list) else [r]
            if width > 0:
                if len(rr) < width:
                    rr = rr + [None] * (width - len(rr))
                elif len(rr) > width:
                    rr = rr[:width]
            matrix.append([_coerce_jsonable(x) for x in rr])

        values: List[List[Any]] = []
        if hdr:
            values.append(hdr)
        values.extend(matrix)

        rng = f"{self._safe_sheet_a1(sheet_name)}!{start_a1}"
        body = {"majorDimension": "ROWS", "values": values}
        # v6.18.1 (WHY 1): retry the atomic update on TRANSIENT transport
        # failures only (SSL EOF, reset, timeout, 429/5xx). values.update is
        # idempotent — same block, same range — so a retry after an ambiguous
        # mid-response EOF cannot corrupt the sheet. Non-transient errors raise
        # on the first attempt exactly as v6.18.0 did.
        _attempts = _write_retry_attempts()
        _backoffs = (2.0, 5.0, 5.0, 5.0)
        for _try in range(_attempts):
            try:
                svc.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=rng,
                    valueInputOption="RAW",
                    body=body,
                ).execute()
                break
            except Exception as _we:
                if _try + 1 >= _attempts or not _is_transient_write_error(_we):
                    raise
                logger.warning(
                    f"write_table transient failure on '{sheet_name}' "
                    f"(attempt {_try + 1}/{_attempts}); retrying in "
                    f"{_backoffs[min(_try, len(_backoffs) - 1)]:.0f}s: {_we}"
                )
                time.sleep(_backoffs[min(_try, len(_backoffs) - 1)])

        return max(0, len(values) - (1 if hdr else 0))

    def read_values(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        a1_range: str = "A1:EZ2000",
    ) -> Optional[List[List[Any]]]:
        """
        Read a rectangular block of UNFORMATTED cell values from a sheet.

        Returns the list of rows on success (possibly an empty list when the
        sheet/range holds no data), or None on ANY failure (no service, API
        error) so callers can distinguish 'sheet is empty' (->[]) from 'read
        could not be performed' (->None). The write service account has full
        spreadsheets scope (read + write), so this reuses the same service the
        writer already builds.
        """
        svc = self._get_service()
        if not svc:
            return None
        try:
            rng = f"{self._safe_sheet_a1(sheet_name)}!{a1_range}"
            resp = svc.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=rng,
                majorDimension="ROWS",
                valueRenderOption="UNFORMATTED_VALUE",
            ).execute()
            vals = resp.get("values", [])
            return vals if isinstance(vals, list) else []
        except Exception:
            return None


# -----------------------------------------------------------------------------
# My_Portfolio manual-cell write guard (v6.5.0)
#
# Prevents an upstream read miss (blank Qty/Avg Cost in the payload) from
# overwriting the user's real, irreplaceable manual inputs on the live sheet.
# Degraded-payload detection -> whole-write skip. See module docstring for the
# full rationale. Fail-safe: any uncertainty skips the write to protect data.
# -----------------------------------------------------------------------------
_GUARD_TAG = "[v6.5.0 PORTFOLIO-GUARD]"

# Default page(s) the guard protects. Overridable via env (comma list).
_GUARD_DEFAULT_PAGES = ("My_Portfolio",)

# High-confidence, unambiguously user-authored columns used as the degradation
# sentinel. Deliberately limited to the position-math INPUTS (quantity +
# average cost): their blanking is the exact symptom of an upstream read miss,
# and they are never produced by a market feed (so a fresh payload that has
# them blank — while the sheet still holds them — is a reliable failure signal).
_GUARD_SENTINEL_ALIASES = frozenset({
    # quantity
    "qty", "positionqty", "quantity", "positionquantity", "shares", "units",
    # average cost / entry price
    "avgcost", "averagecost", "avgcostprice", "positionavgcost",
    "avgprice", "averageprice", "costbasis", "avgbuyprice", "averagebuyprice",
})

# Symbol/identifier column aliases (for row matching across payload <-> sheet).
_GUARD_SYMBOL_ALIASES = frozenset({
    "symbol", "ticker", "tickersymbol", "symbolticker", "code", "instrument",
})

# Company-name column aliases (v6.22.0 — identity tripwire needs the Name cell).
_GUARD_NAME_ALIASES = frozenset({
    "name", "companyname", "company", "longname", "shortname", "securityname",
})

# -----------------------------------------------------------------------------
# Decision-owned (cockpit) page guard (v6.6.0)
# -----------------------------------------------------------------------------
# Python-side mirror of the GAS isDecisionOwnedPage_ guard (00_Config.gs). A
# decision-owned page (Top_10_Investments) carries cockpit-authored decision
# columns AND is served fresh on demand by data_engine_v2 via the route, so the
# daily sync must NOT write (and clear) it — doing so blanks the user's
# decisions every cycle. Unlike the column-level My_Portfolio guard, the WHOLE
# page is owned, so the guard is a page-level SKIP taken before any fetch/write.
_DECISION_GUARD_TAG = "[v6.6.0 DECISION-GUARD]"

# Default decision-owned page(s). Overridable via env (comma list).
_DECISION_GUARD_DEFAULT_PAGES = ("Top_10_Investments",)


def _guard_norm(s: Any) -> str:
    """Lowercase + strip non-alphanumerics (matches rows_reader normalization)."""
    return re.sub(r"[^a-z0-9]+", "", str(s if s is not None else "").lower())


def _guard_is_blank(v: Any) -> bool:
    """A cell is blank iff it is None or a whitespace-only string. 0 is NOT blank."""
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    return False


def _guard_pages() -> set:
    raw = (os.getenv("TFB_SYNC_MANUAL_GUARD_PAGES") or "").strip()
    pages = [p.strip() for p in raw.split(",") if p.strip()] if raw else list(_GUARD_DEFAULT_PAGES)
    return {_guard_norm(p) for p in pages}


def _guard_enabled() -> bool:
    return (os.getenv("TFB_SYNC_MANUAL_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _guard_should_apply(sheet_name: str) -> bool:
    """True iff the guard is enabled AND this page is in the protected set."""
    if not _guard_enabled():
        return False
    return _guard_norm(sheet_name) in _guard_pages()


# -----------------------------------------------------------------------------
# Cross-page price-delta guard (v6.20.0, Fix 1b) — observe-and-report only
# -----------------------------------------------------------------------------
# The same symbol served with materially different prices on two pages in ONE
# run (live fingerprint 2026-07-05: 1211.SR 17.73 on Market_Leaders vs 58.90
# on Global_Markets) means at least one page is stale/contaminated. The runner
# is the only place that sees every page's final matrix in-process, so it
# detects and LOGS the disagreement; it deliberately does not decide which
# page is wrong (no row mutation, no write blocking).
_XPAGE_PRICE_ALIASES = frozenset({"currentprice", "price", "lastprice"})
_XPAGE_PRICES: Dict[str, List[Tuple[str, float]]] = {}
_XPAGE_TAG = "[v6.20.0 XPAGE]"


def _xpage_check_enabled() -> bool:
    """Master switch. DEFAULT OFF (backward-compatible); set
    TFB_XPAGE_PRICE_CHECK=1/true/on/yes to enable. OFF -> no harvest, no
    report line, v6.19.2 byte-identical."""
    return (os.getenv("TFB_XPAGE_PRICE_CHECK") or "").strip().lower() in {"1", "true", "on", "yes"}


def _xpage_delta_threshold_pct() -> float:
    """Spread threshold in percent ((hi-lo)/lo*100). Default 2.0, clamped
    0.1..100.0 — wide enough to ignore provider rounding / minor timing skew,
    tight enough to catch every real staleness/contamination case seen live
    (the smallest real offender observed was ~7%)."""
    try:
        v = float((os.getenv("TFB_XPAGE_PRICE_DELTA_PCT") or "2.0").strip())
    except Exception:
        v = 2.0
    return max(0.1, min(100.0, v))


def _xpage_max_report() -> int:
    """Max WARN lines emitted (worst offenders first). Default 50, clamped 1..500."""
    try:
        v = int(float((os.getenv("TFB_XPAGE_MAX_REPORT") or "50").strip()))
    except Exception:
        v = 50
    return max(1, min(500, v))


def _xpage_collect(sheet_name: str, headers: List[Any], rows_matrix: List[List[Any]]) -> int:
    """Harvest (page, symbol, price) from a task's FINAL matrix into the
    run-level collector. Returns rows harvested. Pages lacking a symbol or
    price column contribute 0. Never raises (caller also wraps)."""
    try:
        if not headers or not rows_matrix:
            return 0
        sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
        px_i = _guard_find_col(list(headers), _XPAGE_PRICE_ALIASES)
        if sym_i < 0 or px_i < 0:
            return 0
        page = str(sheet_name or "").strip() or "?"
        n = 0
        hi_idx = max(sym_i, px_i)
        for row in rows_matrix:
            if not isinstance(row, (list, tuple)) or len(row) <= hi_idx:
                continue
            sym = str(row[sym_i] if row[sym_i] is not None else "").strip().upper()
            if not sym:
                continue
            try:
                px = float(row[px_i])
            except Exception:
                continue
            if not (0.0 < px < 1e15):  # rejects 0/negative/NaN/inf
                continue
            _XPAGE_PRICES.setdefault(sym, []).append((page, px))
            n += 1
        return n
    except Exception:
        return 0


def _xpage_report() -> Tuple[Dict[str, int], List[str]]:
    """Compare every symbol seen on 2+ pages; return (stats, warn_lines) and
    CLEAR the collector (re-entrant). A symbol conflicts when its max spread
    (hi-lo)/lo*100 exceeds the threshold. Lines are worst-first, capped."""
    stats = {"pages": 0, "symbols": 0, "symbols_multi_page": 0, "conflicts": 0}
    lines: List[str] = []
    try:
        thr = _xpage_delta_threshold_pct()
        pages_seen: set = set()
        conflicts: List[Tuple[float, str, List[Tuple[str, float]]]] = []
        for sym, obs in _XPAGE_PRICES.items():
            for pg, _px in obs:
                pages_seen.add(pg)
            by_page: Dict[str, float] = {}
            for pg, px in obs:
                by_page.setdefault(pg, px)  # first write per page wins
            if len(by_page) < 2:
                continue
            stats["symbols_multi_page"] += 1
            lo = min(by_page.values())
            hi = max(by_page.values())
            if lo <= 0.0:
                continue
            delta = (hi - lo) / lo * 100.0
            if delta > thr:
                conflicts.append((delta, sym, sorted(by_page.items())))
        stats["pages"] = len(pages_seen)
        stats["symbols"] = len(_XPAGE_PRICES)
        stats["conflicts"] = len(conflicts)
        conflicts.sort(key=lambda t: (-t[0], t[1]))
        for delta, sym, pairs in conflicts[: _xpage_max_report()]:
            detail = "; ".join("%s=%.6g" % (pg, px) for pg, px in pairs)
            lines.append("%s %s spread=%.1f%% :: %s" % (_XPAGE_TAG, sym, delta, detail))
    except Exception:
        pass
    finally:
        try:
            _XPAGE_PRICES.clear()
        except Exception:
            pass
    return stats, lines


# -----------------------------------------------------------------------------
# Small-page starvation fixes (v6.21.0, Fix #6) — order override + empty retry
# -----------------------------------------------------------------------------
def _page_order_override() -> List[str]:
    """v6.21.0 (6a): csv of sheet names / task keys from TFB_SYNC_PAGE_ORDER.
    Unset/blank -> [] (byte-identical v6.20.0 launch order)."""
    raw = (os.getenv("TFB_SYNC_PAGE_ORDER") or "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()] if raw else []


def _apply_page_order(tasks: List["TaskSpec"]) -> List["TaskSpec"]:
    """v6.21.0 (6a): reassign launch priorities for the ENRICHED market tasks
    per the override list. Listed pages take positions 1..k in the given
    order; unlisted enriched tasks follow in their original relative order;
    analysis/cockpit tasks are untouched (their priorities 6+ keep them after
    every universe). Unknown tokens -> one warning, ignored. Never raises."""
    order = _page_order_override()
    if not order:
        return tasks
    try:
        def _tok(s: str) -> str:
            return _guard_norm(s).replace("_", "")
        want = [_tok(p) for p in order]
        enriched = [t for t in tasks if t.gateway == "enriched"]
        by_tok: Dict[str, "TaskSpec"] = {}
        for t in enriched:
            by_tok[_tok(t.sheet_name)] = t
            by_tok[_tok(t.key)] = t
        listed: List["TaskSpec"] = []
        seen: set = set()
        unknown: List[str] = []
        for raw_tok, disp in zip(want, order):
            t = by_tok.get(raw_tok)
            if t is None:
                unknown.append(disp)
                continue
            if id(t) not in seen:
                seen.add(id(t))
                listed.append(t)
        if unknown:
            logger.warning(
                "[v6.21.0 ORDER] unknown page token(s) ignored: %s",
                ", ".join(unknown),
            )
        if not listed:
            return tasks
        rest = [t for t in enriched if id(t) not in seen]
        for i, t in enumerate(listed + rest, start=1):
            t.priority = i
        logger.info(
            "[v6.21.0 ORDER] enriched launch order: %s",
            " -> ".join(t.sheet_name for t in sorted(enriched, key=lambda x: x.priority)),
        )
    except Exception as e:
        logger.warning("[v6.21.0 ORDER] override skipped (error: %s)", e)
    return tasks


def _empty_retry_enabled() -> bool:
    """v6.21.0 (6b) master switch. DEFAULT OFF; TFB_SYNC_EMPTY_RETRY=1 arms
    the one-shot re-fetch of empty-price rows after a batched market fetch."""
    return (os.getenv("TFB_SYNC_EMPTY_RETRY") or "").strip().lower() in {"1", "true", "on", "yes"}


def _empty_retry_max() -> int:
    try:
        v = int(float((os.getenv("TFB_SYNC_EMPTY_RETRY_MAX") or "120").strip()))
    except Exception:
        v = 120
    return max(1, min(1000, v))


def _empty_retry_delay_sec() -> float:
    try:
        v = float((os.getenv("TFB_SYNC_EMPTY_RETRY_DELAY_SEC") or "0").strip())
    except Exception:
        v = 0.0
    return max(0.0, min(120.0, v))


async def _retry_empty_rows(
    backend: "BackendClient",
    task: "TaskSpec",
    headers: List[Any],
    rows_matrix: List[List[Any]],
    base_payload: Dict[str, Any],
    eff_gw: str,
    res: "TaskResult",
    fetch_fn: Any = None,
) -> Tuple[List[List[Any]], int]:
    """v6.21.0 (6b): ONE bounded re-fetch pass for rows whose price cell is
    empty/non-positive, splicing healed rows back BY SYMBOL. The splice only
    happens when the retry returns the IDENTICAL header row — a mismatch
    skips it with a warning, so the retry can never make the page worse.
    Returns (rows_matrix, healed_count). Never raises."""
    try:
        if not headers or not rows_matrix:
            return rows_matrix, 0
        sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
        px_i = _guard_find_col(list(headers), _XPAGE_PRICE_ALIASES)
        if sym_i < 0 or px_i < 0:
            return rows_matrix, 0
        hi = max(sym_i, px_i)

        def _px_ok(row: List[Any]) -> bool:
            try:
                v = float(str(row[px_i]).replace(",", ""))
                return 0.0 < v < 1e15
            except Exception:
                return False

        empty_syms: List[str] = []
        empty_pos: Dict[str, int] = {}
        for pos, row in enumerate(rows_matrix):
            if not isinstance(row, (list, tuple)) or len(row) <= hi:
                continue
            s = str(row[sym_i] or "").strip().upper()
            if not s or _px_ok(list(row)):
                continue
            if s not in empty_pos:
                empty_pos[s] = pos
                empty_syms.append(s)
        if not empty_syms:
            return rows_matrix, 0
        capped = empty_syms[: _empty_retry_max()]
        delay = _empty_retry_delay_sec()
        if delay > 0:
            await asyncio.sleep(delay)
        _fetch = fetch_fn or _fetch_market_rows_batched
        r_headers, r_matrix, _r_ep, _r_err = await _fetch(
            backend, task, capped, dict(base_payload), eff_gw, res
        )
        if not r_matrix:
            logger.info(
                "[v6.21.0 RETRY] %s: %d empty row(s), retry returned nothing (%s)",
                task.sheet_name, len(capped), _r_err or "no rows",
            )
            return rows_matrix, 0
        if list(r_headers or []) != list(headers):
            _w = ("[v6.21.0 RETRY] %s: header mismatch on retry — splice "
                  "skipped (page left as fetched)" % task.sheet_name)
            res.warnings.append(_w)
            logger.warning(_w)
            return rows_matrix, 0
        healed = 0
        for row in r_matrix:
            if not isinstance(row, (list, tuple)) or len(row) <= hi:
                continue
            s = str(row[sym_i] or "").strip().upper()
            pos = empty_pos.get(s)
            if pos is None or not _px_ok(list(row)):
                continue
            rows_matrix[pos] = list(row)
            healed += 1
        logger.info(
            "[v6.21.0 RETRY] %s: empties=%d retried=%d healed=%d",
            task.sheet_name, len(empty_syms), len(capped), healed,
        )
        if healed:
            res.warnings.append(
                "[v6.21.0 RETRY] healed %d empty row(s) on second pass" % healed
            )
        return rows_matrix, healed
    except Exception as e:
        logger.warning("[v6.21.0 RETRY] %s skipped (error: %s)", task.sheet_name, e)
        return rows_matrix, 0


def _decision_guard_enabled() -> bool:
    """Decision-owned-page guard master switch. Default ON; set
    TFB_SYNC_DECISION_GUARD=0/false/off/no to restore the v6.5.0 behavior
    (the daily sync writes decision-owned pages again)."""
    return (os.getenv("TFB_SYNC_DECISION_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _decision_guard_pages() -> set:
    """Decision-owned (cockpit) page set. Overridable via
    TFB_SYNC_DECISION_GUARD_PAGES (comma-separated); defaults to
    Top_10_Investments."""
    raw = (os.getenv("TFB_SYNC_DECISION_GUARD_PAGES") or "").strip()
    pages = [p.strip() for p in raw.split(",") if p.strip()] if raw else list(_DECISION_GUARD_DEFAULT_PAGES)
    return {_guard_norm(p) for p in pages}


def _decision_guard_should_skip(sheet_name: str) -> bool:
    """True iff the decision-owned-page guard is enabled AND this page is
    cockpit/decision-owned. Python-side mirror of the GAS isDecisionOwnedPage_
    guard: the daily sync must not write (and clear) a page the user owns, or
    it blanks the cockpit's decision cells."""
    if not _decision_guard_enabled():
        return False
    return _guard_norm(sheet_name) in _decision_guard_pages()


def _page_limit_fix_enabled() -> bool:
    """Page-driven limit fix (v6.7.0) master switch. Default ON; set
    TFB_SYNC_PAGE_LIMIT_FIX=0/false/off/no to restore the v6.6.0 behavior
    (an empty symbol list sends limit:1, which silently truncates every
    page-driven page — Market_Leaders, Global_Markets, Commodities_FX,
    Mutual_Funds — to a single written row)."""
    return (os.getenv("TFB_SYNC_PAGE_LIMIT_FIX") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _min_coverage_pct() -> float:
    """v6.18.2: minimum fetched-rows coverage (percent of REQUESTED symbols)
    below which a partial fetch is treated like a degenerate one — the write
    is skipped and last-good rows are preserved (the Market_Leaders 288->163
    universe ratchet of 2026-07-02). Default 70. 0 disables the guard and
    restores v6.18.1 behavior exactly."""
    try:
        v = float((os.getenv("TFB_SYNC_MIN_COVERAGE_PCT") or "70").strip())
    except Exception:
        v = 70.0
    return max(0.0, min(100.0, v))


def _empty_guard_enabled() -> bool:
    """Empty-rows wipe guard (v6.9.0) master switch. Default ON; set
    TFB_SYNC_EMPTY_GUARD=0/false/off/no to restore the v6.8.0 behavior (a page
    that returns headers + 0 data rows is CLEARED and rewritten headers-only,
    blanking the tab and reporting status="success"). With the guard ON, a
    TaskSpec(expects_rows=True) page that fetched 0 rows skips the clear AND the
    write, preserving last-good rows; it self-heals on the next healthy sync."""
    return (os.getenv("TFB_SYNC_EMPTY_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _top10_selfheal_enabled() -> bool:
    """Top_10 header self-heal (v6.15.1). Default ON; set
    TFB_TOP10_HEADER_SELFHEAL=0/false/off/no to disable. When a Top_10 fetch
    returns 0 data rows (the data write is skipped to preserve last-good rows),
    still repair a blank header row so the existing rows stay labeled and the
    validator can map columns. No ENV change is needed to activate it."""
    return (os.getenv("TFB_TOP10_HEADER_SELFHEAL") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _guard_find_col(header_row: List[Any], aliases: frozenset) -> int:
    """Index of the first header whose normalized name is in aliases, else -1."""
    for i, h in enumerate(header_row or []):
        if _guard_norm(h) in aliases:
            return i
    return -1


def _guard_find_header_row(grid: List[List[Any]]) -> int:
    """
    Locate the header row within the first rows of a sheet read. Robust to any
    title/branding rows above the header (e.g. a header written at the A5
    default). The header is the first row that contains BOTH a symbol column and
    at least one sentinel (manual) column. Returns the row index, or -1.
    """
    scan = min(len(grid or []), 15)
    for r in range(scan):
        row = grid[r] if isinstance(grid[r], list) else []
        if _guard_find_col(row, _GUARD_SYMBOL_ALIASES) >= 0 and _guard_find_col(row, _GUARD_SENTINEL_ALIASES) >= 0:
            return r
    return -1


def _portfolio_write_guard(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    sheet_name: str,
    headers: List[Any],
    rows_matrix: List[List[Any]],
) -> Tuple[bool, str]:
    """
    Decide whether it is safe to write a manual-input page (My_Portfolio) now.

    Returns (allow_write, note):
      - (True,  "")    -> safe; proceed with the normal write.
      - (True,  note)  -> safe; proceed; note is informational only.
      - (False, note)  -> NOT safe; SKIP the write to protect manual cells.

    The guard reads the live sheet independently of the engine's reader and
    refuses the write if any symbol that currently holds Qty/Avg Cost would be
    blanked by the outgoing payload, or if the verification read cannot be
    trusted (fail-safe -> skip, never write blind).
    """
    # Locate sentinel + symbol columns on the OUTGOING payload.
    out_sym_idx = _guard_find_col(headers, _GUARD_SYMBOL_ALIASES)
    out_sentinels = [i for i, h in enumerate(headers or []) if _guard_norm(h) in _GUARD_SENTINEL_ALIASES]
    if out_sym_idx < 0 or not out_sentinels:
        return (False, f"{_GUARD_TAG} skip: outgoing {sheet_name} payload is missing a symbol or manual (Qty/Avg Cost) column; write skipped to protect manual cells.")

    # Read the live sheet (independent of the engine's reader path).
    grid = sheets.read_values(spreadsheet_id, sheet_name) if sheets is not None else None
    if grid is None:
        return (False, f"{_GUARD_TAG} skip: could not read live {sheet_name} to verify manual cells; write skipped to protect data.")
    if not grid:
        # Read succeeded but sheet is empty (first write) -> nothing to lose.
        return (True, "")

    hdr_idx = _guard_find_header_row(grid)
    if hdr_idx < 0:
        return (False, f"{_GUARD_TAG} skip: could not locate a header row in live {sheet_name}; write skipped to protect data.")

    ex_header = grid[hdr_idx] if isinstance(grid[hdr_idx], list) else []
    ex_sym_idx = _guard_find_col(ex_header, _GUARD_SYMBOL_ALIASES)
    if ex_sym_idx < 0:
        return (False, f"{_GUARD_TAG} skip: live {sheet_name} header has no symbol column; write skipped to protect data.")

    # Map existing sentinel columns by normalized header name so the comparison
    # is like-for-like even if column ORDER differs between writes.
    ex_sentinel_by_norm: Dict[str, int] = {}
    for i, h in enumerate(ex_header):
        n = _guard_norm(h)
        if n in _GUARD_SENTINEL_ALIASES and n not in ex_sentinel_by_norm:
            ex_sentinel_by_norm[n] = i

    # Build {SYMBOL -> {sentinel_norm -> populated?}} from existing data rows.
    existing: Dict[str, Dict[str, bool]] = {}
    for r in range(hdr_idx + 1, len(grid)):
        row = grid[r] if isinstance(grid[r], list) else []
        if ex_sym_idx >= len(row):
            continue
        sym = str(row[ex_sym_idx]).strip().upper()
        if not sym:
            continue
        flags: Dict[str, bool] = {}
        for n, ci in ex_sentinel_by_norm.items():
            val = row[ci] if ci < len(row) else None
            flags[n] = not _guard_is_blank(val)
        existing[sym] = flags

    if not existing:
        # No existing holdings carry manual data -> nothing to lose.
        return (True, "")

    # Normalized name for each outgoing sentinel column (for like-for-like cmp).
    out_sentinel_norm = {i: _guard_norm(headers[i]) for i in out_sentinels}

    regressed: List[str] = []
    for row in rows_matrix or []:
        if out_sym_idx >= len(row):
            continue
        sym = str(row[out_sym_idx]).strip().upper()
        if not sym or sym not in existing:
            continue
        ex_flags = existing[sym]
        for i, n in out_sentinel_norm.items():
            new_blank = _guard_is_blank(row[i]) if i < len(row) else True
            if new_blank and ex_flags.get(n, False):
                regressed.append(sym)
                break

    if regressed:
        uniq = sorted(set(regressed))
        shown = ", ".join(uniq[:8]) + (" …" if len(uniq) > 8 else "")
        return (False, f"{_GUARD_TAG} skip: outgoing payload would blank existing Qty/Avg Cost for {len(uniq)} holding(s) [{shown}]; write skipped to protect manual cells (self-heals on next healthy sync).")

    return (True, "")


# -----------------------------------------------------------------------------
# Symbols reading (uses repo module if present)
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# My_Portfolio rebuild from _Portfolio_CostBasis (v6.14.0)
#
# WHY: My_Portfolio's authoritative content is the user's manually-maintained
# holdings — symbol, quantity, average (buy) cost — which live ONLY in the
# _Portfolio_CostBasis tab. The page-driven enriched request (empty symbol
# list) returns the backend's own default/page rows WITHOUT the user's
# quantities, so the v6.5.0 guard correctly refuses every write (it would blank
# Qty/Avg Cost). Net effect: My_Portfolio never refreshes.
# FIX (gated OFF by TFB_PORTFOLIO_REBUILD): when enabled AND the task is
# My_Portfolio, (1) source the symbol list from _Portfolio_CostBasis so the
# backend returns enriched rows for the user's ACTUAL holdings with live
# prices/recommendation; (2) inject the user's Qty + Avg Cost back into those
# rows and recompute the position-math columns (MV / Cost / P&L) consistently
# with a per-row FX derived from the payload's own Price vs Price-SAR — so the
# guard passes and no internally-inconsistent half-row (fresh price against a
# blank value) is ever written; (3) classify known sukuk / fixed-income
# instruments so they are not framed by equity valuation columns.
# SAFETY: applies to My_Portfolio only. On ANY uncertainty (cost basis
# unreadable/empty, or the payload lacks a symbol / Qty / Avg-Cost column) the
# rebuild NO-OPS and the existing page-driven flow + guard run unchanged — so a
# failed rebuild can only fall back to the current (safe) blocked state, never
# to corrupted data. The FX/position math reproduces the engine's own
# Portfolio_Decision figures (unit-tested in tests/test_portfolio_rebuild.py).
# Full fixed-income analytics (yield/duration/credit) are NOT claimed here;
# sukuk are LABELED and held, not valued as equities.
# -----------------------------------------------------------------------------
_PORTFOLIO_REBUILD_TAG = "[v6.14.0 PORTFOLIO-REBUILD]"
_COST_BASIS_SHEET = "_Portfolio_CostBasis"

_CB_SYMBOL_ALIASES = frozenset({"symbol", "ticker", "code", "instrument"})
_CB_QTY_ALIASES = frozenset({"quantity", "qty", "shares", "units", "positionqty", "positionquantity"})
_CB_COST_ALIASES = frozenset({"buyprice", "avgcost", "averagecost", "avgbuyprice",
                              "averagebuyprice", "costbasis", "avgcostprice", "cost", "price"})

# Position-math columns recomputed after injection (alias-matched, normalized).
_PM_QTY_ALIASES = frozenset({"qty", "quantity", "shares", "units", "positionqty", "positionquantity"})
_PM_AVGCOST_ALIASES = frozenset({"avgcost", "averagecost", "avgcostprice", "positionavgcost",
                                 "avgprice", "averageprice", "costbasis", "avgbuyprice", "averagebuyprice"})
_PM_PRICE_ALIASES = frozenset({"price", "lastprice", "currentprice"})
_PM_PRICESAR_ALIASES = frozenset({"pricesar"})
_PM_MVSAR_ALIASES = frozenset({"mvsar", "marketvaluesar", "positionvaluesar", "positionvalue", "marketvalue"})
_PM_COSTSAR_ALIASES = frozenset({"costsar"})
_PM_PNLSAR_ALIASES = frozenset({"plsar", "pnlsar", "unrealizedplsar", "unrealizedpnlsar"})
_PM_PNLPCT_ALIASES = frozenset({"plpct", "pnlpct"})
_PM_ASSETCLASS_ALIASES = frozenset({"assetclass", "type", "instrumenttype", "class"})


def _portfolio_rebuild_enabled() -> bool:
    """My_Portfolio rebuild master switch. DEFAULT OFF; set
    TFB_PORTFOLIO_REBUILD=1/true/on/yes to enable cost-basis-sourced refresh."""
    return (os.getenv("TFB_PORTFOLIO_REBUILD") or "0").strip().lower() in {"1", "true", "on", "yes"}


def _fixed_income_symbols() -> set:
    """Symbols to classify as fixed income (sukuk/bonds) and exclude from the
    equity sell/valuation framing. Comma-list override via
    TFB_FIXED_INCOME_SYMBOLS; defaults to the known Cenomi Centers Sukuk."""
    raw = (os.getenv("TFB_FIXED_INCOME_SYMBOLS") or "5023.SR").strip()
    return {s.strip().upper() for s in raw.split(",") if s.strip()}


def _pm_to_float(v: Any) -> Optional[float]:
    try:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def _find_pnl_pct_col(headers: List[Any]) -> int:
    """Index of the P&L-percent column. Matches the unambiguous normalized
    forms ('plpct'/'pnlpct'), OR a 'pl'/'pnl' header that visibly carries a '%'
    (e.g. 'P&L %', which normalizes to 'pl' — so it must NOT be matched by the
    bare 'pl' of a 'P&L' SAR column). Returns -1 if absent."""
    for i, h in enumerate(headers or []):
        n = _guard_norm(h)
        if n in {"plpct", "pnlpct", "plpercent", "pnlpercent"}:
            return i
        if n in {"pl", "pnl"} and "%" in str(h if h is not None else ""):
            return i
    return -1


def _read_cost_basis(sheets: "SheetsWriter", spreadsheet_id: str) -> Dict[str, Dict[str, float]]:
    """Read _Portfolio_CostBasis -> {SYMBOL: {'qty': float, 'cost': float}}.
    Returns {} on ANY failure so the caller no-ops the rebuild (fail-safe)."""
    try:
        grid = sheets.read_values(spreadsheet_id, _COST_BASIS_SHEET, "A1:Z200")
    except Exception:
        return {}
    if not grid or not isinstance(grid, list) or len(grid) < 2:
        return {}
    header = grid[0] if isinstance(grid[0], list) else []
    s_i = _guard_find_col(header, _CB_SYMBOL_ALIASES)
    q_i = _guard_find_col(header, _CB_QTY_ALIASES)
    c_i = _guard_find_col(header, _CB_COST_ALIASES)
    if s_i < 0 or q_i < 0 or c_i < 0:
        return {}
    out: Dict[str, Dict[str, float]] = {}
    for row in grid[1:]:
        if not isinstance(row, list):
            continue
        sym = str(row[s_i]).strip().upper() if s_i < len(row) and row[s_i] is not None else ""
        if not sym or sym in {"SYMBOL", "TICKER"}:
            continue
        qty = _pm_to_float(row[q_i]) if q_i < len(row) else None
        cost = _pm_to_float(row[c_i]) if c_i < len(row) else None
        if qty is None or cost is None:
            continue
        out[sym] = {"qty": qty, "cost": cost}
    return out


def _inject_portfolio_holdings(
    headers: List[Any],
    rows_matrix: List[List[Any]],
    cost_basis: Dict[str, Dict[str, float]],
) -> Tuple[List[List[Any]], int]:
    """Inject the user's Qty + Avg Cost into the payload rows and recompute the
    position-math columns (MV / Cost / P&L) consistently, using a per-row FX
    derived from the payload's own Price vs Price-SAR. Pure function (no I/O) so
    it is unit-testable. Returns (rows, injected_count). NO-OPS (returns input
    unchanged) when the symbol / Qty / Avg-Cost columns are absent — the guard
    then blocks the still-blank write, so the failure mode is the current safe
    blocked state, never corrupted data."""
    if not headers or not rows_matrix or not cost_basis:
        return rows_matrix, 0
    sym_i = _guard_find_col(headers, _GUARD_SYMBOL_ALIASES)
    qty_i = _guard_find_col(headers, _PM_QTY_ALIASES)
    avg_i = _guard_find_col(headers, _PM_AVGCOST_ALIASES)
    if sym_i < 0 or qty_i < 0 or avg_i < 0:
        return rows_matrix, 0  # cannot inject safely -> no-op
    price_i = _guard_find_col(headers, _PM_PRICE_ALIASES)
    psar_i = _guard_find_col(headers, _PM_PRICESAR_ALIASES)
    mv_i = _guard_find_col(headers, _PM_MVSAR_ALIASES)
    cost_i = _guard_find_col(headers, _PM_COSTSAR_ALIASES)
    pnl_i = _guard_find_col(headers, _PM_PNLSAR_ALIASES)
    pct_i = _find_pnl_pct_col(headers)
    cls_i = _guard_find_col(headers, _PM_ASSETCLASS_ALIASES)
    fi_syms = _fixed_income_symbols()

    width = len(headers)
    injected = 0
    out: List[List[Any]] = []
    for row in rows_matrix:
        rr = list(row) if isinstance(row, list) else [row]
        if len(rr) < width:
            rr = rr + [None] * (width - len(rr))
        sym = str(rr[sym_i]).strip().upper() if sym_i < len(rr) and rr[sym_i] is not None else ""
        hold = cost_basis.get(sym)
        if hold:
            qty = hold["qty"]
            buy = hold["cost"]
            rr[qty_i] = qty
            rr[avg_i] = buy
            price = _pm_to_float(rr[price_i]) if price_i >= 0 else None
            psar = _pm_to_float(rr[psar_i]) if psar_i >= 0 else None
            # Per-row FX from the payload's own native vs SAR price; SAR rows -> 1.0
            fx = (psar / price) if (price not in (None, 0) and psar not in (None, 0)) else 1.0
            unit_sar = psar if psar not in (None, 0) else (price if price not in (None, 0) else None)
            if unit_sar is not None:
                mv_sar = qty * unit_sar
                cost_sar = qty * buy * fx
                pnl_sar = mv_sar - cost_sar
                if mv_i >= 0:
                    rr[mv_i] = round(mv_sar, 2)
                if cost_i >= 0:
                    rr[cost_i] = round(cost_sar, 2)
                if pnl_i >= 0:
                    rr[pnl_i] = round(pnl_sar, 2)
                if pct_i >= 0 and cost_sar not in (None, 0):
                    rr[pct_i] = round(pnl_sar / cost_sar * 100.0, 2)
            if sym in fi_syms and cls_i >= 0:
                rr[cls_i] = "Fixed Income / Sukuk"
            injected += 1
        out.append(rr)
    return out, injected


# =============================================================================
# v6.15.0 — Top_10 header repair + decision-row reconciliation
# =============================================================================
_DECISION_RECONCILE_TAG = "[DECISION-RECONCILE]"
_DECISION_RECONCILE_PAGES = frozenset({
    _guard_norm("My_Portfolio"),
    _guard_norm("Top_10_Investments"),
})
# Recommendation families EXACTLY as scripts/validate_dashboard.py classifies
# them (_norm_token -> _SELL_FAMILY / _BUY_FAMILY), so whatever the validator
# flags, this reconciler also catches. _norm_token upper-cases and turns
# _ - / into spaces (e.g. "STRONG_SELL" -> "STRONG SELL").
_NT_SELL_FAMILY = frozenset({"REDUCE", "SELL", "STRONG SELL", "AVOID"})
_NT_BUY_FAMILY = frozenset({"STRONG BUY", "BUY", "ACCUMULATE"})
# substring tokens for robustness against decorated values ("REDUCE (TRIM)")
_SELL_SUBSTR = ("SELL", "REDUCE", "AVOID", "TRIM")
_BUY_SUBSTR = ("BUY", "ACCUMULATE", "ADD")
_RECO_COL_ALIASES = frozenset({"recommendation", "reco", "rec", "recommend"})
_ACTION_COL_ALIASES = frozenset({"finalaction", "action", "finalcall", "decision"})
_BLOCK_COL_ALIASES = frozenset({"blockreason", "blockedreason", "blockreasons", "block"})


def _norm_token_rds(x: Any) -> str:
    """Mirror of validate_dashboard._norm_token: upper-case, turn _ - / into
    spaces, collapse runs of spaces, strip -> identical classification."""
    s = str(x if x is not None else "").upper().replace("_", " ").replace("-", " ").replace("/", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


def _reco_is_sell(nt: str) -> bool:
    return (nt in _NT_SELL_FAMILY) or any(t in nt for t in _SELL_SUBSTR)


def _reco_is_buy(nt: str) -> bool:
    if "SELL" in nt:
        return False
    return (nt in _NT_BUY_FAMILY) or any(t in nt for t in _BUY_SUBSTR)


def _canonical_top10_schema() -> Tuple[List[str], List[str]]:
    """Return (headers, keys) for Top_10_Investments from the schema registry,
    or ([], []) on any failure (caller then no-ops -> fail-safe)."""
    try:
        from core.sheets import schema_registry as _sr  # optional dep; local import
        gh = getattr(_sr, "get_sheet_headers", None)
        gk = getattr(_sr, "get_sheet_keys", None)
        if callable(gh) and callable(gk):
            h = [str(x) for x in (gh("Top_10_Investments") or [])]
            k = [str(x) for x in (gk("Top_10_Investments") or [])]
            if h and k and len(h) == len(k):
                return h, k
    except Exception:
        pass
    return [], []


def _repair_top10_headers(
    headers: List[Any], data: Any, rows_matrix: List[List[Any]]
) -> List[Any]:
    """Rebuild a blank/short Top_10 header row from the canonical schema.

    The analysis route can return a header row of empty-string cells for
    Top_10; written verbatim this blanks every column title and breaks column
    mapping (validator: all rows 'missing price'). Column ORDER is taken from
    the response's own ``keys`` when present (each key mapped to its canonical
    header); otherwise the canonical order is used, but only when the data width
    matches the canonical width so titles line up with the columns.

    FAIL-SAFE: returns the ORIGINAL headers unchanged when the schema is
    unavailable or a safe rebuild is not possible -- it can never make the page
    worse than the (already blank) current state.
    """
    canon_headers, canon_keys = _canonical_top10_schema()
    if not canon_headers:
        return headers  # schema unavailable -> keep original

    cur = [str(h).strip() for h in (headers or [])]
    nonblank = sum(1 for h in cur if h)
    # Already healthy (right count, almost all labeled) -> keep as-is.
    if len(cur) == len(canon_headers) and nonblank >= int(0.9 * len(canon_headers)):
        return headers

    # Prefer the response's own column keys for exact alignment.
    keys: List[str] = []
    if isinstance(data, dict) and isinstance(data.get("keys"), list):
        keys = [str(k).strip() for k in data["keys"]]
    key_to_header = dict(zip(canon_keys, canon_headers))
    if keys and len(keys) == len(canon_keys) and all(k in key_to_header for k in keys):
        return [key_to_header[k] for k in keys]

    # No usable keys: fall back to canonical order, but ONLY when the data width
    # matches the canonical width (else titles would not line up with columns).
    width = 0
    for r in (rows_matrix or []):
        if isinstance(r, list):
            width = len(r)
            break
    if (not rows_matrix) or width == len(canon_headers):
        return list(canon_headers)
    return headers  # width mismatch -> cannot align safely -> keep original


def _reconcile_decision_rows(
    headers: List[Any], rows_matrix: List[List[Any]], page_label: str = ""
) -> Tuple[List[List[Any]], int]:
    """Make the displayed decision columns self-consistent (neutral only) and
    log exactly what it did so the next run is fully diagnosable.

    Two invariants, mirroring the dashboard integrity gates:
      1. A sell-family Recommendation must not still carry a Final Action of
         INVEST/BUY/ACCUMULATE -> set Final Action to HOLD (neutral; never a
         sell call).
      2. A buy-family Recommendation must not carry a non-empty Block Reason
         -> demote Recommendation to WATCH and Final Action to HOLD (the block
         is treated as legitimate; it is never cleared).

    Classification is IDENTICAL to scripts/validate_dashboard.py (_norm_token +
    its families), with a substring fallback for decorated values, so anything
    the validator flags is caught here. Returns (rows_matrix, changed_count).
    """
    reco_i = _guard_find_col(headers, _RECO_COL_ALIASES)
    action_i = _guard_find_col(headers, _ACTION_COL_ALIASES)
    block_i = _guard_find_col(headers, _BLOCK_COL_ALIASES)

    changed = 0
    seen: set = set()
    for row in rows_matrix:
        if not isinstance(row, list) or reco_i < 0 or reco_i >= len(row):
            continue
        reco_nt = _norm_token_rds(row[reco_i])
        act_nt = _norm_token_rds(row[action_i]) if (0 <= action_i < len(row)) else ""
        seen.add((reco_nt, act_nt))

        # Invariant 1: sell-family reco that still says INVEST/BUY -> HOLD
        if (0 <= action_i < len(row) and _reco_is_sell(reco_nt)
                and ("INVEST" in act_nt or "BUY" in act_nt or "ACCUMULATE" in act_nt)):
            row[action_i] = "HOLD"
            changed += 1
            continue

        # Invariant 2: buy-family reco with a real Block Reason -> WATCH / HOLD
        if 0 <= block_i < len(row) and _reco_is_buy(reco_nt) and str(row[block_i]).strip():
            row[reco_i] = "WATCH"
            if 0 <= action_i < len(row):
                row[action_i] = "HOLD"
            changed += 1

    # OBSERVABILITY: always log what was found (value pairs only, no symbols ->
    # safe for the public repo's Actions logs). Settles WHY a row did/didn't
    # reconcile on the next run.
    try:
        logger.info(
            "%s page=%s reco_col=%d action_col=%d block_col=%d rows=%d changed=%d distinct=%s",
            _DECISION_RECONCILE_TAG, page_label or "?", reco_i, action_i, block_i,
            len(rows_matrix or []), changed, sorted(seen)[:16],
        )
    except Exception:
        pass
    return rows_matrix, changed


# -----------------------------------------------------------------------------
# Market-page symbol read-back (v6.16.0)
# -----------------------------------------------------------------------------
# See the v6.16.0 changelog at the top of this file for the full root-cause
# write-up. In short: the four market DATA pages had no working symbol source
# (_read_symbols() returns [] because the imported ROOT symbols_reader module
# has neither get_page_symbols nor get_universe), so the backend served
# hardcoded placeholder defaults and the sync overwrote any user-added symbols
# every cycle. This reads the symbols the user actually has on the page (its
# Symbol column) and refreshes THAT list instead of sending empty.
#
# FAIL-SAFE: the read-back can only ADD the user's symbols. Any read failure, a
# missing Symbol column, or zero usable symbols returns [] and the caller keeps
# the existing page-driven (empty-symbols) flow. It never blanks a page.
# -----------------------------------------------------------------------------
_MARKET_READBACK_TAG = "[v6.16.0 SYMBOL-READBACK]"

# The page-driven DATA pages whose symbol list lives on the sheet itself.
_MARKET_READBACK_DEFAULT_PAGES = (
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
)


def _market_symbol_readback_enabled() -> bool:
    """Market-page symbol read-back master switch. Default ON; set
    TFB_MARKET_SYMBOL_READBACK=0/false/off/no to restore the prior behavior
    (market pages resolve to backend placeholder defaults and user-added symbols
    are overwritten every sync)."""
    return (os.getenv("TFB_MARKET_SYMBOL_READBACK") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _market_readback_pages() -> set:
    """Pages eligible for symbol read-back. Overridable via
    TFB_MARKET_SYMBOL_READBACK_PAGES (comma-separated); defaults to the four
    market data pages."""
    raw = (os.getenv("TFB_MARKET_SYMBOL_READBACK_PAGES") or "").strip()
    pages = [p.strip() for p in raw.split(",") if p.strip()] if raw else list(_MARKET_READBACK_DEFAULT_PAGES)
    return {_guard_norm(p) for p in pages}


def _read_existing_page_symbols(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    sheet_name: str,
    max_symbols: int,
) -> List[str]:
    """Read the user's existing symbols from a market page's Symbol column so
    the sync refreshes them instead of overwriting with placeholder defaults.

    Mirrors _read_cost_basis: reads a bounded block via the writer's own read
    service (full spreadsheets scope), locates the header row + Symbol column
    with the shared alias logic, then collects every non-blank, normalized,
    de-duplicated symbol below it (capped at max_symbols). Market pages carry no
    manual/sentinel columns, so a SYMBOL-ONLY header scan is used (not
    _guard_find_header_row, which also requires a sentinel column).

    FAIL-SAFE: returns [] on read failure (read_values -> None), a missing
    Symbol column, or zero usable symbols, so the caller falls back to the
    existing page-driven flow. Can only ADD the user's symbols; never blanks.
    """
    if sheets is None:
        return []
    grid = sheets.read_values(spreadsheet_id, sheet_name, "A1:E5000")
    if not grid or not isinstance(grid, list):
        return []
    # Locate the header row (first row with a Symbol-like column) in the top rows.
    sym_i = -1
    hdr_r = -1
    for r in range(min(len(grid), 25)):
        row = grid[r] if isinstance(grid[r], list) else []
        idx = _guard_find_col(row, _GUARD_SYMBOL_ALIASES)
        if idx >= 0:
            sym_i = idx
            hdr_r = r
            break
    if sym_i < 0:
        return []
    out: List[str] = []
    seen: set = set()
    for row in grid[hdr_r + 1:]:
        if not isinstance(row, list) or sym_i >= len(row):
            continue
        raw = row[sym_i]
        if _guard_is_blank(raw):
            continue
        t = str(raw).strip().upper()
        if not t or t in {"SYMBOL", "TICKER"}:
            continue
        if t not in seen:
            seen.add(t)
            out.append(t)
        if max_symbols > 0 and len(out) >= max_symbols:
            break
    return out


_SYMBOL_PERSISTENCE_TAG = "[v6.19.0 SYMBOL-PERSISTENCE]"
_UNIVERSE_FILTER_TAG = "[v6.19.0 UNIVERSE-FILTER]"


def _symbol_persistence_enabled() -> bool:
    """v6.19.0 (WHY 1) master switch. Default ON; set
    TFB_SYNC_SYMBOL_PERSISTENCE=0/false/off/no to restore the v6.18.2 behavior
    exactly (a requested symbol missing from the response is dropped from the
    page — and, because the sheet is the symbol source, from the universe)."""
    return (os.getenv("TFB_SYNC_SYMBOL_PERSISTENCE") or "1").strip().lower() not in {"0", "false", "off", "no"}


_STRICT_MEMBERSHIP_TAG = "[v6.19.1 STRICT-MEMBERSHIP]"


def _strict_membership_enabled() -> bool:
    """v6.19.1 master switch. Default ON; set
    TFB_SYNC_STRICT_MEMBERSHIP=0/false/off/no to restore the v6.19.0 behavior
    exactly (every backend-returned row is written verbatim, so an unrequested
    row expands the page universe on the next read-back)."""
    return (os.getenv("TFB_SYNC_STRICT_MEMBERSHIP") or "1").strip().lower() not in {"0", "false", "off", "no"}


_READBACK_EMPTY_TAG = "[v6.22.2 READBACK-EMPTY-GUARD]"
_PERSISTENCE_HARD_TAG = "[v6.22.2 PERSISTENCE-HARD-GUARD]"


def _readback_empty_guard_enabled() -> bool:
    """v6.22.2 L4a master switch. Default ON; set
    TFB_SYNC_READBACK_EMPTY_GUARD=0/false/off/no to restore the v6.22.1
    behavior exactly (an empty/failed symbol read-back on a ranked market page
    falls through to a page-driven request — the unguarded rewrite that
    amputated ML 1,278 -> 897 overnight 2026-07-08/09)."""
    return (os.getenv("TFB_SYNC_READBACK_EMPTY_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _persistence_hard_enabled() -> bool:
    """v6.22.2 L4b master switch. Default ON; set
    TFB_SYNC_PERSISTENCE_HARD=0/false/off/no to restore the v6.22.1 behavior
    exactly (a persistence pass that silently degrades — e.g. its own
    read_values failure — lets the shrunken write proceed, deleting the
    fetch-missed symbols from the page and therefore from the universe)."""
    return (os.getenv("TFB_SYNC_PERSISTENCE_HARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _unpersisted_missing(
    headers: List[Any],
    rows_matrix: List[List[Any]],
    requested_symbols: List[str],
) -> List[str]:
    """v6.22.2 L4b: the requested symbols STILL absent from the final matrix
    AFTER the persistence pass — i.e. the symbols the write would delete.

    Mirrors _persist_missing_symbol_rows' own diff exactly: Symbol column via
    the shared alias logic on the NEW headers; normalization is
    strip().upper(); deny-pattern junk is excluded (persistence deliberately
    never preserves it, so it must not count as a failure here). Order follows
    the requested list; duplicates collapse to one.

    FAIL-SAFE: returns [] when headers/requested are empty or the Symbol
    column cannot be located — the guard then never blocks a write the
    persistence layer itself could not have protected (identical scope)."""
    if not headers or not requested_symbols:
        return []
    sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
    if sym_i < 0:
        return []
    present: set = set()
    for row in rows_matrix or []:
        if isinstance(row, list) and sym_i < len(row) and not _guard_is_blank(row[sym_i]):
            present.add(str(row[sym_i]).strip().upper())
    _deny = _universe_deny_patterns()
    out: List[str] = []
    seen: set = set()
    for s in requested_symbols:
        t = str(s or "").strip().upper()
        if not t or t in present or t in seen or _universe_junk(t, _deny):
            continue
        seen.add(t)
        out.append(t)
    return out


_KEEP_LAST_GOOD_TAG = "[v6.22.3 KEEP-LAST-GOOD]"

# Data-provider column aliases (normalized via _guard_norm).
_KLG_PROVIDER_ALIASES = frozenset({
    "dataprovider", "provider", "datasource", "source",
})

# Provider markers that identify a backend ERROR STUB (post-_guard_norm form:
# 'fallback_error' -> 'fallbackerror'). A BLANK provider is NOT an error.
_KLG_ERROR_PROVIDERS = frozenset({
    "fallbackerror", "error", "unavailable", "none",
})


def _keep_last_good_enabled() -> bool:
    """v6.22.3 L4c master switch. Default ON; set
    TFB_SYNC_KEEP_LAST_GOOD=0/false/off/no to restore the v6.22.2 behavior
    exactly (a backend error-stub row for a known symbol overwrites the
    symbol's last good row — the Global_Markets fallback_error erosion
    observed 2026-07-10)."""
    return (os.getenv("TFB_SYNC_KEEP_LAST_GOOD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _klg_price_ok(v: Any) -> bool:
    """True iff the cell parses as a strictly positive number (comma/space
    tolerant). Anything blank or unparseable is NOT a usable price."""
    if _guard_is_blank(v):
        return False
    try:
        return float(str(v).replace(",", "").strip()) > 0.0
    except Exception:
        return False


def _klg_provider_is_error(v: Any) -> bool:
    """True iff the Data Provider cell normalizes into the error-marker set
    (blank normalizes to '' and is therefore NEVER an error marker)."""
    return _guard_norm(v) in _KLG_ERROR_PROVIDERS


def _keep_last_good_rows(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    sheet_name: str,
    headers: List[Any],
    rows_matrix: List[List[Any]],
) -> Tuple[List[List[Any]], List[str]]:
    """v6.22.3 L4c: substitute each DATA-FREE stub row in the fetched matrix
    with the symbol's existing last-good sheet row, re-aligned to the NEW
    header order by header NAME (exactly like _persist_missing_symbol_rows).

    STUB (conservative; both forms require NO positive price):
      (a) Data Provider in _KLG_ERROR_PROVIDERS, or
      (b) the Name cell is blank.
    GOOD old row: positive price AND provider not in the error set. A stub
    whose old row is missing or not good keeps the fresh stub — the guard
    substitutes strictly better data or nothing.

    ZERO-COST FAST PATH: the matrix is pre-scanned against the NEW headers;
    with no stub present (every healthy sync) the function returns without
    touching the network. FAIL-SAFE: returns the input matrix unchanged
    (and []) when the Symbol/price columns cannot be located, the page
    cannot be read, or nothing qualifies. Raising is reserved for the
    caller's try/except."""
    swapped: List[str] = []
    if not headers or not rows_matrix:
        return rows_matrix, swapped
    sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
    px_i = _guard_find_col(list(headers), _XPAGE_PRICE_ALIASES)
    if sym_i < 0 or px_i < 0:
        return rows_matrix, swapped
    prov_i = _guard_find_col(list(headers), _KLG_PROVIDER_ALIASES)
    name_i = _guard_find_col(list(headers), _GUARD_NAME_ALIASES)

    def _cell(row: List[Any], i: int) -> Any:
        return row[i] if (0 <= i < len(row)) else ""

    stub_rows: Dict[str, List[int]] = {}
    for r_i, row in enumerate(rows_matrix):
        if not isinstance(row, list) or sym_i >= len(row) or _guard_is_blank(row[sym_i]):
            continue
        if _klg_price_ok(_cell(row, px_i)):
            continue  # carries a fresh price -> never a stub
        is_err = prov_i >= 0 and _klg_provider_is_error(_cell(row, prov_i))
        is_bare = name_i >= 0 and _guard_is_blank(_cell(row, name_i))
        if not (is_err or is_bare):
            continue
        t = str(row[sym_i]).strip().upper()
        stub_rows.setdefault(t, []).append(r_i)
    if not stub_rows:
        return rows_matrix, swapped

    grid = sheets.read_values(spreadsheet_id, sheet_name, "A1:ZZ6000") if sheets is not None else None
    if not grid or not isinstance(grid, list):
        return rows_matrix, swapped

    old_sym_i = -1
    hdr_r = -1
    for r in range(min(len(grid), 25)):
        row = grid[r] if isinstance(grid[r], list) else []
        idx = _guard_find_col(row, _GUARD_SYMBOL_ALIASES)
        if idx >= 0:
            old_sym_i = idx
            hdr_r = r
            break
    if old_sym_i < 0:
        return rows_matrix, swapped

    def _hnorm(h: Any) -> str:
        return str(h or "").strip().casefold()

    old_headers_raw = grid[hdr_r] if isinstance(grid[hdr_r], list) else []
    old_idx: Dict[str, int] = {}
    for i, h in enumerate(old_headers_raw):
        hn = _hnorm(h)
        if hn and hn not in old_idx:
            old_idx[hn] = i
    old_px_i = _guard_find_col(list(old_headers_raw), _XPAGE_PRICE_ALIASES)
    old_prov_i = _guard_find_col(list(old_headers_raw), _KLG_PROVIDER_ALIASES)
    if old_px_i < 0:
        return rows_matrix, swapped  # cannot certify an old row as GOOD without a price

    pending = set(stub_rows.keys())
    for row in grid[hdr_r + 1:]:
        if not pending:
            break
        if not isinstance(row, list) or old_sym_i >= len(row) or _guard_is_blank(row[old_sym_i]):
            continue
        t = str(row[old_sym_i]).strip().upper()
        if t not in pending:
            continue
        pending.discard(t)  # first occurrence wins; duplicate old rows are ignored
        if not _klg_price_ok(row[old_px_i] if old_px_i < len(row) else ""):
            continue  # old row not good -> keep the fresh stub
        if 0 <= old_prov_i < len(row) and _klg_provider_is_error(row[old_prov_i]):
            continue
        aligned: List[Any] = []
        for h in headers:
            j = old_idx.get(_hnorm(h), -1)
            aligned.append(row[j] if 0 <= j < len(row) else "")
        for r_i in stub_rows[t]:
            rows_matrix[r_i] = list(aligned)
        swapped.append(t)
    return rows_matrix, swapped


def _filter_rows_to_requested(
    headers: List[Any],
    rows_matrix: List[List[Any]],
    requested_symbols: List[str],
) -> Tuple[List[List[Any]], List[str]]:
    """v6.19.1: drop response rows whose Symbol is NOT in the requested set.

    The backend can answer a requested-symbol fetch with EXTRA rows (its own
    universe on top of the request). Writing them makes each foreign symbol a
    requested symbol on the next run (the sheet is the symbol source) — the
    749 -> 3,068 Global_Markets ratchet of 2026-07-02/03. This keeps only rows
    carrying a requested symbol; the dropped (unique, normalized) symbols are
    returned for the caller's [STRICT-MEMBERSHIP] warning.

    FAIL-SAFE: returns the matrix unchanged (and []) when headers, rows, or the
    requested set are empty, or when the Symbol column cannot be located in the
    NEW headers (shared alias logic). Rows with a BLANK symbol cell are KEPT
    unchanged — this filter can drop only a row that positively identifies
    itself as an unrequested symbol, never a structural row."""
    if not headers or not rows_matrix or not requested_symbols:
        return rows_matrix, []
    sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
    if sym_i < 0:
        return rows_matrix, []
    wanted: set = set()
    for s in requested_symbols:
        t = str(s or "").strip().upper()
        if t:
            wanted.add(t)
    if not wanted:
        return rows_matrix, []
    kept_rows: List[List[Any]] = []
    dropped: List[str] = []
    dropped_seen: set = set()
    for row in rows_matrix:
        if not isinstance(row, list) or sym_i >= len(row) or _guard_is_blank(row[sym_i]):
            kept_rows.append(row)
            continue
        t = str(row[sym_i]).strip().upper()
        if t in wanted:
            kept_rows.append(row)
        else:
            if t not in dropped_seen:
                dropped_seen.add(t)
                dropped.append(t)
    return kept_rows, dropped


# -----------------------------------------------------------------------------
# v6.22.0 [IDENTITY] — Symbol<->Name transposition firewall, writer side
# -----------------------------------------------------------------------------
# See the v6.22.0 header changelog for the live root cause (2026-07-08:
# enriched-gateway rows carried the requested SYMBOLS with FOREIGN attribute
# payloads; membership filtering is symbol-cell-only and cannot see it).

_IDENTITY_TAG = "[v6.22.0 IDENTITY-TRIPWIRE]"
_BATCH_IDENTITY_TAG = "[v6.22.0 BATCH-IDENTITY]"
_SAFE_GW_TAG = "[v6.22.0 SAFE-GATEWAYS]"

# Built-in anchor pairs: symbol -> accepted casefolded substrings of the TRUE
# company name. Curated for stability (official renames included, e.g. SABB ->
# Saudi Awwal Bank). A pair only participates when the symbol is PRESENT in
# the fetched matrix; a blank Name cell never counts as a mismatch.
_IDENTITY_ANCHORS: Dict[str, Tuple[str, ...]] = {
    # Saudi (Tadawul)
    "1010.SR": ("riyad",),
    "1050.SR": ("fransi",),
    "1060.SR": ("awwal", "sabb"),
    "1080.SR": ("arab national",),
    "1120.SR": ("rajhi",),
    "1150.SR": ("alinma",),
    "1180.SR": ("saudi national bank", "snb"),
    "1211.SR": ("maaden", "saudi arabian mining"),
    "2010.SR": ("sabic", "saudi basic"),
    "2222.SR": ("aramco", "saudi arabian oil"),
    "2280.SR": ("almarai",),
    "4030.SR": ("bahri", "national shipping"),
    "7010.SR": ("stc", "saudi telecom"),
    "7020.SR": ("etihad etisalat", "mobily"),
    # US (plain + .US convention)
    "AAPL": ("apple",), "AAPL.US": ("apple",),
    "MSFT": ("microsoft",), "MSFT.US": ("microsoft",),
    "NVDA": ("nvidia",), "NVDA.US": ("nvidia",),
    "GOOGL": ("alphabet", "google"), "GOOGL.US": ("alphabet", "google"),
    "AMZN": ("amazon",), "AMZN.US": ("amazon",),
    "META": ("meta",), "META.US": ("meta",),
    "JPM": ("jpmorgan", "jp morgan"), "JPM.US": ("jpmorgan", "jp morgan"),
    "XOM": ("exxon",), "XOM.US": ("exxon",),
    # International
    "005930.KS": ("samsung",),
    "7203.T": ("toyota",),
    "2914.T": ("japan tobacco",),
    "0700.HK": ("tencent",),
    "0939.HK": ("china construction",),
    "NESN.SW": ("nestl",),
    "ASML": ("asml",), "ASML.US": ("asml",),
}


def _safe_gateways_enabled() -> bool:
    """v6.22.0 L1 master switch. Default ON; set TFB_SYNC_SAFE_GATEWAYS=
    0/false/off/no to restore the v6.21.0 gateway resolution + candidate
    chains byte-identically (market pages may then serve from the
    unfirewalled enriched/ai routes again — not recommended)."""
    return (os.getenv("TFB_SYNC_SAFE_GATEWAYS") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _batch_identity_enabled() -> bool:
    """v6.22.0 L2 master switch. Default ON; set TFB_SYNC_BATCH_IDENTITY=
    0/false/off/no to restore the v6.21.0 positional batch accumulation
    byte-identically."""
    return (os.getenv("TFB_SYNC_BATCH_IDENTITY") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _identity_tripwire_enabled() -> bool:
    """v6.22.0 L3 master switch. Default ON; set TFB_SYNC_IDENTITY_TRIPWIRE=
    0/false/off/no to disable the pre-write anchor verification (not
    recommended — this is the layer that blocks a transposed payload)."""
    return (os.getenv("TFB_SYNC_IDENTITY_TRIPWIRE") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _identity_min_fails() -> int:
    """v6.22.0: anchor mismatches required to trip (default 2, floor 1,
    cap 50). One odd corporate rename can never block a page; a transposed
    payload fails many anchors at once (tonight's ML: >=5)."""
    return _safe_int(os.getenv("TFB_SYNC_IDENTITY_MIN_FAILS"), 2, lo=1, hi=50)


def _identity_extra_anchors() -> Dict[str, Tuple[str, ...]]:
    """v6.22.0: operator-extendable pairs, csv of SYM=sub|sub entries in
    TFB_SYNC_IDENTITY_ANCHORS_EXTRA (e.g. "2082.SR=acwa,4200.SR=aldrees").
    Malformed entries are skipped with a warning instead of failing the run."""
    raw = (os.getenv("TFB_SYNC_IDENTITY_ANCHORS_EXTRA") or "").strip()
    out: Dict[str, Tuple[str, ...]] = {}
    if not raw:
        return out
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            logger.warning(f"{_IDENTITY_TAG} bad extra anchor {part!r} skipped (no '=')")
            continue
        sym, _, subs = part.partition("=")
        sym = sym.strip().upper()
        toks = tuple(t.strip().casefold() for t in subs.split("|") if t.strip())
        if sym and toks:
            out[sym] = toks
        else:
            logger.warning(f"{_IDENTITY_TAG} bad extra anchor {part!r} skipped")
    return out


def _identity_anchor_map() -> Dict[str, Tuple[str, ...]]:
    """Built-in anchors overlaid with the operator's extra pairs."""
    m = dict(_IDENTITY_ANCHORS)
    m.update(_identity_extra_anchors())
    return m


def _identity_anchor_scan(
    headers: List[Any],
    rows_matrix: List[List[Any]],
) -> Tuple[int, int, List[Tuple[str, str]]]:
    """v6.22.0: verify anchor Symbol->Name pairs PRESENT in the matrix.

    Returns (checked, ok, mismatches) where mismatches is a list of
    (symbol, seen_name). Rules: first occurrence of a symbol wins; a blank
    or missing Name cell is neither ok nor a mismatch (blank != crossed);
    matching is casefolded substring against the anchor's accepted tokens.
    FAIL-SAFE: (0, 0, []) when the Symbol or Name column cannot be located —
    a page without both columns is never blocked by this layer."""
    if not headers or not rows_matrix:
        return 0, 0, []
    sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
    name_i = _guard_find_col(list(headers), _GUARD_NAME_ALIASES)
    if sym_i < 0 or name_i < 0:
        return 0, 0, []
    anchors = _identity_anchor_map()
    hi = max(sym_i, name_i)
    seen: set = set()
    checked = ok = 0
    bad: List[Tuple[str, str]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)) or len(row) <= hi:
            continue
        s = str(row[sym_i] or "").strip().upper()
        if not s or s in seen:
            continue
        toks = anchors.get(s)
        if not toks:
            continue
        seen.add(s)
        if _guard_is_blank(row[name_i]):
            continue  # blank name: cannot confirm, must not condemn
        nm = str(row[name_i]).strip()
        checked += 1
        low = nm.casefold()
        if any(t in low for t in toks):
            ok += 1
        else:
            bad.append((s, nm[:60]))
    return checked, ok, bad


def _universe_deny_patterns() -> List["re.Pattern[str]"]:
    """v6.19.0 (WHY 2): compiled deny-patterns for the read-back universe.
    TFB_SYNC_UNIVERSE_DENY is a comma-separated regex list matched (re.match,
    case-insensitive) against the NORMALIZED symbol. Unset -> the default
    "^TICK\\d+" placeholder family; off/0/-/no/false -> filter disabled.
    A malformed pattern is skipped with a warning instead of failing the run."""
    raw = (os.getenv("TFB_SYNC_UNIVERSE_DENY") or "^TICK\\d+").strip()
    if raw.lower() in {"", "0", "off", "no", "false", "-"}:
        return []
    pats: List["re.Pattern[str]"] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            pats.append(re.compile(part, re.IGNORECASE))
        except re.error as e:
            logger.warning(f"{_UNIVERSE_FILTER_TAG} bad deny pattern {part!r} skipped: {e}")
    return pats


def _universe_junk(symbol: str, pats: Optional[List["re.Pattern[str]"]] = None) -> bool:
    """v6.19.0 (WHY 2): True when the symbol matches a deny pattern. Junk is
    neither requested nor persisted, so it cannot self-perpetuate through the
    sheet-is-the-universe loop again."""
    t = str(symbol or "").strip().upper()
    if not t:
        return False
    for pat in (pats if pats is not None else _universe_deny_patterns()):
        if pat.match(t):
            return True
    return False


def _persist_missing_symbol_rows(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    sheet_name: str,
    headers: List[Any],
    rows_matrix: List[List[Any]],
    requested_symbols: List[str],
) -> Tuple[List[List[Any]], List[str]]:
    """v6.19.0 (WHY 1): append the existing LAST-GOOD row of every requested
    symbol that is absent from the fetched rows, so writing the response cannot
    delete an operator symbol from the page (and therefore from the universe).

    Mechanics: locate the Symbol column in the NEW headers (shared alias
    logic); diff requested vs returned (normalized, junk excluded); read the
    live page once via the writer's read service; re-align each preserved row
    to the NEW header order by header NAME (a header missing from the old grid
    yields ""), so a schema evolution cannot shift cells. Preserved rows are
    appended below the fetched block; the next healthy fetch replaces them
    in-place with fresh data.

    FAIL-SAFE: returns the input matrix unchanged (and []) when the Symbol
    column cannot be located, the page cannot be read, or nothing is missing.
    Raising is reserved for the caller's try/except — any unexpected error
    leaves the v6.18.2 write path untouched."""
    kept: List[str] = []
    if not headers or rows_matrix is None or not requested_symbols:
        return rows_matrix, kept
    new_sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
    if new_sym_i < 0:
        return rows_matrix, kept

    returned: set = set()
    for row in rows_matrix:
        if isinstance(row, list) and new_sym_i < len(row) and not _guard_is_blank(row[new_sym_i]):
            returned.add(str(row[new_sym_i]).strip().upper())

    _deny = _universe_deny_patterns()
    missing: List[str] = []
    seen_missing: set = set()
    for s in requested_symbols:
        t = str(s or "").strip().upper()
        if not t or t in returned or t in seen_missing or _universe_junk(t, _deny):
            continue
        seen_missing.add(t)
        missing.append(t)
    if not missing:
        return rows_matrix, kept

    grid = sheets.read_values(spreadsheet_id, sheet_name, "A1:ZZ6000") if sheets is not None else None
    if not grid or not isinstance(grid, list):
        return rows_matrix, kept

    # Locate the existing header row + Symbol column (same scan as the read-back).
    old_sym_i = -1
    hdr_r = -1
    for r in range(min(len(grid), 25)):
        row = grid[r] if isinstance(grid[r], list) else []
        idx = _guard_find_col(row, _GUARD_SYMBOL_ALIASES)
        if idx >= 0:
            old_sym_i = idx
            hdr_r = r
            break
    if old_sym_i < 0:
        return rows_matrix, kept

    def _hnorm(h: Any) -> str:
        return str(h or "").strip().casefold()

    old_headers = [(_hnorm(h)) for h in (grid[hdr_r] if isinstance(grid[hdr_r], list) else [])]
    old_idx: Dict[str, int] = {}
    for i, h in enumerate(old_headers):
        if h and h not in old_idx:
            old_idx[h] = i

    missing_set = set(missing)
    for row in grid[hdr_r + 1:]:
        if not missing_set:
            break
        if not isinstance(row, list) or old_sym_i >= len(row) or _guard_is_blank(row[old_sym_i]):
            continue
        t = str(row[old_sym_i]).strip().upper()
        if t not in missing_set:
            continue
        aligned: List[Any] = []
        for h in headers:
            j = old_idx.get(_hnorm(h), -1)
            aligned.append(row[j] if 0 <= j < len(row) else "")
        rows_matrix.append(aligned)
        kept.append(t)
        missing_set.discard(t)

    return rows_matrix, kept


def _market_symbol_cap() -> int:
    """v6.19.2: per-page symbol cap for the four MARKET pages (Market_Leaders,
    Global_Markets, Commodities_FX, Mutual_Funds). Default 2500 — sized to the
    Symbol Expansion Pack / build_universes documented ceiling with headroom —
    override with TFB_SYNC_MAX_SYMBOLS_MARKET (clamped 1..5000). A cap SMALLER
    than the sheet universe silently un-requests the overflow, and the
    persistence guard can only protect REQUESTED symbols, so an undersized cap
    acts as a symbol remover (the 2026-07-03 Global_Markets pin at exactly 800
    rows). Fail-safe: any unparsable value falls back to 2500."""
    raw = (os.getenv("TFB_SYNC_MAX_SYMBOLS_MARKET") or "").strip()
    try:
        v = int(raw) if raw else 2500
    except Exception:
        v = 2500
    return max(1, min(v, 5000))


def _read_symbols(task_key: str, spreadsheet_id: str, max_symbols: int) -> List[str]:
    try:
        import importlib

        sym_mod = importlib.import_module("symbols_reader")
        fn = getattr(sym_mod, "get_page_symbols", None)
        if callable(fn):
            data = fn(task_key, spreadsheet_id=spreadsheet_id)
        else:
            fn2 = getattr(sym_mod, "get_universe", None)
            data = fn2([task_key], spreadsheet_id=spreadsheet_id) if callable(fn2) else {}
    except Exception as e:
        logger.warning("symbols_reader unavailable or failed: %s", e)
        return []

    symbols: List[str] = []
    if isinstance(data, dict):
        v = data.get("all") or data.get("symbols") or []
        symbols = v if isinstance(v, list) else []
    elif isinstance(data, list):
        symbols = data

    out: List[str] = []
    seen: set[str] = set()
    for s in symbols:
        t = str(s or "").strip().upper()
        if not t or t in {"SYMBOL", "TICKER"}:
            continue
        if t not in seen:
            seen.add(t)
            out.append(t)
        if max_symbols > 0 and len(out) >= max_symbols:
            break
    return out


# -----------------------------------------------------------------------------
# Task definitions (aligned with your dashboard tabs + canonical schema)
# -----------------------------------------------------------------------------
def _default_tasks() -> List[TaskSpec]:
    return [
        TaskSpec(key="MY_PORTFOLIO", sheet_name="My_Portfolio", gateway="enriched", priority=1, max_symbols=800, allow_empty_symbols=True, expects_rows=True),
        TaskSpec(key="MARKET_LEADERS", sheet_name="Market_Leaders", gateway="enriched", priority=2, max_symbols=_market_symbol_cap(), allow_empty_symbols=True, expects_rows=True),
        TaskSpec(key="GLOBAL_MARKETS", sheet_name="Global_Markets", gateway="enriched", priority=3, max_symbols=_market_symbol_cap(), allow_empty_symbols=True, expects_rows=True),
        TaskSpec(key="COMMODITIES_FX", sheet_name="Commodities_FX", gateway="enriched", priority=4, max_symbols=_market_symbol_cap(), allow_empty_symbols=True, expects_rows=True),
        TaskSpec(key="MUTUAL_FUNDS", sheet_name="Mutual_Funds", gateway="enriched", priority=5, max_symbols=_market_symbol_cap(), allow_empty_symbols=True, expects_rows=True),
        # Special/meta pages — do NOT require symbols
        TaskSpec(key="INSIGHTS_ANALYSIS", sheet_name="Insights_Analysis", gateway="analysis", priority=6, max_symbols=0, allow_empty_symbols=True),
        TaskSpec(key="TOP_10_INVESTMENTS", sheet_name="Top_10_Investments", gateway="analysis", priority=7, max_symbols=0, allow_empty_symbols=True),
        TaskSpec(key="DATA_DICTIONARY", sheet_name="Data_Dictionary", gateway="analysis", priority=8, max_symbols=0, allow_empty_symbols=True),
    ]


def _endpoint_candidates_for_gateway(gw: str) -> List[str]:
    gw = (gw or "enriched").strip().lower()
    # v6.22.0 L1 [SAFE-GATEWAYS]: the market chains drop their unfirewalled
    # tails (/v1/ai/*, /v1/enriched/*). An analysis outage then yields an
    # empty fetch -> the existing empty/shrink guards PRESERVE last-good rows,
    # instead of accepting rows from a route without the transposition
    # firewall. Conscious availability trade; TFB_SYNC_SAFE_GATEWAYS=0
    # restores the v6.21.0 chains byte-identically. The argaam and
    # enriched/default chains below are not market chains and are untouched
    # (in safe mode the four market pages never resolve to them).
    if _safe_gateways_enabled():
        # v6.22.1: analysis endpoints ONLY. /v1/advanced/* is served live by
        # routes.investment_advisor (v2.17.0) which carries no transposition
        # firewall — confirmed serving 200s in the 2026-07-09 01:48 Riyadh
        # Render log — so it cannot sit in the verified market chain.
        if gw in {"analysis", "ai", "advanced"}:
            return [
                "/v1/analysis/sheet-rows",
                "/analysis/sheet-rows",
            ]
    # include ai aliases because route naming can vary
    if gw in {"analysis", "ai"}:
        return [
            "/v1/analysis/sheet-rows",
            "/analysis/sheet-rows",
            "/v1/ai/sheet-rows",
            "/ai/sheet-rows",
            "/v1/advanced/sheet-rows",
            "/advanced/sheet-rows",
            "/v1/enriched/sheet-rows",
            "/enriched/sheet-rows",
        ]
    if gw == "advanced":
        return [
            "/v1/advanced/sheet-rows",
            "/advanced/sheet-rows",
            "/v1/analysis/sheet-rows",
            "/analysis/sheet-rows",
            "/v1/enriched/sheet-rows",
            "/enriched/sheet-rows",
        ]
    if gw == "argaam":
        return ["/v1/argaam/sheet-rows", "/argaam/sheet-rows"]
    return [
        "/v1/enriched/sheet-rows",
        "/enriched/sheet-rows",
        "/v1/analysis/sheet-rows",
        "/analysis/sheet-rows",
        "/v1/advanced/sheet-rows",
        "/advanced/sheet-rows",
        "/v1/ai/sheet-rows",
        "/ai/sheet-rows",
    ]


# v6.10.0 [GLOBAL-RANK/DEDUP ROUTING]: the four cross-sectional market pages whose
# Rank (Overall) must be ranked across the WHOLE page (and whose duplicate-symbol
# rows must be collapsed). Those corrections live ONLY in the analysis router
# (routes/analysis_sheet_rows.py: _apply_global_rank_overall v4.4.0 + the v4.5.0
# global dedup, both default ON), which is the single funnel where the complete
# page exists before pagination. Scope mirrors that router's ranked-market-page
# scope exactly; My_Portfolio (holding order / multi-lot) and the meta pages are
# intentionally excluded.
_RANKED_MARKET_PAGES = frozenset({
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
})


def _market_analysis_gateway_enabled() -> bool:
    """v6.10.0: route the four cross-sectional market pages through the ANALYSIS
    gateway (/v1/analysis/sheet-rows) instead of ENRICHED, so the analysis
    router's page-level Global Rank (v4.4.0) and Global Dedup (v4.5.0) passes
    actually run on the sheet the daily sync writes. DEFAULT OFF -> every task's
    gateway is its configured value and the routing is byte-identical to v6.9.0.
    Set TFB_SYNC_MARKET_ANALYSIS_GATEWAY to 1/true/on/yes to enable."""
    raw = (os.getenv("TFB_SYNC_MARKET_ANALYSIS_GATEWAY", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on", "enabled", "enable"}


def _market_gateway_override() -> str:
    """v6.18.0 (Fix 1): generic gateway override for the four ranked market
    pages. TFB_SYNC_MARKET_GATEWAY set to one of analysis/advanced/enriched/
    argaam selects that candidate chain for Market_Leaders / Global_Markets /
    Commodities_FX / Mutual_Funds. Unset/blank/unknown -> "" (no override; the
    v6.10.0 TFB_SYNC_MARKET_ANALYSIS_GATEWAY boolean then applies, and with
    that off too the TaskSpec default routes — byte-identical to v6.17.0)."""
    raw = (os.getenv("TFB_SYNC_MARKET_GATEWAY", "") or "").strip().lower()
    return raw if raw in {"analysis", "ai", "advanced", "enriched", "argaam"} else ""


_TRANSIENT_WRITE_MARKERS = (
    "eof occurred",            # ssl.SSLEOFError text
    "ssl",                     # generic ssl-layer failures
    "connection reset",
    "connection aborted",
    "broken pipe",
    "timed out",
    "timeout",
    "429",
    "500",
    "502",
    "503",
    "backend error",
    "internal error",
    "the service is currently unavailable",
)


def _is_transient_write_error(err: Exception) -> bool:
    """v6.18.1 (WHY 1): True when a Sheets write failure looks like a transient
    transport/quota condition worth retrying (SSL EOF, reset, timeout,
    429/5xx). Conservative substring match on the error text; anything not
    matching raises immediately as before."""
    s = str(err or "").lower()
    return any(m in s for m in _TRANSIENT_WRITE_MARKERS)


def _write_retry_attempts() -> int:
    """v6.18.1 (WHY 1): total values.update attempts (default 3; min 1, max 5).
    TFB_SYNC_WRITE_RETRIES=1 restores the v6.18.0 single-attempt behavior."""
    try:
        n = int((os.getenv("TFB_SYNC_WRITE_RETRIES") or "3").strip())
    except Exception:
        n = 3
    return max(1, min(5, n))


def _write_then_trim_enabled() -> bool:
    """v6.18.0 (Fix 2): cancellation-safe write ordering master switch. Default
    ON: write_table() runs FIRST (one atomic values.update over the old block),
    then the stale tail below/right of the new rectangle is trimmed. A job
    cancellation can then never leave a cleared-but-unwritten (EMPTY) page —
    the 2026-07-02 Mutual_Funds / Commodities_FX wipe. Set
    TFB_SYNC_WRITE_THEN_TRIM=0/false/off/no to restore the exact v6.17.0
    clear-then-write order."""
    return (os.getenv("TFB_SYNC_WRITE_THEN_TRIM") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _a1_col_to_idx(col: str) -> int:
    """v6.18.0: A -> 1, B -> 2, ..., Z -> 26, AA -> 27. Empty/invalid -> 1."""
    n = 0
    for ch in (col or "").strip().upper():
        if not ("A" <= ch <= "Z"):
            return 1
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n if n > 0 else 1


def _idx_to_a1_col(idx: int) -> str:
    """v6.18.0: 1 -> A, 26 -> Z, 27 -> AA. idx < 1 -> A."""
    idx = int(idx) if idx and idx > 0 else 1
    out = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        out = chr(ord("A") + rem) + out
    return out


def _trim_after_write(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    n_header: int,
    n_rows: int,
    n_cols: int,
) -> List[str]:
    """v6.18.0 (Fix 2): after write_table() has overwritten the block in place,
    clear ONLY the leftovers from a previously-larger table: (a) the tail BELOW
    the new block, full width from the start column; (b) the tail RIGHT of the
    new header width, full depth from the start row. Both are best-effort —
    a failure is reported as a warning string, never raised, because the NEW
    data is already on the sheet and a stale tail self-heals on the next run."""
    warnings: List[str] = []
    m = re.match(r"^\$?([A-Za-z]+)\$?(\d+)$", (start_cell or "").strip())
    if not m:
        return warnings
    col0 = _a1_col_to_idx(m.group(1))
    row0 = int(m.group(2))
    below_row = row0 + max(0, int(n_header)) + max(0, int(n_rows))
    # v6.18.1 (WHY 2): a trim that starts BEYOND the sheet's grid is a no-op by
    # definition (nothing can be stale outside the grid), but the Sheets API
    # answers 400 "exceeds grid limits" instead of succeeding quietly. Treat
    # that specific answer as silent success; every other failure still warns.
    try:
        sheets.clear_from(spreadsheet_id, sheet_name, f"{_idx_to_a1_col(col0)}{below_row}")
    except Exception as e:
        if "exceeds grid limits" not in str(e).lower():
            warnings.append(f"Trim-below failed (stale tail rows may remain; self-heals next run): {e}")
    if n_cols and n_cols > 0:
        try:
            sheets.clear_from(spreadsheet_id, sheet_name, f"{_idx_to_a1_col(col0 + int(n_cols))}{row0}")
        except Exception as e:
            if "exceeds grid limits" not in str(e).lower():
                warnings.append(f"Trim-right failed (stale tail columns may remain; self-heals next run): {e}")
    return warnings


def _effective_gateway(task: TaskSpec) -> str:
    """v6.10.0: the gateway actually used for a task. When the market-analysis
    routing toggle is ON, the four cross-sectional market pages resolve to
    "analysis" (the router that carries the global rank + dedup passes); every
    other page, and the OFF state, returns the task's configured gateway
    unchanged. The "analysis" candidate chain ends at the enriched endpoints, so
    an analysis-route outage falls back to the prior path (the page loses the
    rank/dedup for that cycle -- never a failed write)."""
    # v6.22.0 L1 [SAFE-GATEWAYS]: the four ranked market pages resolve to the
    # ANALYSIS gateway (the only market router carrying the transposition
    # firewall) REGARDLESS of the v6.10.0 boolean and the v6.18.0 override —
    # the 2026-07-08 poisoning entered exactly through the enriched default.
    # TFB_SYNC_SAFE_GATEWAYS=0 restores the v6.21.0 precedence byte-identically.
    if _safe_gateways_enabled() and task.sheet_name in _RANKED_MARKET_PAGES:
        return "analysis"
    # v6.18.0 (Fix 1): generic override wins when set; the v6.10.0 boolean and
    # the TaskSpec default apply unchanged when it is unset/blank.
    _ovr = _market_gateway_override()
    if _ovr and task.sheet_name in _RANKED_MARKET_PAGES:
        return _ovr
    if _market_analysis_gateway_enabled() and task.sheet_name in _RANKED_MARKET_PAGES:
        return "analysis"
    return task.gateway


# ---------------------------------------------------------------------------
# v6.17.0 [SYMBOL-BATCHING] — fetch a many-symbol market page in small batches.
# ---------------------------------------------------------------------------
# See the v6.17.0 header changelog for the full root cause. In short: one
# request carrying a page's ENTIRE symbol set makes the backend burst hundreds
# of Yahoo calls -> 429 (200 with 0 rows -> stale page) or Render ~100s timeout
# (502). Splitting into small sequential batches makes each request light enough
# to finish and spreads the upstream calls so they are far less likely to 429.
# DEFAULT OFF: TFB_SYNC_SYMBOL_BATCH_SIZE unset/0 -> the original single-request
# path runs unchanged. Scope is the four _RANKED_MARKET_PAGES only, and only
# when a page has MORE symbols than the batch size.


def _symbol_batch_size() -> int:
    """v6.17.0: per-request symbol batch size for market pages. <=0 disables
    batching (original single-request path). A positive N fetches the page in
    batches of N. Non-numeric / unset -> 0 (OFF)."""
    raw = (os.getenv("TFB_SYNC_SYMBOL_BATCH_SIZE", "") or "").strip()
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _batch_delay_ms() -> int:
    """v6.17.0: optional sleep (milliseconds) between market-page symbol batches
    for extra upstream cooldown. Default 0 (no delay). Negative / non-numeric
    -> 0."""
    raw = (os.getenv("TFB_SYNC_BATCH_DELAY_MS", "") or "").strip()
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _should_batch_market_page(task: TaskSpec, symbols: List[str]) -> bool:
    """v6.17.0: batch this task iff batching is enabled, it is one of the
    cross-sectional market pages, and it actually carries MORE symbols than the
    batch size (otherwise one request already fits and the original path runs)."""
    size = _symbol_batch_size()
    if size <= 0:
        return False
    if task.sheet_name not in _RANKED_MARKET_PAGES:
        return False
    return bool(symbols) and len(symbols) > size


async def _fetch_market_rows_batched(
    backend: "BackendClient",
    task: TaskSpec,
    symbols: List[str],
    base_payload: Dict[str, Any],
    eff_gw: str,
    res: "TaskResult",
) -> Tuple[List[Any], List[List[Any]], Optional[str], Optional[str]]:
    """v6.17.0: fetch `symbols` for a market page in small SEQUENTIAL batches,
    accumulating the data rows, and return (headers, rows_matrix, used_endpoint,
    last_err) with the SAME shape the inline single-request loop produces — so
    the caller's guards + clear/write run unchanged on the combined result.

    The endpoint is resolved on the FIRST answering batch and reused for the
    rest (the 404-candidate cycling is not repeated per batch). Header + rectify
    handling mirrors the inline loop; the pages this runs for are never
    My_Portfolio / Top_10, so the portfolio-injection, decision-reconcile and
    Top_10 header-repair steps are (by scope) no-ops and are intentionally not
    duplicated here.
    """
    size = _symbol_batch_size()
    delay_ms = _batch_delay_ms()
    batches = [symbols[i:i + size] for i in range(0, len(symbols), size)]
    candidates = _endpoint_candidates_for_gateway(eff_gw)

    headers: List[Any] = []
    combined: List[List[Any]] = []
    used_endpoint: Optional[str] = None
    last_err: Optional[str] = None
    ok_batches = 0

    # v6.22.0 L2 [BATCH-IDENTITY]: accumulate BY SYMBOL instead of by position.
    _idb_on = _batch_identity_enabled()
    _idb_sym_i = -1          # resolved from the first answering batch's headers
    _idb_by_sym: Dict[str, List[Any]] = {}
    _idb_bleed = 0           # rows whose symbol is not in THAT batch's request
    _idb_dupes = 0           # repeated symbol rows (first occurrence wins)
    _idb_blank = 0           # rows with a blank symbol cell (unaddressable)

    for bi, batch in enumerate(batches):
        p = dict(base_payload)
        p["tickers"] = batch
        p["symbols"] = batch
        p["limit"] = min(5000, max(1, len(batch)))
        p["request_id"] = f"{res.request_id}-b{bi + 1}"

        # Reuse the resolved endpoint once one answers; only the first batch
        # pays the candidate-cycling cost.
        cand = [used_endpoint] if used_endpoint else candidates
        b_headers: List[Any] = []
        b_matrix: List[List[Any]] = []
        for ep in cand:
            data, err, _code = await backend.post_json(ep, p)
            if err:
                last_err = f"{ep} -> {err}"
                continue
            if not isinstance(data, dict):
                last_err = f"{ep} -> Non-dict response"
                continue
            b_headers, b_matrix = _extract_table_payload(data)
            if not b_headers:
                last_err = f"{ep} -> Missing headers"
                continue
            b_matrix = _rectify_matrix(b_headers, b_matrix)
            used_endpoint = ep
            break

        if b_headers:
            if not headers:
                headers = b_headers
                if _idb_on:
                    _idb_sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
            if b_matrix:
                if _idb_on and _idb_sym_i >= 0:
                    _batch_set = {str(t or "").strip().upper() for t in batch}
                    _batch_set.discard("")
                    for _row in b_matrix:
                        if (not isinstance(_row, (list, tuple))
                                or _idb_sym_i >= len(_row)
                                or _guard_is_blank(_row[_idb_sym_i])):
                            _idb_blank += 1
                            continue
                        _t = str(_row[_idb_sym_i]).strip().upper()
                        if _t not in _batch_set:
                            _idb_bleed += 1
                            continue
                        if _t in _idb_by_sym:
                            _idb_dupes += 1
                            continue
                        _idb_by_sym[_t] = list(_row)
                else:
                    # v6.21.0 path: Symbol column missing (or L2 off) -> legacy
                    # positional accumulation, byte-identical.
                    combined.extend(b_matrix)
            ok_batches += 1

        if delay_ms > 0 and bi < len(batches) - 1:
            await asyncio.sleep(delay_ms / 1000.0)

    # v6.22.0 L2: emit in the REQUESTED symbol order (no positional artifact
    # can survive), falling through to the legacy `combined` when the Symbol
    # column never resolved or the layer is off.
    if _idb_on and _idb_sym_i >= 0:
        combined = [
            _idb_by_sym[t]
            for t in (str(s or "").strip().upper() for s in symbols)
            if t in _idb_by_sym
        ]
        if _idb_bleed or _idb_dupes or _idb_blank:
            _iw = (
                f"{_BATCH_IDENTITY_TAG} {task.sheet_name}: dropped "
                f"{_idb_bleed} cross-batch row(s), {_idb_dupes} duplicate-symbol "
                f"row(s), {_idb_blank} blank-symbol row(s); kept "
                f"{len(combined)} by-symbol row(s) in requested order."
            )
            res.warnings.append(_iw)
            logger.warning(_iw)

    if headers:
        res.warnings.append(
            f"[SYMBOL-BATCH] fetched {len(symbols)} symbol(s) in "
            f"{ok_batches}/{len(batches)} batch(es) of {size} via "
            f"{used_endpoint or '?'}"
        )
    return headers, combined, used_endpoint, last_err


def _extract_table_payload(resp: Dict[str, Any]) -> Tuple[List[Any], List[List[Any]]]:
    """
    Returns (headers, rows_matrix) ALWAYS as list[list] for Sheets writing.

    Supports:
      - {"headers":[...], "rows":[list|dict]}
      - {"headers":[...], "rows_matrix":[...]}
      - {"keys":[...]} for dict->matrix conversion
      - {"data": {...}} nested
    """
    if not isinstance(resp, dict):
        return [], []

    if isinstance(resp.get("data"), dict):
        return _extract_table_payload(resp["data"])  # type: ignore[index]

    headers = resp.get("headers")
    keys = resp.get("keys")
    rows = resp.get("rows")
    rows_matrix = resp.get("rows_matrix")

    headers_list = list(headers) if isinstance(headers, list) else []
    keys_list = list(keys) if isinstance(keys, list) else []

    # Prefer explicit matrix
    if isinstance(headers_list, list) and isinstance(rows_matrix, list):
        mm = [list(r) for r in rows_matrix if isinstance(r, list)]
        return headers_list, mm

    if not isinstance(rows, list):
        rows = []

    # rows are list[list]
    if rows and isinstance(rows[0], list):
        if not headers_list and keys_list:
            headers_list = keys_list[:]
        return headers_list, [list(r) for r in rows if isinstance(r, list)]

    # rows are list[dict] -> convert to matrix using keys/headers
    if rows and isinstance(rows[0], dict):
        dict_rows: List[Dict[str, Any]] = [r for r in rows if isinstance(r, dict)]  # type: ignore[assignment]
        if not keys_list:
            if headers_list:
                keys_list = [str(h) for h in headers_list]
            else:
                keys_list = [str(k) for k in dict_rows[0].keys()]
                headers_list = keys_list[:]
        if not headers_list:
            headers_list = keys_list[:]
        matrix = [[_coerce_jsonable(r.get(k)) for k in keys_list] for r in dict_rows]
        return headers_list, matrix

    # empty rows, but headers exist
    if headers_list:
        return headers_list, []

    return [], []


def _cell_to_scalar(v: Any) -> Any:
    """Flatten a single value to a Google-Sheets-writable SCALAR.

    The Sheets values API (valueInputOption=RAW) rejects any cell whose value is
    a list or dict ("Invalid values[r][c]: list_value ..."). The backend emits a
    few structured columns for instrument rows (e.g. "Scoring Errors", a list),
    and the matrix path returned them verbatim — so once a page sent more than
    the single truncated row, the whole write 400-ed on the first structured
    cell. This flattens any non-scalar to a readable string; scalars, None,
    Enums, and datetimes are treated as _coerce_jsonable treats them.
      - empty list/tuple/set/dict -> "" (clean empty cell, e.g. no errors)
      - list of scalars           -> "a, b, c"
      - nested list / dict        -> compact JSON (never crashes the write)
    """
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, Enum):
        return _cell_to_scalar(v.value)
    if isinstance(v, (datetime, date)):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    if isinstance(v, (list, tuple, set)):
        seq = list(v)
        if not seq:
            return ""
        if all(x is None or isinstance(x, (str, int, float, bool)) for x in seq):
            return ", ".join("" if x is None else str(x) for x in seq)
        try:
            return json.dumps(seq, ensure_ascii=False, default=str)
        except Exception:
            return str(seq)
    if isinstance(v, dict):
        if not v:
            return ""
        try:
            return json.dumps(v, ensure_ascii=False, default=str)
        except Exception:
            return str(v)
    try:
        if hasattr(v, "model_dump"):
            return _cell_to_scalar(v.model_dump(mode="python"))  # type: ignore[attr-defined]
    except Exception:
        pass
    return str(v)


def _rectify_matrix(headers: List[Any], matrix: List[List[Any]]) -> List[List[Any]]:
    """Pad/truncate each row to header length AND flatten every cell to a
    Sheets-writable scalar.

    v6.8.0: the per-cell scalar pass (_cell_to_scalar) is NEW. _rectify_matrix is
    the single common choke point both the rows_matrix path and the rows[dict]
    path pass through before the write (see _run_one_task), so the flatten lives
    here and covers both. The Sheets RAW write rejects list/dict cells; the
    backend's structured columns (e.g. "Scoring Errors") were 400-ing the page
    write once >1 row was sent. Scalars/None are unchanged; widths are unchanged.
    """
    width = len(headers or [])
    if width <= 0:
        return [[_cell_to_scalar(c) for c in r] for r in (matrix or []) if isinstance(r, list)]
    out: List[List[Any]] = []
    for r in matrix or []:
        if not isinstance(r, list):
            continue
        rr = [_cell_to_scalar(c) for c in r]
        if len(rr) < width:
            rr = rr + [None] * (width - len(rr))
        elif len(rr) > width:
            rr = rr[:width]
        out.append(rr)
    return out


# -----------------------------------------------------------------------------
# Run one task
# -----------------------------------------------------------------------------
async def _run_one_task(
    task: TaskSpec,
    spreadsheet_id: str,
    start_cell: str,
    max_symbols_override: int,
    clear_before_write: bool,
    dry_run: bool,
    backend: BackendClient,
    sheets: Optional[SheetsWriter],
) -> TaskResult:
    t0 = time.perf_counter()
    res = TaskResult(key=task.key, sheet_name=task.sheet_name, status="pending", start_utc=_utc_now().isoformat())

    try:
        canon_task_key = _canon_key(task.key)

        # Hard filters
        if _is_forbidden_key(canon_task_key):
            res.status = "skipped"
            res.warnings.append("Forbidden legacy key; skipped.")
            return res
        if canon_task_key not in _ALLOWED_KEYS:
            res.status = "skipped"
            res.warnings.append(f"Unknown key {canon_task_key}; skipped.")
            return res

        # Decision-owned (cockpit) page guard (v6.6.0): Top_10_Investments is
        # owned by the cockpit — the user records BUY / decision state in its
        # decision columns, and data_engine_v2 serves a fresh Top_10 on demand
        # via the route, so the daily sync must NOT write (and clear) this page
        # or it blanks those decisions every cycle. Python-side mirror of the
        # GAS isDecisionOwnedPage_ guard (00_Config.gs); previously the guard
        # lived only in GAS and the sync bypassed it. Skip is taken HERE, before
        # the symbol read / backend fetch / write, so nothing is fetched,
        # cleared, or written. status="skipped" (not partial) keeps the daily
        # exit code at 0. Reversible: TFB_SYNC_DECISION_GUARD=0 restores the
        # v6.5.0 write (pages overridable via TFB_SYNC_DECISION_GUARD_PAGES).
        if _decision_guard_should_skip(task.sheet_name):
            res.status = "skipped"
            note = (
                f"{_DECISION_GUARD_TAG} {task.sheet_name} is decision-owned "
                f"(cockpit); daily sync write skipped to protect decision cells "
                f"— it refreshes on demand via the route. Set "
                f"TFB_SYNC_DECISION_GUARD=0 to override."
            )
            res.warnings.append(note)
            logger.info(note)
            return res

        max_syms = max_symbols_override if max_symbols_override >= 0 else task.max_symbols

        symbols: List[str] = []
        if max_syms != 0:
            symbols = _read_symbols(canon_task_key, spreadsheet_id, max_syms)

        # v6.14.0: My_Portfolio rebuild — source symbols from the user's
        # _Portfolio_CostBasis (the authoritative holdings) so the backend
        # returns enriched rows for the ACTUAL holdings. Fail-safe: empty cost
        # basis (unreadable/no creds) leaves the page-driven flow untouched.
        _pf_cost_basis: Dict[str, Dict[str, float]] = {}
        if (
            _portfolio_rebuild_enabled()
            and sheets is not None
            and _guard_norm(task.sheet_name) == _guard_norm("My_Portfolio")
        ):
            _pf_cost_basis = _read_cost_basis(sheets, spreadsheet_id)
            if _pf_cost_basis:
                symbols = sorted(_pf_cost_basis.keys())
                res.warnings.append(
                    f"{_PORTFOLIO_REBUILD_TAG} sourced {len(symbols)} holding(s) from {_COST_BASIS_SHEET}"
                )

        # v6.16.0: Market-page symbol read-back — refresh the symbols the user
        # has on the page instead of overwriting them with placeholder defaults.
        # See the SYMBOL-READBACK block / v6.16.0 changelog for the root cause
        # (_read_symbols returns [] because the imported root symbols_reader has
        # no get_page_symbols / get_universe). Fail-safe: an empty read leaves
        # the page-driven flow untouched; the read-back can only ADD symbols.
        if (
            _market_symbol_readback_enabled()
            and sheets is not None
            and _guard_norm(task.sheet_name) in _market_readback_pages()
        ):
            _existing_syms = _read_existing_page_symbols(sheets, spreadsheet_id, task.sheet_name, max_syms)
            # v6.22.2 L4a: one immediate retry — a transient read hiccup
            # (quota blip, concurrent GAS batch mid-write) must not cost the
            # page its symbol source for the whole cycle.
            if not _existing_syms and _readback_empty_guard_enabled():
                _existing_syms = _read_existing_page_symbols(sheets, spreadsheet_id, task.sheet_name, max_syms)
            # v6.19.0 (WHY 2): drop deny-pattern junk BEFORE it is requested —
            # otherwise the persistence fix below would make it immortal.
            if _existing_syms:
                _deny_pats = _universe_deny_patterns()
                if _deny_pats:
                    _clean_syms = [s for s in _existing_syms if not _universe_junk(s, _deny_pats)]
                    _n_dropped = len(_existing_syms) - len(_clean_syms)
                    if _n_dropped:
                        _fw = (
                            f"{_UNIVERSE_FILTER_TAG} dropped {_n_dropped} deny-pattern "
                            f"symbol(s) from the {task.sheet_name} read-back universe "
                            f"(TFB_SYNC_UNIVERSE_DENY)."
                        )
                        res.warnings.append(_fw)
                        logger.warning(_fw)
                    _existing_syms = _clean_syms
            if _existing_syms:
                symbols = _existing_syms
                res.warnings.append(
                    f"{_MARKET_READBACK_TAG} sourced {len(symbols)} symbol(s) from the {task.sheet_name} sheet"
                )
            elif _readback_empty_guard_enabled() and task.expects_rows:
                # v6.22.2 L4a [READBACK-EMPTY-GUARD]: the sheet IS this page's
                # symbol source; ZERO usable symbols after a retry means the
                # read failed or the sheet was mid-rewrite (2026-07-08 21:37
                # Riyadh: GAS "concurrent writer detected" during exactly this
                # runner's window) — never a legitimate empty page. Falling
                # through would issue a PAGE-DRIVEN request (symbols=[]) that
                # bypasses the shrink floor, persistence, strict membership
                # and the identity-tripwire scope all at once, then rewrite
                # the page verbatim (the ML 1,278 -> 897 amputation). SKIP
                # instead: no fetch, no clear, no write — last-good rows are
                # preserved whole and self-heal on the next healthy run.
                # Bootstrap a genuinely empty page with
                # TFB_SYNC_READBACK_EMPTY_GUARD=0 (one run) or via GAS.
                _rb_msg = (
                    f"{_READBACK_EMPTY_TAG} '{task.sheet_name}' symbol read-back "
                    f"returned 0 usable symbols after a retry — the sheet is this "
                    f"page's symbol source, so a page-driven rewrite here can "
                    f"amputate the universe. Skipping fetch+clear+write to "
                    f"PRESERVE last-good rows; self-heals on the next healthy "
                    f"sync. TFB_SYNC_READBACK_EMPTY_GUARD=0 disables (not "
                    f"recommended)."
                )
                res.status = "skipped"
                res.rows_written = 0
                res.rows_failed = 0
                res.warnings.append(_rb_msg)
                logger.error(_rb_msg)
                return res

        res.symbols_requested = len(symbols)

        # Dry run: still success-ish but no backend call and no write
        if dry_run:
            res.status = "skipped"
            res.warnings.append("Dry run: no backend call, no sheet write.")
            return res

        if (not symbols) and not task.allow_empty_symbols:
            res.status = "skipped"
            res.warnings.append("No symbols found and task disallows empty symbols.")
            return res

        if not symbols:
            res.warnings.append(
                "No symbols found; sending a page-driven request (the endpoint "
                "returns the page's own rows, capped by `limit`)."
            )

        # Limit policy.
        #   symbols present -> cap at the symbol count (ceiling 5000).
        #   symbols EMPTY    -> PAGE-DRIVEN request. The enriched endpoint serves
        #     the page's own content (via the `page` field) and honors `limit`
        #     as a ROW CAP on it. v6.6.0 sent limit:1 here, on the (wrong)
        #     assumption that empty symbols meant "schema-only" — but the
        #     page-driven pages (Market_Leaders, Global_Markets, Commodities_FX,
        #     Mutual_Funds) DO have rows, so limit:1 silently truncated each to a
        #     SINGLE written row (confirmed live: Market_Leaders returned 8 rows
        #     at limit:800 vs 1 row at limit:1). Send the task's configured cap
        #     instead (high ceiling when max_symbols=0), so the full page
        #     returns. Never sends literal 0.
        #     Reversible: TFB_SYNC_PAGE_LIMIT_FIX=0 restores the v6.6.0 limit:1.
        if symbols:
            safe_limit = min(5000, max(1, len(symbols)))
        elif _page_limit_fix_enabled():
            safe_limit = task.max_symbols if (task.max_symbols and task.max_symbols > 0) else 5000
        else:
            safe_limit = 1  # v6.6.0 behavior (kill-switch)

        payload: Dict[str, Any] = {
            # identifiers (compat)
            "sheet": task.sheet_name,
            "sheet_name": task.sheet_name,
            "page": task.sheet_name,
            "name": task.sheet_name,
            "tab": task.sheet_name,
            # symbols
            "tickers": symbols,
            "symbols": symbols,
            # behavior
            "refresh": True,
            "include_meta": True,
            "include_matrix": True,
            "limit": safe_limit,
            # tracing
            "request_id": res.request_id,
        }

        last_err: Optional[str] = None
        headers: List[Any] = []
        rows_matrix: List[List[Any]] = []
        used_endpoint: Optional[str] = None
        eff_gw = _effective_gateway(task)  # v6.10.0: ranked market pages -> analysis when enabled

        # v6.17.0 [SYMBOL-BATCHING]: when enabled, a many-symbol market page is
        # fetched in small SEQUENTIAL batches (see _fetch_market_rows_batched /
        # the v6.17.0 changelog) and the combined (headers, rows_matrix) flow
        # into the SAME guards + clear/write below. Default OFF ->
        # _should_batch_market_page returns False and the original single-
        # request candidate loop runs byte-identically (the `for ep in [...]`
        # below evaluates its normal candidate list).
        _use_batching = _should_batch_market_page(task, symbols)
        if _use_batching:
            headers, rows_matrix, used_endpoint, last_err = await _fetch_market_rows_batched(
                backend, task, symbols, payload, eff_gw, res
            )
            # v6.21.0 (6b): one bounded second pass for empty-price rows
            # (breaker-window casualties). OFF by default; splice-by-symbol
            # is header-guarded so it can never make the page worse.
            if _empty_retry_enabled() and rows_matrix:
                rows_matrix, _healed = await _retry_empty_rows(
                    backend, task, headers, rows_matrix, payload, eff_gw, res
                )

        for ep in ([] if _use_batching else _endpoint_candidates_for_gateway(eff_gw)):
            data, err, _code = await backend.post_json(ep, payload)
            if err:
                last_err = f"{ep} -> {err}"
                continue
            if not isinstance(data, dict):
                last_err = f"{ep} -> Non-dict response"
                continue

            headers, rows_matrix = _extract_table_payload(data)
            # v6.15.0 TOP10-HEADER-REPAIR: the analysis route can return a blank
            # header row for Top_10 (118 empty-string cells), which the writer
            # would put on the sheet verbatim -> every column title blank ->
            # validator cannot map columns -> "all rows missing price". Rebuild
            # the header row from the canonical schema (using the response's own
            # keys for column order) so columns are labeled correctly regardless
            # of the route bug. FAIL-SAFE: returns headers unchanged when the
            # schema/keys are unavailable, so it can never make the page worse
            # than the (already blank) current state.
            if _guard_norm(task.sheet_name) == _guard_norm("Top_10_Investments"):
                headers = _repair_top10_headers(headers, data, rows_matrix)
            if not headers:
                last_err = f"{ep} -> Missing headers"
                continue

            rows_matrix = _rectify_matrix(headers, rows_matrix)
            # v6.14.0: inject the user's Qty/Avg Cost from _Portfolio_CostBasis
            # and recompute MV/Cost/P&L so the guard passes and no half-row is
            # written. No-ops if columns absent (guard then blocks the still-
            # blank write -> safe fall-back to the current blocked state).
            if _pf_cost_basis and rows_matrix:
                rows_matrix, _inj = _inject_portfolio_holdings(headers, rows_matrix, _pf_cost_basis)
                if _inj:
                    res.warnings.append(
                        f"{_PORTFOLIO_REBUILD_TAG} injected Qty/Avg Cost + recomputed position math for {_inj} holding(s)"
                    )
            # v6.15.0 DECISION-RECONCILE: keep the displayed decision columns
            # self-consistent on the two decision pages (My_Portfolio, Top_10)
            # so the integrity gates pass and the sheet never shows a
            # contradiction. NEUTRAL — it only removes contradictions (sell-
            # family reco still saying INVEST -> HOLD; buy-family reco carrying a
            # real block_reason -> WATCH/HOLD). It never invents a BUY or SELL
            # call. Engine still emits the raw values; engine-side root fix is a
            # separate follow-up. No-ops when the columns are absent.
            if _guard_norm(task.sheet_name) in _DECISION_RECONCILE_PAGES and rows_matrix:
                rows_matrix, _rec = _reconcile_decision_rows(headers, rows_matrix, page_label=task.sheet_name)
                if _rec:
                    res.warnings.append(
                        f"{_DECISION_RECONCILE_TAG} reconciled {_rec} contradictory decision row(s)"
                    )
            used_endpoint = ep
            break

        if not headers:
            res.status = "failed"
            res.error = last_err or "All endpoints failed"
            return res

        res.gateway_used = f"{eff_gw}:{used_endpoint}" if used_endpoint else eff_gw
        res.symbols_processed = len(symbols)

        # No creds => partial (data fetched but not written)
        if sheets is None or sheets._get_service() is None:
            res.status = "partial"
            res.warnings.append("No Google Sheets credentials. Backend data fetched but not written.")
            res.rows_written = 0
            res.rows_failed = len(rows_matrix or [])
            return res

        # --- Strict response membership (v6.19.1) ----------------------------
        # The backend can return MORE rows than were requested (gateway/universe
        # over-return — the confirmed no-paste origin of the 749 -> 3,068
        # Global_Markets ratchet). Every foreign row written becomes a REQUESTED
        # symbol on the next run because the sheet is the symbol source. Drop
        # unrequested rows BEFORE any guard runs, so the empty-guard catches a
        # fully-foreign response, the shrink guard measures coverage on
        # REQUESTED rows only, and persistence re-appends genuine misses.
        # Scoped exactly like persistence (requested-symbol pages only); rows
        # with a blank Symbol cell are kept; TFB_SYNC_STRICT_MEMBERSHIP=0
        # restores v6.19.0 byte-identically. Never breaks the write path.
        if (_strict_membership_enabled() and task.expects_rows and symbols
                and rows_matrix and headers):
            try:
                _rows_before = len(rows_matrix)
                rows_matrix, _foreign_syms = _filter_rows_to_requested(headers, rows_matrix, symbols)
                _rows_dropped = _rows_before - len(rows_matrix)
                if _rows_dropped > 0:
                    _sm = (
                        f"{_STRICT_MEMBERSHIP_TAG} dropped {_rows_dropped} unrequested "
                        f"row(s) ({len(_foreign_syms)} foreign symbol(s)) returned by the "
                        f"backend for '{task.sheet_name}' — requested {len(symbols)}, "
                        f"kept {len(rows_matrix)}: {', '.join(_foreign_syms[:15])}"
                        f"{'…' if len(_foreign_syms) > 15 else ''} — unrequested rows can "
                        f"no longer expand the page universe."
                    )
                    res.warnings.append(_sm)
                    logger.warning(_sm)
            except Exception as _se:  # never let membership filtering break the write path
                _sm = f"{_STRICT_MEMBERSHIP_TAG} skipped (error: {_se})"
                res.warnings.append(_sm)
                logger.warning(_sm)
        # ---------------------------------------------------------------------

        # --- Symbol<->Name identity tripwire (v6.22.0 L3) --------------------
        # Live failure mode (2026-07-08 17:30-18:12 UTC): the response carried
        # the REQUESTED symbols with attribute payloads belonging to OTHER
        # symbols (ML 1010.SR="AstraZeneca PLC", GOOGL="Arabia Insurance
        # Cooperative Company", ...). Membership filtering reads only the
        # Symbol cell and cannot see it. Verify the built-in anchor pairs
        # PRESENT in the fetched matrix; >= TFB_SYNC_IDENTITY_MIN_FAILS
        # mismatches (default 2; tonight's ML showed >=5) means the payload is
        # transposed at the source -> SKIP clear+write and PRESERVE last-good
        # rows, exactly like the empty/shrink guards. Blank names never count;
        # a page with no anchors present is never blocked. Scoped like
        # membership (requested-symbol pages); TFB_SYNC_IDENTITY_TRIPWIRE=0
        # disables (not recommended).
        if (_identity_tripwire_enabled() and task.expects_rows and symbols
                and rows_matrix and headers):
            try:
                _idn_checked, _idn_ok, _idn_bad = _identity_anchor_scan(headers, rows_matrix)
                if _idn_checked:
                    res.warnings.append(
                        f"{_IDENTITY_TAG} {task.sheet_name}: anchors "
                        f"checked={_idn_checked} ok={_idn_ok} "
                        f"mismatched={len(_idn_bad)}"
                    )
                if len(_idn_bad) >= _identity_min_fails():
                    _pairs = "; ".join(f"{_s}='{_n}'" for _s, _n in _idn_bad[:10])
                    _msg = (
                        f"{_IDENTITY_TAG} TRIPPED on '{task.sheet_name}': "
                        f"{len(_idn_bad)}/{_idn_checked} identity anchors carry "
                        f"a FOREIGN name ({_pairs}) — the response is "
                        f"symbol<->attribute transposed at the source. Skipping "
                        f"clear+write to PRESERVE last-good rows; the next "
                        f"healthy sync self-heals. TFB_SYNC_IDENTITY_TRIPWIRE=0 "
                        f"disables (not recommended)."
                    )
                    res.status = "skipped"
                    res.rows_written = 0
                    res.rows_failed = 0
                    res.warnings.append(_msg)
                    logger.error(_msg)
                    return res
            except Exception as _ie:  # never let the tripwire break the write path
                _iw = f"{_IDENTITY_TAG} skipped (error: {_ie})"
                res.warnings.append(_iw)
                logger.warning(_iw)
        # ---------------------------------------------------------------------

        # --- My_Portfolio manual-cell write guard (v6.5.0) -------------------
        # Independently verify this write will not blank user-authored Qty/Avg
        # Cost on the live sheet. On ANY doubt, skip the write (the existing row
        # is preserved whole and self-heals on the next healthy sync). Placed
        # BEFORE the clear/write so a skip performs neither — never clear-then-
        # skip. Scoped to manual pages; gated by TFB_SYNC_MANUAL_GUARD.
        if rows_matrix and _guard_should_apply(task.sheet_name):
            allow_write, guard_note = _portfolio_write_guard(
                sheets, spreadsheet_id, task.sheet_name, headers, rows_matrix
            )
            if guard_note:
                res.warnings.append(guard_note)
                logger.warning(guard_note)
            if not allow_write:
                res.status = "partial"
                res.rows_written = 0
                res.rows_failed = len(rows_matrix or [])
                return res
        # ---------------------------------------------------------------------

        # --- Empty-rows wipe guard (v6.9.0) ---------------------------------
        # A page that EXPECTS rows but came back with headers + ZERO data rows
        # means the fetch degenerated (e.g., a provider/Yahoo outage where every
        # symbol on the page failed) — NOT a legitimate result. The original code
        # fell through to clear-before-write and wrote headers-only, BLANKING the
        # tab and reporting status="success". Placed BEFORE the clear so a skip
        # performs NEITHER clear nor write — last-good rows are preserved and
        # self-heal on the next healthy sync. Gated by TFB_SYNC_EMPTY_GUARD
        # (default ON); set 0/false/off/no to restore the v6.8.0 behavior.
        if task.expects_rows and (not rows_matrix) and _empty_guard_enabled():
            # v6.15.1 TOP10-HEADER-SELFHEAL: this empty fetch means we PRESERVE
            # the last-good data rows (skip the data write). But a Top_10 header
            # row left blank by a prior route bug would keep the validator blind
            # and the page red forever (blank header -> symbol read finds no
            # Symbol column -> page-driven request -> 0 rows -> skip -> header
            # stays blank). Repair ONLY row 1 from the canonical schema (column
            # order from the response's own keys) so the existing last-good rows
            # become labeled; the data rows below are untouched.
            if (_guard_norm(task.sheet_name) == _guard_norm("Top_10_Investments")
                    and _top10_selfheal_enabled()):
                try:
                    _fixed_hdr = _repair_top10_headers(headers, data, [])
                    _canon_h, _ = _canonical_top10_schema()
                    if _fixed_hdr and _canon_h and len(_fixed_hdr) == len(_canon_h):
                        sheets.write_table(spreadsheet_id, task.sheet_name, "A1", _fixed_hdr, [])
                        _hp = ("[TOP10-HEADER-SELFHEAL] repaired blank Top_10 header "
                               "row from schema (data rows preserved)")
                        res.warnings.append(_hp)
                        logger.warning(_hp)
                except Exception as _e:  # never let a self-heal attempt break the run
                    logger.warning(f"[TOP10-HEADER-SELFHEAL] skipped (error: {_e})")
            msg = (
                f"Empty fetch (headers present, 0 data rows) on '{task.sheet_name}', "
                f"which expects rows. Skipping clear+write to PRESERVE last-good rows; "
                f"self-heals on the next healthy sync."
            )
            res.status = "skipped"
            res.rows_written = 0
            res.rows_failed = 0
            res.warnings.append(msg)
            logger.warning(msg)
            return res
        # ---------------------------------------------------------------------

        # --- Partial-fetch shrink guard (v6.18.2) ----------------------------
        # The empty-guard above catches ZERO rows; this catches the throttled
        # PARTIAL fetch (some symbol-batches failed) that would otherwise write
        # a shorter table, trim the tail, and — because the sheet is the symbol
        # source — permanently delete the missed symbols (the 2026-07-02
        # Market_Leaders 288->163 ratchet). Requested-symbol pages only; the
        # write is skipped and last-good rows self-heal on the next healthy run.
        _cov_floor = _min_coverage_pct()
        if (task.expects_rows and _cov_floor > 0.0 and symbols
                and rows_matrix is not None
                and len(rows_matrix) < (len(symbols) * _cov_floor / 100.0)):
            _cov = 100.0 * len(rows_matrix) / max(1, len(symbols))
            msg = (
                f"Partial fetch on '{task.sheet_name}': {len(rows_matrix)} row(s) "
                f"for {len(symbols)} requested symbol(s) ({_cov:.0f}% coverage, "
                f"floor {_cov_floor:.0f}%). Skipping write to PRESERVE last-good "
                f"rows — writing this would permanently drop the missed symbols "
                f"from the page (it is its own symbol source). Self-heals on the "
                f"next healthy sync."
            )
            res.status = "skipped"
            res.rows_written = 0
            res.rows_failed = 0
            res.warnings.append(msg)
            logger.warning(msg)
            return res
        # ---------------------------------------------------------------------

        # --- Per-symbol persistence (v6.19.0, WHY 1) -------------------------
        # The empty-guard blocks a ZERO-row write and the shrink guard blocks
        # <70% coverage — but a 70-99% fetch still rewrote the page verbatim,
        # silently deleting every requested symbol the backend missed (and,
        # because the sheet is the symbol source, deleting it PERMANENTLY).
        # Append the last-good row of each requested-but-missing symbol so a
        # fetch miss can never remove an operator symbol; the next healthy
        # fetch replaces the preserved row with fresh data in place.
        if (_symbol_persistence_enabled() and task.expects_rows and symbols
                and rows_matrix and headers and sheets is not None):
            try:
                rows_matrix, _kept_syms = _persist_missing_symbol_rows(
                    sheets, spreadsheet_id, task.sheet_name, headers, rows_matrix, symbols
                )
                if _kept_syms:
                    _pw = (
                        f"{_SYMBOL_PERSISTENCE_TAG} preserved {len(_kept_syms)} "
                        f"last-good row(s) for fetch-missed symbol(s) on "
                        f"'{task.sheet_name}': {', '.join(_kept_syms[:15])}"
                        f"{'…' if len(_kept_syms) > 15 else ''} — a fetch miss no "
                        f"longer deletes a requested symbol."
                    )
                    res.warnings.append(_pw)
                    logger.warning(_pw)
            except Exception as _pe:  # never let persistence break the write path
                _pw = f"{_SYMBOL_PERSISTENCE_TAG} skipped (error: {_pe})"
                res.warnings.append(_pw)
                logger.warning(_pw)

        # --- Keep-last-good substitution (v6.22.3 L4c) ------------------------
        # Persistence protects a symbol the backend OMITS; a symbol answered
        # with a DATA-FREE ERROR STUB is "present", passes every membership
        # guard, and overwrites the last good row (the Global_Markets
        # fallback_error erosion, 2026-07-10). Swap each stub for the symbol's
        # existing GOOD row; a stub with no good predecessor stays fresh. Zero
        # stubs (the normal healthy sync) costs zero extra reads.
        # TFB_SYNC_KEEP_LAST_GOOD=0 restores v6.22.2 exactly.
        if (_keep_last_good_enabled() and task.expects_rows
                and rows_matrix and headers and sheets is not None
                and task.sheet_name in _RANKED_MARKET_PAGES):
            try:
                rows_matrix, _klg_syms = _keep_last_good_rows(
                    sheets, spreadsheet_id, task.sheet_name, headers, rows_matrix
                )
                if _klg_syms:
                    _kw = (
                        f"{_KEEP_LAST_GOOD_TAG} substituted {len(_klg_syms)} "
                        f"error-stub row(s) with last-good data on "
                        f"'{task.sheet_name}': {', '.join(_klg_syms[:15])}"
                        f"{'…' if len(_klg_syms) > 15 else ''} — a backend error "
                        f"stub no longer erases a symbol's data."
                    )
                    res.warnings.append(_kw)
                    logger.warning(_kw)
            except Exception as _ke:  # never let the guard break the write path
                _kw = f"{_KEEP_LAST_GOOD_TAG} skipped (error: {_ke})"
                res.warnings.append(_kw)
                logger.warning(_kw)
        # ----------------------------------------------------------------------

        # --- Persistence outcome verification (v6.22.2 L4b) ------------------
        # _persist_missing_symbol_rows is FAIL-SAFE: its own read_values
        # failure (or an unlocatable header) returns the SHRUNKEN matrix
        # unchanged WITHOUT raising, so the try/except above never fires and a
        # 70-99%-coverage write proceeds — permanently deleting every
        # fetch-missed symbol (the sheet is the symbol source). Verify the
        # OUTCOME instead of trusting the pass: any requested symbol still
        # absent from the final matrix means the preservation degraded ->
        # SKIP clear+write and PRESERVE last-good rows, exactly like the
        # empty/shrink/tripwire guards. Valid on the ranked market pages
        # because their requested symbols came FROM the sheet (read-back), so
        # a last-good row exists for each by construction; My_Portfolio keeps
        # v6.22.1 semantics (a brand-new cost-basis holding legitimately has
        # no last-good row; its v6.5.0 manual guard already protects it).
        # Runs only while persistence itself is ON — persistence deliberately
        # OFF restores the documented v6.18.2 drop behavior whole.
        # TFB_SYNC_PERSISTENCE_HARD=0 restores v6.22.1 warn-and-continue.
        if (_persistence_hard_enabled() and _symbol_persistence_enabled()
                and task.expects_rows and symbols and headers
                and rows_matrix and sheets is not None
                and task.sheet_name in _RANKED_MARKET_PAGES):
            try:
                _still_missing = _unpersisted_missing(headers, rows_matrix, symbols)
            except Exception as _ve:  # never let verification break the write path
                _still_missing = []
                _vw = f"{_PERSISTENCE_HARD_TAG} verification skipped (error: {_ve})"
                res.warnings.append(_vw)
                logger.warning(_vw)
            if _still_missing:
                _hm = (
                    f"{_PERSISTENCE_HARD_TAG} TRIPPED on '{task.sheet_name}': "
                    f"{len(_still_missing)} requested symbol(s) are still absent "
                    f"from the final matrix after the persistence pass "
                    f"({', '.join(_still_missing[:15])}"
                    f"{'…' if len(_still_missing) > 15 else ''}) — writing would "
                    f"permanently delete them from the page (it is its own "
                    f"symbol source). Skipping clear+write to PRESERVE last-good "
                    f"rows; self-heals on the next healthy sync. "
                    f"TFB_SYNC_PERSISTENCE_HARD=0 disables (not recommended)."
                )
                res.status = "skipped"
                res.rows_written = 0
                res.rows_failed = 0
                res.warnings.append(_hm)
                logger.error(_hm)
                return res
        # ---------------------------------------------------------------------

        # v6.20.0 (Fix 1b): harvest (page, symbol, price) from the FINAL matrix
        # for the cross-page price-delta report. Read-only; flag-gated; can
        # never affect the write path. Runs in dry-run too (reads the fetched
        # matrix, not the sheet).
        if _xpage_check_enabled():
            try:
                _xn = _xpage_collect(task.sheet_name, headers, rows_matrix)
                if _xn:
                    logger.info("%s harvested %d priced rows from %s", _XPAGE_TAG, _xn, task.sheet_name)
            except Exception as _xe:
                logger.warning("%s harvest skipped for %s (error: %s)", _XPAGE_TAG, task.sheet_name, _xe)

        # v6.18.0 (Fix 2): cancellation-safe ordering. Legacy clear-then-write
        # leaves an EMPTY page when the job dies between the two calls (the
        # 2026-07-02 Mutual_Funds / Commodities_FX wipe). Default is now
        # WRITE-then-TRIM: the atomic values.update overwrites in place first,
        # then _trim_after_write clears only the stale tail below/right.
        # TFB_SYNC_WRITE_THEN_TRIM=0 restores the exact v6.17.0 order.
        _trim_mode = clear_before_write and _write_then_trim_enabled()
        if clear_before_write and not _trim_mode:
            try:
                sheets.clear_from(spreadsheet_id, task.sheet_name, start_cell)
            except Exception as e:
                res.warnings.append(f"Clear failed: {e}")

        try:
            written = sheets.write_table(spreadsheet_id, task.sheet_name, start_cell, headers, rows_matrix)
            res.rows_written = int(written)
            if _trim_mode:
                for _w in _trim_after_write(
                    sheets, spreadsheet_id, task.sheet_name, start_cell,
                    n_header=(1 if headers else 0),
                    n_rows=len(rows_matrix or []),
                    n_cols=len(headers or []),
                ):
                    res.warnings.append(_w)
                    logger.warning(_w)

            # schema-only (0 rows) => success
            if not rows_matrix:
                res.rows_failed = 0
                res.status = "success"
            else:
                res.rows_failed = max(0, len(rows_matrix) - res.rows_written)
                res.status = "success" if res.rows_failed == 0 else ("partial" if res.rows_written > 0 else "failed")
        except Exception as e:
            res.status = "failed"
            res.error = f"Write failed: {e}"

        return res

    except Exception as e:
        res.status = "failed"
        res.error = str(e)
        return res

    finally:
        res.end_utc = _utc_now().isoformat()
        res.duration_ms = (time.perf_counter() - t0) * 1000.0


# -----------------------------------------------------------------------------
# Main runner
# -----------------------------------------------------------------------------
async def main_async(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=f"TFB Dashboard Sync Runner v{SCRIPT_VERSION}")
    parser.add_argument("--sheet-id", default="", help="Spreadsheet ID override")
    parser.add_argument("--backend", default="", help="Backend base URL override (e.g. https://... )")
    parser.add_argument("--keys", nargs="*", default=[], help="Specific keys (space/comma/semicolon/JSON-array supported)")
    parser.add_argument("--start-cell", default="A5", help="Top-left A1 cell where headers will be written (e.g. A5)")
    parser.add_argument("--max-symbols", default="-1", help="Override max symbols for all tasks (-1 = per task default)")
    parser.add_argument("--workers", default="4", help="Parallel workers")
    parser.add_argument("--clear", action="store_true", help="(Deprecated — clear is now the default) Clear from start-cell down before writing.")
    parser.add_argument("--no-clear", action="store_true", help="Disable clear-before-write. NOT recommended: leaves stale trailing rows/columns from prior shorter writes (the ghost-row cause). Use only for deliberate append/preserve runs.")
    parser.add_argument("--dry-run", action="store_true", help="Do not call backend or write sheets")
    parser.add_argument("--no-lock", action="store_true", help="Disable Redis lock even if REDIS_URL exists")
    parser.add_argument("--json-out", default="", help="Write JSON report to this file path")
    parser.add_argument("--timeout", default="30", help="Backend timeout seconds")
    args = parser.parse_args(list(argv) if argv is not None else None)

    spreadsheet_id = _default_spreadsheet_id(args.sheet_id)
    if not spreadsheet_id:
        logger.error("DEFAULT_SPREADSHEET_ID is missing and --sheet-id not provided.")
        return 2

    backend_url = (args.backend or _default_backend_url()).rstrip("/")
    start_cell = _validate_a1_cell(args.start_cell)
    max_symbols = _safe_int(args.max_symbols, -1, lo=-1, hi=5000)
    workers = _safe_int(args.workers, 4, lo=1, hi=32)
    timeout_sec = float(_safe_int(args.timeout, 30, lo=5, hi=180))

    token = _env_token()
    if not token:
        logger.warning("No backend token found (TFB_TOKEN/X_APP_TOKEN/APP_TOKEN/BACKEND_TOKEN). Requests may 401 if protected.")

    tasks = _default_tasks()
    # v6.21.0 (6a): optional launch-order override for the enriched market
    # tasks (starved small pages ahead of the big two). Unset -> unchanged.
    tasks = _apply_page_order(tasks)

    wanted = _parse_keys_tokens(args.keys or [])
    forbidden_requested = [k for k in wanted if _is_forbidden_key(k)]
    if forbidden_requested:
        logger.warning("Forbidden keys requested and will be ignored: %s", ", ".join(forbidden_requested))

    wanted_ok = [k for k in wanted if (k in _ALLOWED_KEYS and not _is_forbidden_key(k))]
    if wanted_ok:
        tasks = [t for t in tasks if _canon_key(t.key) in set(wanted_ok)]

    tasks.sort(key=lambda t: (t.priority, t.key))
    if not tasks:
        logger.warning("No tasks selected.")
        return 0

    # clamp workers to tasks count
    workers = max(1, min(workers, len(tasks)))

    summary = RunSummary()
    summary.total_tasks = len(tasks)
    t0 = time.perf_counter()

    backend = BackendClient(backend_url, timeout_sec=timeout_sec, token=token)
    sheets = SheetsWriter()

    lock_name = f"{spreadsheet_id}:{','.join([_canon_key(t.key) for t in tasks])}"
    lock = RedisLock(lock_name, ttl_sec=600)

    results: List[TaskResult] = []
    try:
        # Preflight health (best-effort)
        for hp in ("/readyz", "/health", "/livez"):
            data, err, _code = await backend.get_json(hp)
            if err:
                logger.info("Backend preflight %s -> %s", hp, err)
                continue
            status_val = (data or {}).get("status") if isinstance(data, dict) else None
            logger.info("Backend preflight %s -> %s", hp, status_val or "ok")
            break

        # Acquire lock
        acquired = True if args.no_lock else await lock.acquire()
        if not acquired:
            logger.error("Could not acquire Redis lock. Use --no-lock to bypass.")
            return 2

        sem = asyncio.Semaphore(workers)

        async def _guarded(task: TaskSpec) -> TaskResult:
            async with sem:
                return await _run_one_task(
                    task=task,
                    spreadsheet_id=spreadsheet_id,
                    start_cell=start_cell,
                    max_symbols_override=max_symbols,
                    clear_before_write=(not bool(args.no_clear)),
                    dry_run=bool(args.dry_run),
                    backend=backend,
                    sheets=sheets,
                )

        out = await asyncio.gather(*[_guarded(t) for t in tasks], return_exceptions=True)

        for i, r in enumerate(out):
            if isinstance(r, Exception):
                tr = TaskResult(
                    key=tasks[i].key,
                    sheet_name=tasks[i].sheet_name,
                    status="failed",
                    start_utc=_utc_now().isoformat(),
                    end_utc=_utc_now().isoformat(),
                    duration_ms=0.0,
                    error=str(r),
                )
                results.append(tr)
            else:
                results.append(r)

        for r in results:
            if r.status == "success":
                summary.success += 1
            elif r.status == "partial":
                summary.partial += 1
            elif r.status == "failed":
                summary.failed += 1
            else:
                summary.skipped += 1
            summary.total_rows_written += r.rows_written
            summary.total_rows_failed += r.rows_failed

        summary.end_utc = _utc_now().isoformat()
        summary.duration_ms = (time.perf_counter() - t0) * 1000.0

        logger.info("============================================================")
        logger.info(
            "SYNC DONE | success=%d partial=%d failed=%d skipped=%d | rows_written=%d | duration_ms=%.2f",
            summary.success,
            summary.partial,
            summary.failed,
            summary.skipped,
            summary.total_rows_written,
            summary.duration_ms,
        )

        # v6.20.0 (Fix 1b): cross-page price-delta report. Observe-and-report
        # only: exit code, results, and writes are untouched. One INFO summary
        # (even at zero conflicts, for observability) + capped WARN lines,
        # worst spread first.
        if _xpage_check_enabled():
            try:
                _xstats, _xlines = _xpage_report()
                logger.info(
                    "%s report | pages=%d symbols=%d multi_page=%d conflicts=%d threshold=%.2f%%",
                    _XPAGE_TAG,
                    _xstats.get("pages", 0),
                    _xstats.get("symbols", 0),
                    _xstats.get("symbols_multi_page", 0),
                    _xstats.get("conflicts", 0),
                    _xpage_delta_threshold_pct(),
                )
                for _xl in _xlines:
                    logger.warning(_xl)
            except Exception as _xe:
                logger.warning("%s report skipped (error: %s)", _XPAGE_TAG, _xe)

        for r in results:
            if r.status == "success":
                logger.info("✅ %s -> %s | rows=%d | %.1fms", _canon_key(r.key), r.sheet_name, r.rows_written, r.duration_ms)
            elif r.status == "partial":
                logger.info(
                    "⚠️  %s -> %s | rows=%d failed=%d | %.1fms | %s",
                    _canon_key(r.key),
                    r.sheet_name,
                    r.rows_written,
                    r.rows_failed,
                    r.duration_ms,
                    "; ".join(r.warnings[:2]),
                )
            elif r.status == "failed":
                logger.info("❌ %s -> %s | %s", _canon_key(r.key), r.sheet_name, r.error or "failed")
            else:
                logger.info("⏭️  %s -> %s | %s", _canon_key(r.key), r.sheet_name, "; ".join(r.warnings[:2]) if r.warnings else "skipped")

        if args.json_out:
            report = {"summary": summary.to_dict(), "results": [x.to_dict() for x in results]}
            Path(args.json_out).write_text(json.dumps(_coerce_jsonable(report), indent=2, ensure_ascii=False), encoding="utf-8")
            logger.info("Report saved: %s", args.json_out)

        # Exit codes
        if summary.failed > 0:
            return 2
        if summary.partial > 0:
            return 1
        return 0

    finally:
        try:
            await lock.release()
        except Exception:
            pass
        await lock.close()
        await backend.close()


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        return 130
    except Exception as e:
        logger.exception("Unhandled error: %s", e)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
