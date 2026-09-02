#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/run_dashboard_sync.py
================================================================================
TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.44.1)
================================================================================
PRODUCTION-HARDENED | ASYNC | NON-BLOCKING | COMPILEALL-SAFE | SCHEMA-FIRST

v6.34.0 — PERSISTENCE TRUTH & SECOND-CHANCE PASS (run 30782099065 forensics)
================================================================================
EVIDENCE (2026-08-03 03:33 UTC, first scheduled 3-leg night): all four ranked
pages ended rows_written=0. The v6.19.0 persistence injector produced ZERO
preserved rows on every page (no 'preserved N' line, no error line) while the
KLG pass read the same sheets successfully seconds later, and the L4b
HARD-GUARD then vetoed each write (GM 3,589 / CFX 75 / ML 3 / MF 1 absent).
Three ordering-proof, loud fixes — kill-switch TFB_SYNC_PERSIST_V2=0 restores
v6.33.0 byte-behavior:
  PV-1 INSTRUMENTED INJECTOR: logs [PERSIST v6.34.0] missing/grid/hdr/
       injected/reason on EVERY invocation (each silent early-return is
       named) and retries an empty page read once after 4s.
  PV-2 SECOND-CHANCE PASS: a final injector invocation runs AFTER
       batch-identity / KLG / ID-FIREWALL, immediately before L4b, so rows
       dropped by later stages (the ML MSI/WM/MSFT class) are re-preserved.
  PV-3 GUARD COUNTS ONLY REAL LOSS: absent symbols whose old row is missing
       or identity-blank/fabricated are EXEMPT (nothing to lose); the veto
       fires only for non-blank last-good rows. ZERO functions removed.
================================================================================
v6.33.0 — INTEGRITY CLOSEOUT (external audit P0-2 / P0-3, 2026-08-03)
================================================================================
P0-3a SHEETS-SERIAL DATES: Google Sheets returns date cells as numeric
  serials under UNFORMATTED_VALUE; the ISO-only parser failed OPEN and
  silently ignored real holds. _mh_parse_hold_until now recognizes plausible
  serials (20000..80000, epoch 1899-12-30, naive => Riyadh) alongside ISO.
P0-3b REJECT, NOT CLAMP: min(dt, now+12h) re-evaluated every read turned a
  far-future cell into a PERPETUALLY-ROLLING hold. A value beyond
  now + 12h (+60s skew) is now REJECTED -> None (fail-open, logged), matching
  the documented ceiling contract instead of renewing it forever.
P0-3c BENIGN HOLD SKIP: "[MANUAL-HOLD" joined _BENIGN_SKIP_MARKERS and the
  per-task deferral now carries the marker in TaskResult.warnings, so the
  v6.26.0 stale-skip escalation can never convert an intentional operator
  hold into a red leg.
P0-2 POISON-PREDECESSOR RESURRECTION CLOSED: FW-5 stripped fresh fabrications
  but KLG/FW-KEEP could certify and restore an OLD fabricated row (nonblank
  fabricated Name passed Leg-1; provider 'placeholder_fallback' was not in
  the KLG error set). Central primitives are now consulted at certification:
  _klg_provider_is_error treats _FABRICATED_PROVIDER_TOKEN as error, and
  _klg_old_row_identity_ok adds unconditional Leg-1b via _name_is_fabricated.
  Honest stubs (no_data_stub/placeholder_stub) remain non-error, non-GOOD.
  ZERO functions removed; all v6.32.0 behavior otherwise byte-identical.
================================================================================
v6.32.0 — MANUAL-HOLD BRIDGE (operator manual refresh gets clean priority)
--------------------------------------------------------------------------
WHY (2026-08-02, operator report + layer audit): manual in-sheet refreshes
collide with the Actions sync legs (now three, every 4h) and stall — e.g.
Global_Markets stuck "PARTIAL — paused at 50 of 6190". The repository GAS
coordinator is deployment-INERT by design (its CI verifies the inert
banner), and even deployed its pause lives in GAS ScriptProperties which
this Python sync cannot see. No bridge existed between the two layers.

FIX — a workbook cell both layers can read is the bus:

  CONTRACT: sheet tab `_Sync_Control`, any row within A1:B6 whose column-A
  text normalizes to "manual hold until"; column B holds the hold-expiry
  timestamp. ISO-8601 accepted with or without offset; a NAIVE value is
  interpreted as Riyadh time (UTC+3) because the operator types local time;
  an explicit Z/offset is honored exactly. Blank cell, past time, garbage,
  a missing tab, or ANY read error => NO hold (fail-open: a signalling
  hiccup must never freeze the automation). Expiry is clamped to at most
  now+12h so a forgotten hold can never deadlock the system.

  GATES (env TFB_SYNC_MANUAL_HOLD_GATE, default ON; =0/false/off/no
  restores v6.31.0 byte-identically):
    * STARTUP: if a hold is active the whole run defers — exit 0, one
      "[MANUAL-HOLD v6.32.0]" line in the log and a best-effort _Run_Log
      append, exactly the "No tasks selected" no-op semantics.
    * PER-TASK: each task re-checks at semaphore acquisition; a task that
      has not started its page defers with status="skipped". A task already
      past that point finishes its page and its WRITE untouched — a write
      in flight is never abandoned (the v6.18.0 mid-write lesson).
  The cell is polled through a 30-second cache so per-task checks cost at
  most one Sheets read per half-minute.

  RESUME (the second half of the operator request) needs NO new code:
  [OLDEST-FIRST v6.27.0] is default-ON, so the first automatic run after a
  hold clears continues from the least-recently-refreshed rows — resume,
  never restart. HEAL-FIRST keeps damaged rows at the very front.

  Operator usage: set the cell before a manual session (by hand, or via the
  optional standalone menu snippet shipped alongside: "TFB Sync Hold");
  clear it — or simply let it expire — when done.
ZERO functions removed; all prior WHYs preserved.

================================================================================
TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.31.0)
================================================================================
PRODUCTION-HARDENED | ASYNC | NON-BLOCKING | COMPILEALL-SAFE | SCHEMA-FIRST

v6.31.0 — FW-5 FABRICATED-PLACEHOLDER TRIPWIRE + HEAL-FIRST POISONED-NAME FIX
-----------------------------------------------------------------------------
EVIDENCE (2026-08-02 Render Shell test campaign, T13):
POST /v1/analysis/sheet-rows returned FABRICATED rows from the route's
placeholder factory (name = "<Page> <Symbol>", sequential prices 100+idx,
"Accumulate" recommendations, fresh timestamps, provider tag
"advanced_analysis.placeholder_fallback"). This sync wrote them verbatim:
Global_Markets Name cells reading "Global_Markets HELN.SW" etc., open_price
cells of 104.00–108.00 on unrelated .SR rows, GAB$H.US at 107.00 / +7,900%.
Backend fix shipped as advanced_analysis v4.15.0 [NO-FABRICATION]; this
release is the sync-side defense-in-depth + residue healer:

FW-5 (_fabrication_tripwire, new): on the ranked market pages, strip any
OUTGOING row whose Name matches the "<Page> <Symbol>" fabrication pattern OR
whose Data Provider cell contains "placeholder_fallback" — Symbol kept, all
other cells blanked, Warnings tagged
'identity_quarantined:fabricated_placeholder:v6.31.0'. Runs inside the FW-2
block; stripped symbols merge into _idfw_stripped so the existing v6.25.1
FW-KEEP last-good restore and the FW-3 _Run_Log verdict cover FW-5 with zero
new plumbing. v4.15.0 stub rows ("no_data_stub" / "placeholder_stub") are
NOT matched — they are honest no-data rows handled by KLG.

HF-2 (HEAL-FIRST extension): rows already POISONED on the sheet carry a
non-blank fabricated Name, so v6.24.2 HF-1 (blank-name only) never healed
them. The heal-first partition now treats a fabricated-pattern Name as
blank-equivalent, so existing "Global_Markets <sym>" rows jump the refresh
queue and get refilled.

ENV: TFB_SYNC_PLACEHOLDER_GUARD (default ON — identity guards armed by
default per the declared-disarm registry lesson). =0/false/off/no disables
BOTH FW-5 and HF-2, restoring v6.30.0 byte-identically.
ZERO functions removed; all prior WHYs preserved.

v6.28.0 — FW-4b SAFE NAME-DEDUP: KEEP-ONE-PER-CURRENCY, NEVER WIPE A GROUP
--------------------------------------------------------------------------
EVIDENCE (2026-07-25 evening workbook audit, _Run_Log 13:58:49 leg):
FW-4 in quarantine mode stubbed ALL carriers of every flagged name group
— Alibaba (BABA + BABA.US + 9988.HK), TSMC (TSM + TSM.US + 2330.TW),
AB InBev, Diageo, Trip.com, Shinhan vanished from Global_Markets in one
pass, 21 rows quarantined, zero survivors. Two design gaps compounded:
  (1) the v6.24.1 stub action was written for the OBSERVE->flip decision
      and stubs the WHOLE group — correct for pure chimeric poison, data
      loss for anything with one legitimate copy; and
  (2) the v6.25.3 family heuristic is alphabetic-root-based, so numeric
      cross-listings (9961.HK vs TCOM, 2330.TW vs TSM, 055550.KS vs SHG)
      and renamed ADR roots (ABI.BR vs BUD, DGE.L vs DEO) can NEVER pass
      it — exactly the class its own docstring predicted and the
      operator exemption list was never populated for.
FIX — FW-4b survivor selection (default ON), quarantine mode only:
  * each flagged group is partitioned by its rows' Currency cell
    (missing column -> one bucket); DIFFERENT currencies are different
    listings, so every currency bucket keeps its own survivor — a
    cross-listing family can no longer be wiped whole;
  * inside one currency bucket exactly ONE row survives: newest
    Last-Updated stamp first (reuses _parse_stamp_cell; ISO 'T'
    separators normalised), explicit exchange suffix second, most
    non-blank cells third, lexicographic symbol last — fully
    deterministic; only the true losers are stubbed, tagged
    'identity_quarantined:name_dedup_loser:v6.28.0';
  * HARD INVARIANT: a group can never lose all members — helper failure
    or an all-survivor verdict resolves to keep-everything with a
    logged [ID-FIREWALL] note (fail-open, never fail-destructive);
  * verdict line reports dedup_mode=quarantine(keep1) and Details JSON
    carries "dedup_safe" so the six-gate morning audit can grep the
    armed path; ST-1 case 6 now proves the keep-one contract (survivor
    intact, losers tagged, bystander untouched) under the configured
    mode, and the legacy assertions remain under the kill-switch.
ENV: TFB_SYNC_NAME_DEDUP_SAFE default ON; =0/false/off/no restores the
v6.27.0 whole-group stub byte-identically. ZERO functions removed;
additions: _name_dedup_safe_enabled, _name_dedup_survivors
(+ tag _NAME_DEDUP_LOSER_TAG).

v6.27.0 — OLDEST-FIRST WORKLIST (kills the fixed-head refresh starvation)
--------------------------------------------------------------------------
EVIDENCE (2026-07-23 morning workbook vs the 07-22 23:10 baseline): the
01:18 UTC global-markets leg refreshed 1,044 rows — and 783 of them were
the 07-21 cohort (the rows ALREADY freshest on the page), while the
week-old 07-16 core of 3,313 rows received exactly FOUR refreshes and
07-17's 1,254 received ZERO. Root cause is structural, not a failure:
_read_existing_page_symbols returns the Symbol column in SHEET ORDER
(only v6.24.2 HEAL-FIRST stubs jump the queue), the fetch loop walks that
order, and the v6.22.4 TIME-BUDGET stops fetching after ~1,000 symbols —
so the SAME sheet-head refreshes every run and rows past the budget
horizon are permanently unreachable. Stale-but-complete beats
fresh-but-amputated (v6.23.0), but a fixed horizon means the amputation
line never moves. FIX — one reorder at the read-back convergence point,
ranked market pages only: _page_symbol_stamps() reads the page's Symbol
and Last Updated columns (two single-column reads; header located by the
same scan as the v6.26.0 stamp reader; cells parsed by _parse_stamp_cell,
so Sheets date serials and text stamps both work) and
_order_symbols_oldest_first() STABLY sorts the worklist by stamp
ascending with never-stamped rows FIRST (datetime.min key). Under any
per-run budget the kept slice is now the STALEST slice, so the page
round-robins by staleness and fully cycles in ~ceil(rows/budget) runs
with zero cross-run state. HEAL-FIRST interaction: never-stamped stubs
still land at the very front; a freshly-stamped-but-still-nameless stub
can defer one cycle — documented trade, repair beats starvation. Stamp
read failure or empty map => order untouched (fail-safe). One
[OLDEST-FIRST] INFO line reports span and unstamped count. Kill:
TFB_SYNC_OLDEST_FIRST=0 restores the v6.26.0 sheet-order worklist
byte-identically. ZERO functions removed; additions:
_oldest_first_enabled, _page_symbol_stamps, _order_symbols_oldest_first.

v6.26.0 — STALE-SKIP ESCALATION + PAGE-VERDICT TELEMETRY
--------------------------------------------------------------------------
EVIDENCE (2026-07-22 evening + GitHub run forensics): three scheduled runs
(10:30 / 14:09 / 17:25 UTC) each ran the dedicated global-markets leg for
65-80 minutes and finished GREEN — yet the 23:10 Riyadh workbook export
carried ZERO Global_Markets rows stamped 07-22 out of 6,512 (histogram:
07-16x3,317 · 07-17x1,254 · 07-18x426 · 07-20x112 · 07-21x1,012 ·
Nonex339). Root cause is the runner's own protective architecture: EVERY
degradation guard — L4a readback-empty, L4b persistence-hard, the
empty-fetch skip, the shrink floor, the L3/L3b identity & coherence
tripwires, FW quarantine soft-landings — resolves to status="skipped",
which the exit-code policy maps to 0 ("stale-but-complete beats
fresh-but-amputated"). That trade is CORRECT for one cycle and
CATASTROPHIC when chronic: five consecutive dark days on the largest page
looked like five green days, and nothing downstream could tell an
intentional skip from a data outage. FIX (single post-run pass, ZERO
changes inside any guard):
  (1) STALE-SKIP ESCALATION — after all tasks settle, any RANKED market
      page whose TaskResult is status="skipped" for a HEALTH reason (not
      dry-run / decision-owned / forbidden / unknown-key / empty-disallowed)
      while the page's NEWEST on-sheet data stamp is older than
      TFB_SYNC_SKIP_MAX_STALE_H (default 30h) is re-classed to
      status="failed" with a [STALE-SKIP] error naming the first guard —
      the existing exit-code policy then returns 2 and the CI leg goes
      RED. A skip on a still-fresh page stays a green skip (the guards'
      availability trade is preserved); an unreadable stamp column
      escalates nothing (fail-safe None).
  (2) PAGE-VERDICT TELEMETRY — one grep-stable INFO line per task:
      [PAGE-VERDICT v6.26.0] page=<name> status=<s> rows_written=<n>
      newest_stamp_age_h=<h|NA> reason=<first warning> — the per-page
      health signal the 5-day outage never produced.
ENV: TFB_SYNC_STALE_SKIP_RED "1" (kill-switch — 0/false/off/no restores
v6.25.3 statuses, exit codes and logs byte-identically);
TFB_SYNC_SKIP_MAX_STALE_H "30" (floor 1, unparseable -> 30).
Stamp reading: header row located by scanning the first 45 rows for a row
holding both a Symbol column and one of Last Updated (Riyadh)/(UTC)/bare;
cells parsed as Sheets date serials OR "YYYY-MM-DD[ HH:MM:SS]" text;
Riyadh-labeled stamps age against UTC+3. ZERO functions removed;
additions: _stale_skip_red_enabled, _skip_max_stale_h, _col_idx_to_a1,
_parse_stamp_cell, _page_newest_stamp_age_h, _apply_stale_skip_escalation.

v6.25.1 fix — FW-2 QUARANTINE KEEPS LAST-GOOD (evidence: 2026-07-17
17:34 leg, out_stripped=30 incl. AMAT/SNPS/NXPI/F/TRV + 15 .SR majors)
--------------------------------------------------------------------------
FW-2 (v6.24.0) rightly distrusts an OUTGOING row whose P/E != Price/EPS
identity is broken — during the recurring ~12:00-14:30 Riyadh provider
degradation, mixed-vintage fields (fresh price + stale EPS) trip it on
perfectly real blue-chips. But its quarantine action wrote a destructive
symbol-only stub OVER the sheet's healthy last-good row. Pre-6.25.0 the
coverage floor happened to veto those writes, masking the defect; the
FLOOR-MERGE unlock surfaced it: 30 Market_Leaders majors landed as stubs
('identity_quarantined' + 113 blank cells). Ordering is the root cause —
the KLG stub-swap (the rescuer) runs BEFORE FW-2, so FW-2's stubs are born
orphaned.
FIX: immediately after FW-2 strips, a TARGETED second stub-swap restores
each stripped symbol's last-good sheet row (same KLG machinery, same
suspect-validation of the old row), then re-tags its Warnings cell
'identity_quarantined:kept_last_good:v6.25.1' so the event stays visible
on-sheet and in the firewall line. Symbols with no last-good (brand-new
additions) correctly remain stubs. Quarantine thus becomes non-destructive:
distrust the fetch, never destroy the sheet.
KILL-SWITCH: TFB_SYNC_FW_KEEP_LAST_GOOD=0 restores the v6.24.0 destructive
stub byte-identically.

v6.25.0 fix — EXPANSION THROUGHPUT + PARTIAL-WRITE UNLOCK (evidence:
run #2413 artifacts, 2026-07-17 06:19 leg)
--------------------------------------------------------------------------
Observed in the uploaded sync logs: GM completed 6/261 batches in the full
3600s budget; ML 447/1025, CFX 125/453, MF 450/2475 — then EVERY page's
write was vetoed by the 70% coverage floor, so a whole hour of fetching
wrote zero rows and the new-universe symbols stayed blank for another leg.
Three defects compounded:
  (1) DEAD ENDPOINT CANDIDATE: the safe-mode analysis chain listed a bare
      "/analysis/sheet-rows" that is NOT in the backend's canonical route
      map (boot-log owners) — it can only ever 404. Every pre-sticky
      failure paid a pointless extra roundtrip on it.
  (2) LOST BATCHES: a batch whose /v1 attempt failed (the ~95s silent
      timeouts under morning contention) was NEVER re-fetched — its 25
      symbols were simply gone from the run (ML's exact 447/1025). FIX:
      failed batches are queued and re-attempted in a second pass on the
      sticky endpoint until the time budget ends (TFB_SYNC_BATCH_RETRY,
      default ON; 0 restores single-attempt behavior).
  (3) FLOOR/PERSISTENCE DEADLOCK: the v6.18.2 shrink floor vetoes any
      <70%-coverage write to avoid dropping missed symbols — but the
      v6.19.0 persistence pass, which runs IMMEDIATELY AFTER the floor,
      appends the last-good row of every requested-but-missing symbol, so
      a floored partial write cannot drop anything anymore. On a 261-batch
      page the floor therefore guaranteed ZERO server writes forever.
      FIX: the floor now vetoes only when persistence cannot run
      (disabled, or no Sheets handle); otherwise the partial proceeds into
      the persistence merge and WRITES, and heal-first (v6.24.2) turns the
      remaining blanks into a natural cross-leg cursor — Global_Markets
      converges over ~2-3 legs instead of never.
      TFB_SYNC_FLOOR_STRICT=1 restores the unconditional veto.
Nothing removed; guards, KLG, identity firewall, tripwires unchanged.

v6.24.3 fix — UNIVERSE CAPS RAISED FOR THE 12,486-SYMBOL EXPANSION
--------------------------------------------------------------------------
WHY (owner-approved staged expansion, 2026-07-16: Market_Leaders 1,025 /
Global_Markets 6,512 / Commodities_FX 453 / Mutual_Funds 4,496): four
hard bounds in this script bind BELOW the new Global_Markets size and
would act as silent symbol removers / guard blind spots:
  (1) _read_existing_page_symbols read a hardcoded "A1:E5000" block —
      rows past 5000 are PHYSICALLY never read, so symbols there are
      never requested and (per the v6.19.2 lesson) never protected by
      the persistence guard. At 6,512 GM rows that un-requests ~1,512.
  (2) _market_symbol_cap clamped TFB_SYNC_MAX_SYMBOLS_MARKET at
      min(v, 5000) — the workflow already sets 5000, the max; no yml
      edit can pass 6,512 until this code ceiling is raised.
  (3)+(4) the KEEP-LAST-GOOD stub-swap and the v6.19.0 PERSISTENCE
      pass each re-read the live page via "A1:ZZ6000" — at 6,513+ total
      rows, last-good rows past 6000 become INVISIBLE to both guards,
      so tail stubs can't be healed and tail requested-but-missing
      symbols can't be preserved.
  (+) the request-limit ceilings (batched p["limit"] and non-batched
      safe_limit) were min(5000, …) — harmless at batch size 25, but a
      latent truncator if batching is ever disabled on a >5000 page.
FIX: one master switch TFB_SYNC_UNIVERSE_CAP_V2 (default ON; set 0 to
restore EVERY legacy literal above byte-for-byte). Under v2: page
re-reads use an env-tunable row bound TFB_SYNC_PAGE_READ_MAX_ROW
(default 12000, clamped 1000..100000) — the readback block becomes
A1:E{bound}, both guard re-reads become A1:ZZ{bound}; the market cap
ceiling rises to 20000 (default value stays 2500 — yml still drives the
actual cap and MUST be raised to ~7000 to take effect); request-limit
ceilings rise to the same 20000. Sheets API responses only contain the
USED range, so raising the requested bound does not inflate payloads.
Fail-safe: every touched path keeps its exact prior fail-soft behavior
([] / skip / unchanged rows) on any read failure. New helpers:
_universe_cap_v2_enabled(), _page_read_row_bound(),
_request_limit_ceiling(). Nothing removed; all v6.24.2 functions
carried verbatim.

v6.24.2 fix — HEAL-FIRST ROTATION (Fix HF-1): blank-name rows jump the queue
--------------------------------------------------------------------------
EVIDENCE (v42 export, 2026-07-16 morning): the 2026-07-15 repair correctly
stubbed 2,539 identity-corrupt Global_Markets rows (+571 ML). The sync
reads its symbol list back from the page in SHEET ORDER and caps it
(TFB_SYNC_MAX_SYMBOLS_MARKET, default 2500), so each leg spends its whole
budget walking the catalog from the top — healthy rows and stubs alike —
and only ~925 stub rows were revisited in a full day of legs. At that
pace the page needs ~3 more days to converge, and (latent, now visible)
the 1,262 GM symbols beyond the 2,500 cap in a 3,762-row page would sit
at the queue's tail indefinitely. The rotation is fair; healing needs it
to be UNFAIR in the stubs' favor.
FIX — _read_existing_page_symbols now also reads the Name column (already
inside the A1:E5000 block it fetches — zero extra API calls), and, when
TFB_SYNC_HEAL_FIRST is on (DEFAULT), stably partitions the symbol list
BLANK-NAME-FIRST before the max_symbols cap is applied. Every repaired
stub therefore lands inside the very next leg's slice; healthy rows keep
their relative order behind them and resume normal rotation once the
blanks are gone (steady-state behavior identical to v6.24.1, since a
healthy page has ~zero blanks). Telemetry: one
[HEAL-FIRST v6.24.2] '<page>: prioritized N blank-name symbol(s) of M'
logger line whenever the partition moved anything.
ENV: TFB_SYNC_HEAL_FIRST default ON; =0/false/off/no restores the
v6.24.1 sheet-order read byte-identically. New helper:
_heal_first_enabled. ZERO functions removed; all prior WHYs preserved.

v6.24.1 fix — SELF-DIAGNOSIS LAYER: FW-3 LOUD-FAILURE, FW-4 NAME-DEDUP CENSUS, STARTUP SELF-TEST
--------------------------------------------------------------------------
EVIDENCE (v41 morning export, 2026-07-15 audit):
(1) FW-1 healing PROVEN in production (GOOG/HSBA.L/NMM.US/2222.SR all
    restored overnight) — yet ZERO [ID-FIREWALL] verdict lines reached
    _Run_Log: FW-3's only failure channel was a logger.warning nobody can
    see. A tripwire whose own failure is silent violates the discipline it
    exists to enforce.
(2) NEW finding the ratio tests CANNOT catch: 196 Names on >=3 symbols in
    Global_Markets and 55 in Market_Leaders — all stamped TODAY. These are
    self-consistent whole-row transpositions (Price/EPS/PE move together,
    so P/E==Price/EPS holds on the wrong symbol). Root fix is engine-side
    pairing (Build 4); until it lands, the sync needs eyes on the class.
FIX (three cuts):
  FW-3b LOUD VERDICT: the _Run_Log append now retries once and, on final
       failure, emits a GitHub ::warning:: annotation carrying the
       exception class+message — visible on the run page like the
       track_performance notices. A silent tripwire is no longer possible.
  FW-4 NAME-DEDUP CENSUS/QUARANTINE: after FW-2, the OUTGOING batch is
       censused Name->symbols. Any non-blank Name on >=
       TFB_SYNC_NAME_DEDUP_MIN (default 3) distinct symbols is reported in
       the [ID-FIREWALL] verdict as name_dup groups. Mode is env-gated:
       TFB_SYNC_NAME_DEDUP_MODE=observe (DEFAULT: report only — legit
       multi-listings like HSBC on 3 exchanges must be measured before any
       automatic action) | quarantine (carriers stubbed to Symbol-only +
       'identity_quarantined:name_dedup' Warnings tag) | off. The observe
       census gives a daily workbook-visible measurement so the flip to
       quarantine (or the Build-4 fix) is made on data, not guesswork.
  ST-1 STARTUP SELF-TEST: _idfw_selftest_() runs canned fixtures through
       _klg_old_row_identity_ok, _row_identity_firewall and the FW-4
       census before any page is touched (poisoned GOOG row must be
       refused/stripped; healthy+GBX rows must pass; 3-symbol name group
       must be detected). Result is logged as [SELFTEST v6.24.1] PASS k/k
       and included in every FW-3 verdict's details. On ANY failure: a
       ::error:: annotation fires and FW-4 quarantine (the only
       destructive-ish new action) self-disables for the run; FW-1/FW-2
       remain armed (their failure mode is the old behavior, never data
       loss). The system now checks its own guards every run and refuses
       to act on a guard it just proved broken.
ENV: TFB_SYNC_NAME_DEDUP_MODE (observe), TFB_SYNC_NAME_DEDUP_MIN (3).
No other changes; all v6.24.0 behavior and every prior WHY preserved.

v6.24.0 fix — ID-FIREWALL: KLG IDENTITY GATE + OUTGOING ROW STRIP + RUN-LOG VERDICT
--------------------------------------------------------------------------
EVIDENCE (v32 export forensics, 2026-07-14 23:00 audit; D3 closeout):
Market_Leaders rows stamped 21:07-21:23 on 2026-07-13 still carried
poisoned identities (2010.SR named "NiSource Inc.", 1140.SR named "Oracle
Corporation", DD.US and HSBA.L both named "Microsoft Corporation") AFTER
the engine's v5.116.0 guards were armed. MECHANISM: the armed guards
correctly DISCARD poisoned enrichment -> the fetched row arrives as a
data-free stub -> v6.22.3 KEEP-LAST-GOOD certifies the symbol's OLD sheet
row as "good" on price+provider alone and swaps it back in -> the old row
IS the poisoned one -> the write re-publishes and re-stamps the poison.
The guard and the cache cooperate to keep the damage alive. Prevention
without an identity test on the KEPT row can never converge.
FIX (three cuts; each honesty-first and fail-open):
  FW-1 KLG IDENTITY GATE (_klg_old_row_identity_ok, inside
       _keep_last_good_rows): an old row is certifiable as GOOD only if,
       in addition to the v6.22.3 price+provider test, (a) its Name cell
       is non-blank AND (b) when its Price/EPS/PE triple is testable, the
       row passes the SAME single-row P/E == Price/EPS identity L3b runs
       page-wide (tolerance _COH_REL_TOL, GBX/GBP unit band excluded).
       An identity-suspect old row is NOT kept: the fresh stub is written
       instead, which finally lets the next healthy backend fetch replace
       the poisoned cells (self-healing instead of self-preserving).
       Suspects are reported per page via module counter
       _LAST_KLG_ID_SUSPECTS (signature unchanged).
       ENV: TFB_SYNC_KLG_IDENTITY_GATE default ON; =0/false/off/no
       restores the v6.22.3 keep-test byte-identically.
  FW-2 OUTGOING ROW FIREWALL (_row_identity_firewall, applied right
       after the L3b page scan): any OUTGOING row that is testable and
       breaks the same single-row identity beyond the unit band is
       QUARANTINED before it can reach the sheet - every cell except its
       Symbol is blanked and the Warnings column (when present) is set to
       'identity_quarantined:v6.24.0'. Page-level L3b still guards bulk
       transposition; FW-2 stops the sub-threshold trickle (a page 24%%
       poisoned passes L3b's 25%% trip yet still writes hundreds of bad
       rows). Expected strip count on a healthy backend: 0 - this is a
       tripwire that also contains.
       ENV: TFB_SYNC_ROW_ID_FIREWALL default ON; =0 restores v6.23.0.
  FW-3 RUN-LOG VERDICT (_append_runlog_idfirewall): one line per market
       page write into the workbook's _Run_Log -
       [ID-FIREWALL v6.24.0] page | klg_kept=K | klg_suspect_dropped=D
       (syms...) | out_stripped=S (syms...) - so the standing six-gate
       morning audit sees the firewall working (or firing) the next day.
       Best-effort append via the existing SheetsWriter service;
       ENV: TFB_SYNC_IDFW_RUNLOG default ON.
DONE-CRITERION (owner-verifiable in the next export): anchor census
(GOOG, DD.US, HSBA.L, 2010.SR, 1140.SR, JPM, KO, MA, ...) correct after
one repair pass + one sync; [ID-FIREWALL v6.24.0] lines present in
_Run_Log; suspect_dropped decays to 0 across consecutive syncs.
New helpers (6): _klg_identity_gate_enabled, _klg_old_row_identity_ok,
_row_firewall_enabled, _row_identity_firewall, _idfw_runlog_enabled,
_append_runlog_idfirewall (+ counter _LAST_KLG_ID_SUSPECTS,
tag _IDFW_TAG). ZERO functions removed; all prior WHYs preserved.

v6.23.0 fix — L3b UNIVERSAL COHERENCE TRIPWIRE + L3 ANCHOR COVERAGE
- WHY (evening export 2026-07-12, audited row-by-row): Global_Markets came
  back 89.2% CHIMERIC and Mutual_Funds 33% chimeric, both written by the
  07-12 sync. The v6.22.0 L3 anchor tripwire WOULD have caught GM (re-run
  against the live sheet: 15/22 anchors carry a foreign name -> TRIPS), so
  the writer that produced them was NOT v6.22.x. That is a deploy gap, not
  a code gap, and it is fixed by committing + dispatching this file.
  BUT the same audit exposed a REAL hole in L3 itself:

      Market_Leaders   anchors checked=19  ok=19  mismatched=0   passes
      Global_Markets   anchors checked=22  ok=7   mismatched=15  TRIPS
      Commodities_FX   anchors checked=0   <-- ZERO COVERAGE
      Mutual_Funds     anchors checked=0   <-- ZERO COVERAGE

  _IDENTITY_ANCHORS held no commodity/FX/fund pairs, so the two pages it
  could not see were the two it never checked. Mutual_Funds was in fact
  POISONED (BND.US="PLUS Korea Manufacturing Core Alliance Index ETF",
  AGG.US="iShares Residential and Multisector Real Estate ETF",
  VEA.US="iShares Intermediate Muni Income Active ETF"; 10/30 verifiable
  ETFs foreign) and BOTH existing layers were blind to it simultaneously.
  Commodities_FX escaped only because the freeze accidentally protected it.
  Anchor coverage on GM was 22 pairs against 3,762 rows — 0.6%. A curated
  table will always lag the universe; it tripped this time by luck of which
  symbols happen to be listed.

- L3b [COHERENCE-TRIPWIRE] (TFB_SYNC_COHERENCE_TRIPWIRE, default ON;
  TFB_SYNC_COHERENCE_MAX_BAD_PCT default 25; TFB_SYNC_COHERENCE_MIN_ROWS
  default 50): a curation-free detector that needs no anchor table at all.
  Every row states Current Price, EPS (TTM) and P/E (TTM) — and those are
  NOT independent: P/E == Price / EPS. Crucially the three span the two
  payload blocks that the transposition splits: Price comes from the QUOTE
  block (symbol-keyed, verified correct in the audit) while EPS and P/E come
  from the ENRICHMENT block (the block that gets misassigned). So a
  transposed row breaks the identity BY CONSTRUCTION, whatever the symbol,
  whatever the page, with no list to maintain. Measured on the live sheet:

      Global_Markets 07-12 (sync-written)   2,037 / 2,283 incoherent = 89.2%
      Global_Markets 07-08 (sync never hit)     0 /   164 incoherent =  0.0%
      Market_Leaders (GAS-written, clean)       7 /   691 incoherent =  1.0%

  The 1.0% ML residue is entirely the LSE pence convention (price in GBX,
  EPS in GBP -> implied P/E is ~100x stated), so the scan treats an
  implied/stated ratio in [50, 200] as COHERENT and never counts it. With a
  25% trip threshold the separation is 89.2% vs 1.0% — a 3.5x margin either
  side. Fail-safe throughout: a page with fewer than MIN_ROWS testable rows
  is never judged (Commodities_FX has 4, Mutual_Funds has 0 — they cannot
  form the ratio at all and are silently skipped, exactly like a page with
  no anchors present). Rows with a missing/blank/zero/negative EPS or a
  non-positive P/E are skipped, not condemned. On trip: SKIP clear+write and
  PRESERVE last-good rows, identical semantics to L3.

- L3 [ANCHOR COVERAGE] — _IDENTITY_ANCHORS gains 27 commodity/FX/index/fund
  pairs so the two pages L3b structurally cannot cover (no EPS -> no ratio)
  stop being invisible. EVERY added pair was verified against the live
  2026-07-12 export before being written into the table: the 15 CFX pairs
  all currently PASS (that page is clean-but-frozen, so they must not trip),
  and the 12 MF pairs catch the live poisoning (AGG/BND/VEA/XLE/LQD/VNQ/
  SCHD/VIG/VTV/BIL are foreign right now). No invented pairs — an anchor
  whose expected name does not match the provider's real one would cause a
  self-inflicted false trip and block a healthy write.

- ZERO functions removed. L1/L2/L3/L4a/L4b/L4c/L5 all byte-identical.
  TFB_SYNC_COHERENCE_TRIPWIRE=0 restores v6.22.4 behavior exactly.

v6.22.4 fix — TIME-BUDGET GRACEFUL FINISH (L5 DEADLINE-OVER-KILL)
- ROOT CAUSE (GitHub Actions runs #2330/#2331, artifacts read 2026-07-11):
  the global-markets leg fetched ~133 successful backend batches over 69
  minutes (3,652 symbols; 502/500 retry bursts and 60-150s slow batches in
  between) and was then KILLED by the job-level timeout-minutes ceiling —
  BEFORE the single end-of-run sheet write. 69 minutes of good data became
  ZERO rows written, every night since the universe outgrew the ceiling.
  A hard kill is the one failure mode no in-process guard can catch,
  because the process simply stops existing.
- FIX L5 [TIME-BUDGET]: an in-process wall-clock deadline the runner
  respects BEFORE the kill can land. TFB_SYNC_TIME_BUDGET_SEC (default
  0 = disabled, byte-identical v6.22.3 behavior; floor 60 when set)
  measures from process start. Inside the batched market fetch
  (_fetch_market_rows_batched — the production mode):
    (a) BETWEEN batches: budget spent -> stop fetching, tagged warning,
        and PROCEED TO THE WRITE with the accumulated partial. The
        existing machinery then composes safely: the coverage floor
        skips a too-thin write, the v6.19.0 persistence pass preserves
        every non-fetched symbol's row, and L4c keeps error stubs from
        erasing good data. A 90%-fetched Global_Markets now LANDS
        instead of dying at 100%-minus-write.
    (b) BEFORE a page's first batch: budget already spent -> the page is
        skipped whole and the empty-fetch guards preserve it (identical
        to a provider outage). Never interrupts mid-batch; the first
        batch of a page is always allowed so headers can resolve.
  Scope: the batched fetch path only — single-request mode is one HTTP
  call and cannot be usefully interrupted (documented, unchanged).
  Deployment note: set the env in the WORKFLOW (GitHub Actions env/vars,
  not Render) — recommended TFB_SYNC_TIME_BUDGET_SEC=3600 with the job
  ceiling at timeout-minutes: 115, so the runner always finishes and
  writes ~15 min before any kill. New helpers: _time_budget_sec,
  _time_budget_exceeded, _time_budget_left (+ module start anchor;
  3 added, 0 removed, 0 signature changes; every other line verbatim
  v6.22.3).

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

try:
    from scripts.critical_symbol_identity import (
        build_isolated_batches,
        canonicalize_symbol,
        fail_result_on_identity,
        quarantine_critical_rows,
        sanitize_active_universe,
        validate_fresh_critical_rows,
    )
except ModuleNotFoundError:  # direct ``python scripts/run_dashboard_sync.py``
    from critical_symbol_identity import (
        build_isolated_batches,
        canonicalize_symbol,
        fail_result_on_identity,
        quarantine_critical_rows,
        sanitize_active_universe,
        validate_fresh_critical_rows,
    )

# -----------------------------------------------------------------------------
# Version
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.25.2 (2026-07-20) — ST-1 SELFTEST MODE-AWARENESS (R1 defect fix)
# WHY: selftest case 6 asserted the observe-default dedup contract (q2==[]).
# When D-4 armed TFB_SYNC_NAME_DEDUP_MODE=quarantine in daily_sync.yml
# (2026-07-19), _name_dedup_apply correctly quarantined the canned dupes,
# case 6 failed, every run logged selftest=FAIL 5/6, and — because FW-4 is
# gated on _IDFW_SELFTEST_OK — the quarantine feature was DISABLED by its
# own arming on every sync since. Reproduced byte-identically offline.
# FIX: case 6 tests the contract under the CONFIGURED mode (observe /
# quarantine / off) and is stricter in quarantine mode (exact set, symbol
# preserved, name blanked, Warnings tag, non-dupe untouched). No behavior
# change outside the selftest; FW-4 gating logic untouched.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.25.3 (2026-07-20) — FW-4 LEGITIMATE-FAMILY EXEMPTION (urgent, pre-15:00)
# v6.29.0 (2026-07-27): B-4 — FORCE-REFETCH OVERRIDE + LIVE FIREWALL TAG.
# WHY (2026-07-27 morning audit): three Market_Leaders rows carry poisoned
# identities that the guard stack now PROTECTS instead of heals — BK
# ("Hanwha Aerospace", 979,000 "USD"), BRK-B ("Taiwan Semiconductor"),
# FI ("Western Digital"). The poison predates FW-1; whenever the incoming
# fetch for these symbols is a stub, KEEP-LAST-GOOD certifies the poisoned
# predecessor (price+provider+name all "plausible", P/E identity untestable
# or accidentally consistent) and writes it back with a fresh stamp. Three
# stale off-policy .SR rows (8270.SR/4328.SR snapshot 07-10, 3001.SR eodhd
# 07-15) are stuck the same way. FIX, two parts:
#   (1) TFB_SYNC_FORCE_REFETCH_SYMBOLS — comma list of symbols whose OLD
#       sheet row may NEVER be substituted by KEEP-LAST-GOOD this run: the
#       fresh fetch (or a fresh stub, which the next healthy fetch heals)
#       always wins. Blocked substitutions are reported per symbol.
#   (2) [FORCE-REFETCH] report line: for every forced symbol the run logs
#       the INCOMING Name/Price/Provider at WARNING — if a provider is
#       re-sending the wrong instrument (the BK->Hanwha mapping class),
#       the very next run makes it visible instead of silent.
# OPERATOR CONTRACT: set the env for ONE workflow run, verify the report,
# then REMOVE it — an empty/absent list is a byte-exact no-op (default).
# Also: _IDFW_TAG stops lying — the log prefix was hardcoded
# "[ID-FIREWALL v6.24.0]" while the JSON version field moved on; the tag
# now derives from SCRIPT_VERSION. Selftest grows to 9/9 (env parsing +
# an end-to-end stub-grid case proving the forced bypass and the
# unforced path byte-identical behavior).
# KILL: unset TFB_SYNC_FORCE_REFETCH_SYMBOLS (default) -> v6.28.0 exactly.
# -----------------------------------------------------------------------------
# WHY (run #2485 artifacts): with the v6.25.2 selftest fix on main, FW-4
# quarantine arms for the FIRST time at the next leg — and the same run's
# census shows it would blank five LEGITIMATE Global_Markets multi-listing
# families (ONB/ONBPO/ONBPP preferred shares; TTE/TTE.PA/TTE.US; TLK/TLK.US/
# TLKM.JK; Trip.com; Teck) exactly like the ONE true chimera it exists for
# (Goodyear's name on BRK-B + FI + GT.US). FIX: the census exempts a
# same-name group whose symbols reduce to one root / one alphabetic prefix
# (share-class + exchange-suffix strip); chimeras with unrelated roots stay
# doomed. Operator override TFB_SYNC_NAME_DEDUP_EXEMPT_NAMES for families
# the heuristic cannot see (numeric cross-listings). Selftest grows to 7/7
# with a discriminator fixture. KILL: TFB_SYNC_NAME_DEDUP_FAMILY_EXEMPT=0
# -> v6.25.2 census verbatim. A wrongly-blanked row still soft-lands via
# v6.25.1 FW-KEEP last-good restore on the following merge.
# -----------------------------------------------------------------------------
# --------------------------------------------------------------------------
# v6.35.0 — [DECISION-FIRST] CROSS-PAGE DECISION-SYMBOL PRIORITY
# --------------------------------------------------------------------------
# EVIDENCE (2026-08-06 Top_10 audit, exports __10_/__12_): 6 of 10 board
# rows GRACE-held on stale quotes while the overnight legs spent their
# budget on Global_Markets tail rows; the operator's holdings and board
# symbols sit inside a 6,646-row staleness queue with NO cross-page rank.
# OLDEST-FIRST (v6.27.0) is fair across the page; decision symbols need it
# to be UNFAIR in their favor — a 402/quota day must starve the tail, never
# the tradable set. FIX: when TFB_SYNC_PRIORITY_FETCH=1, the union of
# My_Portfolio holdings + Top_10_Investments symbols (+ optional
# TFB_SYNC_PRIORITY_EXTRA csv, capped by TFB_SYNC_PRIORITY_MAX, default
# 150) is stably partitioned to the FRONT of every ranked market page's
# worklist AFTER the oldest-first sort (promoted symbols keep stalest-first
# order among themselves). Read cost: <=2 bounded reads per source page,
# memoized once per run; ANY read failure => feature inert for the run.
# The 00:00Z leg (cron 0 */4) lands right after the EODHD budget reset, so
# with priority ON that leg heals decision symbols first — no separate
# retry subsystem needed. Kill: TFB_SYNC_PRIORITY_FETCH unset/0 (DEFAULT)
# restores the v6.34.0 worklist byte-identically. ZERO functions removed;
# additions: _priority_fetch_enabled, _priority_fetch_max,
# _priority_extra_symbols, _page_symbol_column, _decision_priority_symbols,
# _apply_decision_first.
# --------------------------------------------------------------------------
# v6.37.0 — [PL-1] OPERATOR QUARANTINE LIST (POISON-LOCK BREAKER)
# --------------------------------------------------------------------------
# EVIDENCE (2026-08-08 Global_Markets export, 19:54 stamps): 105 rows sat
# frozen at the 2026-08-07T18:46 cross-contamination while 98.4% of the
# page refreshed the same day. MECHANISM (poison-lock): the engine's
# price-coherence guard compares each HONEST refetch against the poisoned
# last-good (THS.US real ~24 vs stored 1.26 under 'AEye, Inc.' = 19x) and
# rejects the fresh row as insane; persistence then re-keeps the poison —
# the contamination defends itself THROUGH the guard, across the main leg
# AND three FULL-FILL recovery cycles of run 31258514412. PV-3 (v6.36.0,
# armed separately) heals the 50 rows whose stored price breaks its own
# 52W band; the remaining 55 are BAND-COHERENT identity swaps (TAG.DE
# carrying Moog Inc.'s name, price AND band — internally consistent,
# numerically invisible to every screen). FIX: env
# TFB_SYNC_QUARANTINE_SYMBOLS (comma/space list, DEFAULT EMPTY = byte
# no-op) — any FINAL-matrix row whose Symbol is listed becomes the FW-2
# stub shape (Symbol kept, cells blanked,
# Warnings='operator_quarantine_stub:v6.37.0'): L4b still counts the
# symbol PRESENT, OLDEST-FIRST fronts the stub, and the next fetch has NO
# poisoned prior to be compared against — the guard passes the honest row
# and the lock is broken through the NORMAL fetch path. DISTINCT from the
# SUPERSEDED TFB_SYNC_FORCE_REFETCH_SYMBOLS (retired 2026-08-04): that
# forced a fetch-path bypass for an obsolete six-symbol heal; this erases
# the stored prior and lets the standard path heal. OPERATOR CONTRACT
# (v6.29.x precedent): set for ONE workflow run, verify the [PL-1] report
# line, then REMOVE the env. Kill: unset/empty (DEFAULT) -> v6.36.0
# byte-identically. ZERO functions removed; additions:
# _operator_quarantine_symbols, _apply_operator_quarantine.
# --------------------------------------------------------------------------
# v6.36.0 — [PERSIST PV-3] SANITY SCREEN ON SECOND-CHANCE RESTORES
# --------------------------------------------------------------------------
# EVIDENCE (2026-08-08, run 31249231779 global-markets leg): the PV-2
# second-chance pass restored 3,518 row(s) and the restore list LED with
# the 2026-08-07T18:46 cross-contaminated batch (THS.US='AEye, Inc.',
# PE&OLES.MX, PEL.NS, BALN.SW, TATAMOTORS.NS='Mr Price Group' px 17,450
# vs own 52W 627-740…) — the poison lives in the last-good store, so
# every time-starved leg (47% coverage vs the 70% floor that run)
# resurrects it and the sheet can never self-heal. FIX: when
# TFB_SYNC_PERSIST_SANITY=1, restored symbols ONLY are screened with
# definite row-internal breaks (px<=0; inverted 52W band; px outside
# [52wLo*0.99, 52wHi*1.01]) and failures become the FW-2 stub shape —
# Symbol kept, every other cell blanked,
# Warnings='persist_sanity_quarantined:v6.36.0' — so the L4b outcome
# check still counts the symbol PRESENT while OLDEST-FIRST's
# never-stamped lead fronts the stub for a real refetch next leg.
# Healthy restores pass untouched; a missing Symbol or price column
# screens nothing (FAIL-SAFE). NOTE: firewall enforce needs NO code —
# FW-4 already ships TFB_SYNC_NAME_DEDUP_MODE=quarantine (+ MIN=2 for
# pairs like the double 'Moog Inc.'); arming it is a workflow-env act.
# Kill: TFB_SYNC_PERSIST_SANITY unset/0 (DEFAULT) restores v6.35.0
# byte-identically. ZERO functions removed; additions:
# _persist_sanity_enabled, _persist_second_chance_sanity,
# _PSAN_52WH_ALIASES, _PSAN_52WL_ALIASES.
# --------------------------------------------------------------------------
# v6.39.2 (2026-08-17, W1A-4b adjudication F2/F3/F4):
#   F2  _Status stamp writes valueInputOption=RAW — USER_ENTERED let Sheets
#       locale-parse the ISO timestamp (the IR-029 two-formats disease
#       re-entering through the fix). Deterministic strings win; consumers
#       already parse GAS's own mixed formats.
#   F3  stamp suppressed (with a ::notice::) under dry-run and MANUAL-HOLD —
#       a mode whose contract is ZERO workbook writes outranks telemetry.
#       Ordinary skips/vetoes/failures still stamp truthfully (P0-3 kept).
#   F4  _Status key-column read unbounded (A:A, was A1:A200).
#   Plus: the stamp tag interpolates SCRIPT_VERSION (IR-064 inside the
#   touched block). No other regions modified.
# v6.39.3 (2026-08-17, external Three-Script Production Audit adjudicated):
#   P0-1 CONFIRMED against the real class: TaskResult is @dataclass(slots=True)
#        and _stamp_meta was never a declared field -> every task raised
#        AttributeError at the v6.39.1 assignment, before try. FIX: _stamp_meta
#        and dry_run are now declared fields; constructor call sites unchanged
#        (both defaulted). Harness now executes the REAL decorated dataclass —
#        stand-in objects are banned from behavioral tests (they hid this).
#   P1-1 CONFIRMED: headers was first assigned deep inside try while the
#        finally stamp read it -> UnboundLocalError on early exits, swallowed.
#        FIX: headers pre-initialized before try. dry-run suppression now
#        rides the declared res.dry_run, populated from the existing dry_run
#        parameter, plus a marker-text belt in the skip helper.
# v6.39.4 (2026-08-18, W1A-6b — morning audit, adjudicated on primary evidence):
#   EVIDENCE: the 2026-08-18 08:30 workbook export carries ZERO
#   "[OHLC-PREWRITE" lines in _Run_Log across 28,028 rows and every sync leg
#   since the v6.38.0 deploy, while [ID-FIREWALL v6.39.3] verdicts landed on
#   all four pages in the same runs. Two independent defects produced that:
#     D1 (workflow, fixed in daily_sync.yml — NOT in this file): the guard's
#        gate TFB_SYNC_OHLC_PREWRITE was never mapped into either job's env
#        block, and a GitHub repo *Variable* is not injected into the runner
#        environment — it is only reachable through ${{ vars.* }}. The
#        operator armed the Variable on 2026-08-17; os.getenv() still saw
#        nothing, so _ohlc_prewrite_enabled() stayed False and the guard
#        never executed. Same class for TFB_SYNC_STATUS_STAMP (W1A-4b).
#     D2 (this file): even once armed, observe mode's ONLY channel was
#        logger.info/logger.warning -> the ephemeral Actions job log. The
#        operator's enforce decision therefore had no durable evidence
#        surface. This is verbatim the failure the v6.24.1 FW-3b comment
#        already records for ID-FIREWALL ("FW-3's only failure channel was
#        a logger.warning nobody can [see]"), which is why FW-3 was given a
#        _Run_Log appender. The OHLC guard shipped without one.
#   FIX (D2): _append_runlog_ohlc_prewrite mirrors _append_runlog_idfirewall
#        exactly in shape — same 10 _Run_Log columns, same two-attempt retry,
#        same LOUD ::warning:: when the tripwire itself fails, same fail-open
#        contract. Called from inside the EXISTING guard block, so it can only
#        run when TFB_SYNC_OHLC_PREWRITE is already armed.
#   BYTE-BEHAVIOUR: with TFB_SYNC_OHLC_PREWRITE unset/0 (the default, and the
#        default the workflow now passes) this file is behaviourally identical
#        to v6.39.3 — the guard block is not entered, so the new function is
#        never reached. Kill switch inside the armed path:
#        TFB_SYNC_OHLC_RUNLOG=0.
#   _OHLC_PREWRITE_TAG deliberately UNCHANGED at "[OHLC-PREWRITE v6.38.0]":
#        it is a pinned/documented tag (the [SELFTEST v6.25.2] convention) and
#        the operator's observation gate greps for that exact literal.
#   ZERO functions removed; additions: _ohlc_prewrite_runlog_enabled,
#        _append_runlog_ohlc_prewrite.
# v6.39.5 (2026-08-18, external W1A-6 Deployment Audit adjudicated — Claude
# re-executed every numeric/code claim against this file before acceptance):
#   F-08 ACCEPTED (P1) — decision-owned pages could be stamped by a writer
#        that does not own them: the v6.6.0 DECISION-GUARD early return
#        (res.status="skipped", marker appended to warnings) still flows
#        through the v6.39.1 finally-stamp, and _status_stamp_should_skip
#        suppressed only dry-run/MANUAL-HOLD. With TFB_SYNC_STATUS_STAMP=1
#        and an empty allow-list, Top_10_Investments would be overwritten to
#        SKIPPED / rows 0 — clobbering the route-owned cockpit status line.
#        FIX: should_skip now returns "decision-owned" when the
#        _DECISION_GUARD_TAG marker is present in the status/warnings blob
#        (the same blob-inspection convention MANUAL-HOLD already uses).
#        Ownership is enforced in code, not by operator allow-list memory.
#   F-09 PARTIAL ACCEPT (P1) — early-exit stamps lost `requested` because
#        _stamp_meta is populated mainly at the persistence stage. FIX:
#        _status_stamp_row falls back to res.symbols_requested. fresh/
#        preserved stay meta-only (they are persistence-stage facts).
#   F-10 ACCEPTED (P1) — the v6.39.1 CAP_BELOW_UNIVERSE warning lives on
#        _read_symbols(), but the ACTUAL market path is
#        _read_existing_page_symbols() (call sites in _run_one_task), whose
#        heal-first branch slices out[:max_symbols] and whose legacy branch
#        breaks at the cap — both silently. GM 6,626 < cap 7,000 today, so
#        the hole is latent; growth past the cap would re-create the
#        2026-07-03 GM-pinned-at-800 class invisibly. FIX: both branches now
#        emit the SAME pinned "[CAP v6.39.1] CAP_BELOW_UNIVERSE" literal
#        (readback-marked) before truncating, so existing grep tooling
#        catches either path.
#   F-17 ACCEPTED (P2) — stale top banner (v6.32.0) refreshed.
#   F-07/F-13/F-14/F-15 land in scripts/harness_w1a6.py v2.0.0 (enforce
#        contract, _Status writer contract, portability, real T1.2) and in
#        daily_sync.yml (deterministic harness wired into ci-tests,
#        merge-blocking). F-11 (priority fetch) and F-12 (dedup enforce) are
#        operator ENV/wave decisions, NOT code — rejected for this PR.
#   ZERO functions removed. No behaviour change while gates stay OFF.
# v6.41.0 (2026-08-20 evening, W1A-6c — BLANK COUNTERS + POST-WRITE READBACK):
#   WHY. On 2026-08-20 the pre-write guard reported, at the write boundary:
#       [OHLC-PREWRITE] Global_Markets | checked=6627 flagged=5
#   The page exported minutes later was then fed to THIS FILE'S OWN
#   _apply_ohlc_prewrite_guard — same aliases, same _ohlc_prewrite_num, same
#   0.01 tolerance, same P1/P2/P3 chain, lifted verbatim from this commit:
#       checked=9809 flagged=618  (GM 448 / CFX 98 / MF 72 / ML 0)
#   Identical code and identical `checked` counts, 5 versus 618. The
#   predicate is not wrong; the guard is simply not being shown those rows.
#   Corroborated twice over, independently:
#     - the engine-side Fix BC guard, armed and in observe, tagged 5 of 623
#       violating rows in the same export: two detectors at two different
#       layers, in different code, converging on ~5. Two healthy detectors do
#       not independently miss the same 618 rows.
#     - the only ohlc tags on the sheet are 5x
#       "ohlc_incoherent_dropped:range:engine" with NO ":observe" suffix,
#       which the engine's observe path cannot emit (it always appends it).
#       Enforce-era tags, still resident. The rows are persisted, not fresh.
#   MECHANISM (this file, upstream of the guard): _keep_last_good_rows —
#   "replace outgoing error stubs with accepted prior rows" — plus
#   _persist_missing_symbol_rows and the FW-KEEP second pass re-inject PRIOR
#   SHEET ROWS into the outgoing matrix. Blank-cell preservation is NOT the
#   mechanism and was ruled out: write_table passes an explicit "" straight
#   through (an empty string is a str, so both scalar converters return it
#   unchanged) and holds no per-cell keep-old-on-blank branch.
#   THE GAP. Both OHLC guards, the identity firewall and every tripwire in
#   this program inspect the OUTGOING MATRIX. Nothing reads a page back after
#   writing it. So nothing can see the resident rows, and nothing can ever
#   clean them: 27 of 31 burst-era symbols were still violating days later.
#   WHAT THIS VERSION ADDS — two measurements, zero new authority:
#     (a) blank_open / blank_hi / blank_lo in the guard's stats. Until now a
#         cell that parsed to None and a genuinely clean cell both produced
#         no offense, so "checked=6627 flagged=5" could not be read: clean
#         matrix, or 6,000 untestable cells? Now it is a number.
#     (b) _ohlc_readback_verify: after a SUCCESSFUL write, read the page back
#         and re-run the SAME guard on what actually landed. Because the
#         predicate is shared verbatim (and forced to observe for the
#         readback window so it cannot mutate the copy), a prewrite/readback
#         delta can only mean different ROWS — never different rules. The
#         first armed run is a DISCRIMINATOR between the two remaining
#         injection topologies, and either outcome is decisive:
#           delta >> 0  -> resident rows enter BETWEEN this guard and the
#                          landed sheet (the KLG/persist merge block);
#           delta ~= 0  -> this leg writes clean and the lake is
#                          re-established AFTERWARD by another writer
#                          (intraday_refresh / page_refresh_recovery /
#                          manual_refresh legs, or non-sync writes) — the
#                          investigation target moves, with proof.
#         The instrument is hypothesis-neutral; it does not assume its
#         author's favourite answer.
#   DISCIPLINE. The readback is READ-ONLY and DEFAULT OFF. It never mutates
#   rows_matrix, never changes res.status, never blocks a write. It fails
#   open twice (helper + call site). A divergence is EVIDENCE, not a verdict:
#   this version deliberately does not repair the lake, because a repair
#   written before the injection point is confirmed would be a guess.
#   COST (audit F5). Per checked page per run: +1 values.get (readback)
#   AND +1 _Run_Log values.append (<=2 attempts), MATCHED lines included —
#   ~4 reads + ~4 appends per run against a ~7-read/page baseline.
#   AUDIT (2026-08-20 pre-merge, external): F1 certification-integration
#   gap, F2 baseline contract, F3 start-offset range, F4 status taxonomy,
#   F5 cost disclosure — ACCEPTED and remediated in this same uncommitted
#   version; F1's tamper reproduction is now harness suite S7.
#   ENV. ONE new gate, TFB_SYNC_OHLC_READBACK, DEFAULT OFF. Unset keeps
#   v6.40.0 behaviour byte-identical. No existing gate changes meaning.
#   NOT CHANGED: the guard predicate itself, enforce semantics, the fail-
#   closed enforce path, W1A-4a/4b, KLG, persistence, trim, or any write.
#
# v6.40.0 (2026-08-18 evening, W1A-4a — Top_10 EXECUTABLE / NOT_ACTIONABLE):
#   WHY: W1A-4 quick form (spec v1.4.x): Top_10 must fail CLOSED — actionable
#   only when its upstream feed is provably healthy. 4b (v6.39.0-5) made each
#   page leg stamp its own truthful _Status A:J row; 4a is the PRODUCER of the
#   cross-page decision verdict the cockpit consumes. The sync cannot write
#   Top_10 itself (v6.6.0 decision-owned guard — deliberately intact), so the
#   verdict is published to the `_Status` L:M global key-value block, which
#   the GAS cockpit already reads. Consumer (a ~15-line ES5 reader rendering
#   the banner + blanking Ticket/Shares on NOT_ACTIONABLE) is the companion
#   GAS delivery; its contract is fully documented at _UPSTREAM_VERDICT_KEY.
#   MECHANICS: after all legs of a job complete, upsert one `TFB Feed <page>`
#   key per ranked page synced in THIS job (state OK/PARTIAL/STALE_COV/
#   FAILED/SKIPPED + fresh_cov + run id + timestamp), then recompute the
#   composite `TFB Decision Feed` by overlaying this job's pages onto the
#   OTHER pages' last-written keys (matrix legs cover disjoint page sets, so
#   no single leg sees all four). Composite = EXECUTABLE only when every
#   required page's freshest state is OK within TFB_SYNC_VERDICT_MAX_AGE_MIN
#   (default 240) trailing minutes; else NOT_ACTIONABLE:<first reason>.
#   SAFETY: bounded L{r}:M{r} RAW updates ONLY — values.append is BANNED here
#   (it inserts whole rows and would shear the A:J page grid). New keys take
#   the first blank L slot within L1:L60; none free => loud skip. Writer
#   self-checks the block is really the key-value column (a known key such as
#   "Backend URL" must be present) before its first write — no blind writes
#   into a moved layout. Two-attempt retry, fail-open, loud ::warning:: on
#   final failure — FW-3 discipline throughout. Parallel matrix legs race
#   only on the composite row; per-page keys are disjoint by construction and
#   the LAST leg of a cycle recomputes the composite over all fresh keys.
#   GATE: TFB_SYNC_UPSTREAM_VERDICT, DEFAULT OFF — unset/0 keeps v6.39.5
#   byte-behaviour (call site not entered). Companions:
#   TFB_SYNC_VERDICT_PAGES (default: the four ranked pages),
#   TFB_SYNC_VERDICT_MAX_AGE_MIN (default 240). Arming is a separate operator
#   decision AFTER the STATUS_STAMP verdict — never bundled.
#   COMMIT-SAFETY PROOF (why this can land before tonight's observe verdict):
#   the v6.38.0 guard block, the v6.39.4 run-log appender, the v6.39.x
#   _Status stamp block and the _run_one_task guard call-site are BYTE-
#   IDENTICAL to v6.39.5 — verified by sha256 over the extracted regions in
#   the build record. The only executable-path delta is one default-OFF gate
#   check after the stale-skip escalation pass. ZERO functions removed;
#   additions: _upstream_verdict_enabled, _upstream_verdict_pages,
#   _upstream_verdict_max_age_min, _uv_page_state, _uv_parse_value,
#   _uv_compose, _write_upstream_verdict.
# v6.44.1 (2026-08-25, W1A-6f2 — FILL-GUARD hardening, external-review adjudication):
#   Independent review of v6.44.0 (Revised-Scripts Audit, 24 Aug) raised six
#   findings against the new guard; five adjudicated VALID on the primary
#   artifact and fixed here, one deferred to the Register:
#   DS-02 FAIL-CLOSED ENFORCE: a core exception under an armed ENFORCE now
#         RAISES before values.update (write aborted, page keeps last-good,
#         task fails loud). observe/off remain fail-open — telemetry never
#         blocks a run.
#   DS-03 CERTIFICATION: enforce requires selftest state EXACTLY True; a None
#         state lazily runs FG-3 once; anything but True forces observe.
#   DS-04 -O-SAFE SELFTEST: bare asserts replaced with explicit checks that
#         survive python -O; a 4th fixture certifies case-folded headers.
#   DS-05 HEADER NORMALIZATION: guarded-column match is case-insensitive and
#         the runlog line reports cols=found/configured, so a silently-inert
#         guard is visible instead of green.
#   DS-06 COLS ALLOWLIST: TFB_SYNC_OHLC_FILL_GUARD_COLS can only SELECT WITHIN
#         {Open, Day High, Day Low}; foreign tokens are rejected and reported
#         (cols_rejected) — the override can never clear non-OHLC fields.
#   DS-10 TELEMETRY GATE: the FILLGUARD appender honors TFB_SYNC_OHLC_RUNLOG
#         and skips pages with zero guarded columns.
#   DS-07 (symbol-keyed preservation for ALL nullable fields) is REAL but a
#         persistence-layer redesign — Improvement Register, not this hotfix.
# v6.44.0 (2026-08-25, W1A-6f — OHLC blank-cell FILL GUARD at the write seam):
#   WHY (READBACK v6.41.0 + LAKE v6.43.0 forensics, runs of 2026-08-24/25):
#   PREWRITE flagged 0 while READBACK flagged 59 (Mutual_Funds, delta +59) and
#   3 -> 104 (Commodities_FX, delta +101) — on exactly the rows LAKE counted as
#   foreign_open_fill (63 / 107): matrix Open blank, lake populated. MECHANISM
#   (code-confirmed this session): _coerce_jsonable(None) -> None serializes as
#   JSON null, and the Sheets values.update contract SKIPS null cells, leaving
#   the PRIOR cell content standing under the NEW row. With write-then-trim (no
#   pre-clear) any row re-ordering between runs grafts ANOTHER SYMBOL's
#   Open/High/Low onto this row — the cross-symbol Open contamination class
#   (HUF-313.61 family). The injection happens AFTER the PREWRITE check, which
#   is why arming PREWRITE enforce alone can never stop it.
#   FIX (env-gated, DEFAULT OFF => byte-identical v6.43.0 write path):
#     FG-1 _ohlc_fill_guard_core()/_apply(): pure, header-name-scoped pass over
#          the final matrix inside write_table (the single choke point both row
#          paths funnel through). Guarded columns default "Open,Day High,Day
#          Low" (override: TFB_SYNC_OHLC_FILL_GUARD_COLS). observe -> count
#          only, zero mutation; enforce -> substitute "" (an explicit clear)
#          for None in guarded columns ONLY, so an honest blank is written
#          instead of inheriting a foreign value. Every other column keeps the
#          null-skip semantics untouched (deliberate keep-last-good paths,
#          e.g. provider targets, are NOT affected).
#     FG-2 [OHLC-FILLGUARD] one _Run_Log line per page per write (armed runs
#          only) through the proven FW-3 channel — fail-loud-but-fail-open.
#     FG-3 startup selftest: 3 canned fixtures through the REAL core function
#          before any page write; a selftest failure FORCES observe and logs
#          loudly — enforcement never runs on an unproven guard (FW-4 lesson).
#   GATES: TFB_SYNC_OHLC_FILL_GUARD (unset/0 = OFF, guard fully inert);
#          =1/true arms OBSERVE. TFB_SYNC_OHLC_FILL_GUARD_MODE=enforce arms
#          substitution. Both are GitHub-Actions repo Variables (sync
#          namespace). ZERO functions removed; unarmed behaviour byte-identical.
# v6.43.0 (2026-08-23, W1A-6e — foreign-writer attribution + identity refetch):
#   CENSUS VERDICT (adjudicated, this build's premise): every Sheets write in
#   this repository was enumerated and classified — 12 .execute() sites in
#   this file (1 guarded data write via write_table @ the W1A-6 seam; 1
#   headers-only repair; _Run_Log/_Status/verdict telemetry for the rest);
#   scripts/intraday_quote_refresh.py writes ONLY Current Price + Last
#   Updated (2 cells, symbol-keyed — it re-STAMPS rows it never repaired);
#   scripts/repair_stores.py BLANKS cells (dry-gated) and cannot populate;
#   scripts/run_inline_page_recovery.py delegates page writes to THIS script
#   (guarded). CONCLUSION: no in-repo leg can populate Open/Name — the
#   same-day lake divergence (GM: guard flagged=5/blank_open=511 outgoing vs
#   440 violations/blank_open=116 on-sheet, 433 fresh-stamped; ML: 255/255
#   Opens blank outgoing vs fully populated on-sheet) is deposited by an
#   OUT-OF-REPO writer: the GAS cockpit and/or the eodhd-screener cron
#   (22:00–00:50Z, overlapping the 00:00Z leg). Two additive mechanisms:
#   (1) [OHLC-LAKE] pre-write lake probe — reads the live page BEFORE this
#       leg writes, runs the REAL _apply_ohlc_prewrite_guard on the SHEET
#       grid in forced-observe (the proven v6.41.0 readback reuse pattern),
#       then joins lake vs the outgoing matrix by Symbol to count
#       foreign_open_fill (matrix Open blank, lake populated) and
#       foreign_name_diff (both non-blank, normalized different) with
#       examples. One _Run_Log line per page per leg via the FW-3 channel.
#       Read-only by construction; rides the W1A-6 gate
#       (TFB_SYNC_OHLC_PREWRITE); kill-switch TFB_SYNC_OHLC_LAKE=0. This
#       turns tonight's manual TSV adjudication into a per-leg measured
#       number and catches the out-of-repo writer red-handed at 00:00Z.
#   (2) [IDENTITY-REFETCH] heal-first extension — HF-1/HF-2 front blank and
#       fabricated Names, but a row carrying another symbol's REAL Name
#       passes as healthy, is never re-fetched, and KLG preserves the wrong
#       identity forever (the 364-row BLOCKED class). Under
#       TFB_SYNC_IDENTITY_REFETCH=1 (DEFAULT OFF — unset keeps v6.42.0
#       ordering byte-identical), symbols whose non-blank Name is shared by
#       >= TFB_SYNC_NAME_DEDUP_MIN (default 3) distinct symbols on the live
#       page are fronted AFTER stubs, BEFORE healthy rows — a real
#       single-symbol refetch lets the ID-FIREWALL adjudicate fresh identity
#       so BLOCKED heals instead of persisting. Fetch-order-only when armed;
#       never drops, never blanks, cap semantics unchanged.
#   ENV: TFB_SYNC_IDENTITY_REFETCH (NEW, default OFF).
#   Kill-switch (never set unless disabling): TFB_SYNC_OHLC_LAKE=0.
#   data_engine_v2.py's gate untouched (standing constraint).
# v6.42.0 (2026-08-21, W1A-6d — six-gate audit adjudicated on the 16:30 TSVs):
#   WHY: the day's audit re-ran this file's own predicate (verbatim: aliases,
#   _ohlc_prewrite_num, 0.01 tol, P1/P2/P3) over the exported sheets and got
#   GM 347 / CFX 98 / MF 60 flagged (505 total; naive strict-band count 542),
#   against the last recorded write-boundary line "checked=6627 flagged=5".
#   Same conclusion as the v6.41.0 WHY, now with the residual measured: even
#   a perfectly-sighted guard is silent on 38 real rows (9 skipped because a
#   [0,*] band aborts ALL tests; 29 forgiven by the 1% tol) and mis-attributes
#   6 more (P2 short-circuit hides the Open offense). An arming decision read
#   off `flagged`/`open` alone would therefore under-see even after placement
#   is fixed. THIS BUILD IS MEASUREMENT ONLY:
#   (1) three ADDITIVE stats counters — zero_band, tol_excused, open_masked —
#       in _apply_ohlc_prewrite_guard; `checked`/`flagged`/`open`/`price_band`
#       /`range`/blank_* are BYTE-IDENTICAL in meaning and value, so every
#       historical line and the prewrite<->readback delta stay comparable;
#   (2) the write-path log line gains the three counters;
#   (3) companion (separate file, same session): daily_sync.yml maps
#       TFB_SYNC_OHLC_READBACK into BOTH job envs — the v6.41.0 readback was
#       fully built and NEVER wired into the workflow env, the exact v6.39.4
#       failure mode one flag over. Default '0' there; arming is an operator
#       ENV decision per standing policy.
#   NOT CHANGED: the predicate (P1/P2/P3, tol, aliases), `flagged` semantics,
#   enforce behaviour, KLG, trim, readback logic, any write. With every ENV
#   unset the write path is byte-identical to v6.41.0.

# =============================================================================
# v6.45.0 (ONE-PASS R1-R6, operator-approved scope freeze 2026-08-25)
# -----------------------------------------------------------------------------
# Evidence base: 2026-08-25 six-gate audit + adjudicated external audit.
# prewrite flags 0-9 vs readback 300-440 (GM) every run; zero Global_Markets
# "Batch refresh completed" since 2026-08-02 19:13:44 => a second writer is
# always mid-flight; _Status is not a freshness contract; ML .SR price_band
# flags fire only at TASI-open runs (stale band vs live price = data artifact,
# not corruption).
#   R1 [SYNC-HOLD]   backend publishes "backend sync hold until <utc>" into
#      _Sync_Control before write_table and clears it after, reusing the
#      v6.32.0 channel in the REVERSE direction (backend already yields to
#      GAS; now GAS can yield to the backend — GAS half ships separately).
#      ENV TFB_SYNC_WRITE_SENTINEL default OFF => zero reads/writes, byte-
#      identical. TTL (TFB_SYNC_HOLD_TTL_SEC, default 180s) self-heals a
#      crash mid-write. Distinct key norm "backendsyncholduntil" is invisible
#      to _mh_read_hold by construction (proven in harness).
#   R2 [OHLC-REPAIR] on a DIVERGENT readback, restore ONLY the Open/High/Low
#      columns from the in-memory payload (the matrix that just passed the
#      prewrite guard), then re-verify once and log before->after. ENV
#      TFB_SYNC_READBACK_REPAIR default off; token "repair" arms; anything
#      else off. Bounded: one pass per page per run; <=3 column updates.
#   R3/R6 [ENFORCE-CLASSES] TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES (default
#      "open,price_band,range" = today's enforce semantics byte-identical)
#      lets enforce act per offense class. Rationale: ML .SR rows flag
#      price_band only during the TASI session because the band is the prior
#      fetch's — blanking H/L there would degrade a live page for a data-
#      freshness artifact. "open,range" enforces the corruption classes while
#      price_band stays measured-only. Counters/logs unchanged in ALL modes.
#   R4 [STATUS-STAMP+] the A..J stamp message now carries data_status
#      (COMPLETE/PARTIAL), guard counts (pw/rb/repair), and payload sha8 —
#      the mini cohort manifest the W2 certificate consumes. Bounded A..J
#      exactly as before; gate TFB_SYNC_STATUS_STAMP unchanged, still OFF.
#   R5 [RUN-META] every _Run_Log Details JSON this file writes gains
#      run_id + ts_utc via _runlog_meta_json (fail-open). Column-A format
#      deliberately UNCHANGED (both-writer ISO migration = Register item).
# Zero functions removed; additive only; every new behavior ENV-gated with
# defaults preserving v6.44.1 byte-identically.
# =============================================================================
SCRIPT_VERSION = "6.57.0"
# -----------------------------------------------------------------------------
# v6.57.0 (2026-09-03) - NULL-CLEAR SCOPE: the fill guard can clear EVERY column
# -----------------------------------------------------------------------------
# EVIDENCE (Global_Markets export 2026-09-03 00:40 vs engine v5.135.0):
#   - MCHPP.US: the engine's own Upside/Downside % = -0.001 (target ~= price
#     at the boundary) while the sheet's Target Price cell reads 218.364;
#     NBRG.US engine upside 0.001 (target ~10.08) vs cell 1,642.86; 14 rows
#     carry the v5.135.0 "*_rejected_outlier" tag in Warnings (the engine
#     emitted target_price=null) and STILL show a value in the cell.
#   - The values differ from the previous day (MCHPP 129,500 -> 218.364),
#     so they are not this symbol's stale cell either.
#   MECHANISM (the v6.44.0 note below, now measured on non-OHLC columns):
#   values.update SKIPS JSON-null cells; the page is written then trimmed with
#   no pre-clear; rows re-sort between runs; so a null Target Price inherits
#   WHATEVER SYMBOL sat in that row position on the previous write. Over
#   weeks of daily re-sorts nearly every position accumulates SOME stale
#   value: 6,080/6,609 GM rows show a Target Price, 89.9% disagree with the
#   row's own Upside %, 40% are outside [0.25x, 3.0x] of price. The same
#   grafting explains P/E != Price/EPS (47.6%), wrong sectors (KLAC.US =
#   "Basic Materials") and the Top_10 Valuation Sanity carnage. The engine
#   firewall (v5.135.0) cannot fix a cell the writer never touches.
# FIX (env-gated, DEFAULT byte-identical v6.56.2):
#   TFB_SYNC_NULL_CLEAR_SCOPE = ohlc (default) -> guard scope unchanged
#                            = all  -> when the guard is armed, EVERY header
#                                      except Symbol (and TFB_SYNC_NULL_KEEP_COLS,
#                                      CSV, default empty) is guarded: observe
#                                      counts the nulls, enforce writes "" so
#                                      an honest blank replaces the graft.
#   Sheet-side null-skip persistence is no longer a feature: the engine
#   carries its own keep-last-good for targets (engine_target_klg) and the
#   row-level persistence paths (SYMBOL-PERSISTENCE, FW-KEEP) are untouched.
#   EXPECTED ON FIRST ENFORCE: a large one-time blanking of grafted cells
#   (Target Price on most synthetic rows, stale P/E/EPS/sector cells);
#   missing_valuation on the board rises; that is the true state.
#   The FILLGUARD _Run_Log line carries scope=all and the total cleared.
# Functions added: 2 (_null_clear_scope, _null_clear_keep_cols). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.56.2 (2026-09-02) - HOTFIX: v6.56.1 placed the ENV-ECHO try/except between
# the self-test "if" and its "else", so the else bound to the try and the
# "::error:: a guard fixture failed; FW-4 quarantine disabled" annotation
# printed on EVERY run after a successful echo (run 33640922781: 3 false
# errors; FW-4 itself stayed ON - it quarantined ZL=F - the print is log-only).
# The echo now runs before the if/else, which is restored verbatim. Harness
# H-SELFTEST captures stdout of the real self-test and asserts no "::error::".
# ALSO corrected in the v6.56.1 note below: the [OHLC-FILLGUARD] line is a
# _Run_Log (sheet) append only - it never reaches the local log file - so its
# absence from a job log is NOT evidence that the guard was off. ENV-ECHO is
# the only valid arming evidence. Functions added: 0. Removed: 0.
# -----------------------------------------------------------------------------
# v6.56.1 (2026-09-02) - ENV-ECHO: the log proves the arming state every run
# -----------------------------------------------------------------------------
# EVIDENCE: run 33609124633 (11:41) and 33620619118 (14:03) both ran with
# the v6.44 fill guard OFF (no [OHLC-FILLGUARD] line) and v6.56.0 with a
# zero tolerance (MF rb-pw=+1 stamped PARTIAL), while the scheduled 07:00
# run of the same day logged mode=enforce. The operator had "set" the
# Variables; nothing in the log could say whether they reached the job
# (the v6.39.4 failure mode: a Variable that is not in env never reaches
# os.getenv). CHANGE: one INFO line at guard self-test time listing the
# EFFECTIVE value of every armed gate as the script sees it. Log-only.
# Functions added: 1 (_env_echo_line). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.56.0 (2026-09-02) - RB-TOLERANCE: a BOUNDED readback residual is COMPLETE
# -----------------------------------------------------------------------------
# EVIDENCE (runs 33556504158 / 33609124633 and the scheduled 07:00 run of
# 2026-09-02): with the v6.44 fill guard armed in ENFORCE (110 nulls
# cleared on GM), the post-write readback still reported GM rb=72 vs pw=66
# (+6); unarmed, rb=78 vs pw=63 (+15). The residual is a handful of
# illiquid foreign names whose Open the OHLC lake cannot fill (9984.T,
# Z74.SI, 7084.KL ...). _status_data_verdict() stamps DIVERGENT as PARTIAL
# unless the readback REPAIR brought the count back to the prewrite
# baseline - and the repair is forced-observe for the S-1 window. Result:
# 0.1-0.2% of 6,609 rows keep the page PARTIAL, the composite feed
# NOT_ACTIONABLE(partial:GM), the board banner red and every ticket
# "SIZING WITHHELD" - indefinitely, while the board's own per-row gates
# (Quote Freshness, Data Trust) already fence those rows off one by one.
# CHANGE: a DIVERGENT readback whose write-survival delta (rb_flagged -
# pw_flagged) is within a bounded tolerance counts as COMPLETE. The
# tolerance is max(TFB_SYNC_RB_TOL_ROWS, ceil(rb_checked *
# TFB_SYNC_RB_TOL_PCT / 100)). The stamp message carries
# "rb_tol=<tol>(+<delta>)" whenever the tolerance decided the verdict, so
# the residual is never hidden. The readback line, the _Run_Log evidence
# and the acceptance counters are unchanged; only the cohort verdict is.
# GATES: both knobs DEFAULT 0 -> tolerance 0 -> v6.55.0 byte-identical.
# Recommended arming: TFB_SYNC_RB_TOL_PCT=0.25, TFB_SYNC_RB_TOL_ROWS=2
# (GM 17 rows, MF 7, CFX 2, ML 2). Functions added: 3 (_rb_tolerance_env,
# _rb_divergence_tolerated, _rb_tolerance_note). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.55.0 (2026-09-01) - F-11: A FAILED EVIDENCE APPEND FAILS THE JOB
# -----------------------------------------------------------------------------
# WHY: every _Run_Log append channel (ID-FIREWALL, OHLC pre-write/readback/
# repair, SYNC-HOLD lifecycle) swallows a final failure as a ::warning:: and
# the job stays green. That is exactly how the 10,000,000-cell freeze of
# 2026-08-30 hid for 22 hours: pages written, stamps written, zero evidence,
# job green. The capacity key (v6.53.0) now REPORTS the state; this makes the
# run itself unable to report success when its evidence clock did not advance.
# CHANGE: every append channel records its final failure (site tag) in
# _RUNLOG_APPEND_FAILS; main() returns 3 with a named ::error:: AFTER the run
# completed normally (return 0) - page writes are never affected, only the job
# status, which the workflow already maps to "Sync script failed" (no recover
# job fires: that job keys on missing pages, not on the exit code).
# GATE: TFB_SYNC_APPEND_FAIL_IS_ERROR default ON; =0/false/off/no restores the
# v6.54.0 exit code (warnings only). Functions added: 2. Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.54.0 (2026-08-31) - IDENTITY DOMAIN: A NON-TICKER CAN NEVER BE LAST-GOOD
#                        OR LEAVE THE SYNC AS INVEST (audit P0 #3, CFX row 454)
# -----------------------------------------------------------------------------
# EVIDENCE (2026-08-31 export, Commodities_FX last row): Symbol="Copper
# Futures", Name="Commodity", Exchange="USD", Day High 6.4955 < Day Low 6.728,
# Warnings "fetch_failed:HTTP 422", Last Updated 2026-08-13 - and STILL
# INVESTABLE / INVEST (the page's only INVEST row). Mechanism, traced at
# source: the shifted 08-13 write left a Name-like string in the Symbol cell;
# every run since, the backend cannot fetch it (422 stub) and KEEP-LAST-GOOD
# certifies the OLD row as GOOD because the keep-gate tests price, provider,
# a non-blank Name and P/E coherence - none of which asks "is this even a
# ticker?". The engine-side fetch-fail block (armed 08-23) never sees the row
# because the poison is re-installed at the sync seam, not fetched.
# CHANGE (two seams, DOWNGRADE-ONLY, no new cell allocation):
#   (1) KLG Leg 0 (inside the existing TFB_SYNC_KLG_IDENTITY_GATE, default
#       ON): an old row whose Symbol is outside the ticker domain
#       (_klg_symbol_domain_ok: A-Z 0-9 . - = ^ & / only, no whitespace) is a
#       suspect, never last-GOOD. Counted in the existing klg_suspect_dropped
#       telemetry and NAMED on the [ID-FIREWALL] line - zero new plumbing.
#   (2) FALSE-GREEN SCREEN on the FINAL matrix, immediately before the
#       pre-write OHLC guard: a row that is INVEST / INVESTABLE while its
#       Symbol fails the domain OR its Warnings carry fetch_failed is set to
#       DO_NOT_INVEST / BLOCKED, Block Reason gains
#       "sync_false_green:<reasons>", Warnings gains
#       "false_green_blocked:v6.54.0". It NEVER upgrades and touches no
#       other cell. One [FALSE-GREEN] line per page when anything is blocked.
# GATE: TFB_SYNC_FALSE_GREEN_SCREEN, DEFAULT ON (protective, downgrade-only,
# FW-3 precedent); =0/false/off/no restores v6.53.0 byte-identically for (2).
# Functions added: 3 (_klg_symbol_domain_ok, _false_green_screen_enabled,
# _apply_false_green_screen). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.53.0 (2026-08-31) - GRID CAPACITY REACHES THE WORKBOOK (P0-1 recurrence)
# -----------------------------------------------------------------------------
# EVIDENCE (2026-08-31 morning export): both v6.52.0 runs (33339574567,
# 33358823331) wrote every page and _Status stamp yet left ZERO _Run_Log rows;
# the GAS auto-refresh logged nothing for 266 trigger fires (8/30 10:44 ->
# 8/31 08:44) and shadow_board's 05:10Z PERF-VERDICT is absent - the exact
# 2026-08-22 signature of the 10,000,000-cell allocation cap (INSERT_ROWS
# appends fail HTTP 400 while in-place updates succeed and jobs stay green).
# The only capacity instrument (google_sheets_service v6.2.0 [CAPACITY-SVC])
# is a job-log INFO line nobody reads from the workbook, so the recurrence was
# invisible for ~22 hours.
# CHANGE (telemetry only; ONE bounded in-place L:M update, never append):
#   (1) _capacity_allocated(): reads sheets[].gridProperties rowCount x
#       columnCount (what Google's limit counts). Fail-open -> None.
#   (2) _capacity_state()/_capacity_value(): PURE. OK < 85% <= NEAR-LIMIT
#       < 99.5% <= AT-LIMIT; UNKNOWN when metadata is unreadable.
#   (3) _write_upstream_verdict() upserts key "TFB Grid Capacity" =
#       "<STATE> | allocated=N (p%) | free=F | run=<id> | <ts>" through the
#       SAME self-checked _upsert closure as the TFB Feed keys - so the value
#       lands even when the workbook is AT the cap (updates do not allocate).
#       Non-OK states also print a ::warning:: annotation on the run page.
# GATE: TFB_SYNC_CAPACITY_STATUS, DEFAULT ON (protective telemetry ships
# armed, FW-3 precedent); =0/false/off/no restores v6.52.0 byte-identically.
# Rides inside TFB_SYNC_UPSTREAM_VERDICT (already live - the TFB Feed keys
# exist in _Status); when that master gate is OFF nothing here runs.
# Functions added: 4. Removed: 0. Cell writes: 1 (L:M in-place).
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.52.0 (2026-08-30) - RB ATTRIBUTION: NAME THE ROWS, NOT JUST THE COUNT
# -----------------------------------------------------------------------------
# CONTEXT: four runs of paired counters (foreign_open_fill vs rb-open:
# 436/431, 281/413, 376/358, 93/346) prove the lake fill is a real but NOT
# dominant channel of the write-seam divergence - the correlation collapsed
# on 08-30. Closing the seam needs to know WHICH symbols diverge, per run,
# so the lake / GAS / assembly-point decomposition becomes measurable
# instead of argued. The planned lake QUARANTINE is deliberately NOT in this
# version: _ohlc_lake_probe is read-only telemetry and the actual fill
# writer has not been located at source - quarantining a mechanism before
# finding its writer is the exact guess-class this week banned; the item
# stays registered pending that trace.
# CHANGE (pure telemetry, zero cell writes, no gate needed):
#   (1) _ohlc_readback_verify captures the SYMBOL SET where the sheet's Open
#       differs from the matrix Open this leg just wrote (direct matrix-vs-
#       sheet diff on the written column; cap 60). Delta gains
#       open_diff_syms / open_diff_n.
#   (2) The readback console/warning line gains " | attr: SYM1, SYM2, ..."
#       so every run names its offenders - directly comparable with the same
#       run's [OHLC-LAKE] examples to measure overlap off-log.
# This version also CARRIES v6.51.0 (status truth) below, still gated OFF.
# Functions added: 0. Removed: 0. Delta keys added: 2.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.51.0 (2026-08-30) - STATUS TRUTH: THE COHORT VERDICT REACHES THE CELL AND
#                        THE FEED (audit F-05/E-03, P0-3 first half, AT-07)
# -----------------------------------------------------------------------------
# EVIDENCE (runs 33255802873 and 33293471258, both re-verified on the live
# _Status exports): Global_Markets Status cell = SUCCESS and the TFB Decision
# Feed flipped "EXECUTABLE ... GM:OK" while the SAME row's stamp message said
# data=PARTIAL guard=rb:415/6609 (then rb:348). The v6.45.0 R4 arithmetic was
# already honest - it computed data_status correctly on every run - but the
# verdict lived only inside the message string: _status_stamp_row chose the
# Status cell from the leg status alone, and _uv_page_state fed OK to the
# EXECUTABLE composite from the leg status alone. Truth existed; nothing
# consumed it.
# CHANGE (all behind TFB_SYNC_STATUS_TRUTH, DEFAULT OFF = v6.50.0 identical):
#   (1) _status_data_verdict(): the R4 arithmetic factored into ONE helper so
#       the stamp message, the C cell and the feed token can never disagree.
#   (2) Status cell: SUCCESS is demoted to PARTIAL when the cohort verdict is
#       PARTIAL. PARTIAL_FRESH (already non-green) is left as the more
#       specific label.
#   (3) _uv_page_state: a success leg whose cohort verdict is PARTIAL feeds
#       PARTIAL, not OK - _uv_compose then yields NOT_ACTIONABLE(partial:<pg>)
#       with zero changes to the composite itself. STALE_COV keeps precedence
#       as the more specific label.
# ARMING NOTE, stated up front: with today's live rb divergence (GM 348+) the
# armed feed will read NOT_ACTIONABLE(partial:GM) on most runs UNTIL the
# w52/quarantine work closes the seam - that is the intended conservative
# behaviour (AT-07: PARTIAL may never surface as EXECUTABLE), and the reason
# the gate ships OFF: observe v6.50's counters first, arm this second.
# Functions added: 2. Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.50.0 (2026-08-30) - w52_band: THE 52-WEEK INVARIANT ENTERS THE WRITE PATH
# -----------------------------------------------------------------------------
# EVIDENCE (2026-08-30 export, run 33293471258): 73 Global_Markets rows carry a
# Current Price outside their OWN 52W band (TOD.MI 257.52 vs 45.18-56.70;
# VUK.AX 0.1888 vs 51.78-97.14), plus 3 in Commodities_FX and 2 in
# Mutual_Funds - while the same run's guard reported pw:2/6609. Both facts are
# true because the write path never tested the 52W band:
#   * _apply_ohlc_prewrite_guard runs on EVERY row of the FINAL matrix, but its
#     P2 'price_band' test uses the DAY high/low aliases only.
#   * _apply_price_sanity_screen (v6.36.0 PV-3) DOES test
#     px outside [52wLo*0.99, 52wHi*1.01] - but it takes a `restored` set and
#     returns immediately when empty, so it screens only PV-2 second-chance
#     re-injections. Normally-arriving rows are never tested.
# A cross-row index-shift hypothesis was RAISED AND REJECTED before this build:
# a uniform +2 shift explains 49/299 sampled clean rows (chance level),
# neighbour offsets are scattered (+1,-5,-1,+3,-2), and 26 of the 73 fit NO
# neighbour band. The defect is therefore screened on the INVARIANT, not on a
# hypothesised mechanism - which also covers those 26 unattributable rows.
# CHANGE: a fourth offense class 'w52_band' joins the existing P1/P2/P3 chain
# in _apply_ohlc_prewrite_guard, reusing the _PSAN_52WH/52WL aliases already
# defined for PV-3 and the same _ohlc_prewrite_tol() widening. It is:
#   (1) ADDITIVE to `flagged` and reported in the [OHLC-PREWRITE] line and the
#       Details JSON, with its own counters (w52_band, w52_zero_band,
#       w52_tol_excused, w52_absent) so an arming decision reads measurements,
#       not guesses;
#   (2) NOT in the default enforce set. _ohlc_prewrite_enforce_classes keeps
#       returning {open, price_band, range} unless the operator names
#       w52_band explicitly, so enforce-mode mutation is byte-identical to
#       v6.49 until armed - observe first, arm on evidence (§ standing rule);
#   (3) INDEPENDENT of the P1/P2 short-circuit: a row that already failed
#       price_band is still tested against its 52W band, because the two bands
#       answer different questions (session coherence vs annual coherence).
# When armed via TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES, w52_band blanks the
# 52W High/Low cells (never Symbol, never price, never Warnings) and appends
# 'ohlc_incoherent_dropped:w52_band:prewrite' - the FW-2-consistent shape.
# Functions added: 0. Functions removed: 0. Counters added: 4.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.49.0 (2026-08-29) - OWNER-SET LEASE: A FINISHING LEG CAN ONLY REMOVE
#                        ITSELF (external audit F-03; AT-02 monotonicity)
# -----------------------------------------------------------------------------
# GROUND TRUTH (pinned-source review at e5c8ffa + run 33255802873 lifecycle):
# v6.48 publish ADOPTED a live foreign owner token and stored it locally;
# clear then compared the cell owner against that adopted token, matched, and
# shortened the shared hold to now+grace while the real owner was still
# writing. The v6.48 harness passed adopt (C4) and foreign-skip (C3) as
# separate states; the composed cross-leg sequence adopt->clear was the
# untested hole. Today's five hold windows were sequential, so the branch
# never fired in production - latent, not triggered - and it is removed
# outright rather than patched:
#   (1) Column C now carries an owners= ledger: one 'token@expiry' entry per
#       live writer, ';'-separated. Tokens are sanitized to [A-Za-z0-9_.:-]
#       and capped at 64 chars (leg ids arrive as comma-separated
#       TFB_SYNC_PAGE_ORDER). Publish drops expired entries, upserts ONLY
#       its own entry, and never adopts: _SH_STATE['owner'] is always our
#       own token.
#   (2) Column B stays a PURE ISO timestamp (the only cell the deployed GAS
#       parser reads) and always equals max(live expiries). Clear removes
#       its own entry and rewrites B to max(remaining); grace applies only
#       when the ledger empties; grace=0 blanks (v6.47 behaviour).
#       Invariant: at every instant the effective hold >= every live lease.
#   (3) Legacy v6.48 'owner=' cells parse as one live entry expiring at B,
#       so a mixed-version overlap still counts the old writer.
#   (4) Publish verifies its own token in the C read-back and retries the
#       merge once on a lost read-modify-write race. Every path stays
#       FAIL-OPEN: hold bookkeeping must never block the write it protects.
# Functions added: 2 (_sync_hold_parse_owners, _sync_hold_fmt_owners).
# Removed: 0. CLEAR_SKIPPED retires (ledger self-removal replaces it).
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.48.0 (2026-08-29) - OWNERSHIP + POST-WRITE GRACE: THE HOLD BECOMES A
#                        MECHANISM INSTEAD OF A LOTTERY
# -----------------------------------------------------------------------------
# GROUND TRUTH (artifacts of run 33215277102, all four processes): the
# producer worked PERFECTLY - six publishes, six clears, all logged, no
# cross-leg overlap that night. The defect is arithmetic: publish->clear
# spans were 2s/5s/6s/10s/26s/23s while GAS probes chunk boundaries every
# ~5-20s. A 2-26s window against boundary probes is a coin flip per page -
# three nights of zero yields are CONSISTENT with correct code on both
# sides. Fix the geometry, keep the correctness:
#   (1) POST-WRITE GRACE (TFB_SYNC_HOLD_POST_GRACE_SEC, default 45): on
#       success the clear SHORTENS the hold to now+grace instead of
#       blanking it, so a chunk ending seconds after our write still
#       yields before touching the page. 0 = v6.47 immediate blank.
#   (2) OWNERSHIP, suffix-free: column B stays a PURE ISO timestamp (the
#       deployed GAS parser replaces the first space and end-anchors its
#       offset regex - any suffix in B would make it reject the hold, a
#       breakage caught in pre-ship compatibility testing). The owner token
#       "<runid>:<leg>:<pid>" therefore lives in the NOTE column C.
#       _sync_hold_clear refuses to touch a hold whose C-owner is another
#       live process; _sync_hold_publish over a live foreign hold EXTENDS B
#       and PRESERVES the foreign owner in C instead of usurping it. TTL
#       remains the crash backstop for every path.
# Functions added: 3 (_sync_hold_owner_token, _sync_hold_post_grace_sec, _sync_hold_note_owner).
# Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.47.0 (2026-08-29) - SYNC-HOLD BECOMES SELF-EVIDENCING
# -----------------------------------------------------------------------------
# FORENSIC WHY: three consecutive nights produced zero coordinator yields
# while write-seam divergence persisted (GM rb 392/403/404/440). The producer
# is PROVEN to have published at least once (the undated "cleared" note in
# _Sync_Control) - but nothing records WHEN a publish happened, whether last
# night's publish succeeded, or what the cell held during the 03:05-03:07
# GAS/backend overlap. That evidence lives only in the GitHub Actions console
# - invisible in every workbook export - the same artifact-blindness class
# that already cost four acceptance gates this week.
# CHANGES (pure observability; hold semantics byte-identical):
#   (1) NEW _append_runlog_sync_hold(): one _Run_Log row per publish, clear,
#       and FAILURE (cloned from _append_runlog_manual_hold; fail-open).
#   (2) _sync_hold_publish(): after the update, READS THE CELL BACK and logs
#       "verified=<cell contents>" - a publish that did not land can never
#       again masquerade as one that did.
#   (3) Cell notes now carry ISO timestamps (held/cleared <iso>), so
#       _Sync_Control itself becomes a dated witness.
# Functions added: 1 (_append_runlog_sync_hold). Removed: 0.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# v6.46.0 (2026-08-27) — _STATUS TIMESTAMPS CARRY AN EXPLICIT UTC OFFSET
# -----------------------------------------------------------------------------
# FORENSIC WHY (confirmed to the minute, 2026-08-27 morning session): the
# _Status page stamps (col B) and the L/M feed-verdict keys were written as
# NAIVE runner-local wall-clock ("2026-08-27 03:11:19", TZ=Asia/Riyadh in the
# workflow). The GAS consumer dt10UvParse_ v1.10.2 resolves a naive stamp
# under BOTH clocks and keeps the smallest plausible age — which picks the
# UTC misreading for any verdict older than 180 minutes, understating feed
# age by exactly the Riyadh offset forever after (measured: banner 173m vs
# true 353m; effective staleness window 660m against a declared 480m). Root
# cause is the PRODUCER emitting an ambiguous timestamp; a consumer cannot
# disambiguate what the producer never stated.
# FIX (this file only, two write sites + one parser):
#   (1) _status_ts_str(): local wall-clock + explicit offset computed from
#       the runner ("2026-08-27 03:11:19+03:00"). Human-readable, and the
#       offset is derived, not hardcoded, so a workflow TZ change cannot
#       reintroduce the lie.
#   (2) _status_stamp_row col B and the upstream-verdict `ts` now use it.
#   (3) _uv_parse_value tolerates both formats (offset stripped via [:19]),
#       so this job keeps reading rows written by v6.45.0 and by itself.
# CONSUMER IMPACT, verified against live sources before this edit:
#   - dt10UvParse_ v1.10.2 regex is UNANCHORED (/(\d{4})-.../): it ignores
#     the suffix, so board behavior is BYTE-IDENTICAL until the GAS Batch-2
#     parser lands and starts honoring the offset. Fix-enabling, not
#     behavior-changing.
#   - Coordinator tfbBackendHoldParse_ has explicit hasTz handling: suffix OK.
#   - send_digest._parse_ts and this file's _mh reader slice [:19]: suffix OK.
# Functions added: 1 (_status_ts_str). Functions removed: 0.
# -----------------------------------------------------------------------------


def _status_ts_str() -> str:
    """Local wall-clock with an explicit UTC offset, e.g.
    '2026-08-27 03:11:19+03:00'. The offset is computed from the runner's
    actual zone so the string is self-describing wherever the job runs."""
    try:
        off = -(time.altzone if time.daylight and time.localtime().tm_isdst
                else time.timezone)
        sign = "+" if off >= 0 else "-"
        off = abs(int(off))
        suffix = "%s%02d:%02d" % (sign, off // 3600, (off % 3600) // 60)
    except Exception:
        suffix = ""
    return time.strftime("%Y-%m-%d %H:%M:%S") + suffix

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
    # v6.39.3 (audit P0-1/P1-1): declared so the slots contract holds.
    dry_run: bool = False
    _stamp_meta: Dict[str, Any] = field(default_factory=dict)

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

        # v6.44.1 (W1A-6f2) OHLC-FILLGUARD at the single write choke point.
        # Unarmed/observe NEVER raises (same object untouched when off).
        # ENFORCE fails CLOSED: an exception here aborts this write, the page
        # keeps its last-good content, and the task fails loud (DS-02).
        _fg_stats = None
        matrix, _fg_stats = _ohlc_fill_guard_apply(hdr, matrix)

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

        # v6.44.0 (W1A-6f) FG-2: one FILLGUARD line per page per write.
        try:
            if _fg_stats:
                _append_runlog_ohlc_fillguard(self, spreadsheet_id,
                                              sheet_name, _fg_stats)
        except Exception:
            pass

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


def _heal_first_enabled() -> bool:
    """v6.24.2 (HF-1): put blank-name (repaired/stub) rows at the FRONT of a
    market page's refresh order so the sync's per-leg budget heals damage
    before re-polishing healthy rows. Default ON; TFB_SYNC_HEAL_FIRST=0/
    false/off/no restores the v6.24.1 sheet-order read byte-identically."""
    return (os.getenv("TFB_SYNC_HEAL_FIRST") or "1").strip().lower() not in {"0", "false", "off", "no"}


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
    # v6.24.3: rows past the read block are PHYSICALLY invisible — the classic
    # symbol remover. Bound is env-tunable under v2; legacy literal preserved.
    _rb_block = f"A1:E{_page_read_row_bound()}" if _universe_cap_v2_enabled() else "A1:E5000"
    grid = sheets.read_values(spreadsheet_id, sheet_name, _rb_block)
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

    # v6.24.2 (HF-1): heal-first partition. The Name column lives inside the
    # same A1:E5000 block we already hold, so prioritizing blank-name rows
    # costs nothing. Partition happens BEFORE the cap so every stub fits in
    # the very next leg's slice; the kill-switch path below preserves the
    # v6.24.1 single-pass capped read byte-identically.
    name_i = _guard_find_col(
        grid[hdr_r] if isinstance(grid[hdr_r], list) else [],
        _GUARD_NAME_ALIASES,
    )
    if _heal_first_enabled() and name_i >= 0:
        blanks: List[str] = []
        named: List[str] = []
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
            if t in seen:
                continue
            seen.add(t)
            nm = row[name_i] if name_i < len(row) else ""
            # v6.31.0 HF-2: fabricated '<Page> <Symbol>' names are
            # poison, not identity — treat them as blank so the row
            # jumps the heal queue and gets refilled.
            _nm_blankish = _guard_is_blank(nm) or (
                _placeholder_guard_enabled() and _name_is_fabricated(nm)
            )
            (blanks if _nm_blankish else named).append(t)
        if blanks:
            logger.info(
                "[HEAL-FIRST v6.24.2] %s: prioritized %d blank-name symbol(s) "
                "of %d — repaired stubs jump the refresh queue.",
                sheet_name, len(blanks), len(blanks) + len(named),
            )
        # v6.43.0 (W1A-6e) IDENTITY-REFETCH: HF-1/HF-2 front blank and
        # fabricated Names, but a row wearing another symbol's REAL Name
        # passes as healthy, never re-fetches, and KLG preserves the wrong
        # identity forever. When armed, hoist over-assigned-Name carriers
        # (same non-blank Name on >= _name_dedup_min() distinct symbols —
        # the ID-FIREWALL / repair_stores B3 signal) to the front AFTER
        # true stubs, so a real single-symbol refetch lets the firewall
        # adjudicate fresh identity and BLOCKED heals instead of
        # persisting. DEFAULT OFF => `suspects` stays empty and the line
        # below is byte-equivalent to v6.42.0's `blanks + named`.
        suspects: List[str] = []
        if _identity_refetch_enabled():
            _susp, _gN = _identity_suspect_symbols(grid, hdr_r, sym_i, name_i)
            if _susp:
                suspects = [t for t in named if t in _susp]
                named = [t for t in named if t not in _susp]
                logger.info(
                    "%s %s: fronted %d identity-suspect symbol(s) across "
                    "%d duplicate-name group(s) (same non-blank Name on "
                    ">= %d symbols) — refetch lets the ID-FIREWALL "
                    "adjudicate fresh identity.",
                    _IDENTITY_REFETCH_TAG, sheet_name, len(suspects), _gN,
                    _name_dedup_min())
        out = blanks + suspects + named
        if max_symbols > 0 and len(out) > max_symbols:
            # v6.39.5 (F-10): same pinned literal as _read_symbols so one
            # grep covers both truncation sites; "readback" marks the path.
            print("::warning::[CAP v6.39.1] CAP_BELOW_UNIVERSE on %s "
                  "(readback/heal-first): page has %d usable symbols, "
                  "cap=%d — overflow is UN-REQUESTED this leg. Raise "
                  "TFB_SYNC_MAX_SYMBOLS_MARKET to cover the universe."
                  % (sheet_name, len(out), max_symbols))
            logger.warning("[CAP v6.39.1] CAP_BELOW_UNIVERSE %s readback "
                           "usable=%d cap=%d", sheet_name, len(out),
                           max_symbols)
            out = out[:max_symbols]
        return out

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
            # v6.39.5 (F-10): loud on the legacy (heal-first OFF) branch too.
            print("::warning::[CAP v6.39.1] CAP_BELOW_UNIVERSE on %s "
                  "(readback/legacy): symbol read stopped at cap=%d — "
                  "overflow is UN-REQUESTED this leg. Raise "
                  "TFB_SYNC_MAX_SYMBOLS_MARKET to cover the universe."
                  % (sheet_name, max_symbols))
            logger.warning("[CAP v6.39.1] CAP_BELOW_UNIVERSE %s readback "
                           "cap=%d", sheet_name, max_symbols)
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


def _persist_v2_enabled() -> bool:
    """v6.34.0 master switch (PV-1/2/3). Default ON; TFB_SYNC_PERSIST_V2=0
    restores v6.33.0 behavior byte-for-byte."""
    return (os.getenv("TFB_SYNC_PERSIST_V2") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _page_old_name_map(sheets, spreadsheet_id: str, sheet_name: str):
    """v6.34.0 PV-3: {SYMBOL: name_is_blank(bool)} from the live page — the
    guard's evidence of what a skipped write would actually lose. Fail-safe:
    {} on any read/locate problem (guard then behaves as v6.33.0)."""
    out: dict = {}
    try:
        blk = f"A1:ZZ{_page_read_row_bound()}" if _universe_cap_v2_enabled() else "A1:ZZ6000"
        grid = sheets.read_values(spreadsheet_id, sheet_name, blk) if sheets is not None else None
        if not grid or not isinstance(grid, list):
            return out
        hdr_r, sym_i, name_i = -1, -1, -1
        for r in range(min(len(grid), 25)):
            row = grid[r] if isinstance(grid[r], list) else []
            si = _guard_find_col(row, _GUARD_SYMBOL_ALIASES)
            if si >= 0:
                hdr_r, sym_i = r, si
                name_i = _guard_find_col(row, _GUARD_NAME_ALIASES)
                break
        if sym_i < 0:
            return out
        for row in grid[hdr_r + 1:]:
            if not isinstance(row, list) or sym_i >= len(row) or _guard_is_blank(row[sym_i]):
                continue
            t = str(row[sym_i]).strip().upper()
            blank_name = (name_i < 0 or name_i >= len(row)
                          or _guard_is_blank(row[name_i])
                          or _name_is_fabricated(row[name_i]))
            if t not in out:
                out[t] = blank_name
    except Exception:
        return {}
    return out


def _unpersisted_missing(
    headers: List[Any],
    rows_matrix: List[List[Any]],
    requested_symbols: List[str],
    old_name_map: Optional[dict] = None,
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
    if old_name_map and _persist_v2_enabled():
        # v6.34.0 PV-3: an absent symbol with NO old row, or an old row whose
        # identity is blank/fabricated, loses nothing if the write proceeds.
        real = [t for t in out if old_name_map.get(t) is False]
        exempt = len(out) - len(real)
        if exempt:
            logger.warning(
                "[PERSIST v6.34.0] hard-guard scope: absent_total=%s "
                "absent_blank_exempt=%s counted=%s", len(out), exempt, len(real))
        return real
    return out


_KEEP_LAST_GOOD_TAG = "[v6.22.3 KEEP-LAST-GOOD]"
# v6.29.0 B-4: the tag derives from SCRIPT_VERSION — it was frozen at
# "v6.24.0" while the engine moved on, and every audit had to cross-check
# the JSON version field to learn the truth.
_IDFW_TAG = f"[ID-FIREWALL v{SCRIPT_VERSION}]"
_FORCE_REFETCH_TAG = "[FORCE-REFETCH v6.29.0]"
# v6.24.0 FW-1: per-page list of symbols whose OLD sheet row was refused
# certification by the identity gate this pass (read by the call site for
# the warnings line + FW-3 verdict; single-threaded sync loop).
_LAST_KLG_ID_SUSPECTS: list = []
# v6.29.0 B-4: per-page list of forced symbols whose old-row substitution
# was blocked this pass (read by the call site for the report line).
_LAST_KLG_FORCED: list = []


def _force_refetch_symbols() -> set:
    """v6.29.0 B-4: parse TFB_SYNC_FORCE_REFETCH_SYMBOLS into an UPPER-cased
    symbol set. Absent/empty -> empty set (byte-exact v6.28.0 behavior).
    One-run operator tool: set for a single workflow dispatch, then remove."""
    raw = os.getenv("TFB_SYNC_FORCE_REFETCH_SYMBOLS") or ""
    return {t.strip().upper() for t in raw.split(",") if t.strip()}


def _klg_identity_gate_enabled() -> bool:
    """v6.24.0 FW-1: an old row must also pass an identity test (non-blank
    Name + single-row P/E==Price/EPS when testable) before KEEP-LAST-GOOD
    may certify it as GOOD. Default ON - the live failure class (guards
    discard poison, KLG restores the poisoned predecessor, 2026-07-13
    21:07-21:23 stamps) violates it. TFB_SYNC_KLG_IDENTITY_GATE=0/false/
    off/no restores the v6.22.3 keep-test byte-identically."""
    return (os.getenv("TFB_SYNC_KLG_IDENTITY_GATE") or "1").strip().lower() not in {"0", "false", "off", "no"}


_KLG_SYMBOL_DOMAIN_RE = re.compile(r"^[A-Z0-9^][A-Z0-9.\-=^&/]{0,23}$")


def _klg_symbol_domain_ok(t: Any) -> bool:
    """v6.54.0 Leg 0 (PURE): a KEEP-LAST-GOOD candidate / a green row must be
    a TICKER: upper-case letters, digits and the venue punctuation used by
    the universe (. - = ^ & /), 1-24 chars, no whitespace. "COPPER FUTURES"
    (a Name that landed in the Symbol cell on 2026-08-13) fails; every
    symbol in the live 9,791-row universe passes (proved in the harness)."""
    s = str(t or "").strip().upper()
    if not s or any(ch.isspace() for ch in s):
        return False
    return bool(_KLG_SYMBOL_DOMAIN_RE.match(s))


_FG_TAG = "[FALSE-GREEN v6.54.0]"
_FG_INVEST_ALIASES = frozenset({
    "investabilitystatus", "investability", "investable", "investstatus",
})
_FG_WARN_ALIASES = frozenset({"warnings", "warning", "flags", "rowwarnings"})
_FG_FETCHFAIL_RE = re.compile(r"fetch_failed", re.IGNORECASE)


def _false_green_screen_enabled() -> bool:
    """v6.54.0 kill-switch. DEFAULT ON; TFB_SYNC_FALSE_GREEN_SCREEN=0/false/
    off/no restores v6.53.0 byte-identically (matrix untouched)."""
    return (os.getenv("TFB_SYNC_FALSE_GREEN_SCREEN") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _apply_false_green_screen(headers: list, rows_matrix: list,
                              page: str) -> tuple:
    """v6.54.0 DOWNGRADE-ONLY final-matrix screen. A row that is INVEST or
    INVESTABLE while (a) its Symbol is outside the ticker domain or (b) its
    Warnings carry fetch_failed becomes DO_NOT_INVEST / BLOCKED with a named
    Block Reason and Warnings token. Never upgrades; no other cell touched.
    Returns (rows_matrix, stats)."""
    stats = {"checked": 0, "blocked": 0, "domain": 0, "fetchfail": 0,
             "examples": []}
    hdr = [str(h or "") for h in (headers or [])]
    si = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    ai = _guard_find_col(hdr, _ACTION_COL_ALIASES)
    ii = _guard_find_col(hdr, _FG_INVEST_ALIASES)
    bi = _guard_find_col(hdr, _BLOCK_COL_ALIASES)
    wi = _guard_find_col(hdr, _FG_WARN_ALIASES)
    if si < 0 or (ai < 0 and ii < 0):
        return rows_matrix, stats
    for r in (rows_matrix or []):
        if not isinstance(r, list) or si >= len(r) or _guard_is_blank(r[si]):
            continue
        stats["checked"] += 1
        act = str(r[ai]).strip().upper() if 0 <= ai < len(r) else ""
        inv = str(r[ii]).strip().upper() if 0 <= ii < len(r) else ""
        if act != "INVEST" and inv != "INVESTABLE":
            continue
        reasons = []
        if not _klg_symbol_domain_ok(r[si]):
            reasons.append("identity_domain")
        warn = str(r[wi]) if 0 <= wi < len(r) else ""
        if _FG_FETCHFAIL_RE.search(warn):
            reasons.append("fetch_failed")
        if not reasons:
            continue
        need = max(ai, ii, bi, wi)
        if need >= len(r):
            r.extend([""] * (need + 1 - len(r)))
        if ai >= 0:
            r[ai] = "DO_NOT_INVEST"
        if ii >= 0:
            r[ii] = "BLOCKED"
        if bi >= 0:
            _prev = str(r[bi] or "").strip()
            _tag = "sync_false_green:" + "+".join(reasons)
            r[bi] = _tag if not _prev else f"{_prev}; {_tag}"
        if wi >= 0:
            _prev = str(r[wi] or "").strip()
            _tok = "false_green_blocked:v6.54.0"
            r[wi] = _tok if not _prev else f"{_prev}; {_tok}"
        stats["blocked"] += 1
        stats["domain"] += int("identity_domain" in reasons)
        stats["fetchfail"] += int("fetch_failed" in reasons)
        if len(stats["examples"]) < 20:
            stats["examples"].append(str(r[si]).strip())
    return rows_matrix, stats


def _klg_old_row_identity_ok(
    row: list,
    name_i: int,
    px_i: int,
    eps_i: int,
    pe_i: int,
) -> bool:
    """v6.24.0 FW-1: single-row identity check for a KEEP-LAST-GOOD
    candidate. Mirrors _coherence_scan's per-row rules exactly (testability
    gate, _COH_REL_TOL, GBX/GBP unit band) so the two layers can never
    disagree about the same row. FAIL-OPEN: an untestable row (missing any
    of the triple, |EPS|<0.01, non-positive stated P/E) passes the ratio
    leg - only the Name leg is unconditional."""
    def _cell(i: int):
        return row[i] if (0 <= i < len(row)) else ""
    # Leg 1 (unconditional): a nameless old row is a stub, not last-GOOD.
    if name_i >= 0 and _guard_is_blank(_cell(name_i)):
        return False
    # Leg 1b (unconditional, v6.33.0 P0-2): a FABRICATED name ("<Page>
    # <Symbol>" pattern) is poison, not last-GOOD — closes the resurrection
    # path where FW-5 strips the fresh fabrication and FW-KEEP/KLG then
    # restores the old one. Same central matcher as FW-5/HF-2.
    if name_i >= 0 and _name_is_fabricated(_cell(name_i)):
        return False
    # Leg 2 (when testable): the row must agree with itself.
    if min(px_i, eps_i, pe_i) < 0:
        return True
    px = _coh_float(_cell(px_i))
    eps = _coh_float(_cell(eps_i))
    pe = _coh_float(_cell(pe_i))
    if px is None or eps is None or pe is None:
        return True
    if abs(eps) < 0.01 or pe <= 0.0 or px <= 0.0:
        return True
    implied = px / eps
    if implied <= 0.0:
        return True
    rel = abs(implied - pe) / abs(pe)
    if rel < _COH_REL_TOL:
        return True
    ratio = implied / pe
    if _COH_FX_UNIT_LO <= ratio <= _COH_FX_UNIT_HI:
        return True  # pence/pound convention - healthy
    return False


def _fw_keep_last_good_enabled() -> bool:
    """v6.25.1: after FW-2 strips a row, restore that symbol's last-good
    sheet row (targeted KLG swap) instead of leaving a destructive stub.
    Default ON; TFB_SYNC_FW_KEEP_LAST_GOOD=0/false/off/no restores the
    v6.24.0 destructive-stub behavior exactly."""
    return (os.getenv("TFB_SYNC_FW_KEEP_LAST_GOOD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _row_firewall_enabled() -> bool:
    """v6.24.0 FW-2: quarantine OUTGOING rows that individually break the
    P/E==Price/EPS identity before they reach the sheet. Default ON;
    TFB_SYNC_ROW_ID_FIREWALL=0/false/off/no restores v6.23.0 (page-level
    L3b only) byte-identically."""
    return (os.getenv("TFB_SYNC_ROW_ID_FIREWALL") or "1").strip().lower() not in {"0", "false", "off", "no"}


# ---------------------------------------------------------------------------
# v6.31.0 FW-5: FABRICATED-PLACEHOLDER TRIPWIRE (WHY: see v6.31.0 header)
# ---------------------------------------------------------------------------
_FABRICATED_NAME_PAGES = (
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
    "My_Portfolio", "Top_10_Investments", "Insights_Analysis",
)
_FABRICATED_NAME_RE = re.compile(
    r"^(?:%s)\s+\S" % "|".join(_FABRICATED_NAME_PAGES)
)
_FABRICATED_PROVIDER_TOKEN = "placeholder_fallback"
_FAB_QUARANTINE_TAG = "identity_quarantined:fabricated_placeholder:v6.31.0"


def _placeholder_guard_enabled() -> bool:
    """v6.31.0: master switch for FW-5 + the HF-2 heal-first extension.
    Default ON (identity guards armed by default); TFB_SYNC_PLACEHOLDER_GUARD
    =0/false/off/no restores v6.30.0 behavior byte-identically."""
    return (os.getenv("TFB_SYNC_PLACEHOLDER_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _name_is_fabricated(value) -> bool:
    """True iff a Name cell matches the route placeholder fabrication
    pattern '<Page> <Symbol>' (e.g. 'Global_Markets HELN.SW'). No real
    instrument name begins with an underscored page token + whitespace."""
    try:
        s = str(value or "").strip()
    except Exception:
        return False
    return bool(s) and bool(_FABRICATED_NAME_RE.match(s))


def _fabrication_tripwire(
    headers: list,
    rows_matrix: list,
) -> tuple:
    """v6.31.0 FW-5: blank every cell except Symbol on each OUTGOING row that
    is fabricated placeholder output (Name matches '<Page> <Symbol>' OR the
    Data Provider cell contains 'placeholder_fallback'); the Warnings column
    (when locatable) is set to the FW-5 quarantine tag. Mutates rows in place.
    Returns (rows_matrix, stripped_symbols). FAIL-SAFE: missing Symbol column
    -> nothing stripped; missing Name AND Provider columns -> nothing
    stripped. v4.15.0 honest stubs ('no_data_stub'/'placeholder_stub') are
    never matched."""
    stripped: list = []
    if not headers or not rows_matrix:
        return rows_matrix, stripped
    hdr = list(headers)
    sym_i = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    if sym_i < 0:
        return rows_matrix, stripped
    name_i = _guard_find_col(hdr, _GUARD_NAME_ALIASES)
    prov_i = -1
    warn_i = -1
    for i, h in enumerate(hdr):
        hh = str(h or "").strip().casefold()
        if prov_i < 0 and hh in {"data provider", "data_provider", "provider"}:
            prov_i = i
        if warn_i < 0 and hh == "warnings":
            warn_i = i
    if name_i < 0 and prov_i < 0:
        return rows_matrix, stripped
    for r_i, row in enumerate(rows_matrix):
        if not isinstance(row, list) or sym_i >= len(row):
            continue
        if _guard_is_blank(row[sym_i]):
            continue
        fab = False
        if name_i >= 0 and name_i < len(row) and _name_is_fabricated(row[name_i]):
            fab = True
        if not fab and prov_i >= 0 and prov_i < len(row):
            try:
                if _FABRICATED_PROVIDER_TOKEN in str(row[prov_i] or "").casefold():
                    fab = True
            except Exception:
                pass
        if not fab:
            continue
        sym = str(row[sym_i]).strip().upper()
        blanked = ["" for _ in row]
        blanked[sym_i] = row[sym_i]
        if 0 <= warn_i < len(blanked):
            blanked[warn_i] = _FAB_QUARANTINE_TAG
        rows_matrix[r_i] = blanked
        stripped.append(sym)
    return rows_matrix, stripped


def _row_identity_firewall(
    headers: list,
    rows_matrix: list,
) -> tuple:
    """v6.24.0 FW-2: blank every cell except Symbol on each OUTGOING row
    that is testable and identity-broken (same per-row rules as
    _coherence_scan); the Warnings column (when locatable) is set to
    'identity_quarantined:v6.24.0'. Mutates rows in place. Returns
    (rows_matrix, stripped_symbols). FAIL-SAFE: any missing column of the
    triple -> nothing is ever stripped."""
    stripped: list = []
    if not headers or not rows_matrix:
        return rows_matrix, stripped
    hdr = list(headers)
    px_i = _guard_find_col(hdr, _COH_PRICE_ALIASES)
    eps_i = _guard_find_col(hdr, _COH_EPS_ALIASES)
    pe_i = _guard_find_col(hdr, _COH_PE_ALIASES)
    sym_i = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    if min(px_i, eps_i, pe_i, sym_i) < 0:
        return rows_matrix, stripped
    warn_i = -1
    for i, h in enumerate(hdr):
        if str(h or "").strip().casefold() == "warnings":
            warn_i = i
            break
    hi = max(px_i, eps_i, pe_i, sym_i)
    for r_i, row in enumerate(rows_matrix):
        if not isinstance(row, list) or len(row) <= hi:
            continue
        if _guard_is_blank(row[sym_i]):
            continue
        px = _coh_float(row[px_i])
        eps = _coh_float(row[eps_i])
        pe = _coh_float(row[pe_i])
        if px is None or eps is None or pe is None:
            continue
        if abs(eps) < 0.01 or pe <= 0.0 or px <= 0.0:
            continue
        implied = px / eps
        if implied <= 0.0:
            continue
        rel = abs(implied - pe) / abs(pe)
        if rel < _COH_REL_TOL:
            continue
        ratio = implied / pe
        if _COH_FX_UNIT_LO <= ratio <= _COH_FX_UNIT_HI:
            continue
        sym = str(row[sym_i]).strip().upper()
        blanked = ["" for _ in row]
        blanked[sym_i] = row[sym_i]
        if 0 <= warn_i < len(blanked):
            blanked[warn_i] = "identity_quarantined:v6.24.0"
        rows_matrix[r_i] = blanked
        stripped.append(sym)
    return rows_matrix, stripped


_PSAN_52WH_ALIASES = frozenset({
    "52whigh", "week52high", "fiftytwoweekhigh", "52weekhigh", "yearhigh",
})
_PSAN_52WL_ALIASES = frozenset({
    "52wlow", "week52low", "fiftytwoweeklow", "52weeklow", "yearlow",
})


def _persist_sanity_enabled() -> bool:
    """v6.36.0 PV-3: sanity screen on SECOND-CHANCE restored rows.
    Default OFF (S-1 window discipline); TFB_SYNC_PERSIST_SANITY=1 arms
    it. Unset/0 keeps v6.35.0 behaviour byte-identically."""
    return (os.getenv("TFB_SYNC_PERSIST_SANITY") or "0").strip().lower() in ("1", "true", "yes", "on")


def _persist_second_chance_sanity(
    headers: list,
    rows_matrix: list,
    restored: set,
) -> tuple:
    """v6.36.0 PV-3: the PV-2 second-chance pass re-injects last-good
    rows BLINDLY, so a poisoned last-good copy (run 31249231779: the
    2026-08-07T18:46 cross-contaminated batch — THS.US='AEye, Inc.',
    TATAMOTORS.NS='Mr Price Group' px 17,450 vs own 52W 627-740 — led
    the restore list) is resurrected on every time-starved leg. Screen
    ONLY the restored symbols with definite row-internal breaks (px<=0;
    inverted 52W band; px outside [52wLo*0.99, 52wHi*1.01]) and convert
    failures to the FW-2 stub shape (Symbol kept, every other cell
    blanked, Warnings tagged) so the L4b hard-persistence outcome check
    still sees every requested symbol PRESENT and the OLDEST-FIRST
    never-stamped lead fronts the stub for a real refetch next leg.
    FAIL-SAFE: a missing Symbol or price column screens nothing.
    Mutates rows in place. Returns (rows_matrix, quarantined_symbols)."""
    quarantined: list = []
    if not headers or not rows_matrix or not restored:
        return rows_matrix, quarantined
    hdr = list(headers)
    sym_i = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    px_i = _guard_find_col(hdr, _COH_PRICE_ALIASES)
    if sym_i < 0 or px_i < 0:
        return rows_matrix, quarantined
    h52_i = _guard_find_col(hdr, _PSAN_52WH_ALIASES)
    l52_i = _guard_find_col(hdr, _PSAN_52WL_ALIASES)
    warn_i = -1
    for i, h in enumerate(hdr):
        if str(h or "").strip().casefold() == "warnings":
            warn_i = i
            break
    for r_i, row in enumerate(rows_matrix):
        if not isinstance(row, list) or len(row) <= sym_i:
            continue
        if _guard_is_blank(row[sym_i]):
            continue
        sym = str(row[sym_i]).strip().upper()
        if sym not in restored:
            continue
        px = _coh_float(row[px_i]) if len(row) > px_i else None
        h52 = _coh_float(row[h52_i]) if (h52_i >= 0 and len(row) > h52_i) else None
        l52 = _coh_float(row[l52_i]) if (l52_i >= 0 and len(row) > l52_i) else None
        broke = False
        if px is not None and px <= 0.0:
            broke = True
        if not broke and h52 is not None and l52 is not None and h52 > 0.0 and l52 > 0.0:
            if h52 < l52:
                broke = True
            elif px is not None and px > 0.0 and not (l52 * 0.99 <= px <= h52 * 1.01):
                broke = True
        if not broke:
            continue
        blanked = ["" for _ in row]
        blanked[sym_i] = row[sym_i]
        if 0 <= warn_i < len(blanked):
            blanked[warn_i] = "persist_sanity_quarantined:v6.36.0"
        rows_matrix[r_i] = blanked
        quarantined.append(sym)
    return rows_matrix, quarantined


def _operator_quarantine_symbols() -> set:
    """v6.37.0 PL-1: parse TFB_SYNC_QUARANTINE_SYMBOLS (comma/space
    separated). Empty/absent (DEFAULT) -> empty set -> byte-exact no-op.
    OPERATOR CONTRACT (the v6.29.x precedent): set for ONE workflow run,
    verify the report, then REMOVE the env."""
    raw = (os.getenv("TFB_SYNC_QUARANTINE_SYMBOLS") or "").strip()
    if not raw:
        return set()
    out = set()
    for tok in raw.replace(",", " ").split():
        t = tok.strip().upper()
        if t:
            out.add(t)
    return out


def _apply_operator_quarantine(headers: list, rows_matrix: list) -> tuple:
    """v6.37.0 PL-1 — POISON-LOCK BREAKER. EVIDENCE (2026-08-08
    Global_Markets export, 19:54 stamp): 105 rows stayed frozen at the
    2026-08-07T18:46 cross-contamination while 98.4% of the page
    refreshed — the engine's price-coherence guard compares each HONEST
    refetch against the poisoned last-good (THS.US real ~24 vs stored
    1.26 'AEye' price = 19x) and rejects the fresh row as insane, so
    persistence re-keeps the poison forever: the contamination defends
    itself THROUGH the guard. PV-3's numeric screens catch the 50 rows
    whose price breaks its own 52W band; the remaining 55 are
    BAND-COHERENT identity swaps (TAG.DE carrying Moog's name, price AND
    band — internally consistent, numerically invisible). The only clean
    break is to erase the poisoned prior: any FINAL row whose Symbol is
    on the operator's list becomes the FW-2 stub shape (Symbol kept,
    every other cell blanked, Warnings tagged) — L4b still counts the
    symbol PRESENT, OLDEST-FIRST fronts it, and the next fetch has no
    poisoned prior to be compared against, so the guard passes the
    honest row and the lock is broken. Applied to the final matrix
    unconditionally while listed (a listed-but-already-healthy row costs
    one refetch cycle — the operator lists deliberately, one-shot).
    DISTINCT from the SUPERSEDED TFB_SYNC_FORCE_REFETCH_SYMBOLS (retired
    2026-08-04): that forced a fetch-path bypass for a six-symbol heal
    objective that no longer exists; THIS stubs the stored prior and
    lets the NORMAL fetch path heal on the next leg — different
    mechanism, different incident, same one-run-then-remove contract.
    FAIL-SAFE: missing Symbol column screens nothing. Mutates in place.
    Returns (rows_matrix, stubbed_symbols)."""
    stubbed: list = []
    listed = _operator_quarantine_symbols()
    if not listed or not headers or not rows_matrix:
        return rows_matrix, stubbed
    hdr = list(headers)
    sym_i = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    if sym_i < 0:
        return rows_matrix, stubbed
    warn_i = -1
    for i, h in enumerate(hdr):
        if str(h or "").strip().casefold() == "warnings":
            warn_i = i
            break
    for r_i, row in enumerate(rows_matrix):
        if not isinstance(row, list) or len(row) <= sym_i:
            continue
        if _guard_is_blank(row[sym_i]):
            continue
        sym = str(row[sym_i]).strip().upper()
        if sym not in listed:
            continue
        blanked = ["" for _ in row]
        blanked[sym_i] = row[sym_i]
        if 0 <= warn_i < len(blanked):
            blanked[warn_i] = "operator_quarantine_stub:v6.37.0"
        rows_matrix[r_i] = blanked
        stubbed.append(sym)
    return rows_matrix, stubbed


# =============================================================================
# v6.38.0 (W1A-6) — PRE-WRITE OHLC COHERENCE GUARD (Class-A arithmetic)
# -----------------------------------------------------------------------------
# WHY (2026-08-16/17 exports + engine-code adjudication): 589 rows across the
# four market pages carried an Open OUTSIDE [Day Low, Day High] while
# current_price sat INSIDE the band; 85-89% of the foreign Opens are byte-equal
# to a VALID Open of a DIFFERENT row in the same export (HUF=X 313.61 planted
# on UNI-USD/ALGO-USD/NG=F...), and the contamination clusters into write-
# seconds. The ENGINE-side guard (data_engine_v2 Fix BC, R3/R4) already tests
# Open and emitted ZERO ":open:" tags across 19,616 row-days — and three of
# its four ":range:" drops (COMI.CA, CT=F, ^TASI.SR) resurfaced ON THE SHEET
# with the dropped values present. Conclusion: the leak is injected AFTER the
# engine guard, in the assembly/write layer. No fetch-time check can see it.
# This guard is therefore placed at the LAST point before the sheet: the FINAL
# rows_matrix, after KLG / persistence / PL-1, immediately before
# write_table() — the same seam the L3 tripwires were bypassed away from.
#
# MECHANISM (definite same-session breaks only; Symbol + Warnings preserved;
# missing/non-positive members -> no judgement on that rule):
#   P1  day_high < day_low                        -> offense "range"
#   P2  price outside [lo*(1-tol), hi*(1+tol)]    -> offense "price"
#   P3  open  outside [lo*(1-tol), hi*(1+tol)]    -> offense "open"
#     (P3 requires a band that survived P1+P2 — a foreign band must not
#      condemn an honest open.)
#
# MODES (TFB_SYNC_OHLC_PREWRITE_MODE, firewall pattern):
#   observe (default when armed): LOG ONLY — one [OHLC-PREWRITE] line per page
#     with counts + first offenders. Sheet bytes untouched. Zero mutation.
#   enforce: blank ONLY the offending member cells (open on P3; high+low on
#     P1/P2; price is NEVER blanked — it is the anchor, and P2 blanks the band
#     it distrusts, not the price) + append tag
#     "ohlc_incoherent_dropped:<open|price_band|range>:prewrite" to Warnings.
#     Downstream gates/reliability then see honest blanks (same contract as
#     the engine guard).
#
# GATE: TFB_SYNC_OHLC_PREWRITE, DEFAULT OFF — unset/0 => v6.37.0 write path
# byte-identical. Arming is RECOMMENDATION-TOUCHING in enforce mode (6 of the
# 2026-08-17 INVESTABLE rows carry a contaminated Open): ship OFF -> observe
# on operator ENV -> before/after ticket diff -> enforce on operator ENV.
# Tunable: TFB_SYNC_OHLC_PREWRITE_TOL (default 0.01, same as engine
# TFB_ENGINE_OHLC_RANGE_TOL). Fail-safe: any exception leaves the matrix
# untouched and logs a skip line. Zero functions removed; five added.
# =============================================================================

_OHLC_PREWRITE_TAG = "[OHLC-PREWRITE v6.38.0]"
_OHLC_READBACK_TAG = "[OHLC-READBACK v6.41.0]"   # v6.41.0 W1A-6c

_GUARD_OPEN_ALIASES = frozenset({"open", "openprice", "dayopen"})
_GUARD_PRICE_ALIASES = frozenset({"currentprice", "price", "lastprice", "last"})
_GUARD_DAYHIGH_ALIASES = frozenset({"dayhigh", "high"})
_GUARD_DAYLOW_ALIASES = frozenset({"daylow", "low"})


def _ohlc_prewrite_enabled() -> bool:
    """v6.38.0 W1A-6: master gate. DEFAULT OFF — unset/0/false/off keeps the
    v6.37.0 write path byte-identical. TFB_SYNC_OHLC_PREWRITE=1 arms it."""
    return (os.getenv("TFB_SYNC_OHLC_PREWRITE") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _ohlc_prewrite_mode() -> str:
    """v6.38.0 W1A-6: observe (default) | enforce — the FW-4 mode pattern.
    observe logs only; enforce blanks offending members + tags Warnings."""
    v = (os.getenv("TFB_SYNC_OHLC_PREWRITE_MODE") or "observe").strip().lower()
    return v if v in {"observe", "enforce"} else "observe"


def _ohlc_prewrite_tol() -> float:
    """v6.38.0 W1A-6: band tolerance (default 0.01 = 1%, engine-aligned)."""
    try:
        v = float((os.getenv("TFB_SYNC_OHLC_PREWRITE_TOL") or "0.01").strip())
    except Exception:
        v = 0.01
    return v if 0.0 <= v < 0.5 else 0.01


def _ohlc_readback_enabled() -> bool:
    """v6.41.0 W1A-6c: post-write READBACK. DEFAULT OFF — unset/0/false/off
    keeps the v6.40.0 write path byte-identical, and the readback NEVER
    mutates anything under any setting: it is a measurement, not a guard.

    WHY IT EXISTS (2026-08-20, adjudicated on primary artifacts):
    the pre-write guard and the engine-side Fix BC guard are BOTH healthy and
    BOTH blind to the same rows. Running this file's own
    _apply_ohlc_prewrite_guard — same aliases, same _ohlc_prewrite_num, same
    0.01 tolerance, same P1/P2/P3 chain — over the EXPORTED sheet returned
    checked=9809 flagged=618 (GM 448 / CFX 98 / MF 72 / ML 0), while the same
    guard at the write boundary that morning reported GM flagged=5. Identical
    code, identical `checked` counts, 5 vs 618. The difference is not the
    predicate; it is WHICH ROWS the guard was shown.

    The mechanism is upstream of the guard, inside this file:
    _keep_last_good_rows ("replace outgoing error stubs with accepted prior
    rows"), _persist_missing_symbol_rows and the FW-KEEP second pass re-inject
    PRIOR SHEET ROWS into the outgoing matrix. Corroborated independently by
    the only ohlc tags on the sheet — 5x "ohlc_incoherent_dropped:range:engine"
    carrying NO ":observe" suffix, which the engine's observe path cannot
    produce (it always appends it), i.e. enforce-era tags still resident.
    Blank-cell preservation is NOT the mechanism: write_table sends an explicit
    "" through unchanged and has no per-cell keep-old branch.

    Nothing that exists today reads the sheet after a write, so nothing can
    see or ever clean those rows. This is the one pass that looks.

    COST (audit F5, full disclosure): per checked page per run this feature
    adds ONE values.get (the readback, bounded by _page_read_row_bound) AND
    ONE _Run_Log values.append with up to 2 attempts — including MATCHED
    lines, mirroring the prewrite appender's OK lines. Four ranked pages =>
    ~4 reads + ~4 appends per run against a ~7-read/page healthy baseline.
    """
    return (os.getenv("TFB_SYNC_OHLC_READBACK") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _ohlc_prewrite_num(v: Any) -> Optional[float]:
    """v6.38.0 W1A-6: tolerant cell-to-float for the FINAL matrix (cells may
    already be display-formatted: '1,908.50', '▲ 12.94%', ''). Returns None
    on blank/unparseable/non-positive-unusable inputs; callers treat None as
    'no judgement'. 0 parses to 0.0 (P1/P2/P3 each require positives)."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except Exception:
            return None
        return f if f == f else None  # NaN screen
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "").replace("▲", "").replace("▼", "").replace("%", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _apply_ohlc_prewrite_guard(headers: list, rows_matrix: list, page: str) -> tuple:
    """v6.38.0 W1A-6 — pre-write Class-A OHLC coherence on the FINAL matrix.

    Runs at the write boundary so it sees exactly what the sheet will see,
    catching the assembly-layer Open leak the engine-side guard (Fix BC)
    structurally cannot. observe: zero mutation, log-line only. enforce:
    blank offending members (never Symbol, never price, never Warnings) and
    append 'ohlc_incoherent_dropped:<offenses>:prewrite' to Warnings.
    FAIL-SAFE by contract: called inside try/except at the call site; any
    internal error propagates to that handler which logs and writes the
    matrix untouched. Returns (rows_matrix, stats_dict)."""
    # v6.41.0 (W1A-6c): blank_* added. Until now a row whose Open/High/Low
    # parsed to None was indistinguishable from a clean row — both simply
    # produced no offense. That ambiguity is what made the 2026-08-20
    # "checked=6627 flagged=5" line unreadable: it could mean the matrix was
    # clean, or that 6,000 cells were untestable. Now it is measured.
    stats = {"checked": 0, "flagged": 0, "open": 0, "price_band": 0,
             "range": 0, "blank_open": 0, "blank_hi": 0, "blank_lo": 0,
             # v6.42.0 (W1A-6d) ADDITIVE counters — `flagged` semantics
             # untouched so every historical line stays comparable:
             #   zero_band   rows silently skipped because hi/lo parse
             #               but are not BOTH > 0 (the [0,*] bands the
             #               2026-08-21 audit found: 9 rows, no test ran)
             #   tol_excused rows whose Open sits strictly outside
             #               [lo,hi] but inside the 1% tol widening
             #               (29 rows on 2026-08-21 — real leaks the
             #               guard forgives by tolerance, now measured)
             #   open_masked rows where P2 (price_band) fired and the
             #               Open ALSO violates — the short-circuit
             #               hid the open attribution (6 rows)
             "zero_band": 0, "tol_excused": 0, "open_masked": 0,
             # v6.50.0: the 52-week invariant, measured on every row of the
             # final matrix for the first time. w52_absent counts rows the
             # page cannot test (no 52W columns) so coverage is never
             # confused with cleanliness.
             "w52_band": 0, "w52_zero_band": 0, "w52_tol_excused": 0,
             "w52_absent": 0, "w52_examples": [],
             "examples": []}
    if not headers or not rows_matrix:
        return rows_matrix, stats
    hdr = list(headers)
    sym_i = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    open_i = _guard_find_col(hdr, _GUARD_OPEN_ALIASES)
    price_i = _guard_find_col(hdr, _GUARD_PRICE_ALIASES)
    hi_i = _guard_find_col(hdr, _GUARD_DAYHIGH_ALIASES)
    lo_i = _guard_find_col(hdr, _GUARD_DAYLOW_ALIASES)
    # v6.50.0: 52-week band columns, reusing the PV-3 alias sets.
    h52_i = _guard_find_col(hdr, _PSAN_52WH_ALIASES)
    l52_i = _guard_find_col(hdr, _PSAN_52WL_ALIASES)
    if hi_i < 0 or lo_i < 0 or (open_i < 0 and price_i < 0):
        return rows_matrix, stats  # page has no testable OHLC contract
    warn_i = -1
    for i, h in enumerate(hdr):
        if str(h or "").strip().casefold() == "warnings":
            warn_i = i
            break
    tol = _ohlc_prewrite_tol()
    enforce = _ohlc_prewrite_mode() == "enforce"
    for r_i, row in enumerate(rows_matrix):
        if not isinstance(row, list):
            continue
        n = len(row)
        if n <= max(hi_i, lo_i):
            continue
        hi = _ohlc_prewrite_num(row[hi_i])
        lo = _ohlc_prewrite_num(row[lo_i])
        op = _ohlc_prewrite_num(row[open_i]) if 0 <= open_i < n else None
        cp = _ohlc_prewrite_num(row[price_i]) if 0 <= price_i < n else None
        stats["checked"] += 1
        if op is None:
            stats["blank_open"] += 1
        if hi is None:
            stats["blank_hi"] += 1
        if lo is None:
            stats["blank_lo"] += 1
        offenses: list = []
        # v6.42.0 (W1A-6d): measure the silent-skip before it happens.
        if hi is not None and lo is not None and not (hi > 0 and lo > 0):
            stats["zero_band"] += 1
        # v6.42.0 (W1A-6d): a strict-band violation the tol widening
        # forgives — counted independently of the offense chain.
        if (op is not None and hi is not None and lo is not None
                and hi >= lo and not (lo <= op <= hi)
                and lo * (1.0 - tol) <= op <= hi * (1.0 + tol)):
            stats["tol_excused"] += 1
        range_ok = hi is not None and lo is not None and hi > 0 and lo > 0
        # P1: inverted band is self-contradictory whatever the symbol.
        if range_ok and hi < lo:
            offenses.append("range")
            range_ok = False
        # P2: the anchor must sit inside its own session band.
        if range_ok and cp is not None and cp > 0 and not (
                lo * (1.0 - tol) <= cp <= hi * (1.0 + tol)):
            offenses.append("price_band")
            range_ok = False
        # P3: an open outside a band that BOTH exists and contains the price
        # is foreign by construction (the 2026-08-17 fingerprint: 7186.T open
        # 10.19 vs band [1908.5, 1945] with price 1911 inside).
        if range_ok and op is not None and op > 0 and not (
                lo * (1.0 - tol) <= op <= hi * (1.0 + tol)):
            offenses.append("open")
        # v6.50.0 P4: the anchor must also sit inside its own 52-WEEK band.
        # Deliberately NOT gated on range_ok - P1/P2 describe the session
        # band, and a row with an incoherent session band can still have a
        # perfectly testable annual band (and vice versa). Same tol widening
        # as P2/P3 so the classes stay comparable.
        if h52_i < 0 or l52_i < 0:
            stats["w52_absent"] += 1
        else:
            _h52 = _ohlc_prewrite_num(row[h52_i]) if h52_i < n else None
            _l52 = _ohlc_prewrite_num(row[l52_i]) if l52_i < n else None
            if _h52 is not None and _l52 is not None:
                if not (_h52 > 0 and _l52 > 0):
                    stats["w52_zero_band"] += 1
                elif cp is not None and cp > 0:
                    _lb = _l52 * (1.0 - tol)
                    _ub = _h52 * (1.0 + tol)
                    if _h52 >= _l52 and not (_lb <= cp <= _ub):
                        offenses.append("w52_band")
                        if len(stats["w52_examples"]) < 12 and 0 <= sym_i < n:
                            stats["w52_examples"].append(
                                str(row[sym_i]).strip() + "(" + str(cp) + " vs "
                                + str(_l52) + "-" + str(_h52) + ")")
                    elif (_h52 >= _l52 and not (_l52 <= cp <= _h52)):
                        stats["w52_tol_excused"] += 1
        # v6.42.0 (W1A-6d): P2 fired with a valid band and the Open is
        # ALSO outside the tol band — the P1/P2 short-circuit hides the
        # open attribution from the counters an arming decision reads.
        if ("price_band" in offenses and op is not None and op > 0
                and not (lo * (1.0 - tol) <= op <= hi * (1.0 + tol))):
            stats["open_masked"] += 1
        if not offenses:
            continue
        stats["flagged"] += 1
        for k in offenses:
            stats[k] += 1
        if len(stats["examples"]) < 12 and 0 <= sym_i < n and not _guard_is_blank(row[sym_i]):
            stats["examples"].append(str(row[sym_i]).strip())
        if enforce:
            # v6.45.0 R3/R6: mutate only the operator-selected classes;
            # default = all three = v6.44.1 byte-identical. Counters and the
            # per-page log line above are class-blind in every setting.
            _ecl = _ohlc_prewrite_enforce_classes()
            _acted = [k for k in offenses if k in _ecl]
            if "open" in _acted and 0 <= open_i < n:
                row[open_i] = ""
            if ("range" in _acted or "price_band" in _acted):
                if 0 <= hi_i < n:
                    row[hi_i] = ""
                if 0 <= lo_i < n:
                    row[lo_i] = ""
            # v6.50.0: blank ONLY the 52W band cells - never Symbol, never
            # price, never Warnings (FW-2 shape). Off unless armed.
            if "w52_band" in _acted:
                if 0 <= h52_i < n:
                    row[h52_i] = ""
                if 0 <= l52_i < n:
                    row[l52_i] = ""
            if _acted and 0 <= warn_i < n:
                tag = "ohlc_incoherent_dropped:" + "+".join(_acted) + ":prewrite"
                prev = "" if _guard_is_blank(row[warn_i]) else str(row[warn_i]).strip()
                if tag not in prev:
                    row[warn_i] = (prev + ("; " if prev else "") + tag)
    return rows_matrix, stats


def _name_dedup_mode() -> str:
    """v6.24.1 FW-4: observe (default) | quarantine | off."""
    v = (os.getenv("TFB_SYNC_NAME_DEDUP_MODE") or "observe").strip().lower()
    return v if v in {"observe", "quarantine", "off"} else "observe"


def _name_dedup_min() -> int:
    """v6.24.1 FW-4: group-size threshold (default 3, floor 2)."""
    try:
        v = int((os.getenv("TFB_SYNC_NAME_DEDUP_MIN") or "3").strip())
    except Exception:
        v = 3
    return max(2, v)


# v6.24.1 ST-1: set False by the startup self-test on failure — quarantine
# (the only new destructive-ish action) refuses to run on a broken guard.
_IDFW_SELFTEST_OK = True
_IDFW_SELFTEST_MSG = "not-run"


def _name_dedup_family_exempt_enabled() -> bool:
    """v6.25.3: default ON. 0/false/off -> v6.25.2 census verbatim."""
    return (os.getenv("TFB_SYNC_NAME_DEDUP_FAMILY_EXEMPT") or "1").strip().lower() not in {
        "0", "false", "off", "no"}


def _name_dedup_exempt_names() -> set:
    """v6.25.3: operator-listed names never quarantined (comma-separated,
    casefolded). For families the root heuristic cannot see (e.g. a numeric
    cross-listing like 9961.HK beside TCOM)."""
    raw = os.getenv("TFB_SYNC_NAME_DEDUP_EXEMPT_NAMES") or ""
    return {t.strip().casefold() for t in raw.split(",") if t.strip()}


# v6.28.0 FW-4b: Warnings tag stamped on a dedup LOSER (the survivor keeps
# its full row; the loser keeps its Symbol so heal-first can re-fetch it).
_NAME_DEDUP_LOSER_TAG = "identity_quarantined:name_dedup_loser:v6.28.0"


def _name_dedup_safe_enabled() -> bool:
    """v6.28.0 FW-4b kill-switch — DEFAULT ON. 0/false/off/no restores the
    v6.27.0 whole-group quarantine stub byte-identically."""
    return (os.getenv("TFB_SYNC_NAME_DEDUP_SAFE") or "1").strip().lower() not in {
        "0", "false", "off", "no"}


def _name_dedup_survivors(headers: list, rows_matrix: list, sym_i: int,
                          syms: list) -> set:
    """v6.28.0 FW-4b: pick the KEEP set for ONE same-name group.

    Partition the group's rows by their Currency cell (missing column or
    blank cell -> one shared bucket); DIFFERENT currencies are different
    listings, so every currency bucket keeps its own survivor — a numeric
    cross-listing family (9961.HK / TCOM / TCOM.US) can never be wiped
    whole. Inside one currency bucket exactly ONE row survives, chosen by
    a fully deterministic key: newest Last-Updated stamp first (reuses
    _parse_stamp_cell; ISO 'T' separators normalised so intraday stamps
    compare), explicit exchange suffix second, most non-blank cells third,
    lexicographically-first symbol last.

    Fail-safe contract: on ANY failure, or when the group has no locatable
    rows, returns set(syms) — keep EVERYTHING; the caller treats an
    all-survivor verdict as keep-all. Never raises."""
    try:
        want = {str(s or "").strip().upper() for s in (syms or []) if str(s or "").strip()}
        if not want:
            return set()
        hdr = [str(h or "").strip().casefold() for h in (headers or [])]
        cur_i = -1
        stamp_i = -1
        for i, h in enumerate(hdr):
            if cur_i < 0 and h in ("currency", "ccy"):
                cur_i = i
            if stamp_i < 0 and h.startswith("last updated"):
                stamp_i = i
        best: dict = {}  # SYM -> (stamp, has_suffix, nonblank, currency)
        for row in rows_matrix:
            if not isinstance(row, list) or len(row) <= sym_i:
                continue
            sym = str(row[sym_i] or "").strip().upper()
            if sym not in want:
                continue
            cur = ""
            if 0 <= cur_i < len(row):
                cur = str(row[cur_i] or "").strip().upper()
            ts = None
            if 0 <= stamp_i < len(row):
                cell = row[stamp_i]
                if isinstance(cell, str):
                    cell = cell.strip().replace("T", " ")
                ts = _parse_stamp_cell(cell)
                if ts is not None and ts.tzinfo is not None:
                    ts = ts.replace(tzinfo=None)
            ts = ts or datetime.min
            has_suffix = 1 if any(sym.endswith(suf) for suf in _SR_FAMILY_SUFFIXES) else 0
            nonblank = sum(1 for c in row if str(c or "").strip() != "")
            cand = (ts, has_suffix, nonblank, cur)
            prev = best.get(sym)
            if prev is None or cand[:3] > prev[:3]:
                best[sym] = cand
        if not best:
            return set(want)
        buckets: dict = {}
        for sym, (ts, suf, nb, cur) in best.items():
            buckets.setdefault(cur, []).append((ts, suf, nb, sym))
        keep = set()
        for cur, members in buckets.items():
            members.sort(key=lambda m: (m[0], m[1], m[2]), reverse=True)
            top = members[0][:3]
            tied = sorted(m[3] for m in members if m[:3] == top)
            keep.add(tied[0])
        # symbols the census saw but this matrix pass could not locate are
        # NEVER doomed on absence of evidence
        keep |= (want - set(best.keys()))
        return keep if keep else set(want)
    except Exception:
        return {str(s or "").strip().upper() for s in (syms or [])}


_SR_FAMILY_SUFFIXES = (".US", ".PA", ".L", ".HK", ".JK", ".SW", ".DE", ".SR",
                       ".T", ".AS", ".BR", ".MC", ".MI", ".TO", ".AX", ".NS",
                       ".IS", ".KW", ".SA", ".SI", ".KS", ".TW")


def _sym_root(sym: str) -> str:
    """v6.25.3: exchange-suffix + share-class strip -> comparable root."""
    s = str(sym or "").strip().upper()
    for suf in _SR_FAMILY_SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    s = s.split(".")[0]
    for sep in ("-", "/"):
        s = s.split(sep)[0]
    return s


def _name_dedup_is_family(syms: list) -> bool:
    """v6.25.3: True when a same-name group is a LEGITIMATE multi-listing /
    share-class family (TTE + TTE.PA + TTE.US; ONB + ONBPO + ONBPP;
    TLK + TLK.US + TLKM.JK) rather than chimeric poison (Goodyear's name on
    BRK-B + FI + GT.US -> roots differ -> NOT family)."""
    roots = {_sym_root(x) for x in syms if str(x).strip()}
    if not roots:
        return False
    if len(roots) == 1:
        return True
    base = min(roots, key=len)
    return (len(base) >= 2 and base.isalpha()
            and all(r.startswith(base) for r in roots))


def _name_dedup_census(headers: list, rows_matrix: list) -> tuple:
    """v6.24.1 FW-4: census Name -> distinct symbols across the OUTGOING
    batch. Returns (groups, name_i, sym_i, warn_i) where groups is a dict
    {name: sorted([symbols])} restricted to groups of size >=
    _name_dedup_min(). Blank names ignored. Fail-safe: missing columns ->
    empty groups."""
    groups: dict = {}
    if not headers or not rows_matrix:
        return groups, -1, -1, -1
    hdr = list(headers)
    name_i = _guard_find_col(hdr, _GUARD_NAME_ALIASES)
    sym_i = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    warn_i = -1
    for i, h in enumerate(hdr):
        if str(h or "").strip().casefold() == "warnings":
            warn_i = i
            break
    if name_i < 0 or sym_i < 0:
        return groups, name_i, sym_i, warn_i
    owners: dict = {}
    for row in rows_matrix:
        if not isinstance(row, list) or len(row) <= max(name_i, sym_i):
            continue
        nm = str(row[name_i] or "").strip()
        sym = str(row[sym_i] or "").strip().upper()
        if not nm or not sym:
            continue
        owners.setdefault(nm, set()).add(sym)
    m = _name_dedup_min()
    fam_exempt = _name_dedup_family_exempt_enabled()
    manual = _name_dedup_exempt_names() if fam_exempt else set()
    for nm, syms in owners.items():
        if len(syms) >= m:
            ss = sorted(syms)
            # v6.25.3: a legitimate multi-listing family shares its name by
            # DESIGN — exempt it from the quarantine census entirely so FW-4
            # (armed for the first time by the v6.25.2 selftest fix) blanks
            # only chimeric poison, never TTE.PA / ONBPO / TLKM.JK.
            if fam_exempt and (nm.casefold() in manual
                               or _name_dedup_is_family(ss)):
                logger.info("%s FW-4 family-exempt: '%s' -> %s "
                            "(multi-listing, not quarantined)",
                            _IDFW_TAG, nm, ",".join(ss[:6]))
                continue
            groups[nm] = ss
    return groups, name_i, sym_i, warn_i


def _name_dedup_apply(headers: list, rows_matrix: list) -> tuple:
    """v6.24.1 FW-4: run the census and, in quarantine mode (and only when
    the startup self-test passed), stub every carrier row to Symbol-only +
    Warnings tag. Returns (rows_matrix, groups, quarantined_symbols).
    Mutates in place. Never raises."""
    quarantined: list = []
    try:
        mode = _name_dedup_mode()
        if mode == "off":
            return rows_matrix, {}, quarantined
        groups, name_i, sym_i, warn_i = _name_dedup_census(headers, rows_matrix)
        if not groups:
            return rows_matrix, groups, quarantined
        if mode == "quarantine" and _IDFW_SELFTEST_OK:
            safe = _name_dedup_safe_enabled()
            tag = _NAME_DEDUP_LOSER_TAG if safe else "identity_quarantined:name_dedup"
            doomed = set()
            for nm, syms in groups.items():
                if not safe:
                    # v6.27.0 legacy path (kill-switch): whole-group stub
                    doomed.update(syms)
                    continue
                # v6.28.0 FW-4b: keep one survivor per currency bucket;
                # stub only the true losers. HARD INVARIANT — a group can
                # never lose all members: an empty or all-member survivor
                # set resolves to keep-everything.
                survivors = _name_dedup_survivors(headers, rows_matrix,
                                                  sym_i, list(syms))
                losers = [s for s in syms if s not in survivors]
                if not survivors or len(losers) >= len(syms):
                    logger.warning("%s FW-4b keep-all failsafe: '%s' -> %s",
                                   _IDFW_TAG, nm, ",".join(sorted(syms)[:6]))
                    continue
                if not losers:
                    logger.info("%s FW-4b cross-listing kept whole: '%s' -> %s",
                                _IDFW_TAG, nm, ",".join(sorted(syms)[:6]))
                    continue
                doomed.update(losers)
            for r_i, row in enumerate(rows_matrix):
                if not isinstance(row, list) or len(row) <= sym_i:
                    continue
                sym = str(row[sym_i] or "").strip().upper()
                if sym in doomed:
                    blanked = ["" for _ in row]
                    blanked[sym_i] = row[sym_i]
                    if 0 <= warn_i < len(blanked):
                        blanked[warn_i] = tag
                    rows_matrix[r_i] = blanked
                    quarantined.append(sym)
        return rows_matrix, groups, quarantined
    except Exception as e:
        logger.warning("%s FW-4 skipped: %s", _IDFW_TAG, e)
        return rows_matrix, {}, quarantined


def _idfw_selftest_() -> bool:
    """v6.25.2 ST-1: prove the guards on canned fixtures BEFORE touching a
    page. Sets _IDFW_SELFTEST_OK/_IDFW_SELFTEST_MSG. Never raises."""
    global _IDFW_SELFTEST_OK, _IDFW_SELFTEST_MSG
    passed = 0
    total = 9
    try:
        H = ["Symbol", "Name", "Price", "EPS (TTM)", "P/E (TTM)", "Warnings"]
        ni, pi, ei, qi = 1, 2, 3, 4
        poisoned = ["GOOG", "Gulfport Energy Corporation", 213.5, 9.18, 3.6, ""]
        healthy = ["MSFT", "Microsoft Corporation", 500.0, 12.5, 40.0, ""]
        gbx = ["NG.L", "National Grid plc", 1050.0, 0.60, 17.5, ""]
        if _klg_old_row_identity_ok(poisoned, ni, pi, ei, qi) is False:
            passed += 1
        if _klg_old_row_identity_ok(healthy, ni, pi, ei, qi) is True:
            passed += 1
        if _klg_old_row_identity_ok(gbx, ni, pi, ei, qi) is True:
            passed += 1
        m = [list(healthy), list(poisoned), list(gbx)]
        _m, stripped = _row_identity_firewall(H, m)
        if stripped == ["GOOG"] and _m[0][1] == "Microsoft Corporation":
            passed += 1
        rows = [
            ["A.US", "Same Name Co", 10, 1, 10, ""],
            ["B.L", "Same Name Co", 11, 1.1, 10, ""],
            ["C.HK", "Same Name Co", 12, 1.2, 10, ""],
            ["D.US", "Other Co", 9, 1, 9, ""],
        ]
        groups, _n, _s, _w = _name_dedup_census(H, rows)
        if list(groups.keys()) == ["Same Name Co"] and len(groups["Same Name Co"]) == 3:
            passed += 1
        # v6.25.2 ST-1 fix: case 6 is MODE-AWARE. The old fixture asserted the
        # observe default (q2 == []); once D-4 armed
        # TFB_SYNC_NAME_DEDUP_MODE=quarantine in the workflow env (2026-07-19),
        # _name_dedup_apply correctly stubbed the canned dupes, the case
        # failed, and the resulting FAIL 5/6 disabled FW-4 for every run —
        # the arming itself switched the feature off. Case 6 now verifies the
        # guard's contract under the CONFIGURED mode, and in quarantine mode
        # it is STRICTER than before: exact quarantine set, symbol preserved,
        # name blanked, Warnings tag stamped, non-duplicate row untouched.
        rows2 = [list(r) for r in rows]
        _prev_ok = _IDFW_SELFTEST_OK
        _IDFW_SELFTEST_OK = True  # exercise the stub path deterministically
        try:
            _r2, g2, q2 = _name_dedup_apply(H, rows2)
        finally:
            _IDFW_SELFTEST_OK = _prev_ok
        _mode6 = _name_dedup_mode()
        if _mode6 == "quarantine":
            if _name_dedup_safe_enabled():
                # v6.28.0 FW-4b contract: no Currency/stamp columns in the
                # fixture -> one bucket -> lexicographic survivor A.US keeps
                # its FULL row; B.L and C.HK are stubbed with the loser tag;
                # the non-duplicate bystander is untouched.
                if (q2 == ["B.L", "C.HK"] and bool(g2)
                        and _r2[0][0] == "A.US"
                        and _r2[0][1] == "Same Name Co"
                        and _r2[1][0] == "B.L" and _r2[1][1] == ""
                        and _r2[1][5] == _NAME_DEDUP_LOSER_TAG
                        and _r2[2][1] == ""
                        and _r2[3][1] == "Other Co"):
                    passed += 1
            else:
                if (q2 == ["A.US", "B.L", "C.HK"] and bool(g2)
                        and _r2[1][0] == "B.L" and _r2[1][1] == ""
                        and _r2[1][5] == "identity_quarantined:name_dedup"
                        and _r2[3][1] == "Other Co"):
                    passed += 1
        elif _mode6 == "off":
            if q2 == [] and g2 == {}:
                passed += 1
        else:  # observe (default)
            if q2 == [] and g2:
                passed += 1
        # v6.25.3 case 7: family discriminator — a legitimate multi-listing
        # group is EXEMPT from the census; a chimeric group is KEPT.
        fam_rows = [
            ["TTE", "TotalEnergies SE", 60, 6, 10, ""],
            ["TTE.PA", "TotalEnergies SE", 60, 6, 10, ""],
            ["TTE.US", "TotalEnergies SE", 60, 6, 10, ""],
            ["BRK-B", "The Goodyear Tire & Rubber Company", 480, 20, 24, ""],
            ["FI", "The Goodyear Tire & Rubber Company", 160, 8, 20, ""],
            ["GT.US", "The Goodyear Tire & Rubber Company", 9, 1, 9, ""],
        ]
        g7, _i7, _j7, _k7 = _name_dedup_census(H, fam_rows)
        _fam_ok = ("TotalEnergies SE" not in g7
                   and g7.get("The Goodyear Tire & Rubber Company")
                   == ["BRK-B", "FI", "GT.US"])
        if not _name_dedup_family_exempt_enabled():
            _fam_ok = ("TotalEnergies SE" in g7
                       and "The Goodyear Tire & Rubber Company" in g7)
        if _fam_ok:
            passed += 1

        # v6.29.0 B-4 case 8: env parsing — case/space tolerant, absent=empty.
        _fr_saved = os.environ.pop("TFB_SYNC_FORCE_REFETCH_SYMBOLS", None)
        _c8 = (_force_refetch_symbols() == set())
        os.environ["TFB_SYNC_FORCE_REFETCH_SYMBOLS"] = " bk , BRK-B ,, 3001.sr "
        _c8 = _c8 and (_force_refetch_symbols() == {"BK", "BRK-B", "3001.SR"})
        if _c8:
            passed += 1
        # v6.29.0 B-4 case 9: end-to-end stub-grid — unforced stub is healed
        # from the old grid; a FORCED stub is NOT (old row blocked), and the
        # blocked symbol is reported in _LAST_KLG_FORCED.
        class _StubSheets:
            def __init__(self, grid): self._g = grid
            def read_values(self, sid, name, rng): return self._g
        _kH = ["Symbol", "Name", "Price", "Data Provider"]
        _old_grid = [list(_kH),
                     ["BK", "Bank of New York Mellon Corp", 137.16, "eodhd"],
                     ["V.US", "Visa Inc.", 355.74, "eodhd"]]
        _mk = lambda: [["BK", "", "", "fallback_error"],
                       ["V.US", "", "", "fallback_error"]]
        os.environ.pop("TFB_SYNC_FORCE_REFETCH_SYMBOLS", None)
        _m9a, _sw9a = _keep_last_good_rows(_StubSheets(_old_grid), "sid", "P", _kH, _mk())
        os.environ["TFB_SYNC_FORCE_REFETCH_SYMBOLS"] = "BK"
        _m9b, _sw9b = _keep_last_good_rows(_StubSheets(_old_grid), "sid", "P", _kH, _mk())
        _c9 = (sorted(_sw9a) == ["BK", "V.US"]
               and _m9a[0][1] == "Bank of New York Mellon Corp"
               and _sw9b == ["V.US"]
               and _m9b[0][1] == ""            # forced stub stays fresh
               and _m9b[1][1] == "Visa Inc."   # unforced path untouched
               and _LAST_KLG_FORCED == ["BK"])
        if _c9:
            passed += 1
        if _fr_saved is not None:
            os.environ["TFB_SYNC_FORCE_REFETCH_SYMBOLS"] = _fr_saved
        else:
            os.environ.pop("TFB_SYNC_FORCE_REFETCH_SYMBOLS", None)
    except Exception as e:
        _IDFW_SELFTEST_OK = False
        _IDFW_SELFTEST_MSG = "EXC %s: %s" % (type(e).__name__, e)
        print("::error::[SELFTEST v6.25.2] guard self-test crashed: %s — "
              "FW-4 quarantine disabled for this run." % _IDFW_SELFTEST_MSG)
        return False
    _IDFW_SELFTEST_OK = (passed == total)
    _IDFW_SELFTEST_MSG = "PASS %d/%d" % (passed, total) if _IDFW_SELFTEST_OK else "FAIL %d/%d" % (passed, total)
    try:
        logger.info(_env_echo_line())  # v6.56.1 ENV-ECHO (v6.56.2: moved ABOVE the if/else)
    except Exception:  # noqa: BLE001
        pass
    if _IDFW_SELFTEST_OK:
        logger.info("[SELFTEST v6.25.2] %s — guards verified on fixtures.", _IDFW_SELFTEST_MSG)
    else:
        print("::error::[SELFTEST v6.25.2] %s — a guard fixture failed; "
              "FW-4 quarantine disabled for this run (FW-1/FW-2 remain on)." % _IDFW_SELFTEST_MSG)
    return _IDFW_SELFTEST_OK


_RUNLOG_APPEND_FAILS: List[str] = []   # v6.55.0: site tags of final append failures


def _note_runlog_append_failure(site: str) -> None:
    """v6.55.0: remember a FINAL _Run_Log append failure (after retries)."""
    try:
        _RUNLOG_APPEND_FAILS.append(str(site or "?"))
    except Exception:
        pass


def _append_fail_is_error() -> bool:
    """v6.55.0 kill-switch. DEFAULT ON; TFB_SYNC_APPEND_FAIL_IS_ERROR=0/false/
    off/no restores the v6.54.0 exit code (warnings only)."""
    return (os.getenv("TFB_SYNC_APPEND_FAIL_IS_ERROR") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _idfw_runlog_enabled() -> bool:
    """v6.24.0 FW-3: append one [ID-FIREWALL] verdict line per market page
    to the workbook's _Run_Log. Default ON; TFB_SYNC_IDFW_RUNLOG=0 off."""
    return (os.getenv("TFB_SYNC_IDFW_RUNLOG") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _append_runlog_idfirewall(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    page: str,
    klg_kept: int,
    klg_suspects: list,
    out_stripped: list,
    name_dup_groups: dict = None,
    name_dup_quarantined: list = None,
) -> None:
    """v6.24.0 FW-3: best-effort, fail-open verdict append (columns match
    the _Run_Log layout: Timestamp, Level, Action, Page, Status, Message,
    Endpoint, HTTP Code, Duration ms, Details JSON). Silence on any error
    - the tripwire must never break the write path it watches."""
    if not _idfw_runlog_enabled() or sheets is None:
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        name_dup_groups = dict(list((name_dup_groups or {}).items())[:12])
        name_dup_quarantined = list(name_dup_quarantined or [])
        _dup_n = len(name_dup_groups)
        level = "WARNING" if (klg_suspects or out_stripped or _dup_n or name_dup_quarantined) else "INFO"
        status = "SUSPECT" if (klg_suspects or out_stripped or _dup_n or name_dup_quarantined) else "OK"
        msg = (
            f"{_IDFW_TAG} {page} | klg_kept={klg_kept} | "
            f"klg_suspect_dropped={len(klg_suspects)}"
            f"{' (' + ', '.join(klg_suspects[:10]) + ('…' if len(klg_suspects) > 10 else '') + ')' if klg_suspects else ''}"
            f" | out_stripped={len(out_stripped)}"
            f"{' (' + ', '.join(out_stripped[:10]) + ('…' if len(out_stripped) > 10 else '') + ')' if out_stripped else ''}"
            f" | name_dup={_dup_n}"
            f"{' [' + '; '.join(k + 'x' + str(len(v)) for k, v in list(name_dup_groups.items())[:5]) + ']' if _dup_n else ''}"
            f" | dedup_mode={_name_dedup_mode() + ('(keep1)' if (_name_dedup_mode() == 'quarantine' and _name_dedup_safe_enabled()) else '')}"
            f"{' | quarantined=' + str(len(name_dup_quarantined)) if name_dup_quarantined else ''}"
            f" | selftest={_IDFW_SELFTEST_MSG}"
        )
        details = json.dumps({
            "klg_kept": int(klg_kept),
            "klg_suspect_dropped": klg_suspects[:50],
            "out_stripped": out_stripped[:50],
            "name_dup_groups": name_dup_groups,
            "name_dup_quarantined": name_dup_quarantined[:50],
            "dedup_safe": bool(_name_dedup_safe_enabled()),
            "selftest": _IDFW_SELFTEST_MSG,
            "version": SCRIPT_VERSION,
        })
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        body = {"values": [[ts, level, "run_dashboard_sync", page, status, msg, "", "", "", details]]}
        _last_err = None
        for _attempt in (1, 2):
            try:
                svc.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range="'_Run_Log'!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                ).execute()
                _last_err = None
                break
            except Exception as _ae:
                _last_err = _ae
                time.sleep(1.0)
        if _last_err is not None:
            raise _last_err
        logger.info(msg)
    except Exception as _e:
        # v6.24.1 FW-3b: a tripwire's own failure must be LOUD — annotate
        # the run page so it can never fail invisibly again.
        _note_runlog_append_failure("verdict")   # v6.55.0
        print("::warning::%s _Run_Log verdict append FAILED for %s — %s: %s"
              % (_IDFW_TAG, page, type(_e).__name__, _e))
        logger.warning("%s run-log verdict skipped: %s", _IDFW_TAG, _e)


def _ohlc_prewrite_runlog_enabled() -> bool:
    """v6.39.4 W1A-6b: append one [OHLC-PREWRITE] verdict line per page to
    the workbook's _Run_Log, mirroring the FW-3 channel. Default ON, but only
    *within* the armed guard — the guard's own gate (TFB_SYNC_OHLC_PREWRITE,
    DEFAULT OFF) still decides whether any of this executes, so an unarmed
    repo keeps v6.39.3 behaviour byte-identically. TFB_SYNC_OHLC_RUNLOG=0
    silences the append while leaving the guard itself running."""
    return (os.getenv("TFB_SYNC_OHLC_RUNLOG") or "1").strip().lower() not in {
        "0", "false", "off", "no"}


def _append_runlog_ohlc_prewrite(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    page: str,
    stats: dict,
) -> None:
    """v6.39.4 W1A-6b: best-effort, fail-open verdict append for the
    pre-write OHLC coherence guard (columns match the _Run_Log layout:
    Timestamp, Level, Action, Page, Status, Message, Endpoint, HTTP Code,
    Duration ms, Details JSON).

    The guard's v6.38.0 observe mode logged only to stdout, which the GitHub
    Actions runner discards; the operator's enforce decision needs a durable
    surface in the workbook itself. Contract is identical to FW-3's: silence
    on any error EXCEPT a loud ::warning:: annotation, because a tripwire
    must never break the write path it watches, and must never fail
    invisibly either.
    """
    if not _ohlc_prewrite_runlog_enabled() or sheets is None:
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        stats = stats or {}
        checked = int(stats.get("checked") or 0)
        flagged = int(stats.get("flagged") or 0)
        n_open = int(stats.get("open") or 0)
        n_band = int(stats.get("price_band") or 0)
        n_range = int(stats.get("range") or 0)
        examples = [str(e) for e in (stats.get("examples") or [])]
        mode = _ohlc_prewrite_mode()
        tol = _ohlc_prewrite_tol()
        level = "WARNING" if flagged else "INFO"
        status = "SUSPECT" if flagged else "OK"
        msg = (
            f"{_OHLC_PREWRITE_TAG} {page} | checked={checked} "
            f"flagged={flagged} (open={n_open} price_band={n_band} "
            f"range={n_range} w52_band={int(stats.get('w52_band') or 0)}) "
            f"| w52(zero/tol/absent)="
            f"{int(stats.get('w52_zero_band') or 0)}/"
            f"{int(stats.get('w52_tol_excused') or 0)}/"
            f"{int(stats.get('w52_absent') or 0)}"
            + (f" | w52ex: {', '.join(stats.get('w52_examples') or [])}"
               if stats.get('w52_examples') else "")
            + f" | blank(o/h/l)="
            f"{int(stats.get('blank_open') or 0)}/"
            f"{int(stats.get('blank_hi') or 0)}/"
            f"{int(stats.get('blank_lo') or 0)}"
            f" | mode={mode} tol={tol}"
            + (f" | ex: {', '.join(examples[:12])}"
               f"{'…' if flagged > 12 else ''}" if flagged else "")
        )
        details = json.dumps({
            "checked": checked,
            "flagged": flagged,
            "open": n_open,
            "price_band": n_band,
            "range": n_range,
            "blank_open": int(stats.get("blank_open") or 0),
            "blank_hi": int(stats.get("blank_hi") or 0),
            "blank_lo": int(stats.get("blank_lo") or 0),
            "examples": examples[:50],
            "mode": mode,
            "tol": tol,
            "version": SCRIPT_VERSION,
        })
        details = _runlog_meta_json(details)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        body = {"values": [[ts, level, "run_dashboard_sync", page, status,
                            msg, "", "", "", details]]}
        _last_err = None
        for _attempt in (1, 2):
            try:
                svc.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range="'_Run_Log'!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                ).execute()
                _last_err = None
                break
            except Exception as _ae:
                _last_err = _ae
                time.sleep(1.0)
        if _last_err is not None:
            raise _last_err
    except Exception as _e:
        # Same FW-3b discipline: the tripwire's own failure must be LOUD.
        _note_runlog_append_failure("verdict")   # v6.55.0
        print("::warning::%s _Run_Log verdict append FAILED for %s — %s: %s"
              % (_OHLC_PREWRITE_TAG, page, type(_e).__name__, _e))
        logger.warning("%s run-log verdict skipped: %s",
                       _OHLC_PREWRITE_TAG, _e)


def _ohlc_readback_verify(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    page: str,
    headers: list,
    rows_matrix: list,
    start_cell: str,
    prewrite_stats: dict,
) -> Optional[dict]:
    """v6.41.0 W1A-6c — read the page BACK after the write and re-run this
    file's own guard on what actually landed.

    Contract, deliberately narrow:
      * READ-ONLY. Never writes, never mutates rows_matrix, never changes
        res.status. A divergence is evidence, not a verdict.
      * Fail-open, twice: internal try/except here AND at the call site.
        Telemetry must never damage the write path it observes.
      * Reuses _apply_ohlc_prewrite_guard verbatim, in a forced-observe
        window, so prewrite and readback are the SAME predicate. A delta can
        therefore only mean different ROWS, never different rules.

    Returns a delta dict, or None when disabled/unavailable.
    """
    if not _ohlc_readback_enabled() or sheets is None:
        return None
    # v6.41.0 pre-merge audit F2 (ACCEPTED): the delta is only meaningful
    # against a real prewrite baseline. With TFB_SYNC_OHLC_PREWRITE=0 the
    # baseline dict is empty and every readback flag would present as
    # "divergence" against zero — false telemetry. Same self-disable
    # pattern as the ROW_SANITY/BLOCKED_INVARIANT combo matrix.
    if not _ohlc_prewrite_enabled():
        return None
    try:
        max_row = _page_read_row_bound()
        n_cols = max(1, len(headers or []))
        # v6.41.0 review fix (harness E6): _idx_to_a1_col is 1-BASED
        # (1 -> A, 26 -> Z — its own docstring). The pre-review draft passed
        # n_cols-1 and silently truncated the LAST column of every page.
        # v6.41.0 review fix + pre-merge audit F3 (ACCEPTED): honor the
        # task's start_cell on BOTH axes. Six columns written from B5 occupy
        # B..G — the end column is start_index + n_cols - 1, not the absolute
        # n_cols column (the earlier draft read B..F, silently dropping the
        # last column, and the harness accepted it). Unparseable -> A1.
        _m = re.match(r"^\$?([A-Za-z]+)\$?(\d+)$",
                      str(start_cell or "").strip())
        _scol = (_m.group(1).upper() if _m else "A")
        _srow = (int(_m.group(2)) if _m else 1)
        _sidx = 0
        for _ch in _scol:
            _sidx = _sidx * 26 + (ord(_ch) - ord("A") + 1)
        end_col = _idx_to_a1_col(_sidx + n_cols - 1)
        grid = sheets.read_values(
            spreadsheet_id, page,
            f"{_scol}{_srow}:{end_col}{max(max_row, _srow + 1)}")
        if grid is None:
            return {"error": "read_failed"}
        if not grid:
            return {"error": "empty_readback"}
        # v6.52.0: per-symbol Open attribution - direct matrix-vs-sheet diff
        # on the column this leg just wrote. Read-only; capped 60; fail-open.
        _ods = []
        try:
            _si = _guard_find_col(headers, _GUARD_SYMBOL_ALIASES)
            _oi = _guard_find_col(headers, _GUARD_OPEN_ALIASES)
            if _si >= 0 and _oi >= 0:
                _sheet_open = {}
                for _gr in grid:
                    if (isinstance(_gr, list) and _si < len(_gr)
                            and not _guard_is_blank(_gr[_si])):
                        _sheet_open[str(_gr[_si]).strip().upper()] = (
                            str(_gr[_oi]).strip() if _oi < len(_gr) else "")
                for _mr in (rows_matrix or []):
                    if (not isinstance(_mr, list) or _si >= len(_mr)
                            or _guard_is_blank(_mr[_si])):
                        continue
                    _t = str(_mr[_si]).strip().upper()
                    _mo = str(_mr[_oi]).strip() if _oi < len(_mr) else ""
                    _so = _sheet_open.get(_t)
                    if _so is not None and _so != _mo:
                        _ods.append(_t)
                        if len(_ods) >= 60:
                            break
        except Exception:
            _ods = []
        live_hdr = [str(c or "").strip() for c in (grid[0] or [])]
        live_rows = [list(r) for r in grid[1:] if any(
            str(c or "").strip() for c in r)]
        # Force observe for the readback window so the shared guard can never
        # mutate the copy we just read, whatever the operator armed.
        _prev = os.environ.get("TFB_SYNC_OHLC_PREWRITE_MODE")
        os.environ["TFB_SYNC_OHLC_PREWRITE_MODE"] = "observe"
        try:
            _, rb = _apply_ohlc_prewrite_guard(live_hdr, live_rows, page)
        finally:
            if _prev is None:
                os.environ.pop("TFB_SYNC_OHLC_PREWRITE_MODE", None)
            else:
                os.environ["TFB_SYNC_OHLC_PREWRITE_MODE"] = _prev
        pw = prewrite_stats or {}
        return {
            "page": page,
            "prewrite_checked": int(pw.get("checked") or 0),
            "prewrite_flagged": int(pw.get("flagged") or 0),
            "readback_checked": int(rb.get("checked") or 0),
            "readback_flagged": int(rb.get("flagged") or 0),
            "open_diff_n": len(_ods),           # v6.52.0
            "open_diff_syms": list(_ods),        # v6.52.0
            "delta_flagged": int(rb.get("flagged") or 0)
            - int(pw.get("flagged") or 0),
            "delta_checked": int(rb.get("checked") or 0)
            - int(pw.get("checked") or 0),
            "readback_open": int(rb.get("open") or 0),
            "readback_price_band": int(rb.get("price_band") or 0),
            "readback_range": int(rb.get("range") or 0),
            "readback_blank_open": int(rb.get("blank_open") or 0),
            "prewrite_blank_open": int(pw.get("blank_open") or 0),
            "matrix_rows": len(rows_matrix or []),
            "examples": [str(e) for e in (rb.get("examples") or [])][:12],
        }
    except Exception as _e:
        return {"error": "%s: %s" % (type(_e).__name__, _e)}


def _ohlc_readback_status(delta: dict) -> tuple:
    """v6.41.0 pre-merge audit F4 (ACCEPTED): (level, status) for a readback
    delta. The first draft labelled every non-positive delta "MATCHED",
    which mislabelled enforcement cleanup (negative delta) and hid pure
    row-count divergence. Taxonomy:
        DIVERGENT  d > 0                 (WARNING)  more offenses landed
        REDUCED    d < 0                 (INFO)     cleanup/enforcement seen
        ROWS_DELTA d == 0, rows differ   (WARNING if extra rows else INFO)
        MATCHED    d == 0, rows equal    (INFO)
    """
    d = int(delta.get("delta_flagged") or 0)
    dc = int(delta.get("delta_checked") or 0)
    if d > 0:
        return "WARNING", "DIVERGENT"
    if d < 0:
        return "INFO", "REDUCED"
    if dc != 0:
        return ("WARNING" if dc > 0 else "INFO"), "ROWS_DELTA"
    return "INFO", "MATCHED"


def _append_runlog_ohlc_readback(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    page: str,
    delta: dict,
) -> None:
    """v6.41.0 W1A-6c: durable _Run_Log line for the readback, same channel
    and same fail-loud-but-fail-open discipline as W1A-6b's appender."""
    if not delta or sheets is None:
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        if delta.get("error"):
            msg = (f"{_OHLC_READBACK_TAG} {page} | UNAVAILABLE "
                   f"({delta['error']})")
            level, status = "WARNING", "UNKNOWN"
        else:
            d = int(delta.get("delta_flagged") or 0)
            msg = (
                f"{_OHLC_READBACK_TAG} {page} | "
                f"prewrite={delta['prewrite_flagged']}"
                f"/{delta['prewrite_checked']} "
                f"readback={delta['readback_flagged']}"
                f"/{delta['readback_checked']} "
                f"delta={d:+d} rows_delta={delta['delta_checked']:+d} | "
                f"open={delta['readback_open']} "
                f"band={delta['readback_price_band']} "
                f"range={delta['readback_range']} "
                f"blank_open(rb/pw)={delta['readback_blank_open']}"
                f"/{delta['prewrite_blank_open']}"
                + (f" | ex: {', '.join(delta['examples'])}"
                   if delta.get("examples") else ""))
            level, status = _ohlc_readback_status(delta)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        body = {"values": [[ts, level, "run_dashboard_sync", page, status,
                            msg, "", "", "",
                            _runlog_meta_json(json.dumps(dict(delta,
                                            version=SCRIPT_VERSION)))]]}
        _last = None
        for _ in (1, 2):
            try:
                svc.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range="'_Run_Log'!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                ).execute()
                _last = None
                break
            except Exception as _ae:
                _last = _ae
                time.sleep(1.0)
        if _last is not None:
            raise _last
    except Exception as _e:
        _note_runlog_append_failure("line")   # v6.55.0
        print("::warning::%s _Run_Log append FAILED for %s — %s: %s"
              % (_OHLC_READBACK_TAG, page, type(_e).__name__, _e))
        logger.warning("%s run-log line skipped: %s", _OHLC_READBACK_TAG, _e)


# =============================================================================
# v6.45.0 (R1/R2/R5) — SYNC-HOLD, READBACK-REPAIR, RUN-META
# =============================================================================
import hashlib as _sh_hashlib
import uuid as _sh_uuid

_RUN_ID = _sh_uuid.uuid4().hex[:12]
_SYNC_HOLD_TAG = f"[SYNC-HOLD v{SCRIPT_VERSION}]"
_OHLC_REPAIR_TAG = f"[OHLC-REPAIR v{SCRIPT_VERSION}]"
_SH_KEY = "backend sync hold until"
_SH_KEY_NORM = "backendsyncholduntil"
_SH_STATE = {"active": False, "row": None}


def _runlog_meta_json(details: str) -> str:
    """v6.45.0 R5: inject run_id + ts_utc into a Details-JSON string.
    FAIL-OPEN: any parse error returns the input unchanged."""
    try:
        d = json.loads(details or "{}")
        if isinstance(d, dict):
            d.setdefault("run_id", (os.getenv("GITHUB_RUN_ID") or "").strip()
                         or _RUN_ID)
            d.setdefault("ts_utc", datetime.now(timezone.utc).isoformat())
            return json.dumps(d)
    except Exception:
        pass
    return details


def _payload_sha8(headers: list, rows_matrix: list) -> str:
    """v6.45.0 R4: stable 8-hex fingerprint of the written payload."""
    try:
        h = _sh_hashlib.sha256()
        h.update(("|".join(str(x) for x in (headers or []))).encode(
            "utf-8", "replace"))
        for r in (rows_matrix or []):
            h.update(("|".join("" if c is None else str(c)
                               for c in (r if isinstance(r, list) else [r]))
                      ).encode("utf-8", "replace"))
        return h.hexdigest()[:8]
    except Exception:
        return ""


def _sync_hold_enabled() -> bool:
    """v6.45.0 R1 master gate. DEFAULT OFF — unset/0/false/off keeps the
    v6.44.1 write path byte-identical (no _Sync_Control writes at all)."""
    return (os.getenv("TFB_SYNC_WRITE_SENTINEL") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _sync_hold_ttl_sec() -> int:
    try:
        v = int(float((os.getenv("TFB_SYNC_HOLD_TTL_SEC") or "180").strip()))
    except Exception:
        v = 180
    return min(900, max(30, v))


def _sync_hold_find_row(grid) -> int:
    """1-based row index of the backend hold key in _Sync_Control, or -1."""
    try:
        for i, row in enumerate(grid or [], start=1):
            if not isinstance(row, list) or not row:
                continue
            key = re.sub(r"[^a-z0-9]+", "", str(row[0] or "").lower())
            if key == _SH_KEY_NORM:
                return i
    except Exception:
        pass
    return -1


def _sync_hold_owner_token() -> str:
    """v6.48.0: stable identity for this writer process.
    v6.49.0: sanitized to [A-Za-z0-9_.:-] and capped at 64 chars - the leg
    component may arrive as comma-separated TFB_SYNC_PAGE_ORDER, and the
    owners= ledger reserves ';' between entries and '@' before expiries."""
    rid = (os.getenv("GITHUB_RUN_ID") or "local").strip() or "local"
    leg = (os.getenv("TFB_SYNC_LEG") or os.getenv("TFB_SYNC_PAGE_ORDER")
           or "leg").strip() or "leg"
    tok = f"{rid}:{leg}:{os.getpid()}"
    return re.sub(r"[^A-Za-z0-9_.:-]", "-", tok)[:64]


def _sync_hold_post_grace_sec() -> int:
    """v6.48.0: seconds the hold outlives a successful write (default 45).
    0 = v6.47 behaviour (immediate blank). Invalid input -> default."""
    raw = (os.getenv("TFB_SYNC_HOLD_POST_GRACE_SEC") or "").strip()
    if not raw:
        return 45
    try:
        v = int(float(raw))
        return v if v >= 0 else 45
    except (TypeError, ValueError):
        return 45


def _sync_hold_note_owner(note: str) -> str:
    """v6.48.0: extract owner=<tok> from a note cell ('' if absent)."""
    for _tok in str(note or "").split():
        if _tok.startswith("owner="):
            return _tok[6:]
    return ""


def _sync_hold_parse_owners(note: str, b_iso: str) -> dict:
    """v6.49.0: parse the owners= ledger in note column C into
    {token: aware-UTC expiry}. Entries are 'tok@ISO' joined by ';'.
    Legacy v6.48 cells carry a bare 'owner=tok' with the expiry living
    only in column B - synthesized here as one live entry at B so a
    mixed-version overlap still counts the old writer. Unparseable
    entries are dropped; every path is FAIL-OPEN."""
    out: dict = {}
    txt = str(note or "")
    ledger = ""
    for _tok in txt.split():
        if _tok.startswith("owners="):
            ledger = _tok[7:]
            break
    if ledger:
        for _ent in ledger.split(";"):
            if "@" not in _ent:
                continue
            _t, _, _iso = _ent.partition("@")
            _t = _t.strip()
            if not _t:
                continue
            try:
                _exp = datetime.fromisoformat(_iso.strip())
            except Exception:
                continue
            if _exp.tzinfo is None:
                _exp = _exp.replace(tzinfo=timezone.utc)
            out[_t] = _exp
        return out
    _legacy = _sync_hold_note_owner(txt)
    if _legacy:
        try:
            _exp = datetime.fromisoformat(str(b_iso or "").strip())
        except Exception:
            _exp = None
        if _exp is not None:
            if _exp.tzinfo is None:
                _exp = _exp.replace(tzinfo=timezone.utc)
            out[re.sub(r"[^A-Za-z0-9_.:-]", "-", _legacy)[:64]] = _exp
    return out


def _sync_hold_fmt_owners(owners: dict) -> str:
    """v6.49.0: serialize {token: expiry} to 'owners=tok@ISO;...' sorted
    by (expiry, token) for deterministic cells; '' when empty. Tokens are
    pre-sanitized to [A-Za-z0-9_.:-] so ';' and '@' stay structural and
    the whole ledger is one whitespace-free word."""
    if not owners:
        return ""
    _items = sorted(owners.items(),
                    key=lambda kv: (kv[1].isoformat(), kv[0]))
    return "owners=" + ";".join(
        t + "@" + e.isoformat() for t, e in _items)


def _append_runlog_sync_hold(sheets: Any, spreadsheet_id: str, level: str,
                             status: str, msg: str) -> None:
    """v6.47.0: best-effort, fail-open _Run_Log line so the hold lifecycle
    is auditable from workbook exports alone."""
    try:
        svc = sheets._get_service()
        if not svc:
            return
        svc.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="'_Run_Log'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [[
                datetime.now(timezone.utc).isoformat(),
                level, "sync_hold", "ALL", status,
                f"{_SYNC_HOLD_TAG} {msg}", "", "", "",
                _runlog_meta_json(json.dumps({"version": SCRIPT_VERSION})),
            ]]},
        ).execute()
    except Exception as _e:
        # v6.55.0: was fully silent — the hold lifecycle lines were part of the
        # evidence that vanished on 2026-08-30. Still fail-open, now counted.
        _note_runlog_append_failure("sync_hold")
        print("::warning::%s _Run_Log append FAILED (sync_hold) — %s: %s"
              % (_SYNC_HOLD_TAG, type(_e).__name__, _e))


def _sync_hold_publish(sheets: Any, spreadsheet_id: str, page: str) -> None:
    """v6.45.0 R1: publish the backend write-window hold. FAIL-OPEN - a
    publish failure must never block the write it protects.
    v6.49.0: OWNER-SET LEASE. Column C carries an owners= ledger (one
    'token@expiry' entry per live writer); column B stays a PURE ISO
    timestamp equal to max(live expiries) - the only cell the deployed
    GAS parser reads. Publish drops expired entries, upserts ONLY this
    process' entry, and NEVER adopts a foreign identity:
    _SH_STATE['owner'] is always our own token. Closes the v6.48 defect
    (external audit F-03) where an adopted token let the first-finishing
    leg pass the clear-time ownership check and shorten a lease it did
    not own. A lost read-modify-write race is detected by re-reading C
    for our own token and the merge is retried once."""
    if not _sync_hold_enabled() or sheets is None:
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        row_i = _SH_STATE.get("row")
        if not row_i:
            grid = []
            try:
                _resp = svc.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{_MH_SHEET}'!A1:C12").execute()
                grid = _resp.get("values", []) or []
            except Exception:
                grid = []
            row_i = _sync_hold_find_row(grid)
            if row_i < 1:
                svc.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{_MH_SHEET}'!A1:C1",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [[_SH_KEY, "", ""]]}).execute()
                row_i = max(1, len(grid) + 1)
            _SH_STATE["row"] = row_i
        _td = __import__("datetime").timedelta
        owner = _sync_hold_owner_token()
        _SH_STATE["owner"] = owner  # v6.49.0: always our OWN token
        until = ""
        owners_live = 0
        for _attempt in (0, 1):
            _now = datetime.now(timezone.utc)
            _biso, _cnote = "", ""
            try:
                _cur = svc.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range="'" + _MH_SHEET + "'!B" + str(row_i) + ":C"
                          + str(row_i)).execute()
                _row = ((_cur.get("values") or [["", ""]])[0] or ["", ""])
                _biso = str((_row[0] if len(_row) > 0 else "") or "").strip()
                _cnote = str((_row[1] if len(_row) > 1 else "") or "")
            except Exception:
                pass
            owners = _sync_hold_parse_owners(_cnote, _biso)
            owners = {t: e for t, e in owners.items() if e > _now}
            owners[owner] = _now + _td(seconds=_sync_hold_ttl_sec())
            until = max(owners.values()).isoformat()
            owners_live = len(owners)
            note = (f"{_SYNC_HOLD_TAG} held {page} " + _now.isoformat()
                    + " " + _sync_hold_fmt_owners(owners))
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{_MH_SHEET}'!A{row_i}:C{row_i}",
                valueInputOption="RAW",
                body={"values": [[_SH_KEY, until, note]]}).execute()
            _vc = ""
            try:
                _vr = svc.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range="'" + _MH_SHEET + "'!C" + str(row_i)).execute()
                _vv = (_vr.get("values") or [[""]])
                _vc = str((_vv[0] or [""])[0]) if _vv else ""
            except Exception:
                _vc = ""
            if (owner + "@") in _vc or _attempt == 1:
                break
        _SH_STATE["active"] = True
        verified = ""
        try:  # v6.47.0: read the cell back - the publish proves itself.
            _vr = svc.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range="'" + _MH_SHEET + "'!B" + str(row_i)).execute()
            _vv = (_vr.get("values") or [[""]])
            verified = str((_vv[0] or [""])[0]) if _vv else ""
        except Exception as _ve:
            verified = "<read-back failed: " + type(_ve).__name__ + ">"
        logger.info("%s published for %s until %s (row %s) owner=%s "
                    "owners_live=%s verified=%s",
                    _SYNC_HOLD_TAG, page, until, row_i, owner,
                    owners_live, verified)
        _append_runlog_sync_hold(
            sheets, spreadsheet_id, "INFO", "HELD",
            "published page=" + page + " until=" + until +
            " row=" + str(row_i) + " owner=" + owner +
            " owners_live=" + str(owners_live) +
            " verified=" + repr(verified))
    except Exception as _e:
        print("::warning::%s publish failed for %s — %s: %s"
              % (_SYNC_HOLD_TAG, page, type(_e).__name__, _e))
        logger.warning("%s publish skipped: %s", _SYNC_HOLD_TAG, _e)
        _append_runlog_sync_hold(
            sheets, spreadsheet_id, "WARNING", "PUBLISH_FAILED",
            "page=" + page + " " + type(_e).__name__ + ": " + str(_e))


def _sync_hold_clear(sheets: Any, spreadsheet_id: str) -> None:
    """v6.45.0 R1: clear the hold. No-op unless a publish succeeded this
    run. FAIL-OPEN; TTL is the crash backstop.
    v6.49.0: removes ONLY this process' entry from the owners= ledger.
    While other live entries remain, column B is rewritten to
    max(remaining expiries) - a finishing leg can never shorten another
    leg's lease (AT-02 monotonicity). Grace applies only when the ledger
    empties; grace=0 blanks the cell (v6.47 behaviour). CLEAR_SKIPPED is
    retired: ledger self-removal replaces the v6.48 foreign-skip path."""
    if not _SH_STATE.get("active") or sheets is None:
        return
    _remaining = -1
    row_i = _SH_STATE.get("row")
    my_owner = str(_SH_STATE.get("owner") or "")
    try:
        svc = sheets._get_service()
        if svc and row_i:
            _biso, _cnote = "", ""
            try:
                _cur = svc.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range="'" + _MH_SHEET + "'!B" + str(row_i) + ":C"
                          + str(row_i)).execute()
                _row = ((_cur.get("values") or [["", ""]])[0] or ["", ""])
                _biso = str((_row[0] if len(_row) > 0 else "") or "").strip()
                _cnote = str((_row[1] if len(_row) > 1 else "") or "")
            except Exception:
                pass
            _now = datetime.now(timezone.utc)
            owners = _sync_hold_parse_owners(_cnote, _biso)
            owners = {t: e for t, e in owners.items()
                      if e > _now and t != my_owner}
            _remaining = len(owners)
            _td = __import__("datetime").timedelta
            if owners:
                new_b = max(owners.values()).isoformat()
                note = (f"{_SYNC_HOLD_TAG} write done; live="
                        + str(_remaining) + " " + _now.isoformat()
                        + " " + _sync_hold_fmt_owners(owners))
            else:
                grace = _sync_hold_post_grace_sec()
                if grace > 0:
                    new_b = (_now + _td(seconds=grace)).isoformat()
                    note = (f"{_SYNC_HOLD_TAG} write done; grace "
                            + str(grace) + "s " + _now.isoformat())
                else:
                    new_b = ""
                    note = (f"{_SYNC_HOLD_TAG} cleared "
                            + _now.isoformat())
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{_MH_SHEET}'!B{row_i}:C{row_i}",
                valueInputOption="RAW",
                body={"values": [[new_b, note]]}).execute()
        _SH_STATE["active"] = False
        logger.info("%s cleared (row %s) owner=%s remaining=%s",
                    _SYNC_HOLD_TAG, row_i, my_owner, _remaining)
        _append_runlog_sync_hold(
            sheets, spreadsheet_id, "INFO", "CLEARED",
            "row=" + str(row_i) + " owner=" + my_owner +
            " remaining=" + str(_remaining))
    except Exception as _e:
        _SH_STATE["active"] = False
        print("::warning::%s clear failed — %s: %s (TTL will expire it)"
              % (_SYNC_HOLD_TAG, type(_e).__name__, _e))
        _append_runlog_sync_hold(sheets, spreadsheet_id, "WARNING",
                                 "CLEAR_FAILED",
                                 type(_e).__name__ + ": " + str(_e) +
                                 " (TTL expires it)")


def _ohlc_prewrite_enforce_classes() -> frozenset:
    """v6.45.0 R3/R6: offense classes enforce may MUTATE. Default = all
    three = v6.44.1 enforce semantics byte-identical. CSV restricted to
    {open, price_band, range}; empty/invalid input fails SAFE to default.
    Observe-mode counters and log lines are unaffected in every setting."""
    default = frozenset({"open", "price_band", "range"})
    # v6.50.0: w52_band is SELECTABLE but NOT default - an unset/blank env
    # keeps v6.49 enforce semantics byte-identical. Observe first, arm on
    # measured evidence.
    allowed = frozenset({"open", "price_band", "range", "w52_band"})
    raw = (os.getenv("TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES") or "").strip()
    if not raw:
        return default
    out = {t.strip().lower() for t in raw.split(",") if t.strip()}
    out &= allowed
    return frozenset(out) if out else default


def _readback_repair_mode() -> str:
    """v6.45.0 R2: 'repair' only on the exact token; anything else = off."""
    raw = (os.getenv("TFB_SYNC_READBACK_REPAIR") or "").strip().lower()
    return "repair" if raw == "repair" else "off"


def _append_runlog_ohlc_repair(sheets: Any, spreadsheet_id: str, page: str,
                               info: dict) -> None:
    """v6.45.0 R2: durable line for the repair pass, same channel/discipline
    as the readback appender."""
    if not info or sheets is None:
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        level = "WARNING" if info.get("warn") else "INFO"
        status = "REPAIRED" if not info.get("warn") else "REPAIR_PARTIAL"
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        body = {"values": [[ts, level, "run_dashboard_sync", page, status,
                            str(info.get("line") or ""), "", "", "",
                            _runlog_meta_json(json.dumps(dict(
                                info, version=SCRIPT_VERSION)))]]}
        for _ in (1, 2):
            try:
                svc.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range="'_Run_Log'!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body).execute()
                return
            except Exception:
                time.sleep(1.0)
    except Exception as _e:
        logger.warning("%s run-log line skipped: %s", _OHLC_REPAIR_TAG, _e)
        _note_runlog_append_failure("ohlc_repair")   # v6.55.0
        print("::warning::%s _Run_Log append FAILED (ohlc_repair) — %s: %s"
              % (_OHLC_REPAIR_TAG, type(_e).__name__, _e))


def _ohlc_readback_repair(sheets: Any, spreadsheet_id: str, page: str,
                          headers: list, rows_matrix: list,
                          start_cell: str, delta: dict) -> Optional[dict]:
    """v6.45.0 R2 — restore the OHLC trio from the payload after a DIVERGENT
    readback, then re-verify ONCE.

    Contract:
      * Acts only when TFB_SYNC_READBACK_REPAIR=repair AND the readback
        verdict is DIVERGENT with a real baseline (prewrite armed).
      * Touches ONLY the Open / Day High / Day Low columns, over exactly the
        row span this run wrote (start_row+1 .. +len(rows_matrix)). Every
        other column and any row outside the span is untouched by
        construction. Payload None -> "" (explicit clear, FG semantics).
      * Bounded: <=3 values.update calls (1 when the trio is contiguous),
        one re-verify, one _Run_Log line. One pass per page per run.
      * FAIL-OPEN end to end: any error returns {'error': ...} and the
        write's verdict stands.
    """
    try:
        if _readback_repair_mode() != "repair":
            return None
        if not delta or delta.get("error") or not _ohlc_prewrite_enabled():
            return None
        _lvl, _st = _ohlc_readback_status(delta)
        if _st != "DIVERGENT":
            return None
        if not headers or not rows_matrix or sheets is None:
            return None
        svc = sheets._get_service()
        if not svc:
            return None
        hdr = list(headers)
        open_i = _guard_find_col(hdr, _GUARD_OPEN_ALIASES)
        hi_i = _guard_find_col(hdr, _GUARD_DAYHIGH_ALIASES)
        lo_i = _guard_find_col(hdr, _GUARD_DAYLOW_ALIASES)
        cols = sorted(i for i in (open_i, hi_i, lo_i) if i >= 0)
        if not cols:
            return None
        _m = re.match(r"^\$?([A-Za-z]+)\$?(\d+)$",
                      str(start_cell or "").strip())
        _scol = (_m.group(1).upper() if _m else "A")
        _srow = (int(_m.group(2)) if _m else 1)
        _sidx = 0
        for _ch in _scol:
            _sidx = _sidx * 26 + (ord(_ch) - ord("A") + 1)
        r0 = _srow + 1                      # first data row (headers at _srow)
        r1 = _srow + len(rows_matrix)
        def _cell(row, ci):
            v = row[ci] if (isinstance(row, list) and ci < len(row)) else None
            return "" if v is None else v
        updates = 0
        contiguous = (len(cols) >= 2 and cols == list(
            range(cols[0], cols[0] + len(cols))))
        if contiguous:
            a = _idx_to_a1_col(_sidx + cols[0])
            b = _idx_to_a1_col(_sidx + cols[-1])
            vals = [[_cell(r, ci) for ci in cols] for r in rows_matrix]
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{page}'!{a}{r0}:{b}{r1}",
                valueInputOption="RAW",
                body={"majorDimension": "ROWS", "values": vals}).execute()
            updates = 1
        else:
            for ci in cols:
                a = _idx_to_a1_col(_sidx + ci)
                vals = [[_cell(r, ci)] for r in rows_matrix]
                svc.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{page}'!{a}{r0}:{a}{r1}",
                    valueInputOption="RAW",
                    body={"majorDimension": "ROWS", "values": vals}).execute()
                updates += 1
        pw = {"checked": int(delta.get("prewrite_checked") or 0),
              "flagged": int(delta.get("prewrite_flagged") or 0),
              "blank_open": int(delta.get("prewrite_blank_open") or 0)}
        rb2 = _ohlc_readback_verify(sheets, spreadsheet_id, page, headers,
                                    rows_matrix, start_cell, pw) or {}
        after = (int(rb2.get("readback_flagged"))
                 if rb2.get("readback_flagged") is not None
                 and not rb2.get("error") else None)
        before = int(delta.get("readback_flagged") or 0)
        warn = (after is None) or (after > pw["flagged"])
        line = (f"{_OHLC_REPAIR_TAG} {page} | restored ohlc cols={len(cols)} "
                f"rows={len(rows_matrix)} updates={updates} | "
                f"flagged rb:{before}->"
                f"{after if after is not None else '?'} (pw={pw['flagged']})")
        info = {"page": page, "cols": len(cols), "rows": len(rows_matrix),
                "updates": updates, "before_flagged": before,
                "after_flagged": after, "pw_flagged": pw["flagged"],
                "warn": bool(warn), "line": line}
        try:
            _append_runlog_ohlc_repair(sheets, spreadsheet_id, page, info)
        except Exception:
            pass
        return info
    except Exception as _e:
        logger.warning("%s failed on %s — %s: %s", _OHLC_REPAIR_TAG, page,
                       type(_e).__name__, _e)
        return {"error": f"{type(_e).__name__}: {_e}"}


# =============================================================================
# v6.39.0 (W1A-4b) — BACKEND _Status STAMP AT THE WRITE SEAM
# -----------------------------------------------------------------------------
# WHY (2026-08-17 `_Status` export, adjudicated against the same morning's
# page exports and _Run_Log): `_Status` is treated project-wide as the
# freshness contract, and it is not one. It records the last *GAS* action per
# page; the BACKEND sync legs write the pages and never stamp it. Measured
# the same morning:
#     page            _Status says          actually written by a sync leg
#     Market_Leaders  8/4/2026   255 rows   07:53:49   255 rows   (13d stale)
#     Commodities_FX  7/14/2026  251 rows   07:57:38   453 rows   (34d, -202)
#     Mutual_Funds    7/22/2026    1 row    08:19:09  2474 rows   (26d, -2473)
# The Mutual_Funds row is a single-row GAS refresh ("Selected row refreshed:
# UCRD.US", Rows=1) from 22 July standing as the whole page's status ever
# since. Global_Markets simultaneously reports Status=PARTIAL "paused at 290
# of 6626" AND Rows=6626 — `Rows` is the sheet's physical height, not what
# was refreshed, so a consumer reading it for coverage gets 6,626 and
# concludes the page is complete (IR-044).
#
# CONSEQUENCE ALREADY BANKED: the W2 coverage certificate cannot be built as
# a CONSUMER of `_Status`; the stamp has to be emitted by the writer. This is
# that emitter, and it is deliberately the same seam v6.38.0 proved out —
# after write_table() returns and the page verdict is decided, so the stamp
# reports the OUTCOME rather than the intention.
#
# WHAT IT WRITES: columns A..J of the page's own row (Page, Last Updated,
# Status, Message, Endpoint, HTTP Code, Rows, Columns, Duration ms,
# Warnings). Columns K.. — including the L/M global key-value block — are
# NEVER touched: the update range is bounded to A:J. If the page has no row,
# one is appended. Rows carries rows_WRITTEN (the IR-044 fix); Endpoint
# carries "backend:run_dashboard_sync" so a reader can always tell which
# layer authored the row; Message carries version, leg status, written/failed
# and the run id when present.
#
# GATE: TFB_SYNC_STATUS_STAMP, DEFAULT OFF — unset/0 => v6.38.0 behaviour
# byte-identical, no extra API call. Fail-open by construction: every failure
# path is swallowed with a ::warning:: annotation, exactly like FW-3b. A
# status stamp must never break the write path it reports on.
# =============================================================================

_STATUS_STAMP_TAG = f"[STATUS-STAMP v{SCRIPT_VERSION}]"
_STATUS_SHEET_NAME = "_Status"
_STATUS_STAMP_ENDPOINT = "backend:run_dashboard_sync"


def _status_truth_enabled() -> bool:
    """v6.51.0 gate: TFB_SYNC_STATUS_TRUTH=1 arms AT-07 semantics - the
    Status cell and the Decision-Feed per-page token consume the cohort
    verdict. DEFAULT OFF: unset/0 keeps v6.50.0 behaviour byte-identical."""
    return (os.getenv("TFB_SYNC_STATUS_TRUTH") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _env_echo_line() -> str:
    """v6.56.1 [ENV-ECHO]: effective arming state as the script sees it. The
    values are the parsed results of the same readers the guards use, so a
    Variable that never reached env shows up here as its default."""
    def _raw(k):
        v = os.getenv(k)
        return "unset" if v is None else repr(v.strip())[:24]
    try:
        return (f"[ENV-ECHO v{SCRIPT_VERSION}] "
                f"status_truth={_status_truth_enabled()} "
                f"status_stamp={_status_stamp_enabled()} "
                f"ohlc_prewrite={_ohlc_prewrite_enabled()} "
                f"ohlc_readback={_ohlc_readback_enabled()} "
                f"fill_guard={_ohlc_fillguard_enabled()}({_raw('TFB_SYNC_OHLC_FILL_GUARD')}) "
                f"fill_mode={_ohlc_fillguard_mode()}({_raw('TFB_SYNC_OHLC_FILL_GUARD_MODE')}) "
                f"rb_tol_rows={_raw('TFB_SYNC_RB_TOL_ROWS')} rb_tol_pct={_raw('TFB_SYNC_RB_TOL_PCT')} "
                f"rb_tol@6609={_rb_tolerance_env(6609)}")
    except Exception as exc:  # noqa: BLE001
        return f"[ENV-ECHO v{SCRIPT_VERSION}] unavailable: {exc}"


def _rb_tolerance_env(rb_checked: int) -> int:
    """v6.56.0 [RB-TOLERANCE]: bounded write-survival residual that still
    counts as COMPLETE. max(TFB_SYNC_RB_TOL_ROWS, ceil(rb_checked *
    TFB_SYNC_RB_TOL_PCT / 100)). Both DEFAULT 0 -> 0 (byte-identical)."""
    try:
        rows_abs = int(float((os.getenv("TFB_SYNC_RB_TOL_ROWS") or "0").strip()))
    except Exception:  # noqa: BLE001
        rows_abs = 0
    try:
        pct = float((os.getenv("TFB_SYNC_RB_TOL_PCT") or "0").strip())
    except Exception:  # noqa: BLE001
        pct = 0.0
    rows_abs = max(0, rows_abs)
    pct_rows = 0
    if pct > 0 and rb_checked > 0:
        pct_rows = int(-(-(rb_checked * pct) // 100.0))  # ceil without math
    return max(rows_abs, pct_rows)


def _rb_divergence_tolerated(meta: dict, pw_fl: int):
    """v6.56.0: (tolerated: bool, tol: int, delta: int). Tolerated iff the
    readback counters exist, tol > 0 and 0 <= rb_flagged - pw_flagged <= tol.
    Any missing counter -> not tolerated (never false-green)."""
    try:
        m = meta or {}
        if m.get("rb_checked") is None or m.get("rb_flagged") is None:
            return False, 0, 0
        rb_fl = int(m.get("rb_flagged") or 0)
        checked = int(m.get("rb_checked") or 0)
        tol = _rb_tolerance_env(checked)
        delta = rb_fl - int(pw_fl or 0)
        return (tol > 0 and checked > 0 and 0 <= delta <= tol), tol, delta
    except Exception:  # noqa: BLE001
        return False, 0, 0


def _rb_tolerance_note(meta: dict, pw_fl: int) -> str:
    """v6.56.0: " rb_tol=<tol>(+<delta>)" when the tolerance decided a
    DIVERGENT readback; "" otherwise. Stamp-message only."""
    try:
        rbst = str((meta or {}).get("rb_status") or "").strip().upper()
        if rbst != "DIVERGENT":
            return ""
        ok, tol, delta = _rb_divergence_tolerated(meta, pw_fl)
        return f" rb_tol={tol}(+{delta})" if ok else ""
    except Exception:  # noqa: BLE001
        return ""


def _status_data_verdict(status_lower: str, failed: int, cov, fresh_min,
                         meta: dict) -> str:
    """v6.51.0: THE cohort verdict, factored verbatim from _status_stamp_row
    (v6.45.0 R4) so the stamp message, the Status cell and the feed token can
    never disagree again. COMPLETE only when the leg succeeded, nothing
    failed, refresh coverage met the floor, and the readback matched or was
    repaired back to the prewrite baseline. Any internal error -> PARTIAL
    (never false-green)."""
    try:
        rbst = str((meta or {}).get("rb_status") or "").strip().upper()
        rep_after = (meta or {}).get("repair_after")
        pw_fl = int((meta or {}).get("pw_flagged") or 0)
        out = "COMPLETE"
        if str(status_lower or "") != "success" or int(failed or 0) > 0:
            out = "PARTIAL"
        if cov is not None and float(cov) < float(fresh_min):
            out = "PARTIAL"
        if rbst == "DIVERGENT" and not (
                isinstance(rep_after, int) and 0 <= rep_after <= pw_fl):
            # v6.56.0 [RB-TOLERANCE]: a bounded write-survival residual is
            # COMPLETE; the stamp message records rb_tol=<tol>(+<delta>).
            if not _rb_divergence_tolerated(meta, pw_fl)[0]:
                out = "PARTIAL"
        return out
    except Exception:  # noqa: BLE001
        return "PARTIAL"


def _status_stamp_enabled() -> bool:
    """v6.39.0 W1A-4b: master gate. DEFAULT OFF — unset/0/false/off keeps
    v6.38.0 behaviour byte-identical (no read, no write, no API call)."""
    return (os.getenv("TFB_SYNC_STATUS_STAMP") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _status_stamp_pages() -> set:
    """v6.39.0: optional allow-list. Empty (default) = every page the sync
    writes. TFB_SYNC_STATUS_STAMP_PAGES='Global_Markets,Market_Leaders'."""
    raw = (os.getenv("TFB_SYNC_STATUS_STAMP_PAGES") or "").strip()
    return {p.strip() for p in raw.split(",") if p.strip()}


def _status_find_page_row(grid: list, page: str) -> int:
    """v6.39.0: 1-based sheet row index of the page's own `_Status` row, or
    -1 if absent. Column A is the page key; the header row is skipped by
    exact-match on the page name, so a title/branding row cannot collide."""
    want = str(page or "").strip().casefold()
    if not want:
        return -1
    for i, row in enumerate(grid or []):
        if not isinstance(row, (list, tuple)) or not row:
            continue
        if str(row[0] or "").strip().casefold() == want:
            return i + 1
    return -1


def _status_stamp_row(page: str, res: Any, n_cols: int) -> list:
    """v6.39.0: build the A..J payload for one page. Pure — no I/O, so the
    row shape is unit-testable without a sheet. `Rows` is rows WRITTEN, not
    sheet height (IR-044); a PARTIAL leg can no longer report full
    coverage."""
    status = str(getattr(res, "status", "") or "unknown").strip()
    written = int(getattr(res, "rows_written", 0) or 0)
    failed = int(getattr(res, "rows_failed", 0) or 0)
    warns = list(getattr(res, "warnings", None) or [])
    err = str(getattr(res, "error", "") or "").strip()
    dur_ms = ""
    try:
        _st = getattr(res, "start_utc", "") or ""
        _en = getattr(res, "end_utc", "") or _utc_now().isoformat()
        if _st:
            dur_ms = int(max(0.0, (
                datetime.fromisoformat(str(_en)) - datetime.fromisoformat(str(_st))
            ).total_seconds() * 1000.0))
    except Exception:
        dur_ms = ""
    run_id = (os.getenv("GITHUB_RUN_ID") or "").strip() or _RUN_ID
    # v6.39.1 (external audit P0-3, ACCEPTED): write completeness is NOT
    # refresh completeness. A FLOOR-MERGE leg can write a full page of which
    # only a fraction is fresh — publish fresh/preserved/coverage explicitly
    # and never let values.update success masquerade as a full refresh.
    meta = dict(getattr(res, "_stamp_meta", None) or {})
    # v6.39.5 (F-09): early exits (identity fail, floor veto, guard skips)
    # stamp before the persistence stage populates _stamp_meta — fall back to
    # the leg's own symbols_requested so diagnostics keep the denominator.
    requested = int(meta.get("requested")
                    or getattr(res, "symbols_requested", 0) or 0)
    klg = int(meta.get("klg_kept") or 0)
    preserved = klg + int(meta.get("persist_restored") or 0) + int(
        meta.get("pv2_restored") or 0)
    stubbed = int(meta.get("stubbed") or 0)
    pre_rows = meta.get("pre_persist_rows")
    fresh = max(0, int(pre_rows) - klg) if isinstance(pre_rows, int) else None
    cov = (round(100.0 * fresh / requested, 1)
           if (fresh is not None and requested > 0) else None)
    fresh_min = 95.0
    try:
        fresh_min = float((os.getenv("TFB_SYNC_STATUS_FRESH_MIN") or "95").strip())
    except Exception:
        pass
    status_cell = status.upper() if status else "UNKNOWN"
    if (status_cell == "SUCCESS" and cov is not None and cov < fresh_min):
        status_cell = "PARTIAL_FRESH"
    # v6.45.0 R4: data_status = the cohort verdict a consumer can trust.
    # COMPLETE only when the leg succeeded, nothing failed, refresh coverage
    # met the floor, and the readback either matched or was repaired back to
    # the prewrite baseline. Anything else is PARTIAL — never false-green.
    rbst = str(meta.get("rb_status") or "").strip().upper()
    rep_after = meta.get("repair_after")
    pw_fl = int(meta.get("pw_flagged") or 0)
    # v6.51.0: single source of truth - the same arithmetic now also decides
    # the Status cell (below, gated) and the per-page feed token (AT-07).
    data_status = _status_data_verdict(status.lower(), failed, cov,
                                       fresh_min, meta)
    if (_status_truth_enabled() and data_status == "PARTIAL"
            and status_cell == "SUCCESS"):
        # v6.51.0 AT-07: SUCCESS may never sit over data=PARTIAL.
        status_cell = "PARTIAL"
    msg = (f"{_STATUS_STAMP_TAG} leg={status} written={written} failed={failed}"
           + (f" requested={requested}" if requested else "")
           + (f" fresh={fresh}" if fresh is not None else "")
           + (f" preserved={preserved}" if preserved else "")
           + (f" stubbed={stubbed}" if stubbed else "")
           + (f" fresh_cov={cov}%" if cov is not None else "")
           + (f" warnings={len(warns)}" if warns else "")
           + (f" error={err[:120]}" if err else "")
           + f" | data={data_status}"
           + _rb_tolerance_note(meta, pw_fl)
           + (f" guard=pw:{pw_fl}/{int(meta.get('pw_checked') or 0)}"
              + (f",rb:{int(meta.get('rb_flagged') or 0)}"
                 f"/{int(meta.get('rb_checked') or 0)}"
                 if meta.get("rb_checked") is not None else "")
              + (f",rep:{rep_after}" if rep_after is not None else "")
              if meta.get("pw_checked") is not None else "")
           + (f" sha={meta.get('payload_sha8')}"
              if meta.get("payload_sha8") else "")
           + (f" run={run_id}" if run_id else ""))
    return [
        page,                                       # A Page
        _status_ts_str(),                           # B Last Updated (v6.46.0: +offset)
        status_cell,                                # C Status (PARTIAL_FRESH when refresh coverage < min)
        msg,                                        # D Message
        _STATUS_STAMP_ENDPOINT,                     # E Endpoint
        "",                                         # F HTTP Code
        written,                                    # G Rows  (WRITTEN, not height)
        int(n_cols or 0),                           # H Columns
        dur_ms,                                     # I Duration ms
        len(warns),                                 # J Warnings
    ]


def _status_stamp_should_skip(res: Any) -> str:
    """v6.39.2 (F3): modes whose contract is ZERO workbook writes outrank
    telemetry. Returns the suppression reason, '' to stamp. dry_run rides
    `res` when the runner sets it; MANUAL-HOLD arrives via the leg's
    status/warnings marker (the v6.32.0 convention). Fail-open on
    inspection error: stamp (the enabled/pages gates still apply)."""
    try:
        if bool(getattr(res, "dry_run", False)):
            return "dry-run"
        blob = " ".join(
            [str(getattr(res, "status", "") or "")]
            + [str(w) for w in (getattr(res, "warnings", None) or [])]
        )
        if "MANUAL-HOLD" in blob:
            return "manual-hold"
        # v6.39.5 (F-08): the daily sync deliberately does NOT own
        # decision-owned cockpit pages — the v6.6.0 guard skips their write
        # precisely so the route-owned freshness line survives. A writer that
        # refuses to write a page must also refuse to stamp its status.
        if _DECISION_GUARD_TAG in blob:
            return "decision-owned"
        if "dry-run" in blob.lower() or "dry_run" in blob.lower():
            return "dry-run"
    except Exception:
        return ""
    return ""


def _stamp_page_status(sheets: "SheetsWriter", spreadsheet_id: str, page: str,
                       res: Any, n_cols: int) -> None:
    """v6.39.0 W1A-4b: write the page's own `_Status` row after the page
    write has completed and its verdict is known.

    Bounded to columns A..J of exactly one row, so the L/M global key-value
    block and every other page's row are untouched by construction. Appends
    a row only when the page has none. FAIL-OPEN: any error is annotated and
    swallowed — the stamp must never break the write path it reports on."""
    if not _status_stamp_enabled() or sheets is None:
        return
    allow = _status_stamp_pages()
    if allow and page not in allow:
        return
    _skip = _status_stamp_should_skip(res)
    if _skip:
        print("::notice::%s stamp suppressed (%s) for %s"
              % (_STATUS_STAMP_TAG, _skip, page))
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        payload = _status_stamp_row(page, res, n_cols)
        grid = []
        try:
            _resp = svc.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{_STATUS_SHEET_NAME}'!A:A",
            ).execute()
            grid = _resp.get("values", []) or []
        except Exception as _re:
            print("::warning::%s could not read %s key column for %s — %s: %s"
                  % (_STATUS_STAMP_TAG, _STATUS_SHEET_NAME, page,
                     type(_re).__name__, _re))
            return
        row_i = _status_find_page_row(grid, page)
        if row_i > 0:
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{_STATUS_SHEET_NAME}'!A{row_i}:J{row_i}",
                valueInputOption="RAW",
                body={"values": [payload]},
            ).execute()
            _where = f"row {row_i}"
        else:
            svc.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{_STATUS_SHEET_NAME}'!A1:J1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [payload]},
            ).execute()
            _where = "appended"
        logger.info("%s %s %s | status=%s rows_written=%s", _STATUS_STAMP_TAG,
                    page, _where, payload[2], payload[6])
    except Exception as _e:
        print("::warning::%s stamp FAILED for %s — %s: %s"
              % (_STATUS_STAMP_TAG, page, type(_e).__name__, _e))
        logger.warning("%s stamp skipped for %s: %s", _STATUS_STAMP_TAG, page, _e)


# =============================================================================
# v6.43.0 (W1A-6e) — LAKE PROBE + IDENTITY-REFETCH (see header changelog)
# =============================================================================
_OHLC_LAKE_TAG = f"[OHLC-LAKE v{SCRIPT_VERSION}]"
_IDENTITY_REFETCH_TAG = f"[IDENTITY-REFETCH v{SCRIPT_VERSION}]"


def _identity_refetch_enabled() -> bool:
    """v6.43.0 W1A-6e master gate for the identity-suspect fronting.
    DEFAULT OFF — unset/0/false/off keeps v6.42.0 refresh ordering
    byte-identical. Fetch-ORDER-only when armed; it can never drop a
    symbol, blank a cell, or change what is written."""
    return (os.getenv("TFB_SYNC_IDENTITY_REFETCH") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _ohlc_lake_enabled() -> bool:
    """v6.43.0 W1A-6e: the lake probe rides the W1A-6 gate — armed iff
    TFB_SYNC_OHLC_PREWRITE is armed. TFB_SYNC_OHLC_LAKE=0 is the probe's
    own kill-switch (default ON under the gate). With the W1A-6 gate off
    the probe performs NO read and NO write — v6.42.0 byte-identical."""
    if not _ohlc_prewrite_enabled():
        return False
    return (os.getenv("TFB_SYNC_OHLC_LAKE") or "1").strip().lower() not in (
        "0", "false", "off", "no")


def _identity_suspect_symbols(grid: List[Any], hdr_r: int, sym_i: int,
                              name_i: int) -> Tuple[set, int]:
    """v6.43.0 W1A-6e: pure classifier over the SAME grid block the
    heal-first read already holds. Returns (suspect_symbol_set, n_groups):
    a symbol is suspect when its non-blank, non-fabricated Name is shared
    by >= _name_dedup_min() DISTINCT symbols on the page — the exact
    over-assignment signal the ID-FIREWALL and repair_stores B3 use.
    First occurrence per symbol wins (mirrors the caller's seen-set).
    Fail-safe: ({}, 0) on any problem — the caller's ordering is then
    v6.42.0-identical."""
    try:
        if name_i < 0 or sym_i < 0 or hdr_r < 0:
            return set(), 0
        by_name: Dict[str, set] = {}
        seen: set = set()
        for row in grid[hdr_r + 1:]:
            if not isinstance(row, list) or sym_i >= len(row):
                continue
            raw = row[sym_i]
            if _guard_is_blank(raw):
                continue
            t = str(raw).strip().upper()
            if not t or t in {"SYMBOL", "TICKER"} or t in seen:
                continue
            seen.add(t)
            nm = row[name_i] if name_i < len(row) else ""
            if _guard_is_blank(nm):
                continue
            if _placeholder_guard_enabled() and _name_is_fabricated(nm):
                continue
            key = _guard_norm(nm)
            if not key:
                continue
            by_name.setdefault(key, set()).add(t)
        _min = _name_dedup_min()
        suspects: set = set()
        n_groups = 0
        for _k, syms in by_name.items():
            if len(syms) >= _min:
                n_groups += 1
                suspects |= syms
        return suspects, n_groups
    except Exception:
        return set(), 0


def _ohlc_lake_probe(sheets: Any, spreadsheet_id: str, page: str,
                     headers: List[Any], rows_matrix: List[List[Any]]) -> dict:
    """v6.43.0 W1A-6e: read the live page BEFORE this leg writes and
    attribute the foreign writer's residue. READ-ONLY by construction:
    exactly one read_values call, zero writes, `rows_matrix` never
    touched. Runs the REAL _apply_ohlc_prewrite_guard on the SHEET grid
    under forced-observe (save/force/restore MODE — the proven v6.41.0
    readback reuse), then joins lake vs the OUTGOING matrix by Symbol:
      foreign_open_fill — matrix Open blank, lake Open populated
      foreign_name_diff — both Names non-blank, normalized different
    Fail-open: any error returns {"error": ...} and the write path is
    untouched. Gate checks live in the caller AND here (belt/braces)."""
    stats: dict = {}
    try:
        if not _ohlc_lake_enabled() or sheets is None:
            return {}
        blk = (f"A1:ZZ{_page_read_row_bound()}"
               if _universe_cap_v2_enabled() else "A1:ZZ6000")
        grid = sheets.read_values(spreadsheet_id, page, blk)
        if not grid or not isinstance(grid, list):
            return {"error": "lake read unavailable"}
        hdr_r, l_sym = -1, -1
        for r in range(min(len(grid), 25)):
            row = grid[r] if isinstance(grid[r], list) else []
            idx = _guard_find_col(row, _GUARD_SYMBOL_ALIASES)
            if idx >= 0:
                hdr_r, l_sym = r, idx
                break
        if l_sym < 0:
            return {"error": "lake Symbol column not found"}
        lake_hdr = grid[hdr_r] if isinstance(grid[hdr_r], list) else []
        lake_rows = grid[hdr_r + 1:]
        _prev = os.environ.get("TFB_SYNC_OHLC_PREWRITE_MODE")
        os.environ["TFB_SYNC_OHLC_PREWRITE_MODE"] = "observe"
        try:
            _, _lg = _apply_ohlc_prewrite_guard(lake_hdr, lake_rows, page)
        finally:
            if _prev is None:
                os.environ.pop("TFB_SYNC_OHLC_PREWRITE_MODE", None)
            else:
                os.environ["TFB_SYNC_OHLC_PREWRITE_MODE"] = _prev
        stats["lake_checked"] = int(_lg.get("checked") or 0)
        stats["lake_flagged"] = int(_lg.get("flagged") or 0)
        stats["lake_blank_open"] = int(_lg.get("blank_open") or 0)
        l_open = _guard_find_col(lake_hdr, _GUARD_OPEN_ALIASES)
        l_name = _guard_find_col(lake_hdr, _GUARD_NAME_ALIASES)
        m_sym = _guard_find_col(headers or [], _GUARD_SYMBOL_ALIASES)
        m_open = _guard_find_col(headers or [], _GUARD_OPEN_ALIASES)
        m_name = _guard_find_col(headers or [], _GUARD_NAME_ALIASES)
        lake_map: Dict[str, Tuple[Any, Any]] = {}
        for row in lake_rows:
            if not isinstance(row, list) or l_sym >= len(row):
                continue
            if _guard_is_blank(row[l_sym]):
                continue
            t = str(row[l_sym]).strip().upper()
            if t in lake_map:
                continue
            lake_map[t] = (
                row[l_open] if 0 <= l_open < len(row) else "",
                row[l_name] if 0 <= l_name < len(row) else "",
            )
        fills, ndiffs = [], []
        if m_sym >= 0:
            for row in (rows_matrix or []):
                if not isinstance(row, list) or m_sym >= len(row):
                    continue
                if _guard_is_blank(row[m_sym]):
                    continue
                t = str(row[m_sym]).strip().upper()
                lk = lake_map.get(t)
                if lk is None:
                    continue
                if (m_open >= 0 and m_open < len(row)
                        and _guard_is_blank(row[m_open])
                        and not _guard_is_blank(lk[0])):
                    fills.append(t)
                if (m_name >= 0 and m_name < len(row)
                        and not _guard_is_blank(row[m_name])
                        and not _guard_is_blank(lk[1])
                        and _guard_norm(row[m_name]) != _guard_norm(lk[1])):
                    ndiffs.append(t)
        stats["foreign_open_fill"] = len(fills)
        stats["foreign_name_diff"] = len(ndiffs)
        stats["examples"] = ([f"{t}(open)" for t in fills[:8]]
                             + [f"{t}(name)" for t in ndiffs[:8]])[:12]
        stats["version"] = SCRIPT_VERSION
        return stats
    except Exception as _e:
        return {"error": f"{type(_e).__name__}: {_e}"}


def _append_runlog_ohlc_lake(sheets: Any, spreadsheet_id: str, page: str,
                             stats: dict) -> None:
    """v6.43.0 W1A-6e: one [OHLC-LAKE] line per page per leg through the
    proven FW-3 _Run_Log channel — same 10-column shape, same retry x2,
    same fail-loud-but-fail-open discipline as the W1A-6b appender."""
    if not stats or sheets is None:
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        _ff = int(stats.get("foreign_open_fill") or 0)
        _fn = int(stats.get("foreign_name_diff") or 0)
        _lf = int(stats.get("lake_flagged") or 0)
        level = "WARNING" if (_ff or _fn or _lf) else "INFO"
        status = "SUSPECT" if (_ff or _fn or _lf) else "OK"
        msg = (
            f"{_OHLC_LAKE_TAG} {page} | lake_checked="
            f"{int(stats.get('lake_checked') or 0)} lake_flagged={_lf} "
            f"lake_blank_open={int(stats.get('lake_blank_open') or 0)} | "
            f"foreign_open_fill={_ff} foreign_name_diff={_fn}"
            + ((" | ex: " + ", ".join(stats.get("examples") or []))
               if (stats.get("examples") and (_ff or _fn)) else "")
        )
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        body = {"values": [[ts, level, "run_dashboard_sync", page, status,
                            msg, "", "", "",
                            _runlog_meta_json(json.dumps(
                                dict(stats, version=SCRIPT_VERSION)))]]}
        _last = None
        for _ in (1, 2):
            try:
                svc.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range="'_Run_Log'!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                ).execute()
                _last = None
                break
            except Exception as _ae:
                _last = _ae
                time.sleep(1.0)
        if _last is not None:
            raise _last
    except Exception as _e:
        _note_runlog_append_failure("line")   # v6.55.0
        print("::warning::%s _Run_Log append FAILED for %s — %s: %s"
              % (_OHLC_LAKE_TAG, page, type(_e).__name__, _e))
        logger.warning("%s run-log line skipped: %s", _OHLC_LAKE_TAG, _e)


# =============================================================================
# =============================================================================
# v6.44.0 (W1A-6f) — OHLC BLANK-CELL FILL GUARD (write seam; header changelog)
# =============================================================================
_OHLC_FILLGUARD_TAG = f"[OHLC-FILLGUARD v{SCRIPT_VERSION}]"
_OHLC_FILLGUARD_DEFAULT_COLS: Tuple[str, ...] = ("Open", "Day High", "Day Low")
_OHLC_FILLGUARD_SELFTEST_OK: Optional[bool] = None


def _ohlc_fillguard_enabled() -> bool:
    """v6.44.0 FG-1 master gate. DEFAULT OFF — unset/0/false/off keeps the
    v6.43.0 write path byte-identical (no scan, no mutation, no log line)."""
    return (os.getenv("TFB_SYNC_OHLC_FILL_GUARD") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _ohlc_fillguard_mode() -> str:
    """v6.44.0: 'enforce' only on the exact token; anything else = observe."""
    raw = (os.getenv("TFB_SYNC_OHLC_FILL_GUARD_MODE") or "").strip().lower()
    return "enforce" if raw == "enforce" else "observe"


def _ohlc_fillguard_env_tokens() -> Tuple[str, ...]:
    """v6.44.1 DS-06: raw CSV tokens from TFB_SYNC_OHLC_FILL_GUARD_COLS."""
    raw = (os.getenv("TFB_SYNC_OHLC_FILL_GUARD_COLS") or "").strip()
    out: List[str] = []
    for tok in raw.split(","):
        t = tok.strip()
        if t and t not in out:
            out.append(t)
    return tuple(out)


def _ohlc_fillguard_cols() -> Tuple[str, ...]:
    """v6.44.1 DS-06: guarded header names, ALLOWLIST-RESTRICTED. The env can
    only SELECT WITHIN the default triple (case-insensitive); it can never
    add a column outside it, so enforce can never clear a non-OHLC field."""
    toks = _ohlc_fillguard_env_tokens()
    if not toks:
        return _OHLC_FILLGUARD_DEFAULT_COLS
    allow = {c.casefold(): c for c in _OHLC_FILLGUARD_DEFAULT_COLS}
    out: List[str] = []
    for t in toks:
        c = allow.get(t.casefold())
        if c and c not in out:
            out.append(c)
    return tuple(out) if out else _OHLC_FILLGUARD_DEFAULT_COLS


def _null_clear_scope() -> str:
    """v6.57.0: TFB_SYNC_NULL_CLEAR_SCOPE = ohlc (default) | all."""
    raw = (os.getenv("TFB_SYNC_NULL_CLEAR_SCOPE") or "ohlc").strip().lower()
    return "all" if raw == "all" else "ohlc"


def _null_clear_keep_cols() -> Tuple[str, ...]:
    """v6.57.0: TFB_SYNC_NULL_KEEP_COLS (CSV, case-insensitive) - headers
    excluded from the all-columns scope. Symbol is always excluded."""
    raw = (os.getenv("TFB_SYNC_NULL_KEEP_COLS") or "").strip()
    out: List[str] = []
    for tok in raw.split(","):
        t = tok.strip()
        if t and t not in out:
            out.append(t)
    return tuple(out)


def _null_clear_all_cols(hdr: Sequence[Any]) -> Tuple[str, ...]:
    """v6.57.0: every header except Symbol and the keep list."""
    keep = {c.casefold() for c in _null_clear_keep_cols()} | {"symbol"}
    out: List[str] = []
    for h in (hdr or []):
        hs = str(h).strip()
        if hs and hs.casefold() not in keep and hs not in out:
            out.append(hs)
    return tuple(out)


def _ohlc_fillguard_cols_rejected() -> Tuple[str, ...]:
    """v6.44.1 DS-06: env tokens outside the allowlist (reported, unused)."""
    allow = {c.casefold() for c in _OHLC_FILLGUARD_DEFAULT_COLS}
    return tuple(t for t in _ohlc_fillguard_env_tokens()
                 if t.casefold() not in allow)


def _ohlc_fill_guard_core(hdr: List[Any], matrix: List[List[Any]],
                          mode: str, cols: Sequence[str]) -> Tuple[List[List[Any]], dict]:
    """v6.44.0 FG-1 PURE CORE (env-free; selftest + harness target).
    Scans the FINAL write matrix for None cells in the guarded columns.
    observe -> zero mutation. enforce -> rr[i] = "" for exactly those cells
    (an explicit Sheets clear, defeating the values.update null-skip that
    lets a foreign prior value survive). Returns (matrix, stats)."""
    names = [str(h).strip().casefold() for h in (hdr or [])]
    guard_idx: Dict[int, str] = {}
    for want in cols:
        wf = str(want).strip().casefold()
        for i, nm in enumerate(names):
            if nm == wf:
                guard_idx[i] = want  # v6.44.1 DS-05: case-insensitive, all hits
    stats: dict = {
        "rows": len(matrix or []),
        "cols": sorted(set(guard_idx.values())),
        "configured": list(cols),
        "nulls": {c: 0 for c in cols},
        "total": 0,
        "mode": mode,
        "action": "observed",
        "examples": [],
    }
    if not guard_idx or not matrix:
        return matrix, stats
    sym_i = 0 if (names and names[0] == "symbol") else None
    for rn, rr in enumerate(matrix):
        for ci, cname in guard_idx.items():
            if ci < len(rr) and rr[ci] is None:
                stats["nulls"][cname] = stats["nulls"].get(cname, 0) + 1
                stats["total"] += 1
                if len(stats["examples"]) < 3:
                    who = str(rr[sym_i]) if (sym_i is not None and sym_i < len(rr) and rr[sym_i] not in (None, "")) else f"r{rn}"
                    stats["examples"].append(f"{who}({cname.split()[-1].lower()})")
                if mode == "enforce":
                    rr[ci] = ""
    if mode == "enforce" and stats["total"]:
        stats["action"] = "cleared"
    return matrix, stats


def _ohlc_fill_guard_apply(hdr: List[Any], matrix: List[List[Any]]):
    """v6.44.1 FG-1 env wrapper. OFF -> (matrix, None): the SAME object,
    untouched — byte-identical v6.43.0. Armed: observe NEVER raises
    (fail-open telemetry); ENFORCE fails CLOSED — certification must be
    exactly True (lazy FG-3 run when None), and a core exception RAISES so
    the caller aborts the write and the page keeps its last-good content
    (DS-02 / DS-03)."""
    if not _ohlc_fillguard_enabled():
        return matrix, None
    mode = _ohlc_fillguard_mode()
    cols = _ohlc_fillguard_cols()
    rejected = _ohlc_fillguard_cols_rejected()
    _scope = _null_clear_scope()  # v6.57.0
    if _scope == "all":
        cols = _null_clear_all_cols(hdr)
        rejected = ()
    forced = None
    if mode == "enforce":
        if _OHLC_FILLGUARD_SELFTEST_OK is None:
            _ohlc_fillguard_selftest_()
        if _OHLC_FILLGUARD_SELFTEST_OK is not True:
            mode, forced = "observe", "FAIL->observe"
    try:
        matrix, stats = _ohlc_fill_guard_core(hdr, matrix, mode, cols)
    except Exception as _ce:
        if mode == "enforce":
            logger.error("%s core FAILED under ENFORCE — failing CLOSED, "
                         "write aborted: %s", _OHLC_FILLGUARD_TAG, _ce)
            raise RuntimeError(
                f"{_OHLC_FILLGUARD_TAG} enforce fail-closed: {_ce}") from _ce
        logger.warning("%s core failed under observe (fail-open): %s",
                       _OHLC_FILLGUARD_TAG, _ce)
        return matrix, {"armed": True, "mode": mode,
                        "rows": len(matrix or []), "cols": [],
                        "configured": list(cols), "nulls": {}, "total": 0,
                        "action": "observed", "examples": [],
                        "error": type(_ce).__name__}
    stats["armed"] = True
    stats["scope"] = _scope  # v6.57.0
    if forced:
        stats["selftest"] = forced
    if rejected:
        stats["cols_rejected"] = list(rejected)
    return matrix, stats


def _append_runlog_ohlc_fillguard(sheets: Any, spreadsheet_id: str, page: str,
                                  stats: dict) -> None:
    """v6.44.0 FG-2: one [OHLC-FILLGUARD] line per page per write through the
    proven FW-3 _Run_Log channel — same 10-column shape, same retry x2, same
    fail-loud-but-fail-open discipline as the LAKE/PREWRITE appenders."""
    if not stats or sheets is None:
        return
    if not _ohlc_prewrite_runlog_enabled():
        return  # v6.44.1 DS-10: honor the existing OHLC run-log kill switch
    if not stats.get("cols") and not stats.get("error"):
        return  # v6.44.1 DS-10: no guarded columns on this page — skip noise
    try:
        svc = sheets._get_service()
        if not svc:
            return
        _t = int(stats.get("total") or 0)
        _n = stats.get("nulls") or {}
        level = "WARNING" if _t else "INFO"
        status = "SUSPECT" if (_t and stats.get("action") != "cleared") else "OK"
        msg = (
            f"{_OHLC_FILLGUARD_TAG} {page} | rows={int(stats.get('rows') or 0)} "
            f"nulls(open/high/low)={int(_n.get('Open') or 0)}/"
            f"{int(_n.get('Day High') or 0)}/{int(_n.get('Day Low') or 0)} "
            f"total={_t} | cols={len(stats.get('cols') or [])}/"
            f"{len(stats.get('configured') or [])} | "
            f"action={stats.get('action')} | mode={stats.get('mode')}"
            + (f" | scope={stats['scope']}" if stats.get("scope") == "all" else "")
            + (f" | rejected={','.join(stats['cols_rejected'])}"
               if stats.get("cols_rejected") else "")
            + (f" | error={stats['error']}" if stats.get("error") else "")
            + (f" | selftest={stats['selftest']}" if stats.get("selftest") else "")
            + ((" | ex: " + ", ".join(stats.get("examples") or []))
               if (stats.get("examples") and _t) else "")
        )
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        body = {"values": [[ts, level, "run_dashboard_sync", page, status,
                            msg, "", "", "",
                            _runlog_meta_json(json.dumps(
                                dict(stats, version=SCRIPT_VERSION)))]]}
        _last = None
        for _ in (1, 2):
            try:
                svc.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range="'_Run_Log'!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body=body,
                ).execute()
                _last = None
                break
            except Exception as _ae:
                _last = _ae
                time.sleep(1.0)
        if _last is not None:
            raise _last
    except Exception as _e:
        _note_runlog_append_failure("line")   # v6.55.0
        print("::warning::%s _Run_Log append FAILED for %s — %s: %s"
              % (_OHLC_FILLGUARD_TAG, page, type(_e).__name__, _e))
        logger.warning("%s run-log line skipped: %s", _OHLC_FILLGUARD_TAG, _e)


def _ohlc_fillguard_selftest_() -> bool:
    """v6.44.1 FG-3: prove the REAL core on 4 canned fixtures BEFORE any page
    write. Explicit checks (NOT assert) so python -O cannot hollow the proof
    (DS-04). Failure -> loud log + module flag; _apply then forces observe so
    an unproven guard can never mutate a write (FW-4 lesson)."""
    global _OHLC_FILLGUARD_SELFTEST_OK

    def _ck(cond: bool, msg: str) -> None:
        if not cond:
            raise AssertionError(msg)

    try:
        hdr = ["Symbol", "Open", "Day High", "Day Low", "Target Price"]
        cols = _OHLC_FILLGUARD_DEFAULT_COLS
        m1 = [["AAA", None, 10.0, None, None], ["BBB", 5.0, None, 4.0, 7.0]]
        m1, s1 = _ohlc_fill_guard_core(hdr, m1, "enforce", cols)
        _ck(m1[0][1] == "" and m1[0][3] == "" and m1[1][2] == "", "F1 clear")
        _ck(m1[0][4] is None and m1[1][0] == "BBB" and m1[1][1] == 5.0,
            "F1 untouched")
        _ck(s1["total"] == 3 and s1["action"] == "cleared", "F1 stats")
        m2 = [["CCC", None, 1.0, 2.0, None]]
        m2, s2 = _ohlc_fill_guard_core(hdr, m2, "observe", cols)
        _ck(m2[0][1] is None and s2["total"] == 1
            and s2["action"] == "observed", "F2 observe")
        m3 = [[None, None]]
        m3, s3 = _ohlc_fill_guard_core(["A", "B"], m3, "enforce", cols)
        _ck(m3[0][0] is None and s3["total"] == 0, "F3 inert")
        m4 = [["DDD", None, 2.0, 1.0]]
        m4, s4 = _ohlc_fill_guard_core(["symbol", "open", "DAY HIGH",
                                        "day low"], m4, "enforce", cols)
        _ck(m4[0][1] == "" and s4["total"] == 1, "F4 casefold")
        _OHLC_FILLGUARD_SELFTEST_OK = True
        return True
    except Exception as _e:
        _OHLC_FILLGUARD_SELFTEST_OK = False
        logger.error("%s SELFTEST FAILED — enforce disabled, observe forced: "
                     "%s", _OHLC_FILLGUARD_TAG, _e)
        return False


# v6.40.0 (W1A-4a) — UPSTREAM DECISION-FEED VERDICT (producer)
# =============================================================================
_UPSTREAM_VERDICT_TAG = f"[UPSTREAM-VERDICT v{SCRIPT_VERSION}]"
# Composite key the cockpit reads. Value contract (single cell, RAW):
#   "EXECUTABLE | run=<id> | <YYYY-mm-dd HH:MM:SS> | ML:OK GM:OK CFX:OK MF:OK"
#   "NOT_ACTIONABLE(<reason>) | run=<id> | <ts> | ML:OK GM:STALE_COV ..."
# Per-page key: "TFB Feed <Page>" ->
#   "<STATE> | cov=<pct|n/a> | run=<id> | <ts>"   STATE in
#   OK / PARTIAL / STALE_COV / FAILED / SKIPPED.
_UPSTREAM_VERDICT_KEY = "TFB Decision Feed"
_UV_PAGE_KEY_PREFIX = "TFB Feed "
_UV_BLOCK_RANGE = "L1:M60"          # the documented global key-value block
_UV_SELFCHECK_KEYS = {"backend url", "last global update", "token loaded"}


def _upstream_verdict_enabled() -> bool:
    """v6.40.0 W1A-4a master gate. DEFAULT OFF — unset/0/false/off keeps
    v6.39.5 behaviour byte-identical (no read, no write, no API call)."""
    return (os.getenv("TFB_SYNC_UPSTREAM_VERDICT") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _upstream_verdict_pages() -> list:
    """Ordered required pages. TFB_SYNC_VERDICT_PAGES overrides (csv)."""
    raw = (os.getenv("TFB_SYNC_VERDICT_PAGES") or "").strip()
    if raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    return ["Market_Leaders", "Global_Markets", "Commodities_FX",
            "Mutual_Funds"]


def _upstream_verdict_max_age_min() -> int:
    """Trailing-freshness window for a per-page key to count (default 240
    minutes ≈ one morning cycle across the 3-leg matrix; clamp 30..1440)."""
    try:
        v = int((os.getenv("TFB_SYNC_VERDICT_MAX_AGE_MIN") or "240").strip())
    except Exception:
        v = 240
    return max(30, min(1440, v))


def _uv_page_state(res: Any) -> tuple:
    """(STATE, cov_or_None) for one leg result. Mirrors the stamp's coverage
    arithmetic (v6.39.1 P0-3) without touching the verified stamp code:
    fresh = pre_persist_rows - klg_kept; cov = fresh/requested."""
    status = str(getattr(res, "status", "") or "").strip().lower()
    meta = dict(getattr(res, "_stamp_meta", None) or {})
    requested = int(meta.get("requested")
                    or getattr(res, "symbols_requested", 0) or 0)
    pre = meta.get("pre_persist_rows")
    klg = int(meta.get("klg_kept") or 0)
    cov = None
    if isinstance(pre, int) and requested > 0:
        cov = round(100.0 * max(0, pre - klg) / requested, 1)
    if status == "success":
        fmin = 95.0
        try:
            fmin = float((os.getenv("TFB_SYNC_STATUS_FRESH_MIN") or "95")
                         .strip())
        except Exception:
            pass
        if cov is not None and cov < fmin:
            return "STALE_COV", cov
        if _status_truth_enabled():
            # v6.51.0 AT-07: a success leg whose cohort verdict is PARTIAL
            # (failed rows, coverage floor, or unrepaired DIVERGENT readback)
            # may not feed OK into the EXECUTABLE composite.
            _failed = int(getattr(res, "rows_failed", 0) or 0)
            if _status_data_verdict("success", _failed, cov, fmin,
                                    meta) == "PARTIAL":
                return "PARTIAL", cov
        return "OK", cov
    if status == "partial":
        return "PARTIAL", cov
    if status == "failed":
        return "FAILED", cov
    return "SKIPPED", cov


def _uv_parse_value(val: str) -> tuple:
    """Parse a stored per-page value -> (STATE, epoch_seconds|None).
    Tolerant: unknown shapes -> ("", None) and the page counts unhealthy."""
    try:
        parts = [p.strip() for p in str(val or "").split("|")]
        state = parts[0].split("(")[0].strip().upper() if parts else ""
        ts = None
        for p in parts:
            try:
                ts = time.mktime(time.strptime(p, "%Y-%m-%d %H:%M:%S"))
                break
            except Exception:
                pass
            try:
                # v6.46.0: tolerate an explicit UTC-offset suffix
                # ("2026-08-27 03:11:19+03:00") by parsing the naive prefix;
                # producer and runner share the same zone, so [:19] is exact.
                if len(p) >= 19:
                    ts = time.mktime(time.strptime(p[:19], "%Y-%m-%d %H:%M:%S"))
                    break
            except Exception:
                continue
        return state, ts
    except Exception:
        return "", None


def _uv_compose(page_states: dict, now_epoch: float) -> tuple:
    """Pure composite over {page: (STATE, epoch|None)} -> (verdict, summary).
    EXECUTABLE iff EVERY required page is OK and within the age window.
    First failing page names the reason — deterministic, order = required
    list. Unit-tested in scripts/harness_w1a6.py S12."""
    max_age = _upstream_verdict_max_age_min() * 60.0
    abbr = {"Market_Leaders": "ML", "Global_Markets": "GM",
            "Commodities_FX": "CFX", "Mutual_Funds": "MF"}
    reason = ""
    frags = []
    for page in _upstream_verdict_pages():
        state, ts = page_states.get(page, ("", None))
        label = state or "MISSING"
        if ts is not None and (now_epoch - ts) > max_age:
            label = "AGED"
        frags.append(f"{abbr.get(page, page)}:{label}")
        if not reason and label != "OK":
            reason = f"{label.lower()}:{abbr.get(page, page)}"
    verdict = "EXECUTABLE" if not reason else f"NOT_ACTIONABLE({reason})"
    return verdict, " ".join(frags)


_CAP_STATUS_KEY = "TFB Grid Capacity"          # v6.53.0
_CAP_LIMIT = 10_000_000                        # Google Sheets allocation cap
_CAP_NEAR_PCT = 85.0
_CAP_AT_PCT = 99.5
_CAP_TAG = "[CAPACITY-STATUS v6.53.0]"


def _capacity_status_enabled() -> bool:
    """v6.53.0 kill-switch. DEFAULT ON; TFB_SYNC_CAPACITY_STATUS=0/false/off/no
    restores v6.52.0 byte-identically (no metadata read, no key write)."""
    return (os.getenv("TFB_SYNC_CAPACITY_STATUS") or "1").strip().lower() \
        not in ("0", "false", "off", "no")


def _capacity_allocated(svc: Any, spreadsheet_id: str) -> Optional[int]:
    """v6.53.0: allocated cells = sum(rowCount * columnCount) over all sheets
    (allocation, not content, is what the 10M limit counts). Fail-open."""
    try:
        meta = (svc.spreadsheets()
                .get(spreadsheetId=spreadsheet_id,
                     fields="sheets(properties(gridProperties("
                            "rowCount,columnCount)))")
                .execute())
        total, seen = 0, False
        for sh in (meta.get("sheets") if isinstance(meta, dict) else []) or []:
            try:
                gp = sh["properties"]["gridProperties"]
                total += int(gp["rowCount"]) * int(gp["columnCount"])
                seen = True
            except Exception:
                continue
        return total if seen else None
    except Exception:
        return None


def _capacity_state(alloc: Optional[int]) -> tuple:
    """v6.53.0 PURE: (state, pct). UNKNOWN when alloc is None."""
    if alloc is None:
        return "UNKNOWN", None
    pct = 100.0 * float(alloc) / float(_CAP_LIMIT)
    if pct >= _CAP_AT_PCT:
        return "AT-LIMIT", pct
    if pct >= _CAP_NEAR_PCT:
        return "NEAR-LIMIT", pct
    return "OK", pct


def _capacity_value(alloc: Optional[int], run_id: str, ts: str) -> str:
    """v6.53.0 PURE: the _Status M-cell text for the TFB Grid Capacity key."""
    state, pct = _capacity_state(alloc)
    if pct is None:
        return f"UNKNOWN | allocated=n/a | run={run_id} | {ts}"
    return (f"{state} | allocated={int(alloc):,} ({pct:.2f}%) | "
            f"free={max(0, _CAP_LIMIT - int(alloc)):,} | run={run_id} | {ts}")


def _write_upstream_verdict(sheets: "SheetsWriter", spreadsheet_id: str,
                            results: list) -> None:
    """v6.40.0 W1A-4a producer. Upsert this job's per-page keys, then the
    composite, into `_Status` L:M. Bounded L{r}:M{r} RAW updates ONLY —
    NEVER values.append (row insertion would shear the A:J page grid).
    Fail-open at every step; loud ::warning:: on final write failure."""
    if not _upstream_verdict_enabled() or sheets is None:
        return
    try:
        svc = sheets._get_service()
        if not svc:
            return
        grid = []
        try:
            grid = (svc.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{_STATUS_SHEET_NAME}'!{_UV_BLOCK_RANGE}",
            ).execute().get("values", []) or [])
        except Exception as _re:
            print("::warning::%s could not read %s L:M — %s: %s"
                  % (_UPSTREAM_VERDICT_TAG, _STATUS_SHEET_NAME,
                     type(_re).__name__, _re))
            return
        keys = {}
        blanks = []
        known = False
        for i in range(60):
            row = grid[i] if i < len(grid) else []
            k = str(row[0]).strip() if row and len(row) > 0 else ""
            v = str(row[1]).strip() if row and len(row) > 1 else ""
            if k:
                keys[k.casefold()] = (i + 1, v)
                if k.casefold() in _UV_SELFCHECK_KEYS:
                    known = True
            else:
                blanks.append(i + 1)
        if not known:
            print("::warning::%s self-check failed — no known global key in "
                  "%s!%s; refusing blind write (layout moved?)."
                  % (_UPSTREAM_VERDICT_TAG, _STATUS_SHEET_NAME,
                     _UV_BLOCK_RANGE))
            return

        def _upsert(key: str, value: str) -> None:
            slot = keys.get(key.casefold(), (None, ""))[0]
            if slot is None:
                if not blanks:
                    print("::warning::%s no free L-slot for '%s' in %s — "
                          "skipped." % (_UPSTREAM_VERDICT_TAG, key,
                                        _UV_BLOCK_RANGE))
                    return
                slot = blanks.pop(0)
                keys[key.casefold()] = (slot, "")
            body = {"values": [[key, value]]}
            _err = None
            for _ in (1, 2):
                try:
                    svc.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"'{_STATUS_SHEET_NAME}'!L{slot}:M{slot}",
                        valueInputOption="RAW",
                        body=body,
                    ).execute()
                    _err = None
                    break
                except Exception as _ae:
                    _err = _ae
                    time.sleep(1.0)
            if _err is not None:
                raise _err
            keys[key.casefold()] = (slot, value)

        ts = _status_ts_str()  # v6.46.0: explicit offset
        run_id = (os.getenv("GITHUB_RUN_ID") or "").strip() or "local"
        required = set(_upstream_verdict_pages())
        for r in (results or []):
            page = str(getattr(r, "sheet_name", "") or "")
            if page not in required:
                continue
            state, cov = _uv_page_state(r)
            _upsert(_UV_PAGE_KEY_PREFIX + page,
                    f"{state} | cov={cov if cov is not None else 'n/a'} | "
                    f"run={run_id} | {ts}")
        page_states = {}
        now_e = time.time()
        for page in required:
            stored = keys.get((_UV_PAGE_KEY_PREFIX + page).casefold())
            page_states[page] = (_uv_parse_value(stored[1]) if stored
                                 else ("", None))
        verdict, summary = _uv_compose(page_states, now_e)
        _upsert(_UPSTREAM_VERDICT_KEY,
                f"{verdict} | run={run_id} | {ts} | {summary}")
        line = f"{_UPSTREAM_VERDICT_TAG} {verdict} | {summary}"
        (logger.info if verdict == "EXECUTABLE" else logger.warning)(line)
        # v6.53.0: grid-capacity key - the same bounded in-place _upsert, so
        # it lands even when the workbook is AT the allocation cap.
        if _capacity_status_enabled():
            try:
                _alloc = _capacity_allocated(svc, spreadsheet_id)
                _cap_val = _capacity_value(_alloc, run_id, ts)
                _upsert(_CAP_STATUS_KEY, _cap_val)
                _cst, _ = _capacity_state(_alloc)
                _cline = f"{_CAP_TAG} {_cap_val}"
                if _cst == "OK":
                    logger.info(_cline)
                else:
                    logger.warning(_cline)
                    print("::warning::%s" % _cline)
            except Exception as _ce:
                print("::warning::%s skipped — %s: %s"
                      % (_CAP_TAG, type(_ce).__name__, _ce))
    except Exception as _e:
        print("::warning::%s write FAILED — %s: %s"
              % (_UPSTREAM_VERDICT_TAG, type(_e).__name__, _e))
        logger.warning("%s skipped: %s", _UPSTREAM_VERDICT_TAG, _e)


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
    (blank normalizes to '' and is therefore NEVER an error marker).
    v6.33.0 [P0-2]: a fabricated-placeholder provider
    (_FABRICATED_PROVIDER_TOKEN) is unconditionally an ERROR marker, so a
    poisoned predecessor can never be certified last-GOOD. Honest stubs
    (no_data_stub / placeholder_stub) do NOT match and stay non-error."""
    try:
        if _FABRICATED_PROVIDER_TOKEN in str(v or "").casefold():
            return True
    except Exception:
        pass
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
    # v6.24.0 FW-1: fresh suspects list per invocation (read by the caller).
    del _LAST_KLG_ID_SUSPECTS[:]
    del _LAST_KLG_FORCED[:]                     # v6.29.0 B-4
    _forced = _force_refetch_symbols()
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

    # v6.24.3: last-good rows past the read bound were invisible to the swap.
    _klg_block = f"A1:ZZ{_page_read_row_bound()}" if _universe_cap_v2_enabled() else "A1:ZZ6000"
    grid = sheets.read_values(spreadsheet_id, sheet_name, _klg_block) if sheets is not None else None
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
    # v6.24.0 FW-1: identity columns of the OLD grid for the keep-gate.
    old_name_i = _guard_find_col(list(old_headers_raw), _GUARD_NAME_ALIASES)
    old_eps_i = _guard_find_col(list(old_headers_raw), _COH_EPS_ALIASES)
    old_pe_i = _guard_find_col(list(old_headers_raw), _COH_PE_ALIASES)
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
        if t in _forced:                         # v6.29.0 B-4
            _LAST_KLG_FORCED.append(t)
            continue  # forced symbol: the old row may NEVER ride back in
        if _klg_identity_gate_enabled() and not _klg_symbol_domain_ok(t):
            _LAST_KLG_ID_SUSPECTS.append(t)      # v6.54.0 Leg 0
            continue  # non-ticker identity ("COPPER FUTURES") is poison, never last-GOOD
        if not _klg_price_ok(row[old_px_i] if old_px_i < len(row) else ""):
            continue  # old row not good -> keep the fresh stub
        if 0 <= old_prov_i < len(row) and _klg_provider_is_error(row[old_prov_i]):
            continue
        # v6.24.0 FW-1: price+provider is no longer enough - the 2026-07-13
        # poison rode back in on exactly that certification. The old row
        # must also carry a Name and agree with its own P/E==Price/EPS
        # identity (when testable) before it may be kept.
        if _klg_identity_gate_enabled() and not _klg_old_row_identity_ok(
            row, old_name_i, old_px_i, old_eps_i, old_pe_i
        ):
            _LAST_KLG_ID_SUSPECTS.append(t)
            continue  # identity-suspect predecessor -> write the fresh stub
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
        t = canonicalize_symbol(s)
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
        t = canonicalize_symbol(row[sym_i])
        row[sym_i] = t
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
_COHERENCE_TAG = "[v6.23.0 COHERENCE-TRIPWIRE]"

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

    # ---- v6.23.0: Commodities_FX coverage (L3 checked 0 anchors there) ----
    # Every pair below was verified PASSING against the live 2026-07-12 export
    # (that page is clean-but-frozen). An unverified pair would false-trip and
    # block a healthy write, so nothing here is invented.
    "GC=F": ("gold",),
    "SI=F": ("silver",),
    "CL=F": ("crude", "wti"),
    "BZ=F": ("brent",),
    "NG=F": ("natural gas",),
    "HG=F": ("copper",),
    "^GSPC": ("s&p 500",),
    "^N225": ("nikkei",),
    "^BSESN": ("sensex",),
    "USO.US": ("oil fund", "united states oil"),
    "UGA.US": ("gasoline",),
    "CPER.US": ("copper",),
    "OUNZ.US": ("gold",),
    "EURUSD=X": ("eur/usd", "eurusd"),
    "GBPUSD=X": ("gbp/usd", "gbpusd"),

    # ---- v6.23.0: Mutual_Funds coverage (L3 checked 0 anchors there) ----
    # Mutual_Funds has ZERO rows with an EPS/PE pair, so L3b cannot form the
    # coherence ratio for it at all — anchors are the ONLY layer that can see
    # this page. It was 33% poisoned in the 2026-07-12 export and both layers
    # were blind simultaneously. The four PASSING pairs pin the page's health;
    # the rest were FOREIGN on 07-12 and are exactly what must trip.
    "SPY.US": ("s&p 500",),
    "VOO.US": ("vanguard s&p 500",),
    "QQQ.US": ("qqq",),
    "VTI.US": ("total stock market",),
    "IVV.US": ("core s&p 500",),
    "GLD.US": ("gold",),
    "AGG.US": ("aggregate bond",),
    "BND.US": ("total bond",),
    "VEA.US": ("ftse developed", "developed markets"),
    "EFA.US": ("eafe",),
    "IEMG.US": ("emerging markets",),
    "TLT.US": ("treasury",),
}


# -----------------------------------------------------------------------------
# v6.23.0 L3b — COHERENCE TRIPWIRE (curation-free transposition detector)
# -----------------------------------------------------------------------------
# The three columns below are NOT independent:  P/E (TTM) == Current Price / EPS.
# They also straddle the exact seam the transposition splits: Current Price comes
# from the QUOTE block (symbol-keyed, correct even in a poisoned payload) while
# EPS and P/E come from the ENRICHMENT block (the block that gets misassigned).
# A transposed row therefore breaks the identity BY CONSTRUCTION — no anchor
# table, no curation, every symbol, every equity page.
_COH_PRICE_ALIASES = frozenset({
    "currentprice", "price", "lastprice", "lasttradeprice", "regularmarketprice",
})
_COH_EPS_ALIASES = frozenset({
    "epsttm", "eps", "earningspershare", "epstrailingtwelvemonths", "epsbasicttm",
})
_COH_PE_ALIASES = frozenset({
    "pettm", "pe", "peratio", "priceearnings", "priceearningsratio", "trailingpe",
})

# LSE and a few other venues quote PRICE in pence (GBX) while reporting EPS in
# pounds (GBP). The implied ratio is then ~100x the stated P/E even though the
# row is perfectly healthy (Tesco: px 471.8 GBX, EPS 0.27 GBP, stated P/E 17.47).
# Treat an implied/stated ratio inside this window as COHERENT — never a mismatch.
# This band is the entire reason clean Market_Leaders scores 1.0% and not 4%.
_COH_FX_UNIT_LO = 50.0
_COH_FX_UNIT_HI = 200.0
# Relative tolerance on the identity itself (rounding, as-of skew between the
# quote and the fundamentals snapshot).
_COH_REL_TOL = 0.05


def _coherence_enabled() -> bool:
    """v6.23.0 L3b master switch. Default ON; TFB_SYNC_COHERENCE_TRIPWIRE=
    0/false/off/no restores v6.22.4 behavior byte-identically."""
    return (os.getenv("TFB_SYNC_COHERENCE_TRIPWIRE") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _coherence_max_bad_pct() -> int:
    """Percent of TESTABLE rows that may break the identity before the page is
    judged transposed. Default 25. Measured separation on the 2026-07-12 export:
    poisoned Global_Markets 89.2% vs clean Market_Leaders 1.0% — 25 sits with a
    ~3.5x margin on both sides."""
    return _safe_int(os.getenv("TFB_SYNC_COHERENCE_MAX_BAD_PCT"), 25, lo=1, hi=100)


def _coherence_min_rows() -> int:
    """Minimum TESTABLE rows before the scan is allowed to judge a page at all.
    Default 50. FAIL-SAFE: Commodities_FX has 4 testable rows and Mutual_Funds
    has 0 (funds/FX carry no EPS), so they are never blocked by this layer —
    exactly like a page with no anchors present. Those two pages are covered by
    the v6.23.0 L3 anchor expansion instead."""
    return _safe_int(os.getenv("TFB_SYNC_COHERENCE_MIN_ROWS"), 50, lo=5, hi=1000000)


def _coh_float(v: Any) -> Optional[float]:
    """Tolerant numeric read: strips thousands separators, %, currency noise."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except Exception:
            return None
        return None if (f != f or f in (float("inf"), float("-inf"))) else f
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "").replace("%", "").replace("\u2212", "-").strip()
    if s in {"-", "--", "\u2014", "N/A", "n/a", "NA", "null", "None"}:
        return None
    try:
        f = float(s)
    except Exception:
        return None
    return None if (f != f or f in (float("inf"), float("-inf"))) else f


def _coherence_scan(
    headers: List[Any],
    rows_matrix: List[List[Any]],
) -> Tuple[int, int, List[Tuple[str, float, float]]]:
    """v6.23.0 L3b: verify P/E == Price / EPS row by row.

    Returns (testable, incoherent, samples) where samples is a list of
    (symbol, stated_pe, implied_pe) for the first few offenders.

    A row is TESTABLE only when Price, EPS and P/E are all numerically present,
    |EPS| >= 0.01 (below that the quotient explodes on rounding alone) and the
    stated P/E is > 0 (a negative/zero P/E is a legitimate loss-maker convention,
    not evidence of transposition). Everything else is SKIPPED, never condemned.

    FAIL-SAFE: (0, 0, []) when any of the three columns cannot be located — a
    page without the full triple is never blocked by this layer.
    """
    if not headers or not rows_matrix:
        return 0, 0, []
    hdr = list(headers)
    px_i = _guard_find_col(hdr, _COH_PRICE_ALIASES)
    eps_i = _guard_find_col(hdr, _COH_EPS_ALIASES)
    pe_i = _guard_find_col(hdr, _COH_PE_ALIASES)
    if px_i < 0 or eps_i < 0 or pe_i < 0:
        return 0, 0, []
    sym_i = _guard_find_col(hdr, _GUARD_SYMBOL_ALIASES)
    hi = max(px_i, eps_i, pe_i)

    testable = 0
    bad = 0
    samples: List[Tuple[str, float, float]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)) or len(row) <= hi:
            continue
        px = _coh_float(row[px_i])
        eps = _coh_float(row[eps_i])
        pe = _coh_float(row[pe_i])
        if px is None or eps is None or pe is None:
            continue
        if abs(eps) < 0.01 or pe <= 0.0 or px <= 0.0:
            continue
        testable += 1
        implied = px / eps
        if implied <= 0.0:
            # price > 0 and eps > 0 cannot land here; a negative implied means
            # eps < 0 while the provider still published a positive P/E. That is
            # a provider quirk, not a transposition signature — skip it.
            testable -= 1
            continue
        rel = abs(implied - pe) / abs(pe)
        if rel < _COH_REL_TOL:
            continue
        ratio = implied / pe
        if _COH_FX_UNIT_LO <= ratio <= _COH_FX_UNIT_HI:
            continue  # GBX/GBP pence convention — healthy row, not a mismatch
        bad += 1
        if len(samples) < 8:
            s = ""
            if 0 <= sym_i < len(row):
                s = str(row[sym_i] or "").strip().upper()
            samples.append((s or "?", round(pe, 2), round(implied, 2)))
    return testable, bad, samples


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
    _pv_note = {"missing": 0, "grid": 0, "hdr": -1, "reason": "ok"}
    def _pv_log():
        if _persist_v2_enabled():
            logger.warning(
                "[PERSIST v6.34.0] %s | missing=%s grid=%s hdr=%s injected=%s reason=%s",
                sheet_name, _pv_note["missing"], _pv_note["grid"],
                _pv_note["hdr"], len(kept), _pv_note["reason"])
    if not headers or rows_matrix is None or not requested_symbols:
        _pv_note["reason"] = "empty_inputs"; _pv_log()
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
    _pv_note["missing"] = len(missing)
    if not missing:
        _pv_note["reason"] = "no_missing"; _pv_log()
        return rows_matrix, kept

    # v6.24.3: last-good rows past the read bound could not be preserved.
    _pp_block = f"A1:ZZ{_page_read_row_bound()}" if _universe_cap_v2_enabled() else "A1:ZZ6000"
    grid = sheets.read_values(spreadsheet_id, sheet_name, _pp_block) if sheets is not None else None
    if (not grid or not isinstance(grid, list)) and _persist_v2_enabled():
        try:
            time.sleep(4)
        except Exception:
            pass
        grid = sheets.read_values(spreadsheet_id, sheet_name, _pp_block) if sheets is not None else None
        _pv_note["reason"] = "grid_empty_retried"
    if not grid or not isinstance(grid, list):
        _pv_note["reason"] = "grid_empty"; _pv_log()
        return rows_matrix, kept
    _pv_note["grid"] = len(grid)

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
    _pv_note["hdr"] = hdr_r
    if old_sym_i < 0:
        _pv_note["reason"] = "header_not_found"; _pv_log()
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

    _pv_log()
    return rows_matrix, kept


def _universe_cap_v2_enabled() -> bool:
    """v6.24.3 master switch for the expansion-sized universe caps. Default ON;
    set TFB_SYNC_UNIVERSE_CAP_V2=0/false/off/no to restore EVERY legacy bound
    byte-for-byte: readback "A1:E5000", guard re-reads "A1:ZZ6000", market cap
    ceiling min(v, 5000), request-limit ceilings min(5000, ...)."""
    return (os.getenv("TFB_SYNC_UNIVERSE_CAP_V2") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _page_read_row_bound() -> int:
    """v6.24.3: row bound for full-page re-reads (symbol read-back, KEEP-LAST-
    GOOD stub-swap, PERSISTENCE pass). Env TFB_SYNC_PAGE_READ_MAX_ROW, default
    12000 (Global_Markets 6,512 data rows + header, with ~1.8x headroom),
    clamped 1000..100000. Sheets API responses contain only the USED range, so
    a larger requested bound is a ceiling, not a payload inflator. Fail-safe:
    unparsable values fall back to 12000."""
    raw = (os.getenv("TFB_SYNC_PAGE_READ_MAX_ROW") or "").strip()
    try:
        v = int(raw) if raw else 12000
    except Exception:
        v = 12000
    return max(1000, min(v, 100000))


def _request_limit_ceiling() -> int:
    """v6.24.3: ceiling applied to the backend request 'limit' field. Legacy
    hardcoded 5000; under v2 this rises to 20000 (above the full 12,486-symbol
    universe) so a non-batched >5000-symbol page can no longer be silently
    limit-truncated. With batching active (batch size 25) the effective value
    is unchanged: min(ceiling, 25) == 25 either way."""
    return 20000 if _universe_cap_v2_enabled() else 5000


def _market_symbol_cap() -> int:
    """v6.19.2: per-page symbol cap for the four MARKET pages (Market_Leaders,
    Global_Markets, Commodities_FX, Mutual_Funds). Default 2500 — sized to the
    Symbol Expansion Pack / build_universes documented ceiling with headroom —
    override with TFB_SYNC_MAX_SYMBOLS_MARKET. A cap SMALLER
    than the sheet universe silently un-requests the overflow, and the
    persistence guard can only protect REQUESTED symbols, so an undersized cap
    acts as a symbol remover (the 2026-07-03 Global_Markets pin at exactly 800
    rows). Fail-safe: any unparsable value falls back to 2500.
    v6.24.3: ceiling raised 5000 -> 20000 under TFB_SYNC_UNIVERSE_CAP_V2 (the
    2026-07 expansion puts Global_Markets at 6,512 — the old ceiling made the
    yml's value of 5000 the binding truncator). Legacy path keeps min(v, 5000)
    exactly. NOTE: the yml still drives the ACTUAL cap; raise
    TFB_SYNC_MAX_SYMBOLS_MARKET to ~7000 for the expansion to take effect."""
    raw = (os.getenv("TFB_SYNC_MAX_SYMBOLS_MARKET") or "").strip()
    try:
        v = int(raw) if raw else 2500
    except Exception:
        v = 2500
    ceiling = 20000 if _universe_cap_v2_enabled() else 5000
    return max(1, min(v, ceiling))


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
            # v6.39.1 (external audit P0-1, PARTIAL ACCEPT): truncating here
            # UN-REQUESTS the overflow — persistence protects only requested
            # symbols, so an undersized cap acts as a silent symbol remover
            # (the 2026-07-03 GM-pinned-at-800 incident class). Truncation
            # stays (workload control) but is now LOUD and countable.
            print("::warning::[CAP v6.39.1] CAP_BELOW_UNIVERSE on %s: sheet "
                  "universe exceeds cap=%d — overflow is UN-REQUESTED this "
                  "leg (persistence preserves existing rows; heal-first "
                  "fronts the remainder next leg). Raise "
                  "TFB_SYNC_MAX_SYMBOLS_MARKET to cover the universe."
                  % (task_key, max_symbols))
            logger.warning("[CAP v6.39.1] CAP_BELOW_UNIVERSE %s cap=%d",
                           task_key, max_symbols)
            break
    return out


# -----------------------------------------------------------------------------
# Task definitions (aligned with your dashboard tabs + canonical schema)
# -----------------------------------------------------------------------------
# --------------------------------------------------------------------------- #
# v6.32.0 MANUAL-HOLD BRIDGE (WHY: see the v6.32.0 header block)               #
# --------------------------------------------------------------------------- #
_MANUAL_HOLD_TAG = "[MANUAL-HOLD v6.32.0]"
_MH_SHEET = "_Sync_Control"
_MH_RANGE = "_Sync_Control!A1:B6"
_MH_KEY_NORM = "manualholduntil"
_MH_MAX_HOLD_HOURS = 12.0
_MH_CACHE_TTL_SEC = 30.0
_MH_RIYADH_OFFSET_HOURS = 3
_MH_CACHE = {"at": None, "active": False, "msg": ""}  # at=None => never checked (monotonic() can be tiny on fresh runners)


def _manual_hold_gate_enabled() -> bool:
    """v6.32.0: default ON. TFB_SYNC_MANUAL_HOLD_GATE=0/false/off/no restores
    v6.31.0 behavior byte-identically (no _Sync_Control reads at all)."""
    return (os.getenv("TFB_SYNC_MANUAL_HOLD_GATE") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _mh_parse_hold_until(raw: Any):
    """v6.33.0 [P0-3a/b]: Parse the hold-expiry cell -> aware-UTC datetime or None.

    Accepts (1) ISO-8601 ('...Z', '+03:00', or space-separated) and
    (2) numeric Google Sheets DATE SERIALS (epoch 1899-12-30) in the
    plausible band 20000..80000 — Sheets returns date cells as serials
    under UNFORMATTED_VALUE, which previously failed OPEN and silently
    ignored real holds. NAIVE values (string or serial) are Riyadh local
    (UTC+3). A value beyond now + _MH_MAX_HOLD_HOURS (+60s clock skew) is
    REJECTED -> None (fail-open, never a rolling re-clamp: the old
    min(dt, now+12h) renewed a far-future hold on every read, forever).
    Anything unparsable -> None (no hold)."""
    try:
        s = str(raw or "").strip()
        if not s:
            return None
        _td = __import__("datetime").timedelta
        dt_utc = None
        # (2) Sheets serial branch first: pure numerics are never valid ISO.
        try:
            serial = float(s)
            if 20000.0 < serial < 80000.0:
                base = datetime(1899, 12, 30, tzinfo=timezone.utc)
                dt_utc = (base + _td(days=serial)
                          - _td(hours=_MH_RIYADH_OFFSET_HOURS))
        except ValueError:
            pass
        if dt_utc is None:
            if s.endswith(("Z", "z")):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc) - _td(
                    hours=_MH_RIYADH_OFFSET_HOURS)
            dt_utc = dt.astimezone(timezone.utc)
        now = datetime.now(timezone.utc)
        ceiling = now + _td(hours=_MH_MAX_HOLD_HOURS) + _td(seconds=60)
        if dt_utc > ceiling:
            try:
                logger.warning("%s rejected far-future hold value %r "
                               "(> %.0fh ceiling) — fail-open, no hold",
                               _MANUAL_HOLD_TAG, s, _MH_MAX_HOLD_HOURS)
            except Exception:
                pass
            return None
        return dt_utc
    except Exception:
        return None


def _mh_read_hold(sheets: Any, spreadsheet_id: str):
    """Read the hold cell. FAIL-OPEN: any error -> (None, ''). Never raises."""
    try:
        grid = sheets.read_values(spreadsheet_id, _MH_SHEET, "A1:B6")
        if not grid or not isinstance(grid, list):
            return None, ""
        for row in grid:
            if not isinstance(row, list) or not row:
                continue
            key = re.sub(r"[^a-z0-9]+", "", str(row[0] or "").lower())
            if key == _MH_KEY_NORM:
                raw = row[1] if len(row) > 1 else ""
                return _mh_parse_hold_until(raw), str(raw or "").strip()
        return None, ""
    except Exception:
        return None, ""


def _manual_hold_active(sheets: Any, spreadsheet_id: str) -> tuple:
    """(active: bool, human message). 30s cache keeps per-task checks cheap.
    Gate OFF or dry callers should not reach here; callers hold that logic."""
    now_mono = time.monotonic()
    _at = _MH_CACHE.get("at")
    if _at is not None and (now_mono - float(_at)) < _MH_CACHE_TTL_SEC:
        return bool(_MH_CACHE["active"]), str(_MH_CACHE["msg"])
    until, raw = _mh_read_hold(sheets, spreadsheet_id)
    active = bool(until and until > datetime.now(timezone.utc))
    msg = (f"manual hold active until {until.isoformat()} (cell: {raw!r})"
           if active else "")
    _MH_CACHE.update({"at": now_mono, "active": active, "msg": msg})
    return active, msg


def _append_runlog_manual_hold(sheets: Any, spreadsheet_id: str, msg: str) -> None:
    """Best-effort, fail-open _Run_Log line so deferrals stay auditable."""
    try:
        svc = sheets._get_service()
        if not svc:
            return
        svc.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="'_Run_Log'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [[
                datetime.now(timezone.utc).isoformat(),
                "WARNING", "manual_hold", "ALL", "DEFERRED",
                f"{_MANUAL_HOLD_TAG} {msg}", "", "", "",
                _runlog_meta_json(json.dumps({"version": SCRIPT_VERSION})),
            ]]},
        ).execute()
    except Exception as _e:
        _note_runlog_append_failure("manual_hold")   # v6.55.0: was silent
        print("::warning::[MANUAL-HOLD] _Run_Log append FAILED — %s: %s"
              % (type(_e).__name__, _e))


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
            # v6.25.0: bare "/analysis/sheet-rows" removed — it is absent from
            # the backend's canonical route map and can only ever 404 (run
            # #2413 logs show it 404ing on every pre-sticky attempt).
            return [
                "/v1/analysis/sheet-rows",
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


_TIME_BUDGET_TAG = "[v6.22.4 TIME-BUDGET]"
# The runner is a one-shot script: module import happens at process start,
# so this anchor approximates the process birth time with zero plumbing.
_TIME_BUDGET_START = time.monotonic()


def _time_budget_sec() -> float:
    """v6.22.4 L5: wall-clock budget in seconds (TFB_SYNC_TIME_BUDGET_SEC).
    Default 0 = DISABLED (v6.22.3 behavior byte-identical). When set, floor
    60s; unparseable values disable."""
    raw = (os.getenv("TFB_SYNC_TIME_BUDGET_SEC", "") or "").strip()
    if not raw:
        return 0.0
    try:
        v = float(raw)
    except Exception:
        return 0.0
    return 0.0 if v <= 0 else max(60.0, v)


def _time_budget_left() -> float:
    """Seconds remaining (inf when the budget is disabled)."""
    b = _time_budget_sec()
    if b <= 0:
        return float("inf")
    return b - (time.monotonic() - _TIME_BUDGET_START)


def _time_budget_exceeded() -> bool:
    return _time_budget_left() <= 0.0


def _batch_retry_enabled() -> bool:
    """v6.25.0: second-pass retry of failed symbol batches. Default ON;
    TFB_SYNC_BATCH_RETRY=0/false/off/no restores single-attempt behavior."""
    v = (os.getenv("TFB_SYNC_BATCH_RETRY") or "1").strip().lower()
    return v not in {"0", "false", "off", "no"}


def _floor_strict_enabled() -> bool:
    """v6.25.0: TFB_SYNC_FLOOR_STRICT=1 restores the unconditional coverage
    floor veto (pre-6.25.0), even when persistence could merge safely."""
    v = (os.getenv("TFB_SYNC_FLOOR_STRICT") or "0").strip().lower()
    return v in {"1", "true", "on", "yes"}


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
    # v6.22.4 L5 [TIME-BUDGET]: a page whose fetch would START after the
    # budget is spent is skipped whole -> the caller's empty-fetch guards
    # PRESERVE its last-good rows (identical to a provider outage).
    if _time_budget_exceeded():
        _tw = (
            f"{_TIME_BUDGET_TAG} {task.sheet_name}: budget "
            f"{_time_budget_sec():.0f}s already exhausted before fetch — page "
            f"skipped; last-good rows preserved by the empty-fetch guards."
        )
        res.warnings.append(_tw)
        logger.warning(_tw)
        return [], [], None, "time budget exhausted before fetch"

    size = _symbol_batch_size()
    delay_ms = _batch_delay_ms()
    batches = build_isolated_batches(symbols, size)
    candidates = _endpoint_candidates_for_gateway(eff_gw)

    headers: List[Any] = []
    combined: List[List[Any]] = []
    used_endpoint: Optional[str] = None
    last_err: Optional[str] = None
    ok_batches = 0
    # v6.25.0: batches whose fetch failed outright (timeout/5xx on every
    # candidate) are re-attempted in a second pass instead of being lost.
    failed_batches: List[Tuple[int, List[str]]] = []

    # v6.22.0 L2 [BATCH-IDENTITY]: accumulate BY SYMBOL instead of by position.
    _idb_on = _batch_identity_enabled()
    _idb_sym_i = -1          # resolved from the first answering batch's headers
    _idb_by_sym: Dict[str, List[Any]] = {}
    _idb_bleed = 0           # rows whose symbol is not in THAT batch's request
    _idb_dupes = 0           # repeated symbol rows (first occurrence wins)
    _idb_blank = 0           # rows with a blank symbol cell (unaddressable)

    for bi, batch in enumerate(batches):
        # v6.22.4 L5: deadline check BETWEEN batches — stop fetching and let
        # the accumulated partial proceed to the write path (coverage floor +
        # persistence + KEEP-LAST-GOOD then decide safely). Never mid-batch;
        # the first batch is always allowed so headers can resolve.
        if bi > 0 and _time_budget_exceeded():
            _tw = (
                f"{_TIME_BUDGET_TAG} {task.sheet_name}: budget "
                f"{_time_budget_sec():.0f}s exhausted after {ok_batches}/"
                f"{len(batches)} batch(es); proceeding to write with the "
                f"partial fetch instead of losing it to a hard kill."
            )
            res.warnings.append(_tw)
            logger.warning(_tw)
            break

        p = dict(base_payload)
        p["tickers"] = batch
        p["symbols"] = batch
        p["limit"] = min(_request_limit_ceiling(), max(1, len(batch)))
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
                    _batch_set = {canonicalize_symbol(t) for t in batch}
                    _batch_set.discard("")
                    for _row in b_matrix:
                        if (not isinstance(_row, (list, tuple))
                                or _idb_sym_i >= len(_row)
                                or _guard_is_blank(_row[_idb_sym_i])):
                            _idb_blank += 1
                            continue
                        _t = canonicalize_symbol(_row[_idb_sym_i])
                        _row[_idb_sym_i] = _t
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
        else:
            # v6.25.0: remember the whole batch for the retry pass.
            failed_batches.append((bi, list(batch)))

        if delay_ms > 0 and bi < len(batches) - 1:
            await asyncio.sleep(delay_ms / 1000.0)

    # ---- v6.25.0 RETRY PASS: one more attempt per failed batch, on the
    # sticky endpoint (or full candidates if none resolved), inside the same
    # time budget. Converts transient morning timeouts into coverage instead
    # of permanently losing 25 symbols per failure.
    if failed_batches and _batch_retry_enabled():
        retried_ok = 0
        for rbi, rbatch in failed_batches:
            if _time_budget_exceeded():
                _rw = (
                    f"[v6.25.0 BATCH-RETRY] {task.sheet_name}: budget ended "
                    f"with {len(failed_batches) - retried_ok} failed batch(es) "
                    f"still unrecovered; heal-first fronts them next leg."
                )
                res.warnings.append(_rw)
                logger.warning(_rw)
                break
            p = dict(base_payload)
            p["tickers"] = rbatch
            p["symbols"] = rbatch
            p["limit"] = min(_request_limit_ceiling(), max(1, len(rbatch)))
            p["request_id"] = f"{res.request_id}-r{rbi + 1}"
            cand = [used_endpoint] if used_endpoint else candidates
            b_headers = []
            b_matrix = []
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
            if not b_headers:
                continue
            if not headers:
                headers = b_headers
                if _idb_on:
                    _idb_sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
            if b_matrix:
                if _idb_on and _idb_sym_i >= 0:
                    _batch_set = {canonicalize_symbol(t) for t in rbatch}
                    _batch_set.discard("")
                    for _row in b_matrix:
                        if (not isinstance(_row, (list, tuple))
                                or _idb_sym_i >= len(_row)
                                or _guard_is_blank(_row[_idb_sym_i])):
                            _idb_blank += 1
                            continue
                        _t = canonicalize_symbol(_row[_idb_sym_i])
                        _row[_idb_sym_i] = _t
                        if _t not in _batch_set:
                            _idb_bleed += 1
                            continue
                        if _t in _idb_by_sym:
                            _idb_dupes += 1
                            continue
                        _idb_by_sym[_t] = list(_row)
                else:
                    combined.extend(b_matrix)
            ok_batches += 1
            retried_ok += 1
        if retried_ok:
            _rw = (
                f"[v6.25.0 BATCH-RETRY] {task.sheet_name}: recovered "
                f"{retried_ok}/{len(failed_batches)} previously failed batch(es)."
            )
            res.warnings.append(_rw)
            logger.warning(_rw)

    # v6.22.0 L2: emit in the REQUESTED symbol order (no positional artifact
    # can survive), falling through to the legacy `combined` when the Symbol
    # column never resolved or the layer is off.
    if _idb_on and _idb_sym_i >= 0:
        combined = [
            _idb_by_sym[t]
            for t in (canonicalize_symbol(s) for s in symbols)
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
_STALE_SKIP_TAG = "[STALE-SKIP v6.26.0]"
_PAGE_VERDICT_TAG = "[PAGE-VERDICT v6.26.0]"
# Skips that are CONFIGURATION, not health — never escalated.
_BENIGN_SKIP_MARKERS = (
    "Dry run", "decision-owned", "Forbidden legacy key", "Unknown key",
    "disallows empty symbols",
    "[MANUAL-HOLD",  # v6.33.0 P0-3c: operator hold is a benign deferral, never a red leg
)
_STAMP_HEADER_CANDIDATES = (
    "last updated (riyadh)", "last updated (utc)", "last updated",
)


def _stale_skip_red_enabled() -> bool:
    """v6.26.0 kill-switch — DEFAULT ON. 0/false/off/no restores v6.25.3
    statuses, exit codes and logging byte-identically."""
    raw = (os.getenv("TFB_SYNC_STALE_SKIP_RED", "1") or "1").strip().lower()
    return raw not in ("0", "false", "off", "no")


def _skip_max_stale_h() -> float:
    """v6.26.0: newest-stamp age (hours) beyond which a guard-skip on a
    ranked page is a data outage, not an availability trade. Default 30
    (one missed day + margin); floor 1; unparseable -> 30."""
    try:
        return max(1.0, float(os.getenv("TFB_SYNC_SKIP_MAX_STALE_H") or "30"))
    except Exception:
        return 30.0


def _col_idx_to_a1(idx0: int) -> str:
    """0-based column index -> A1 letters (0->A, 25->Z, 26->AA)."""
    out = ""
    n = int(idx0)
    while True:
        out = chr(ord("A") + (n % 26)) + out
        n = n // 26 - 1
        if n < 0:
            return out


def _parse_stamp_cell(v: Any) -> Optional[datetime]:
    """One stamp cell -> naive datetime. Accepts Sheets date serials
    (UNFORMATTED_VALUE numbers; epoch 1899-12-30) and 'YYYY-MM-DD[ HH:MM:SS]'
    text prefixes. Anything else -> None; never raises."""
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            f = float(v)
            if 20000.0 < f < 80000.0:  # ~1954..2118: plausible date serial
                base = datetime(1899, 12, 30)
                from datetime import timedelta as _td
                return base + _td(days=f)
            return None
        t = str(v or "").strip()
        if len(t) >= 19:
            try:
                return datetime.strptime(t[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        if len(t) >= 10:
            return datetime.strptime(t[:10], "%Y-%m-%d")
        return None
    except Exception:
        return None


def _page_newest_stamp_age_h(sheets: Any, spreadsheet_id: str,
                             sheet_name: str) -> Optional[float]:
    """v6.26.0: age in hours of the NEWEST data stamp on a page, read from
    its Last Updated column. Header row found by scanning the first 45 rows
    for a row carrying both a Symbol column and a stamp header. Riyadh-
    labeled stamps age against UTC+3, others against UTC (both naive; a
    3h label error is immaterial vs a 30h threshold). Returns None on ANY
    failure — an unreadable page must never escalate anything."""
    try:
        if sheets is None:
            return None
        head = sheets.read_values(spreadsheet_id, sheet_name, "A1:EZ45")
        if not head:
            return None
        hdr_row = -1
        stamp_idx = -1
        riyadh = False
        for ri, row in enumerate(head):
            low = [str(c or "").strip().lower() for c in (row or [])]
            if "symbol" not in low:
                continue
            for ci, cell in enumerate(low):
                if cell in _STAMP_HEADER_CANDIDATES:
                    hdr_row, stamp_idx = ri, ci
                    riyadh = "riyadh" in cell
                    break
            if hdr_row >= 0:
                break
        if hdr_row < 0 or stamp_idx < 0:
            return None
        col = _col_idx_to_a1(stamp_idx)
        rng = "%s%d:%s" % (col, hdr_row + 2, col)
        vals = sheets.read_values(spreadsheet_id, sheet_name, rng)
        if not vals:
            return None
        newest: Optional[datetime] = None
        for r in vals:
            dt = _parse_stamp_cell(r[0] if r else None)
            if dt is not None and (newest is None or dt > newest):
                newest = dt
        if newest is None:
            return None
        from datetime import timedelta as _td
        now = _utc_now().replace(tzinfo=None) \
            + (_td(hours=3) if riyadh else _td(0))
        return max(0.0, (now - newest).total_seconds() / 3600.0)
    except Exception:
        return None


_OLDEST_FIRST_TAG = "[OLDEST-FIRST v6.27.0]"


_DECISION_FIRST_TAG = "[DECISION-FIRST v6.35.0]"
_DECISION_SOURCE_PAGES = ("My_Portfolio", "Top_10_Investments")
_PRIORITY_SET_CACHE = {}


def _priority_fetch_enabled() -> bool:
    """v6.35.0 kill-switch — DEFAULT OFF (v6.34.0 byte-identical).
    TFB_SYNC_PRIORITY_FETCH=1 promotes decision symbols to the front of
    every ranked market page's worklist (see header WHY block)."""
    raw = (os.getenv("TFB_SYNC_PRIORITY_FETCH", "0") or "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _priority_fetch_max() -> int:
    """v6.35.0: hard cap on the priority set (TFB_SYNC_PRIORITY_MAX,
    default 150, clamped 10..1000) so a corrupted source page can never
    promote a whole universe to the head of the queue."""
    try:
        v = int(float((os.getenv("TFB_SYNC_PRIORITY_MAX", "150") or "150").strip()))
    except Exception:
        v = 150
    return max(10, min(1000, v))


def _priority_extra_symbols():
    """v6.35.0: operator-supplied csv (TFB_SYNC_PRIORITY_EXTRA), normalized
    UPPER, ';' accepted as ',' — same tolerance as the force-refetch parser."""
    raw = (os.getenv("TFB_SYNC_PRIORITY_EXTRA", "") or "").replace(";", ",")
    return [t.strip().upper() for t in raw.split(",") if t.strip()]


def _page_symbol_column(sheets, spreadsheet_id, sheet_name):
    """v6.35.0: non-blank normalized Symbol column of `sheet_name` via one
    bounded header scan + ONE single-column read (the _page_symbol_stamps
    scan family, but stamp-optional — Top_10_Investments carries no Last
    Updated column and must still contribute). Returns None on ANY failure:
    an unreadable source page must never reorder anything."""
    try:
        if sheets is None:
            return None
        head = sheets.read_values(spreadsheet_id, sheet_name, "A1:EZ45")
        if not head:
            return None
        hdr_row = -1
        sym_idx = -1
        for ri, row in enumerate(head):
            low = [str(c or "").strip().lower() for c in (row or [])]
            if "symbol" in low:
                hdr_row = ri
                sym_idx = low.index("symbol")
                break
        if hdr_row < 0 or sym_idx < 0:
            return None
        s_col = _col_idx_to_a1(sym_idx)
        first = hdr_row + 2
        vals = sheets.read_values(
            spreadsheet_id, sheet_name, "%s%d:%s" % (s_col, first, s_col))
        if not vals:
            return None
        out = []
        seen = set()
        for r in vals:
            raw = r[0] if (r and len(r)) else ""
            sym = str(raw or "").strip().upper()
            if not sym or sym in ("SYMBOL", "TICKER") or sym in seen:
                continue
            seen.add(sym)
            out.append(sym)
        return out or None
    except Exception:
        return None


def _decision_priority_symbols(sheets, spreadsheet_id):
    """v6.35.0: memoized-per-run union of decision symbols from
    _DECISION_SOURCE_PAGES plus TFB_SYNC_PRIORITY_EXTRA, capped by
    _priority_fetch_max(). A source page that fails to read contributes
    nothing; empty union => None => feature inert for this run."""
    key = str(spreadsheet_id or "")
    if key in _PRIORITY_SET_CACHE:
        return _PRIORITY_SET_CACHE[key]
    merged = []
    seen = set()
    for page in _DECISION_SOURCE_PAGES:
        col = _page_symbol_column(sheets, spreadsheet_id, page)
        for sym in (col or []):
            if sym not in seen:
                seen.add(sym)
                merged.append(sym)
    for sym in _priority_extra_symbols():
        if sym and sym not in seen:
            seen.add(sym)
            merged.append(sym)
    cap = _priority_fetch_max()
    result = frozenset(merged[:cap]) if merged else None
    _PRIORITY_SET_CACHE[key] = result
    return result


def _apply_decision_first(symbols, priority_set):
    """v6.35.0: STABLE partition — symbols in `priority_set` first (keeping
    their relative order, i.e. stalest-first among themselves after
    OLDEST-FIRST), everything else after in unchanged order. Returns
    (reordered_list, promoted_count); promoted_count==0 => list unchanged."""
    if not symbols or not priority_set:
        return symbols, 0
    front = []
    rest = []
    for sym in symbols:
        if str(sym or "").strip().upper() in priority_set:
            front.append(sym)
        else:
            rest.append(sym)
    if not front:
        return symbols, 0
    return front + rest, len(front)


def _oldest_first_enabled() -> bool:
    """v6.27.0 kill-switch — DEFAULT ON. TFB_SYNC_OLDEST_FIRST=0 restores
    the sheet-order worklist byte-identically."""
    raw = (os.getenv("TFB_SYNC_OLDEST_FIRST", "1") or "1").strip().lower()
    return raw not in ("0", "false", "off", "no")


def _page_symbol_stamps(sheets: Any, spreadsheet_id: str,
                        sheet_name: str) -> Optional[Dict[str, Any]]:
    """v6.27.0: {SYMBOL: parsed Last-Updated datetime-or-None} for a page,
    via two single-column reads (Symbol + stamp), rows aligned by index.
    Header row located exactly like _page_newest_stamp_age_h. Returns None
    on ANY failure — an unreadable page must never reorder anything."""
    try:
        if sheets is None:
            return None
        head = sheets.read_values(spreadsheet_id, sheet_name, "A1:EZ45")
        if not head:
            return None
        hdr_row = -1
        sym_idx = -1
        stamp_idx = -1
        for ri, row in enumerate(head):
            low = [str(c or "").strip().lower() for c in (row or [])]
            if "symbol" not in low:
                continue
            for ci, cell in enumerate(low):
                if cell in _STAMP_HEADER_CANDIDATES:
                    hdr_row = ri
                    sym_idx = low.index("symbol")
                    stamp_idx = ci
                    break
            if hdr_row >= 0:
                break
        if hdr_row < 0 or sym_idx < 0 or stamp_idx < 0:
            return None
        s_col = _col_idx_to_a1(sym_idx)
        t_col = _col_idx_to_a1(stamp_idx)
        first = hdr_row + 2
        syms = sheets.read_values(
            spreadsheet_id, sheet_name, "%s%d:%s" % (s_col, first, s_col))
        stamps = sheets.read_values(
            spreadsheet_id, sheet_name, "%s%d:%s" % (t_col, first, t_col))
        if not syms:
            return None
        out: Dict[str, Any] = {}
        n = len(syms)
        for i in range(n):
            raw = syms[i][0] if (syms[i] and len(syms[i])) else ""
            sym = str(raw or "").strip().upper()
            if not sym or sym in ("SYMBOL", "TICKER"):
                continue
            cell = ""
            if stamps and i < len(stamps) and stamps[i]:
                cell = stamps[i][0]
            if sym not in out:
                out[sym] = _parse_stamp_cell(cell)
        return out or None
    except Exception:
        return None


def _order_symbols_oldest_first(symbols: List[str],
                                stamps: Dict[str, Any]) -> List[str]:
    """v6.27.0: STABLE sort of the worklist by stamp ascending; symbols
    with no parseable stamp sort FIRST (datetime.min) — never-refreshed
    rows outrank everything. Ties keep the incoming (heal-first-aware)
    order, so equal-stamp cohorts round-robin deterministically."""
    _min = datetime.min
    return sorted(symbols,
                  key=lambda s: stamps.get(str(s).strip().upper()) or _min)


def _apply_stale_skip_escalation(results: List["TaskResult"], sheets: Any,
                                 spreadsheet_id: str) -> None:
    """v6.26.0 post-run pass (see header WHY block). Mutates TaskResult
    statuses in place BEFORE the summary tally, so the existing exit-code
    policy (failed>0 -> 2) turns a chronic dark page into a RED leg. Also
    emits one [PAGE-VERDICT] line per task. Entirely inert when
    TFB_SYNC_STALE_SKIP_RED=0."""
    if not _stale_skip_red_enabled():
        return
    thr = _skip_max_stale_h()
    for r in results:
        ranked = r.sheet_name in _RANKED_MARKET_PAGES
        age: Optional[float] = None
        if ranked:
            age = _page_newest_stamp_age_h(sheets, spreadsheet_id,
                                           r.sheet_name)
        if ranked and r.status == "skipped":
            wtxt = " | ".join(r.warnings or [])
            benign = any(m in wtxt for m in _BENIGN_SKIP_MARKERS)
            if (not benign) and age is not None and age > thr:
                first_guard = (r.warnings[0][:180] if r.warnings else "n/a")
                r.status = "failed"
                r.error = (
                    "%s '%s' guard-skipped while its newest data stamp is "
                    "%.1fh old (> %.0fh): a protective skip is tolerable "
                    "once, but skipping an ALREADY-STALE page is a data "
                    "outage — failing the leg so it cannot report green. "
                    "First guard: %s" % (_STALE_SKIP_TAG, r.sheet_name,
                                         age, thr, first_guard))
                logger.error(r.error)
        try:
            reason = (r.warnings[0] if r.warnings else (r.error or "clean"))
            logger.info(
                "%s page=%s status=%s rows_written=%d "
                "newest_stamp_age_h=%s reason=%s",
                _PAGE_VERDICT_TAG, r.sheet_name, r.status, r.rows_written,
                ("%.1f" % age) if age is not None else "NA",
                str(reason)[:120])
        except Exception:
            pass


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
    # v6.39.1 (W1A-4b): freshness accounting for the _Status stamp — updated at
    # each preservation/stub site, read only by _stamp_page_status (finally).
    res.dry_run = bool(dry_run)
    headers = None  # v6.39.3: finally-stamp reads this; must exist on every exit
    res._stamp_meta = {"requested": 0, "pre_persist_rows": None, "klg_kept": 0,
                       "persist_restored": 0, "pv2_restored": 0, "stubbed": 0}

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
                _existing_syms, _critical_universe_changes = sanitize_active_universe(
                    _existing_syms
                )
                if _critical_universe_changes:
                    _change_notes = []
                    for _change in _critical_universe_changes[:20]:
                        if _change.target_symbol:
                            _change_notes.append(
                                f"{_change.source_symbol}->{_change.target_symbol} "
                                f"({_change.action})"
                            )
                        else:
                            _change_notes.append(
                                f"{_change.source_symbol} ({_change.action}: "
                                f"{_change.reason})"
                            )
                    _cw = (
                        "[CRITICAL-IDENTITY v1.0.0] sanitized active universe on "
                        f"'{task.sheet_name}': " + "; ".join(_change_notes)
                    )
                    res.warnings.append(_cw)
                    logger.warning(_cw)
            if _existing_syms:
                symbols = _existing_syms
                res.warnings.append(
                    f"{_MARKET_READBACK_TAG} sourced {len(symbols)} symbol(s) from the {task.sheet_name} sheet"
                )
                # v6.27.0 [OLDEST-FIRST]: under any per-run budget the kept
                # slice must be the STALEST slice, not the sheet head (the
                # 2026-07-23 GM starvation — see the header WHY block).
                if (_oldest_first_enabled()
                        and task.sheet_name in _RANKED_MARKET_PAGES):
                    _stamp_map = _page_symbol_stamps(
                        sheets, spreadsheet_id, task.sheet_name)
                    if _stamp_map:
                        symbols = _order_symbols_oldest_first(
                            symbols, _stamp_map)
                        _n_uns = sum(
                            1 for _s in symbols
                            if _stamp_map.get(str(_s).strip().upper()) is None)
                        try:
                            logger.info(
                                "%s %s: worklist reordered stalest-first "
                                "(%d symbols; %d never-stamped lead)",
                                _OLDEST_FIRST_TAG, task.sheet_name,
                                len(symbols), _n_uns)
                        except Exception:
                            pass
                # v6.35.0 [DECISION-FIRST]: the operator's tradable set must
                # never wait behind the tail (header WHY). Applied AFTER the
                # oldest-first sort so promoted symbols keep stalest-first
                # order among themselves; ahead of never-stamped stubs by
                # design — decision freshness beats stub repair by minutes.
                if (_priority_fetch_enabled()
                        and task.sheet_name in _RANKED_MARKET_PAGES):
                    _pri_set = _decision_priority_symbols(
                        sheets, spreadsheet_id)
                    if _pri_set:
                        symbols, _n_moved = _apply_decision_first(
                            symbols, _pri_set)
                        if _n_moved:
                            try:
                                logger.info(
                                    "%s %s: prioritized %d decision "
                                    "symbol(s) of %d",
                                    _DECISION_FIRST_TAG, task.sheet_name,
                                    _n_moved, len(symbols))
                            except Exception:
                                pass
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
            safe_limit = min(_request_limit_ceiling(), max(1, len(symbols)))
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

        # Capture current-run critical identity proof before persistence or
        # KEEP-LAST-GOOD can restore a valid predecessor.  Those mechanisms
        # may preserve stored data, but may never turn a missing/rejected
        # provider response into a green verdict.
        _critical_identity_failures: list = []

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

        if task.expects_rows and symbols and headers:
            rows_matrix, _critical_identity_failures = validate_fresh_critical_rows(
                headers, rows_matrix, symbols
            )
            if _critical_identity_failures:
                _fresh_msg = (
                    "[CRITICAL-IDENTITY v1.0.0] current fetch did not provide "
                    "valid identity proof for: "
                    + "; ".join(
                        f"{failure.symbol} ({failure.reason})"
                        for failure in _critical_identity_failures
                    )
                )
                res.warnings.append(_fresh_msg)
                logger.error(_fresh_msg)

        # No creds => partial (data fetched but not written). Critical identity
        # validation has already run, so this path cannot report green when
        # fresh identity proof is missing or invalid.
        if sheets is None or sheets._get_service() is None:
            res.status = "partial"
            res.warnings.append("No Google Sheets credentials. Backend data fetched but not written.")
            res.rows_written = 0
            res.rows_failed = len(rows_matrix or [])
            fail_result_on_identity(res, _critical_identity_failures)
            return res

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
                    fail_result_on_identity(res, _critical_identity_failures)
                    return res
            except Exception as _ie:  # never let the tripwire break the write path
                _iw = f"{_IDENTITY_TAG} skipped (error: {_ie})"
                res.warnings.append(_iw)
                logger.warning(_iw)
        # ---------------------------------------------------------------------

        # --- Coherence tripwire (v6.23.0 L3b) --------------------------------
        # L3 can only see symbols someone thought to curate: on the 2026-07-12
        # export it checked 19 anchors on Market_Leaders, 22 on Global_Markets
        # (0.6% of 3,762 rows) and ZERO on Commodities_FX and Mutual_Funds — and
        # Mutual_Funds was 33% poisoned while both layers looked straight past it.
        # L3b needs no list. P/E == Price / EPS is an identity the payload states
        # about itself, and the three fields straddle the seam the transposition
        # splits (Price = quote block, correct; EPS + P/E = enrichment block,
        # misassigned). Break rate on the live sheet: poisoned GM 89.2%, clean GM
        # 07-08 rows 0.0%, clean ML 1.0% (pure GBX/GBP pence, excluded by the
        # [50,200] unit band). Trip at >25% of testable rows. Pages that cannot
        # form the ratio (< MIN_ROWS testable — funds/FX carry no EPS) are never
        # judged. Same remedy as L3: skip clear+write, PRESERVE last-good rows.
        # TFB_SYNC_COHERENCE_TRIPWIRE=0 disables (not recommended).
        if (_coherence_enabled() and task.expects_rows and rows_matrix and headers):
            try:
                _coh_n, _coh_bad, _coh_ex = _coherence_scan(headers, rows_matrix)
                _coh_min = _coherence_min_rows()
                if _coh_n >= _coh_min:
                    _coh_pct = (100.0 * _coh_bad / _coh_n) if _coh_n else 0.0
                    res.warnings.append(
                        f"{_COHERENCE_TAG} {task.sheet_name}: testable={_coh_n} "
                        f"incoherent={_coh_bad} ({_coh_pct:.1f}%) "
                        f"threshold={_coherence_max_bad_pct()}%"
                    )
                    if _coh_pct > float(_coherence_max_bad_pct()):
                        _cex = "; ".join(
                            f"{_s} statedPE={_p} impliedPE={_i}" for _s, _p, _i in _coh_ex[:6]
                        )
                        _cmsg = (
                            f"{_COHERENCE_TAG} TRIPPED on '{task.sheet_name}': "
                            f"{_coh_bad}/{_coh_n} testable rows ({_coh_pct:.1f}%) state a "
                            f"P/E that does not equal Price/EPS ({_cex}) — the enrichment "
                            f"block is symbol<->attribute transposed against the quote "
                            f"block. Skipping clear+write to PRESERVE last-good rows; the "
                            f"next healthy sync self-heals. "
                            f"TFB_SYNC_COHERENCE_TRIPWIRE=0 disables (not recommended)."
                        )
                        res.status = "skipped"
                        res.rows_written = 0
                        res.rows_failed = 0
                        res.warnings.append(_cmsg)
                        logger.error(_cmsg)
                        fail_result_on_identity(res, _critical_identity_failures)
                        return res
                else:
                    res.warnings.append(
                        f"{_COHERENCE_TAG} {task.sheet_name}: not judged "
                        f"(testable={_coh_n} < min={_coh_min}; this page carries no "
                        f"EPS/PE pair — L3 anchors cover it instead)"
                    )
            except Exception as _ce:  # never let the tripwire break the write path
                _cw = f"{_COHERENCE_TAG} skipped (error: {_ce})"
                res.warnings.append(_cw)
                logger.warning(_cw)
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
                fail_result_on_identity(res, _critical_identity_failures)
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
            fail_result_on_identity(res, _critical_identity_failures)
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
            # v6.25.0: the veto exists to stop a short FULL-TABLE write from
            # dropping missed symbols — but the v6.19.0 persistence pass runs
            # immediately below and appends the last-good row of EVERY
            # requested-but-missing symbol, so a floored partial cannot drop
            # anything when persistence can run. Veto only when it cannot
            # (persistence off, or no Sheets handle), or when explicitly
            # forced strict via TFB_SYNC_FLOOR_STRICT=1. Run #2413 evidence:
            # the unconditional veto turned a whole hour of fetching into
            # rows_written=0 on every page, every leg.
            _persist_ok = (_symbol_persistence_enabled() and sheets is not None
                           and not _floor_strict_enabled())
            if not _persist_ok:
                msg = (
                    f"Partial fetch on '{task.sheet_name}': {len(rows_matrix)} row(s) "
                    f"for {len(symbols)} requested symbol(s) ({_cov:.0f}% coverage, "
                    f"floor {_cov_floor:.0f}%). Skipping write to PRESERVE last-good "
                    f"rows — persistence unavailable, so writing this would "
                    f"permanently drop the missed symbols from the page (it is its "
                    f"own symbol source). Self-heals on the next healthy sync."
                )
                res.status = "skipped"
                res.rows_written = 0
                res.rows_failed = 0
                res.warnings.append(msg)
                logger.warning(msg)
                fail_result_on_identity(res, _critical_identity_failures)
                return res
            msg = (
                f"[v6.25.0 FLOOR-MERGE] Partial fetch on '{task.sheet_name}': "
                f"{len(rows_matrix)} fresh row(s) for {len(symbols)} requested "
                f"({_cov:.0f}% coverage, floor {_cov_floor:.0f}%) — proceeding "
                f"through the persistence merge; every missed symbol keeps its "
                f"last-good row, heal-first fronts the remainder next leg."
            )
            res.warnings.append(msg)
            logger.warning(msg)
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
                res._stamp_meta["requested"] = len(symbols or [])
                res._stamp_meta["pre_persist_rows"] = len(rows_matrix or [])
                rows_matrix, _kept_syms = _persist_missing_symbol_rows(
                    sheets, spreadsheet_id, task.sheet_name, headers, rows_matrix, symbols
                )
                if _kept_syms:
                    res._stamp_meta["persist_restored"] = len(_kept_syms)
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
                    res._stamp_meta["klg_kept"] = len(_klg_syms)
                    _kw = (
                        f"{_KEEP_LAST_GOOD_TAG} substituted {len(_klg_syms)} "
                        f"error-stub row(s) with last-good data on "
                        f"'{task.sheet_name}': {', '.join(_klg_syms[:15])}"
                        f"{'…' if len(_klg_syms) > 15 else ''} — a backend error "
                        f"stub no longer erases a symbol's data."
                    )
                    res.warnings.append(_kw)
                    logger.warning(_kw)
                _idfw_klg_suspects = list(_LAST_KLG_ID_SUSPECTS)
                if _idfw_klg_suspects:
                    _sw = (
                        f"{_IDFW_TAG} refused to keep {len(_idfw_klg_suspects)} "
                        f"identity-suspect predecessor row(s) on "
                        f"'{task.sheet_name}': "
                        f"{', '.join(_idfw_klg_suspects[:15])}"
                        f"{'…' if len(_idfw_klg_suspects) > 15 else ''} — fresh "
                        f"stub written so the next healthy fetch can heal it."
                    )
                    res.warnings.append(_sw)
                    logger.warning(_sw)
            except Exception as _ke:  # never let the guard break the write path
                _kw = f"{_KEEP_LAST_GOOD_TAG} skipped (error: {_ke})"
                res.warnings.append(_kw)
                logger.warning(_kw)
                _idfw_klg_suspects = []
            # v6.29.0 B-4: forced-refetch visibility — report the INCOMING
            # identity for every forced symbol so a provider re-sending the
            # wrong instrument is caught on the very next run.
            _forced_now = _force_refetch_symbols()
            if _forced_now:
                try:
                    _f_sym_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
                    _f_name_i = _guard_find_col(list(headers), _GUARD_NAME_ALIASES)
                    _f_px_i = _guard_find_col(list(headers), _XPAGE_PRICE_ALIASES)
                    _f_prov_i = _guard_find_col(list(headers), _KLG_PROVIDER_ALIASES)
                    for _f_row in rows_matrix:
                        if not isinstance(_f_row, list) or _f_sym_i < 0 or _f_sym_i >= len(_f_row):
                            continue
                        _f_t = str(_f_row[_f_sym_i]).strip().upper()
                        if _f_t not in _forced_now:
                            continue
                        def _f_cell(i):
                            return _f_row[i] if 0 <= i < len(_f_row) else ""
                        _fw = (
                            f"{_FORCE_REFETCH_TAG} {task.sheet_name} {_f_t} "
                            f"incoming name='{str(_f_cell(_f_name_i))[:48]}' "
                            f"price={_f_cell(_f_px_i)} provider={_f_cell(_f_prov_i)}"
                            f"{' | klg-substitution BLOCKED' if _f_t in _LAST_KLG_FORCED else ''}"
                            f" — verify identity, then REMOVE the env after this run."
                        )
                        res.warnings.append(_fw)
                        logger.warning(_fw)
                except Exception as _fe:
                    logger.warning(f"{_FORCE_REFETCH_TAG} report skipped (error: {_fe})")
        else:
            _klg_syms = []
            _idfw_klg_suspects = []

        # --- v6.24.0 FW-2: OUTGOING row identity firewall -----------------
        # Runs on every market page AFTER KLG (so kept rows are re-checked
        # too - belt over the FW-1 suspenders) and BEFORE the write. A page
        # 24% poisoned sails under L3b's 25% page trip; FW-2 stops each row
        # individually. Expected strips on a healthy backend: 0.
        _idfw_stripped: list = []
        if (_row_firewall_enabled() and task.expects_rows
                and rows_matrix and headers
                and task.sheet_name in _RANKED_MARKET_PAGES):
            try:
                rows_matrix, _idfw_stripped = _row_identity_firewall(
                    headers, rows_matrix
                )
                # --- v6.31.0 FW-5: fabricated-placeholder tripwire --------
                # Merges into _idfw_stripped so FW-KEEP last-good restore
                # and the FW-3 verdict cover FW-5 strips with zero plumbing.
                if _placeholder_guard_enabled():
                    rows_matrix, _fab_stripped = _fabrication_tripwire(
                        headers, rows_matrix
                    )
                    if _fab_stripped:
                        _fab_msg = (
                            f"{_IDFW_TAG} FW-5 quarantined "
                            f"{len(_fab_stripped)} FABRICATED placeholder "
                            f"row(s) on '{task.sheet_name}': "
                            f"{', '.join(_fab_stripped[:15])}"
                            f"{'…' if len(_fab_stripped) > 15 else ''} — "
                            f"route-fabricated name/provider pattern."
                        )
                        res.warnings.append(_fab_msg)
                        logger.warning(_fab_msg)
                        _seen_strip = set(_idfw_stripped)
                        _idfw_stripped = list(_idfw_stripped) + [
                            s for s in _fab_stripped if s not in _seen_strip
                        ]
                if _idfw_stripped:
                    _fw = (
                        f"{_IDFW_TAG} quarantined {len(_idfw_stripped)} "
                        f"identity-broken outgoing row(s) on "
                        f"'{task.sheet_name}': "
                        f"{', '.join(_idfw_stripped[:15])}"
                        f"{'…' if len(_idfw_stripped) > 15 else ''} — Symbol "
                        f"kept, every other cell blanked + tagged."
                    )
                    res.warnings.append(_fw)
                    logger.warning(_fw)

                    # v6.25.1 FW-KEEP: the KLG stub-swap ran BEFORE FW-2, so
                    # these fresh stubs have no rescuer — run a targeted
                    # second swap for exactly the stripped symbols, then
                    # re-tag Warnings so the quarantine stays visible. New
                    # symbols with no last-good remain stubs (correct).
                    if _fw_keep_last_good_enabled() and sheets is not None:
                        try:
                            rows_matrix, _fwk_restored = _keep_last_good_rows(
                                sheets, spreadsheet_id, task.sheet_name,
                                headers, rows_matrix,
                            )
                            _fwk_set = {s for s in _fwk_restored
                                        if s in set(_idfw_stripped)}
                            if _fwk_set:
                                _w_i = -1
                                for _hi2, _h2 in enumerate(headers):
                                    if str(_h2 or "").strip().casefold() == "warnings":
                                        _w_i = _hi2
                                        break
                                _s_i = _guard_find_col(list(headers), _GUARD_SYMBOL_ALIASES)
                                if _w_i >= 0 and _s_i >= 0:
                                    for _row2 in rows_matrix:
                                        if (isinstance(_row2, list)
                                                and _s_i < len(_row2)
                                                and str(_row2[_s_i] or "").strip().upper() in _fwk_set
                                                and _w_i < len(_row2)):
                                            _row2[_w_i] = "identity_quarantined:kept_last_good:v6.25.1"
                            _unrest = len(_idfw_stripped) - len(_fwk_set)
                            _kw = (
                                f"[v6.25.1 FW-KEEP] '{task.sheet_name}': restored "
                                f"{len(_fwk_set)}/{len(_idfw_stripped)} quarantined "
                                f"row(s) from last-good"
                                f"{f'; {_unrest} had no last-good (left as stub)' if _unrest else ''}."
                            )
                            res.warnings.append(_kw)
                            logger.warning(_kw)
                        except Exception as _ke:
                            logger.warning(
                                "[v6.25.1 FW-KEEP] restore skipped on '%s': %s",
                                task.sheet_name, _ke,
                            )
            except Exception as _fe:
                logger.warning("%s outgoing firewall skipped: %s", _IDFW_TAG, _fe)

        # --- v6.24.1 FW-4: name-dedup census / quarantine ------------------
        _idfw_dup_groups: dict = {}
        _idfw_dup_quar: list = []
        if (task.expects_rows and rows_matrix and headers
                and task.sheet_name in _RANKED_MARKET_PAGES):
            rows_matrix, _idfw_dup_groups, _idfw_dup_quar = _name_dedup_apply(
                headers, rows_matrix
            )
            if _idfw_dup_groups:
                _dw = (
                    f"{_IDFW_TAG} name_dup: {len(_idfw_dup_groups)} Name(s) on "
                    f">= {_name_dedup_min()} symbols in the outgoing batch on "
                    f"'{task.sheet_name}' (mode={_name_dedup_mode()}"
                    f"{', quarantined ' + str(len(_idfw_dup_quar)) if _idfw_dup_quar else ''}). "
                    f"Top: " + "; ".join(
                        k + " -> " + ",".join(v[:4]) + ("…" if len(v) > 4 else "")
                        for k, v in list(_idfw_dup_groups.items())[:3]
                    )
                )
                res.warnings.append(_dw)
                logger.warning(_dw)

        # --- v6.30.0: exact critical Symbol->Issuer firewall -----------------
        # Page-level anchor thresholds intentionally tolerate one mismatch; these
        # known collision symbols do not. Purge a poisoned predecessor by writing
        # a tagged symbol-only stub, then force the page result RED after write.
        if (task.expects_rows and rows_matrix and headers
                and task.sheet_name in _RANKED_MARKET_PAGES):
            rows_matrix, _outgoing_critical_failures = quarantine_critical_rows(
                headers, rows_matrix
            )
            _known_critical_failures = {
                (failure.symbol, failure.reason) for failure in _critical_identity_failures
            }
            _critical_identity_failures.extend(
                failure for failure in _outgoing_critical_failures
                if (failure.symbol, failure.reason) not in _known_critical_failures
            )
            if _critical_identity_failures:
                _cf = (
                    "[CRITICAL-IDENTITY v1.0.0] quarantined "
                    f"{len(_critical_identity_failures)} exact identity mismatch(es) "
                    f"on '{task.sheet_name}': "
                    + "; ".join(
                        f"{_f.symbol}={_f.seen_name!r} ({_f.reason})"
                        for _f in _critical_identity_failures[:10]
                    )
                    + " — page verdict will be failed even if the stub write succeeds."
                )
                res.warnings.append(_cf)
                logger.error(_cf)

        # --- v6.24.0 FW-3: workbook verdict line (best-effort) ------------
        if (task.expects_rows and task.sheet_name in _RANKED_MARKET_PAGES
                and sheets is not None):
            try:
                _append_runlog_idfirewall(
                    sheets, spreadsheet_id, task.sheet_name,
                    len(_klg_syms or []), _idfw_klg_suspects, _idfw_stripped,
                    _idfw_dup_groups, _idfw_dup_quar,
                )
            except Exception:
                pass
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
            _old_name_map: dict = {}
            if _persist_v2_enabled():
                # v6.34.0 PV-2: SECOND-CHANCE persistence — later stages
                # (batch-identity / KLG / firewall) may have dropped rows the
                # first pass saw as present. Idempotent re-injection.
                try:
                    rows_matrix, _kept2 = _persist_missing_symbol_rows(
                        sheets, spreadsheet_id, task.sheet_name, headers,
                        rows_matrix, symbols)
                    res._stamp_meta["pv2_restored"] = len(_kept2 or [])
                    if _kept2:
                        _p2 = (f"[PERSIST v6.34.0] second-chance pass restored "
                               f"{len(_kept2)} row(s) dropped by later stages on "
                               f"'{task.sheet_name}': {', '.join(_kept2[:12])}"
                               f"{'…' if len(_kept2) > 12 else ''}")
                        res.warnings.append(_p2)
                        logger.warning(_p2)
                        if _persist_sanity_enabled():
                            rows_matrix, _psq = _persist_second_chance_sanity(
                                headers, rows_matrix, set(_kept2))
                            if _psq:
                                _p3 = (f"[PERSIST v6.36.0 PV-3] sanity screen "
                                       f"quarantined {len(_psq)} of "
                                       f"{len(_kept2)} restored row(s) on "
                                       f"'{task.sheet_name}': "
                                       f"{', '.join(_psq[:12])}"
                                       f"{'…' if len(_psq) > 12 else ''}")
                                res.warnings.append(_p3)
                                logger.warning(_p3)
                except Exception as _p2e:
                    logger.warning("[PERSIST v6.34.0] second-chance skipped (%s)", _p2e)
                try:
                    _old_name_map = _page_old_name_map(sheets, spreadsheet_id, task.sheet_name)
                except Exception:
                    _old_name_map = {}
            # v6.37.0 PL-1: operator quarantine list — runs on the FINAL
            # matrix regardless of PV-2/PV-3 state so a poison-locked row
            # is stubbed whether it arrived via restore or via a
            # guard-rejected fetch's carry. Empty env (default) = no-op.
            try:
                rows_matrix, _plq = _apply_operator_quarantine(headers, rows_matrix)
                if _plq:
                    res._stamp_meta["stubbed"] += len(_plq)
                    _pl = (f"[PL-1 v6.37.0] operator quarantine stubbed "
                           f"{len(_plq)} row(s) on '{task.sheet_name}': "
                           f"{', '.join(_plq[:12])}"
                           f"{'…' if len(_plq) > 12 else ''} — poisoned "
                           f"prior erased; next leg refetches with no "
                           f"prior to be compared against. REMOVE "
                           f"TFB_SYNC_QUARANTINE_SYMBOLS after one green run.")
                    res.warnings.append(_pl)
                    logger.warning(_pl)
            except Exception as _ple:
                logger.warning("[PL-1 v6.37.0] operator quarantine skipped (%s)", _ple)
            try:
                _still_missing = _unpersisted_missing(headers, rows_matrix, symbols, _old_name_map)
            except Exception as _ve:
                # v6.39.1 (external audit P0-4, ACCEPTED): the HARD guard exists
                # to prevent silent symbol deletion — its own failure must fail
                # CLOSED, not convert into a clean pass. Skip the write,
                # preserve last-good; self-heals next leg.
                _vw = (f"{_PERSISTENCE_HARD_TAG} verification ERRORED on "
                       f"'{task.sheet_name}' ({type(_ve).__name__}: {_ve}) — "
                       f"fail-closed: skipping clear+write to PRESERVE last-good "
                       f"rows. TFB_SYNC_PERSISTENCE_HARD=0 restores the old "
                       f"warn-and-continue.")
                res.status = "skipped"
                res.rows_written = 0
                res.rows_failed = 0
                res.warnings.append(_vw)
                logger.error(_vw)
                fail_result_on_identity(res, _critical_identity_failures)
                return res
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
                fail_result_on_identity(res, _critical_identity_failures)
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

        # --- v6.43.0 (W1A-6e) LAKE PROBE — foreign-writer attribution --------
        # In-repo census: the guarded seam below is this repository's ONLY
        # row-writer for these pages, yet the lake diverges from what each
        # leg writes — the filler is out-of-repo (GAS cockpit /
        # eodhd-screener). Read the lake BEFORE overwriting it; join by
        # Symbol; log the residue. Read-only; rides the W1A-6 gate;
        # fail-open twice (helper + this guard). NEVER touches rows_matrix
        # or res.status.
        try:
            if _ohlc_lake_enabled():
                _lk = _ohlc_lake_probe(sheets, spreadsheet_id,
                                       task.sheet_name, headers, rows_matrix)
                if _lk and not _lk.get("error"):
                    _ff = int(_lk.get("foreign_open_fill") or 0)
                    _fn = int(_lk.get("foreign_name_diff") or 0)
                    _lkl = (f"{_OHLC_LAKE_TAG} {task.sheet_name} | "
                            f"lake_checked={_lk.get('lake_checked')} "
                            f"lake_flagged={_lk.get('lake_flagged')} "
                            f"lake_blank_open={_lk.get('lake_blank_open')} | "
                            f"foreign_open_fill={_ff} "
                            f"foreign_name_diff={_fn}"
                            + ((" | ex: " + ", ".join(_lk.get("examples") or []))
                               if (_ff or _fn) else ""))
                    if _ff or _fn:
                        res.warnings.append(_lkl)
                        logger.warning(_lkl)
                    else:
                        logger.info(_lkl)
                    try:
                        _append_runlog_ohlc_lake(
                            sheets, spreadsheet_id, task.sheet_name, _lk)
                    except Exception:
                        pass
                elif _lk.get("error"):
                    logger.warning("%s unavailable on %s (%s)",
                                   _OHLC_LAKE_TAG, task.sheet_name,
                                   _lk["error"])
        except Exception as _lke:
            logger.warning("%s probe skipped on %s (%s)",
                           _OHLC_LAKE_TAG, task.sheet_name, _lke)
        # ---------------------------------------------------------------------

        # --- v6.54.0 FALSE-GREEN SCREEN (identity domain / fetch_failed) -----
        # DOWNGRADE-ONLY: a non-ticker or fetch_failed row may never leave the
        # sync as INVEST / INVESTABLE (CFX "Copper Futures", 08-13 -> 08-31).
        if _false_green_screen_enabled():
            try:
                rows_matrix, _fg = _apply_false_green_screen(
                    headers, rows_matrix, task.sheet_name)
                if _fg.get("blocked"):
                    _fgl = (f"{_FG_TAG} {task.sheet_name} | "
                            f"checked={_fg['checked']} blocked={_fg['blocked']} "
                            f"(domain={_fg['domain']} fetchfail={_fg['fetchfail']})"
                            f" | ex: {', '.join(_fg['examples'][:8])}")
                    logger.warning(_fgl)
                    print("::warning::%s" % _fgl)
            except Exception as _fge:
                logger.warning("%s skipped: %s", _FG_TAG, _fge)
        # --- v6.38.0 (W1A-6) PRE-WRITE OHLC COHERENCE ------------------------
        # LAST checkpoint before the sheet: the engine-side Fix BC guard runs
        # at fetch and cannot see the assembly-layer Open leak (589 foreign
        # Opens on 2026-08-17, zero ":open:" engine tags, three engine drops
        # resurfacing on-sheet). observe = log-only; enforce = blank offending
        # members + tag Warnings. OFF (default) => matrix byte-untouched.
        _oc_stats_for_readback: dict = {}
        if _ohlc_prewrite_enabled():
            try:
                rows_matrix, _oc = _apply_ohlc_prewrite_guard(
                    headers, rows_matrix, task.sheet_name)
                _oc_stats_for_readback = dict(_oc or {})
                if _oc.get("checked"):
                    _ocl = (f"{_OHLC_PREWRITE_TAG} {task.sheet_name} | "
                            f"checked={_oc['checked']} flagged={_oc['flagged']} "
                            f"(open={_oc['open']} price_band={_oc['price_band']} "
                            f"range={_oc['range']}) | "
                            f"blank(o/h/l)={_oc.get('blank_open', 0)}/"
                            f"{_oc.get('blank_hi', 0)}/"
                            f"{_oc.get('blank_lo', 0)} | "
                            f"zero_band={_oc.get('zero_band', 0)} "
                            f"tol_excused={_oc.get('tol_excused', 0)} "
                            f"open_masked={_oc.get('open_masked', 0)} | "
                            f"mode={_ohlc_prewrite_mode()} tol={_ohlc_prewrite_tol()}"
                            + (f" | ex: {', '.join(_oc['examples'][:12])}"
                               f"{'…' if _oc['flagged'] > 12 else ''}"
                               if _oc["flagged"] else ""))
                    if _oc["flagged"]:
                        res.warnings.append(_ocl)
                        logger.warning(_ocl)
                    else:
                        logger.info(_ocl)
                    # v6.39.4 (W1A-6b): the two lines above land ONLY in the
                    # ephemeral Actions job log. Mirror the verdict into the
                    # workbook's _Run_Log through the proven FW-3 channel so
                    # the observe->enforce decision has durable evidence.
                    # Fail-open twice over (inner helper + this guard): a
                    # telemetry fault must never touch the write path.
                    try:
                        _append_runlog_ohlc_prewrite(
                            sheets, spreadsheet_id, task.sheet_name, _oc)
                    except Exception:
                        pass
            except Exception as _oce:
                if _ohlc_prewrite_mode() == "enforce":
                    # v6.39.1 (external audit P0-4, ACCEPTED): in ENFORCE the
                    # guard IS the integrity contract — its own failure means
                    # an unverified payload. Fail closed; preserve last-good.
                    _om = (f"{_OHLC_PREWRITE_TAG} guard ERRORED in enforce mode "
                           f"on '{task.sheet_name}' ({type(_oce).__name__}: "
                           f"{_oce}) — fail-closed: skipping write.")
                    res.status = "skipped"
                    res.rows_written = 0
                    res.rows_failed = 0
                    res.warnings.append(_om)
                    logger.error(_om)
                    fail_result_on_identity(res, _critical_identity_failures)
                    return res
                logger.warning("%s skipped on %s (observe; %s)",
                               _OHLC_PREWRITE_TAG, task.sheet_name, _oce)
        # ---------------------------------------------------------------------

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

        # v6.45.0 R1: hold the write window (no-op unless armed; fail-open).
        _sync_hold_publish(sheets, spreadsheet_id, task.sheet_name)
        try:
            written = sheets.write_table(spreadsheet_id, task.sheet_name, start_cell, headers, rows_matrix)
            res.rows_written = int(written)
            # v6.45.0 R4: seed the stamp manifest with payload identity +
            # prewrite verdict (additive; _stamp_meta declared since 6.39.3).
            try:
                _sm = dict(getattr(res, "_stamp_meta", None) or {})
                _sm["payload_sha8"] = _payload_sha8(headers, rows_matrix)
                _sm["pw_flagged"] = int(
                    (_oc_stats_for_readback or {}).get("flagged") or 0)
                _sm["pw_checked"] = int(
                    (_oc_stats_for_readback or {}).get("checked") or 0)
                res._stamp_meta = _sm
            except Exception:
                pass
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
            if _critical_identity_failures:
                fail_result_on_identity(res, _critical_identity_failures)

            # --- v6.41.0 (W1A-6c) POST-WRITE READBACK ---------------------
            # The ONLY code in this program that reads a page after writing
            # it. Everything else — both OHLC guards, the identity firewall,
            # every tripwire — inspects the OUTGOING matrix, which is why a
            # 618-row on-sheet contamination has been invisible to a guard
            # that this same morning reported flagged=5 on the same 6,627
            # rows. Strictly observational: it cannot change res.status, it
            # cannot touch rows_matrix, and it is DEFAULT OFF.
            # Fail-open twice (inner helper + this guard).
            try:
                if _ohlc_readback_enabled() and not _ohlc_prewrite_enabled():
                    logger.info(
                        "%s skipped on %s — no baseline "
                        "(TFB_SYNC_OHLC_PREWRITE=0; audit F2 contract)",
                        _OHLC_READBACK_TAG, task.sheet_name)
                elif _ohlc_readback_enabled():
                    _rb = _ohlc_readback_verify(
                        sheets, spreadsheet_id, task.sheet_name,
                        headers, rows_matrix, start_cell,
                        _oc_stats_for_readback)
                    if _rb:
                        if _rb.get("error"):
                            _rl = (f"{_OHLC_READBACK_TAG} "
                                   f"{task.sheet_name} | UNAVAILABLE "
                                   f"({_rb['error']})")
                            logger.warning(_rl)
                        else:
                            _d = int(_rb.get("delta_flagged") or 0)
                            _rl = (
                                f"{_OHLC_READBACK_TAG} {task.sheet_name} | "
                                f"prewrite={_rb['prewrite_flagged']}"
                                f"/{_rb['prewrite_checked']} "
                                f"readback={_rb['readback_flagged']}"
                                f"/{_rb['readback_checked']} "
                                f"delta={_d:+d} "
                                f"rows_delta={_rb['delta_checked']:+d}")
                            _lvl, _st = _ohlc_readback_status(_rb)
                            _rl += f" status={_st}"
                            # v6.52.0: name the offenders on the same line.
                            _oda = list(_rb.get("open_diff_syms") or [])
                            if _oda:
                                _rl += (f" | attr({len(_oda)}): "
                                        + ", ".join(_oda[:60]))
                            if _lvl == "WARNING":
                                res.warnings.append(_rl)
                                logger.warning(_rl)
                            else:
                                logger.info(_rl)
                        try:
                            _append_runlog_ohlc_readback(
                                sheets, spreadsheet_id,
                                task.sheet_name, _rb)
                        except Exception:
                            pass
                        # v6.45.0 R4: readback verdict into the manifest.
                        try:
                            _sm = dict(getattr(res, "_stamp_meta", None) or {})
                            _sm["rb_flagged"] = int(
                                _rb.get("readback_flagged") or 0)
                            _sm["rb_checked"] = int(
                                _rb.get("readback_checked") or 0)
                            _sm["rb_status"] = ("UNKNOWN" if _rb.get("error")
                                                else _ohlc_readback_status(_rb)[1])
                            res._stamp_meta = _sm
                        except Exception:
                            pass
                        # v6.45.0 R2: one bounded repair pass (off by default).
                        try:
                            _rp = _ohlc_readback_repair(
                                sheets, spreadsheet_id, task.sheet_name,
                                headers, rows_matrix, start_cell, _rb)
                            if _rp and not _rp.get("error"):
                                _sm = dict(getattr(res, "_stamp_meta",
                                                   None) or {})
                                _sm["repair_after"] = _rp.get("after_flagged")
                                res._stamp_meta = _sm
                                if _rp.get("warn"):
                                    res.warnings.append(str(_rp.get("line")))
                                    logger.warning(str(_rp.get("line")))
                                else:
                                    logger.info(str(_rp.get("line")))
                        except Exception as _rpe:
                            logger.warning("%s skipped on %s (%s)",
                                           _OHLC_REPAIR_TAG,
                                           task.sheet_name, _rpe)
            except Exception as _rbe:
                logger.warning("%s skipped on %s (%s)",
                               _OHLC_READBACK_TAG, task.sheet_name, _rbe)
            # ---------------------------------------------------------------
        except Exception as e:
            res.status = "failed"
            res.error = f"Write failed: {e}"

        return res

    except Exception as e:
        res.status = "failed"
        res.error = str(e)
        return res

    finally:
        # v6.45.0 R1: release the write-window hold on EVERY exit path
        # (no-op unless this run published; TTL backstops a hard crash).
        _sync_hold_clear(sheets, spreadsheet_id)
        res.end_utc = _utc_now().isoformat()
        # --- v6.39.1 (W1A-4b) BACKEND _Status STAMP (moved to finally) -------
        # External audit P0-3, ACCEPTED: the terminus-only stamp missed every
        # early return (skips, identity fails, floor vetoes) and ran before
        # end_utc so Duration was blank. In finally it covers EVERY exit path
        # including exceptions, reports the decided OUTCOME, and carries real
        # duration + fresh/preserved accounting. OFF by default => no-op.
        try:
            _stamp_page_status(sheets, spreadsheet_id, task.sheet_name, res,
                               len(headers or []) if isinstance(headers, list) else 0)
        except Exception:
            pass
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
    # v6.39.1 (external audit P0-1, PARTIAL ACCEPT): the CLI clamp silently
    # reduced --max-symbols 7000 to 5000 while the v2 ceiling is 20000 and
    # Global_Markets holds 6,626 — the clamp itself was the binding truncator
    # on manual runs. Align the CLI ceiling with the active cap regime.
    max_symbols = _safe_int(args.max_symbols, -1, lo=-1,
                            hi=(20000 if _universe_cap_v2_enabled() else 5000))
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
    _idfw_selftest_()  # v6.24.1 ST-1: verify guards on fixtures before any page
    _ohlc_fillguard_selftest_()  # v6.44.0 FG-3: prove the fill guard pre-write

    # --- v6.32.0 MANUAL-HOLD startup gate --------------------------------
    if _manual_hold_gate_enabled() and not bool(args.dry_run):
        _mh_active, _mh_msg = _manual_hold_active(sheets, spreadsheet_id)
        if _mh_active:
            logger.warning("%s deferring entire run — %s", _MANUAL_HOLD_TAG, _mh_msg)
            _append_runlog_manual_hold(sheets, spreadsheet_id, _mh_msg)
            summary.total_tasks = 0
            return 0

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
                # v6.32.0: a task that has NOT started defers under a manual
                # hold; a task already past this point finishes page + write.
                if _manual_hold_gate_enabled() and not bool(args.dry_run):
                    _mh_a, _mh_m = _manual_hold_active(sheets, spreadsheet_id)
                    if _mh_a:
                        logger.warning("%s skipping '%s' — %s",
                                       _MANUAL_HOLD_TAG, task.sheet_name, _mh_m)
                        _now = _utc_now().isoformat()
                        return TaskResult(
                            key=task.key, sheet_name=task.sheet_name,
                            status="skipped", start_utc=_now, end_utc=_now,
                            duration_ms=0.0,
                            error=f"{_MANUAL_HOLD_TAG} {_mh_m}",
                            warnings=[f"{_MANUAL_HOLD_TAG} deferred (benign): {_mh_m}"],
                        )
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

        # v6.26.0 [STALE-SKIP]: escalate health-skips on already-stale ranked
        # pages to FAILED (existing policy below then exits 2 => RED CI leg)
        # + one [PAGE-VERDICT] line per task. Inert when
        # TFB_SYNC_STALE_SKIP_RED=0; a crash here must never sink the run.
        try:
            _apply_stale_skip_escalation(results, sheets, spreadsheet_id)
        except Exception as _sse:
            logger.warning("%s escalation pass skipped (error: %s)",
                           _STALE_SKIP_TAG, _sse)

        # --- v6.40.0 (W1A-4a) UPSTREAM DECISION-FEED VERDICT -----------------
        # Publishes the cross-page EXECUTABLE / NOT_ACTIONABLE token the
        # Top_10 cockpit consumes (contract at _UPSTREAM_VERDICT_KEY). Runs
        # AFTER escalation so a stale-skip->FAILED page correctly poisons the
        # composite. DEFAULT OFF; a crash here must never sink the run.
        try:
            _write_upstream_verdict(sheets, spreadsheet_id, results)
        except Exception as _uve:
            logger.warning("%s pass skipped (error: %s)",
                           _UPSTREAM_VERDICT_TAG, _uve)

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
        rc = asyncio.run(main_async())
        # v6.55.0 (F-11): a normally-completed run whose evidence appends failed
        # must not report success. Writes are done; only the exit code changes.
        if rc == 0 and _RUNLOG_APPEND_FAILS and _append_fail_is_error():
            _sites = ", ".join(sorted(set(_RUNLOG_APPEND_FAILS)))
            print("::error::[EVIDENCE-CLOCK v%s] %d _Run_Log append(s) FAILED this run "
                  "(sites: %s) — pages were written, but the evidence clock did not "
                  "advance. Check TFB Grid Capacity in _Status. "
                  "Kill-switch: TFB_SYNC_APPEND_FAIL_IS_ERROR=0"
                  % (SCRIPT_VERSION, len(_RUNLOG_APPEND_FAILS), _sites))
            return 3
        return rc
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        return 130
    except Exception as e:
        logger.exception("Unhandled error: %s", e)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
