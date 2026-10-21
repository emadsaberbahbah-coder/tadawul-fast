TFB Commit Sheet — apps_script/26_Decision_Chain.gs v1.0.0 [refresh Portfolio_Decision + Top_10 when the sync's data lands]
Date: 2026-10-16 (Friday, evidence day) · Lane: Apps Script (paste into the bound project) · File status: NEW file · Nothing armed by this delivery

S0/S1 — Base pin
Item	Value
Base	`origin/main` 11ef134 (apps_script: 10_My_Portfolio v1.5.13, 11_Manual_Refresh_Coordinator v1.1.0, 16_Decision_Top10 v1.13.9, 24_Sync_Dispatch v1.1.0)
Delivered	`apps_script/26_Decision_Chain.gs` v1.0.0 — ES5 only (no let/const/arrow/template), ASCII only, compiled by `tests/js/apps_script_syntax.test.js` (5 sources)
Other files	`tests/js/decision_chain.test.js` (NEW), `tests/test_decision_chain_js.py` (NEW wrapper for the whole-suite job), this sheet
Existing files	UNTOUCHED (0 lines changed in 10/11/16/24)

S2 — Why (evidence)
The sync publishes My_Portfolio ~2 min into a run and the market pages at the end of its recovery leg, but no producer reacts to a publication:
- Top_10: 08:07 morning trigger + 4-hourly wrapper (13_AutoRefresh, not mirrored). 14 Oct: Global_Markets landed 03:43, Top_10 rendered ~4 h 24 min later.
- Portfolio_Decision: no scheduled producer in the mirror except the IBKR capture watcher (08:30-10:30). 16 Oct: My_Portfolio published 02:07:45 Riyadh (run 38821487562), decision ~08:45 → 6 h 37 min; 14 Oct: 02:46 → 08:50.
- So the final audit inside every sync run (`coverage_failed, decision_failed` on 16 Oct 03:54) always meets decision pages older than its cohort.
Three independent reviewers (Top_10 path, Portfolio_Decision path, Apps Script) reached the same finding on 10-16.

S3 — What
5-minute trigger `tfbDecisionChainTick` (installed only by `tfbInstallDecisionChain`), Script Property `TFB_DECISION_CHAIN` = off (DEFAULT: returns 'off' before any read or write) | observe | on. Each tick reads `_Status` once and plans at most ONE refresh (PURE `tfbDcPlanCore_`):
1. Portfolio_Decision when the My_Portfolio publication passes the existing `pdSrcReadiness_` (SUCCESS, run=N, ≤ 8 h, acquired 100%, data COMPLETE, policy known) and N is new to the chain.
2. Top_10 when `TFB Feed Global_Markets` carries run=N AND `TFB Decision Feed` carries run=N (that run's final audit job has finished, so pages are final) and N is new.
Skips: sync in flight (`dt10SyncInflightCore_`), cockpit run in progress (`DT10_RUN_MARKER` < 7 min), another chain claim < 7 min, `tfbAutomaticRefreshAllowed_` false (manual priority / backend hold), and for Top_10 the quiet windows around the S-1 board slots (`TFB_DECISION_CHAIN_QUIET`, default `07:55-08:25,16:55-17:25` Riyadh; `none` disables) so a board never reads a half-rendered cockpit.
observe: one `_Run_Log` row per (action, run) it WOULD perform (`OBSERVE`, `[DECISION-CHAIN v1.0.0] would refresh pd|t10 for run N`).
on: claim under the script lock → run the existing `refreshPortfolioDecision()` / `refreshDecisionTop10()` OUTSIDE the lock (the cockpit takes the script lock itself) → record; failures are logged `ERROR`/`FAILED` and retried on later ticks, at most 3 per (action, run).
UNTOUCHED: every refresh, gate, receipt, verdict and the release path (`release_decision_feed.yml` stays manual). No HTTP call from this file.

S4 — Audits
Battery	Result
`tests/js/decision_chain.test.js` (REAL 10/11/16/26 sources in one vm; only the two refresh entry points are spies)	C1 off = no read/refresh/log · C2 observe walks pd then t10, 2 rows, 0 refreshes · C3 on: pd → t10 → idle · C4 Top_10 waits for the run's final audit · C5 sync in flight blocks all · C6 quiet window defers Top_10 only · C7 cockpit marker defers Top_10 · C8 HTTP 503 → ERROR row, 3 attempts, then gives up and Top_10 proceeds · C9 fresh claim blocks / expired does not · C10 manual priority pauses (real coordinator) · C11 unready My_Portfolio (3/4) skips pd, t10 proceeds · C12 cockpit exception → FAILED row, never thrown · C13 lock busy · C14 pure helpers · C15 installer/remover touch only their trigger — 15/15 ×3, output digest `50c8ab90d1d3a26e`
Mutation	drop the final-audit check → C4 fails; drop in-flight → C5; drop quiet → C6; drop the attempt cap → C8
Other native harnesses	unchanged and green: decision_board_funding 64, decision_source_readiness 73, dt10_feed_cohort_binding 21/21, pf_ibkr_evidence 14/14, pf_source_receipt 8/8, portfolio_reconciliation_inputs 20, decision_input_truth digest unchanged, syntax 5/5

S5 — Delivery
Repo: `apps_script/26_Decision_Chain.gs`, `tests/js/decision_chain.test.js`, `tests/test_decision_chain_js.py`, this sheet.
Apps Script (owner): create a script file named `26_Decision_Chain` in the bound project and paste the file. Nothing runs until both steps below are taken.

S6 — Arming / read-back (owner; NOT done by this delivery)
1. Run `tfbDecisionChainStatus()` → `mode=off … next=<pd|t10|none> …` (read-only plan).
2. Set Script Property `TFB_DECISION_CHAIN=observe`, run `tfbInstallDecisionChain()`. Read-back after the next sync: `_Run_Log` rows `tfbDecisionChainTick | OBSERVE | … would refresh pd for run N` (minutes after My_Portfolio publishes) and `… would refresh t10 for run N` (minutes after `TFB Decision Feed` carries run=N).
3. Set `TFB_DECISION_CHAIN=on`. Read-back: `OK` rows for pd then t10 with the same run N; Portfolio_Decision B2 and Top_10 B2 stamped minutes after the publication instead of at 08:07/08:45; the next sync's final audit sees decision pages from the previous cohort.
Interplay: the 08:07 morning trigger, the 4-hourly wrapper and the IBKR capture watcher keep working (a cockpit run in progress defers the chain; a redundant refresh is harmless). With #781/#782 armed, the 06:15 dispatched sync lands Global_Markets ~07:10-07:35, so Top_10 would be ready ~07:15-07:40 instead of 08:07+.
Rollback: `TFB_DECISION_CHAIN=off` (instant) or `tfbRemoveDecisionChain()`.
