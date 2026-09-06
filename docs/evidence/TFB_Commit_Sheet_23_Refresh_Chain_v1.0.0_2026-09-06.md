# Commit sheet — 23_Refresh_Chain.gs v1.0.0 (build #5, 2026-09-06)

| Item | Value |
|---|---|
| Target | Apps Script project → **new file** `23_Refresh_Chain.gs` (paste the delivered text as a new script file). Repo copy of this sheet: `docs/evidence/TFB_Commit_Sheet_23_Refresh_Chain_v1.0.0_2026-09-06.md`; optional mirror `apps_script/23_Refresh_Chain.gs` |
| Scope change vs the GO | Planned as 05_Refresh.gs v1.20.0. Delivered instead as a separate additive module: 05_Refresh.gs v1.19.0 stays **byte-identical** (no anchored edits into a 2,900-line file, no transcription risk); the chain only calls its existing public entry points. |
| New file | 495 lines, 28 functions, blob `810ed41a`, ES5 only, smart-quote scan CLEAN, never-throws |
| Depends on (typeof-guarded) | `refreshPageInBatches_`, `loadBatchCheckpoint_`, `normalizePageNameRefresh_`, `_refreshIsDecisionOwnedPage_`, `logRun_`, `writePageStatus_`, `getActivePageName_` (05_Refresh.gs v1.19.0) |
| Kill / stop | Script property `TFB_REFRESH_CHAIN_STOP=1` ends every chain at its next hop; `rchStopAllChains()` cancels immediately. The trigger handler is inert without chain state, so existing behaviour is unchanged unless `refreshPageFullManual_` is called. |
| ENV / repo | None. |

## What it does
`refreshPageFullManual_(page)` runs one normal checkpointed pass through 05_Refresh (DocumentLock; tripwire, keep-last-good, ID-firewall, manual-priority and backend-hold yields all intact). If the pass returns paused/partial and the page's batch checkpoint is still present, a single one-shot trigger `tfbRefreshChainContinue_` (≥ 60 s) runs the next hop, until the checkpoint clears. Completion is read from the checkpoint, not from the status panel. Each hop writes a PARTIAL page status (`hop N/40 … i / total symbols … next hop in ~60 s`) and a `_Run_Log` line (action `refreshPageFullManual`); completion writes SUCCESS/WARN with the hop count and elapsed minutes.

Guards: one trigger at a time (deduped by handler), hop cap `TFB_REFRESH_CHAIN_MAX_HOPS` (40), TTL `TFB_REFRESH_CHAIN_TTL_MIN` (240), a throwing/ok=false hop ends the chain with FAIL, decision-owned pages refused, duplicate start refused, single-shot pages complete in one hop, Global_Markets start carries the "use daily_sync single_key" note.

## Proofs
| Step | Result |
|---|---|
| Compile (node vm), ES5 lint, smart quotes | OK / CLEAN / CLEAN |
| Classifier truth table | paused / complete / failed(throw) / failed(ok=false) / paused(checkpoint present) |
| Harness ×3 — real module, mocked 05_Refresh surface + properties + triggers | **24/24 PASS ×3**, identical: 3-hop Market_Leaders run to SUCCESS with one trigger throughout; backend-hold reason surfaced; single-shot page one hop; decision-owned and unknown pages refused; throw and ok=false end with FAIL; hop cap, stop switch and TTL end with WARN keeping the checkpoint; two chains share one trigger and complete oldest-first; operator cancel; inert handler; validation warnings → WARN completion |

## Operator steps (after paste)
1. Script property `TFB_REFRESH_BATCH_SIZE = 25` (05_Refresh v1.16.0 knob; server-proven) — halves hops on every path.
2. Open Market_Leaders → editor → run `refreshPageFullManual` (active sheet) — first hop runs at once; watch `_Status`/`_Run_Log` for `hop 1/40 … next hop in ~60 s`, then SUCCESS `completed in N hop(s)`.
3. `rchChainStatus()` shows chains + trigger; `rchStopAllChains()` cancels.
4. Menu wiring belongs to 01_Menu.gs (separate file, next revision).

## Read-back
`_Run_Log` sequence for the page: `STARTED` → `PARTIAL` per hop → final `OK`/`WARN`; `_Status` row for the page ends at SUCCESS; no `tfbRefreshChainContinue_` trigger left in the project after completion.
