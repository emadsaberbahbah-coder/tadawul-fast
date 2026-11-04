# Commit Sheet - Render priority: `26_Decision_Chain.gs` v1.0.5 + `28_Plan_Of_Record.gs` v1.0.1 - 2026-11-05

**Scope:** step 2 (Top_10 render, receipt and publication sequence) of the owner's brief "Fix the Google Sheets recommendation and outcome reports" (5 Nov). It follows the 5 Nov audit of run 41901407829.

## S0 - Evidence (Apps Script executions page, `_Run_Log`, Render logs, GitHub; read-only; Riyadh times)
| Time | Execution | Result |
|---|---|---|
| 00:58:51 | `porRefreshTrigger` | 321 s |
| 01:01:14 | sync writes `UPSTREAM_READY` for run 41901407829 | all four feeds `OK` |
| 01:04:49 | `tfbDecisionChainTick` (early Top_10) | **Timed Out, 361 s**; the backend answered at 01:08:19 |
| 01:09:43 | `buildDecisionPagesV2` | 329 s (inside the render's minutes) |
| 01:13:51 | `porRefreshTrigger` | 220 s |
| 01:14:49 | `tfbDecisionChainTick` (retry) | **Timed Out, 361 s**; it started beside the Plan of Record refresh |
| 01:17:57 | the finalizer publishes `NOT_ACTIONABLE(final_publication)` | the 15-minute handshake ended at 01:15:30 |
| 01:24:49 | `tfbDecisionChainTick` | 216 s, `VERIFIED (run 41901407829)` at 01:27:55 |
| 01:30:43 | Release After Sync releases the run | expires 06:55:55 |
| 01:34:49 | `tfbDecisionChainTick` (release re-render) | **Timed Out, 361 s**; the backend answered at 01:41:12 (5 min 25 s) |
| 01:44:49 | `tfbDecisionChainTick` (retry, beside `porRefreshTrigger` 01:43:51) | 219 s; the board reads `FEED ACTIONABLE — HELD … 0 EXECUTABLE + 2 FAST-TRACK (SIZING SUSPENDED) + 8 GRACE-HELD` |

- **The board looked stuck:** a stopped execution writes nothing. There is no `_Run_Log` row and no receipt, and B2 keeps `status: running…` until the next render. Three of the four failed renders left no trace.
- **The Plan of Record is the most frequent neighbour:** it runs every 15 minutes for 125-321 s, so about half of the renders overlap one of its runs.

## S1 - WHAT (gate `TFB_RENDER_PRIORITY`, default OFF; one Script Property read per execution while off)
1. **`26_Decision_Chain.gs` 1.0.4 -> 1.0.5:**
   - **The wait:** no render (pd, t10e, t10, release re-render) is planned while the Plan of Record holds its run claim (CacheService `POR_RUNNING`). The tick reads `Plan of Record refresh running; the render waits for the next tick`. An unreadable cache never blocks. The skips that come first keep their own words (claim, manual priority, sync in flight).
   - **The stopped-render row:** a claim found expired (older than the 7-minute TTL) is a render whose execution never reached its outcome. The tick writes one `WARN` / `NOT_FINISHED` row: `<act> run N -> did not finish (claimed M min ago; an Apps Script execution is stopped at 6 minutes without a trace; no receipt was written; the attempt counts and the next tick retries)`. It then clears the claim, so the row is written once.
   - **Status:** `tfbDecisionChainStatus` adds `priority=on|off`.
2. **`28_Plan_Of_Record.gs` 1.0.0 -> 1.0.1:**
   - **The yield:** `porRefresh` skips while the cockpit's `DT10_RUN_MARKER` or the decision chain's claim is younger than 7 minutes. It returns `{skipped: 'a Top_10 / Portfolio_Decision render is running'}` and nothing is read or written; the next 15-minute tick refreshes.
   - **The claim:** the run claim lives 360 s instead of 240 s, because runs reach 321 s and the chain now reads the claim.
   - **The lock** (Codex P2 on this PR): the render check and the claim run under the same script lock `tfbDecisionChainTick` plans under (`porPriorityGate_`, tryLock 5 s).
     - Without it, a chain tick could read an empty `POR_RUNNING` while the refresh sat between its check and its claim, and both would start.
     - The lock is released before the refresh itself.
     - A busy lock skips with `the decision chain holds the script lock`; a lock fault skips with `the script lock is unavailable`.
   - **Unblocked:** a stale marker (a stopped render), a stale claim, a future stamp or junk does not block.

## S2 - Tests (node v22)
- **`node tests/js/decision_chain.test.js`: `RESULT 47/47 PASS`**, run twice, stdout SHA-256 prefix `15e7565ffe797c78`.
  - C43: the golden negative. With the gate absent, blank, off, junk or faulted, the POR claim and an expired claim change nothing, and no cache is read.
  - C44: the wait. It covers pd, t10, the early render and the release re-render; `sync in flight` keeps its words; an unreadable cache does not block.
  - C45: the `NOT_FINISHED` row is written once. The claim is cleared, the retry renders, and a `pd` claim names Portfolio_Decision.
  - C46: a fresh claim is not reported.
  - C47: the gate spellings, the status line and observe mode.
  - The v1.0.4 file fails C42 (`priority=` field).
  - Mutants 9/9 killed: gate always/never on, the POR wait removed, a cache fault blocking, no row, the claim not cleared, fresh claims reported, the status field missing, the cache read while off.
- **`node tests/test_por_v100.js`: `PASS 62/62`**, run twice, SHA-256 prefix `1b2e77a94248322f`.
  - T0 and T9 are re-pinned to 1.0.1 and 66 functions.
  - T13 is new: off = the v1.0.0 refresh. On covers a fresh marker, a bare ISO marker and a fresh claim (each skips with no read, write or claim) and a stale, future, junk or absent marker (runs). It also checks the claim TTL 360/240 and the pure core at 6.9/7 minutes.
  - T13, the lock: one `tryLock(5000)` and a release, with the lock already released when the refresh reads its first sheet. A busy lock, a lock fault, a running render and a held claim each skip and take no claim. While off, the lock is never touched.
  - Every v1.0.0 section T1-T12 passes with the gate off.
  - v1.0.0 run as the delivered file fails T0, T9 and T13.
  - Mutants: 27 killed (18 from v1.0.0, 9 new: lock not taken, never released, fault runs, gate-off path takes the lock).
- **`pytest tests/test_decision_chain_js.py tests/test_por_v100.py tests/test_repository_workflow_audit.py`:** 14 passed, 10 subtests passed.
- **Delivered files:**
  - `26_Decision_Chain.gs`: SHA-256 `86192cf44b236c6c5ce066bd0fa8e83d3f75436e59b9a95f88bdfca4a24492e9` (835 lines).
  - `28_Plan_Of_Record.gs`: SHA-256 `7dff8187f041e1ab3d0222ef20122dae49cc5bfc8e566fdb318befe17efd6eeb` (1,295 lines).
  - Every added line is ASCII and ES5.

## S3 - Operator steps (the repo files are mirrors; nothing changes until the paste)
1. Merge.
2. Paste `apps_script/26_Decision_Chain.gs` and `apps_script/28_Plan_Of_Record.gs` from main, then Save.
3. `zz_Property_Tool`: run `tfbSetupRenderPriority` (zz_Property_Tool 2.1, PR #914). Alternatively, set `TFB_PT_SET_KEY = 'TFB_RENDER_PRIORITY'`, `TFB_PT_SET_VALUE = 'on'` and run `tfbSetAnyProperty`.
4. Run `tfbDecisionChainStatus`; expect `v1.0.5 … priority=on`.
5. **Watch:**
   - `_Run_Log` shows `NOT_FINISHED` rows when a render is stopped;
   - Plan of Record ticks return `skipped` while a render runs;
   - the execution list shows no `Timed Out` chain tick beside a `porRefreshTrigger`.
6. **Rollback:** delete `TFB_RENDER_PRIORITY`, or Project History -> 26 1.0.4 / 28 1.0.0.

## S4 - Known limits
- **v2 build still overlaps:** the hourly `buildDecisionPagesV2` (329 s) does not yield yet, so a render can still share minutes with it. The next step is the same yield in `27_Decision_Pages_v2.gs`, which needs its stacked harness.
- **Slow backend calls remain:** a render can still be stopped without any neighbour. The 01:34:49 call waited 5 min 25 s on the backend. The 30-minute handshake (#917) and the chain's 3-attempt retry absorb one such stop. The lasting fix is a lighter render: send fewer pool rows, or split the read/POST from the write.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
