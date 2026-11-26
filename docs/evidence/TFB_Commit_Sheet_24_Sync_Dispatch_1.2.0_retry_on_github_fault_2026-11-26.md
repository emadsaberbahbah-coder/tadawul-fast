# TFB Commit Sheet — 24_Sync_Dispatch.gs 1.2.0 (a fault at GitHub is tried again)

Date: 2026-11-26 (Thursday) · Lane: Apps Script dispatcher (daily sync, shadow board, shadow scorer) · Gate: none changed · No Render deploy · No order is placed, changed or cancelled

## Why
`_Run_Log`, 25 Nov 2026 18:47:58 Riyadh, `tfbDispatchShadowScorer`: the scorer's `workflow_dispatch` got **HTTP 500** from GitHub. The dispatcher sent it once. No run started. The scorer's min gap is 1,200 minutes, so no second trigger comes that day. The evidence day of 25 Nov was recorded only because the workflow was dispatched by hand at 23:18Z (run 45720358524, `[S1-GATE v1.9.7] PASS day 37/28`). One answer from GitHub decided a whole evidence day.

## Change
`apps_script/24_Sync_Dispatch.gs` 1.1.1 → **1.2.0**

| | |
|---|---|
| `tfbSdSendDispatch_` | Every dispatch (daily sync, shadow board, shadow scorer) goes through it. A final answer returns at once. After a fault — transport error, HTTP 5xx or 429 — it waits (5 s, then 20 s), reads the workflow's runs list, and sends again **only when no `workflow_dispatch` run has started since the first try**. Three sends at most; after the last one the runs list is read once more. |
| `tfbSdRunStarted_`, `tfbSdRunStartedSince_` (pure), `tfbSdBuildRunsRequest_` | `GET …/actions/workflows/<file>/runs?event=workflow_dispatch&per_page=5` with the same token; a run created at or after the first try (less 10 s of clock tolerance) counts. An unreadable list counts as "none", so the send is tried again. |
| `tfbSdRetryable_` (pure) | −1, 429 and 5xx are tried again. A 4xx is ours (token, inputs, path) and is final. |
| Result and log | A clean first try returns `OK:204` and writes the row it always wrote. A retried accept returns `OK:204` and its row ends ` after 2 tries (500, 204)`. A run found after a fault returns `OK:run_started`, sets LAST_OK (so the min gap holds the next trigger) and says `answered HTTP 500 but the run started (seen in the runs list); not sent again`. A failure lists its tries. `details.attempts` is written only when there was more than one. |

Why read the runs list before sending again: a 5xx or a timeout can still have started the run. A second full sync queues behind the first on the production write lease and spends provider quota for nothing; a second scorer run is refused as a duplicate day. Reading first costs one GET and removes both.

Worst case: three sends, three readings, 30 s of waiting — well inside the trigger's six minutes.

Unchanged: the guards (kill switches, token, min gap, fresh board, before-slot, write hold), the schedule, the payloads, the probe, the status line.

## Tests
| File | What it pins |
|---|---|
| `tests/test_gas_dispatch_retry_v120.js` (34) | the real `.gs` in a vm, GitHub answering from a script: the 25 Nov case (500 → no run → sent again → 204); a run found after the fault is not sent twice and holds the min gap; three faults → FAILED with the tries listed; 422 / 401 / 404 / 403 final with one send; 429 and a transport error tried again; an unreadable runs list does not stop the next send; a run seen on the final reading counts; the daily sync the same way with its own payload and LAST_OK; an earlier run and a scheduled run are not adopted, one inside the clock tolerance is; a clean first try writes the old row; kill switch and before-slot send nothing; the token never reaches a row; ES5 only |
| `tests/test_gas_sync_dispatch_p170.js` (44), `tests/test_gas_s1_dispatch_p170b.js` (53) | the earlier harnesses, re-pinned on 1.2.0 — every earlier behaviour unchanged |
| `tests/test_gas_dispatch_v120.py` (4) | runs the three harnesses under pytest (the two earlier ones had no collector in CI), and pins that both lanes go through the one sender |
| `tfbSyncDispatchSelfTest` | +11 pure checks (which answers are tried again, the runs-list reading, the tries text) |

## Owner action after merge
Paste `apps_script/24_Sync_Dispatch.gs` into the Apps Script project (or ask Claude to do it in the connected browser), then run `tfbSyncDispatchSelfTest` — it must return `sync dispatch core: ok | s1 lane core: ok` — and `tfbSyncDispatchStatus`, which must start `SYNC-DISPATCH v1.2.0`. No trigger, property or token changes. The token already reads the workflow record (the probe); the runs list needs the same Actions read permission.

## Rollback
Apps Script → Project History → 1.1.1.

Not investment advice.
