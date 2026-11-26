# TFB Commit Sheet — 24_Sync_Dispatch.gs 1.2.0 (a fault at GitHub is tried again)

Date: 2026-11-26 (Thursday) · Lane: Apps Script dispatcher (daily sync, shadow board, shadow scorer) · Gate: none changed · No Render deploy · No order is placed, changed or cancelled

## Why
`_Run_Log`, 25 Nov 2026 18:47:58 Riyadh, `tfbDispatchShadowScorer`: the scorer's `workflow_dispatch` got **HTTP 500** from GitHub. The dispatcher sent it once. No run started. The scorer's min gap is 1,200 minutes, so no second trigger comes that day. The evidence day of 25 Nov was recorded only because the workflow was dispatched by hand at 23:18Z (run 45720358524, `[S1-GATE v1.9.7] PASS day 37/28`). One answer from GitHub decided a whole evidence day.

## Change
`apps_script/24_Sync_Dispatch.gs` 1.1.1 → **1.2.0** · `daily_sync.yml` 1.0.15 → 1.0.16 · `shadow_scorer.yml` 1.0.2 → 1.0.3 · `shadow_board.yml`

| | |
|---|---|
| `dispatch_id` | Every dispatch carries one more input: a name made for that one dispatch, `gas-<lane>-<time>-<random>` (`tfbSdNewDispatchId_`, pure). The three workflows take it (optional, default empty) and print it in their run name: `run-name: ${{ inputs.dispatch_id && format('{0} [{1}]', github.workflow, inputs.dispatch_id) || '' }}`. A run started without it — a push, a pull request, a schedule, a manual run — keeps GitHub's own name (an empty `run-name` is ignored). No job, step or env reads the input. |
| `tfbSdSendDispatch_` | Every dispatch (daily sync, shadow board, shadow scorer) goes through it. A final answer returns at once. After a fault — transport error, HTTP 5xx or 429 — it waits (5 s, then 20 s), reads the workflow's runs list, and sends again **only when no run carries this dispatch's id**. Three sends at most; after the last one the runs list is read once more. |
| `tfbSdRunStarted_`, `tfbSdRunHasId_` (pure), `tfbSdBuildRunsRequest_` | `GET …/actions/workflows/<file>/runs?event=workflow_dispatch&per_page=10` with the same token; a run counts only when its name carries the id. An unreadable list counts as "not found", so the send is tried again. |
| `tfbSdRetryable_` (pure) | −1, 429 and 5xx are tried again. A 4xx is ours (token, inputs, path) and is final. |
| `tfbSdIdRefused_` (pure) | One exception: a workflow that does not take `dispatch_id` yet answers 422 `Unexpected inputs`. The dispatch is then sent once without the id, as 1.1.1 sent it, and the row says so. So the order of rollout cannot lose a dispatch: the file may be pasted before or after the workflows reach the ref. Without an id a fault is not sent again — there is nothing to tell its run by. |
| Result and log | A clean first try returns `OK:204` with the message it always had. A retried accept returns `OK:204` and its row ends ` after 2 tries (500, 204)`. A run found by its id after a fault returns `OK:run_started`, sets LAST_OK (so the min gap holds the next trigger) and says `answered HTTP 500 but its run started (found by its dispatch id in the runs list); not sent again`. A failure lists its tries. `details.dispatch_id` is always written; `details.attempts` only when there was more than one. |

Why read the runs list before sending again: a 5xx or a timeout can still have started the run. A second full sync queues behind the first on the production write lease, spends provider quota and supersedes the first one's release; two scorer runs at once have no lease between them.

Why by id and not by time (Codex P2 on #1087): the first cut took any `workflow_dispatch` run created since the first try. A manual `health_only` sync, a dry-run board or a preview started in the same seconds would then have been taken for the production dispatch — reported as accepted, never sent again, and the min gap holding the next trigger. Runs of one workflow are not interchangeable; the runs list does not show a run's inputs; the id in the run name is the only thing that says whose run it is.

Worst case: three sends, three readings, 30 s of waiting — well inside the trigger's six minutes.

Unchanged: the guards (kill switches, token, min gap, fresh board, before-slot, write hold), the schedule, the other inputs, the probe, the status line.

## Tests
| File | What it pins |
|---|---|
| `tests/test_gas_dispatch_retry_v120.js` (45) | the real `.gs` in a vm, GitHub answering from a script: the 25 Nov case (500 → the id not in the runs list → sent again with the same id → 204); a run found by its id is not sent twice and holds the min gap; three faults → FAILED with the tries listed; 401 / 403 / 404 / an unrelated 422 final with one send; 429 and a transport error tried again; an unreadable runs list does not stop the next send; a run seen on the final reading counts; the daily sync the same way with its own inputs, id and LAST_OK; **a concurrent manual sync, a dry-run board, a scheduled run and a nearly equal id are not adopted**; two dispatches at the same instant carry different ids; a workflow that refuses the id gets the plain dispatch once; the guards send nothing; the token never reaches a row; ES5 only; the three workflow files take the id and print it |
| `tests/test_gas_sync_dispatch_p170.js` (44), `tests/test_gas_s1_dispatch_p170b.js` (54) | the earlier harnesses, re-pinned on 1.2.0 — every earlier behaviour unchanged (the pinned daily-sync payload is compared without the id, and the id's shape is checked) |
| `tests/test_gas_dispatch_v120.py` (5) | runs the three harnesses under pytest (the two earlier ones had no collector in CI); pins that both lanes go through the one sender and that a run is matched by id, never by time; the three workflows declare the input, carry the run name, and read the id nowhere else |
| `tfbSyncDispatchSelfTest` | +15 pure checks (which answers are tried again, the id's shape, the runs-list reading, the refused id, the payloads) |

## Owner action after merge
Paste `apps_script/24_Sync_Dispatch.gs` into the Apps Script project (or ask Claude to do it in the connected browser), then run `tfbSyncDispatchSelfTest` — it must return `sync dispatch core: ok | s1 lane core: ok` — and `tfbSyncDispatchStatus`, which must start `SYNC-DISPATCH v1.2.0`. No trigger, property or token changes. The token already reads the workflow record (the probe); the runs list needs the same Actions read permission. Merge first, paste second is the tidy order, but either order is safe (see `tfbSdIdRefused_`).

## Rollback
Apps Script → Project History → 1.1.1. The workflows take a dispatch with or without the id, so they need no rollback.

Not investment advice.
