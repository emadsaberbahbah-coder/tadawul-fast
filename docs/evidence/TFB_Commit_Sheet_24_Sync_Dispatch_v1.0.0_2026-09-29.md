# TFB Commit Sheet — 24_Sync_Dispatch.gs v1.0.0 [P-170 DISPATCH-FROM-GAS]

Date: 2026-09-29 (Tuesday) · Lane: GAS (Apps Script editor paste; repo mirror under `apps_script/`) · Build #1 of the day · Protocol: One-Pass (new file — no base to pin)

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Repo HEAD at build | `045d5f9a4ac8513700757517d77bea99de3eceae` (#617, 2026-09-28 12:38 Riyadh) — zero drift since the 09-28 verification |
| Base file | none — NEW file; no existing function, trigger or sheet cell is touched |
| Related live facts (pinned at HEAD) | `.github/workflows/daily_sync.yml` sha `67a0a4e82313…` (1,720 lines): cron `"17 4,12,20 * * *"`; `workflow_dispatch` inputs `run_mode` (required, default `full_sync`), `backend_url`, `sheet_id`, `single_key`, `enable_debug`; production write lease `tadawul-production-write-<ref>` with `cancel-in-progress: false` for schedule + dispatch (a late schedule run QUEUES behind a dispatched one — it does not cancel) |

## S2 — Root (P-170, measured)
GitHub `schedule` events are delayed under load and never guaranteed. `_Run_Log`/`_Status` evidence: 09-26 the 20Z slot fired 22:59Z and the 04Z slot 09:00Z; after the `:17` minute change (P-163b/P-170 first attempt) the 09-28 slots executed ≈ 10:30Z, ≈ 19:15Z and ≈ 00:25Z (6 h / 7 h / 3.7–4 h late; EODHD-QUOTA leg-end lines 14:04 / 22:46 / 03:27 Riyadh); on 09-29 the 04:17Z slot had not started by 05:51Z and the 08:10 Riyadh cockpit ran on the 04:27 epoch (`feed: EXECUTABLE age 220m`). Program v2's "cockpit only after the 07:17 sync" therefore cannot be met by `schedule` at all. A `workflow_dispatch` REST call starts a run within seconds; an Apps Script time-driven trigger fires inside a 15-minute window of its minute.

## S3 — Change (new file, 593 lines, ES5)
| Function | Role |
|---|---|
| `tfbDispatchDailySync()` | trigger handler → `tfbSdDispatch_(false)`: reads config, guards, POSTs `…/actions/workflows/daily_sync.yml/dispatches` with `{ref, inputs:{run_mode:"full_sync"}}`; HTTP 204 = OK (stores `TFB_SYNC_DISPATCH_LAST_OK_MS`) |
| `tfbDispatchDailySyncNow()` | manual variant — bypasses the min-gap and fresh-page guards only (kill switch, token check and a live write hold still block) |
| `tfbSyncDispatchProbe()` | GET the workflow record — proves token + Actions permission, dispatches nothing; 401/403/404 hints |
| `tfbInstallSyncDispatchTriggers()` / `tfbRemoveSyncDispatchTriggers()` | idempotent: deletes only this handler's triggers, creates one daily trigger per hour in `TFB_SYNC_DISPATCH_HOURS` near `TFB_SYNC_DISPATCH_MINUTE`, timezone Asia/Riyadh; foreign triggers untouched |
| `tfbSyncDispatchStatus()` | one-line status (token shown as `present(n chars)` only) |
| `tfbSyncDispatchSelfTest()` | pure-logic self-test (no network, no write) → `sync dispatch core: ok` |
| pure helpers | `tfbSdParseHours_`, `tfbSdParseIntProp_`, `tfbSdIsOn_`, `tfbSdParseIsoMs_` (Date / ISO with 6-digit fractions / `"2026-09-29 04:27:48+03:00"` / the `_Status` feed-key form), `tfbSdDecide_` (decision table), `tfbSdBuildDispatchRequest_`, `tfbSdBuildProbeRequest_`, `tfbSdRedact_`, `tfbSdClip_` |
| sheet helpers (fail-open) | `tfbSdReadHoldUntilMs_` (`_Sync_Control` "backend sync hold until"), `tfbSdReadGmStampMs_` (`_Status` Global_Markets row + `TFB Feed Global_Markets` key, newest wins), `tfbSdLog_` (one `_Run_Log` row: Timestamp · Level · Action · Page=`daily_sync.yml` · Status · Message · Endpoint · HTTP Code · Duration ms · Details JSON; also `TFB_SYNC_DISPATCH_LAST_EVENT`) |

Guard order: kill switch → no token → live write hold (future, ≤ 16 min ceiling) → min gap (default 120 min) → fresh GM stamp (default 90 min) → dispatch. Secrets: the token lives only in Script Property `TFB_GH_DISPATCH_TOKEN`; every outgoing string (log row, Logger, LAST_EVENT, error echo) passes `tfbSdRedact_`.

Script Properties (operator): `TFB_GH_DISPATCH_TOKEN` (required) · `TFB_GH_DISPATCH_REPO` (default `emadsaberbahbah-coder/tadawul-fast`) · `TFB_GH_DISPATCH_WORKFLOW` (`daily_sync.yml`) · `TFB_GH_DISPATCH_REF` (`main`) · `TFB_SYNC_DISPATCH_HOURS` (`6`; `6,15` for the 2-runs/day plan) · `TFB_SYNC_DISPATCH_MINUTE` (`15`) · `TFB_SYNC_DISPATCH_DISABLED` (kill, `1`) · `TFB_SYNC_DISPATCH_MIN_GAP_MIN` (`120`) · `TFB_SYNC_DISPATCH_FRESH_SKIP_MIN` (`90`).

Deliberate scope cuts: the cockpit trigger time (08:10) is untouched — with a 06:15–06:30 dispatch the GM leg (≈ 61 min) lands ≈ 07:35–07:50; the `daily_sync.yml` cron edit is the operator's hand edit (GitHub lane, one line); no menu entry (01_Menu.gs is paste-blocked) — functions run from the editor; Render cron alternative not built (needs a PAT in Render env + a new service).

Rollback = delete the file's triggers (`tfbRemoveSyncDispatchTriggers`) or set `TFB_SYNC_DISPATCH_DISABLED=1`; deleting the file removes everything (no other file references it).

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `566d66cd63e368671e0a672f829f9fa89a3fe5865cef106fde9fd1231391039d` (593 lines, LF) |
| `node --check` (on a `.js` copy) + vm parse | PASS |
| ES5 | 0 `let`/`const`/`class`, 0 arrow functions, 0 template literals |
| Non-ASCII / smart quotes | 0 / 0 |
| Harness `tests/test_gas_sync_dispatch_p170.js` (REAL file in a vm context; stubs only for PropertiesService / UrlFetchApp / SpreadsheetApp / ScriptApp / Logger) | **43/43 PASS ×3, digest `315440c79a8a` ×3** |
| T1 | embedded self-test → `sync dispatch core: ok`; version 1.0.0 |
| T2 | happy path: stale GM (3 h) + expired hold → one POST to the exact dispatch URL, payload `{ref:"main", inputs:{run_mode:"full_sync"}}`, Bearer header, HTTP 204 → `OK:204`, `_Run_Log` row (Action/Status/HTTP/Page), Details `reason:"ok"` with `gm_stamp_age_min≈180`, `LAST_OK_MS` set, token absent from every row/Logger/LAST_EVENT |
| T3 | immediate second call → `SKIPPED:min_gap` (INFO row, no fetch); `tfbDispatchDailySyncNow` → dispatches with `reason:"forced"` |
| T4 | 10-min-old GM stamp → `SKIPPED:gm_fresh`; live hold → `SKIPPED:sync_in_flight` for both handler and Now; kill switch → `SKIPPED:disabled` for both; `TFB_SYNC_DISPATCH_FRESH_SKIP_MIN=5` lets the 10-min stamp through |
| T5 | no token → `FAILED:no_token`, ERROR row, zero network (handler and probe) |
| T6 | GitHub 401 → `FAILED:401`, `LAST_OK` untouched, token redacted from the echoed body; fetch exception → `FAILED:-1`, no leak |
| T7 | all three sheets missing → guards fail open, dispatch proceeds, Logger fallback |
| T8 | probe 200 `{state:"active"}` → `OK:200:active` via GET; 404 → `FAILED:404:repo/workflow not visible to this token` |
| T9 | install with hours `15,6` minute `20` over 2 own + 1 foreign trigger → `OK:hours=6,15:minute=20:removed=2`, only own triggers deleted, specs `everyDays(1)/atHour/nearMinute(20)/Asia/Riyadh`; status line `triggers=2`, no token; remove → `OK:removed=2`; defaults → `OK:hours=6:minute=15:removed=0` |
| T10 | the three live timestamp forms parse to the exact epoch ms |

## S5 — Delivery
| File | Destination |
|---|---|
| `apps_script/24_Sync_Dispatch.gs` | Apps Script editor — NEW file (Files ＋ → Script → name `24_Sync_Dispatch`), paste the FULL file; also the repo mirror |
| `tests/test_gas_sync_dispatch_p170.js` | repo `tests/` (node) |
| `docs/evidence/TFB_Commit_Sheet_24_Sync_Dispatch_v1.0.0_2026-09-29.md` | repo `docs/evidence/` |

## S6 — Arming (operator; flagged requirements)
1. **Token (approval needed):** GitHub → Settings → Developer settings → Personal access tokens → Fine-grained → repository `tadawul-fast` only → Repository permissions: **Actions: Read and write**, Metadata: Read-only → expiry 90 days. Store as Script Property `TFB_GH_DISPATCH_TOKEN` (Apps Script → Project Settings → Script properties). Never paste it into a sheet cell or a workflow input.
2. Paste the file → run `tfbSyncDispatchSelfTest` (expect `sync dispatch core: ok`) → run `tfbSyncDispatchProbe` (expect `OK:200:active`; a `_Run_Log` row `tfbSyncDispatchProbe OK`).
3. Run `tfbInstallSyncDispatchTriggers` (expect `OK:hours=6:minute=15:removed=0`; Triggers page shows one time-driven trigger for `tfbDispatchDailySync`).
4. **Companion (GitHub lane, hand edit):** `daily_sync.yml` line 24 cron `"17 4,12,20 * * *"` → `"17 12,20 * * *"` (remove the morning slot the dispatcher now owns). Under the 2-runs/day cost plan: `TFB_SYNC_DISPATCH_HOURS=6,15` and delete the `schedule` block.
5. Do NOT run `tfbDispatchDailySyncNow` today unless a run is wanted now (it spends one full run ≈ 35–60 k EODHD calls).

Read-back (Wednesday 09-30 morning): `_Run_Log` row `tfbDispatchDailySync OK … HTTP 204` at ≈ 06:15–06:30 Riyadh; Actions run with `event: workflow_dispatch` starting within minutes; `_Status` GM stamp ≈ 07:30–07:50; cockpit 08:10 on the fresh epoch (`epoch=2026-09-30/feed`, feed age < 60 min). Negative read-back = a `SKIPPED:*` or `FAILED:*` row (reason in Details JSON).
