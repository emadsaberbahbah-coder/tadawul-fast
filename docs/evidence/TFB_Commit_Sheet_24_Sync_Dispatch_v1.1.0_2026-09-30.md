# TFB Commit Sheet — apps_script/24_Sync_Dispatch.gs v1.1.0 [P-170b S-1 LANE DISPATCH]

Date: 2026-09-30 (Wednesday) · Lane: GAS (Apps Script editor paste first, repo mirror second) · Build #2 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `apps_script/24_Sync_Dispatch.gs` v1.0.0 at HEAD `6bcf7df` — sha `566d66cd63e36867…` (593 lines, LF, ASCII, 27 functions) = the 09-29 delivery byte-for-byte |
| Live state | pasted in the production project (proof: `_Run_Log` 2026-09-29 11:54:23 `tfbDispatchDailySync … FAILED dispatch failed: no_token`, details `{"reason":"no_token", gm_stamp_age_min:447, …}`); PAT absent; no triggers installed (no rows this morning) |
| Workflow inputs verified at HEAD | `daily_sync.yml`: `run_mode` (choice, default full_sync) · `shadow_board.yml`: `dry_run` (choice, **default "true"**) · `shadow_scorer.yml`: `dry_run` (default "false") + `rollback_drill_passed` |

## S2 — Root
The S-1 lane runs on the same GitHub `schedule` mechanism as the sync and slips the same way: the last six shadow-board crons fired 4.2–6.5 h late; the scorer cron fired at 20:00Z on 09-29. A scorer past 21:00Z crosses midnight Riyadh and, under v1.8.0, steals the next day's key (P-176 — fixed in run_shadow_scorer v1.9.0, build #1). The dispatcher already owns the cure for the sync lane; this release extends it to both S-1 workflows so the evidence lane leaves `schedule` entirely. Coupling hazard identified and closed: an Apps Script trigger fires inside a ±15-minute window, so a scorer dispatched "at 18:20 Riyadh" could start at 15:05Z — before the 15:20Z slot boundary — and be keyed to the previous evidence day by v1.9.0 (refused as a duplicate). Hence the scorer lane defaults to **18:40 Riyadh** (window 15:25–15:55Z) and carries a **before-slot guard** that the manual variant cannot bypass.

## S3 — Change (3 anchored edits + 1 appended section; every v1.0.0 function carried verbatim)
| # | Site | Edit |
|---|---|---|
| E1 | header | title, v1.1.0 WHY block above the v1.0.0 block, `TFB_SYNC_DISPATCH_VERSION = '1.1.0'` |
| E2 | `tfbSdLog_` | optional trailing `page` argument (blank → `daily_sync.yml` as before) so lane rows carry their own workflow in the Page column; all v1.0.0 call sites unchanged |
| E3 | `tfbSyncDispatchSelfTest` | appends ` \| ` + the new lane battery (`s1 lane core: ok`) to the verdict string |
| E4 | EOF | **S-1 lane section** — `TFB_S1_DISPATCH_` (two lanes, per-lane Script Properties), pure helpers `tfbS1ParseSlot_`, `tfbS1BeforeSlot_`, `tfbS1ParseBoardAsOfMs_`, `tfbS1DecideLane_`, `tfbS1BuildDispatchRequest_`; GAS-facing `tfbS1LaneConfig_`, `tfbS1ReadBoardAsOfMs_`, `tfbS1Dispatch_`; handlers `tfbDispatchShadowBoard` / `tfbDispatchShadowScorer` (+ `…Now` manual variants); `tfbS1DispatchProbe`; `tfbS1TriggersFor_`, `tfbInstallS1DispatchTriggers`, `tfbRemoveS1DispatchTriggers`; `tfbS1DispatchStatus`; `tfbS1DispatchSelfTest_` |

| Lane | Default trigger (Asia/Riyadh) | Payload | Guards (in order) | Properties |
|---|---|---|---|---|
| shadow_board | 08:10 and 17:10 | `{ref:"main", inputs:{dry_run:"false"}}` — explicit false because the yml default is **true** (the 09-14 #118 lesson) | global kill · lane kill · token · min gap 60 min · `Shadow_Board` "as of" younger than 60 min → `board_fresh` (tab missing → fail-open) | `TFB_SB_DISPATCH_HOURS`=8,17 · `_MINUTE`=10 · `_DISABLED` · `_MIN_GAP_MIN` · `_FRESH_SKIP_MIN` · `_LAST_OK_MS` |
| shadow_scorer | 18:40 | `{ref:"main", inputs:{dry_run:"false"}}` | global kill · lane kill · token · **before_slot** (UTC time-of-day < `TFB_S1_DISPATCH_SLOT_UTC` 15:20 → SKIPPED, manual too) · min gap 1,200 min (once per evidence day) | `TFB_S1_DISPATCH_HOURS`=18 · `_MINUTE`=40 · `_DISABLED` · `_MIN_GAP_MIN` · `_LAST_OK_MS` · `TFB_S1_DISPATCH_SLOT_UTC` |
| daily_sync (v1.0.0) | 06:15 | `{ref:"main", inputs:{run_mode:"full_sync"}}` — **byte-untouched** | unchanged | unchanged |

Secrets: one PAT for all three lanes (`TFB_GH_DISPATCH_TOKEN`; fine-grained, repo `tadawul-fast`, Actions read/write + Metadata read). The token is still never written anywhere (redaction inherited; harness asserts no leak on every lane).

Coexistence with the yml crons (keep them this week as fallback): a scheduled and a dispatched board both write the same atomic rectangle (harmless); a scorer that runs second is `DUPLICATE_REFUSED` visibly under v1.9.0. Remove the two S-1 crons (and daily_sync's 04:17Z slot) at the Saturday 10-03 sitting after one clean week of dispatch rows.

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `8a9808a77c5543f2…` (1,064 lines, LF, pure ASCII, 0 smart quotes) |
| `node --check` | PASS; ES5 scan: 0 `let`/`const`/arrow/template/class |
| Functions | 27 → 45 (+18 listed above, **0 removed**); every v1.0.0 function body verbatim except the two named edits (`tfbSdLog_` page arg, selftest hook) |
| Embedded selftest | `tfbSyncDispatchSelfTest()` → `sync dispatch core: ok \| s1 lane core: ok` (v1.0.0 battery 30 checks + 26 lane checks) |
| Harness `tests/test_gas_s1_dispatch_p170b.js` (REAL .gs in a vm context; stubs only for Apps Script services + an injectable clock) | **53/53 PASS ×3, digest `bc3ec6fbce04` ×3** |
| S1 | both batteries ok; version; the whole v1.0.0 surface still callable |
| S2 | board dispatch at 08:10 Riyadh on a 09-29 board: POST `…/shadow_board.yml/dispatches`, payload `dry_run:"false"` and **no `run_mode`**, HTTP 204, row Page `shadow_board.yml`, `board_asof_age_min` 587, SB LAST_OK set / sync LAST_OK untouched; second call → `min_gap`; `…Now` → forced |
| S3 | 20-min-old board → `board_fresh`; `…Now` bypasses; missing tab fails open; custom fresh window honoured |
| S4 | scorer at 15:05Z → `before_slot` (also for `…Now`, row carries `slot_utc`); at 15:40Z → 204 with `dry_run:"false"`; +6 h → `min_gap`; next day before/after the slot; custom slot 16:00 blocks 15:40Z |
| S5 | no token → FAILED/ERROR rows per lane, probe both FAILED, zero fetches; global kill; per-lane kills independent |
| S6 | GitHub 422 → FAILED:422, no LAST_OK, token redacted in the echoed body; fetch throw → FAILED:-1 |
| S7 | probe GETs both workflows, rows name hours/minutes |
| S8 | install: `shadow_board` 08:10/17:10 + `shadow_scorer` 18:40 in Asia/Riyadh, stale own trigger removed, `tfbDispatchDailySync` and foreign triggers untouched; idempotent re-install; remove = 3; custom hour/minute props |
| S9 | v1.0.0 daily_sync path: payload `{ref, inputs:{run_mode}}` byte-identical, Page `daily_sync.yml`, status line renders |
| Existing harness `tests/test_gas_sync_dispatch_p170.js` | 43/43 ×3 digest `4643b48b0103` after re-pinning three literals (version 1.1.0; selftest verdict prefix) |

## S5 — Delivery
| File | Destination |
|---|---|
| `24_Sync_Dispatch.gs` | **Apps Script editor — replace the whole `24_Sync_Dispatch` file** (contains v1.0.0 verbatim; one paste) |
| `apps_script/24_Sync_Dispatch.gs` | repo mirror (same bytes) |
| `tests/test_gas_s1_dispatch_p170b.js` | repo `tests/` (new) |
| `tests/test_gas_sync_dispatch_p170.js` | repo `tests/` (3 pin literals; otherwise byte-identical) |
| `docs/evidence/TFB_Commit_Sheet_24_Sync_Dispatch_v1.1.0_2026-09-30.md` | repo `docs/evidence/` |

## S6 — Deploy order (GAS lane; operator)
1. Paste v1.1.0 over `24_Sync_Dispatch` → run `tfbSyncDispatchSelfTest` → expect `sync dispatch core: ok | s1 lane core: ok` (paste-is-live proof).
2. Create the fine-grained PAT (repo `tadawul-fast`; Actions: Read and write; Metadata: Read-only; 90-day expiry) → Script Property `TFB_GH_DISPATCH_TOKEN`.
3. `tfbSyncDispatchProbe` → `OK:200:active`; `tfbS1DispatchProbe` → `shadow_board=OK:200:active | shadow_scorer=OK:200:active` (three `_Run_Log` rows, token never shown).
4. `tfbInstallSyncDispatchTriggers` (06:15) and `tfbInstallS1DispatchTriggers` (08:10 / 17:10 / 18:40) → `tfbSyncDispatchStatus` + `tfbS1DispatchStatus` show `triggers=1 / 2 / 1`.
5. Read-backs: tomorrow 06:15–06:30 a `tfbDispatchDailySync OK` row and a daily_sync run whose `_Status` stamps land before 08:08; **17:10 today** (if installed before) a `tfbDispatchShadowBoard OK` row + a board `as of 2026-09-30 17:1x`; **18:40 today** a `tfbDispatchShadowScorer OK` row + a scorer `_Run_Log` row with `{"version":"1.9.0", "day_key":{"mode":"slot","key":"2026-09-30"}}` (needs build #1 committed first). If the cron also fires, expect one visible `DUPLICATE_REFUSED` — not an error.
6. Companion yml edits (Saturday sitting, after one clean week): daily_sync cron `"17 4,12,20"` → `"17 12,20"`; remove the `schedule:` blocks of shadow_board.yml and shadow_scorer.yml (keep `workflow_dispatch`).
7. Kill: `TFB_SYNC_DISPATCH_DISABLED=1` (all), `TFB_SB_DISPATCH_DISABLED=1` / `TFB_S1_DISPATCH_DISABLED=1` (per lane); rollback = `tfbRemoveS1DispatchTriggers`.
