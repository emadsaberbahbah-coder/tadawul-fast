# TFB Commit Sheet — apps_script/24_Sync_Dispatch.gs v1.2.0 [P-170c ARMING PRECONDITIONS]

Date: 2026-10-15 (Thursday) · Lane: GAS + GitHub (yml merges FIRST, then the Apps Script paste) · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `apps_script/24_Sync_Dispatch.gs` v1.1.0 at `main` `11ef134` — sha `8a9808a77c5543f2…` (1,064 lines, 45 functions) = the 09-30 delivery byte-for-byte |
| Live state | dispatcher never fired: `TFB_GH_DISPATCH_TOKEN` was never created, every trigger since 2026-09-30 logged `dispatch failed: no_token` |
| Companion | PR #781 (P-170b): daily_sync cron `"17 12,20"`, shadow_board crons removed, shadow_scorer non-cancelling lease. Merge it before arming. |

## S2 — Root
1. **Silent failure.** The trigger handlers *returned* `FAILED:no_token` (and would return `FAILED:401` on an expired PAT, `FAILED:422` on a bad input). A returned string is a successful trigger execution, so Apps Script sent no failure email: 15 days of outage were visible only in `_Run_Log`.
2. **The dispatched morning sync was not the scheduled run it replaces.** `daily_sync.yml` adds `MY_PORTFOLIO` (step "Resolve Configuration") and runs the Track Performance record only for `schedule`, or for a dispatch that opts in. A `{run_mode}`-only dispatch skipped both: My_Portfolio would keep the previous 20:17Z prices into the 08:10 cockpit, and the day's only on-time run would record nothing.

## S3 — Change
| # | Site | Edit |
|---|---|---|
| E1 | header | title, v1.2.0 WHY block, `TFB_SYNC_DISPATCH_VERSION = '1.2.0'`; COMPANION / COEXISTENCE marked done by PR #781 |
| E2 | `TFB_SYNC_DISPATCH_` | `INCLUDE_PORTFOLIO: 'true'`, `RECORD_TRACKER: 'true'` |
| E3 | `tfbSdDispatchInputs_` (new) + `tfbSdBuildDispatchRequest_` | payload `{ref:"main", inputs:{run_mode:"full_sync", include_portfolio:"true", record_tracker:"true"}}`; the OK row prints the inputs |
| E4 | `tfbSdRaiseIfFailed_` (new) | the three **trigger** handlers (`tfbDispatchDailySync`, `tfbDispatchShadowBoard`, `tfbDispatchShadowScorer`) throw on any `FAILED…` result after the ERROR row is written; `OK` and `SKIPPED` return as before. `…Now`, probes, installers and status still return strings. Error text carries no token. `no_token` / 4xx say "nothing was dispatched"; `-1` (fetch exception, e.g. a timeout) and 5xx say "dispatch NOT CONFIRMED; check the Actions runs before re-dispatching", because GitHub may already have queued the run. |
| E5 | selftest T4/T4b | payload inputs; raise on FAILED, no raise on OK/SKIPPED |
| Y1 | `daily_sync.yml` input `record_tracker` (boolean, default false) | record-only tracker; `--audit` stays behind `run_tracker == "true"` |
| Y2 | `daily_sync.yml` tracker gate | `… \|\| github.event.inputs.record_tracker == 'true'` |

## S4 — Audits
| Check | Result |
|---|---|
| Delivered SHA-256 | `744ef145cd737d82…` (1,130 lines, LF, pure ASCII) |
| `node --check` | PASS; ES5 scan: 0 `let`/`const`/arrow/class (backticks only inside comments, as before) |
| Functions | 45 → 47 (+`tfbSdDispatchInputs_`, `tfbSdRaiseIfFailed_`; 0 removed) |
| Embedded selftest | `sync dispatch core: ok \| s1 lane core: ok` |
| `tests/test_gas_sync_dispatch_p170.js` | 45/45 PASS (T2 payload inputs; T5/T6 handlers throw after the ERROR row, 401 = "nothing was dispatched", fetch exception = "NOT CONFIRMED", `…Now` still returns, no token in the error text) |
| `tests/test_gas_s1_dispatch_p170b.js` | 54/54 PASS (S5/S6 board and scorer handlers throw; S9 payload re-pinned to v1.2.0) |
| `tests/test_gas_dispatch_inputs_contract_p170c.py` (new) | every key the dispatcher sends is a declared input of its target workflow (a missing one is HTTP 422); fails on the v1.1.0 + pre-change yml pair |
| `tests/test_tracker_record_only_workflows.py` | +5 cases: `record_tracker=true` runs the record with no `--audit`; absent/false still skips a manual dispatch |

## S5 — Deploy order (operator)
1. Merge PR #781 and this PR (the yml must declare `record_tracker` on `main` **before** the paste, or every sync dispatch answers 422).
2. Paste v1.2.0 over `24_Sync_Dispatch` → `tfbSyncDispatchSelfTest` → `sync dispatch core: ok | s1 lane core: ok`.
3. Create the PAT (fine-grained, repo `tadawul-fast`, Actions read/write + Metadata read, 90-day expiry; calendar the rotation) → `TFB_GH_DISPATCH_TOKEN`.
4. `tfbSyncDispatchProbe` → `OK:200:active`; `tfbS1DispatchProbe` → `shadow_board=OK:200:active | shadow_scorer=OK:200:active`.
5. `tfbInstallSyncDispatchTriggers` + `tfbInstallS1DispatchTriggers` → status `triggers=1 / 2 / 1`.
6. **After** the installers: Apps Script editor → Triggers → edit each of the four dispatch triggers → failure notification **Notify me immediately** (a script-created trigger defaults to a daily digest). Every later run of an installer deletes and recreates the triggers, so redo this step after any re-install. Do not change the trigger times while there: the scorer must stay after its 15:20Z before-slot guard.
7. Read-backs: a `tfbDispatchShadowBoard OK` row and a board `as of <D> 17:1x` before 15:20Z; next morning a `tfbDispatchDailySync OK` row whose message shows the three inputs, a daily_sync run that refreshed My_Portfolio, and a Track Performance step that ran **without** `--audit`.
Steps 2–6 in one sitting, 06:00–11:30Z (clear of every slot).

Owner decision (not changed here): the sync lane's `gm_fresh` guard (`TFB_SYNC_DISPATCH_FRESH_SKIP_MIN`, default 90) skips the 06:15 dispatch when Global_Markets was stamped in the last 90 min, e.g. by a 20:17Z cron that fired 4–5 h late. With the 04:17Z cron gone, that skip leaves the day without a morning run until the 12:17Z cron. Setting the property to `0` disables only that guard (the 120-min `min_gap` and the non-cancelling write lease still prevent overlaps), at the cost of a possible extra full sync on such days.

Kill: `TFB_SYNC_DISPATCH_DISABLED=1` (all lanes), `TFB_SB_DISPATCH_DISABLED=1` / `TFB_S1_DISPATCH_DISABLED=1` (per lane); rollback = paste v1.1.0 back (the yml input is inert when unsent).
