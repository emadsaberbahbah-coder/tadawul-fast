# Commit Sheet: three pages follow the hourly Exit Plan (news_board.yml 1.1.0, trial_labels.yml 1.1.0, pre_open_check.yml v1.2.0, ci.yml 1.1.91), 2026-11-21

**Scope.** Owner, 21 Nov 2026 02:20 Riyadh (in Arabic: "review the updates for 6 hours and see the updates they need, based on the updates being made"). The watch measured when GitHub actually starts each scheduled workflow. Three pages that depend on a time of day still waited for GitHub's schedule:
- the Pre-Open Check;
- the News Board;
- the Trial Labels.

Each now also runs after the Exit Plan workflow, which the workbook dispatches every hour (`apps_script/29_Page_Refresh_Dispatch.gs`). It runs only in the UTC hours of a repository Variable. This is the rule News & Macro Gate 1.1.0 uses since 16 Nov. The scripts are unchanged, and no score, gate, ticket or broker action changes.

## Why (measured 21 Nov, GitHub Actions API, read-only)
| Workflow | Due | What GitHub did | Effect |
|---|---|---|---|
| Pre-Open Check | 13:00 and 14:00 UTC Mon-Fri (16:00 / 17:00 Riyadh). The script acts only inside [open - 75 min, open + 15 min]. | Every scheduled run since 9 Nov (10 runs) started between 20:03 and 22:54 UTC, after the 21:00 UTC close. The 20 Nov run printed "outside the pre-open window (16:15 .. 17:45 Riyadh) ... nothing to do". No dispatch since 18 Nov. | The WAIT / GO guard built after the 5 Nov open loss has never been written by its schedule. |
| News Board | 04:55 and 13:55 UTC (07:55 / 16:55 Riyadh) | All 3 scheduled runs since it arrived (19 Nov) were late: 8 h 00, 8 h 49 and 7 h 43. | The morning page reached the owner at 16:44 Riyadh. |
| Trial Labels | 05:05, 09:05 and 14:05 UTC | Its first slot (20 Nov 14:05 UTC) started at 21:45 UTC, 7 h 40 late. | The C-2 and news labels in each stock's row would stand still for most of the day. |

The whole repository shows the same pattern. Since 18 Nov, 22 of the 27 workflows with one to four slots a day started 4 to 9.5 hours late (median). The hourly lanes had most of their slots dropped: Daily Plan 6 scheduled runs of 51 slots, Exit Plan 9 of 54. The pages that matter most are already started by Apps Script or chained to it:
- Daily Plan, Exit Plan and Consistency Check: dozens of dispatches each since 18 Nov;
- the sync;
- Holding Read and News & Macro Gate: chained to the Exit Plan;
- Today Action Plan: chained to the Daily Plan and the Exit Plan.

GitHub community reports describe the scheduler delaying or dropping cron runs by 3 to 14 hours since late August 2026, while `workflow_dispatch` starts at once.

## Changes
| File | Change | Default chain hours (UTC) | Riyadh |
|---|---|---|---|
| `.github/workflows/pre_open_check.yml` v1.2.0 | `workflow_run` after "Exit Plan". A first step decides whether a chained run is due: weekdays (Mon-Fri UTC) and an hour in `TFB_PREOPEN_CHAIN_HOURS`. Every other step carries `if: steps.due.outputs.run == 'true'`. | `12 13` | about 15:40 and 16:40 |
| `.github/workflows/news_board.yml` 1.1.0 | The same gate with `TFB_NEWS_BOARD_CHAIN_HOURS`. | `4 13` | about 07:35 (after the 07:30 e-mail run) and 16:40 |
| `.github/workflows/trial_labels.yml` 1.1.0 | The same gate with `TFB_TRIAL_LABELS_CHAIN_HOURS`. | `5 9 14` | about 08:40, 12:40 and 17:40 |
| `.github/workflows/ci.yml` 1.1.91 | `tests/test_page_chains.py` added to the blocking lean lane. | - | - |
| `tests/test_page_chains.py` (new), `tests/test_news_board.py` N7, `tests/test_trial_labels.py` L7 | C1 wiring, C2 the gate run in bash with a fixed clock, C3 the pre-open window in both DST regimes. N7 and L7 pin the new trigger. | - | - |

Rules kept in every file:
- A chained run writes, like a scheduled one. Only a dispatch can ask for a dry run.
- Schedule and dispatch are never held back by the gate.
- The crons stay as the backstop.
- `off` (or no number) in a Variable means no chained runs, cron only. Hours are separated by spaces; `04` reads as 4, not octal.
- The Pre-Open Check script's own window still decides:
  - In winter (open 14:30 UTC), the 13:4x run lands inside the window about 45 minutes before the open, and the 12:4x run prints "nothing to do".
  - In summer (open 13:30 UTC), the 12:4x run lands before the open. The 13:40 - 13:45 run reads inside the window's 15 minutes after the open, and later runs read outside it.

## Pre-merge checks
- **Tests.** test_page_chains 17/17. With test_news_board, test_trial_labels and test_pre_open_check: 59 passed, 1 skipped. The skip is the `exchange_calendars` import, the same on main. test_ci_required_verdict, test_workflow_audit_repairs and the three whole-tree workflow suites: 107 passed, 13 skipped (environment), the same as main.
- **Mutants: 8 / 8 killed.**
  1. weekends allowed for the pre-open chain;
  2. the news hours moved;
  3. the labels step left ungated;
  4. every chained run due;
  5. a wrong workflow name;
  6. pre-open hours that miss summer;
  7. a cron removed;
  8. the hour read without the number check: "off" became hour 0, and "08" was read as octal.
- **Lint and audit.** The workflow audit reports errors=0, warnings=0, and ruff (E9, F63, F7, F82) is clean.

## Post-merge (lead)
- **Variables.** No variable is needed: the defaults apply. Rollback per page: set the Variable to `off`.
- **First chained runs.** News Board about 07:35 Riyadh and Trial Labels about 08:40 Riyadh on the next day. The Pre-Open Check runs on the next US weekday, about 16:40 Riyadh in winter.
- **Read-back.** Read each one's run log for "Chained run at UTC hour H (hours: ...): run=true", and the tab's build time.
- **Not changed here (follow-ups).**
  - **Intraday Quote Refresh.** It is set for Tadawul hours and still waits on the owner's quote-age decision (16 Nov).
  - **TFB Performance Clock.** Its schedule behaves unlike a dispatch: the scheduled maturity audit runs on schedule only. Its 15:47 UTC slot now lands after Riyadh midnight, so its date keys need a check before any change.
  - **The archives and the backup.** Their daily cadence tolerates a late start.

Sources: GitHub Actions run history of this repository (API, read-only); GitHub community discussion 201738 (scheduled workflows delayed 8-14 hours; the `workflow_dispatch` workaround).

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
