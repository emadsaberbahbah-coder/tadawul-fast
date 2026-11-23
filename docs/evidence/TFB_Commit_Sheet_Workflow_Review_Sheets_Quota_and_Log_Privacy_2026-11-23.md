# Commit sheet: workflow review - Sheets quota and run-log privacy (23 Nov 2026)

Owner's request (23 Nov 2026 08:42 Riyadh): "review the full project and check specially the workflows and if there
is any failing or other issue please update and try to improve as well".

## What the review found (all non-PR runs, 22 Nov 05:40 UTC to 23 Nov 05:45 UTC, 524 runs, 42 workflows)

Every workflow is enabled, and every scheduled workflow ran on its schedule. Nine workflows had at least one red
run. Six had already turned green again on their own, or were fixed the same evening:
- provider target coverage, workflow repair, options reading;
- today action plan, holding read, evidence board, all red only on the 20:27 UTC quota burst.

Two were still red on their latest run:

| Run | Failure | Cause |
|---|---|---|
| Dashboard Advanced Sync 45105656708 (23 Nov 04:23 UTC) | Supersede released decision feed: exit 3 `layout_fault` | HTTP 429, Sheets read quota. The one 65 s retry met the same answer: the sync's own recovery job kept the shared quota full for more than one minute. |
| Daily Plan 45117259438 (23 Nov 05:21 UTC) | `cannot read the workbook: APIError` | HTTP 429. Three reads 20 s apart, all in the same quota minute. |

Earlier the same pattern hit the page writes:
- 22 Nov 20:27-20:29 UTC, about 15 jobs started within three minutes of one sync.
- Daily Plan and Holding Read ended `publication unconfirmed: APIError`.
- Today Action Plan and Evidence Board failed their reads.

Every job reads through one service account, with a quota of 60 reads per minute.

The review also found a standing-rule breach in public run logs. The owner's rule: "Repo logs are public: no amounts,
quantities, account numbers or holdings' symbols in run logs".
- Today Action Plan printed one `FINAL <symbol> <decision>` line per stock on every run, 18 lines on 23 Nov 05:42 UTC, holdings among them. Its ALIGN, STEP and picks lines also named symbols.
- Holding Read printed one line per holding with its symbol and its last session, 8 lines on the 22 Nov 20:42 UTC run.
- Daily Plan prints `PICK <symbol> qty <n> entry <p> stop <p>` whenever the plan has picks.

## What changes

| File | Version | Change |
|---|---|---|
| scripts/daily_plan.py | 1.10.1 -> 1.10.2 | Reads: a quota answer or a passing 5xx waits 65 s (`sheets_quota.wait_for_transient`). Writes: retried on a quota answer (`sheets_quota.call`, 65 / 65 s); both overwrite their whole range, so repeating them is safe. Log: `plan pick i of n: <status>` and a count of live prices on GitHub Actions. Gate `TFB_PLAN_LOG_DETAIL` = auto, on or off. |
| scripts/holding_read.py | 1.0.0 -> 1.0.1 | Writes retried on a quota answer. Log: `holding i of n: <driver> \| <read>` on GitHub Actions. Gate `TFB_HREAD_LOG_DETAIL`. |
| scripts/exit_plan.py | 1.6.1 -> 1.6.2 | Writes retried on a quota answer. Its reads already waited the quota out (1.3.1). |
| scripts/today_action_plan.py | 1.8.1 -> 1.8.2 | Log on GitHub Actions: `final i of n: <decision> (rule: <rule>)`, `step i of n: <kind>` (no symbol, no number), `align i of n: <level>`, and picks counted by status. Gate `TFB_TAP_LOG_DETAIL`. |
| scripts/release_decision_feed.py | 1.4.2 -> 1.4.3 | The key-block read gets two retries on a quota answer (65 s each), up from one. Any other transient fault keeps one retry; a layout or auth fault gets none. Reads only. |
| scripts/verify_deployment.py | pins | The five versions above. |
| tests | | daily_plan Q1 (2 tests), holding_read P1 (2), today_action_plan L1, release_decision_feed n34. Each suite reads the detailed lines through its gate set to on, so the suites pass the same way on GitHub Actions. |

Exit Plan 1.6.1 set the logging pattern on 22 Nov, and these changes copy it:
- `auto` gives no detail when `GITHUB_ACTIONS=true` and the full lines elsewhere.
- The pages, the ledgers, the e-mail and every decision are unchanged.

## Checks

- **Targeted suites, run locally:** 201 passed, 1 skipped.
  Suites: daily_plan, holding_read, exit_plan, today_action_plan, release_render_hold, manifest pins, sheets_quota.
- **release_decision_feed:** 46 passed, 10 skipped.
- **All 20 test files that touch the five scripts, with `GITHUB_ACTIONS=true` as in CI:** 495 passed, 12 skipped.
- **Workflow audit:** errors 0, warnings 0.

## Not done here (the owner's decision)

- **Old run logs that name holdings.**
  - Today Action Plan and Holding Read runs before this change are still readable on GitHub.
  - Deleting them, like the 238 old Exit Plan logs, waits for the owner's explicit yes.
- **The quota itself: a higher limit.** Either step is the owner's to take:
  - Ask Google Cloud for a higher "Read requests per minute per user" limit on project 351053104652. The default is 60.
  - Or give the heavy readers a second service account. That is an ENV change.

Not investment advice; the owner decides and executes every trade.
