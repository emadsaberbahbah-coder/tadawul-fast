# Commit Sheet - `.github/workflows/daily_sync.yml` v1.0.7 + `ci.yml` 1.1.49 - 2026-10-28

**Scope:** reliability of the decision chain - one slow Google Sheets read must not withhold the Top-10 board. Same class as E1 (21 Oct; `run_dashboard_sync` v6.70.2 added the retry), which the default budget still leaves exposed.
**Files:** `.github/workflows/daily_sync.yml` (header + one env key in each of the two page-writing jobs), `tests/test_daily_sync_stamp_budget.py` (new), `.github/workflows/ci.yml` 1.1.49 (lean lane), this sheet. No script, gate, threshold or step changes.

## S1 - WHY (evidence)
- Run `40556442682` (scheduled 12:17 UTC slot, started 27 Oct 18:03 UTC): all three data legs **success**; the Global_Markets leg wrote 6,611 rows, then:
  - `[STATUS-STAMP v6.70.2] could not read _Status key column for Global_Markets (attempt 1/4) - TimeoutError: The read operation timed out; retrying in 3s`
  - `[STATUS-STAMP v6.70.2] stamp FAILED for Global_Markets after 2 attempt(s) (90 s stamp budget reached) ... The stamp was NOT written`
- `_Status` then held Market_Leaders on run `40556442682` (21:08 Riyadh) and Global_Markets on run `40484148059` (13:45 Riyadh).
- The owner's `dt10SelfTest` at 23:08 Riyadh read `source readiness: WITHHELD: Global_Markets source run 40484148059 differs from cohort 40556442682`, and the board stayed WITHHELD until a later sync.
- Mechanism: each read waits the client's full 60 s socket timeout. With the 90 s default retry budget, attempt 2 starts at 63 s and ends at 123 s; the next backoff (8 s) would pass 90 s, so attempts 3 and 4 never run.

## S2 - WHAT
- Both jobs that map `TFB_SYNC_STATUS_STAMP` (sync-dashboard and recover-missing-market-pages) now also map:
  `TFB_SYNC_STATUS_STAMP_DEADLINE_S: ${{ vars.TFB_SYNC_STATUS_STAMP_DEADLINE_S || '300' }}`
- With 300 s, the default 4 attempts (backoff 3 / 8 / 15 s) all fit even when every read times out: worst case 4 x 60 + 26 = 266 s, paid only by a stamp that keeps failing.
- The script already reads the variable and clamps it to 10..600 (`_status_stamp_deadline_s`); its default stays 90.
- Header `VERSION 1.0.7`: 1.0.5 and 1.0.6 are claimed by open PRs #778 and #779.

## S3 - Tests
`tests/test_daily_sync_stamp_budget.py` runs the real `_stamp_page_status` on a fake clock.

| Case | Proves |
|---|---|
| B1 | Every job that maps the stamp gate maps the budget exactly as above (2 jobs). The header names v1.0.7. |
| B2 | Each read takes 60 s and times out. Budget 90 gives 2 attempts and the run-40556442682 annotation word for word. Budget 300 gives all 4 attempts (266 s). Budget 300 with a read that heals on attempt 3 writes the row. |
| B3 | The script accepts 300; the default stays 90. |
| B4 | `yaml(v1.0.7)` minus the two added keys equals `yaml(v1.0.4 at 21dc13e9)` (env mapping only). The golden negative: B1 finds 2 problems on that base. Needs the base commit locally; SKIPs on a shallow checkout. |

Results:
- Local run: 32 passed (with `tests/test_sync_status_stamp_retry.py`).
- Every test that reads `daily_sync.yml` plus the workflow suites: 641 passed, 16 skipped (environment).
- `scripts/harness_w1a6.py`: 94/94 PASS.
- `scripts/audit_repository_workflows.py`: errors 0, warnings 0.

## S4 - Rollback
Set the repository Variable `TFB_SYNC_STATUS_STAMP_DEADLINE_S=90` (= v1.0.4 behaviour), or revert this commit.

## S5 - Known limits / next
- This fix gives the stamp more attempts; it does not make Sheets reads faster.
- If a stamp still fails after 4 attempts, the board waits for the next sync, as before. A post-run "stamp repair" step that re-stamps a page whose leg succeeded is the structural follow-up (P1).
- `recover-missing-market-pages` re-stamps only pages it recovers.

## S6 - Operator steps
1. Merge. No Render deploy and no Apps Script change.
2. Read-back: the next run's Global_Markets leg shows either a written stamp or, after a slow read, `attempt 3/4` / `attempt 4/4` lines instead of `after 2 attempt(s) (90 s stamp budget reached)`.

Not a licensed financial adviser; the owner decides and executes every trade.
