# Commit Sheet: archive_market_series v1.1.1, window-aware minimum bars (Codex P2 on #880), 2026-10-29

**Scope:** this PR fixes the Codex review finding on #880 (comment 4410899972, P2, `scripts/archive_market_series.py:526`).

It keeps the same read-only contract as v1.0.0 and v1.1.0:
- nothing is written to the workbook;
- no decision, gate or count changes;
- the SQL objects and the workflow are unchanged.

## Correction
| Defect | Root cause | Change | Tests | Deployed/installed version | Effective mode | Live acceptance | Remaining dependency |
|---|---|---|---|---|---|---|---|
| A run with `--days 30` refused every series and exited 1, even when Yahoo returned a complete history. The script accepts 30 days (its range is 30..4000), and so does the workflow's `days` input. | v1.1.0 set a fixed `MIN_BARS = 30`. A 30-calendar-day window holds only 20-22 trading days. The hourly fallback is filtered to the same window, so neither path could reach 30. | New `min_bars(p1, p2)` = `max(2, min(MIN_BARS, window_days * 5 // 7 // 2))`, which is half the trading days the window holds, capped at 30 and never below 2. `fetch_series` uses it to decide when to rebuild from hourly bars; `_one` uses it to decide when to refuse a series. Results: `--days 30` needs 11 bars; the nightly 1,100 days still need 30. `__version__` is 1.1.1, with the matching pin in `verify_deployment.py`. | New test M14 (below). The suite gives 103 passed: `test_archive_market_series`, `test_bq_views`, `test_bq_archive`, `test_verify_manifest_pins`, `test_ci_required_verdict`, `test_repository_workflow_audit` and `test_workflow_audit_repairs`. `audit_repository_workflows.py` reports errors=0 warnings=0. The CI ruff rules (E9, F63, F7, F82) pass on the whole tree. `compileall` is clean. | Not installed yet. The job runs from `main` in GitHub Actions (`bq_markets.yml`), so v1.1.1 takes effect on the first run after merge. | Unchanged from v1.1.0. The nightly schedule (22:35 UTC, `days=1100`) behaves exactly as before. A manual dispatch with a short window now works. | After merge, a **dry-run** dispatch (`write=false`, `days=30`) should list 48/48 series and touch nothing. The next nightly run then loads 48/48. | Owner merge. |

## Test M14
- **Thresholds:**
  - `min_bars` gives 11 for `--days 30` and 30 for the default and the maximum window.
  - It gives 2 for an empty or inverted window.
  - It grows with the window and stops at 30.
- **Loads:**
  - A complete 30-day answer (22 weekday bars) loads every series.
  - A one-bar series still fails and keeps its rows; the message names the 11 bars needed.
- **`fetch_series` on the 30-day window:**
  - A full daily answer makes no hourly call.
  - A one-bar daily answer is rebuilt from hourly bars inside the window (between 11 and 30 bars), and that result loads.
- **Earlier tests:** M4, M12 and M13 are unchanged and pass. M13 still checks that `MIN_BARS == 30` for the long window.

## Operator note (behaviour unchanged, now reachable)
`write=true` with a short window replaces each loaded series' partition with that window. This is the v1.0.0 design (WRITE_TRUNCATE per partition). The page's year and YTD views would then show only that window until the next nightly 1,100-day run restores it.

Before this fix, such a run failed and kept the history. Now it succeeds. Use `write=false` (dry run) for short checks.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
