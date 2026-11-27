# TFB Commit Sheet — Workbook grid trim 1.0.0: the empty grid Google counts, reported tab by tab; trimmed only by hand

Date: 2026-11-27 (Friday) · Lane: GitHub Actions, manual dispatch only · Report by default: nothing is written until the owner arms it · No Render deploy · Second commit of PR #1106

## Why
The full project review of 27 Nov (owner's approval at 14:06 Riyadh: "ok approved and start doing the need") found the workbook at 9,736,507 of Google's 20,000,000 allocated cells (48.7%), growing about 100,000 a day. Google counts every tab's rows × columns, used or not. The owner's export of the same hour holds values in a used range of 4.58 million cells, so about half of the grid is empty: tabs left at the default 1,000 × 26, log tabs with unused columns. At this pace the limit arrives between late February and mid-March 2027.

The August tool (`scripts/repair_grid_capacity.py`) trims the rows of six named tabs. Its dry run of today (run 45987841282) found five of them already at or under target; the sixth would delete log rows. It cannot recover the empty grid of the other 93 tabs.

## What

### New `scripts/trim_workbook_grid.py` **1.0.0**
- **Report (the default).** Every tab's grid, its used extent measured exactly, its target and the cells a trim would free; the totals before and after; and the tabs the export audit calls dead or dated, listed as candidates to archive. Nothing is written.
- **Used extent, exact.** The last row and column holding a value or a formula, read with FORMULA rendering (a formula that shows nothing still counts). Row 1 and column A are read first; then the two regions outside that probe, and anything found there extends the extent. A chunk of tabs whose read fails is read again tab by tab; a tab that still cannot be read is skipped, never guessed.
- **Structure is extent too.** Charts and slicers (anchor cell plus size, counted at 15 px a row and 50 px a column, so an object is taken to reach further than it does), merged ranges and named ranges come from the workbook metadata. The export holds 37 charts on three pages; one page has 28 charts down column A over a few rows of values.
- **Targets (reductions only).** A tab with more than 5,000 used rows keeps used + 1,000 rows and used + 4 columns. A smaller tab keeps the larger of 2 × used and used + 300 rows, and all its columns (its readers and writers use fixed widths). Never inside frozen rows or columns. A tab is touched only when that frees at least 5,000 cells.
- **Never touched:** the five pages the sync sizes itself (`MANAGED_TABS`), any tab in `--exclude`, a sheet that is not a grid, a tab whose extent was not measured. The tool never deletes a tab and never writes a cell: its only write request is `deleteDimension` on trailing rows and columns.
- **Apply (four gates, as the August tool):** `--apply`, `--i-have-backup`, `--confirm TRIM`, env `TFB_GRID_TRIM_APPLY=1`. A half-armed call is refused before the workbook is opened. The grids are read again first (a tab whose grid changed since the plan is skipped); then, tab by tab, the region to delete is read again and must come back empty. One failing tab does not stop the others; any problem ends the run red (exit 1) with the tab named. One wait and one more try on a Sheets 429.
- **Logs:** tab names and counts only. No cell value is printed.

### New `.github/workflows/grid_trim.yml` **1.0.0**
- Manual dispatch only. Inputs: `apply` (false), `have_backup` (no), `confirm`, `exclude`. Inputs reach the shell as env values only.
- **A private concurrency group, on purpose.** The lesson recorded in `track_performance.yml` (6 Oct): a group keeps one pending run and a newly queued run cancels the one already waiting, so a trim queued in the production-write group could evict a waiting Daily Sync. Instead an armed trim first reads the Actions API and refuses, fail-closed, while a Daily Sync, a page-refresh recovery or a grid-capacity repair is queued or running, or when that state cannot be read. The report needs no such check.
- `scripts/release_decision_feed.py` is not changed (`INFLIGHT_WORKFLOWS` stays as it is): the trim deletes empty grid on tabs the release does not write, and it refuses to start beside the writers that list names.

### Pins and CI
`scripts/verify_deployment.py`: trim_workbook_grid 1.0.0 (new, pinned on arrival). `ci.yml` 1.1.105: `tests/test_trim_workbook_grid.py` joins the blocking list (same version; its note is extended).

## Limits, stated
- The API does not show an image placed over cells, or a dropdown rule in an empty cell. One that sits below the kept margin goes with its rows. `--exclude` leaves any tab alone.
- A formula whose range ends on a fixed row (`A2:A1000`) is shortened to the new last row. The export of 27 Nov holds 674 formulas; none points at a tab this tool may trim with such a range (the one bounded cross-tab formula reads the four market pages, which are never trimmed; the rest use whole columns).
- A writer that reads a tab's row count and then writes to it could meet a trim between its two calls and fail once. The 12 scripts on `write_grid` grow a tab that is too small, and so do the calendar, corporate-events and pre-open writers. The armed trim refuses to run beside the production writers; a quiet hour (Saturday) covers the rest.
- The freed total is not known until the report runs on main: the export carries values, not grid sizes. From the export, about 3.7 million cells is the estimate.

## Tests
- New `tests/test_trim_workbook_grid.py` (17 tests) on an in-memory workbook that answers like the Sheets API and **refuses** any delete that would remove a value or the cells under a chart, a merged range or a named range:
  names and ranges; the metadata; structure as extent; the exact extent (values off the probe, a formula that shows nothing, an apostrophe in a tab name, more tabs than a chunk, a failed read, a short answer on either read); the targets; the plan (the gain floor at exactly 5,000, managed, excluded, unmeasured, frozen rows, archive candidates, structure holding the floor); the requests; the trim end to end (every value where it was, totals equal the plan, a second pass finds nothing); a tab that gained a value or changed its grid since the plan; unreadable grids; one failure not stopping the others; the quota wait; the four gates through the real `main()`; the report (no cell value in it); the two lists held equal to their owners (`tfb_export_audit.DEAD_TABS`, the August tool's page map); the workflow (manual only, private group, the fail-closed guard, env-only inputs); what the tool cannot write.
- 400 seeded random workbooks with random values, charts, merges and named ranges: the measured extent equals the true one, every value and object survives, no grid grows, the result equals the plan (290 trims, 101 of them held by structure).
- 55 hand mutants, each failing a test: both row margins and the 5,000 boundary, the frozen floors, each term of the extent, both verify ranges, the probe, managed and excluded tabs, the gain floor (two ways), structure rows and columns, growth, structure beyond the grid, named ranges of another sheet, the chart cell sizes and offset, merges, slicers, open named ranges, dated tabs, both delete indexes, the column re-read range, the re-read, the short re-read, the grid re-check, the pause, a failure stopping the rest, unreadable grids ignored, the quota retry (four ways), each gate, a lone flag, the exit code, gates checked after opening, managed pages read, the tab-by-tab retry, the short second read, a failed tab guessed empty, the report order and its two notes, displayed values instead of formulas, the dead-tab list.
- Related tests (every test that reads the workflows, `ci.yml` or the deployment manifest): see the PR body for the count. The file also passes with `GITHUB_ACTIONS=true`. Workflow audit: 0 errors, 0 warnings.

## After the merge
1. Claude dispatches `grid_trim.yml` on main with `apply=false` and brings the per-tab plan to the owner. Nothing is written.
2. The trim itself runs only on the owner's word, after a workbook backup, on a quiet hour: `apply=true`, `have_backup=yes`, `confirm=TRIM`.
3. Archive candidates (dead and dated tabs) are the owner's decision; the tool only lists them.

## Owner
Merge when you have read it. Nothing to set.

## Rollback
- A deleted empty row or column holds nothing. Any writer that needs more grid adds it; by hand: Insert > Rows / Columns.
- To retire the tool: delete `grid_trim.yml`. Nothing else reads it.

Not a licensed financial adviser; the owner decides and executes every trade.
