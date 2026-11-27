# TFB Commit Sheet — challenger_books 1.0.0: each forward book of the shadow challenger, frozen once, the day it is final

Date: 2026-11-27 (Friday) · Lane: GitHub Actions, chained after the BigQuery Symbol Deep Data workflow · Research only: nothing reads the table yet; no recommendation, gate or order changes · No Render deploy

## Why
The full project review of 27 Nov listed "save the challenger's weekly books" for Thu 3 Dec; the owner's word of 16:30 Riyadh ("ok let's start do your recommdadtions and optimzation phase") brings it forward, so the first forward book is kept.

`v_challenger_v0_walkforward` recomputes every weekly book each time it is read. Its inputs change under it: `symbol_bars_daily` is Yahoo's two-year history, replaced whole every night (the deep and outcome sets also change from night to night: 320 to 396 deep names in the last ten runs), and `v_symbol_sector` holds today's followed names. A book read in January can therefore differ from the book formed on 30 Nov. A forward test is judged on the books as they were formed, so each book is written down once, the day it is final, and never rewritten. The first forward book forms on Mon 30 Nov 2026 (the view's first session of each ISO week; scorecard `forward from 2026-11-27`).

## What

### New `scripts/challenger_books.py` **1.0.0**
- **Reads** (SELECT only, the shared reader `evidence_board.bq_query`): the frozen keys of `challenger_books`; the forward books of the walk-forward view from the later of 27 Nov and three weeks ago that the table does not hold yet (`NOT EXISTS`), each row with its book's own size (`COUNT(*) OVER (PARTITION BY market, d)`); each market's archived sessions (a date with at least 20 closes; the market of a symbol is its exchange suffix, the same `CASE` as `v_challenger_v0_features`); and, only when something is frozen, the SHA-256 of each installed view the book is read through (`INFORMATION_SCHEMA.VIEWS`).
- **Freezes** each (market, d) that is **final**: the archive holds a later session of that market, so the close of d is settled. Rows go into the partition of d with `WRITE_APPEND` through the tested loader of `archive_symbol_deep` (1.3.1), the one `news_now` uses. A frozen book is never rewritten or deleted by this script.
- **Oldest first.** Each book is judged against the books frozen before it, this run's own included, so a run catching up on two weeks judges the second book against the first; a book whose write failed is no reference (Codex P2 on #1109, second commit).
- **Not frozen, and the log says why:**
  - *short* — fewer rows came back than the book's own size (the reader returns one page of 5,000 rows; the rows are ordered by market and date, so a cut falls on the last book and the whole books before it are still frozen);
  - *thin* — under 90 % of the names of the market's last frozen book while only one later session is archived (a night Yahoo answered fewer names: the outcome set is best effort). With a second later session it is frozen as it is, and the log says so. The first book of a market has no earlier book to compare with; in the last ten runs the deep set answered 100 % and the outcome set 97 %;
  - *week* — the market already has a frozen book of that ISO week (a formation date that moved);
  - *not final*, *frozen already*, *before the forward start*.
- **Columns** (37): the formation date, market, symbol, sector; the five inputs, the four ranks, the sector-ranked flag; both scores and quintiles; the training dates, the weights' basis, the four trained ICs and the four weights; the shadow action, size and benchmark. **No result column** (entry and exit dates, returns, the label, `is_latest`): those close 22 sessions later and are scored from the archive. Then `book_n`, `frozen_at`, `frozen_days_after`, `run_id`, `views_version` (this checkout's `apply_bq_views`) and `views_sha` (`name=first 12 hex` of each installed view of the chain, the same hashes `tests/test_bq_views.py` pins: on 27 Nov the live walk-forward view reads `0e39890fc235`, its pinned hash). A book frozen before and after the clean bars of views 1.37.0 can be told apart by this column alone.
- **Table** `challenger_books`: DAY-partitioned on `d`, clustered by market and symbol; created on the first write run.
- **Logs:** counts per market and action only — never a symbol.
- **Switch:** repository Variable `TFB_CHALLENGER_BOOKS=off` reads and writes nothing (default on).
- **Exit:** 0 (frozen, or nothing to freeze), 1 (a market's load failed; the others are still frozen), 2 (configuration missing, or a read failed: nothing written).

### New `.github/workflows/challenger_books.yml` **1.0.0**
- Runs after every completed run of `🔬 BigQuery Symbol Deep Data` on main (the bars it reloads are what makes a book final), whatever its result: a run that kept yesterday's table leaves nothing new to freeze. No cron (GitHub starts this repository's schedules hours late). A manual dispatch is a dry run unless `write=true`.
- Own concurrency group `tfb-challenger-books` (no other workflow shares it); `contents: read`; inputs reach the shell as env values only. About 0.65 GB read per run.

### Pins and CI
`scripts/verify_deployment.py`: challenger_books 1.0.0 (new, pinned on arrival). `ci.yml` 1.1.106: `tests/test_challenger_books.py` joins the blocking lean lane after `tests/test_bq_snapshots.py`.

## When the first books are frozen
| Market | Book of | Final when the archive holds | Expected freeze |
|---|---|---|---|
| JP, HK | Mon 30 Nov | a Tue 1 Dec bar (Tokyo has closed and Hong Kong is trading by 06:42 UTC) | Tue 1 Dec morning |
| SA, OTHER | Mon 30 Nov | a Tue 1 Dec bar (a part-day bar is enough: Monday's session is over) | Tue 1 Dec or Wed 2 Dec, by which morning run has it |
| US | Mon 30 Nov | a Tue 1 Dec bar (in Wednesday morning's bars) | Wed 2 Dec morning |

The first runs after the merge find no forward book yet and say so; they create the table and prove the reads live.

## Limits, stated
- The book is frozen from the archive of the freeze night, not of the formation night: one or two days later. Yahoo can still revise a bar inside that gap; `frozen_days_after` records it.
- The first book of each market has no reference for the thin rule.
- Two runs a day can find the same book final; the second finds it frozen (`NOT EXISTS`). A load that BigQuery finished after the loader gave up could be followed by a second copy on the next run; the scorecard keeps the earliest `frozen_at` per book.

## Tests
- New `tests/test_challenger_books.py` (34 tests): every query passes the real SELECT-only reader; the book query reads every input and no result column (against the view's 39 live columns); every book column is named in the three challenger views; the session market equals the features view's `CASE`; the views query and its chain; the schema; the plan (final, not final, short, frozen, before the start, one book a week, thin held one session then frozen as it is, the 90 % boundary, the reference is the latest earlier book of the same market, the first book; a run catching up on two weeks: the second judged against the first, a book not frozen or whose write failed is no reference, the dry run the same); rows and their types (UTC freeze date even at 01:30 Riyadh); the hash string; a write run end to end (the order of reads, one load per book into its partition, `WRITE_APPEND`, counts-only log); a second run; the three-week window; a dry run before the table exists; a write run never reads round a missing table; failed reads; one market failing; a missing hash; the page limit; nothing final; the switch and the configuration; `main()`; the source (ASCII, one log writer, the pin, the CI entry); the workflow (chained to the deep workflow's own name on main, no cron, dry run unless asked, own group, env-only inputs).
- 32 hand mutants, each failing a test: the final rule, the short rule, the thin share and its hold, the reference (later books, the largest instead of the latest), the week rule, the forward start, `NOT EXISTS`, the date floor, the dry-run fallback, the disposition, the partition, the UTC date, the hash length, the table created in a dry run or not in a write run, the switch, `book_n`, the exit code, the frozen rule, the ISO week, the chain filter, the window, the hold on the second session, the dry run, and the six of the catch-up order (the pass's own freezes as references in the plan, in a write run and in a dry run; a failed write counted; the dry run's count; the hash read once).
- Related tests (every test that reads the workflows, `ci.yml` or the deployment manifest): 89 files, 2,344 passed, 42 skipped. Workflow audit: 0 errors, 0 warnings.
- Live checks in BigQuery (27 Nov): the session query (HK, JP to 27 Nov, SA and OTHER to 26 Nov, US to 25 Nov), the book query with `NOT EXISTS` (the excluded key left out; every book whole: US 503, SA 254, OTHER 109–110, JP 59, HK 52; JP's book of the week of 23 Nov is Tue 24 Nov, a Japanese holiday on the Monday), the views hashes (equal to the pinned ones).

## After the merge
1. Claude dispatches `challenger_books.yml` on main as a dry run and reads the log back.
2. From Tue 1 Dec each book is frozen after the morning bars; Claude reads `challenger_books` back on Wed 2 Dec.
3. A scorecard of the frozen books follows when their first results close (22 sessions after 30 Nov).

## Owner
Merge when you have read it (better before Tue 1 Dec, and after #1108 so the first books are read through the clean bars). Nothing to set.

## Rollback
Repository Variable `TFB_CHALLENGER_BOOKS=off`. The table holds research rows only; nothing reads it.

Not a licensed financial adviser; the owner decides and executes every trade.
