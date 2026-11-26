# TFB Commit Sheet — exit_plan 1.7.1 · holding_read 1.0.3 (the news rows are read from the bottom of the tab)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (🎯 Exit Plan, Holding Read) · Gate: none changed · No Render deploy · No ENV change · No page layout or ledger column change

## What was wrong
Both pages read the news gate's `_News_Items` tab as a range fixed from the top:

| Page | Read | Used for |
|---|---|---|
| Exit Plan | `A1:M12000` | the rule "a negative item (severity H or M) of the last 3 days tightens the trail", and the line "latest negative item" |
| Holding Read | `A1:P12000` | naming a company item as the reason for a holding's move |

The news gate **appends at the bottom**, up to 200 rows a run. The tab passed 12,000 rows on **20 Nov 2026 at 22:42 Riyadh** (19:42 UTC). From then on neither page could see a new headline. Nothing failed and nothing said so.

On 26 Nov (`v_news_items`, snapshot of that day):

| | Rows |
|---|---|
| In the tab | 19,526 |
| Outside the range the pages read | 7,527 |
| … of them about a name (the rest are market-wide) | 65, on 15 names |
| … on four current holdings | 27 |
| … negative, severity M, tier A | 1 — an 8-K item 4.01 (auditor change) published 24 Nov 21:54 UTC |

That one item is inside the rule's 3-day window until 28 Nov 00:54 Riyadh, and the Exit Plan has not shown it.

Found while checking what the five new news sources (PR 1089) would change downstream.

## Why the tests did not catch it
The test worksheet returned every row whatever range it was asked for. A fixed range therefore passed every test while it had stopped seeing the live tab's newest rows.

## Change
`scripts/exit_plan.py` 1.7.0 → **1.7.1** — `read_newest(book, tab, names, rows=20000)`:
1. reads the header row;
2. reads the length of column A (the item id — filled on every row);
3. reads, in one batch, the tails of the columns the page uses, from the newest 20,000 rows.

About the same number of cells as before (7–8 columns × 20,000 against 13–16 × 12,000). 20,000 rows are a month at November's rate (650 a day) and eleven days at the gate's ceiling.

`scripts/holding_read.py` 1.0.2 → **1.0.3** — reads its items through the same function.

Each run logs one line: `news: rows A-B of _News_Items read (the newest N)` — row numbers only, no name and no headline.

A missing tab still reads as empty; a connection or quota fault is still raised to the run's retry, never read as "no news".

## What changes on the pages after the merge
- A holding with a negative item (H or M, not a law-firm notice, cluster grade 1–3) of the last 3 days shows it again: `trail tightened to 2xATR: negative news (…)` and `latest negative item: …`.
- The **stop number** moves only where the trail is already in force (the holding has reached +1R); otherwise the line appears and the stop stays.
- The Holding Read can again name an item recorded after 20 Nov as the reason for a move.
- For the one item above: the holding has not reached +1R, so its stop number is not expected to move; the two lines appear until the window ends.

## Tests
- `tests/test_exit_plan.py` **X24** (2 tests): a tab of 12,502 rows read through a worksheet that honours the range — the old read sees nothing of the newest row, `read_newest` does; the budget leaves out the *oldest* rows; short tab = whole read; a blank cell does not shift rows; missing tab / empty / header only; a fault is raised. End to end: the page and the ledger carry the negative item recorded after row 12,000, and the public log names no holding and no headline.
- `tests/test_holding_read.py` **H12**: the same tab; the page with the long tab equals the page with the short one.
- 378 tests of the 15 files that touch either page pass. Six hand mutants (either page reading the top again, the budget ignored, the oldest rows kept, a fault read as an empty tab, the logged rows wrong) each fail a test.

## Files
| File | Change |
|---|---|
| `scripts/exit_plan.py` | 1.7.0 → **1.7.1** |
| `scripts/holding_read.py` | 1.0.2 → **1.0.3** |
| `scripts/verify_deployment.py` | pins |
| `tests/test_exit_plan.py`, `tests/test_holding_read.py`, `tests/test_exit_plan_offers.py` | X24, H12, pins |

## Not in this change
Other tabs are read with fixed ranges too (`_News_Gate` 4,000 — 137 rows today; `_Daily_Plan` 2,001; `_Cash_Snapshot` 2,000; `_Dividend_Log` 3,000; calendar tabs 4,000). Each is to be checked against its live length and its writer's own cap in the next morning build.

## Owner
Merge when content. Nothing to set.

## Rollback
Revert the two scripts to 1.7.0 / 1.0.2 (the pages then read the first 12,000 rows again).

Not a licensed financial adviser; the owner decides and executes every trade.
