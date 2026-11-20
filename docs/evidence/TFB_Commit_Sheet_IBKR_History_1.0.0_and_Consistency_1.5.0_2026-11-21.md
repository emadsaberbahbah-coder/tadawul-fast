# Commit Sheet: the broker's own return and trade record on the v2 page (ibkr_history 1.0.0, consistency_check 1.5.0), 2026-11-21

**Scope.**
- **Request.**
  - Owner, 20 Nov 2026 22:40 Riyadh, in Arabic: a full review of the file against the market, and of the differences between the pages and between the file and the market.
  - 23:34: "Keep review the file for coming 3 hours and give me next update and improvements".
- **Basis.** Consistency_Check C15 (added the same evening) read STALE on every run:
  - `_IBKR_NAV_History` ends 17 Oct and `_IBKR_Account` is as of 17 Oct (34 days);
  - both are hand-filled from one connector read.
- **Limits.**
  - Reporting only: no score, pick, gate, level or order changes.
  - The broker is read by a scheduled task outside the repository, never by this code.

## Why
| Finding (20 Nov, IBKR read-only, 00:26 Riyadh) | Why it matters | Fix |
|---|---|---|
| Portfolio_Decision_v2 showed the return since 11 Aug as −0.1%. IBKR's TWR series gives −4.7%. | The page the owner reads for "how is the account doing" was a month old. | `_IBKR_NAV_History` is rewritten every night from IBKR's own cumulative TWR. |
| The trade record showed 3 wins in 15 closed trades. IBKR's fills since 28 Jul give 5 wins in 22. | The 7 trades closed since 8 Oct were missing, including the four stops of November. | `_IBKR_Trades` is rebuilt from IBKR's fills (round trips, IBKR's realized P&L). |
| C15 reads `_IBKR_Account`, which nothing rewrites. Since 1.2.22 the page takes the account and orders from the hourly capture. | C15 could never read MATCH, even with fresh tabs. | C15 reads `_IBKR_Trades`' new `as_of`. `_IBKR_Account` is read only while no capture is read. |

## Changes
- **`scripts/ibkr_history.py` 1.0.0 (new).**
  - **Input.** Reads the newest `TFB_IBKR_History_*.json` files (schema `tfb.ibkr-history/v1`, at most 10) from the capture folder, read-only.
    - The capture readers ignore these files: they take only `TFB_IBKR_Capture_*`.
  - **`_IBKR_NAV_History`.** IBKR's longest daily series from `TFB_IBKR_NAV_FROM` (2026-08-11).
    - The tab's rows up to the first day both share are kept.
    - Every later day is IBKR's, rebased onto that day. So the line survives IBKR's period resets (YTD restarts on 1 Jan) and IBKR's revisions flow in.
    - A tab IBKR's series cannot be joined to is left as it is.
  - **`_IBKR_Fills`.** Every stock fill, merged by trade id, so fills older than the task's 90-day read are kept.
  - **`_IBKR_Trades`.**
    - Columns: symbol | name | ccy | exit_type | span | realized_usd. One row per closed round trip (flat → holding → flat), best first.
    - A sale of more than the fills show held closes a trip with an unknown start ("?").
    - A non-USD trip is converted at the nearest USD.<ccy> conversion IBKR reported.
    - H1:I4 carry as_of, source, fills and written.
  - **Failures.** No usable file: nothing is written (exit 0, reason in the log). A failed read or write: exit 2.
  - **Log.** Counts and dates only.
- **`scripts/consistency_check.py` 1.4.0 → 1.5.0.**
  - **C15** reads the `_IBKR_Trades` stamp. A record with rows and no stamp is a hand-filled snapshot: STALE.
  - **C15** reads `_IBKR_Account` only without a usable capture.
  - **`run_checks`** keeps the capture under its own name. C12 reuses `cap` for the position cap, so before the fix C15 received a number.
  - **RANGES** adds `_IBKR_Trades!A1:I6`.
- **`.github/workflows/ibkr_history.yml` 1.0.0 (new).**
  - Runs 00:45 Riyadh Tue–Sat, before the 00:50 Consistency Check, and again at 09:45. Also by dispatch, with dry run.
  - Rollback: `TFB_IBKR_HISTORY=off`.
- **`.github/workflows/ci.yml` 1.1.90.** `tests/test_ibkr_history.py` runs in the blocking lean lane.
- **`scripts/verify_deployment.py`.** Pins consistency_check 1.5.0 and ibkr_history 1.0.0.

## Checked on the real account (21 Nov, outside the repository)
- **Input.** IBKR's trades for LAST_QUARTER and DAYS_90 (80 stock fills, 3 conversions), through the same functions.
- **The 15 trips the hand-filled tab held.** All come out with the same realized figure, span and exit type.
  - The one SGD trip comes out at 83.65 against 82.90: the tab had used another rate.
- **Added.** The 7 trips closed since 8 Oct, for 22 trips with 5 wins.
- **NAV.** The rebuilt line gives −4.73% since 11 Aug, against SPY −10.34% over the same closes.

## What the owner does
- **Approve one scheduled task.** It reads IBKR performance and 90 days of trades, read-only, at 00:20 Riyadh Tue–Sat, and writes `TFB_IBKR_History_<date>-<time>.json` to the capture folder.
- **Optional seed file.** On approval, Claude can write one seed file with IBKR's LAST_QUARTER trades (Jul–Sep), read-only, so the 4 trips bought before the 90-day read show their real start instead of "?". The record is complete without it: realized figures and the 22 trips are the same.
- **Until then** the workflow writes nothing and says why.

## Review fixes (Codex, PR 1041)
- **P2: a non-USD amount in the USD column.** A trip in another currency with no USD conversion in the fills is now left out of the record. It is named in the stamp ("not converted"). Before, it was written in its own currency into `realized_usd`, which the page sums as USD.
- **P2: a stamp without a trades read.** The record and its `as_of` now come only from files that carry a trades read. A file with only the performance series updates the return; `_IBKR_Fills` and `_IBKR_Trades` keep what they hold.
  - A first run can no longer replace the hand-filled record with a shorter one. Its rows that close before the first fill are carried forward and marked in column G.
- **Also found on the real data.** One market order filled in two pieces (4 + 1 shares) read as two trips. Fills of one order now count as one execution.
- **P2 (second pass): an order filled around a sale.** Only consecutive fills of one order are joined. A fill on the other side in between starts a new execution, so BUY 10, SELL 10, BUY 10 (same buy order), SELL 10 stays two trips.
- **P2 (second pass): the closing year of a carried row.** Column G now records the closing day when a row is first carried ("carried: closed 2026-08-06 ..."). It is read back on every later run, never guessed again from the yearless span.

## Tests (`tests/test_ibkr_history.py`, 23; `tests/test_consistency_check.py` K11)
- **Files.**
  - Parsing: schema, captured_at against creation time, a future time, the longest series, fills.
  - Every date shape.
- **NAV.**
  - Rebuilt from an empty tab.
  - Rebased at the first shared day, with the tab's later non-IBKR row dropped.
  - Kept when it cannot be joined.
- **Round trips.** Partial sales, a same-day trip, an unknown start, an open position not listed, an order filled in pieces, a scale-out, the SGD conversion, and a trip with no USD rate (left out and named).
- **Trades read.** A file with only the series updates the return and keeps both trade tabs. Hand-filled rows before the first fill are carried; a second run carries only the marked rows. Span end dates are read across a year end.
- **Merge, names, spans, exit types.**
  - Fills: one per trade id, the later read wins, and they read back the same from the tab.
  - Names are tidied, spans across a year end are right, and each closing order type maps to its exit type.
- **Drive.** The newest usable files first. Another prefix, an oversize file and bad JSON are skipped.
- **Run.**
  - The three tabs and the stamp are written, and the log carries no symbol or amount.
  - The newest file rules the NAV, and older files add their fills.
  - A dry run, the off switch and no file write nothing.
  - A failed read or write exits 2.
  - A tab that cannot be joined is left untouched.
- **C15.**
  - MATCH with the stamp and a capture.
  - STALE on the account tab without a capture.
  - STALE on a hand-filled record.
