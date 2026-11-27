# TFB Commit Sheet — BigQuery views 1.36.0: the bar archive check (jumps, the jump that undid each, a reading per symbol)

Date: 2026-11-27 (Friday) · Lane: BigQuery research views · Research only: no decision, gate, count, recommendation or order reads these views · No Render deploy

## Why
Plan item 'Split and bad-print check on the nightly bars' of the full project review of 27 Nov (owner, 27 Nov 16:30 Riyadh: "ok let's start do your recommdadtions and optimzation phase").

Every return in the research views is a ratio of two archived closes: the outcome engine, the forward ledger, the shadow challenger, the news and stop studies. A bar recorded on another price basis (a split applied to part of the history, a bad print), or a split recorded without adjustment, becomes a return of +200 % or −70 % that never happened. No view listed such bars, so nothing could explain them or keep them out.

## What

### New `sql/bigquery/148_v_bar_jumps.sql`
One row per jump in `symbol_bars_daily`.
- **Jump:** close ≥ 1.5 × the previous archived close, or close × 1.5 ≤ it. A close of 0 or less is no bar.
- **Undone:** the next jump of the same symbol undoes it when the product of the two ratios is between 0.8 and 1.25 and the two are at most 60 days apart.
- **Leg:** `out` and `back` for such a pair (a chain alternates), `alone` for a jump nothing undid. `bars_away` is the bars between the two.
- **Kind:** `spike` (back within 3 bars), `detour` (back later), `step` (alone, 3 or more later bars), `too new` (alone, fewer).
- It looks at later bars on purpose: it checks the archive, it is not a signal. No clock is read.

### New `sql/bigquery/149_v_bar_jump_symbols.sql`
One row per symbol with a fixed reading, in this order: `two price bases` (a detour), `bad bar` (a spike), `large step` (a step of 3 times or more: a split or reverse split recorded without adjustment is likely), `too new`, `steps under 3 times` (a real move is possible; price alone cannot tell). `is_new` marks a last jump within 7 days of the archive's newest bar.

### `scripts/apply_bq_views.py` 1.35.0 → **1.36.0**
Docstring entry and version. No existing view changes. `scripts/verify_deployment.py`: pin 1.36.0.

## Read on live data (27 Nov 2026, archive of 557,744 bars on 1,112 symbols, 28 Oct 2024 to 27 Nov 2026)
| Reading | Symbols | Jumps | Bars on another level | Call records on them (scored) |
| --- | --- | --- | --- | --- |
| two price bases | 3 | 26 | 59 | 20 (0) |
| bad bar | 5 | 31 | 6 | 26 (0) |
| large step | 6 | 22 | 0 | 44 (12) |
| steps under 3 times | 15 | 28 | 0 | 130 (25) |
| **all** | **29** | **107** | **65** | **220 (37)** |

- 26 of the 107 jumps are dated 21 Jul 2026 or later. 5 of the 29 symbols are Saudi.
- 27 of the 29 symbols are in the shadow challenger's walk-forward view. The outcome engine scored 37 records on them, of 7,733.
- The clearest case is one Saudi series that sits at a third of its price for 7 bars, returns, and has one more such bar three weeks later: two price bases in one series.

## Tests
- `tests/test_bq_views.py` `case_v70` (70 cases pass): both files' fragments, both body hashes, the order and kinds lists, the file list, version 1.36.0, no clock in either body, `LEAD` only over the chain of jumps.
- **The rule was run in BigQuery itself.** Twelve short series (two price bases, one bad bar, an unadjusted reverse split, too new, a chain of three, a way back after 65 days, an overshoot, a close of 0, back on the third against the fourth bar, the exact thresholds, a flat series) were given to both view bodies in place of the archive table. All 22 jump rows and 11 symbol rows came back equal to the Python mirror in the test, column for column. The series and the expected rows are pinned in the test.
- The edges, each deciding a case: exactly 1.5 times and exactly two thirds are jumps, a hair inside is not; products of 0.8 and 1.25 pair, 0.79 and 1.26 do not; 60 days pair, 61 do not; 3 bars is a spike, 4 a detour; 3 later bars is a step, fewer is too new.
- Any change to either body changes its pinned hash.
- Related tests (the 60 test files that name the views tool, the bar archive or the deployment manifest): 1,650 pass, 35 skipped. The changed test file also passes with `GITHUB_ACTIONS=true`.

## Not in this change (next, as planned for Mon 30 Nov)
1. A line on a page each morning when `is_new` is true.
2. Keeping the affected bars out of the return maths: the outcome engine, the forward ledger and the challenger, then the challenger re-read.

## After the merge
Claude dispatches BigQuery views `--write` on main, then reads `v_bar_jump_symbols` back.

## Owner
Merge when you have read it. Nothing to set.

## Rollback
Delete the two files and run views `--write` (the tool drops nothing by itself; `DROP VIEW` the two by hand if wanted). Nothing reads them.

Not a licensed financial adviser; the owner decides and executes every trade.
