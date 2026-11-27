# TFB Commit Sheet — BigQuery views 1.37.0: clean bars for the return maths; the shadow challenger reads them

Date: 2026-11-27 (Friday) · Lane: BigQuery research views · Research only: no decision, gate, count, recommendation or order reads these views · No Render deploy

## Why
Plan item 'Split and bad-print check on the nightly bars' (full project review of 27 Nov; owner, 27 Nov 16:30 Riyadh: "ok let's start do your recommdadtions and optimzation phase"). Its exit test reads: the known symbols explained or adjusted; any new jump flagged; **the challenger re-read**.

Views 1.36.0 (PR #1107, merged 21:53 Riyadh) listed the jumps. Nothing kept them out of a return yet:
- `symbol_bars_daily` is Yahoo's own history, reloaded whole every night, so each jump is in Yahoo's data: a bad print, a stretch on another price basis, a reverse split left unadjusted, or a real move.
- 27 of the 29 symbols with jumps are in the shadow challenger's books. On 27 Nov, a challenger result that spans one of the jumps averaged **302 points** of excess in the US and **98** in Saudi, against 7 and 6 for the others.
- The challenger's first forward book forms on **Mon 30 Nov**. This fix lands before it.

## What

### File order: the bar check moves ahead of every view that computes a return
`148_v_bar_jumps.sql` → `43_v_bar_jumps.sql`, `149_v_bar_jump_symbols.sql` → `44_v_bar_jump_symbols.sql`. The bodies are unchanged (their hashes are the same). Now the outcome engine (70) and every later view can read the clean bars.

### New `sql/bigquery/45_v_bars_clean.sql`
`symbol_bars_daily` with its recording errors taken out, plus a price `basis` per bar:
- **removed:** the bars of a `spike` (out and back within 3 bars), from its `out` jump up to the bar before its `back` jump. On 27 Nov: 8 bars on 7 symbols.
- **basis:** a new price basis starts on every jump of a `detour` (out and back after more than 3 bars: a stretch on another level, or a crash and a recovery; the archive cannot tell) and on every jump of 3 times or more that nothing undid (`step` or `too new`). On 27 Nov: 23 breaks on 10 symbols. A window partitioned by symbol and basis never spans one.
- **kept:** a jump under 3 times that nothing undid. A real move is possible, so the series stays whole.
- The view reads later bars on purpose (whether a jump came back). It removes recording errors; it never reads a result.

### `144_v_challenger_v0_features.sql` and `145_v_challenger_v0_ranks.sql`
- The features read `v_bars_clean` and run every window (the lags, the 63- and 126-session windows) inside one basis. An input whose window would span a break is left empty, so its rank is neutral, as for any missing input.
- The ranks measure the 22-session result inside one basis. A result that would span a break is left empty, like one beyond 45 days.
- The model, the ranks, the weights and the reading rules are unchanged. `146` and `147` are not touched.

### `scripts/apply_bq_views.py` 1.36.0 → **1.37.0**; `scripts/verify_deployment.py` pin.

## Re-read on 27 Nov (the whole chain run in BigQuery with the new bodies, beside the installed scorecard)
| Market | Variant | Rank correlation (old → new) | t | Top fifth net (pp) | Reading |
| --- | --- | --- | --- | --- | --- |
| HK | equal weights | 0.1439 → 0.1439 | 2.48 → 2.48 | +1.928 → +1.928 | CANDIDATE (forward test), unchanged |
| HK | walk-forward | 0.2379 → 0.2379 | 3.17 → 3.17 | +2.304 → +2.304 | CANDIDATE (forward test), unchanged |
| JP, OTHER | both | identical | identical | identical | NO EDGE, unchanged |
| SA | equal weights | 0.1175 → 0.1202 | 2.38 → 2.42 | −0.207 → −0.173 | NO EDGE, unchanged |
| SA | walk-forward | 0.0719 → 0.0747 | 1.26 → 1.29 | +0.133 → +0.141 | NO EDGE, unchanged |
| US | equal weights | 0.0055 → 0.0028 | 0.18 → 0.09 | +0.036 → +0.061 | NO EDGE, unchanged |
| US | walk-forward | −0.0062 → −0.0106 | −0.14 → −0.24 | −1.189 → −1.184 | NO EDGE, unchanged |

The 1st/99th-percentile clip and the rank labels of 1.34.0 had already contained most of the damage, so no reading moves. What changes is that a bad bar can no longer pick or veto a name in a book, from the first forward book on.

## Tests
- `tests/test_bq_views.py` `case_v71` (new): fragments of the clean view; no clock, no `LEAD`, the unrounded factor decides; the order (the bar check and the clean bars ahead of the outcome engine and of the challenger); the challenger reads only the clean bars and partitions every window by basis; the outcome engine and the forward ledger still read the raw archive (the next step).
- **The rule was run in BigQuery itself:** the bodies of `43_v_bar_jumps.sql` and `45_v_bars_clean.sql` were given 15 test series in place of the archive. The bars kept, the dates removed and the basis of every bar came back equal to the Python mirror pinned in the test. That includes a new series, too new at exactly 3 times, added after a hand mutant showed no series exercised that case.
- 8 hand mutants of the rule, each failing a test: the 3-times boundary, detours as breaks, detour bars removed, either end of a spike, the break counted from the next bar, steps under 3 times breaking, too-new jumps not breaking.
- `case_v69` (challenger) and `case_v70` (bar check): new sources, windows, file numbers; the planted look-ahead leaks are all still refused.
- Related tests (the 60 test files that name the views tool, the bar archive, the challenger or the deployment manifest): 1,651 pass, 35 skipped. `tests/test_bq_views.py` also passes with `GITHUB_ACTIONS=true`.

## Next (not in this change)
1. The outcome engine and the forward ledger read the clean bars too. 10 scored records span a jump today; their results run from −75 % to −40 %.
2. Freeze the challenger's weekly books in a dated table, appended and never rewritten, from the first forward book (Mon 30 Nov).
3. A line each morning when `v_bar_jump_symbols.is_new` is true.

## After the merge
Claude dispatches BigQuery views `--write` on main, then reads the scorecard back.

## Rollback
Revert the PR and run views `--write`. Nothing reads the challenger or the clean bars.

Not a licensed financial adviser; the owner decides and executes every trade.
