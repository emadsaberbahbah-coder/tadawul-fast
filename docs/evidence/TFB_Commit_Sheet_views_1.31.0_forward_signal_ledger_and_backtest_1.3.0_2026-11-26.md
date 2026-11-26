# TFB Commit Sheet — apply_bq_views 1.31.0 (forward signal ledger, gate record, reconciliation) + tfb_backtest 1.3.0 + learning_watch 1.2.0

Date: 2026-11-26 (Thursday) · Lane: BigQuery research views and the offline backtester · Gate: none (research only: no decision, gate or count reads these views; the backtester is read-only) · No Render deploy · No order is placed, changed or cancelled

Stacked on PR #1085 (views 1.30.0). Merge #1085 first.

## Why
Week plan of 23 Nov 2026, package 4 (owner, 23 Nov: "go and Start build and daily in the moring start to build the remaing"):

- (a) the backtester must not be able to earn its score from later rows;
- (b) every signal is set against the benchmark of its own market after cost, the names the gates kept out are followed beside the names they let in, and the ledger's counts are held to the outcome engine's.

PR #1082 (tfb_backtest 1.2.0, merged 26 Nov) already replaced the shuffled 5-fold verdict with a purged walk-forward and planted the entry-day leak test. This change finishes (a) and builds (b).

## Change
| # | File | What it holds |
|---|---|---|
| 137 | `v_forward_signals` | One row per signal and horizon (the tracker's Key), from `v_outcome_engine`: provenance, entry, gate (`admitted` / `kept out`) and block reason class, the benchmark of its own market over the same dates, `active_net_pp` (return above the benchmark less 0.50 points of cost), `beat_benchmark`, `call_right`, `status`, `period`. HOLD records are kept and get the long-side outcome the engine does not give them. |
| 138 | `v_forward_signal_gates` | Per period, market, horizon, gate and block reason: signals, outcomes, winners, losers, `blocked_winners`, `admitted_losers`, mean return above the benchmark net of cost, `t_adj`, and a reading fixed on 26 Nov 2026. |
| 139 | `v_forward_signal_recon` | One row: the ledger's rows, keys and scored count against the engine's, state and return differences, verdict `reconciled` / `DIFFERENCE`. |
| — | `scripts/apply_bq_views.py` 1.30.0 → **1.31.0** | header only; no file of 1.30.0 changes |
| — | `scripts/tfb_backtest.py` 1.2.0 → **1.3.0** | quintiles cut per formation date; every output named descriptive or out-of-sample; the train-to-test gap reported |
| — | `scripts/learning_watch.py` 1.1.0 → **1.2.0** | the gate record joins the registry (22 read) |
| — | `scripts/verify_deployment.py` | pins `apply_bq_views 1.31.0`, `learning_watch 1.2.0` |
| — | `docs/evidence/backtest_h29_*.json` (2), `docs/hypotheses/TFB_Hypothesis_H28_*`, `_H29_*` | labelled descriptive; no figure changed |

### The ledger (137)
- **Rows.** The engine's cohort row when the Key was scored, else the Key's first record, in the engine's own order. Nothing is dropped after that pick: open, unscorable and HOLD records stay, each with a status.
- **Benchmark by market.** US → SPY, total return. Tadawul → ^TASI.SR, **price only**: the index series pays no dividends, so a Saudi name's total return is flattered by about the index's dividend yield over the horizon. Any other market → ACWI in USD, total return — a rough yardstick for a local-currency return. `bench_basis` says which on every row.
- **Benchmark dates.** From the last benchmark session that had **closed when the record was made** (16:00 New York, 15:00 Riyadh — the engine's own venue table) to the benchmark's last close on or before the target bar. See "Found" below for why not "the close of the recording day".
- **Cost.** 0.50 points a round trip: 0.10 commission + 0.15 spread a side, as `v_hold_vs_switch` (the commission is the measured 0.094 % of the owner's fills; the spread is an allowance).
- **Beat and right.** `beat_benchmark` is the long side whatever the stance: did buying it beat buying the market, after cost? `call_right` reads the stance: a bullish signal is right when it beat the benchmark net of cost, a bearish one when it trailed the benchmark.
- **Gate and reason.** `admitted` = entry investability INVESTABLE; `kept out` = WATCHLIST or BLOCKED. The reason is the nightly market page's Block Reason for the symbol on the recording day, cut at the first `:`, `(` or long dash, taken only when that page shows the record's own tier; otherwise `(reason not archived)` — the pages are archived from 31 Oct 2026.
- **`outcome_state`** is the engine's `engine_state` test without the stance line (a test pins the two CASE blocks line for line), so for a bullish or bearish record the two are equal and `stock_roi_pp` equals `engine_roi_pp`.

### The backtester (tfb_backtest 1.3.0)
- **Quintiles per formation date.** v1.2.0 cut a numeric signal into quintiles of all rows pooled (refitted on each fold's training rows for the walk-forward, but pooled over dates). A score whose level moves with the market then fills a quintile with whole weeks: the table reads the market, not the choice between names, and a pooled cut point is made of later rows too. A row's quintile is now its rank among the rows recorded the same day (five or more values that day; fewer → the row is left out and counted in `rows_without_date_quintile`). No value of another day can enter it. The day's rows are **all the records made that day** (`formation_records`: one per Key, in the same horizon, filter and date scope, with no test of status or outcome), not only the ones that later matured as WIN or LOSS — who else was on the list is known at formation, which of them matured is not (Codex P2 on #1086: cut among the decided rows only, a record's quintile moved when a same-day record matured). The console and the JSON say how many records the cut was made among (`quintile_universe`, `formation_rows`). When no day has five values the pooled cut is used and `bands` says so. Rollback: `--quintiles pooled` or `TFB_BACKTEST_QUINTILES=pooled`.
- **Descriptive or out-of-sample.** Each result carries `evidence` (its keys listed as `descriptive` or `out_of_sample`), `verdict_basis` and `bands`; the console tags every line; the JSON carries `output_legend`. Only `wf_brier`, `wf_base_brier` and `wf_gain_vs_base` are out-of-sample. The group table, the win spread and its z, the pooled Spearman, the raw Brier and the legacy random K-fold are descriptive.
- **The purge, as measured.** `wf_min_gap_days` is the smallest distance, over the folds, between the last training entry and the first test day; `wf_embargo_days` is the flag. A training row is usable only once its own horizon (plus the 3-day availability pad) has passed, plus the embargo: 11 days for 1W, 34 for 1M.
- **Stored outputs labelled.** The two H-29 JSON files gain `evidence_class: descriptive` and a note; the H-28 and H-29 pages gain a status line. They were produced by v1.0.0 / v1.1.0 with the random K-fold and pooled quintiles. H-29a and H-29b stay proposals: neither may be armed on those numbers.

## Reconciliation (run on the archive, 26 Nov, with the SQL of these files)
| engine rows | engine keys | ledger rows | ledger keys | engine cohort | ledger scored | difference | state differences | return differences | verdict |
|---|---|---|---|---|---|---|---|---|---|
| 34,057 | 34,030 | 34,030 | 34,030 | 7,601 | 7,601 | 0 | 0 | 0 | **reconciled** |

Scored with a benchmark 7,601 · scored without one 0 · HOLD records with an outcome 11,292 · open 10,049 · rows with an outcome and a benchmark 18,917.

## First reading (history — in-sample, context, not evidence)
Records made before 26 Nov 2026, long side, against the benchmark, after 0.50 points of cost. "Winners" beat the benchmark net of cost.

| Market | Horizon | Admitted: outcomes | winners | **admitted losers** | mean | Kept out: outcomes | **blocked winners** | share | mean | t (kept out) |
|---|---|---|---|---|---|---|---|---|---|---|
| US | 1W | 564 | 271 (48.0 %) | 293 | −0.13 | 2,634 | 1,123 | 42.6 % | −0.68 | −1.25 |
| US | 2W | 504 | 261 (51.8 %) | 243 | +0.96 | 2,406 | 1,047 | 43.5 % | −0.97 | −0.86 |
| US | 1M | 427 | 242 (56.7 %) | 185 | +1.80 | 2,488 | 1,143 | 45.9 % | −0.12 | −0.06 |
| US | 3M | 160 | 66 (41.3 %) | 94 | −0.22 | 974 | 308 | 31.6 % | −4.12 | −0.78 |
| Tadawul | 1W | 173 | 79 (45.7 %) | 94 | −0.93 | 1,792 | 667 | 37.2 % | −0.88 | −3.28 |
| Tadawul | 2W | 171 | 79 (46.2 %) | 92 | −1.58 | 1,633 | 621 | 38.0 % | −1.14 | −2.00 |
| Tadawul | 1M | 203 | 87 (42.9 %) | 116 | −2.33 | 1,528 | 607 | 39.7 % | −1.32 | −0.86 |
| Tadawul | 3M | 97 | 58 (59.8 %) | 39 | −1.50 | 627 | 317 | 50.6 % | −1.48 | −0.08 |
| Other | 1W | 157 | 100 (63.7 %) | 57 | +1.13 | 589 | 254 | 43.1 % | −0.47 | −0.40 |
| Other | 2W | 150 | 117 (78.0 %) | 33 | +2.06 | 579 | 309 | 53.4 % | −0.14 | −0.34 |
| Other | 1M | 95 | 71 (74.7 %) | 24 | +3.55 | 697 | 393 | 56.4 % | +0.30 | −0.19 |

Means are in points above the benchmark after cost. No admitted row reaches t 2 either way (the highest is +0.79, Other 2W; the lowest −1.04, Tadawul 1W).

By reason (archived from 31 Oct only, so 17 recording dates at most — thin):

| Market · 1W | Reason | Outcomes | Blocked winners | Mean |
|---|---|---|---|---|
| US | Engine neutral | 211 | 104 (49.3 %) | −0.48 |
| US | Conservative gate | 197 | 97 (49.2 %) | −1.02 |
| US | Engine recommends REDUCE | 100 | 42 (42.0 %) | −0.08 |
| US | Engine recommends SELL | 41 | 30 (73.2 %) | +1.95 |
| Tadawul | Engine neutral | 238 | 102 (42.9 %) | −0.42 |
| Tadawul | Conservative gate | 71 | 23 (32.4 %) | −0.89 |
| Tadawul | Engine recommends SELL | 40 | 13 (32.5 %) | −0.40 |
| Tadawul | Engine recommends REDUCE | 30 | 4 (13.3 %) | −1.48 |

What it says, and what it does not:
- **US and other markets:** the admitted names did better than the names kept out at 1W, 2W and 1M (US 1M: +1.80 against −0.12 points). The gate separated in the right direction, but not clearly (no t at 2), and it is not free: about 43–46 % of the US names kept out still beat SPY after cost, and about half of the admitted ones did not.
- **Saudi:** the admitted names did **not** do better than the names kept out. Both trailed the index after cost at 1W, 2W and 1M, the admitted ones by slightly more. The names kept out trailed clearly at 1W and 2W (t −3.3, −2.0) — keeping them out was right — but being admitted added nothing. The Saudi benchmark is price only, which works in the names' favour, so the shortfall is if anything understated. This agrees with the confidence finding of 24 Nov (Saudi HIGH confidence inverted).
- **US SELL-rated names** kept out beat SPY in 30 of 41 cases over one week (17 dates, t 1.0): a small, thin sample from three falling weeks — noted, not a finding.
- All of it is history: the reading says `in-sample, not evidence` for every row recorded before 26 Nov 2026. The forward rows start today and read `collecting` until 30 outcomes on 8 dates.

## Found while building — not changed here
1. **The entry price is the previous close, not the recording day's.** For US records made between 00:00 and 05:59 Riyadh on a day that has a bar, the entry price equals the close of the bar **before** in 1,632 of 1,713 cases; Saudi records made before the session closes show the same. A benchmark anchored on "the last close on or before the recording date" therefore starts one session after the stock's entry. The ledger anchors on the last session closed when the record was made. Effect on the means, old anchor → new: US −0.22 → −0.24, Tadawul −1.44 → −1.33, Other +1.05 → +1.08 — small, no sign.
2. `v_confidence_calibration` (125) uses the older anchor. Same small shift; to be aligned in the next views bump, with any other view that sets a record against a market series. Backlog.
3. Records made while their market is open (about one US record in nine, 18:00–23:59 Riyadh) carry an intraday entry price and are still set against the benchmark's previous close. No intraday benchmark is archived. Stated in the view's header.
4. 4,648 US rows read `not scorable: no_bar_for_symbol`: HOLD records of symbols outside the 1,110 with archived daily bars. They stay in the ledger with that status.

## Tests
| File | What it pins |
|---|---|
| `tests/test_bq_views.py` (`case_v67`, +3 pins, order, kinds, positions) | the three files, their sources and the 1.31.0 mark; one row per Key in the engine's order; the gate and the reason rule; SPY total return, ^TASI.SR price only, ACWI; the benchmark anchored on `closes_at <= recorded_at` and never on a later close of the recording day; closing times equal to the engine's venue table; `outcome_state` equal to the engine's CASE line for line, less the stance; the cost equal to `v_hold_vs_switch`'s; every status; winners, losers, blocked winners, admitted losers; `t_adj`; the five readings; the reconciliation's five conditions |
| `tests/test_learning_watch.py` (+5 cases) | 22 records; each reading of the gate record lands in its group forward and IN-SAMPLE for history; the registry filter names rows the views write |
| `tests/test_tfb_backtest_walk_forward.py` (+9 cases) | a date quintile ranks inside its own day, ties share one, thin and undated rows get none; **a planted look-ahead**: under the pooled cut the same day-0 rows are the bottom quintile or the top one depending on a later day's values — cut per formation date the later day changes nothing; **a planted selection** (Codex P2): cut among the decided rows only, the same record is the top quintile while five of its day's ten records have matured and the middle one once all ten have — cut among the formation records it is the middle one both times; `formation_records` keeps every Key once in the same scope; the command line passes the formation set; a market-level score separates only under pooled quintiles; flag over env, fallback on thin days; the train-to-test gap covers the horizon; every key of a result is named descriptive or out-of-sample and every console line is tagged |
| `tests/test_tfb_backtest_rank_evidence.py` | golden dict gains the new keys; version 1.3.0; `output_legend` in the JSON. All earlier values unchanged |
| `scripts/tfb_backtest.py --selftest` | 10/10 |

Local: `pytest tests/test_bq_views.py tests/test_learning_watch.py tests/test_verify_manifest_pins.py tests/test_tfb_backtest_walk_forward.py tests/test_tfb_backtest_rank_evidence.py` → 177 passed. `apply_bq_views.py --dry-run` → 107 objects, static checks clean.

## After merge (Claude, on main)
1. Dispatch `bq_views` write, then `--verify` (three new objects; nothing else changes).
2. Read `v_forward_signal_recon`: the verdict must be `reconciled`.
3. Learning Watch reads the gate record on its next daily run (about 1.1 GB a read).

## Rollback
- Views: research only — drop the three views, or leave them; nothing reads them but Learning Watch, which reports a missing record without failing.
- Backtester: `--quintiles pooled` / `TFB_BACKTEST_QUINTILES=pooled` restores the v1.2.0 cuts; `--cv legacy` restores the v1.1.1 verdict.
- Learning Watch: remove the registry entry, or `TFB_LEARNING_WATCH=off`.

Not investment advice. Research measurement only.
