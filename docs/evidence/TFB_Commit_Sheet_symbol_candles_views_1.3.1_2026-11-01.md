# Commit Sheet: candles against the latest close (apply_bq_views 1.3.1), 2026-11-01

**Scope.** This is a follow-up to #884 for the one-symbol page and the candle charts the owner asked for on 31 Oct 2026 at 22:12.

The problem it fixes:
- The Looker Studio candlestick chart always draws its value axis from zero. Its style panel has no "start at zero" switch and no Missing-data option, and a log scale only partly helps.
- So a share at 25.92 SAR draws its candles squeezed into a thin band at the top of the chart.

The fix: the view also places every candle against the symbol's newest close, in percent points. The newest close is 0 and the candles straddle zero, so they fill the chart. The price line chart next to it keeps the prices themselves.

What the change does not touch:
- It is read-only for the workbook and the live system.
- No decision, gate, floor or executable count changes, and no broker call.
- Every other object stays byte-identical.

## Changes
| Change | Why | What | Tests | Installed | Live acceptance | Dependency |
|---|---|---|---|---|---|---|
| `sql/bigquery/60_v_symbol_candles.sql` | The candlestick is unreadable in prices. | Adds `last_close`, the symbol's newest close: `LAST_VALUE(close IGNORE NULLS)` over the symbol's whole history in date order. Also adds `open_vs_last_pct`, `high_vs_last_pct`, `low_vs_last_pct` and `close_vs_last_pct`, each `(value / last_close - 1) x 100`. | V20 (new); V1-V19 unchanged | Not installed | After `bq_views` write: the newest candle of every symbol is 0 | Owner merge |
| `scripts/apply_bq_views.py` 1.3.0 -> 1.3.1 | Version lockstep | Header block and `__version__` | V1-V20 | - | `bq_views` verify: every object matches | - |
| `scripts/verify_deployment.py` | Manifest pin | `apply_bq_views` 1.3.1 | `test_verify_manifest_pins.py` | - | - | - |
| `tests/test_bq_views.py` | Pins and the new check | The eight 1.3.0 objects that do not change move from PENDING to INSTALLED. `v_symbol_candles` stays PENDING under its new hash. | V4, V20 | - | - | - |

**Why the window spans the whole history.** A window with `ORDER BY` and no frame ends at the current row. Under that default, `LAST_VALUE` returns each row's own close, and every candle would read 0: a flat line. The window is named `wall` and reads `PARTITION BY m.symbol ORDER BY m.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

## Pins
- **Read-back (1 Nov 2026).** After bq_views write run 41278750795, the INFORMATION_SCHEMA hashes of all nine objects that 1.3.0 added or changed equal the reviewed files.
  - The eight that 1.3.1 leaves alone are now INSTALLED pins: `v_symbol_analyst`, `v_symbol_news`, `v_symbol_rating`, `v_dive_signals`, `v_dive_board`, `v_dive_events`, `v_top10_insight` and `v_portfolio_insight`.
  - `v_symbol_candles` was installed at `ed957281...`. Its new reviewed body hashes to `c30bd475ffd4bb38bdf2e60676d34c3c4181925e4a44b6b4b54c68b28f4caf93` (PENDING).

## Pre-merge checks
- **Live BigQuery run.** The new view body ran in full against the real archive on 1 Nov 2026, read-only:
  - 170,535 candles, the same count as the installed view, for 9,718 symbols;
  - the newest candle reads 0 for all 9,718 symbols;
  - 0 rows have no reference close;
  - 0 rows have high below low;
  - 0 rows differ from (close / last_close - 1) x 100;
  - 0 duplicate (symbol, session) pairs.
  - Saudi Aramco (2222.SR): the latest close of 25.92 SAR reads 0.0. Over its two-year history the candles span -11.11 % to +11.88 %, and over the last 130 sessions -3.94 % to +7.87 %.
- **Tests.**
  - 125 passed in the related suites: views (V1-V20), archive, symbol deep, market series, manifest pins, CI verdict and the two workflow audits.
  - Full local suite: 6,052 passed, 78 skipped. 65 failed and 30 errored, and the same 95 fail identically on a clean `origin/main` checkout: `fastapi` and `httpx` are not installed in this sandbox. CI runs them with the full requirements.
  - `audit_repository_workflows.py`: errors=0, warnings=0.
  - CI ruff rules (E9, F63, F7, F82): clean.
  - `apply_bq_views --dry-run`: 36 objects, static checks pass.
- **Golden negatives.** V20 fails on each of nine mutants:
  - the frame ending at the current row;
  - the default frame;
  - no partition by symbol;
  - a descending date order;
  - NULLs not ignored;
  - `high_vs_last_pct` computed from the close;
  - a column not in percent;
  - the open used as the reference;
  - `last_close` dropped.
  - V20 also fails on the 1.3.0 file.

## Post-merge order (agent)
1. Dispatch `bq_views` with mode=write. The write includes the verify step, which must report 36/36 objects matching.
2. In Looker Studio, refresh the fields of the `v_symbol_candles` data source.
3. Switch the deep-dive candlestick to `open_vs_last_pct`, `high_vs_last_pct`, `low_vs_last_pct` and `close_vs_last_pct`, with the log scale off. Then copy it to My Portfolio (holding picker) and Top 10 (board picker).

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
