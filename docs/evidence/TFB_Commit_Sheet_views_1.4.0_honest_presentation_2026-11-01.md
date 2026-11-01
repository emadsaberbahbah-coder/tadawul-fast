# Commit Sheet: honest presentation on the Looker pages (apply_bq_views 1.4.0), 2026-11-01

**Scope.** On 1 Nov 2026 at 10:36 Riyadh the owner asked for a full review of GitHub, Render, the workbook, BigQuery and Looker Studio, with the required updates "regarding data accuracy, improving content, avoid any data presentation interpretation, continuous presenting recommendation, avoid misleading, improve design and presentation".

This PR is the data-layer part. The review found four places where a correct number was presented in a way that could mislead a reader. Each is fixed here as a view column, so every page draws the same words from the same place. It is read-only for the workbook and the live system:
- no decision, gate, floor or executable count changes;
- no broker call;
- no engine change: the rating, the board and the holdings are read as the sheets publish them.

## What was misleading, and the fix
| Finding (live, 1 Nov) | Why it misleads | Fix | View |
|---|---|---|---|
| My Portfolio shows "Value SAR 9,611" and "P&L SAR -461" over a two-holding book worth 19,459 SAR. The table's Value column mixes 9,847.9 SAR (the sukuk) and 2,563.02 USD (FRST) with no currency. | The decision page carries only the equity, so the page totals omit the sukuk; the sheet's position value is in each holding's currency. | `value_sar`, `cost_sar`, `pnl_sar`, `pnl_sar_pct`, `weight_sar_pct` for every holding: the decision page's figure when it has the row, else the sheet's value times the riyal rate. `value_source` names which. Live: 9,847.9 + 9,611.0 = 19,458.9 SAR, P&L -613 SAR, weights 50.6 / 49.4 % (the sheet's own Actual Weight agrees). | v_portfolio_insight, new v_fx_sar |
| FRST.US carries a TFB rating of 75.9, "Very positive", 5 stars, while the engine says WATCH and the decision page says BLOCK. | A rating is a weighing of evidence; the gate is the decision layer's verdict. Shown alone, the stars read as a buy. | `rating_gate` and `tfb_rating_display` ("Very positive - engine: watch only"), plus `analyst_source` and `news_source`. Live: of 9,788 rated names, 1,775 Positive or better sit on names the engine only watches and 867 on names it says not to invest in; 96 names have no gate. | v_symbol_rating, both insight views |
| The Top-10 board lists ten names with "Engine call BUY" on every row and "null" for half the ranks, while the board is QUALIFIED_UNFUNDED: no ticket, no stop, no target, five seats in grace. | A reader sees ten buys. The board is research; nothing is sized. | `seat_kind` ("ranked seat 1", "grace, no seat", "new, no seat yet") and `allocation` ("none - research board", or the ticket in riyals). | v_top10_insight |
| The track-record panels show a win rate with no denominator. 30.5 % of due Performance_Log records (5,356) have not matured; the oldest overdue target is 3 Oct; the 3M horizon is 41.9 % matured. | A win rate over a partial, non-random subset reads as the whole record. | `v_outcome_coverage`: per horizon and ALL, due / matured / scored / wins, `win_pct` over decided calls only, `matured_share_pct` over due records, `overdue_open`, the oldest overdue target. | new v_outcome_coverage |

**v_fx_sar.** One row per currency from the latest Commodities_FX page: USD from `SAR=X` (3.7526 on 1 Nov), every other currency through its dollar cross (`CCYUSD=X`, else the inverse of `CCY=X`, direct first), pence (GBX, GBp) as a hundredth of a pound. 29 currencies live; JPY has no dollar cross on the page and so no row (a JPY holding would show no riyal value rather than a wrong one, with `value_source` = 'no rate for the currency').

## Changes
| Change | Tests | Installed |
|---|---|---|
| NEW `sql/bigquery/57_v_fx_sar.sql` | V21 | Not installed |
| NEW `sql/bigquery/69_v_outcome_coverage.sql` | V23 | Not installed |
| `63_v_symbol_rating.sql`: rating_gate, tfb_rating_display, analyst_source, news_source | V18, V22 | Not installed (1.3.0 body live) |
| `67_v_top10_insight.sql`: seat_kind, allocation, the gate and sources | V22 | Not installed (1.3.0 body live) |
| `68_v_portfolio_insight.sql`: riyal values, weight, value_source, the gate and sources | V22 | Not installed (1.3.0 body live) |
| `scripts/apply_bq_views.py` 1.3.1 -> 1.4.0; `verify_deployment` pin | V1-V23, manifest pins | - |
| `tests/test_bq_views.py`: 38 objects, V21-V23, pins | - | - |

Pins: `v_symbol_candles` (1.3.1, c30bd475) and the five objects above are PENDING until the next `bq_views` write; the eight unchanged 1.3.0 objects stay INSTALLED.

## Pre-merge checks
- **Live BigQuery runs (read-only, 1 Nov, 11:00-11:40 Riyadh).** Every new body ran in full against the real archive:
  - v_fx_sar: 29 currencies; USD 3.7526, GBP 5.0259, GBX 0.0503, HKD 0.4784 (direct quote preferred over HKD=X), EGP 0.0671 (inverse only).
  - v_symbol_rating: 2222.SR "Positive - engine: watch only", sheet analyst rating, no news coverage; FRST.US "Very positive - engine: watch only", EODHD consensus of 4 analysts, 11 news items; ITRN.US and KB.US gate none.
  - v_portfolio_insight: the two holdings above; total 19,458.9 SAR.
  - v_top10_insight: five ranked seats, five "grace, no seat", all "none - research board".
  - v_outcome_coverage: 1W 73.6 % matured (win 57.2 %), 2W 70.9 % (55.3 %), 1M 77.2 % (57.4 %), 3M 41.9 % (41.0 %), ALL 69.5 % (55.0 %); 5,356 overdue open records, oldest target 3 Oct.
- **Tests.** 128 passed across the related suites (views V1-V23, archive, symbol deep, market series, manifest pins, CI verdict, the two workflow audits). `audit_repository_workflows.py`: errors=0 warnings=0. CI ruff rules: clean. `apply_bq_views --dry-run`: 38 objects, static checks pass.
- **Golden negatives.** 18 mutants, each fails the suite: pence as pounds; the inverse quote preferred; the inverse read as direct; crypto rows counted as FX; the gate reading the score; the watch gate dropped; the floors gate inverted; the display without its gate; label cuts that disagree between label and display; the value left in local currency; the weight taken over the decision rows only; the FX join on a fixed currency; a ticket-less row reading as an order; grace reading as a seat; the win rate over matured rows; due ignoring the target date; no ALL row; overdue counting matured rows. V22 also fails on the three 1.3.1 files.

## Post-merge order (agent)
1. Dispatch `bq_views` with mode=write (a fresh dispatch, outside 10:00-15:00 Riyadh and never during a sync). The write includes the verify step: 38/38 must match.
2. Refresh the fields of the changed data sources in Looker (v_symbol_rating, v_top10_insight, v_portfolio_insight) and add v_fx_sar and v_outcome_coverage.
3. Wire the pages: My Portfolio totals from `value_sar` / `pnl_sar`, the table's riyal columns and `tfb_rating_display`; the Top-10 board's `seat_kind` and `allocation` with a research-board banner; the track-record panels with `matured_share_pct` and `overdue_open` beside the win rate; the deep-dive rating cards with the gate.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
