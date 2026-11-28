# TFB Commit Sheet — daily_plan 1.13.0 + news_macro_gate 1.13.2 + views 1.40.0 [AUDIT-28NOV] (three confirmed findings of the external audit)

Date: 2026-11-28 (Saturday) · Lane: GitHub Actions (🗓️ Daily Plan, 📰 News & Macro Gate, 🗄️ BigQuery Looker Views) · Gate: no buy, sell, size, stop or level changes · No Render change

## Why
Owner, 28 Nov 2026 at 18:30 Riyadh, in Arabic, with an external audit of the same day attached ("TFB · production and monthly return review", 13 pages): a full review of the project to improve the return and the recommendations.

The audit's ten findings were checked against the code on main (973ae64) and the live runs. Three are confirmed, small, and stand in the way of measuring the December picks honestly. They are fixed here. The others are answered in the review doc.

| Audit finding | Checked | Result |
|---|---|---|
| 01 · a holdings list that was not read was published as "no flags" | run 46150899640 (28 Nov 13:42 UTC): `scope read failed (read My_Portfolio: HTTP 429)`, `scope 0`, `Names: no flags`, `wrote=True` | confirmed in production |
| 04 · the upside order was compared before the cash and held-sector checks | `daily_plan.would_pick`: "the day's sector cap alone (no cash or held-sector check)" | confirmed in code |
| 08 · the cuts of the promise thirds were worked out again on every read | `152_v_forecast_record.sql`, CTEs `fixed` and `cuts` | confirmed in code; the cuts had not moved yet |

## What
**`scripts/daily_plan.py` 1.13.0 [ARMS]:**
- One rule, `choose`, takes an order of candidates and returns what it buys: the picks count, the day's sector cap, the sector limit with holdings, then the cash. The plan and the upside arm both go through it.
- In observe, each name the upside arm would buy carries `[observe] upside arm: N share(s) at P (the plan's cash, sector and holdings checks)`. The label is frozen in the ledger with the plan, so the two arms can be scored as portfolios from the same cash.
- The run log counts the arm's names (no symbol, no quantity).
- The arm's list is, by construction, what `TFB_PLAN_RANKER=upside` would pick with the same cash.
- No pick, size, stop, order or page of the plan changes.

**`scripts/news_macro_gate.py` 1.13.2 [SCOPE-STATE]:**
- The scope read is asked again after HTTP 429, on the memory read's waits (at once, after 20 s, after 40 s). Any other failure is not asked again.
- A scope that could not be read is said: the `_News_Gate` row's Names Flagged cell, the digest's Names line and the run-log line read "NOT CHECKED - the list of names was not read (why)".
- The macro verdict is written as before. No verdict rule, tab or column changes.

**`sql/bigquery/152_v_forecast_record.sql`, `scripts/apply_bq_views.py` 1.40.0:**
- The 37 pairs of cuts (market, horizon) that 1.39.0 read on 28 Nov at 18:30 Riyadh are written into the view as constants. Nothing works them out again.
- A late correction of an old record can no longer move a band after the forward test began (calls from 29 Nov).
- The same day `TRACK_DEDUP_MATURED` went to enforce and 27 old records were expired. The cuts held (US one-month 2.6724 and 4.2143).

## Not changed
- The plan's picks, sizes, stops and waiting reasons; the ranker stays in observe (`TFB_PLAN_RANKER`).
- The news gate stays observe-only; its verdict rules and tabs.
- Every other BigQuery object; the view's columns and rows.
- No ENV, repository variable or Render setting.

## Evidence
- **The plan is unchanged.** 4,000 random cases (ranker engine, upside and observe; cash, picks count, sector caps, held-sector limit, entry level, prior picks) gave the same picks, quantities, entries, stops, waiting reasons and labels on 1.12.0 and 1.13.0, the new arm label aside. Differences: 0.
- **The 91 tests of 1.12.0 pass unchanged**, except three that pinned the old observe words.
- **New tests:** `test_r9` (the arm equals the upside switch's picks, share for share, in seven settings; less cash buys fewer names), `test_r10` (a name dropped for cash keeps the labels it earned, as in 1.12.0), `test_s1`–`test_s4` (one refusal is asked again; three refusals are said and never read "no flags"; another failure is not asked again; a read scope changes no word), `test_v2` (the cuts are constants).
- **Hand mutants on `choose` and the arm:** 13 of 13 killed.
- **The view returns the same rows.** The new text and the installed 1.39.0 view were compared in BigQuery before the change: 133 rows each, 0 rows in one and not in the other.
- `tests/test_daily_plan.py` + `tests/test_forecast_record.py`: 96 passed. `tests/test_news_macro_gate.py`: 343 passed. `tests/test_bq_views.py` + `tests/test_regime_shadow.py`: passed. Workflow audit: 0 errors, 0 warnings.

## After the merge
1. Dispatch 🗄️ BigQuery Looker Views on main with `write`, then `verify` (121 objects, the number unchanged).
2. Dispatch 📏 Forecast Record once and compare the tab's cuts with the constants.
3. Read the next 📰 News & Macro Gate run (version 1.13.2 in the row).
4. Read the next Daily Plan cut: the "ranker observe" line and the arm labels in the ledger.

## Rollback
`git revert` of this commit, then 🗄️ BigQuery Looker Views `write`. Nothing else holds state: the arm labels are text in the day's ledger rows.
