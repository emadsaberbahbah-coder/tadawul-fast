# TFB Commit Sheet — archive_to_bigquery 1.2.0 · apply_bq_views 1.32.0 [NEWS-LLM-BQ] (the news read twice, scored against what the prices did next)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (BigQuery archive, nightly; BigQuery views on dispatch) · No gate, page or decision changes · No Render deploy · Research views only

## Why
Owner, 26 Nov 15:34 Riyadh (in Arabic): "let us start with this too and see how we can develop the system — linked with BigQuery and the other files". PR 1093 (merged 13:00 UTC) made a language model read the news gate's headlines beside the word lists, in observe mode, into the tab `_News_LLM`. A second reading is worth keeping only if it says more about the next sessions than the first. This change keeps every day's labels and scores both readings the same way.

First live read (dry run on main after the merge, 13:01 UTC): 107 of 120 headlines labelled. Against the word lists: same 7, direction differs 12, event differs 8, the model alone found an event 35, the lists alone 5, neither 40. That shows how often the two readings differ, not which one is right. These views measure which one is right.

## What
**`scripts/archive_to_bigquery.py` 1.1.0 → 1.2.0** — one more tab in `TABS`: `("_News_LLM", 1)` (header row 1) → table `news_llm`. Both the nightly writer and the coherent publisher iterate `TABS`; `ensure_table` creates the table on first use.

**`scripts/apply_bq_views.py` 1.31.0 → 1.32.0** — `news_llm` joins `ARCHIVE_TABLES`. There are four new views. No earlier file changes.

| File | View | What it gives |
|---|---|---|
| 140 | `v_news_llm` | One row per labelled headline, the latest copy. Holds both readings side by side and how they compare. A reading with no event counts as direction 0. `news_date` is the Riyadh date of the headline. |
| 141 | `v_news_llm_days` | Each reading summed per name-day (US names) and per news day (MACRO). Class: positive ≥ 1, negative ≤ −1, else neutral. Each day is set against the next 1 and 5 sessions. |
| 142 | `v_news_llm_eval` | The scoreboard of each reading alone, per reader × kind × class, with a fixed reading. |
| 143 | `v_news_llm_duel` | The days the two readings part (`opposite`, `model only`, `rules only`, `agree`) and which one was right over 5 sessions. |

**Method** (the same as `v_company_news_eval`, so the scoreboards compare):
- **Entry:** the close of the first session **after** the news day, within 6 calendar days. The news day's own close could come before the headline, so it is never used (no look-ahead).
- **Name:** the adjusted return minus the same-day average of every US name.
- **MACRO:** SPY's own adjusted return, because the MACRO direction is the effect on equity markets.

**Readings (fixed today; never "proven"):**
- **eval:** `NOT MEASURABLE (one side)`, then `BIASED (one side > 90 %)`, then `THIN` (the smaller side has fewer than 30 matured 5-session days), then `NO EDGE` (corr ≤ 0.05, or positive not above negative), else `CANDIDATE (forward test)`. THIN is checked before any edge call: on a few weeks of days, a "no edge" call is as noisy as an edge.
- **duel:**
  - `opposite`: THIN under 20 days; `MODEL AHEAD` when the model is right on ≥ 60 %; `RULES AHEAD` when the word lists are right on ≥ 60 %; else `LEVEL`. Each reader is judged on its own share, because a day with no move is right for neither (Codex P2, fixed).
  - `model only` / `rules only`: THIN under 30 calls; `ADDS` when right on ≥ 55 %; `NOISE` at ≤ 45 %; else `UNCLEAR`.

## Verified
- **Run on BigQuery itself** (read-only query, 26 Nov): the four bodies, chained over 30 synthetic `_News_LLM` rows against the real `symbol_bars_daily` and `market_series_daily`.
  - All four compile and return rows.
  - `Read At (Riyadh)` 16:42:10 is read as 13:42:10 UTC.
  - A Saturday MACRO day enters on the Monday close.
  - The Saudi name is left out.
  - The classes, `went_its_way`, the duels and the readings come out as specified.
- `tests/test_bq_views.py`: 68 pass. New `case_v68` checks:
  - every column of the reader's `HEADER` is read by name;
  - the references;
  - the entry and peer rules;
  - the class and reading thresholds;
  - THIN comes before the edge call;
  - no clock, and no look forward beyond the four target `LEAD`s.

  Body hashes are pinned in `PENDING`, and the relative positions of the earlier views shift by 4.
- `tests/test_bq_archive.py`: new A8, which builds a `_News_LLM` row, checks `record_json` by header and the 18-tab list. Together with `test_archive_coherent_snapshot.py` and `test_news_llm_reader.py`: all pass.
- `verify_deployment.py` pins: archive_to_bigquery 1.2.0, apply_bq_views 1.32.0.

## Order (why this waits for the tab)
The coherent nightly capture reads every tab in `TABS` and refuses the whole capture if one is missing. The tab is created by the gate's first written run after 1.8.0 (the run chained after the 13:41 UTC Exit Plan). This PR is opened only after that tab is confirmed to exist.

## After merge (Claude)
1. Dispatch `bq_views` with write on main to create the four views. `--verify` must then find the pinned bodies.
2. The nightly archive (23:05 UTC) makes the first `news_llm` partition. Read `v_news_llm` the next morning.
3. The 10-session review (8 Dec) reads `v_news_llm_eval` and `v_news_llm_duel`. Most readings will still say THIN then: the 30-day floor is deliberate.

## Rollback
Revert this change. The views are research only; the archive table can stay (a partition per day, nothing reads it).

Not a licensed financial adviser; the owner decides and executes every trade.
