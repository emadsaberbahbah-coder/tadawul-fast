# Commit sheet - the forward event scenarios and their scorecard (21 Nov 2026)

Branch `feat/event-scenarios-views`, against main, after #1049 (the economic calendar, merged 21 Nov 2026 14:30 UTC). Observe only: shown and logged, it controls no buy.

| File | Version |
|---|---|
| `scripts/apply_bq_views.py` | 1.20.0 -> **1.21.0** (5 views; `econ_calendar` and `event_scenario_log` are source tables) |
| `scripts/archive_econ_calendar.py` | 1.0.0 -> **1.1.0** (after each written run, the scenarios of the events ahead are logged) |
| `sql/bigquery/109_v_event_calendar.sql` | new |
| `sql/bigquery/110_v_event_reactions.sql` | new |
| `sql/bigquery/111_v_event_scenarios.sql` | new |
| `sql/bigquery/112_v_event_trial.sql` | new |
| `sql/bigquery/113_v_event_trial_summary.sql` | new |
| `scripts/markets_board.py` | 1.1.0 -> **1.2.0** (the next event, sections 7 and 8) |
| `.github/workflows/markets_board.yml` | 1.1.0 -> **1.2.0** (also runs after the economic calendar archive) |
| `scripts/news_macro_gate.py` | 1.4.0 -> **1.4.1** (the Fed's revised 2027 days) |
| `scripts/verify_deployment.py` | pins: apply_bq_views 1.21.0, archive_econ_calendar 1.1.0, markets_board 1.2.0, news_macro_gate 1.4.1 |
| `tests/test_bq_views.py` | V52-V56, V45 (file order to 113), five PENDING pins; the 37 pins read back on 21 Nov (run 44910876350, 76 of 76) move to INSTALLED |
| `tests/test_markets_board.py` | the event sections, the page before the views exist, M9 |
| `tests/test_archive_econ_calendar.py` | E11 (the scenario log); E6 and E10 for the second table and INSERT |
| `tests/test_news_macro_gate.py` | G13 (the 2027 schedule) |

No new test file, so `ci.yml` is unchanged.

## Why

The owner wrote on 21 Nov 2026 at 11:03 Riyadh (the build list after the voice call):

> "5. News: a news reading per market; forward event scenarios; momentum/surprise; influential figures of 19 Nov; score every news signal.
> 6. Accuracy: log and score everything. Observe for two weeks (10 sessions), then review with the owner, and switch on only what proved itself."

and, for all of it: "Everything starts in observe mode: shown and logged, but it controls no buys."

The design of 16 Nov 2026 asked for the same thing in more detail: for each big event, scenarios drawn from historical precedents, each with its probability, recorded before the outcome and scored after it (Brier score), and "the decision that stays acceptable in most scenarios".

#1047 built the news reading per market. #1049 archived the economic calendar with its history. This PR turns both into scenarios for the events ahead, and scores them.

## What is built

- **The events (109).** The policy meetings on their official dates, 2023-2027: the Fed (the 2027 days as the Fed revised them on 12 Nov 2026), the ECB, the Bank of Japan, OPEC+. And 15 release families from `econ_calendar`: US core CPI, core PCE, payrolls, retail sales, ISM manufacturing and services, GDP; euro area, Japan and China inflation; China's official PMI and GDP; Saudi inflation, PMI and GDP. Each figure is read against its forecast, in units of the family's own earlier misses (the last 36, 6 or more): above, in line or below at half a usual miss.
- **One event per release.** EODHD lists some releases twice, from two feeds ("Oct" and "Oct. 2026": US retail sales and Saudi CPI in November), and a first estimate with its revisions under the same period (US GDP: advance, second, third; a flash and its final). The period is read without its year, and the same period of a family within 120 days is one release: the earliest with a figure and a forecast.
- **The reactions (110).** For each event and each of the 11 markets of the Markets Board: the move from the last close before the event to the first close after it, each at its own close time, then over 5 closes, and over the 5 closes after the event close; against the market's normal daily move (250 closes).
- **The scenarios (111).** For each event, held or ahead, from the earlier events of its family only: the 10th, 50th and 90th percentile of the event-day move, the chance of a move of a normal day or more, the median move after a figure above or below its forecast, and the chances of the figure itself.
- **The trial and its scorecard (112, 113).** Each scenario against what came: did the move fall inside the band, did the big-move chance beat any day's (Brier), did the direction and the drift after the figure hold (a z against a coin), did the figure chances beat equal thirds. Forward from Monday 23 Nov 2026 00:00 Riyadh; the same rules read back on the archive are the backtest.
- **The Markets Board 1.2.0.** A NEXT EVENT line under the US line; section 7, the events of the next 14 days with their band per market, the move after a figure above or below, the big-move chance and the market that moved most, and the last 7 days against their band; section 8, the scorecard. Before the views exist the page is built all the same and says so.

## The scenario log (Codex P2 on the first push)

Codex found that the views rebuild every scenario from today's calendar: if a later read changes an earlier release (a revised figure or forecast, a release EODHD drops), the scenario of an event already scored changes with it, even on the rows marked forward. So the forward test would not score what was said before each event.

- `archive_econ_calendar` 1.1.0 appends, after each written run, the scenarios of the events of the next 21 days, as `v_event_scenarios` reads them on the calendar just loaded, to **`event_scenario_log`** (DAY partitions on `logged_at`): the forecast, the family's usual miss (`surprise_sd`, new in 109), and per market the band, the big-move chance, the medians after a figure and the figure chances. Three runs a day: a US release (13:30 UTC) is scored on the 04:50 log, China's PMI (01:30 UTC) on the evening before.
- `v_event_trial` scores a **forward** event on the last row logged before it (`scenario = 'logged'`, with `logged_at`); its figure is read against the forecast logged, in the usual miss logged. With none logged in time: `'not logged'`, nothing scored, and the scorecard counts it apart (`not_logged`; the board says so). The backtest stays rebuilt (`'rebuilt'`): a read-back, not a record.
- Checked in BigQuery with a stand-in log (each event since 1 Sep 2026 logged an hour before it, US payrolls left out): 43 events logged, their figures, forecasts and z identical to the calendar's; the 3 payroll events read 'not logged' with nothing scored. And on a hand-made trial: the scored and not-logged counts and the event-clustered z as computed by hand.

## Changes after the first read on the real history

The views were first validated on a stand-in calendar (the news gate's four weeks). On the loaded history, three things changed before review:

1. **The duplicate releases** above: without the rule, a release with its figure and its twin without one were two events, one of them "still to come".
2. **The z of an all-markets line counts each event once.** An event's markets move together, so 10 market rows of one event are not 10 coin tosses. The z is now the sum of (hit - 1/2) over the square root of the sum of its squares, each event's markets summed first (on a one-market line this is the usual (hits - n/2) / sqrt(n/4)). "Too few" is now counted in events (under 20). With rows counted as independent, Saudi CPI read "direction holds" (62.4 % of 157 rows, z 3.11); counted by event it is 15 events, z 2.09, too few.
3. **The figure chances are scored on releases only.** A rate decision is nearly always the forecast: the Fed's and the Bank of Japan's at 24 of 25 decisions since November 2023, the ECB's at all. Scored, they raised the figure skill of all families from -0.01 to +0.11 without telling anything.

## Evidence

The history: run 44914578389 (21 Nov 2026 14:33 UTC, `write=true`, from 2023-11-01): 27,886 releases of the US, the euro area, Germany, Japan, China and Saudi Arabia, 222 EODHD calls, the table created. The dry run before it (44914339498) listed every event name; the 15 families' names were confirmed on it (the ISM services index under both of its names, the Saudi PMI under both).

The releases per family (one per release, as above): 491, of them 10 still to come by 12 Dec. 7 of the 481 past releases have no figure in EODHD (among them US retail sales of April 2024, and the US core CPI of November 2025, of which EODHD has the yearly figure, not the monthly one); they stay out of the trial.

The first read, backtest (read-only queries of the five views inlined over the real table):

| | Value |
|---|---|
| Events, event-market rows | 544, 5,805 |
| The band held | 72.9 % of 4,342 rows (true band 80 %; a band read off 8 to 35 earlier events is narrower than the truth) |
| Big moves on event days / any day | 25.7 % / 25.2 % |
| Big-move skill against any day | -0.08 (the family's chance did not beat any day's) |
| Direction after the figure | 52.1 % of 1,502 rows over 148 events, z 1.38: not proven |
| Drift after the close | 52.4 % of 1,512 rows over 147 events, z 1.59 |
| Figure chances against equal thirds | -0.01 over 342 release figures |
| Verdict, all families | band calibrated, direction not proven |

By family the band ranges from 54.9 % (US GDP, 12 events) to 78.4 % (China PMI); the widest event-day moves against any day are the ECB's (33.1 % against 24.9 %) and US GDP's (40 % on 20 rows). No family has 20 events scored for direction yet (the most: US payrolls, 19).

## The forward test

- From Monday 23 Nov 2026 00:00 Riyadh, the same 10-session window as the market and news trials. In it: US core PCE (25 Nov), the OPEC+ meeting (29 Nov), China's PMI (30 Nov), euro area inflation and the US ISM (1 Dec), the Saudi PMI and the US ISM services (3 Dec), US payrolls (4 Dec).
- That is about 8 events by the review on 8 Dec: the forward lines will still read "collecting" (10 needed). At that review the backtest is the evidence; the forward read only shows that the rules ran as written. The Fed (9 Dec) and US CPI (10 Dec) come after it.
- The rules are frozen for the window: the families, the meeting dates, the thresholds (the band's 8 earlier events, the direction's 4, the figure's half of a usual miss, 20 events for a verdict) and the formulas.

## Safety

- Observe only. The live system never reads BigQuery. No score, pick, gate, order or variable changes.
- The board reads only the views and writes only `Markets_Board`. Its run log prints counts, never a symbol.
- The calendar loader's only DML: the INSERTs into `econ_calendar` and `event_scenario_log` (research tables). A dry run writes neither.
- The news gate's change touches only its 2027 FOMC days: nothing before 2027 moves.
- The broker stays read-only.
- Rollback: `TFB_MARKETS_BOARD=off` for the page; the views can be dropped without touching anything else.

## After the merge

1. `bq_econ_calendar.yml` with `write=true`: creates `event_scenario_log` (it logs nothing yet: the views are not there).
2. `bq_views.yml` in `write` mode: the five views (`v_event_trial` reads the log table, so step 1 comes first).
3. `bq_econ_calendar.yml` with `write=true` again: the first scenarios logged (the schedule does it three times a day after that). This must happen before the first forward event, US core PCE on 25 Nov 13:30 UTC.
4. The Markets Board shows sections 7 and 8 on its next run.

Not investment advice; the owner decides and executes every trade.
