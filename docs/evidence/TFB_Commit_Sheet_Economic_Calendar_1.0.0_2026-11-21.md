# Commit sheet - the economic calendar of six economies, history included (21 Nov 2026)

Branch `feat/event-scenarios`, against main. It is stacked on #1047 (which carries #1048): its own work is the last commit. Observe only: it fills a research table and controls nothing.

| File | Version |
|---|---|
| `scripts/archive_econ_calendar.py` | new, **1.0.0** |
| `.github/workflows/bq_econ_calendar.yml` | new, **1.0.0** |
| `tests/test_archive_econ_calendar.py` | new, E1-E10 (18 tests) |
| `.github/workflows/ci.yml` | 1.1.93 -> **1.1.94** (the new test in the blocking lean lane) |
| `scripts/verify_deployment.py` | pin: archive_econ_calendar 1.0.0 |

## Why

The owner wrote on 21 Nov 2026 at 11:03 Riyadh (the build list after the voice call):

> "5. News: a news reading per market; forward event scenarios; momentum/surprise; influential figures of 19 Nov; score every news signal."

and, for all of it: "Everything starts in observe mode: shown and logged, but it controls no buys."

#1047 built the news reading per market, its momentum and surprise, the people and the scorecard. The forward event scenarios come next: for each release or meeting in the next two weeks, how each market has moved on the same event before, and after a figure above or below its forecast. That needs the history of every release with its forecast.

The archive does not have it. The news gate keeps the calendar of the US and Saudi Arabia for five days around each run, so `econ_events` holds 51 kinds of release since 26 Oct 2026: about four weeks.

This PR is the first half of PR-C2: the history. The views and the Markets Board sections are the second half. They are built on the real history once it is loaded, so they can be checked against it before review.

## What is built

**The loader, `archive_econ_calendar` 1.0.0**, reads EODHD economic-events (1 API call per request, whatever the window):
- six economies: the US, the euro area (EODHD code EU), Germany, Japan, China, Saudi Arabia;
- windows of up to 31 days. Each request asks for one day more on each side, because EODHD does not say whether its bounds are inclusive; the overlap is removed by the release key;
- 1,000 releases a page. A full first page brings a second (offset 1,000, EODHD's largest), and a window whose second page is full too is cut in two;
- every field the provider gives: the release time (UTC), the country, the event, the comparison (m/m, q/q, y/y), the period, the figure, the forecast, the previous figure, the change and the change %;
- the key is the news gate's own (`time|country|event|comparison|period`), built the same way. Test E1 checks it against `news_macro_gate.parse_econ_events`.

**The table, `econ_calendar`, is a change log**: one row per release per change, partitioned by month of the release and clustered by country and event. The releases read are loaded into `econ_calendar_stage` (replaced each run). One INSERT then appends:
- `new`: a release not in the table yet;
- `changed`: a release whose time, figure, forecast, previous figure or change differs from its latest row;
- `gone`: a release that EODHD no longer lists (moved or withdrawn), inside a window read this run.

A release that did not change appends nothing. So the table records what was known when: the forecast before the figure, then the figure. A window that failed, or came back empty, marks nothing gone.

**The workflow, `bq_econ_calendar.yml` 1.0.0**:
- runs at 04:50 UTC daily, and at 14:10 and 21:30 UTC on weekdays (after the Asian and Saudi releases, the US 8:30 releases and the ECB, and the Fed and the US close);
- a scheduled run reads one window, 3 days back to 21 days ahead: 6 calls;
- a manual dispatch is a dry run unless `write` is ticked. A history fill is `write=true`, `start=2023-11-01`: about 230 calls.

## Checked before the push

The INSERT was run in BigQuery on stand-in tables (a read-only query: no table was created). Given 7 stored releases and 5 read:
- the figure that arrived came back `changed`;
- the release missing from a read window came back `gone`;
- the release that had gone and came back is `new`, and the never-seen one is `new`;
- the unchanged releases, the one outside the window and the one in another country appended nothing.

Seven deliberate faults were each caught by the tests: no one-day margin, an empty window counted as read, the older copy of a release kept, no retry, no window split, the budget guard ignored, NaN accepted as a figure.

## Safety

- Observe only. The live system never reads BigQuery. No score, pick, gate, order or variable changes.
- The EODHD daily limit is shared with the live sync. The run reads `/api/user` first (free) and is skipped when today's calls plus its own would pass 60 % of the limit (`TFB_ECON_EODHD_MAX_SHARE`), or when the limit cannot be read. This is `archive_symbol_deep.budget_check`, the same function.
- `--max-calls` (default 400) caps a run. A window past the cap is not read and the run fails, so the gap shows.
- The token never reaches a log line (test E10). The log prints counts and event names only: economic releases are public figures.
- The only DML is the INSERT into `econ_calendar`. Nothing is deleted or rewritten.
- Rollback: the repository Variable `TFB_BQ_ECON=off` stops the job. The table can be dropped without touching anything else.

## Tests

`tests/test_archive_econ_calendar.py`, 18 tests:
- E1: the parsing, and the gate's key;
- E2: the windows;
- E3: the request shape, the second page, the split, the retry, the cap;
- E4: the budget guard;
- E5: the dry run, a failed window, the cap;
- E6: the write path (table, stage, INSERT, coverage);
- E7: the INSERT text and the table layout;
- E8: the configuration;
- E9: the workflow;
- E10: no workbook, one DML, no token.

With the scans of every workflow and script (runner pin, manifest pins, secret redaction, CI verdict) and the suites of the market series, deep data and news gate modules it builds on: 436 passed, 3 skipped.

## After the merge

1. Dispatch `bq_econ_calendar.yml` once as a dry run with `start=2023-11-01` and `list_types=true`. This shows which event names each economy has, and whether EODHD answers for the euro area.
2. Then dispatch it with `write=true` and `start=2023-11-01`: the history fill.
3. The schedule keeps it current from then on. No Variable needs to be set.
4. PR-C2b builds the views and the Markets Board sections on this table.

## Known limits

- **EODHD is the only source of the forecasts.** Its consensus is what it shows. The release times are as it gives them (UTC).
- **The euro area's code is not documented.** If EODHD does not answer for `EU`, Germany stands for Europe, and the ECB's meetings come from the ECB's own calendar in PR-C2b.
- **The policy meetings are not releases.** The Fed's, the ECB's and the Bank of Japan's decisions are in the calendar with their forecasts. OPEC+ meetings are not; PR-C2b takes their dates from OPEC's own statements.
- **A gone release is a provider's word, not a fact.** A release EODHD drops and later lists again comes back as `new`.

Not investment advice; the owner decides and executes every trade.
