# Commit Sheet - `scripts/news_board.py` v1.1.0 [EVENTS] and `data/news_event_calendar.json` 1.0.0 - 2026-11-19

**Scope:** owner, Thu 19 Nov 2026, 16:09 Riyadh: "Next" - part 3 of the stage: the calendar of summits and deadlines.

**Files:**
- `data/news_event_calendar.json` (new, 1.0.0): 14 dated events from 18 Nov 2026 to 4 Feb 2027, each with the page its date was read from. Checked on 19 Nov 2026.
- `scripts/news_board.py` 1.0.0 -> 1.1.0: section 6, EVENTS AHEAD, and one line under the verdict.
- `scripts/verify_deployment.py`: pin moved to 1.1.0.
- `tests/test_news_board.py`: pins moved; an invented calendar for every run of the suite; `test_n9_*` added (six tests).

## S0 - WHY
- The gate reads headlines after they are written. A summit, a rate decision or a deadline is known weeks ahead, and the workbook showed none of them.
- The gate's own schedule covers only the next 30 hours (a Fed decision, a high-impact US release). The longer view was missing.

## S1 - WHAT
- **Section 6 of News_Board, EVENTS AHEAD:** the events of the next 60 days that have not ended, soonest first; those without an announced date come last.
  - Columns: Date, When ("in 10 days", "today", "under way", ...), Event, Kind, Time (Riyadh), Why it matters, Markets, Status, Source.
- **One line under the verdict:** the next dated event, when it begins within 14 days.
- **The calendar says when it needs care.** The page states it when:
  - the file was last checked more than 21 days ago;
  - no dated event begins 14 days or more ahead (the file is running out);
  - an entry cannot be read (a date that is not a date, an end before its start, no title, an unknown kind or status, a source that is not an https page) - the entry is left out and counted;
  - the file is missing or broken - the section says so and the rest of the page is built as before.
- The file is read from the repository. No query and no fetch is added; a run still reads the same seven BigQuery queries.

## S2 - Safety
- Information only. The section and the line change no recommendation, no verdict and no order; the section says so in its heading.
- The page is written RAW (no formula is evaluated), as before.
- A calendar problem never fails the run: exit code and every other section are unchanged (tested against a broken file).
- Nothing about the portfolio is added to the run log. The log gains the count of events, the next event and any calendar note - all public facts.

## S3 - The events and their sources (read on 19 Nov 2026)
| Date | Event | Status | Source |
|---|---|---|---|
| 2026-11-18 to 2026-11-19 | APEC leaders' meeting, Shenzhen | confirmed | https://chinaglobalsouth.com/analysis/apec-summit-2026-china-shenzhen-trump-xi-putin/ |
| 2026-11-20 to 2026-12-31 | Saudi Arabia's 2027 budget: final approval, due before year-end | date not announced | https://www.mof.gov.sa/en/w/saudi-ministry-of-finance-announces-pre-budget-statement-for-fy2027-expenditures-estimated-at-sar-1-392-bn-and-revenues-at-sar-1-202-bn |
| 2026-11-29 | OPEC+ ministers meet (42nd ministerial meeting) | confirmed | https://opec.org/pr-detail/605-7-june-2026.html |
| 2026-12-08 to 2026-12-09 | US Federal Reserve rate decision, with new projections | confirmed | https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm |
| 2026-12-11 | US government funding runs out (the stopgap law ends) | confirmed | https://www.nasfaa.org/news-item/39735/House_Passes_Continuing_Resolution_Funding_Government_Through_December_11 |
| 2026-12-14 to 2026-12-15 | G20 leaders' summit, Miami (Doral) | confirmed | https://www.cityofdoral.com/Elected-officials/Christi-Fraga/Doral-at-the-Center-of-G20 |
| 2026-12-16 to 2026-12-17 | European Central Bank rate decision | confirmed | https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html |
| 2026-12-17 | Bank of England rate decision | confirmed | https://www.bankofengland.co.uk/news/2025/september/monetary-policy-committee-dates-for-2026 |
| 2026-12-17 to 2026-12-18 | Bank of Japan rate decision | confirmed | https://www.boj.or.jp/en/mopo/mpmsche_minu/index.htm |
| 2027-01-10 | US-China trade truce ends unless it is extended again | confirmed | https://www.spglobal.com/energy/en/news-research/latest-news/agriculture/092426-us-says-trade-truce-with-china-extended-till-jan-10-2027-ahead-of-trump-xi-meet |
| 2027-01-18 to 2027-01-22 | World Economic Forum, Davos | confirmed | https://www.davos.ch/en/experience/events/world-economic-forum-annual-meeting-2027 |
| 2027-01-21 to 2027-01-22 | Bank of Japan rate decision, with its outlook report | confirmed | https://www.boj.or.jp/en/mopo/mpmsche_minu/index.htm |
| 2027-01-26 to 2027-01-27 | US Federal Reserve rate decision | tentative | https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm |
| 2027-02-04 | Bank of England rate decision, with its policy report | confirmed | https://www.bankofengland.co.uk/news/2026/september/mpc-dates-for-2027 |

Notes on the reading:
- **Fed:** the official calendar lists 8-9 Dec 2026; every 2027 date is tentative until confirmed at the meeting before it. The decision is at 14:00 New York time, 22:00 in Riyadh in winter.
- **ECB:** the official calendar lists 16-17 Dec 2026 and then 17-18 Mar 2027 in the months read. Other sites show a meeting on 4 Feb 2027; it is left out until the official page is read again.
- **OPEC+:** the 42nd ministerial meeting was set for 29 Nov 2026 in the statement of 7 Jun 2026; the seven-country group's statement of 1 Nov sets its next meeting for the same day.
- **US funding:** the stopgap law runs to 11 Dec 2026.
- **US-China truce:** extended from 10 Nov 2026 to 10 Jan 2027 (US Treasury, 24 Sep; China's commerce ministry, 28 Sep and 9 Nov).
- **Saudi budget:** no date is announced. The pre-budget statement is of 30 Sep 2026; the final budget is due before year-end.

## S4 - Not in this version
- The calendar is kept by hand. It is not fetched, so it is only as fresh as its check date - which is why the page states that date and says when it is old.
- The gate does not read this calendar. Its own 30-hour schedule (Fed decisions, US releases) is unchanged.
- No company dates (earnings, dividends): those are on the holding pages.

## S5 - Tests
- `pytest tests/test_news_board.py` run 3 times: 20 passed.
- The real file is checked on every CI run: ASCII, the ten fields in order, strict dates, https sources, unique ids, sorted, and every entry one the page will show.
- The suite reads an invented calendar, so it does not change when the world's dates do.
- With every suite around it (Evidence Board, Daily Plan, Today Action Plan, Holding_Read, the news suites, manifest pins, workflow audits): 977 passed.
- Mutants: 53 of 53 on the new code caught, and the 57 of v1.0.0 still caught; the file restored byte-identical each time.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S6 - Deploy, upkeep and rollback
- **Deploy:** merge. The next News Board run (07:55 and 16:55 Riyadh) writes the section; nothing to paste.
- **Upkeep:** when the page says the calendar is old or running out, the file is re-read against its sources and its `checked` date moved. One file, one PR.
- **Rollback:** revert the PR; or `TFB_NEWS_BOARD=off` stops the whole page.
