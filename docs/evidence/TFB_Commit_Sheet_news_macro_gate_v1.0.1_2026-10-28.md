# Commit Sheet - `scripts/news_macro_gate.py` v1.0.1 - 2026-10-28

**Scope:** a follow-up to PR #859, carrying three fixes:
1. Codex's re-review finding. The owner merged #859 at 21:56:53 UTC with head `45748da0`; the fix was pushed at 22:05 UTC and never reached the PR.
2. A collection-time defect found by the first live dry run.
3. Three classification gaps shown by the same run's real headlines.

**Files:**
- `scripts/news_macro_gate.py`: 1.0.0 -> 1.0.1; WHY block prepended, the v1.0.0 text kept verbatim.
- `data/news_gate_taxonomy.json`: regenerated, still ASCII.
- `tests/test_news_macro_gate.py`: 69 cases.
- `scripts/verify_deployment.py`: pin 1.0.1.
- This sheet.

## S1 - Evidence: the first live dry run `40595558188` (28 Oct 22:32-22:43 UTC, main `78faeb6e`, write=false)
- **Sources:**
  - Every source answered: econ 98 events, edgar 19 filings, fed_rss 0 (no release in 3 days), gdelt 24 items, google_news 400 items, market 2 quotes.
  - GDELT answered the geopolitics query with HTTP 429 after the retry.
  - Scope: 15 names.
  - Totals: 424 collected, 151 classified.
- **Names:**
  - FRST.US: INFO, 8-K 2.02 five days earlier. This is the filing the 26 Oct entry missed.
  - OPBK.US: INFO, 8-K 2.02 five days earlier.
  - WPC.US: INFO, 8-K 2.02 the same day.
- **Verdict NO_NEW_ENTRY**, driven by:
  - FOMC, listed twice ("FOMC rate decision" and EODHD's "FOMC Statement");
  - an Iran cluster graded 1 from 6 sources, whose lead headline was *"oil falls as diplomatic efforts to contain the Iran war grow"*.

  The same run also showed:
  - an easing cluster from 18 sources, "Gulf bourses rise amid de-escalation efforts";
  - a C-graded "tanker on fire in Hormuz" story;
  - an F-graded "coalition intercepts Houthi missiles" story.
- **Run time:** 11 minutes for the whole job.

## S2 - WHAT
| # | Defect | Fix | Test (golden negative on 78faeb6e) |
|---|---|---|---|
| 1 | `--write` with a spreadsheet id but no usable Sheets credentials ran as a silent dry run and exited 0 (Codex re-review). | `main()` returns 2 before collecting; a session that fails to open is reported. | `test_g8_write_without_credentials_fails_loudly` fails on v1.0.0 |
| 2 | A throttled or unreachable provider cost about a minute per query (two 25 s attempts and a back-off), with no bound across ~20 queries, so the 20-minute job limit was within reach. | Per-source circuit breaker (2 consecutive failed queries) and time budget (`SOURCE_BUDGET_S`: gdelt 240 s, google_news 120 s, edgar 150 s). Skipped queries are counted in the failure note. Per-request timeout 20 s (`TFB_NEWS_GATE_TIMEOUT_S`, 5..60). A GDELT plain-text answer is not a breaker failure. One progress line per source in the log. | `test_g7_breaker_stops_a_dead_source`, `test_g7_time_budget_bounds_a_slow_source`, `test_g7_request_timeout_is_bounded` fail on v1.0.0 |
| 3a | The bare Arabic word for *war* (harb) classified any headline mentioning a war as escalation, including diplomacy and "end of the war" stories. | harb removed from war_escalation. Added to war_escalation (EN + AR): outbreak or declaration of war, attacks, a tanker hit, burning or ablaze. Added to de_escalation (EN + AR): ending the war, diplomatic efforts, containment, peace plan. | both live de-escalation headlines fail on v1.0.0 |
| 3b | EODHD's "FOMC Statement" doubled the FOMC driver. | `FOMC_NAME_RE` folds every EODHD FOMC-named event on a static decision day into one driver. | `test_g5_eodhd_fomc_statement_is_not_a_second_driver` fails on v1.0.0 |

What the 22:32 UTC run would have said with v1.0.1:
- **Verdict CAUTION**, not NO_NEW_ENTRY.
- **Drivers:** FOMC today at 21:00 Riyadh; unconfirmed Gulf escalation stories (the burning tanker in Hormuz, Houthi missiles intercepted).
- **Easing:** de-escalation efforts reported by 18 sources.

This matches the hand check, which keeps the gate observe-only until it is scored.

### Review fix (Codex, PR #861, head `3d84c263`)
| Finding | Verified path | Fix | Golden negative |
|---|---|---|---|
| Failed diplomacy read as easing. *"Iran launches missile strikes as diplomatic efforts fail"* became de-escalation; from Reuters, AP and BBC the verdict fell from NO_NEW_ENTRY to GO. | Fix 3a added *diplomatic efforts* to de-escalation. `Classifier.classify()` turns a war match into de-escalation whenever a de-escalation term matches and no breach marker does, and failure was not a breach marker. | Breach markers (EN + AR) now cover failed, stalled or rejected diplomacy, a breakdown, deadlock or impasse, and no progress. A leading space in a taxonomy term now marks a word start: ` stall` misses *installed*, and ` fail` catches *failed* and *failure*. A trailing space keeps `broke ` out of *US-brokered* (a v1.0.0 slip that kept a brokered ceasefire as war). When attack and diplomacy words meet, a failure word keeps the story escalation, the safer error for an entry gate. | 10 new cases fail on `3d84c263`: 7 headlines (EN + AR), the brokered ceasefire, the word-start rule, and Reuters / AP / BBC giving NO_NEW_ENTRY. Two guards pass on both: easing diplomacy stays easing, and *installed* is not *stalled*. |

## S3 - Tests
- `tests/test_news_macro_gate.py`: 69 passed (49 before).
- 16 of the 69 fail on 78faeb6e: the live-headline, FOMC, breaker, budget, timeout and credentials cases, two version-string checks, and the review-fix cases that v1.0.0 also got wrong.
- The 10 review-fix cases fail on `3d84c263`.
- Manifest pins, the workflow audit (errors 0, warnings 0) and the related suites pass (119). All files are ASCII.

## S4 - Rollback
Revert `scripts/news_macro_gate.py` and `data/news_gate_taxonomy.json` to 78faeb6e. No workflow, gate or tab changes.

## S5 - Operator steps
1. Merge before the 09:41 Riyadh run if possible; otherwise the scheduled runs use v1.0.0 (observe-only either way).
2. Claude dispatches a dry run on the merge commit and checks the progress lines and the verdict. One `write=true` dispatch follows; scheduled runs take over from there.

Not a licensed financial adviser; the owner decides and executes every trade.
