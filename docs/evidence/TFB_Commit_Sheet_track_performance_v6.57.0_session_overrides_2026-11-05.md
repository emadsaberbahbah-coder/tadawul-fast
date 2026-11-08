# TFB Commit Sheet - track_performance v6.57.0 [SESSION-OVERRIDES]

| Item | Value |
| --- | --- |
| Date | 2026-11-05 (daily improvement routine) |
| Branch | `claude/daily-2026-11-05` |
| Base | origin/main `a85edfc6` (track_performance v6.53.0, track_performance.yml 1.0.7, daily_sync.yml 1.0.11, ci.yml 1.1.61) |
| Lane | GitHub Actions only (`track_performance.yml`, `daily_sync.yml`); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TRACK_SESSION_OVERRIDES` = on / 1 / true / yes; default off = v6.53.0 byte for byte (the version string aside) |
| Priority | (a) a published outcome number can be wrong: on main, with no gate, a .KS / .KO record due on the 19 Nov 2026 exam day can mature on an intraday price |
| Version | 6.57.0. 6.54.0, 6.55.0 and 6.56.0 are claimed by the open PRs #845, #857 and #915 (all mine); whichever lands later re-numbers. track_performance.yml 1.0.11 (1.0.8-1.0.10 claimed by #845 / #857 / #915); ci.yml 1.1.69 (1.1.62-1.1.68 claimed by #778, #882, #889, #897, #909, #915) |

## S1 - Defect and evidence

The tracker has one exchange-calendar lookup, `_outcome_sessions`. It is used for:
- the live-quote session check;
- the target session (the first calendar session whose close is at or after the target);
- the v6.47.0 historical close, which is stamped with that session's calendar close.

The pinned exchange_calendars 4.13.2 gets some base-map sessions wrong:

- **XKRX (.KS .KO), the Korean college-exam (CSAT) day.**
  - The library shifts every CSAT day by +1h (`special_offsets_adhoc`), but its date list `precomputed_csat_days` ends at 2020-12-03.
  - So 2024-11-14, 2025-11-13 and 2026-11-19 read 09:00-15:30 KST. KRX trades 10:00-16:30 those days (continuous to 16:20, closing call to 16:30). The 2025 hours were announced on 30 Oct 2025; the 2027 exam is Thu 19 Nov 2026.
  - Replayed offline on main `a85edfc6` with the REAL audit loop: a 005930.KS record targeting 2026-11-19 05:00Z is audited at 07:05Z with a regular-market quote stamped 06:40Z (15:40 KST, continuous trading). It **MATURES (WIN)**, because the quote falls inside the 15-minute close slack of the calendar's 15:30 close. That is about 50 minutes before the real close (harness T4a).
  - It needs a quote stamped 06:30-06:45Z: a delayed 01:30Z clock run (delays of 3-7 h are documented in the workflow header) or a manual audit.
- **XJSE (.JSE), half days.** The calendar keeps 17:00 SAST on 24 and 31 Dec; the JSE closes at 12:00 on both (2024 and 2025 notices). Under `TRACK_HIST_TARGET_CLOSE=enforce`, an NPN.JSE record targeting 2026-12-24 12:00Z (14:00 SAST) matures on the 24 Dec bar, struck at 12:00 SAST before its target (T4c).

A base-map audit then checked every base MIC from 2025-11-01 to 2027-11-04:
- four checkers compared the pinned calendar with each venue's official sessions;
- an independent refuter re-checked every flagged day.

It confirmed:

| MIC | Day(s) | Calendar | Venue | Error |
| --- | --- | --- | --- | --- |
| XSES (.SI .SG) | 2025-12-24, 2025-12-31, 2026-02-16 | 09:00-17:00 SGT | eve half days, continuous trading to 12:00 (the library's own docstring lists them as a TODO) | hist path, one session early |
| XTAE (.TA) | 2026-04-03, 2027-04-23 (Passover-interim Fridays) | close 14:15 IDT | ordinary Friday, end of trading 13:50 IDT | hist path, one session early (T4g) |
| XTAE | 2026-01-04 | Sunday session | closed (Monday-Friday week from 5 Jan 2026) | phantom session |
| XTAE | 2026-10-27 | session | closed (Knesset election) | phantom session |
| XJSE | 2026-11-04 | session | closed (local elections) | phantom session (T4f) |
| XKRX | 2026-06-03 | session | closed (local elections) | phantom session |
| XKRX | 2026-07-17 | session | closed (Constitution Day, a public holiday again from 2026) | phantom session |
| XKRX | 2027-05-03, 2027-07-19 | session | closed (2027 substitute days for Labor Day / Constitution Day under the 2026 Public Holidays Act amendment) | phantom session |

A record whose target falls on a phantom session never finds a quote inside it:
- it stays ACTIVE for ever;
- or, under hist enforce, it matures on a provider bar dated the closed day (T4f shows the second case).

Refuted and left alone:
- XETR and XSWX extended-hours claims.
- TASE ordinary Fridays. After the 13:44-13:45 auction, trade-at-last runs at the closing price until the calendar's 13:50, the same reasoning that kept .AT in #915.
- TASE Thursday claims.
- The 2026 SGX half days. XSES data ends 2026-12-31 and every lookup reads +/-14 days, so no lookup can reach them.

No open PR covers this (dedupe over all open PRs, Codex branches included). #915 denies .KQ and .JO for these reasons but leaves the base map's .KS / .KO / .JSE as they are.

The survey of the last 24 h found nothing that outranks it:
- name_label_renewal is red: the bindings expired (an operator action).
- daily_sync coverage_failed on Mutual_Funds names: #921 addresses it on main.
- a daily_sync decision_failed render race: known, fails closed.
- news_archive is red on identity_coverage_incomplete, and provider_target has a stale baseline: controls only.

## S2 - Change

| Site | Change |
| --- | --- |
| `scripts/track_performance.py` | v6.57.0; the title line and `SCRIPT_VERSION`; a dated WHY block after the v6.53.0 block and before `SCRIPT_VERSION`; prior blocks verbatim |
| `_OUTCOME_SESSION_OVERRIDES` | added: {MIC: {session label: (open, close) in UTC ISO stamps, None keeping the calendar's value, or "closed"}} for XKRX, XJSE, XTAE and XSES (S1) |
| `_session_overrides_enabled` | added: `TRACK_SESSION_OVERRIDES` in on / 1 / true / yes (fallback literal "off") |
| `_outcome_session_override` | added, PURE: re-times the named sessions, drops the "closed" ones, leaves every other session and MIC alone; a malformed entry returns the calendar's schedule; never raises |
| `_outcome_sessions` | three added lines before the unchanged return: with the gate on, the schedule goes through `_outcome_session_override` |
| `.github/workflows/track_performance.yml` | 1.0.11 header; `TRACK_SESSION_OVERRIDES: ${{ vars.TRACK_SESSION_OVERRIDES \|\| 'off' }}` in the 'Run performance clock' step env, after `TFB_BQ_LOCATION`. **Not armed.** |
| `.github/workflows/daily_sync.yml` | the same mapping in the tracker step (ENV PARITY), after `TFB_BQ_LOCATION`. **Not armed.** |
| `scripts/verify_deployment.py` | pin track_performance 6.57.0 (pin only) |
| `.github/workflows/ci.yml` | 1.1.69: a heavy-job step after the venue calendar coverage step runs the harness. It fetches `a85edfc6` and sets `TFB_TEST_REQUIRE_BASE=1` |
| `tests/test_track_performance_v6530_archive_bar.py` | the exact 6.53.0 pins become floors; the title and function-set checks read the delivered version |
| `tests/test_track_session_overrides.py` | new harness (S4) |

Functions: 2 added, 0 removed.

Unchanged in every mode:
- the base map;
- every refusal;
- the maturation rule;
- the receipt schema `tfb.outcome-price.v1`;
- the CA guard;
- the cron and the concurrency.

The mappings and the CI step sit away from #915's edits. Trial merges with #915, #845, #857 and #909 conflict only on the version lines, the WHY region, the pins (including the archive-bar test's comment lines) and the CI / workflow headers. In `_outcome_sessions` the #915 extension and this override combine cleanly.

## S3 - Off identity

`_outcome_sessions` was run over 21 distinct symbols x 22 days, whose +/-14-day windows reach every override day (T3 asserts that every row is exercised), under ten off words (unset, blank, off, 0, false, no, enforce, observe, onn, y). It returns or raises exactly as the untouched v6.53.0 (`git show a85edfc6`), sessions included (T2).

The real audit loop on ordinary days (.KO, AAPL.US, 2222.SR, VOD.L, .JSE) gives the same record state and receipt with the gate off and on (T4e).

## S4 - Measured

`tests/test_track_session_overrides.py` passes **PASS 40/40** as a script and under pytest (33 at the first head; the review round added T4h and T7, S7).

| Case | What it checks |
| --- | --- |
| T1 | the table, pinned literally. Every entry names a session the pinned calendar has. A re-time changes it (open < close, same UTC day). XKRX re-times are exactly the library's +1h exam-day rule. XJSE half days close 12:00 SAST, XTAE Fridays 13:50 IDT (the calendar says 14:15), XSES half days 12:00 SGT, and every non-XKRX re-time shortens the day. The "closed" days are exactly the seven phantom sessions |
| T2 / T2b / T2c | off = v6.53.0 over the matrix and ten off words; the six on-spellings; junk stays off |
| T3 | on: only the named sessions change (re-timed, or gone when "closed"), only on their MIC; every other session, MIC and refusal is unchanged; nothing is added; the changed set equals the table, every row exercised |
| T4a | .KS exam day, a 15:40 KST quote at 07:05Z: off MATURES (the defect; receipt `target_session_close` 06:30Z); on stays ACTIVE `target_session_not_complete` (close 07:30Z) |
| T4b | .KS exam day, the 16:30 KST close quote at 08:00Z: on MATURES (WIN, close 07:30Z); off stays ACTIVE |
| T4c | .JSE 24 Dec half day, hist enforce, a 14:00 SAST target: off MATURES on the 24 Dec bar (ROI 5.0, the defect); on, the target session is 28 Dec and its own close matures it (ROI 10.0, no bar read) |
| T4d | .JSE half day, a 12:05 SAST quote: on MATURES on the real close (10:00Z); off stays ACTIVE |
| T4e | ordinary days: identical on and off |
| T4f | .JSE 4 Nov 2026 election day (closed), hist enforce: off, the phantom session is the target and a bar dated 4 Nov matures it; on, 5 Nov's own close does |
| T4g | .TA 3 Apr 2026 Passover Friday, a 14:00 IDT target: off, hist enforce matures it on the Friday bar struck at 13:50; on, Monday 6 Apr's close does |
| T4h | .JSE 24 Dec half day, hist enforce, a 10:00 SAST target audited on 28 Dec: both modes mature on the 24 Dec bar (ROI 5.0, one bar read); on stamps `quote_asof` and `target_session_close` at the real 12:00 SAST close (10:00Z), off at the calendar's 17:00 (15:00Z) |
| T7 | six malformed entries (an unparseable open, an hour 25 close, "Closed", a 1-tuple, an int, a 1-tuple with a valid open): `_outcome_session_override` returns the calendar's own schedule (the same object) and does not raise, directly and through `_outcome_sessions` with the gate on |
| T5 | golden negative: T4a-on and T4c-on FAIL on the untouched v6.53.0 |
| T6 | both tracker steps map the gate exactly once with `\|\| 'off'`, identically; the reader's fallback literal is "off"; ci.yml's step runs the file, fetches the full base SHA and sets `TFB_TEST_REQUIRE_BASE: "1"` |
| V1 / V1b | version floor, verifier pin, title line, WHY order, ASCII test file. Against the base: only the title and `SCRIPT_VERSION` lines removed, the added lines ASCII apart from the title, exactly two functions added (skips once #845 / #857 / #915 blocks are merged in) |

Golden negative of the whole file against the untouched v6.53.0 (`TFB_TEST_TRACK_DELIVERED=` that copy): **32 failed, 7 passed, 1 skipped** (25 / 7 / 1 at the first head).
- The 7 that pass compare with the base by construction (T2, T4e x5) or assert the base's own defect (T5).
- The skip is V1b.

Mutants, each run against the harness (rebuilt on the review-round file): **16 / 16 killed**:
1. the reader defaulting on (11 failed);
2. the gate check dropped (9);
3. the exam-day close kept at 15:30 (4);
4. the 2026 exam-day row dropped (4);
5. a JSE half-day close at 11:00Z (4);
6. the open never re-timed (2);
7. the close never re-timed (9);
8. any word turning it on (8);
9. a row for a day the calendar does not open (1);
10. the XSES rows filed under another MIC (2);
11. "closed" days not dropped (2);
12. the TASE Friday row dropped (3);
13. an SGX half day at 12:30 (2);
14. the JSE election-day row dropped (3);
15. the fallback narrowed to `except ZeroDivisionError`, so a malformed entry raises (6; review round);
16. a malformed entry silently dropping its session instead of returning the calendar's schedule (5; review round).

Other checks: see the PR body (the tracker, workflow and flag suites; compileall; ruff; the ci.yml lean list and the contract-lane emulation on an export of this tree).

## S5 - Owner steps (proposed; nothing is armed here)

1. Merge. This is GitHub Actions only: no deploy, no paste.
2. Set the repository Variable `TRACK_SESSION_OVERRIDES=on` **before Thu 19 Nov 2026**.
3. On 19 Nov, check that receipts on .KS / .KO records due that day show `"target_session_close": "2026-11-19T07:30:00+00:00"`.

Rollback: unset the Variable. Records that matured meanwhile keep their receipts, and `target_session_close` shows the hours used.

## S6 - Known limits and not done

- The table is maintained by hand. A new special session needs a new row; the harness checks each row against the pinned calendar, not against the venue.
- Some rows follow the venues' recurring rules or the 2026 holiday law; not every notice was published on 2026-11-05:
  - the 2026 JSE half days;
  - the 2026 exam-day hours;
  - the 2027 KRX substitute holidays.
- **Not fixed here (base map, pre-existing, time-bound):** XSES data ends 2026-12-31 in 4.13.2 (`holidays are only recorded to the year 2026`). From 2026-12-18 every .SI / .SG lookup raises `outcome_calendar_unknown` until the pinned package is bumped; no exchange_calendars release after 4.13.2 was found. Proposed as the next item.
- Once this and #915 both land, #915's .KQ and .JO denials can be lifted for these days.
- No change to the base map, the maturation rule, the receipt schema, the cron or the concurrency. No live workbook read or write and no provider call: everything is offline with stubs.

## S7 - Review round (2026-11-05)

**Codex (P2, SGX half days should close after the closing auction, 12:06 or 12:16 SGT): kept at 12:00, answered on the thread and resolved.**
- The pinned XSES calendar closes every ordinary SGX session at 17:00 SGT, the end of continuous trading, before the closing routine (17:00-17:06) and trade-at-close (to 17:16). The half-day rows use the same point. On both kinds of day the closing print falls to the 15-minute regular-market slack.
- No record can reach the three rows. They are 2025-12-24, 2025-12-31 and 2026-02-16, and the first target date in the log is 2026-07-28.
- Replayed with the real audit loop with the gate off on an ordinary day (2026-11-04): a 17:03 SGT target maps to the next session, and a 17:16 regular-market print is refused as `quote_session_unknown`. So the case the reviewer describes is a base-map property of every SGX session (and of other venues whose calendar close excludes the auction), not of this override. It is reported to the owner as a separate item.
- A comment on the XSES rows now states the convention. It is three comment lines in the table and changes no value.

**Independent four-lens review (maturation, off identity, workflows / CI, tests and claims):**
- No finding on maturation or on the workflows / CI lens.
- Fixed in this round:
  - **(minor) The matrix was smaller than claimed.** SYMBOLS listed D05.SI and TEVA.TA twice, so it held 19 distinct symbols, not 21. No DAYS window reached the 2024 JSE half days, and T3 carved them out by hand. Now: Z74.SI and LUMI.TA replace the duplicates, 2024-12-24 09:00Z is added (22 days), a module-level assert pins 21 distinct symbols and 22 distinct days, and T3 asserts that the changed set equals the table.
  - **(nit) The malformed-entry contract was untested.** Mutants 15 and 16 survived the first head. T7 now covers it.
  - **(nit) No case ran the hist path with the gate on.** T4h now covers it.
  - **(nit) T1's docstring** said every re-time is a shorter day. XKRX shifts by +1h; the docstring now says so.
  - **(nit) `_state`'s receipt normaliser** expected a space after each colon, but the receipt is compact JSON, so it never matched. It now matches. T4e is unaffected: it compares the delivered module with itself.

**CI:**
- The advisory "Whole suite (pinned Python 3.11)" job (`timeout-minutes: 15`, `continue-on-error: true`) was cancelled at its timeout in push run 42042240471 at 97%.
- It passed on the same commit in the pull_request run 42042353188, in 13m43s.
- It is cancelled the same way on main `a85edfc6` (push run 42017020363). Not this PR's; noted on the PR. Raising the cap or sharding the suite is an owner choice.

## Addendum 2026-11-08 - merge of main `4b478e7`

Main `4b478e7` (#935-#937) was merged in with a merge commit. The ci.yml header conflicted: main took 1.1.68-1.1.70.

- ci.yml: this release is re-numbered **1.1.69 -> 1.1.74** (its header, its `v1.1.74` line and its step comment).
- The header's claim note lists the open claims after the re-numbering: 1.1.71 #926, 1.1.72 #938, 1.1.73 #915, 1.1.74 #922, 1.1.75 #923, 1.1.76 #909.
- Main's 1.1.70 / 1.1.69 / 1.1.68 blocks follow verbatim. Against main, the ci.yml diff adds lines only.
- After the merge: `tests/test_track_session_overrides.py` 40 passed with `TFB_TEST_REQUIRE_BASE=1`. The verifier pins, workflow flag mappings and tracker flags suites pass (49 passed). compileall and ruff (CI rules) are clean, and no conflict marker remains.
