# TFB Commit Sheet - exchange_calendar_ext 1.0.0 [XCAL-DATA-EXT]: SGX calendar data through 2027

| Item | Value |
| --- | --- |
| Date | 2026-11-06 (daily improvement routine) |
| Branch | `claude/daily-2026-11-06` |
| Base | origin/main `a85edfc6` (track_performance v6.53.0, opportunity_builder 1.28.0, track_performance.yml 1.0.7, daily_sync.yml 1.0.11, ci.yml 1.1.61) |
| Lanes | GitHub Actions (track_performance.yml, daily_sync.yml: Singapore outcomes) and Render (opportunity_builder: Top_10 .SI quote freshness). No Apps Script paste |
| Gate | `TFB_XCAL_DATA_EXT` = on / 1 / true / yes (repository Variable for the tracker, Render env for the builder); default off = main byte for byte (the version strings aside) |
| Priority | (a) a published outcome / recommendation can go missing: from 18 Dec 2026 no Singapore forecast can be judged, and from 31 Dec 2026 a .SI latest close can no longer be certified on Top_10 |
| Versions | `core/exchange_calendar_ext.py` 1.0.0 (new). track_performance 6.58.0 (6.54.0-6.57.0 are claimed by the open PRs #845, #857, #915 and #922). opportunity_builder 1.28.3 (1.28.1 / 1.28.2 are claimed by #778 / #779). track_performance.yml 1.0.12 (1.0.8-1.0.11 claimed). ci.yml 1.1.70 (1.1.62-1.1.69 claimed). Whichever lands later re-numbers |

## S1 - Defect and evidence

The pinned `exchange_calendars==4.13.2` is still the newest release on PyPI (checked on 2026-11-06: `only exchange-calendars<=4.13.2 is available`). Three of its precomputed calendars record holidays only to 2026 and so end at the 2026-12-31 close: XSES (SGX), XSHG (SSE) and XBOM (BSE). XHKG and XKRX run to 2049 / 2050.

**Tracker (Singapore outcomes).**
- `_outcome_calendar_schedule` reads a +/-14-day window around the target (and around the audit clock for the quote session).
- From 2026-12-18 that window passes 2026-12-31. `sessions_in_range` raises `DateOutOfBounds` and every .SI / .SG record reads `outcome_calendar_unknown`, in the live check and the target session both.
- From 2027-01-01 no Singapore record can mature at all.
- Replayed offline on main `a85edfc6` with the REAL audit loop: a D05.SI record targeting 2026-12-24 05:00Z (13:00 SGT), audited at 09:30Z with the 17:00 SGT close quote, stays ACTIVE (`outcome_calendar_unknown`). Harness X5a; the same record matures with the gate on.

**Builder (Top_10 .SI quote freshness).**
- `_venue_state` asks the XSES calendar for the last close before a .SI quote that is older than `TFB_TICKET_MAX_QUOTE_AGE_MIN`.
- From 17:00 SGT on 2026-12-31 (09:00Z), `previous_close` raises `MinuteOutOfBounds` for every later instant (checked: 2026-12-31 12:00Z, 2027-01-04 03:00Z, 2027-01-05 12:00Z). The venue state is unknown.
- So a .SI quote that IS that venue's latest close reads `UNVERIFIED_PRICE: ... venue calendar unavailable` on every weekend, holiday and overnight. Harness X6b replays it on Fri 1 Jan 2027.

**The 2027 data.**
- The 2027 SGX closures are the 2027 Singapore public holidays that fall on a weekday, with a Sunday holiday moving to the Monday. That is the rule the pinned XSES table follows. They are:
  - Fri 1 Jan, New Year's Day;
  - Mon 8 Feb, Chinese New Year (Sat 6 / Sun 7 Feb);
  - Wed 10 Mar, Hari Raya Puasa;
  - Fri 26 Mar, Good Friday;
  - Mon 17 May, Hari Raya Haji;
  - Thu 20 May, Vesak Day;
  - Mon 9 Aug, National Day;
  - Thu 28 Oct, Deepavali.
  - Labour Day and Christmas fall on Saturdays.
- Source: the `holidays` package 0.108 (vacanza/holidays, installed from PyPI into a scratch venv; nothing of it is a project dependency). Its Singapore data cites the Ministry of Manpower public-holiday page (archived 2026-06-18) and lists 2027 as a confirmed year for the lunar and Islamic dates.
- Its SGX (`XSES`) market calendar equals the pinned XSES closed weekdays in **19 of the 20 years 2007-2026**: every year from 2010. The one difference is 2009-11-16, which the package lists and the pinned calendar trades. Its 2027 list is the eight days above.
- The live MOM and SGX pages are not reachable from this container (outbound HTTP blocked); the archived MOM page is the package's cited source.

**Not extended (S6).** XSHG and XBOM 2027 were not official on 2026-11-06. The package's China table is official only to 2026 (2027 is rule-based), and the BSE / NSE 2027 lists are published in December.

**Dedupe.** No open PR covers it; all open PRs were checked, Codex branches included. #922's KNOWN LIMITS (4) named this bound as the next item.

**Survey of the last 24 h.** Nothing outranks it:
- daily_sync, release_decision_feed and track_performance are green since #921;
- intraday_refresh 15:05Z timed out on the backend (both target pages, 45 s) and its next run passed;
- release_decision_feed 20:15Z refused `release_window_consumed` (the window was already released);
- name_label_renewal is red because the bindings expired (an operator action);
- news_archive and provider_target are controls.

## S2 - Change

| Site | Change |
| --- | --- |
| `core/exchange_calendar_ext.py` | NEW 1.0.0. `XCAL_EXTRA_HOLIDAYS` = {"XSES": the eight 2027 days}. `extended_calendar(name)`: with the gate on and an extension for `name`, a subclass of the pinned precomputed calendar whose `precomputed_holidays` are the pinned ones plus the added days (bound 2027-12-31), one instance per process. Otherwise None. It also returns None, so the pinned calendar wins, when the pinned table already reaches the first added day (a newer package retires the extension). Never raises. `calendar_ext_enabled()` reads `TFB_XCAL_DATA_EXT` (fallback literal "off") |
| `scripts/track_performance.py` | v6.58.0: the title line, `SCRIPT_VERSION`, and a dated WHY block after the v6.53.0 block (prior blocks verbatim). `_outcome_calendar_ext(name)` added. `_outcome_calendar_schedule` asks it first, then `calendars.get_calendar(name)` as before |
| `core/analysis/opportunity_builder.py` | 1.28.3: `OPPORTUNITY_BUILDER_VERSION` and a dated WHY block before it. `_venue_cal_data_ext(code)` added. `_venue_state` uses it after the unchanged cache lookup, when it is not None |
| `.github/workflows/track_performance.yml` | 1.0.12 header. `TFB_XCAL_DATA_EXT: ${{ vars.TFB_XCAL_DATA_EXT \|\| 'off' }}` in the 'Run performance clock' step, after `TRACK_LOG_UNFORMATTED_READ` (away from #845 / #857 / #915 / #922's lines). **Not armed** |
| `.github/workflows/daily_sync.yml` | the same mapping in the tracker step, after `TRACK_LOG_UNFORMATTED_READ` (ENV PARITY). **Not armed** |
| `.github/workflows/ci.yml` | 1.1.70: a heavy-job step before the outcome-evidence step runs the harness. It fetches `a85edfc6` and sets `TFB_TEST_REQUIRE_BASE=1` |
| `scripts/verify_deployment.py` | pins: track_performance 6.58.0, opportunity_builder 1.28.3, and the new `core.exchange_calendar_ext` 1.0.0 (pins only) |
| `tests/test_track_performance_v6530_archive_bar.py` | the exact 6.53.0 pins become floors (the same code as #922's) |
| `tests/test_xcal_data_ext.py` | new harness (S4) |

Functions: 5 added (`calendar_ext_enabled`, `_extended_calendar` and `extended_calendar` in the new module, plus its nested calendar subclass; `_outcome_calendar_ext` in the tracker; `_venue_cal_data_ext` in the builder), 0 removed.

Unchanged in every mode:
- every session the pinned calendars have (same open and close);
- every other MIC;
- the maturation and freshness rules, every refusal and the receipt schema;
- the cron and the concurrency.

## S3 - Off identity

With the gate unset and under ten off words (blank, off, 0, false, no, enforce, observe, onn, y, enabled):
- `extended_calendar` is None for every MIC (X2);
- the tracker's `_outcome_sessions` equals the untouched v6.53.0 (`git show a85edfc6`) over 12 symbols x 9 days, raising cases included (X4).

With the gate on:
- the result equals the base everywhere except the 21 .SI / .SG windows that pass 2026-12-31, where the base raises and the delivered file returns the extended sessions (X4b);
- AAPL.US, 2222.SR and VOD.L audits give the same record state and receipt on and off (X5d);
- the builder's AAPL.US / 2222.SR venue states are identical on and off (X6c).

## S4 - Measured

`tests/test_xcal_data_ext.py`: **PASS 47/47** as a script and under pytest.

| Case | What it checks |
| --- | --- |
| X1 | the table, pinned literally; only XSES; every added day a 2027 weekday after the pinned bound (2026-12-31) |
| X2 / X2b | unset and ten off words read off (every MIC None); six on-spellings read on |
| X3 | on: the extended XSES equals the pinned XSES on all 5,059 pinned sessions (opens and closes); 2027 has 253 sessions (its weekdays minus the eight); bound 2027-12-31; `previous_close` on 2027-01-04 is the 2026-12-31 close; one instance per process; other MICs, unknown names None |
| X3b | a failing import reads None (never raises); a table the package already covers retires itself |
| X4 / X4b | tracker off = v6.53.0 under the off words (the 18 Dec defect pinned); on changes exactly the 21 Singapore windows past 2026 |
| X5a | D05.SI due 2026-12-24, the close quote at 09:30Z: off ACTIVE `outcome_calendar_unknown` (the defect); on MATURED WIN, `target_session_close` 2026-12-24T09:00Z |
| X5b | D05.SI due 2027-01-08: off unknown; on MATURED |
| X5c | D05.SI due on 8 Feb 2027 (closed): on, the target session is 9 Feb and its close matures it |
| X5d | AAPL.US, 2222.SR, VOD.L: identical on and off |
| X6a | builder `_venue_state('D05.SI', 2027-01-04 03:00Z)`: off None; on (open, 2026-12-31 09:00Z) |
| X6b | a .SI quote at the 2026-12-31 close judged on Fri 1 Jan 2027: off `UNVERIFIED_PRICE ... venue calendar unavailable` (the defect); on certified "last close" |
| X6c | other venue states identical on and off |
| X7 | golden negative: X5a-on and X6b-on FAIL on the untouched main copies |
| X8 | both tracker steps map the gate exactly once with `\|\| 'off'`, identically; the reader's fallback literal is "off"; ci.yml's step runs the file, fetches the full base SHA and sets `TFB_TEST_REQUIRE_BASE: "1"` |
| V1 / V1b | version floors, the three verifier pins, title line, WHY order, ASCII files. Against the base: only the version lines and the one re-indented `get_calendar` line removed, added lines ASCII (the title aside), exactly one new function per file (skips once an open PR's block is merged in) |

Golden negative of the whole files (`TFB_TEST_TRACK_DELIVERED` / `TFB_TEST_OB_DELIVERED` = the main copies): **7 failed, 39 passed, 1 skipped**.
- The failures are X4b, X5a, X5b, X5c, X6a, X6b and V1.
- The passes are the new module's own cases (X1-X3, which have no base), off-identity cases (equal by construction), X5d / X6c, X7 (which asserts the base's failure) and X8.
- The skip is V1b.

Mutants, each run against the harness in its own copy of the tree: **14 / 14 killed**:
1. the reader defaulting on (9 failed);
2. any word turning it on (16);
3. the tracker hook dropped (4);
4. the builder hook dropped (2);
5. the 8 Feb closure dropped (3);
6. Chinese New Year on 9 Feb instead (3);
7. the bound guard dropped (1);
8. every MIC extended (8);
9. an error raising out of `extended_calendar` (1);
10. the pinned holidays replaced instead of extended (6);
11. the bound not extended (7);
12. the builder bypassing the gate (2);
13. the tracker bypassing the gate (13);
14. daily_sync.yml unmapped (1).

Other checks: see the PR body (compileall, ruff, `render_env_contract --check`, the affected suites, the ci.yml lean list and the contract-lane emulation on an export of this tree).

## S5 - Owner steps (proposed; nothing is armed here)

1. Merge.
2. Set the repository Variable `TFB_XCAL_DATA_EXT=on` **before Fri 18 Dec 2026** (the tracker lane).
3. Set the Render env `TFB_XCAL_DATA_EXT=on` and deploy **before Thu 31 Dec 2026** (the builder lane).
4. After 18 Dec, receipts on .SI / .SG records should show `"calendar": "XSES"` with a `target_session` instead of `outcome_calendar_unknown`.

Arming is safe early: every 2006-2026 session is unchanged (X3), so nothing moves before 2027 except the windows that reach it.

Rollback: unset the Variable or the Render env.

## S6 - Known limits and not done

- **No SGX half day is modelled.** The pinned calendar models none either. #922 re-times the 2025 and early-2026 ones.
- **A later SGX closure** (a polling day announced in 2027) needs a new row.
- **The new bound is 2027-12-31.** The tracker's window reaches it on 2027-12-17, so the 2028 rows are needed before then, unless a package release carries them; the extension retires itself once the pinned table covers its year.
- **XSHG (.SS, and .SZ in #778) and XBOM (.BO)** end at 2026-12-31 too. In the builder their rows read `venue calendar unavailable` from 31 Dec 2026 until a package or this table carries 2027. #915 adds .SS / .BO to the tracker's map and meets the same bound.
- **If #922 lands:** its 2026 SGX half-day rows (24 and 31 Dec 2026) become reachable with this gate on. #922 left them out because no lookup could reach them; a follow-up row can add them.
- **Not done:** no gate armed and no Variable set; the flag is not registered in verify_deployment FLAGS (optional, owner choice); no change to the maturation rule, the receipt schema, the cron or the concurrency; no live workbook read or write and no provider call (everything is offline).
