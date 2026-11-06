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
- Replayed offline on main `a85edfc6` with the REAL audit loop: a D05.SI record targeting 2026-12-23 05:00Z (13:00 SGT), audited at 09:30Z with the 17:00 SGT close quote, stays ACTIVE (`outcome_calendar_unknown`). Harness X5a; the same record matures with the gate on.

**Builder (Top_10 .SI quote freshness).**
- `_venue_state` asks the XSES calendar for the last close before a .SI quote that is older than `TFB_TICKET_MAX_QUOTE_AGE_MIN`.
- From the last pinned close on 2026-12-31, `previous_close` raises `MinuteOutOfBounds` for every later instant (checked: 2026-12-31 12:00Z, 2027-01-04 03:00Z, 2027-01-05 12:00Z). The venue state is unknown.
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

**Half days.** The same source lists the SGX half days ("markets close at 12:00 p.m. SGT"). These are the eves of Christmas, New Year and Chinese New Year:
- in 2026: Thu 24 Dec and Thu 31 Dec;
- in 2027: Fri 5 Feb, Fri 24 Dec and Fri 31 Dec.

The pinned calendar models no half day; its docstring lists them as a TODO. The extension models these five because it is what makes them reachable (S7).

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
| `core/exchange_calendar_ext.py` | NEW 1.0.0. Two tables: `XCAL_EXTRA_HOLIDAYS` = {"XSES": the eight 2027 days} and `XCAL_HALF_DAYS` = {"XSES": ("12:00", the five half days from 2026-12-24)}. `extended_calendar(name)` returns, with the gate on and an extension for `name`, a subclass of the pinned precomputed calendar, one instance per process. The subclass adds those days to the pinned `precomputed_holidays` (bound 2027-12-31) and adds the half days as adhoc special closes at 12:00 SGT, the end of continuous trading (the convention of the calendar's own 17:00 close, as in #922). Otherwise it returns None. It also returns None, so the pinned calendar wins, when the pinned table already reaches the first added day (a newer package retires the extension). Never raises. `calendar_ext_enabled()` reads `TFB_XCAL_DATA_EXT` (fallback literal "off") |
| `scripts/track_performance.py` | v6.58.0: the title line, `SCRIPT_VERSION`, and a dated WHY block after the v6.53.0 block (prior blocks verbatim). `_outcome_calendar_ext(name)` added. `_outcome_calendar_schedule` asks it first, then `calendars.get_calendar(name)` as before |
| `core/analysis/opportunity_builder.py` | 1.28.3: `OPPORTUNITY_BUILDER_VERSION` and a dated WHY block before it. `_venue_cal_data_ext(code)` added. `_venue_state` uses it after the unchanged cache lookup, when it is not None |
| `.github/workflows/track_performance.yml` | 1.0.12 header. `TFB_XCAL_DATA_EXT: ${{ vars.TFB_XCAL_DATA_EXT \|\| 'off' }}` in the 'Run performance clock' step, after `TRACK_LOG_UNFORMATTED_READ` (away from #845 / #857 / #915 / #922's lines). **Not armed** |
| `.github/workflows/daily_sync.yml` | the same mapping in the tracker step, after `TRACK_LOG_UNFORMATTED_READ` (ENV PARITY). **Not armed** |
| `.github/workflows/ci.yml` | 1.1.70: a heavy-job step before the outcome-evidence step runs the harness. It fetches `a85edfc6` and sets `TFB_TEST_REQUIRE_BASE=1` |
| `scripts/verify_deployment.py` | pins: track_performance 6.58.0, opportunity_builder 1.28.3, and the new `core.exchange_calendar_ext` 1.0.0 (pins only) |
| `tests/test_verify_manifest_pins.py` | `_MODULE_FILES` maps the new pin to `core/exchange_calendar_ext.py` |
| `tests/test_track_performance_v6530_archive_bar.py` | the exact 6.53.0 pins become floors (the same code as #922's) |
| `tests/test_xcal_data_ext.py` | new harness (S4) |

Definitions: 7 functions added and 0 removed.
- 5 at top level: `calendar_ext_enabled`, `_extended_calendar` and `extended_calendar` in the new module; `_outcome_calendar_ext` in the tracker; `_venue_cal_data_ext` in the builder.
- 2 nested in the extension subclass: the `precomputed_holidays` and `special_closes_adhoc` overrides.
- 1 nested class (the subclass).

Unchanged in every mode:
- every pinned session but the two 2026 half days (S3);
- every other MIC;
- the maturation and freshness rules, every refusal and the receipt schema;
- the cron and the concurrency.

## S3 - Off identity, and what the gate moves

**Off.** With the gate unset and under eleven off words (blank, off, 0, false, no, enforce, observe, onn, y, enabled, enable):
- `extended_calendar` is None for every MIC (X2);
- the tracker's `_outcome_sessions` equals the untouched v6.53.0 (`git show a85edfc6`) over 12 symbols x 9 days, raising cases included (X4).

**On.** The tracker differs from the base only in .SI / .SG windows:
- the 21 windows that pass 2026-12-31, where the base raises and the delivered file returns the extended sessions;
- the 3 windows that end earlier but hold 24 / 31 Dec 2026, which differ only in those two half days' close (09:00Z -> 04:00Z). No lookup that early ever selects those entries: a quote cannot be stamped in the future, and the target session is the first close at or after a target that is still on or before 18 Dec (X4b).

The rest is unchanged:
- AAPL.US, 2222.SR and VOD.L audits give the same record state and receipt on and off (X5d);
- the builder's AAPL.US / 2222.SR venue states are identical on and off (X6c).

**Builder half days.** The builder reads the two 2026 half days directly. From 12:00 SGT on those days the venue reads closed, so a witnessed close is certified instead of `STALE_PRICE intraday` (X6d). That is the real session.

## S4 - Measured

`tests/test_xcal_data_ext.py`: **PASS 52/52** as a script and under pytest.

| Case | What it checks |
| --- | --- |
| X1 | the tables, pinned literally; only XSES; every added day a 2027 weekday after the pinned bound (2026-12-31); the half days are weekday eves from 2026-12-24, closing 12:00 |
| X2 / X2b | unset and eleven off words read off (every MIC None); six on-spellings read on |
| X3 | on: the extended XSES equals the pinned XSES on all 5,059 pinned sessions, except the two 2026 half days, whose close moves 09:00Z -> 04:00Z. 2027 has 253 sessions (its weekdays minus the eight), all opening 01:00Z and closing 09:00Z except the three half days (04:00Z). Bound 2027-12-31; `previous_close` on 2027-01-04 is the 2026-12-31 half-day close; one instance per process; other MICs and unknown names read None |
| X3b | a failing import reads None (never raises); a table the package already covers retires itself |
| X4 / X4b | tracker off = v6.53.0 under the off words (the 18 Dec defect pinned); on changes exactly the 21 Singapore windows past 2026 and re-times the two half days in 3 earlier windows |
| X5a | D05.SI due 2026-12-23, the close quote at 09:30Z: off ACTIVE `outcome_calendar_unknown` (the defect); on MATURED WIN, `target_session_close` 2026-12-23T09:00Z |
| X5b | D05.SI due 2027-01-08: off unknown; on MATURED |
| X5c | D05.SI due on 8 Feb 2027 (closed): on, the target session is 9 Feb and its close matures it |
| X5d | AAPL.US, 2222.SR, VOD.L: identical on and off |
| X5e | the 24 Dec 2026 half day. An 11:00 SGT target matures on the 12:06 SGT print (`target_session_close` 04:00Z); a 13:00 SGT target belongs to Mon 28 Dec and matures there. Off, both read `outcome_calendar_unknown` |
| X6a | builder `_venue_state('D05.SI', 2027-01-04 03:00Z)`: off None; on (open, 2026-12-31 04:00Z) |
| X6b | a .SI quote at the 31 Dec 2026 half-day close (12:06 SGT) judged on Fri 1 Jan 2027: off `UNVERIFIED_PRICE ... venue calendar unavailable` (the defect); on certified "last close" |
| X6c | other venue states identical on and off |
| X6d | 24 Dec 2026 at 14:00 SGT: off the pinned calendar says the venue trades (`STALE_PRICE intraday`); on it is closed and the 12:06 print is certified |
| X7 | golden negative: X5a-on, X5e-on and X6b-on FAIL on the untouched main copies |
| X8 | both tracker steps map the gate exactly once with `\|\| 'off'`, identically; the reader's fallback literal is "off". ci.yml, parsed as YAML: the heavy job carries the step, with no `if` and no `continue-on-error` (on the job or the step), `TFB_TEST_REQUIRE_BASE: "1"`, the full base SHA fetched, and the pytest line last |
| X9 | retirement: once the pinned package records XSES 2027 the extension returns None, and the defect / bound legs SKIP instead of failing that upgrade |
| V1 / V1b | version floors, the three verifier pins, title line, WHY order, ASCII files. Against the base: only the version lines and the one re-indented `get_calendar` line removed, added lines ASCII (the title aside), exactly one new function per file (skips once an open PR's block is merged in) |

Golden negative of the whole files (`TFB_TEST_TRACK_DELIVERED` / `TFB_TEST_OB_DELIVERED` = the main copies): **9 failed, 42 passed, 1 skipped**.
- The failures are X4b, X5a, X5b, X5c, X5e, X6a, X6b, X6d and V1.
- The passes are the new module's own cases (which have no base), off-identity cases (equal by construction), X5d / X6c, X7 (which asserts the base's failure), X8 and X9.
- The skip is V1b.

Mutants, each run against the harness in its own copy of the tree: **21 / 21 killed**. The failing-test counts are in the PR body.
1. the reader defaulting on;
2. any word turning it on;
3. the tracker hook dropped;
4. the builder hook dropped;
5. the 8 Feb closure dropped;
6. Chinese New Year on 9 Feb instead;
7. the bound guard dropped;
8. every MIC extended;
9. an error raising out of `extended_calendar`;
10. the pinned holidays replaced instead of extended;
11. the bound not extended;
12. the builder bypassing the gate;
13. the tracker bypassing the gate;
14. daily_sync.yml unmapped;
15. the half days dropped;
16. the half days closing at 17:00;
17. the 24 Dec 2026 half day dropped;
18. `|| true` after the CI pytest line;
19. `continue-on-error: true` on the CI step;
20. `if: ${{ false }}` on the CI step;
21. "enable" read as on.

Other checks: see the PR body (compileall, ruff, `render_env_contract --check`, the affected suites, the ci.yml lean list and the contract-lane emulation on an export of this tree).

## S5 - Owner steps (proposed; nothing is armed here)

1. Merge.
2. Set the repository Variable `TFB_XCAL_DATA_EXT=on` **before Fri 18 Dec 2026** (the tracker lane).
3. Set the Render env `TFB_XCAL_DATA_EXT=on` and deploy **before Thu 31 Dec 2026**, the last pinned close (the builder lane). Before Thu 24 Dec also gives that half day its 12:00 SGT close.
4. After 18 Dec, receipts on .SI / .SG records should show `"calendar": "XSES"` with a `target_session` instead of `outcome_calendar_unknown`.

Arming is safe early: every pinned session up to 2026-12-23 is unchanged (X3), and the tracker reaches the two half days only through windows that would otherwise raise.

Rollback: unset the Variable or the Render env.

## S6 - Known limits and not done

- **Earlier half days stay at 17:00.** Only the half days from 2026-12-24 on are modelled. The earlier ones (2025-12-24, 2025-12-31, 2026-02-16, and 2025-01-28 too) stay as the pinned calendar has them; #922 re-times three of them.
- **A later SGX closure** (a polling day announced in 2027) needs a new row.
- **The new bound is the 2027-12-31 close.** The tracker's window reaches it from 2027-12-18, so the 2028 rows are needed before then unless a package release carries them. The extension retires itself once the pinned table covers its year (X9).
- **XSHG (.SS, and .SZ in #778) and XBOM (.BO)** end at 2026-12-31 too. In the builder their rows read `venue calendar unavailable` from 31 Dec 2026 until a package or this table carries 2027. #915 adds .SS / .BO to the tracker's map and meets the same bound.
- **#922 interplay.** #922 carries no 2026 SGX half-day rows. Its table has 2025-12-24, 2025-12-31 and 2026-02-16 only, and it set the Dec 2026 ones aside as unreachable. This extension now models 24 and 31 Dec 2026 itself, so there is no overlap. #922's T1 checks each row against the pinned calendar, so a 2027 XSES row there would need its check moved to the extended calendar.
- **#778 interplay.** #778 adds `_venue_next_close`, which reads the pinned calendar directly. On a merged tree with the gate on, .SI rows in 2027 lose the "next close" time in the freshness note and alert; no verdict moves. Whichever PR lands second sends both lookups through one helper (cache, then `_venue_cal_data_ext`).
- **Not done:**
  - no gate armed and no Variable set;
  - the flag is not registered in verify_deployment FLAGS or main.py `_OB_GATE_ENVS` (optional, owner choice; the earlier builder gates `TFB_TICKET_VENUE_CAL_EXT` and `TFB_OPP_RANK_MODE` are not registered either);
  - no change to the maturation rule, the receipt schema, the cron or the concurrency;
  - no live workbook read or write and no provider call (everything is offline).

## S7 - Review round (2026-11-06)

An independent four-lens review ran before the PR opened: calendar correctness, tracker integration, builder / Render integration, and CI with the claims. A refuter re-checked every finding above nit, and none was refuted.

**Fixed:**
- **(major) The half days.** At the first head (7084f25) the extension made the SGX half days reachable with the calendar's 17:00 close. With the gate on and `TRACK_HIST_TARGET_CLOSE=enforce`, a record due at 13:00 SGT on 24 Dec 2026 would have matured on a bar struck at 12:00 SGT, one session early. The headline cases had also used a 17:00 SGT quote that cannot exist on those days.
  - Now `XCAL_HALF_DAYS` closes the five half days at 12:00 SGT.
  - X5a moves to 23 Dec; X6b uses the real 12:06 print.
  - New X5e and X6d pin the half days. Mutants 15-17 kill their removal.
- **(major) The verifier pin.** The new verifier pin had no `_MODULE_FILES` mapping, so the blocking lean-unit test `test_verify_manifest_pins` failed. Fixed in 41365f6 and caught independently by the lean-lane emulation.
- **(minor) X8 now parses ci.yml.** Disabling the step (`|| true`, `continue-on-error`, `if: false`) survived the text check. Mutants 18-20 now fail.
- **(minor) S6 wording on #922.** S6 said #922 carries the 2026 SGX half days; it does not (corrected above).
- **(minor) The #778 `_venue_next_close` interplay** is recorded in S6 and in the builder's WHY merge note.
- **(nit) The 2027-12-18 cliff** is named in the module's KNOWN LIMITS (4) and the tracker's KNOWN LIMITS (4).
- **(nit) Package upgrades.** The defect and bound legs now SKIP once a package release records XSES 2027 (X9), so the upgrade is not blocked by this harness.
- **(nit) "enable"** is an off word in the test list (mutant 21).
- **(nit) The function count** is corrected (S2).

**Left as it is:**
- **(nit) Arming visibility.** The Render arming is not visible in the verifier or /health. That would mean registering a FLAGS entry, which is an owner choice; precedent leaves builder gates unregistered.
