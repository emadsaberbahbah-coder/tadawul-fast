# TFB Commit Sheet - track_performance v6.56.0 [OUTCOME-CAL-EXT]

| Item | Value |
| --- | --- |
| Date | 2026-11-04 (daily improvement routine) |
| Branch | `claude/daily-2026-11-04` |
| Base | origin/main `c54eba40` (track_performance v6.53.0, track_performance.yml 1.0.7, daily_sync.yml 1.0.10) |
| Lane | GitHub Actions only (`track_performance.yml`, `daily_sync.yml`); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TRACK_OUTCOME_CAL_EXT` = on / 1 / true / yes; default off = v6.53.0 byte for byte (the version string aside) |
| Priority | (a) an outcome number is missing: forecasts on venues outside the 32-suffix map can never mature, so they never reach Realized ROI, WIN/LOSS or the Directional Win Rate. This build lets 18 of them mature and denies 12 (S7) |
| Version | 6.56.0. 6.54.0 is claimed by open #845 and 6.55.0 by open #857 (both mine); whichever lands later re-numbers. track_performance.yml 1.0.10 (1.0.8 / 1.0.9 claimed by #845 / #857); ci.yml 1.1.68 (1.1.62-1.1.67 claimed by #778, #882, #889, #897, #909) |

## S1 - Defect and evidence

`_outcome_sessions` (scripts/track_performance.py) is the tracker's only exchange-calendar lookup. It sits behind three things:
- the live-quote session check (`_outcome_quote_session`);
- the target session (`_outcome_target_session`);
- the v6.47.0 historical close.

It knows 32 suffixes (`_OUTCOME_CALENDARS`). Any other suffix raises `outcome_calendar_unknown`, and a record with that reason stays ACTIVE / PENDING for ever. It never matures, never expires, and never reaches Realized ROI, WIN/LOSS or the Directional Win Rate.

Live:
- The 31 Oct audit receipts counted about 100 `outcome_calendar_unknown` records ("MX and suffix-less names", the v6.53.0 WHY block).
- The 4 Nov 07:53Z Performance Clock run 41795448626 records MEGACPO.MX again (its ACTION-TREND list reads MEGACPO.MX ACCUMULATE, stable 5d, `[STABLE, 106 obs, flips=0]`; DEDUP-SIGNAL sample `MEGACPO.MX|20261104`). Its forecasts can never be scored.
- The same run: `[PERF-VERDICT v6.53.0] ... matured 9 | pending 5844`.

Of the 45 venue suffixes `v_outcome_engine` knows, 21 have no calendar here: MC AT IR NS BO KQ TW SS SZ KL JK BK JO MX SA CA QA AE AD KW BH.

`core/analysis/opportunity_builder.py` already ships reviewed venue calendars:
- `_VENUE_CAL_MAP` (16 venues);
- `_VENUE_CAL_EXT_MAP` (v1.26.2 / v1.27.2, 40 venues, every MIC instantiated in the pinned exchange_calendars 4.13.2).

The builder's v1.26.2 WHY records that the live Global_Markets roster trades on about 25 venues outside its base map (Render logged `venue calendar unavailable` for them on every scheduled sync).

`.NS` is deliberately absent there, because XBOM does not model NSE's special sessions. 30 of the builder's suffixes are missing from the tracker, and none of them disagrees with a key the tracker already has.

Offline, with the real `audit_active_records` loop and the real pinned exchange_calendars:
- **main:** a due record on 1155.KL / SAN.MC / 2330.TW / RYA.IR / ETE.AT / 500325.BO / 600519.SS / PTT.BK / BBCA.JK / WEED.CN / MAREL.IC / OTP.BD / LHV1T.TL, with a witnessed quote at its target-session close, stays ACTIVE with `outcome_calendar_unknown`.
- **v6.56.0, gate on:** the same record MATURES (WIN), and its receipt names the MIC (harness C4). Under `TRACK_HIST_TARGET_CLOSE=enforce` an extension record whose target session closed days earlier matures on that session's bar (C5b).
- **MEGACPO.MX stays unscored in this build.** .MX is DENIED (S7): its calendar closes an hour after BMV's real close from March to November.

Overlap checked:
- #857 (6.55.0, outcome tally) only reads the base map; its `_outcome_tally_calendar` repeats the lookup.
- #845 (6.54.0) does not touch it.
- #778 / #779 add a builder-only `.SZ` entry behind `TFB_OPP_VENUE_CAL_EXT`. They don't change the tracker.

## S2 - Change

| Site | Change |
| --- | --- |
| `scripts/track_performance.py` | v6.56.0; the title line and `SCRIPT_VERSION`; a dated WHY block after the v6.53.0 block and before `SCRIPT_VERSION` (this file stacks oldest first); prior blocks verbatim |
| `_OUTCOME_CALENDARS_EXT` | added: the builder's two maps minus the tracker's keys minus `_OUTCOME_CAL_EXT_DENIED` (18 entries). The harness pins the parity and checks every MIC against a literal table: MC JK TW TWO KL BO BK IR CN NE SS IC BD TL RG VS RO AT |
| `_OUTCOME_CAL_EXT_DENIED` | added: MX SA LM BA F PH CR PK SN JO KQ PR, builder keys the tracker refuses (S7). They stay `outcome_calendar_unknown` |
| `_OUTCOME_CAL_EXT_EXCLUDED` | added: NS SZ CA QA AE AD KW BH, which stay unknown (in neither map). Both lists are pinned both ways |
| `_outcome_cal_ext_enabled` | added: `TRACK_OUTCOME_CAL_EXT` in on / 1 / true / yes (fallback literal "off") |
| `_outcome_sessions` | two added lines: `if name is None and _outcome_cal_ext_enabled(): name = _OUTCOME_CALENDARS_EXT.get(suffix)`. The base map always wins, and every refusal before and after (`^`, `=`, `-USD`, bare non-letter names, the US share-class dot rule) is unchanged |
| `.github/workflows/track_performance.yml` | 1.0.10 header; `TRACK_OUTCOME_CAL_EXT: ${{ vars.TRACK_OUTCOME_CAL_EXT \|\| 'off' }}` in the 'Run performance clock' step env, after `TRACK_AUDIT_DUE_ONLY`. **Not armed.** |
| `.github/workflows/daily_sync.yml` | the same mapping in the tracker step (ENV PARITY), after `TRACK_AUDIT_DUE_ONLY`. **Not armed.** |
| `scripts/verify_deployment.py` | pin track_performance 6.56.0 (pin only) |
| `.github/workflows/ci.yml` | 1.1.68: a heavy-job step after the outcome-evidence step runs the harness. It needs the pinned exchange_calendars, and this is the only blocking job that installs it (the advisory whole-suite job installs it too). The step fetches `c54eba40` for the golden legs and sets `TFB_TEST_REQUIRE_BASE=1`, so a missing base FAILS them instead of skipping |
| `tests/test_track_performance_v6530_archive_bar.py` | the exact 6.53.0 pins become floors; the title and function-set checks read the delivered version (the same code #857 has; only the comment lines differ, so a merge with #857 gives two comment-only conflict hunks) |

Functions: 1 added, 0 removed.

Unchanged in every mode:
- the base map;
- every refusal;
- the maturation rule itself (first exchange-session close at or after the target, never a later session);
- the receipt schema `tfb.outcome-price.v1`;
- the CA guard;
- the cron and concurrency.

## S3 - Off identity

`_outcome_sessions` was run over a matrix of 80 symbols:
- base suffixes (US bare and dotted, share classes, LSE / XETRA / JSE / SG / KO aliases);
- every extension and denied suffix;
- the excluded venues;
- indices, futures, FX, crypto and bare digits.

Under ten off words (unset, blank, off, 0, false, no, enforce, observe, onn, y) it returns or raises exactly as the untouched v6.53.0 (`git show c54eba40`), sessions included (C2).

The real audit loop on base venues (AAPL.US, 2222.SR, VOD.L, 7203.T) gives the same record state and receipt as v6.53.0 (the receipt's `tracker_version` aside), with the gate off and on (C4b). For an extension venue, the gate-off record equals the base's (C4g).

## S4 - Measured

`tests/test_track_outcome_cal_ext.py` passes **PASS 64/64 (1 skipped)** as a script and under pytest. The one skip is C8, the #857 tripwire, which skips while `_outcome_tally_calendar` is absent.
- Without git history (an export of the tree): 51 passed, 14 skipped (C2, C4g x4, C4bg x4, G1 x3, V1b and C8). With `TFB_TEST_REQUIRE_BASE=1`, as ci.yml sets it, those 13 base legs fail instead (13 failed, 51 passed, 1 skipped).
- The contract-lane emulation skips the file: it needs exchange_calendars and pandas, so it runs in the heavy job.

| Case | What it checks |
| --- | --- |
| C1 | parity with the builder's two maps minus the base keys minus DENIED; no overlap with the base map; DENIED and EXCLUDED pinned both ways; DENIED keys are builder keys; EXCLUDED in neither map; every MIC instantiates |
| C2 / C2b / C2c | off = v6.53.0 over the 80-symbol matrix and ten off words; the six on-spellings; junk (enabled, 2, y ...) stays off |
| C3 | on: every extension suffix maps to the MIC of a literal table (an anchor independent of the module constant); base symbols, every refusal and every DENIED symbol unchanged and unknown; BRK.B stays XNYS |
| C4 / C4g | on, the REAL audit loop: 13 venues mature (WIN, receipt `calendar` = MIC); off they stay ACTIVE `outcome_calendar_unknown`; C4g: that off record equals v6.53.0's |
| C4b / C4bg | base venues: identical on and off; C4bg: and equal to v6.53.0 |
| C5 | on: a quote one hour before the calendar close (XKLS, XDUB) is refused `quote_before_target_close`, and the record stays ACTIVE |
| C5b | on, `TRACK_HIST_TARGET_CLOSE=enforce`, the real loop with a stub bar source: .KL .MC .CN .BO records whose target session closed days earlier mature on THAT session's bar (ROI 5.0, `hist_bar_date`, `quote_asof` = the calendar close); .MX .SA .F records with a target 30 minutes before the calendar close stay `outcome_calendar_unknown`, and no bar is read |
| C5c | on, the four cases the venue audit confirmed, replayed in the real loop at their own clock with a regular-market quote: .KQ on the 19 Nov 2026 exam day (quote 06:40Z = 15:40 KST), .PR on 5 Jan 2027 (quote 16:27 CET), .JO on the 24 Dec half day and .SN on 5 Nov under hist enforce. Each stays `outcome_calendar_unknown` with no bar read |
| C6 | on: .BO / .SS lookups work on 2026-12-17 and raise from 2026-12-18 (also 24 and 31 Dec); a target there stays `outcome_calendar_unknown`; the pinned bound is 2026-12-31 |
| C7 | both tracker steps map the gate exactly once with `\|\| 'off'`, identically; the reader's fallback literal is "off"; ci.yml's step runs the file, fetches the full base SHA and sets `TFB_TEST_REQUIRE_BASE: "1"` |
| C8 | tripwire for #857: when `_outcome_tally_calendar` exists it must name the calendar `_outcome_sessions` uses, gate off and on |
| G1 | golden negative: C4's on-leg and C5b's matured leg FAIL on v6.53.0 with the gate set (3 venues) |
| V1 / V1b | version floor, verifier pin, title line, WHY order, ASCII test file; against the base: only the title and `SCRIPT_VERSION` lines removed, the added lines ASCII apart from the title, exactly one function added (skips once #845 / #857 blocks are merged in) |

Golden negatives:
- the whole file against the untouched v6.53.0 (`TFB_TEST_TRACK_DELIVERED=` that copy): **40 failed, 23 passed, 2 skipped**. The 23 that pass compare with the base by construction (C2, C4g x4, C4b x4, C4bg x4), assert the base's own defect (G1 x3), or assert a refusal the base also gives (C5b DENIED x3, C5c x4). The skips are C8 and V1b;
- C1, C3 and C5b-DENIED against this PR's previous head `2a645313`, where MX SA F were still mapped: **5 failed**. Under hist enforce that head matured WALMEX.MX and PETR4.SA (WIN) on the 6 Oct session's bar for a target at 2026-10-06 20:30Z, because XMEX / BVMF put that session's close at 21:00Z. BMV and B3 closed at about 20:00Z that day, before the target (the `[HIST-CLOSE]` line reads `witnessed=1 matured=1`);
- the whole file against the second head `16b49b9a`, where SN JO KQ PR were still mapped: **6 failed** (C1, C3, C5c x4). There every C5c case MATURED (WIN): 035720.KQ on a 15:40 KST quote, about 50 minutes before KRX's 16:30 exam-day close; CEZ.PR on a 16:27 CET continuous-trading quote; NPN.JO on the 24 Dec bar (struck at 12:00 SAST, before its 14:00 target); SQM-B.SN on the 5 Nov bar (struck at 16:00 local, before its 16:30 target). Against `2a645313`: 9 failed (the same plus C5b DENIED x3);
- C8 against this file plus #857's `_outcome_tally_calendar` as it stands: **fails**; with the tally consulting the extension: passes.

Mutants, each run against the harness: **12 / 12 killed**:
1. MX -> XMEX added back (3 failed);
2. KL dropped (5);
3. TWO -> XKRX (2);
4. NS dropped from EXCLUDED (1);
5. the reader defaulting on (21);
6. the gate check dropped (19);
7. PK dropped from DENIED (1);
8. the extension consulted before the base map (1, V1b only: behaviour-equivalent, because C1 proves the two maps share no key);
9. to 12. SN, KQ, JO or PR mapped again (3 each: C1, C3, C5c).

Other checks:
- tracker, workflow and flag suites, on the final tree (stamp budget, outcome evidence, workflow audit, sync outcome audit, flags mapped, v6.53.0 archive bar, hist target close, record-only workflows, workflow audit repairs, workflow flag mappings and the verifier / flag-registry / env-contract / rollout suites): 278 passed, 2 skipped, 1 failed. The failure is `test_daily_sync_stamp_budget::test_b4_env_mapping_only_against_base`, which fails the same way on a clean main worktree (c54eba40); this PR does not cause it.
- `compileall` (main.py core routes scripts tests): rc 0. ruff E9/F63/F7/F82 on the changed .py files: clean.
- The ci.yml lean list on an export of this tree: see the PR body. The contract-lane emulation over the contract job's 25 files: 418 passed, 21 skipped.

## S5 - Owner steps (proposed; nothing is armed here)

1. Merge. This is GitHub Actions only: no deploy, no paste.
2. Set the repository Variable `TRACK_OUTCOME_CAL_EXT=on`.
3. After the next Performance Clock audit pass, check:
   - the Outcome receipts on records with those suffixes read `"calendar": "<MIC>"`;
   - each either matures or carries an ordinary session reason (`quote_outside_target_session`, `target_session_not_complete`, `quote_before_target_close` ...) instead of `outcome_calendar_unknown`;
   - `[PERF-VERDICT]` `matured` counts them.

Rollback: unset the Variable. Records that matured meanwhile keep their receipts, and `calendar` names the MIC used.

## S6 - Known limits and not done

- exchange_calendars 4.13.2 ends XBOM (.BO) and XSHG (.SS) on 2026-12-31, and every lookup reads +/-14 days around its day. So from **2026-12-18** every .BO / .SS lookup raises again and those records stay `outcome_calendar_unknown` (fail closed, C6) until the pinned package is bumped. The other calendars' bound is one year after the day they are built, so it moves forward with the clock.
- A calendar close that differs from the venue's real close is unsafe either way. A later one lets the historical-close path score a session that closed before the target; an earlier one lets an intraday quote pass as the close. The DENIED list removes the cases known here. The rest use the builder's reviewed calendars as they are.
- A special session the calendar does not model (a Muhurat hour on .BO, on a day XBOM reads as closed) is refused as `quote_session_unknown`: no calendar session contains the quote.
- Still unknown with the gate on: the 12 DENIED suffixes; .NS (the builder's review keeps it out), .SZ (no XSHE in 4.13.2), .CA, .QA, .AE, .AD, .KW and .BH; and suffix-less non-US names (a bare name that does not start with a letter).
- #857's outcome tally (`_outcome_tally_calendar`) repeats the base lookup. C8 fails once both are merged until the tally consults the extension too.
- **Not fixed here (base map, pre-existing, time-bound):** the same calendar gaps hit base venues on main today, with no gate. XKRX (`.KS`, `.KO`) reads 19 Nov 2026 as 09:00-15:30, but KRX trades 10:00-16:30 on exam day; on main `c54eba40` a 005930.KS or 005930.KO record targeting that day matures on a 15:40 KST regular-market quote (replayed offline with C5c's KQ scenario). XJSE (`.JSE`) reads 24 and 31 Dec as 17:00 closes; under hist enforce an NPN.JSE record matures on the 12:00 SAST half-day bar for a 14:00 target. The exam-day case needs a quote stamp between 06:30Z and 06:45Z (a delayed 01:30Z clock run or a manual audit). A tracker-side override for those dates is the fix; it is proposed as the next item, due before 19 Nov.
- Not fixed here (builder, pre-existing): `core/analysis/opportunity_builder` maps the same denied suffixes for its quote-freshness check. There, .F (XFRA's 17:30 close) can certify an intraday Frankfurt quote as "last close", and .MX / .SA can mark a real-close quote STALE. Proposed as a separate builder item.
- No change to the base map, the maturation rule, the receipt schema, the cron or the concurrency. No live workbook read or write, no provider call: everything is offline with stubs.

## S7 - Pre-merge review (2026-11-04)

An internal multi-lens review of `2a645313` confirmed two MAJOR wrong-maturation paths and several minors. All were fixed in the second commit.

| Finding | Fix |
| --- | --- |
| MAJOR: .SA (BVMF) and .MX (XMEX) close at 21:00Z all year, but B3 and BMV close at about 20:00Z while New York is on daylight time. Under `TRACK_HIST_TARGET_CLOSE=enforce`, a target between the two closes matured on a session that closed before the target | MX and SA DENIED. LM and BA are denied too: they are the same fixed-UTC pattern, and their hours could not be verified offline. C5b proves the hist lane refuses them |
| MAJOR: .F is mapped to XFRA (Xetra 09:00-17:30), but Yahoo's .F is the Frankfurt floor (08:00-22:00). A regular-market quote inside the 15-minute close slack matured on an intraday price | F DENIED |
| .PH .CR .PK are not Yahoo's code for the builder's venue (Manila is .PS; .CR is Caracas, not Zagreb; .PK is the legacy US Pink Sheets suffix), and the symbol layer produces none of them | DENIED |
| .BO / .SS: from 2026-12-18 every lookup is refused, not only targets near the bound | WHY limit (1) and S6 corrected; a C6 leg pins 17 vs 18 Dec |
| #857 tally collision documented only | C8 tripwire (proved failing on #857's current tally) |
| V1b's exact diff breaks after a re-merge over #845 / #857 | it skips when their WHY blocks are present |
| golden legs dropped out silently (`if b is not None`) | split into C4g / C4bg on `base()`; ci.yml sets `TFB_TEST_REQUIRE_BASE=1`; a base given through `TFB_TEST_TRACK_BASE` must be v6.53.0 |
| G1 ended in a tautology | G1 now runs C4's on-leg and C5b's matured leg against the base inside `pytest.raises` |
| C3 compared with the module's own constant; C4 had a redundant assertion; EXCLUDED was pinned one way only | a literal EXPECTED_MIC table; the line removed; DENIED and EXCLUDED pinned both ways |
| the Muhurat reason was misstated; the sheet said 39 builder venues (40); ci.yml said "only this job installs" | reworded; 40; "the only blocking job that installs it" |
| the archive-bar test's comment lines differ from #857's (two comment-only conflict hunks) | kept: copying #857's lines would put "#857 merged as 6.55.0" into this branch, which is false here. The conflict resolves by taking either side |

### Second round: venue audit of the remaining 22 (2026-11-04)

Five checkers compared every venue still mapped at `16b49b9a` (calendar hours in the pinned 4.13.2 for January to December 2026, Fridays included) with its official 2026 trading phases, and confirmed Yahoo's suffix for each. A separate refuter re-checked each flagged venue. Web pages were read through search summaries; several exchange sites were blocked by the sandbox network, and each verdict says so.

| Venue | Verdict | Action |
| --- | --- | --- |
| .SN XSGO | confirmed: XSGO closes 17:00 local from November to February, but Santiago equities close at 16:00 all year (Rankia, Bloomberg Linea, Visfin). Under hist enforce a 16:30 target matures on a session that closed at 16:00 | DENIED; C5c SN |
| .JO XJSE | confirmed: XJSE keeps 17:00 on 24 and 31 Dec; the JSE half days close at 12:00 SAST (2024 and 2025 notices). Same hist-path error | DENIED; C5c JO |
| .KQ XKRX | confirmed: XKRX's exam-day list (`precomputed_csat_days`) ends at 2020-12-03, so 2025-11-13 and 2026-11-19 read 09:00-15:30 while KRX traded / trades 10:00-16:30. A regular-market quote inside the 15-minute slack after 15:30 matures on an intraday price | DENIED; C5c KQ. The same gap affects base `.KS` / `.KO` (S6) |
| .PR XPRA | confirmed: XPRA closes 16:20 for good; Prague moves its end of day to 17:00 from 2027-01-04 (several Czech outlets), with continuous trading to about 16:45. A 16:27 quote then matures as the close | DENIED; C5c PR |
| .AT ASEX | refuted: ASEX's 17:20 is the real end of the session; the 17:00-17:10 closing auction sets the price, and at-the-close trades run at that price until 17:20 | kept. Re-check before the June 2027 Optiq migration |
| .KL XKLS | refuted: the 16:50-17:00 closing phases fall inside the slack and carry the closing price | kept |
| the other 16 | no risk found: the calendar close equals the official close, or the closing auction ends inside the 15-minute slack with no continuous trading after the calendar close | kept |

## Addendum 2026-11-08 - merge of main `4b478e7`

Main `4b478e7` (#935-#937) was merged in with a merge commit. The ci.yml header conflicted: main took 1.1.68-1.1.70.

- ci.yml: this release is re-numbered **1.1.68 -> 1.1.73** (its header, its `v1.1.73` line and its step comment).
- The header's claim note lists the open claims after the re-numbering: 1.1.71 #926, 1.1.72 #938, 1.1.73 #915, 1.1.74 #922, 1.1.75 #923, 1.1.76 #909.
- Main's 1.1.70 / 1.1.69 / 1.1.68 blocks follow verbatim. Against main, the ci.yml diff adds lines only.
- After the merge: `tests/test_track_outcome_cal_ext.py` 64 passed, 1 skipped (the #857 leg, by design) with `TFB_TEST_REQUIRE_BASE=1`. The verifier pins, workflow flag mappings and tracker flags suites pass (49 passed). compileall and ruff (CI rules) are clean, and no conflict marker remains.
