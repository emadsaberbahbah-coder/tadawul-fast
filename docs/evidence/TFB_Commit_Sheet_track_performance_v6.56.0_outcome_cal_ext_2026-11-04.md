# TFB Commit Sheet - track_performance v6.56.0 [OUTCOME-CAL-EXT]

| Item | Value |
| --- | --- |
| Date | 2026-11-04 (daily improvement routine) |
| Branch | `claude/daily-2026-11-04` |
| Base | origin/main `c54eba40` (track_performance v6.53.0, track_performance.yml 1.0.7, daily_sync.yml 1.0.10) |
| Lane | GitHub Actions only (`track_performance.yml`, `daily_sync.yml`); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TRACK_OUTCOME_CAL_EXT` = on / 1 / true / yes; default off = v6.53.0 byte for byte (the version string aside) |
| Priority | (a) an outcome number is missing: forecasts on 30 venues can never mature, so they never reach Realized ROI, WIN/LOSS or the Directional Win Rate |
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
- `_VENUE_CAL_EXT_MAP` (v1.26.2 / v1.27.2, 39 venues, every MIC instantiated in the pinned exchange_calendars 4.13.2).

`.NS` is deliberately absent there, because XBOM does not model NSE's special sessions. 30 of the builder's suffixes are missing from the tracker, and none of them disagrees with a key the tracker already has.

Offline, with the real `audit_active_records` loop and the real pinned exchange_calendars:
- **main:** a due record on WALMEX.MX / 1155.KL / NPN.JO / SAN.MC / PETR4.SA / 2330.TW / RYA.IR / ETE.AT / SAP.F / 500325.BO / 600519.SS / PTT.BK / BBCA.JK, with a witnessed quote at its target-session close, stays ACTIVE with `outcome_calendar_unknown`.
- **v6.56.0, gate on:** the same record MATURES (WIN), and its receipt names the MIC (harness C4).

Overlap checked:
- #857 (6.55.0, outcome tally) only reads the base map; its `_outcome_tally_calendar` repeats the lookup.
- #845 (6.54.0) does not touch it.
- #778 / #779 add a builder-only `.SZ` entry behind `TFB_OPP_VENUE_CAL_EXT`. They don't change the tracker.

## S2 - Change

| Site | Change |
| --- | --- |
| `scripts/track_performance.py` | v6.56.0; the title line and `SCRIPT_VERSION`; a dated WHY block after the v6.53.0 block and before `SCRIPT_VERSION` (this file stacks oldest first); prior blocks verbatim |
| `_OUTCOME_CALENDARS_EXT` | added: the builder's two maps minus the tracker's keys (30 entries). The harness pins the parity: F MC MX JK KQ TW TWO KL BO BA SN BK JO SA IR CN NE SS IC PR BD LM PH TL RG VS CR RO PK AT |
| `_OUTCOME_CAL_EXT_EXCLUDED` | added: NS SZ CA QA AE AD KW BH, which stay unknown (documentation; the harness checks they are in neither map) |
| `_outcome_cal_ext_enabled` | added: `TRACK_OUTCOME_CAL_EXT` in on / 1 / true / yes (fallback literal "off") |
| `_outcome_sessions` | two added lines: `if name is None and _outcome_cal_ext_enabled(): name = _OUTCOME_CALENDARS_EXT.get(suffix)`. The base map always wins, and every refusal before and after (`^`, `=`, `-USD`, bare non-letter names, the US share-class dot rule) is unchanged |
| `.github/workflows/track_performance.yml` | 1.0.10 header; `TRACK_OUTCOME_CAL_EXT: ${{ vars.TRACK_OUTCOME_CAL_EXT \|\| 'off' }}` in the 'Run performance clock' step env, after `TRACK_AUDIT_DUE_ONLY`. **Not armed.** |
| `.github/workflows/daily_sync.yml` | the same mapping in the tracker step (ENV PARITY), after `TRACK_AUDIT_DUE_ONLY`. **Not armed.** |
| `scripts/verify_deployment.py` | pin track_performance 6.56.0 (pin only) |
| `.github/workflows/ci.yml` | 1.1.68: a heavy-job step after the outcome-evidence step runs the harness. It needs the pinned exchange_calendars, which only that job installs, and it fetches `c54eba40` for the golden legs |
| `tests/test_track_performance_v6530_archive_bar.py` | the exact 6.53.0 pins become floors; the title and function-set checks read the delivered version (the same edits #857 makes) |

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
- every extension suffix;
- the excluded venues;
- indices, futures, FX, crypto and bare digits.

Under ten off words (unset, blank, off, 0, false, no, enforce, observe, onn, y) it returns or raises exactly as the untouched v6.53.0 (`git show c54eba40`), sessions included (C2).

The real audit loop on base venues (AAPL.US, 2222.SR, VOD.L, 7203.T) gives the same record state and receipt as v6.53.0 (the receipt's `tracker_version` aside), with the gate off and on (C4b). For an extension venue, the gate-off record equals the base's (C4).

## S4 - Measured

`tests/test_track_outcome_cal_ext.py` passes **PASS 43/43** as a script and under pytest.
- Without git history: 40 passed, 3 skipped (C2, G1 and V1b need the base).
- The contract-lane emulation skips the file: it needs exchange_calendars and pandas, so it runs in the heavy job.

| Case | What it checks |
| --- | --- |
| C1 | parity with the builder's two maps; no overlap with the base map; the excluded venues in neither; every MIC instantiates |
| C2 / C2b / C2c | off = v6.53.0 over the 80-symbol matrix and ten off words; the six on-spellings; junk (enabled, 2, y ...) stays off |
| C3 | on: every extension suffix maps to its MIC; base symbols and every refusal unchanged; NS SZ CA QA AE AD KW BH unknown; BRK.B stays XNYS; SAP.F is XFRA |
| C4 | on, the REAL audit loop: 13 venues mature (WIN, receipt `calendar` = MIC); off they stay ACTIVE `outcome_calendar_unknown`, equal to v6.53.0 |
| C4b | base venues: identical on and off, and equal to v6.53.0 |
| C5 | on: a quote one hour before the calendar close (XMEX, BVMF) is refused `quote_before_target_close`, and the record stays ACTIVE |
| C6 | on: .BO / .SS targets whose window passes the 2026-12-31 calendar bound stay `outcome_calendar_unknown`; a November target works |
| C7 | both tracker steps map the gate exactly once with `\|\| 'off'`, identically; the reader's fallback literal is "off"; ci.yml's heavy job runs the file and fetches the full base SHA |
| G1 | golden negative: v6.53.0 with the gate set leaves WALMEX.MX ACTIVE `outcome_calendar_unknown` |
| V1 / V1b | version floor, verifier pin, title line, WHY order, ASCII test file; against the base: only the title and `SCRIPT_VERSION` lines removed, the added lines ASCII apart from the title, exactly one function added |

Golden negative of the whole file (`TFB_TEST_TRACK_DELIVERED=` the v6.53.0 copy): **36 failed, 6 passed, 1 skipped**. The 6 that pass are C2 and C4b x4, which compare with the base by construction, and G1, which asserts the base's defect. V1b applies to 6.56.0 only.

Mutants, each run against the harness: **10 / 10 killed**:
1. the gate check dropped;
2. NS -> XBOM added;
3. F dropped;
4. the reader accepting any word;
5. the reader defaulting on;
6. the bare-digit refusal dropped;
7. MX mapped to XNYS;
8. MX excluded;
9. the extension overriding a base venue;
10. SS mapped to XHKG.

Other checks:
- tracker, workflow and flag suites (outcome evidence, performance math and summary, sync outcome audit, board coverage, flags mapped, force coverage, v6.53.0 archive bar, canonical publications, hist target close, log read precision, record-only workflows, verifier pins, optional rollout flags, workflow flag mappings, flag registry, workflow audit, env contract, scheduled portfolio refresh, stamp budget): 390 passed, 2 skipped, 1 failed. The failure is `test_daily_sync_stamp_budget::test_b4_env_mapping_only_against_base`, which fails the same way on a clean main worktree (c54eba40); this PR does not cause it.
- `compileall` (main.py core routes scripts tests): rc 0. ruff E9/F63/F7/F82 on the changed .py files: clean. `audit_repository_workflows.py`: errors=0 warnings=0. `render_env_contract.py --check`: OK.
- The ci.yml lean list on an export of this tree: see the PR body.

## S5 - Owner steps (proposed; nothing is armed here)

1. Merge. This is GitHub Actions only: no deploy, no paste.
2. Set the repository Variable `TRACK_OUTCOME_CAL_EXT=on`.
3. After the next Performance Clock audit pass, check:
   - the Outcome receipts on records with those suffixes read `"calendar": "<MIC>"`;
   - each either matures or carries an ordinary session reason (`quote_outside_target_session`, `target_session_not_complete`, `quote_before_target_close` ...) instead of `outcome_calendar_unknown`;
   - `[PERF-VERDICT]` `matured` counts them.

Rollback: unset the Variable. Records that matured meanwhile keep their receipts, and `calendar` names the MIC used.

## S6 - Known limits and not done

- exchange_calendars 4.13.2 ends XBOM (.BO) and XSHG (.SS) on 2026-12-31 (the others one year ahead). A target whose +/-14-day session window passes that bound stays `outcome_calendar_unknown`: it fails closed (C6). A library upgrade extends it.
- XMEX keeps 08:30-15:00 Mexico City and BVMF 10:00-18:00 Sao Paulo all year. If a venue's real close is ever earlier than its calendar's (for example one that follows New York's daylight-saving hours), a quote at the real close is refused `quote_before_target_close`. That means fewer maturations, never a wrong one (C5). I did not verify BMV's current hours from here.
- A special session the calendar does not model (a Muhurat hour on .BO) refuses its quote the same way.
- Still unknown with the gate on: .NS (the builder's review keeps it out), .SZ (no XSHE in 4.13.2), .CA, .QA, .AE, .AD, .KW and .BH, and suffix-less non-US names (a bare name that does not start with a letter).
- #857's outcome tally (`_outcome_tally_calendar`) repeats the base lookup. Whichever of the two lands second must consult the extension there too, so the tally names the calendar the maturation used.
- No change to the base map, the maturation rule, the receipt schema, the cron or the concurrency. No live workbook read or write, no provider call: everything is offline with stubs.
