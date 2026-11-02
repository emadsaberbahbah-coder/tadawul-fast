# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.18.0 [POST-EARNINGS] + `scripts/run_daily_brief.py` v1.19.0 [POST-EARNINGS-HELD] - 2026-10-27

**Scope:** program register **A4 part 2** (FRST post-mortem P1; program doc `TFB_Self_Correcting_Recommendation_Program_2026-10-27.md`, loop piece L6a). The Top-10 board now says when a seat has just reported earnings: an advisor tag on the seats inside their post-earnings quarantine. Observe only on the board; gated by a Script Property, default off. The daily brief, which already left such a seat out of its picks, now says so.
**Files:** `apps_script/16_Decision_Top10.gs` (1.17.1 -> 1.18.0; 514,797 -> 529,345 chars; seven anchored hunks; zero removals), `scripts/run_daily_brief.py` (1.18.1 -> 1.19.0; zero function removals), `tests/test_dt10_post_earnings.js` (new, 686 checks), `tests/test_daily_brief_post_earnings_tag.py` (new, 6 cases), `tests/js/dt10_edits_v1180.json` + `tests/js/dt10_cash_utc_history.js` (v1.18.0 hunks reversible, v1.17.1 base SHA-256 pinned; `v1170Source` now starts from `v1171Source`), version / census / self-test pins moved in nine harnesses, `scripts/verify_deployment.py` (brief manifest pin), `.github/workflows/ci.yml` 1.1.47, this sheet.

## S1 - WHY
- The first funded ticket (FRST.US, Mon 26 Oct 2026) was bought two sessions after a 22% EPS miss (Q3 reported Thu 22 Oct after the close; -4.1% and -3.7% on heavy volume the next sessions).
- Every calendar signal on the board looks forward (v1.6.0 `⚠ earnings ≤Nd`); nothing showed that a seat had just reported, let alone missed. Interim trading policy rule 3 (no entry within 5 sessions after a report, 10 after a negative reaction) has been a hand check since.
- The data now exists: `Calendar_Events` carries `Last Earnings Date | Days Since Earnings | EPS Surprise %` (calendar_sync v1.2.0 with `TFB_CAL_LAST_REPORT=on`, calendar_provider v1.3.1; v1.3.2 in PR #854 completes the US names). Read 27 Oct 14:44: 34 of 327 rows, FRST.US 2026-10-22, -22.2%.

## S2 - WHAT
`16_Decision_Top10.gs` v1.18.0 (only when `DT10_POST_EARN` is `observe` or `on`; absent / blank / anything else = off)
- Every SELECTED ticket whose symbol reported inside its quarantine gets `⚠ post-earnings S/L · EPS x% · ` on its Advisor Note, after the v1.6.0 forward tag when the note carries one (that tagger checks for its own prefix at position 0), else at the start:
  - S = trading sessions after the report date up to today (Riyadh calendar day; Mon-Fri, Sun-Thu for KSA symbols; exchange holidays not known) = the number of the session an entry today would be;
  - L = 5, or 10 when the EPS surprise is negative; a blank surprise is unknown -> 5; text surprises accept `%` and `,`.
- The report is the latest PAST date of `Last Earnings Date` and `Next Earnings Date`: the 08:07 morning render runs before the 08:15 calendar sync, so a name that reported after the previous close still shows its report as the Next Earnings Date - that case counts, surprise unknown. Reports older than 31 days are ignored (the session walk is bounded).
- One `Calendar_Events` read per render (A1 to the last column, at most 16), only when armed. Any read fault -> no tag, token `post-earn n/a`.
- Status line (B2): `post-earn ⚠N/M` (tagged / selected) after the forward `earn` token; `post-earn 0/M` when no seat is inside a quarantine; `post-earn n/a` when the calendar holds no recent report.
- A withheld row (feed NOT_ACTIONABLE) keeps both research prefixes, forward then post-earnings (`dt10UvWithholdRow_`; without a post-earnings tag the kept prefix is the v1.17.1 one - proved on a note corpus against the base).
- **Observe only on this board:** nothing gates, blocks, resizes or re-ranks. KSA names carry no last report today (EODHD has no Saudi coverage); only a past Next Earnings Date can tag one.
- Functions added (0 removed): `dt10PostEarnMode_`, `dt10WeekdaysBetween_` (pure), `dt10PostEarnMapFromValues_` (pure), `dt10PostEarnMap_`, `dt10ApplyPostEarnTags_` (pure), `dt10PostEarnAnnotate_`. Changed: `dt10RenderPayload_` (one call after the forward tag), `dt10UvWithholdRow_` (keeps the new prefix), `dt10SelfTest` (new line `post-earnings core: ok (FRST 3/10; +EPS at 10 sessions untagged; blank EPS 5/5; past next date 1/5) | mode now: off|observe`). Vars added: `DT10_POST_EARN_PROP`, `DT10_POST_EARN_SESSIONS` (5), `DT10_POST_EARN_SESSIONS_NEG` (10), `DT10_POST_EARN_MAX_DAYS` (31).

`run_daily_brief.py` v1.19.0
- A tagged note does not begin with INVEST, so `extract_top10` already left the seat out of the picks - silently. Now each tagged seat that would otherwise have been a pick (positive INVEST note under the tag, shares > 0, a confirmed stability, not held) is recorded in `top10['post_earnings_held']` and `_post_earnings_held_line(model)` prints one sentence on HTML, PDF and text, next to the v1.17.0 earnings line: `N candidate(s) held back — reported earnings recently (post-earnings quarantine, interim rule 3): FRST.US (session 3 of 10, EPS -22.2%) ... They re-enter automatically when the quarantine ends.`
- No tag on the sheet -> no key and no line: extract_top10, the text and the HTML equal v1.18.1 byte for byte (golden test against `git show e8337a16`).

## S3 - Gate / arming
| Item | Before | After | Effect |
|---|---|---|---|
| `DT10_POST_EARN` (Script Property, new) | absent | `observe` | tags + B2 token on the next render; the brief names held seats from its next run |

Arming order after merge: paste and save v1.18.0 (Claude stages it in the editor with the SHA-256 checked; the Save click is the owner's), run Self-Test (expect `[DT10 v1.18.0] self-test`, `post-earnings core: ok ... | mode now: off`, zero FAIL), then set `DT10_POST_EARN=observe`. The brief needs nothing (it runs from main). Rollback: delete the property (or `off`) with no re-paste; or re-paste v1.17.1.

## S4 - Tests
- `node tests/test_dt10_post_earnings.js` -> **ALL PASS (686 checks)**, deterministic (clock frozen at Tue 27 Oct 2026 10:00 Riyadh): **P1** kill identity - property absent / off / blank / `yes` / `1` / `onn` / `enforce` / service fault: the REAL `dt10RenderPayload_` paints the identical sheet as the v1.17.1 base (every cell, merge and border; EXECUTABLE and NOT_ACTIONABLE feed), the same status token, no extra Calendar_Events read, tickets untouched, and the only extra property read is `DT10_POST_EARN`; **P2** armed (`observe`, `on`, ` On `, `OBSERVE`): exactly five Advisor Note cells change - FRST-like 3/10 at -22.2%, blank surprise 5/5, a Riyadh-midnight Date cell 2/10, a past Next Earnings Date 1/5, a seat with both tags (forward first, then post) - each = the base note with the tag; withheld rows keep the prefixes before the withhold text; token `post-earn ⚠5/9`; one extra read; the Logger line; a second pass of either tagger adds nothing; **P3** golden negative - the v1.17.1 base, armed, shows nothing for the FRST case; **P4** pure cores - sessions (Thu->Tue 3, Fri->Mon 1, weekend 0, four weeks 20, pre-1970, Sun-Thu for Tadawul, the 366-day bound), the map (title row before the header, duplicates first-wins, numeric / `%` / `,` text, unicode-minus text unknown, future / same-day / invalid / timestamp / serial dates skipped, next vs last precedence, 31 vs 37 days, 5,000 ancient rows skipped fast, huge values clamped), the 5 / 10 boundaries, formatting, idempotency, withheld-note identity with the base on a corpus; **P5** tokens (`n/a` with no past report / no tab / read fault, `⚠1/9` from the next-date fallback alone, `0/9` when nothing is inside); **P6** the embedded self-test line off / observe / ON; **P7** version lockstep, WHY newest-first and ASCII, the v1.17.1 WHY verbatim, ES5/ASCII lint of the 216 added lines, census 218 -> 224 functions and 117 -> 121 vars with exactly the designed names, seven hunks whose reversal is SHA-256 `d209ca3e...`.
- `tests/test_daily_brief_post_earnings_tag.py`: 6 passed - parse, the held list (GRACE / zero shares / withheld / held names never named), the exact line, golden byte-identity with v1.18.1 on untagged boards (extract, text, HTML), the line on text / HTML / PDF and its absence in v1.18.1. With `test_daily_brief_blocked_inputs.py`, `test_release_decision_feed.py` and the manifest pins: 95 passed, 10 skipped.
- Every native harness on the delivered file: `decision_board_funding` 64, `decision_source_readiness` 73, `apps_script_syntax` 5 sources, `portfolio_reconciliation_inputs` 20, `dt10_calendar_completeness`, `test_dt10_confirm_basis` 8/8, `test_dt10_pending_grace` 8/8, `test_dt10_funding_evidence` 103, `test_dt10_shadow_rank_log` 86, `test_dt10_shadow_momentum` 365 (M8 now compares the rebuilt v1.17.1 with v1.17.0), `test_pd_evidence_publish` 59, `decision_input_truth` (self-test must print the new line), `dt10_feed_cohort_binding` 21/21, `gas_ledger_fx_failed_state` 11/11, `dt10_bulk_format` 9/9, `decision_chain` 28/28. Python tests that read the GAS file: 177 passed, 10 skipped. Workflow regressions 338 passed; `scripts/audit_repository_workflows.py` errors 0, warnings 0.
- Delivered SHA-256 `0b560e3dd9642ec13321da23ca62e59ce7db4b227c92e232f78296d9c9d54a1b` (UTF-16 length 529,347 in the editor).

## Review fixes (27 Oct, independent review before merge - no blocker)
1. **Morning render before the calendar sync (should-fix):** the 08:07 render reads the previous day's calendar, where a name that reported after the last close still carries the report as its Next Earnings Date and no Last Earnings Date - no tag on the reaction session. The past Next Earnings Date now counts as the report (surprise unknown).
2. **The brief dropped tagged seats silently (should-fix):** run_daily_brief v1.19.0 names them on all three surfaces.
3. **Unbounded walk (nit):** reports older than 31 days are skipped before any walk; the walk itself is bounded to 366 days.
4. **Forward-tag duplication on a second pass (latent nit):** the post-earnings tag now goes after a leading forward tag, so that tagger still finds its own prefix at position 0.
5. **EPS edge cases (nit):** `%` and `,` stripped from text; values clamped to +-999999 so the display never turns exponential (the withheld regex keeps matching).
6. **The off-path property read (nit):** kept (one read per render), now asserted in P1.
7. **Venue week (latent nit):** KSA symbols count Sun-Thu sessions.

## S5 - Rollback
- `DT10_POST_EARN` off / deleted: the v1.17.1 render with no re-paste (and no brief line, since no note carries the tag).
- Or re-paste v1.17.1 (`d209ca3e...`); brief: revert to v1.18.1.

## S6 - Known limits / next
- Exchange holidays are not known (a holiday counts as a session - the tag ends at most a day early around a holiday).
- The "negative reaction" half of rule 3 (release-day move <= -3%) is not in `Calendar_Events`; it is register A5 (heavy-volume drop rule). Until then a positive-surprise report that sold off is tagged for 5 sessions, not 10.
- A past Next Earnings Date that was only an estimate (the report did not happen) tags the seat until the next calendar sync replaces it - the cautious side.
- Enforce step (no new money for a tagged seat) after a week of observed tags; the v2 pages (`01_Menu`) and the ticket card (A12) read the tag in later changes.

## S7 - Operator steps
1. Merge.
2. Save `apps_script/16_Decision_Top10.gs` v1.18.0 in the bound project (Claude stages it; the Save click is the owner's), run Top_10_Investments -> Self-Test.
3. Script Property `DT10_POST_EARN` = `observe`.
4. Verify on the next render: B2 shows `post-earn ⚠N/M` (or `0/M`), and a seat that reported in the last 5 / 10 sessions carries `⚠ post-earnings S/L · EPS x% · ` in its Advisor Note; the next daily brief names any such seat it held back.

Not a licensed financial adviser; the owner decides and executes every trade.
