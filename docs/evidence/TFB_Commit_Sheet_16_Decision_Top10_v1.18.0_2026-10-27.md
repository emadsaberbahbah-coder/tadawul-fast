# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.18.0 [POST-EARNINGS] - 2026-10-27

**Scope:** program register **A4 part 2** (FRST post-mortem P1; program doc `TFB_Self_Correcting_Recommendation_Program_2026-10-27.md`, loop piece L6a). The Top-10 board now says when a seat has just reported earnings: an advisor tag for the seats inside their post-earnings quarantine. Observe only; gated by a Script Property, default off.
**Files:** `apps_script/16_Decision_Top10.gs` (1.17.1 -> 1.18.0; 514,797 -> 526,635 chars; seven anchored hunks; zero removals), `tests/test_dt10_post_earnings.js` (new, 560 checks), `tests/js/dt10_edits_v1180.json` + `tests/js/dt10_cash_utc_history.js` (v1.18.0 hunks reversible, v1.17.1 base SHA-256 pinned; `v1170Source` now starts from `v1171Source`), `tests/test_daily_brief_post_earnings_tag.py` (new, 3 cases), version / census / self-test pins moved in nine harnesses, `.github/workflows/ci.yml` 1.1.47, this sheet.

## S1 - WHY
- The first funded ticket (FRST.US, Mon 26 Oct 2026) was bought two sessions after a 22% EPS miss (Q3 reported Thu 22 Oct after the close; -4.1% and -3.7% on heavy volume the next sessions).
- Every calendar signal on the board looks forward (v1.6.0 `⚠ earnings ≤Nd`); nothing showed that a seat had just reported, let alone missed. Interim trading policy rule 3 (no entry within 5 sessions after a report, 10 after a negative reaction) has been a hand check since.
- The data now exists: `Calendar_Events` carries `Last Earnings Date | Days Since Earnings | EPS Surprise %` (calendar_sync v1.2.0 with `TFB_CAL_LAST_REPORT=on`, calendar_provider v1.3.1; v1.3.2 in PR #854 completes the US names). Read 27 Oct 14:44: 34 of 327 rows, FRST.US 2026-10-22, -22.2%.

## S2 - WHAT (only when `DT10_POST_EARN` is `observe` or `on`; absent / blank / anything else = off)
- After the v1.6.0 forward tag, every SELECTED ticket whose symbol reported inside its quarantine gets its Advisor Note prefixed with `⚠ post-earnings S/L · EPS x% · `:
  - S = weekday sessions after the report date up to today (Riyadh calendar day; Mon-Fri, exchange holidays not known) = the number of the session an entry today would be;
  - L = 5, or 10 when the EPS surprise is negative; a blank surprise is unknown -> 5.
- One `Calendar_Events` read per render (A1 to the last column, at most 16), only when armed. Any read fault -> no tag, token `post-earn n/a`.
- Status line (B2): `post-earn ⚠N/M` (tagged / selected) after the forward `earn` token; `post-earn 0/M` when no seat is inside a quarantine; `post-earn n/a` when the calendar holds no last report.
- A withheld row (feed NOT_ACTIONABLE) keeps the post-earnings prefix as research context, like the forward prefix (`dt10UvWithholdRow_`; without a post-earnings tag the kept prefix is the v1.17.1 one - proved on a note corpus against the base).
- **Observe only on this board:** nothing gates, blocks, resizes or re-ranks. Downstream, the daily brief lists only notes that begin with INVEST (optionally after the forward tag), so a tagged seat drops out of the brief's executable picks - interim rule 3; pinned by `tests/test_daily_brief_post_earnings_tag.py`. The v2 decision pages show the tag once `01_Menu` reads it (register A12).
- KSA names carry no last report (EODHD has no Saudi coverage) and are never tagged here; rule 3 stays a hand check for them.
- Functions added (0 removed): `dt10PostEarnMode_`, `dt10WeekdaysBetween_` (pure), `dt10PostEarnMapFromValues_` (pure), `dt10PostEarnMap_`, `dt10ApplyPostEarnTags_` (pure), `dt10PostEarnAnnotate_`. Changed: `dt10RenderPayload_` (one call after the forward tag), `dt10UvWithholdRow_` (keeps the new prefix), `dt10SelfTest` (new line `post-earnings core: ok (FRST 3/10 tagged; +EPS at 10 sessions untagged; blank EPS 5/5) | mode now: off|observe`). Vars added: `DT10_POST_EARN_PROP`, `DT10_POST_EARN_SESSIONS` (5), `DT10_POST_EARN_SESSIONS_NEG` (10).

## S3 - Gate / arming
| Item | Before | After | Effect |
|---|---|---|---|
| `DT10_POST_EARN` (Script Property, new) | absent | `observe` | tags + B2 token on the next render |

Arming order after merge: paste and save v1.18.0 (Claude stages it in the editor with the SHA-256 checked; the Save click is the owner's), run Self-Test (expect `[DT10 v1.18.0] self-test`, `post-earnings core: ok ... | mode now: off`, zero FAIL), then set `DT10_POST_EARN=observe`. Rollback: delete the property (or `off`) with no re-paste; or re-paste v1.17.1.

## S4 - Tests
- `node tests/test_dt10_post_earnings.js` -> **ALL PASS (560 checks)**, deterministic (clock frozen at Tue 27 Oct 2026 10:00 Riyadh): **P1** kill identity - property absent / off / blank / `yes` / `1` / `onn` / `enforce` / service fault: the REAL `dt10RenderPayload_` paints the identical sheet as the v1.17.1 base (every cell, merge and border; EXECUTABLE and NOT_ACTIONABLE feed), the same status token, no extra Calendar_Events read, tickets untouched; **P2** armed (`observe`, `on`, ` On `, `OBSERVE`): exactly four Advisor Note cells change - FRST-like 3/10 at -22.2%, blank surprise 5/5, a Riyadh-midnight Date cell 2/10, a seat with both tags (post first, then forward) - each = prefix + the base note; withheld rows keep the prefixes before the withhold text; token `post-earn ⚠4/9`; one extra read; the Logger line; a second pass tags nothing; **P3** golden negative - the v1.17.1 base, armed, shows nothing for the FRST case; **P4** pure cores - sessions (Thu->Tue 3, Fri->Mon 1, weekend 0, four weeks 20, pre-1970), the map (header row found under a title row, duplicates first-wins, numeric text, unicode-minus text unknown, future / same-day / invalid / timestamp / serial dates skipped, 7-column calendar -> empty), the 5 / 10 boundaries (5 in, 6 out, 10 in, 11 out, zero = not negative, -0.04 = negative), formatting, idempotency, withheld-note identity with the base on a corpus; **P5** tokens (`n/a` for no rows / 7 columns / no tab / read fault, `0/9` when nothing is inside); **P6** the embedded self-test line off / observe / ON; **P7** version lockstep, WHY newest-first and ASCII, the v1.17.1 WHY verbatim, ES5/ASCII lint of the 168 added lines, census 218 -> 224 functions and 117 -> 120 vars with exactly the designed names, seven hunks whose reversal is SHA-256 `d209ca3e...`.
- `tests/test_daily_brief_post_earnings_tag.py`: 3 passed (with `test_daily_brief_blocked_inputs.py`: 46 passed).
- Every native harness on the delivered file: `decision_board_funding` 64, `decision_source_readiness` 73, `apps_script_syntax` 5 sources, `portfolio_reconciliation_inputs` 20, `dt10_calendar_completeness`, `test_dt10_confirm_basis` 8/8, `test_dt10_pending_grace` 8/8, `test_dt10_funding_evidence` 103, `test_dt10_shadow_rank_log` 86, `test_dt10_shadow_momentum` 365 (M8 now compares the rebuilt v1.17.1 with v1.17.0), `test_pd_evidence_publish` 59, `decision_input_truth` (self-test must print the new line), `dt10_feed_cohort_binding` 21/21, `gas_ledger_fx_failed_state` 11/11, `dt10_bulk_format` 9/9, `decision_chain` 28/28. Python tests that read the GAS file: 171 passed, 10 skipped. Workflow regressions 338 passed; `scripts/audit_repository_workflows.py` errors 0, warnings 0.
- Delivered SHA-256 `392bc14a39da5b21344303361f226f7b7351e52b7c897adff9aa7c27530bb38b` (UTF-16 length 526,637 in the editor).

## S5 - Rollback
- `DT10_POST_EARN` off / deleted: the v1.17.1 render with no re-paste.
- Or re-paste v1.17.1 (`d209ca3e...`).

## S6 - Known limits / next
- Sessions are weekdays; exchange holidays are not known (a holiday counts as a session - the tag ends at most a day early around a holiday).
- The "negative reaction" half of rule 3 (release-day move <= -3%) is not in `Calendar_Events`; it is register A5 (heavy-volume drop rule, engine side). Until then a positive-surprise report that sold off is tagged for 5 sessions, not 10.
- KSA names are never tagged (no last-report source); rule 3 stays a hand check for them.
- Enforce step (no new money for a tagged seat) after a week of observed tags; the v2 pages (01_Menu) and the ticket card (A12) read the tag in later changes.

## S7 - Operator steps
1. Merge.
2. Save `apps_script/16_Decision_Top10.gs` v1.18.0 in the bound project (Claude stages it; the Save click is the owner's), run Top_10_Investments -> Self-Test.
3. Script Property `DT10_POST_EARN` = `observe`.
4. Verify on the next render: B2 shows `post-earn ⚠N/M` (or `0/M`), and a seat that reported in the last 5/10 sessions carries `⚠ post-earnings S/L · EPS x% · ` in its Advisor Note.

Not a licensed financial adviser; the owner decides and executes every trade.
