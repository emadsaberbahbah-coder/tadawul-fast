# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.21.0 [EXDIV-GUARD] - 2026-11-03

(ASCII sheet: `--` stands for the em dash, `<=` / `>=` for the comparison signs.)

**Scope:** item A1 part B of `claude/TFB_Gap_Analysis_and_Build_Order_2026-11-03.md` (owner request 3 Nov 14:05 Riyadh: "make the required update to avoid what happen"). The EXISTING v1.20.0 ticket guard (`DT10_TICKET_GUARD` observe | enforce) gains an ex-dividend filter behind two new Script Properties, both default OFF. With the days knob absent, blank or junk, the run is the v1.20.0 run byte for byte (every cell, merge and border, the banner, B2, the KPI strip, `_Run_Log` and Logger lines, `_Selection_Log` rows, both POST bodies, the state blob); the only difference is one property read while the guard is armed. Like the v1.20.0 guard, the filter can only take tickets off today's plan, never adds one, never re-sizes one, and the cash it frees stays unallocated.

**Files:**
- `apps_script/16_Decision_Top10.gs` 1.20.0 -> 1.21.0: 600,979 -> 613,875 bytes, 11,352 -> 11,562 lines, 13 anchored hunks, 181 added lines; seven base lines replaced (the two version lines and five lines of the guard: the config object, the core's doc line and signature, the core call, the Logger line); zero removals.
- `tests/test_dt10_v1210_exdiv_guard.js` (new; 12 sections x3, `PASS 12/12`) and `tests/test_dt10_v1210_exdiv_guard.py` (new pytest wrapper, the v1.20.0 pattern, skips without node).
- `tests/js/dt10_edits_v1210.json` (new, the 13 hunks) and `tests/js/dt10_cash_utc_history.js` (`v1200Source`, the v1.20.0 base SHA-256 `dcb22bb1...` pinned; `v1190Source` now starts from it).
- Version and census pins moved in twelve harnesses: `test_dt10_v1200_ticket_guard` (its V10 lint, inventory and hunks now run on the v1.20.0 file rebuilt from the delivered one; V11 counts the one extra read), `test_dt10_v1190_hold_live_rule`, `test_dt10_confirm_basis`, `test_dt10_pending_grace` (T6 inventory +3 functions, +6 vars; census 253), `test_dt10_funding_evidence`, `test_dt10_post_earnings`, `test_dt10_shadow_momentum`, `test_dt10_shadow_rank_log`, `decision_input_truth` (census 253 functions / 146 vars), `decision_source_readiness`, `dt10_feed_cohort_binding`, `gas_ledger_fx_failed_state`, `dt10_bulk_format`.
- This sheet.

**Not touched:** the backend, `scripts/verify_deployment.py` (it pins no DT10 version), `.github/workflows/ci.yml` (the pytest wrapper brings the harness into the whole-suite job), the live Apps Script project, the workbook. The repo file is a mirror: nothing changes until the owner pastes it.

**Depends on:** PR #905 (`calendar_provider` 1.4.0 / `run_calendar_sync` 1.3.0, repository Variable `TFB_CAL_DIVIDENDS=on`), which writes the `Dividend_Events` tab. Without the tab the knob holds nothing (an empty map, as for a read failure).

## S0 - Evidence (read-only)
- 2 Nov 16:28 Riyadh: `Calendar_Events` read `INDV.US | Next Ex-Div Date 2026-11-03 | Days To ExDiv 1`.
- The 2 Nov 08:49 board issued INDV.US for 12,335 SAR; the owner bought at the 17:30 open.
- The dividend was Indivior's 8.13 USD special (declared 17 Sep, ex 3 Nov, paid 6 Nov), 22.4 % of the 36.28 close. The shares opened ex-dividend at about 28.15; the holding read about -20 %, and the engine plan stop (30.92) sat above the price.
- A 30 % US withholding applies to a Saudi-resident holder (no treaty), about 2.44 USD a share.
- The v1.20.0 guard holds for earnings, the owner rule and a cap. No gate looked at the ex-dividend date or its size, and no tab stored the size.

## S1 - WHY
A ticket bought the day before a large ex-dividend opens with a price gap the board never mentioned. For a Saudi-resident holder, 30 % of the dividend is withheld, so the gap is not fully paid back. The decision page must hold such a ticket the way it holds an earnings ticket, with the reason on the row.

## S2 - WHAT (edit sites)
1. Header `Version:` line 4 and `var DT10_VERSION = '1.21.0'` in lockstep. One dated WHY / WHAT / GATES / ROLLBACK block is prepended; every prior WHY is kept verbatim.
2. New vars after the v1.20.0 guard constants:
   - `DT10_GUARD_EXDIV_DAYS_PROP` and `DT10_GUARD_EXDIV_DAYS_MAX` (30);
   - `DT10_GUARD_EXDIV_PCT_PROP`, `DT10_GUARD_EXDIV_PCT_DEF` (2) and `DT10_GUARD_EXDIV_PCT_MAX` (100);
   - `DT10_DIVIDEND_SHEET` (`Dividend_Events`).
3. `dt10TicketGuardConfig_`: while the guard is armed it reads `DT10_GUARD_EXDIV_DAYS`.
   - When the value is 1-2 digits and <= 30 it sets `exdivDays`, then reads `DT10_GUARD_EXDIV_PCT` (default 2, clamped 0..100, a negative or junk value gives 2) into `exdivPct`.
   - Otherwise the object is exactly the v1.20.0 object, with no exdiv keys.
4. `dt10ExDivMapFromValues_` (new, pure) builds `{SYMBOL: {days, amount, currency}}` from the tab:
   - The header row is found by its `Symbol` and `Ex-Div Date` tokens; `Amount` and `Currency` are optional.
   - Days are counted from the Riyadh calendar day, like the earnings map. The ex-day itself is 0; the lane's stored `Days To ExDiv` is not trusted.
   - Skipped: a past or invalid ex-date, a repeated symbol (the first row wins) and a repeated header.
   - Amounts: a number >= 0 or a numeric string; anything else gives null. Currency labels are upper-cased.
5. `dt10ExDivMap_` (new) reads the live tab A:J, bounded at 5,000 body rows like Calendar_Events. Any fault, an absent tab or an oversized grid gives `{}`. It never throws.
6. `dt10ExDivHold_` (new, pure) holds a ticket when the ex-date is 0..`exdivDays` days away:
   - Measured case: when the dividend is >= `exdivPct` % of the ticket's native price in the same unit (compared unrounded), reason `ex-dividend in Nd (P.P% of price)`, tag `ex-div Nd P.P%`.
   - Unmeasured case: with no amount, another currency, `MIXED` or no price, reason `ex-dividend in Nd (size unknown)`, tag `ex-div Nd ?`. A special dividend cannot be ruled out.
   - **Units:** a subunit price (`GBp` / `GBX`, `ZAc` / `ILA`: the existing `DT10_FX_SUBUNIT_PARENT` keys) is measured only against a subunit label of the same parent (`GBX`).
   - The calendar lane upper-cases labels, so a `GBP` label may be pounds or an upper-cased `GBp`. That size reads unknown: the hold stays, and a percentage is never guessed.
7. `dt10TicketGuardCore_` gains a fifth argument `divMap` and one filter, AFTER earnings and BEFORE the owner rule and the cap, only when `cfg.exdivDays` is a number. Earnings still wins, and the cap counts survivors after the filter.
8. `dt10TicketGuard_`:
   - It reads the tab once per render, only while the knob is set.
   - Its Logger line gains `, ex-div <= Nd >= P%` only then.
   - Observe and enforce are the v1.20.0 paths unchanged: the token names `INDV.US(ex-div 1d 22.4%)`, and enforce sends the ticket through the existing research path with Funds From `Held -- ex-dividend in 1d (22.4% of price)`, the `[held: ...] ` note prefix, KPIs recomputed and the banner `+ K HELD BY GUARD`.
9. `dt10SelfTest`: one line, only while the knob is set and the guard is armed: `ticket guard ex-div (DT10_GUARD_EXDIV_DAYS): <= Nd, >= P% of price, tab Dividend_Events | core: ok (INDV 22.4% held, FRST 0.7% passes at 2% and holds at 0.5%, GBP size unknown, GBX on a GBp price measured, earnings first)`.

Functions added (0 removed): `dt10ExDivMapFromValues_`, `dt10ExDivMap_`, `dt10ExDivHold_`. The census goes 250 -> 253 top-level functions and 140 -> 146 vars. ES5 only; every added line is ASCII; the file's non-ASCII code-point multiset is unchanged.

## S3 - Gates / arming
| Script Property (new) | Default | Values | Effect |
|---|---|---|---|
| `DT10_GUARD_EXDIV_DAYS` | absent = filter off | integer 0..30 (1-2 digits) | Holds an executable ticket whose ex-date is 0..N days away. Read only while `DT10_TICKET_GUARD` is `observe` or `enforce`. |
| `DT10_GUARD_EXDIV_PCT` | 2 | percent 0..100 | The minimum dividend size, as % of the native price. Read only while the days knob is set. |

## S4 - Consumers of the new strings (the v1.20.0 consumer map otherwise holds unchanged)
| Consumer | What it reads | Verdict |
|---|---|---|
| `apps_script/27_Decision_Pages_v2.gs` | Funds From `/^Held\b/i` -> research (line 2179) | A held ticket reads research, as for earnings. |
| `scripts/run_daily_brief.py` | rows with Shares <= 0 skipped | The held row is skipped (Shares 0). |
| `scripts/audit_decision_surface_freshness.py`, `release_decision_feed.py`, `await_cockpit_receipts.py` | the B2 patterns (`N/M`, `run=`, `source readiness`, `status:`) | Unaffected: the token adds `%` and `?` only. |
| `sql/bigquery/20_v_top10_board.sql`, `23_v_top10_status.sql`, `67_v_top10_insight.sql` | `ticket_sar`, `funds_from` | Ticket 0, as research; `funds_from` carries the reason. |

## S5 - Tests (3 Nov 2026, node v22)
- `node tests/test_dt10_v1210_exdiv_guard.js`: `run 1/2/3: 12/12`, digest `d524bca31bbc45bc` x3, **`PASS 12/12`**. With the explicit base `git show origin/main:apps_script/16_Decision_Top10.gs`: `PASS 12/12`, same digest.
- Sections:
  - **E0** base oracle and lockstep.
  - **E1** golden negative: five guard configurations at render level, plus both feed states; 23 refreshes with the knob absent, blank or junk (`''`, `' '`, `abc`, `-1`, `31`, `1.5`, `3d`, `+3`) under off, observe and enforce; the knob set while the guard is off; a property outage; no tab. All deep-equal the v1.20.0 base. Exactly one extra property read while armed; the pct knob and the tab are never read.
  - **E2** observe:
    - The token is `tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)`, with every cell, ticket and KPI unchanged and one tab read.
    - Thresholds: 1.5 % adds GLPI 1.7 %; 22.4 % is inclusive; 22.5 % releases INDV; 100 % holds nothing measured.
    - Windows: 0 days excludes tomorrow; 10 days adds ITRN; the past DDI date never holds.
    - With earnings 0, EFC is held by its ex-date today.
    - The REAL refresh's B2 is the off line plus the token.
  - **E3** enforce: INDV.US's row equals the research-path control cell for cell, except Funds From and the note prefix. CRC and EFC equal the v1.20.0 earnings holds. KPIs equal the control's (`3 exec + 4 research + 3 grace / 10`; Unallocated + the three tickets; Fundable Now 3). Banner `... 3 EXECUTABLE TICKETS + 1 RESEARCH (NO ALLOCATION) + 3 GRACE-HELD (NO PLAN TODAY) + 3 HELD BY GUARD`. The REAL refresh shows the B2 token, `funding=final/3exec`, the seat and `_Selection_Log` rows, no new POST and no SEAT-CHECK.
  - **E4** the Riyadh day boundary (23:59:59 = 1d, 00:00:01 = 0d, the next day skipped), a Date cell, and the stored days column ignored.
  - **E5** units and unknown sizes (12 cases), subunits (GBp / GBX / ZAc), and a render-level `size unknown` hold.
  - **E6** precedence (owner rule, cap 1 / 2), non-executable seats never checked, monotonicity over 288 knob combinations (never fewer holds; kept never rises; only a cap hold can move), and a withheld feed.
  - **E7** the map builder (header search, column order, duplicates, a repeated header, blank symbols, bad dates and amounts) and the tab read (bounds 5,000 / 5,001, columns beyond J, faults).
  - **E8** a read failure or a missing tab gives the v1.20.0 enforce board and no FAILED line.
  - **E9** the self-test is the v1.20.0 report in five unarmed configurations, with exactly one line added when armed.
  - **E10** lint and inventory: the seven replaced lines pinned, 13 hunks reversing to `dcb22bb1...`, and the filter order earnings < ex-div < owner rule < cap.
  - **E11** service reads.
- Every node harness that loads the cockpit passes on the delivered file (24 files), including `test_dt10_v1200_ticket_guard` 12/12, `test_dt10_v1190_hold_live_rule` 9/9, `test_dt10_post_earnings` 686, `test_dt10_shadow_momentum` 365, `test_dt10_funding_evidence` 103, `test_dt10_shadow_rank_log` 86, `test_por_v100` 51/51, `test_dp2_v1231_engine_tickets` 11/11, `decision_source_readiness` 73 and `decision_board_funding` 64.
- Python wrappers and readers (`pytest`, 18 files including the release suites): **262 passed, 11 skipped**.
- Delivered SHA-256 `e153f4dc516f4837e3665357242d6c1ad6edff176a7abde15d4c5839feb762f9` (11,562 lines, 613,875 bytes, UTF-16 length 612,937 in the editor).
- Review fix (Codex P2 on PR #906): the threshold is compared at full precision and the share is rounded only for the reason and the tag. E5 adds 1.96 % at 2 % (no hold), 2.04 % at 2.03 % (held) and 2.02 % at 2.03 % (no hold).

## S6 - Golden negatives
`node tests/test_dt10_v1210_exdiv_guard.js <v1.20.0> <v1.20.0>` exits 1 with `FAIL 0/12`; every section fails:
- E0: `delivered version: "1.20.0" !== "1.21.0"`.
- E1: property reads (the base reads no knob).
- E2 / E3: the token lacks INDV.
- E4 / E5 / E7: `dt10ExDivMapFromValues_` / `dt10ExDivHold_ is not a function`.
- E6: the owner-rule token.
- E8: the Logger suffix.
- E9: the version line.
- E10: `added lines 0`.
- E11: `days "3": undefined !== 3`.

## S7 - Mutants (one exact replacement each, run against the explicit v1.20.0 base: 18/18 killed)
| Mutant | Killed by (behavioural sections) |
|---|---|
| hold always null | E2 E3 E4 E5 E6 E9 |
| threshold exclusive (`<=`) | E2 E5 |
| window exclusive (`>=`) | E2 E4 E9 |
| a past ex-date allowed | E4 E7 |
| the stored Days To ExDiv trusted | E4 E7 |
| no subunit rule | E5 E9 |
| size unknown passes | E5 E9 |
| ex-div before earnings | E2 E6 E9 |
| the tab read without the knob | E1 |
| the Logger suffix without the knob | E1 |
| pct default 3 | E0 E2 E5 E9 E11 |
| days max 31 | E1 E11 |
| the pct knob never read | E2 E9 E11 |
| no row bound | E7 |
| a read fault throws | E7 E8 |
| pct not clamped | E11 |
| the last duplicate wins | E7 |
| a negative amount accepted | E7 |

Each mutant also fails E10, whose hunk reversal no longer reproduces the v1.20.0 SHA-256.

## S8 - Rollback
- Delete `DT10_GUARD_EXDIV_DAYS` (or set junk): the v1.20.0 guard, with no re-paste. Held tickets return on the next render; nothing is stored.
- Or re-paste v1.20.0 (`dcb22bb1...`).

## S9 - Known limits / deliberate cuts
- **Board-side brake only.** The backend still sizes the held ticket in its research and allocate calls; the board shows it as research with the reason, as for earnings.
- **Declared dividends only.** The data is what EODHD has declared within 90 days (`run_calendar_sync` 1.3.0). An undeclared special dividend cannot be seen, and the tab is as fresh as the last calendar sync.
- **Unit caution.** A `GBP` label on a pence price reads `size unknown` and holds within the window. This is conservative: within a 3-day window an LSE ticket is rarely affected.
- **New tickets only.** Holdings are not touched. A1 part C (dividend-aware P&L and stop on My_Portfolio) is a separate item.

## S10 - Operator steps (the repo file is a mirror; nothing changes until the paste)
1. Merge PR #905 and set the repository Variable `TFB_CAL_DIVIDENDS` = `on`. Let one calendar sync write `Dividend_Events`.
2. Merge this PR.
3. Paste the full `apps_script/16_Decision_Top10.gs` v1.21.0 into the Apps Script editor (SHA-256 `e153f4dc...`, 11,562 lines) and Save.
4. Script Properties: `DT10_TICKET_GUARD` = `observe` (if not already) and `DT10_GUARD_EXDIV_DAYS` = `3`. Optionally set `DT10_GUARD_EXDIV_PCT` (default 2).
5. Run `dt10SelfTest`. Expect `[DT10 v1.21.0] self-test`, the line `ticket guard ex-div (DT10_GUARD_EXDIV_DAYS): <= 3d, >= 2% of price, tab Dividend_Events | core: ok (...)` and zero FAIL.
6. After one observed render, set `DT10_TICKET_GUARD` = `enforce` if the named tickets match your judgement.
7. Rollback: delete `DT10_GUARD_EXDIV_DAYS`.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
