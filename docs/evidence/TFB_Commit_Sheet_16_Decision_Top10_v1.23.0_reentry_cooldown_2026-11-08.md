# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.23.0 [REENTRY-COOLDOWN] - 2026-11-08

(ASCII sheet: `--` stands for the em dash, `<=` / `>=` for the comparison signs.)

**Scope:** item A4 of `claude/TFB_Full_Review_Health_Check_and_Improvement_Plan_2026-11-07.md` (owner, Sun 8 Nov 2026 00:37 Riyadh: "go with the best options and recommended ones"). A name whose sell date in the lot ledger (`_Portfolio_CostBasis`) falls inside the last N calendar days cannot be seated on the Top-10 board, and a seated member that was just sold hard-exits. One new Script Property, default OFF: with `TFB_T10_REENTRY_COOLDOWN_DAYS` absent, blank, `0`, junk or a PropertiesService fault, every render is the v1.22.0 render byte for byte (tickets, labels, state blob, audit, note, knobs object, Logger lines), apart from one property read, and the ledger tab is never opened. No gate, floor, cap, sizing rule or clock changes for any name not in the map.

**Files:**
- `apps_script/16_Decision_Top10.gs` 1.22.0 -> 1.23.0: 629,583 -> 642,581 bytes, 11,831 -> 12,060 lines, 14 anchored hunks, 186 added lines; five base lines replaced (the two version lines, the core's signature, the hardOut test, the WHY anchor); zero removals.
- `tests/test_dt10_v1230_reentry_cooldown.js` (new; 9 sections x3, `PASS 9/9`, digest `d1cb64dc62035d7b` x3 identical) and `tests/test_dt10_v1230_reentry_cooldown.py` (pytest wrapper).
- `tests/js/dt10_edits_v1230.json` (new, the 14 hunks as `[current, previous]`) and `tests/js/dt10_cash_utc_history.js` (`v1220Source`, the v1.22.0 base SHA-256 `584d08c8...` pinned; `v1211Source` now starts from it, so every older oracle still rebuilds its pinned file from the delivered one -- verified `v1220 584d08c8 / v1211 d4afc834 / v1210 e153f4dc`).
- `tests/test_dt10_v1220_confirm_window.js` reads the rebuilt v1.22.0 file (`history.v1220Source`), so it keeps proving the v1.22.0 diff against v1.21.1 while this release has its own harness.
- `apps_script/zz_Property_Tool` 2.2 -> 2.3: `tfbPropsCooldownOn` (sets the property to 10, names the pasted cockpit version, points at `dt10SelfTest`), `tfbPropsCooldownOff`, and `tfbPropsStabilityArmAll` (confirm window 5 + cool-down 10 in one run).
- Version pins moved to 1.23.0 in fifteen harnesses, as every prior cockpit release did: `test_dt10_confirm_basis` and `test_dt10_pending_grace` (inventories +3 functions, +3 vars; census 260), `test_dt10_funding_evidence`, `test_dt10_post_earnings`, `test_dt10_shadow_momentum`, `test_dt10_shadow_rank_log`, `test_dt10_v1190_hold_live_rule` and `test_dt10_v1200_ticket_guard` (rebuilt-oracle condition also matches 1.23.0; self-test line counts take the two new lines), `test_dt10_v1210_exdiv_guard` (E9 drops the `reentry cooldown` lines too), `tests/js/dt10_bulk_format.test.js`, `tests/js/dt10_feed_cohort_binding.test.js`, and three pins the v1.22.0 commit had missed (they still read 1.21.1 and would have failed CI on PR #927): `tests/js/decision_input_truth.test.js` (T9 census 260 functions / 151 vars, run stamp, self-test lines, details.version), `tests/js/decision_source_readiness.test.js`, `tests/js/gas_ledger_fx_failed_state.test.js`.
- `.github/workflows/ci.yml` 1.1.62 -> 1.1.63: the new harness joins the native Apps Script step after the v1.22.0 one.
- This sheet.

**Not touched:** the backend, `scripts/verify_deployment.py`, the live Apps Script project, the workbook, the ledger tab itself (read only, A1:Z, at most `DT10_CALENDAR_MAX_ROWS + 1` rows, inside try/catch). The repo file is a mirror: nothing changes until the owner pastes it, and the cool-down changes nothing until the property is set.

## S0 - Evidence (read-only, 7 Nov 2026)
- IBKR trades: ITRN.US stopped out 5 Nov 17:53Z (67 sh @ 49.89, -169 USD realized). BigQuery `tfb_archive.v_top10_board`, 7 Nov 04:38 render: ITRN.US seated again as FAST-TRACK day 1 at rank 3 -- two sessions after the stop.
- The same pattern on 9 Sep: HCI.US fully exited, re-qualified FAST-TRACK day 1 the next day (`claude/TFB_PostMortem_FRST_First_Ticket_2026-10-26.md` and the 9 Sep ledger rows).
- The owner's standing rule since the HCI exit (no re-buy of a name just sold at a loss) existed only as a note; the board reads the engine's raw top-N, the ledger records the sale, nothing joined the two.
- `_Portfolio_CostBasis` carries a `Sell Date` column per lot (10_My_Portfolio v1.5.18 ledger), so the join needs no new data.

## S1 - WHY
A stop is the owner's statement that the thesis failed at that price; letting the same name back onto the board one or two sessions later asks the owner to re-decide the same trade on the same evidence. Ten calendar days keeps the name off the board long enough for a new sync cycle and the next weekly review, after which it competes again on equal terms. The rule is a hold, not a ban: the engine still scores the name, the shadow log still sees it, and the day it leaves the window it may fast-track as any other name.

## S2 - WHAT (edit sites)
1. Header `Version:` line 4 and `var DT10_VERSION = '1.23.0'` in lockstep. One dated WHY / WHAT / GATES / ROLLBACK block is prepended; the v1.22.0 and every prior WHY are kept verbatim.
2. Constants beside the v1.22.0 pair: `DT10_T10_COOLDOWN_PROP = 'TFB_T10_REENTRY_COOLDOWN_DAYS'`, `DT10_T10_COOLDOWN_MAX = 60`, `DT10_LEDGER_SHEET = '_Portfolio_CostBasis'`.
3. `dt10CooldownMapFromValues_(values, asOfDate, maxDays)` (PURE): header row = the first row carrying a `symbol` token and a `selldate` / `solddate` / `exitdate` token (`dt10NormToken_`); each later row with a parseable sell day (`dt10CalendarDay_`: a Date cell is a Riyadh instant, a text cell an ISO date) whose age against the as-of calendar day (default: now) is `0..maxDays` joins the map as `{days, date}`; the most recent sale per symbol wins; blank symbol, blank / future / older date and a repeated header are skipped; `maxDays <= 0`, no headers, an empty or null grid, an unparseable as-of all give `{}`.
4. `dt10CooldownMap_(ss, maxDays, asOfDate)`: reads `DT10_LEDGER_SHEET` A1:Z (at most 26 columns, at most `DT10_CALENDAR_MAX_ROWS + 1` rows) and returns `{}` for a missing tab, an oversized grid or any fault.
5. `dt10StabCooldownDays_()`: the service read, `parseInt` of the property, `1..60` arms (clamped at 60), anything else 0, a fault 0.
6. `dt10StabCore_(rawTickets, cands, state, knobs, limit, today, outage, cooldown)`: `cdDays = knobs.cooldown_days` (integer >= 1 or 0), `cdMap` = the 8th argument when armed and an object, else `null`; `cooledDown(sym)`; `hardOut(sym)` is true for a cooled name; a raw ticket whose symbol is cooled is moved into `cooldownHeld` BEFORE the counter pass; a member in the map takes the existing hard-exit path and is listed in `cooldownHeld` with `member: true`.
7. Note token `, cooldown=Nd` while armed, plus ` held k SYM(sold Md ago) ...` (up to three names) when names were held; `audit.cooldown_held` carries `{symbol, days, date, member}`. Unarmed: no token, no key.
8. `dt10ApplyStability_`: `cdRead = dt10StabCooldownDays_()`; when `> 0`, `knobs.cooldown_days = cdRead` and `cdMapApply = dt10CooldownMap_(SpreadsheetApp.getActiveSpreadsheet(), cdRead)`; the map is the 8th argument of the core call. Unarmed: `knobs` byte-identical, `cdMapApply = null`, no sheet lookup.
9. `dt10SelfTest`: the property line `reentry cooldown (TFB_T10_REENTRY_COOLDOWN_DAYS): N (...)` follows the property, and the pure battery `reentry cooldown core: ok` (parser fixture with the header at row 4, a Date cell and ISO text, age bounds, most recent sale wins; core: CHAL sold 3 days ago not seated and named, member B sold yesterday hard-exits; map absent = the v1.22.0 replay byte for byte).

## S3 - Gates / arming
- Default OFF. `tfbPropsCooldownOn` sets `TFB_T10_REENTRY_COOLDOWN_DAYS=10`; `tfbPropsCooldownOff` deletes it; `tfbPropsStabilityArmAll` sets it together with `TFB_T10_CONFIRM_WINDOW=5`.
- Rollback: delete the property (the board is the v1.22.0 board on the next render; the state blob carries nothing from this release) or paste the v1.22.0 file.
- Order of operations for the owner: paste `16_Decision_Top10.gs` 1.23.0 and `zz_Property_Tool` 2.3 from `main`; run `tfbPropsStabilityArmAll`; run `dt10SelfTest` (expect `[DT10 v1.23.0] self-test`, `confirm window core: ok`, `reentry cooldown core: ok`, zero FAIL); Refresh Decision Top-10 from the menu and read the note token.

## S4 - Harness (`tests/test_dt10_v1230_reentry_cooldown.js`, PASS 9/9 x3)
- R0 base oracle: the delivered file with the 14 hunks reversed is the v1.22.0 file, SHA-256 `584d08c8...`; versions in lockstep; both files compile.
- R1 GOLDEN NEGATIVE on v1.22.0: the ITRN pattern -- a raw ticket for a name sold 2 days ago is seated `FAST-TRACK (day 1 ...)`; the base ignores the knob and the 8th argument.
- R2 KILL: knob absent / 0 / junk, map absent / null, property `0`, PropertiesService fault -- every fixture, every day deep-equals the base through the pure core and through `dt10ApplyStability_` (note, selected, audit, saved blob, knobs); the new file reads the property once, the base never; the ledger tab is never looked up while unarmed.
- R3 armed: the sold raw ticket is never seated, counted or stored, named in note and audit; a sold member hard-exits (`exited_hard`, `member: false`, `ci 0`) and is named with `member: true`; the other seats, clocks, labels and sizing equal the base replay with the cooled name stripped from the raw list.
- R4 parser: titles above the header, three header spellings, Date (Riyadh instant) and ISO cells, 0..N kept, future / older / blank skipped, most recent sale wins, repeated header skipped, N = 0 / no headers / empty / null / unparseable as-of -> `{}`; the live reader on a missing tab, a null spreadsheet and an oversized grid -> `{}`.
- R5 service read: `1`..`60` -> n, `99` -> 60, `07` -> 7, blank / `0` / `-1` / `2.5` / `ten` / absent / fault -> 0; `dt10StabKnobs_` (panel snapshot) never carries the knob; the knobs object carries it only while armed; one ledger lookup and one bounded read per armed run; a 1-day cool-down lets a name sold 2-3 days ago seat (bound is exact).
- R6 embedded `dt10SelfTest`: both new lines present, exactly two lines added, every v1.22.0 line kept, zero FAIL; armed property line follows the property.
- R7 lint / inventory: no `let` / `const` / `class` / `=>` / backtick in the diff; ASCII apart from the pre-existing header em dash; LF only; 3 functions and 3 vars added, 0 removed; 14 hunks each unique in the delivered file and reversing to the pinned base; WHY block newest-first; core signature exact.
- R8 mutants: "cooled name still seated", "token leaks unarmed", "future sale counts" -- each caught.
- Whole cockpit set on the v1.23.0 file: confirm_basis 8/8, pending_grace 8/8, funding_evidence 103, post_earnings 686, shadow_momentum 365, shadow_rank_log 86, v1190 9/9, v1200 12/12, exdiv 12/12, mf_scope 7/7 (rebuilt v1.21.1), v1220 10/10 (rebuilt v1.22.0), p142 / p145 / calendar / cash_source digests; `tests/js`: bulk_format 9/9, feed_cohort 21/21, decision_input_truth, decision_source_readiness 73, gas_ledger_fx 11/11, decision_chain 47/47, board_funding 64, syntax 7 sources.

## S5 - What this does NOT do
- It does not write to the ledger, the board, or any property. It does not read the ledger unless armed.
- It does not touch the Python engine (`core/analysis/top10_selector.py`), which still proposes the name; the cockpit holds it.
- It does not distinguish a loss from a gain at the sale: any sale inside the window holds the name (the owner's rule as stated; a profit-only exemption would be a later knob).
- It does not reach the 27_Decision_Pages_v2 ticket layer: a name held here never reaches that layer, so no change is needed there.
