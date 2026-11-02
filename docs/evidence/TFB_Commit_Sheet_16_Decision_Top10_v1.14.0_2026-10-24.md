# Top-10 v1.14.0 — confirmation counts qualified days inside the rank buffer

Date: 24 October 2026. Base: `origin/main ca2f87c7`, `apps_script/16_Decision_Top10.gs` v1.13.11 (SHA-256 `268b85adafa64b9d8e026646f5227f266541a7fd6a178352fb3d62a3c290b979`).

## WHY (the owner's problem, 24 Oct)

> A stock that qualifies on day 1 and day 2 is pushed out of the raw top-10 on day 3 by a higher-ranked newcomer. It is still INVEST-qualified, just ranked lower, but its confirmation count resets to 0 and it must start over.

Mechanism, pinned on v1.13.11 (`dt10StabCore_`, counter pass, day-advance branch):

```js
if (rawSet[sym]) { st.ci++; st.co = 0; }
else { st.ci = 0; if (st.member) { /* rank_buffer pauses the exit clock only */ } }
```

`rawSet` is `payload.selected`, the backend's sized top-N (`T10: Max Selected`, default 10). Confirmation was keyed to presence in that sized list, not to qualification. A member absent from the list honours the rank buffer only on its exit clock (pause); its confirmation count still falls to 0, so a FAST-TRACK seat displaced from the raw set on day 3 restarts at 1/3 when it returns.

**Live trail, 22 → 24 Oct.** The 22 Oct board seated XIOR.BR, GBCI.US and PINE.US at FAST-TRACK 1/3 (`TFB_Execution_Plan_Pending_Issues_2026-10-21.md`, correction 22 Oct 09:10). On the 24 Oct advance (16:44 Riyadh) none of them was in the raw set and every count read 0 again; day 1 (1/3) was PHP.L, FRST.US, ZTO.US, CRC.US, CBAN.US (`TFB_Saturday_Decision_Pack_2026-10-24.md` §1). Stated honestly: those three had also left INVEST on 24 Oct (cold pass-1 reliability and HOLD moves, §2 of the pack), so the rule below would **not** have kept them. It keeps the case the owner describes — a name that stays INVEST inside the buffer while a newcomer takes its sized slot — which the code made impossible to confirm.

## WHAT

1. **Script Property `TFB_T10_CONFIRM_BASIS`**, read once per refresh in `dt10ApplyStability_` by the new `dt10StabConfirmBasis_()` (the `dt10HardVerdictStrict_` pattern: the service read stays outside the pure core) into `knobs.confirm_basis`:
   - `qualified` — **default** (owner decision 2026-10-24; blank, absent, any other value, or a PropertiesService fault read as `qualified`);
   - `raw` (trimmed, any case) — legacy: every clock, label, note and state byte is v1.13.11.
2. **Pure rule** (`dt10StabQualifiedWithin_`, new; wired as the `qualifiedWithin` closure in `dt10StabCore_`): on a day-advance that is not outage-paused, when the symbol is **not** in `rawSet`, the basis is `qualified`, and the symbol's audited row has verdict exactly `INVEST` (any case), `structural_block !== true`, `!hardOut(sym)`, `knobs.rank_buffer > 0` and its opportunity-score rank over the deduped audited grid is `<= knobs.rank_buffer`, the day counts exactly like a raw sighting: `ci + 1`, `co = 0`, score point recorded. Otherwise the v1.13.11 branch runs unchanged. Members benefit the same way: a GRACE or FAST-TRACK seat still INVEST inside the buffer gets `co` reset and `ci` advanced instead of a pause, so a fast-track seat can graduate on a qualified day (that day's label stays `GRACE (0/3 missed)`, ghost path, sizing suspended — no raw ticket, no sizing).
3. **Seating is unchanged.** Entry as `NEW (confirmed n/n)` still requires presence in `rawSyms` (a sized ticket); fast-track fills only from `rawSyms`; displacement, capacity, hard/soft exits and every label are v1.13.11. The rule applies to symbols already tracked in the state blob (members and past raw sightings inside the 14-day prune window); a symbol never seen in raw is not tracked and accrues nothing.
4. **Read-back.** Under `qualified` the stab note gains `, confirm=qualified` and, when a non-raw challenger inside the buffer has `ci > 0`, `, near <k> SYM(ci/confirm_days) …` (up to 3 entries, ordered by `ci` desc then rank; the `pending` style). Under `raw` no token is added. The `steady — no membership change` text is kept; tokens are appended after it.
5. **Self-test.** `dt10SelfTest` gains `confirm basis (TFB_T10_CONFIRM_BASIS): qualified|raw …` and the pure battery `confirm basis core: ok (…)`: (a) `raw` reproduces the knob-less P-168 replay and the knob-absent replay byte for byte; (b) challenger in raw days 1–2 (ci 2), absent on day 3 while INVEST at global rank 12 → ci 3 (legacy 0), back in raw on day 4 → seated `NEW (confirmed 3/3)` carrying the raw ticket's sizing fields; (c) day-3 verdict WATCH → 0; (d) rank 16 (= buffer + 1) → 0 and rank 15 (= buffer) → 3; (e) rank buffer 0 → clocks, tickets and audit identical to `raw`; (f) a GRACE member (co 1) INVEST at rank 15 → co 0, ci advanced. All 27 pre-existing `core: ok` lines are unchanged.
6. **Version** 1.13.11 → 1.14.0; header line 4 and `DT10_VERSION` in lockstep; dated WHY block prepended. Function census 195 → 197 (`dt10StabQualifiedWithin_`, `dt10StabConfirmBasis_`); variable census 104 → 105 (`DT10_T10_CONFIRM_BASIS_PROP`). Zero functions or vars removed. ES5 only; every added line ASCII (the version header line keeps only its pre-existing em dash); no non-ASCII code point added or removed.

### Design decision to flag: the rank window

The owner's spec described the window as `globalRank <= limit + rank_buffer`. The file's own doctrine since v1.3.0 defines `T10: Stability Rank Buffer` (default 15) as an **absolute** audit-grid rank — header RULES: "a missed day while still ranking <= `Rank Buffer` across ALL audited candidates does not count"; `DT10_STAB_DEF_BUFFER = 15; // audit-grid rank <= buffer pauses exit clock`; and the member pause code `r <= knobs.rank_buffer`. This build uses that same window (`rank <= rank_buffer`) so the panel knob keeps one meaning for the pause and for confirmation, and the spec's own phrase "co reset **instead of a pause**" holds exactly (the pause zone is the confirmation zone). It is the narrower of the two readings (ranks 11–15 by default rather than 11–25); every example in the spec (rank 12 counts, `limit + buffer + 1` does not, buffer 0 = legacy) passes under it. If the wider window is wanted, it is a one-line change in `dt10StabQualifiedWithin_` (`r <= buf` → `r <= limit + buf`, plus passing `limit`) and the T2 boundary cases (`out16`, `edge15`) re-pinned.

## Risk analysis

| Area | Can change under `qualified` | Cannot change |
|---|---|---|
| Confirmation clock `ci` | advances on a non-raw day that is INVEST inside the buffer (members and tracked challengers) | never advances for WATCH / DO_NOT_INVEST / BLOCKED / structural block / hardOut / rank outside the buffer / buffer 0 / absent from the grid / outage-paused |
| Exit clock `co` | reset to 0 on such a day (legacy: paused inside the buffer, incremented outside) | a miss outside the buffer still counts; soft/hard exit rules untouched |
| Seating | a confirmed name can be seated one or more days earlier **when its raw ticket returns** | no seat without a raw ticket; entry/fast-track/displacement/capacity logic byte-identical |
| Labels | the FAST-TRACK → ACTIVE graduation may happen on a non-raw day (label that day: GRACE, ghost, unsized) | vocabulary unchanged; no new label |
| Sizing | none | a qualified-only day never produces sizing (ghost `—` fields, `_grace_hold`) |
| State blob | values of `ci`/`co` | field set unchanged (no new key); `raw` byte-identical |
| Status line | `, confirm=qualified` and `, near k …` tokens inside the stab note | B2 grammar the audits parse (`status: ok`, receipts, `pool_reconciliation_v1`, epoch) untouched; `raw` byte-identical |
| Backend POST | none | `dt10StabKnobs_` (the criteria snapshot) does not carry the knob |
| Property faults | none | fault reads as the default `qualified`; one read per refresh |

The only behavioural change is **which days count as confirmation** (and, for members, that such a day clears the miss counter). A board that is already seated and in raw every day is unaffected (A.US in the fixtures is byte-identical across bases).

## Tests

**New `tests/test_dt10_confirm_basis.js` — `PASS 8/8`, run 3×, identical output** (digest `661458605f6a71ac`). Usage: `node tests/test_dt10_confirm_basis.js [new.gs] [base.gs]`. The base is the v1.13.11 file rebuilt from the delivered one by reversing exactly the eleven enumerated v1.14.0 edits (`tests/js/dt10_cash_utc_history.js`: `EDITS_V1140`, `v11311Source`, `V11311_SHA256`); a supplied base must carry the same SHA-256. Both files run in separate vm contexts with GAS shims (PropertiesService / Logger / SpreadsheetApp with a `_Status` feed stamp / Utilities).

| Case | Proves |
|---|---|
| T0 | base SHA-256 pinned; version lockstep (header line 4, `DT10_VERSION`) |
| T1 golden negative | on v1.13.11 the displaced challenger resets to 0 on day 3, is `pending 1/3` and unseated on day 4; a displaced FAST-TRACK seat restarts at `FAST-TRACK (day 5, 1/3 confirmed)`, unsized; the base ignores the knob |
| T2 | qualified: day 3 ci 3 with `near 1 CHAL.US(3/3)`; day 4 `NEW (confirmed 3/3)` rank 2 with `suggested_sar 5000`, `suggested_shares 133`, funds_from intact; B (GRACE 1/3, rank 15) → co 0, ci 4; FAST-TRACK seat graduates on day 3 and is `ACTIVE (day 5)` sized on day 4; WATCH, structural block, rank 16, absent-from-grid → legacy; rank 15 counts; buffer 0 → clocks identical to raw; near token cap 3 and order; helper edge cases never throw |
| T3 kill | `raw` and knob-absent deep-equal the base on every day of eight scenarios (tickets, state, audit, note); through the REAL `dt10ApplyStability_`: note, `payload.selected`, audit, saved blob and knobs (minus the new key) equal the base; one property read; state field set unchanged under `qualified` |
| T4 | embedded `dt10SelfTest`: `confirm basis core: ok`, basis line follows the property (`' RAW '` → raw), every base self-test line present, 27+ `core: ok` lines, zero FAIL |
| T5 lint | added lines (256) carry no `let`/`const`/`=>`/backtick/`class`, are ASCII except the version header rewrite; non-ASCII code-point multiset identical to the base; LF only; file compiles |
| T6 inventory | 0 functions removed (all `function name(` incl. nested), 0 vars removed; added exactly `dt10StabQualifiedWithin_`, `dt10StabConfirmBasis_`, `DT10_T10_CONFIRM_BASIS_PROP`; top-level census 195 → 197 |
| T7 | property spellings (`' RAW '`, `raw`, `Raw` → raw; blank, `rawish`, `legacy`, `1`, `0`, absent, service fault → qualified); `dt10StabKnobs_` unchanged |

**Mutants (scratch copies, not committed), each rejected:** `r < buf` (T2, T4), branch without `qualifiedBasis` (T3, T4), missing `confirm=qualified` token (T2, T3), verdict check loosened (T2, T4), property parser leaking other values (T7); the base file passed as the delivered file (T0, T2). Without an explicit base, any unlisted edit fails T0 (ledger reversal).

**Sibling harnesses (all green locally, Node 22):** `apps_script_syntax`, `decision_input_truth` (20 passed, 3 skipped; `[DT10 v1.14.0] self-test` and `confirm basis core: ok` added to T8's required lines; census pins 197/105), `dt10_feed_cohort_binding` 21/21, `dt10_bulk_format` 9/9, `decision_board_funding` 64, `decision_source_readiness` 73, `gas_ledger_fx_failed_state` 11/11, `decision_chain` 28/28, `test_dt10_calendar_completeness`, `test_dt10_p145_grace_sizing` (x3), `test_dt10_p142_containment` (x3), `test_dt10_cash_source`. Version pins moved to 1.14.0 in five harnesses. `tests/js/dt10_cash_utc_history.js`: the v1.14.0 ledger is reversed first; `v11310Source` and `feedHistorySource` still land on their pinned digests (`914592a3…`, `9d11832a…`).

**Python (local):** `test_approved_scope_native_binding`, `test_daily_brief_blocked_inputs`, `test_executable_quote_conflicts`, `test_gas_ledger_fx_failed_state`, `test_release_decision_feed`: 170 passed, 10 skipped. `test_ci_required_verdict`, `test_runner_pin_ubuntu2404`, `test_repository_workflow_audit`: 30 passed, 3 skipped. `scripts/audit_repository_workflows.py`: errors=0 warnings=0.

**Strict ES5 parse** of the whole delivered file (acorn, `ecmaVersion: 5`): passes. `vm.Script` compile: passes.

**CI v1.1.35:** `node tests/test_dt10_confirm_basis.js` in the lean lane's native Apps Script step, next to `tests/test_dt10_calendar_completeness.js`.

**Delivered GAS SHA-256:** `b888d7fd0f0b90f3170bd0e84cabb9c0efd1e665a1a55914746df9fdb9eda52c` (8,798 lines; +256 / −2 against v1.13.11).

## Anchored edits (each anchor matched exactly once; line numbers in the delivered file)

| # | Edit | Lines |
|---|---|---|
| E1 | header `Version: 1.13.11` → `1.14.0` (rewrite) | 4 |
| E2 | dated WHY block prepended before the v1.13.11 block | 10–70 |
| E3 | `var DT10_VERSION = '1.14.0';` (rewrite) | 1866 |
| E4 | `var DT10_T10_CONFIRM_BASIS_PROP` after `DT10_STAB_PRUNE_DAYS` | 2730–2732 |
| E5 | pure `dt10StabQualifiedWithin_` before the `dt10StabCore_` doc comment | 3637–3655 |
| E6 | `qualifiedBasis` + `qualifiedWithin` closure before the counter pass | 3749–3756 |
| E7 | `else if (qualifiedBasis && qualifiedWithin(sym))` branch in the day-advance counter pass | 3803–3809 |
| E8 | note tokens after `var note = …`, before `return {` | 4095–4120 |
| E9 | `dt10StabConfirmBasis_` after `dt10EpochKeyLegacy_` | 4264–4281 |
| E10 | `knobs.confirm_basis = dt10StabConfirmBasis_();` after `knobs.hard_strict` in `dt10ApplyStability_` | 6333–6335 |
| E11 | self-test basis line + battery before `var msg = report.join` | 8683–8791 |

Nine insertions and two version-line rewrites; reversing them in order recovers the v1.13.11 bytes (SHA-256 above), which is what the harness and the history module assert.

## OPERATOR STEPS (bound Apps Script project)

0. **Check what is installed first.** Open `16_Decision_Top10.gs` in the editor and read the `var DT10_VERSION = '…';` line (around line 1805 in v1.13.11). The bound project may still hold an older version than v1.13.11 (unverified from here); whatever it holds, the step below replaces the whole file, so back it up from the editor before pasting.
1. Paste the **whole** `apps_script/16_Decision_Top10.gs` from this commit into the bound project, replacing the entire contents of `16_Decision_Top10.gs`. Save.
2. Run `dt10SelfTest()` from the editor. Expect the first line `[DT10 v1.14.0] self-test`, the lines `confirm basis (TFB_T10_CONFIRM_BASIS): qualified (default qualified; raw = v1.13.11 clocks)` and `confirm basis core: ok (…)`, and **every** `core:` line `ok` (zero `FAIL`).
3. Nothing else. No property needs to be set for the default (`qualified`); the next day-advance refresh applies it and the stab note in B2 shows `confirm=qualified` (and `near k …` when a displaced challenger is still counting).

**Rollback** (either, no evidence or funding state changes):
- Script Property `TFB_T10_CONFIRM_BASIS` = `raw` (no re-paste): the next refresh runs the v1.13.11 clocks byte for byte and the note loses the tokens; or
- re-paste v1.13.11 (`268b85ad…`).

## Limits (stated)

- The rule can only act on symbols the state blob already tracks. A qualified name that never appeared in the sized list is not counted until it does.
- A qualified day produces no sizing and no seat; the earliest sized ticket is still the day the raw ticket returns with `ci >= confirm_days`.
- Under `qualified`, a member inside the buffer that is INVEST has its miss counter cleared; a member inside the buffer that is **not** INVEST keeps the v1.3.0 pause. The window is the panel's absolute rank buffer (see the design decision above).
- Live read-back of the first qualified day happens on the next day-advance after installation; nothing in this change touches the feed, funding or publication gates.
