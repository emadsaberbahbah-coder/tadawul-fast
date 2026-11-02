# Commit Sheet — `apps_script/16_Decision_Top10.gs` v1.15.0 [PENDING-GRACE] — 2026-10-25

**Scope:** a SOFT criteria miss pauses a pending challenger's confirmation count instead of resetting it. Script Property `TFB_T10_PENDING_GRACE_DAYS`, **default absent/0 = v1.14.0 clocks byte for byte**; 1..5 arms.
**Files:** `apps_script/16_Decision_Top10.gs` (1.14.0 → 1.15.0), `tests/test_dt10_pending_grace.js` (new), `tests/js/dt10_cash_utc_history.js` (+`EDITS_V1150`, `v1140Source`, `V1140_SHA256`), pins moved in `tests/test_dt10_confirm_basis.js`, `tests/js/decision_input_truth.test.js`, `tests/js/decision_source_readiness.test.js`, `tests/js/dt10_bulk_format.test.js`, `tests/js/dt10_feed_cohort_binding.test.js`, `tests/js/gas_ledger_fx_failed_state.test.js`; `.github/workflows/ci.yml` 1.1.37; this sheet.

## S1 — Defect (owner, 24 Oct 19:32; live case 25 Oct 08:10)
`dt10StabCore_`'s counter pass had one branch for every miss: `else { st.ci = 0; … }`. A challenger confirmed on days 1–2 that failed **one** criteria day — still on the audited grid, no structural block, not a hard-out verdict, inside the rank buffer, but not INVEST because a floor was missed — lost its whole count, exactly like a hard exit. Members already enjoy the tolerance (exit clock pauses inside the buffer; "plain DO_NOT_INVEST criteria-misses ARE grace-holdable", v1.2.0); challengers did not.
Live: PHP.L sat at 1/3 on 24 Oct (reliability 71.5, DQ 100). On 25 Oct — a weekend, no LSE session, price unchanged at 92.05 — its Expected ROI 3M read 8.13 % (< 12 %) and the name fell to GRACE with its count gone. The owner asked for a rule that does not restart a positive name's count on one such day.

## S2 — Change (additive; zero removals; §8 kept)
- `DT10_T10_PENDING_GRACE_PROP = 'TFB_T10_PENDING_GRACE_DAYS'`, `DT10_T10_PENDING_GRACE_MAX = 5`.
- `dt10StabSoftMiss_(sym, candBySym, globalRank, knobs, hardOut)` — PURE: audited row present, verdict **not** INVEST, `structural_block !== true`, `!hardOut(sym)`, global rank inside the buffer. Nothing is recomputed: the row, its verdict and its rank are the backend's own. Under `hard_strict` a DO_NOT_INVEST/BLOCKED verdict is still a hard-out (v1.8.0 BE-2 doctrine) → reset.
- `dt10StabPendingGrace_()` — service read outside the pure core (mirror of `dt10StabConfirmBasis_`): integer 1..5 arms (larger clamps to 5); absent/blank/0/negative/non-integer/fault → 0; never throws.
- `dt10ApplyStability_`: `knobs.pending_grace` is set **only when armed**, so an unarmed run publishes the v1.14.0 knobs object byte for byte.
- Counter pass (day-advance, not outage-paused): raw day and qualified day clear `cp`; **new branch** — `pgDays > 0 && !member && ci > 0 && cp < pgDays && softMiss` → `cp += 1`, ci kept, score point recorded, symbol listed in `pendingHeld`; otherwise the v1.14.0 reset (`ci = 0`, `cp` cleared, member exit clock unchanged).
- State blob: `cp` written only while > 0 (sanitizer keeps it the same way `op` is kept) — a blob that never paused is byte-identical.
- Read-back only when armed: note `, pgrace=<n>` and `, pheld <k> SYM(ci/n) …` (≤ 3, ordered ci desc / rank asc / symbol); `audit.pending_held` key only when armed.
- `dt10SelfTest` gains the property line and the `pending grace core` battery (0/absent = the v1.14.0 `cbW` replay; grace 1 pauses 2/3 → NEW 3/3 on the next raw day; two soft misses → reset; structural block → reset; member untouched).

## S3 — Not changed
Seating (still needs a raw ticket), fast-track, displacement, capacity, sizing, hard exits, outage pause (P-168), epoch key (P-144), confirm basis (v1.14.0), panel layout (no new cell), `dt10StabKnobs_` (criteria snapshot).

## S4 — Audits (REAL module, dual-tree, ×3 identical digests)
| Battery | Result |
|---|---|
| `tests/test_dt10_pending_grace.js` T0–T7 | **PASS 8/8 ×3**, digest `9ad1d5c0dbeb18c0`; new SHA-256 `e099e187…`, base (v1.14.0) `b888d7fd…` rebuilt by reversing the 15 enumerated edits |
| T1 golden negative (v1.14.0) | soft miss on day 3 → ci 0; day 4 `pending CHAL.US(1/3)`, not seated; base ignores the knob |
| T2 armed | grace 1: day 3 ci 2 / cp 1 / `pgrace=1` / `pheld 1 CHAL.US(2/3)` / `audit.pending_held`; day 4 `NEW (confirmed 3/3)` with raw sizing, cp cleared, B displaced (`exited_displaced`); two soft misses reset at grace 1, absorbed at grace 2 (cp 2); structural block / SELL / rank 16 / buffer 0 / absent row → reset; rank 15 pauses; DO_NOT_INVEST under hard_strict → reset, without hard_strict → pause; members identical to base through day 3; knob 9 clamps to 5; pheld cap 3 and order |
| T3 kill | pending_grace 0 and absent deep-equal the base on every fixture and day (tickets, state, audit, note); `dt10ApplyStability_` end to end: unarmed knobs/blob/note/selected identical; property `0` identical; armed publishes the knob, keeps ci 2, saves cp 1, note tokens + `epoch=2026-10-25/feed` |
| T4 self-test | `pending grace core: ok`; property line 0 / 2; every base core line present; zero FAIL |
| T5 lint | added lines ES5 + ASCII (only the version header line keeps its pre-existing em dash); non-ASCII multiset unchanged; LF only; WHY ASCII, no smart quotes |
| T6 inventory | 0 functions / 0 vars removed; +2 functions (`dt10StabSoftMiss_`, `dt10StabPendingGrace_`), +2 vars; top-level census 197 → 199; nested `softMiss` present |
| T7 service read | `'1'`..`'5'` → n, `'9'` → 5, `''`/`'0'`/`'-1'`/`'1.5'`/`'two'`/absent/fault → 0; `dt10StabKnobs_` unchanged; sanitizer keeps `cp` only while > 0 |
| Sibling harnesses (pins moved) | confirm basis 8/8 ×3; input truth RUN-DIGEST ok; source readiness 73 passed; bulk format 9/9; feed cohort binding 21/21; ledger FX 11/11; calendar completeness, cash source, P-142, P-145, board funding 64, decision chain 28/28 — all green |
| History helper | `v1140Source`, `v11311Source`, `v11310Source`, `feedHistorySource` all reconstruct their pinned digests |
| Python | `tests/test_gas_ledger_fx_failed_state.py` 3 passed (after the pin move) |

## S5 — Operator steps (NOT before the first funded ticket of Monday 26 Oct)
1. Squash-merge. The repo mirror moves to v1.15.0; the live project stays v1.14.0 until pasted.
2. **After Monday's ticket:** paste the whole file into the bound Apps Script project (editor must show 1.14.0 first), save, run `dt10SelfTest()` → `[DT10 v1.15.0] self-test`, `pending grace core: ok`, `pending grace (TFB_T10_PENDING_GRACE_DAYS): 0 (…)`. Pasting alone changes nothing (clocks byte-identical).
3. Arm: Script Property `TFB_T10_PENDING_GRACE_DAYS = 1` (owner decision; 2 absorbs two consecutive soft days). Read-back on the next board run: `, pgrace=1` in the status note; `pheld …` whenever a count was paused.
4. Rollback: delete the property (no re-paste) or re-paste v1.14.0.

## S6 — Known limits
- A soft miss is defined by the backend's verdict + rank: a name the backend drops from the audited grid entirely is a hard miss (no row = no evidence).
- The grace pauses the count; it never advances it — a name needs real qualifying days to reach 3/3.
- The deeper fix (smoothing the qualification inputs themselves, a backend retention band) is the §10 build of `TFB_Saturday_Decision_Pack_2026-10-24.md`; this change removes the reset-to-zero cliff only.
