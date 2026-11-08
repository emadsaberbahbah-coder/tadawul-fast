# Commit sheet - 16_Decision_Top10.gs v1.26.0 [FUNDABLE-SEATS] - 2026-11-08

**Decision.** Owner decision C, 8 Nov 2026: "Fundable names first. Non-USD names stay visible but no longer take a seat from a USD/SAR name."

**Why the cockpit, not the backend.** Codex's review of PR #929 (P1), verified here, showed a backend re-sort cannot hold:
- The research stage returns every qualifying pick, not just the first Max Selected.
- `dt10StabCore_` then seats names by smoothed score.
- Example: limit 1, S.SR seated at 60, and E.PA (EUR) confirms at 90. v1.25.0 displaces S.SR. The allocate stage then cannot fund E.PA, because the IBKR capture proves only the USD rate.
- #929 is superseded by this change.

**Change (one knob, default off).**
- Script Property `TFB_T10_FUNDABLE_CCY` holds a list of 3-letter currency codes (separators `,` space `;` `+` `/`; any case).
- It is off when the property is absent, blank, holds any token that is not 3 letters, or the read faults. Off means the v1.25.0 seats byte for byte; `knobs.fundable` is set only when armed.
- A symbol's currency comes from its candidate row, else from its raw ticket. A symbol with no currency counts as unlisted.
- When armed, in the stability core:
  - (a) Confirmed challengers and the fast-track fill take listed names first. Score order is kept within each group.
  - (b) A listed challenger takes the seat guard's non-qualifying pick when there is one. Otherwise it takes the weakest unlisted seat without the score test (tag `nf`). Outage-paused seats and seats entered on this run are never taken this way.
  - (c) An unlisted challenger may only take an unlisted seat, and only by the v1.25.0 score test (tag `u`). If there is none, it waits.
- Status line: note token `, fundable=SAR/USD[ took SYM(nf)]`. `audit.fundable {ccys, took}` is written only while armed.

**Diff.**
- 14 anchored hunks.
- Functions: +3 (`dt10StabFundableParse_`, `dt10StabFundableText_`, `dt10StabFundable_`) plus three closures inside the core (`fundOk`, `fundFirst`, `fundVictim`). Vars: +1 (`DT10_T10_FUNDABLE_PROP`). Nothing removed.
- ES5 / ASCII. `dt10SelfTest` gains 2 lines, zero FAIL.
- Base: v1.25.0, sha256 `ce61ed2346b74131a5f5f0e0573006d3cf0163960ad128d0ceb30ddff3013860`.
- New: v1.26.0, sha256 `3294a4affdc58e441d1a103f8db3930680da0060fa7c767ef0865961e23a40e9`.

**Harness.** `tests/test_dt10_v1260_fundable_seats.js` (+ `.py`) gives PASS 7/7, x3 identical, digest `ba36d9b785c89552`.
- F1: GOLDEN NEGATIVE (the #929 trace) on v1.25.0, which ignores the knob.
- F2: the seven armed rules.
- F3: kill. With no knob the core deep-equals v1.25.0 on eight scenarios, with and without the seat guard.
- F4: self-test.
- F5: lint.
- F6: four mutants.

**History and pins.** `v1250Source` (SHA pinned) now heads the chain, and the v1.25.0 harness reads the rebuilt v1.25.0.
- Pins moved to 1.26.0 in 14 harnesses.
- Census: functions 266 -> 269, variables 156 -> 157. Self-test line counts +2. The v1210 report filter drops `fundable seats` lines.
- ci.yml 1.1.67.
- All 31 CI node harnesses and 246 cockpit Python tests pass.

**Arm (after merge and paste).**
1. Run `tfbSetAnyProperty` with `TFB_T10_FUNDABLE_CCY` = `USD,SAR`.
2. Run `dt10SelfTest`. Expect `fundable seats (TFB_T10_FUNDABLE_CCY): SAR/USD` and `fundable seats core: ok`.
3. On the next render, expect `, fundable=SAR/USD`.

**Rollback.** Delete the property.
