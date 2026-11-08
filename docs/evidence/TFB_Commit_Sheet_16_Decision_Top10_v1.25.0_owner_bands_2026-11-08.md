# Commit sheet - 16_Decision_Top10.gs v1.25.0 [OWNER-BANDS] - 2026-11-08

**Decision.** Owner decision A, 8 Nov 2026 (AskUserQuestion answer: "High or Medium (Recommended)" - keep ROI < 34% and the 2-ticket cap; Low stays held).

**Problem.** The 10:14 Riyadh render applied fresh IBKR funding evidence. The only funded ACTIVE seat, EFC.US (Confidence Medium, Engine ROI 12M 20.1), was held by the v1.19.0 owner rule enforced through the v1.20.0 ticket guard: `tguard: held 1 (EFC.US owner rule)`.

**Evidence that the High-only part is the weaker filter.** Source: `tfb_archive.v_outcome_engine`, engine-scored INVESTABLE calls, snapshot 8 Nov.

| Horizon | High: wins, avg | Medium: wins, avg |
|---|---|---|
| 1W | 33.3%, -1.67 pp (n=132) | 58.6%, +0.54 pp (n=483) |
| 2W | 23.1%, -3.47 pp (n=121) | 55.6%, +0.94 pp (n=369) |
| 1M | 26.0%, -7.68 pp (n=177, 35 names) | 61.3%, +0.64 pp (n=328, 101 names) |
| 3M | 24.1%, -12.05 pp (n=83) | 48.1%, +1.94 pp (n=135) |

`v_outcome_calibration` flags "High confidence under-delivers".

The ROI part keeps its own evidence (opportunity_builder v1.28.0 WHY): names at the 34% cap beat their reference 34-35% of the time, against 53-61% below the cap.

Caveat: High calls are concentrated in fewer names, so the effective sample is smaller than n.

**Change (one knob, default off).**
- Script Property `DT10_OWNER_RULE_BANDS` holds a list of HIGH / MEDIUM / LOW.
  - Separators: `,` `;` `+` `/` or spaces. Any case.
  - HIGH is always included.
  - Off when the property is absent, blank, junk, HIGH alone, contains any unknown word, or the read faults. Off means v1.24.0 byte for byte.
- When armed, `dt10OwnerRuleCell_(t, bands)` accepts each listed band. The failing part is still named, e.g. `rule: no (Low; ROI ...)`.
- The ticket guard reads `cfg.ownerBands`. That key is set ONLY when `DT10_GUARD_OWNER_RULE=on` AND the knob is armed.
- The Owner Rule column (`payload._dt10_rule.bands`) and its note use the same band set.
- The tguard status token gains `, rule bands HIGH/MEDIUM`. Its Logger line reads `owner rule on (HIGH/MEDIUM)`.

**Diff.**
- 16 anchored hunks.
- Functions: +4 (`dt10OwnerRuleBandsParse_`, `dt10OwnerRuleBandOk_`, `dt10OwnerRuleBandsText_`, `dt10OwnerRuleBands_`). Vars: +2 (`DT10_OWNER_RULE_BANDS_PROP`, `DT10_OWNER_RULE_BAND_ORDER`). Nothing removed.
- ES5 / ASCII. Every prior WHY kept verbatim. The WHY block is newest-first.
- `dt10SelfTest`: +2 lines (`owner rule bands (DT10_OWNER_RULE_BANDS): ...`, `owner rule bands core: ok ...`).
- Base: v1.24.0, sha256 `a3e5b69d6f1f99c60a7d70ddee8e2743b41ca90bef00d5fe17a70cc459e5e746`.
- New: v1.25.0, sha256 `ce61ed2346b74131a5f5f0e0573006d3cf0163960ad128d0ceb30ddff3013860`, 12,588 lines, 674,830 bytes.

**Harness.** `tests/test_dt10_v1250_owner_bands.js` (+ `.py`) gives PASS 8/8, x3 identical, digest `ebc6b00e68460c24`.
- O0: base oracle, census and hunk reversal.
- O1: GOLDEN NEGATIVE on v1.24.0. The same cfg still holds EFC.US. When armed, EFC.US is kept, Low and Medium-at-ROI-cap are held, earnings go first, and the cap applies after the rule.
- O2: parser.
- O3: kill. Absent, blank, HIGH, junk, owner rule off, or a fault all deep-equal v1.24.0.
- O4: service reads, the token and the Logger line.
- O5: self-test.
- O6: lint.
- O7: four mutants.

**History chain.** `tests/js/dt10_cash_utc_history.js` adds `V1240_SHA256`, `EDITS_V1250` and `v1240Source`. `v1231Source` now starts from `v1240Source`. The v1.24.0 harness now reads the rebuilt v1.24.0 and keeps its digest `608d37e347d28922`.

**Pins moved to 1.25.0.** confirm_basis, pending_grace, v1190, v1200, v1210, post_earnings, shadow_rank_log, shadow_momentum and funding_evidence, plus tests/js decision_input_truth, decision_source_readiness, gas_ledger_fx_failed_state, dt10_bulk_format and dt10_feed_cohort_binding.
- Census: functions 262 -> 266, variables 154 -> 156.
- Self-test line counts: +2.
- The v1210 report filter drops `owner rule bands` lines.
- ci.yml 1.1.66.

All 30 node harnesses in CI pass locally.

**Arm (after merge and paste).**
1. Run `tfbSetAnyProperty` with `DT10_OWNER_RULE_BANDS` = `HIGH,MEDIUM`.
2. Run `dt10SelfTest`. Expect `[DT10 v1.25.0]`, `owner rule bands (DT10_OWNER_RULE_BANDS): HIGH/MEDIUM`, `owner rule bands core: ok`, and zero FAIL.
3. On the next render, the tguard token ends `, rule bands HIGH/MEDIUM`.

**Rollback.** Delete the property.

No gate, floor, cap or sizing rule changed. The 2-ticket cap, earnings and ex-dividend holds, and ROI < 34 are untouched.
