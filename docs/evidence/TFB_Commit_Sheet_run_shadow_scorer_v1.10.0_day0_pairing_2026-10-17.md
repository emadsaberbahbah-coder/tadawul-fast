# TFB Commit Sheet — run_shadow_scorer.py v1.10.0: day-0 seat pairing, dark (T6, scorer half)

Date: 2026-10-17 (Saturday) · Lane: GitHub (shadow_scorer.yml runs this script daily at the 15:20Z slot) · Brief: Codex Brief 2026-10-17 v2 §3 **T6** (scorer half; the cockpit half — `turnover=N/10 new` token + `_Selection_Log` column — ships with the DT10 Apps Script bump) · Decision for Emad: arm `TFB_S1_NEW_SEAT_DAY0=spot` or not (scoring-integrity question)

## S0/S1 — Base pin
Branch `claude/new-session-twoifp` on `cf8e875` (G5/G10) ← `4e88ae6` (L2) ← `fa42a55` (L1) ← main `11ef134`.

| File | Base | Delivered | Change |
|---|---|---|---|
| `scripts/run_shadow_scorer.py` | sha256 `254269f002bb18892c534260fbbb6c0553182c40820894a16ffae2b9e370978f` (v1.9.3) | sha256 `fde0a38da5d258c502cf348064d8c3ecc5d92c815b9ecff92eff687a89de299c` (v1.10.0) | **+128 / −3** (10 anchored edits + 2 follow-ups, each `count == 1`); the 3 removed lines are re-emitted (`measured[...]` dict gains `day0`; `_freshness_detail` call gains a kwarg; `SCRIPT_VERSION`) |
| `tests/test_s1_day0_seat_pairing_t6.py` | — | sha256 `119b13c2b7a26322d8192f9d09c5a658d35a6e4eee41393f5622ac643f62c6e8` | NEW, 9 tests: real `main()` on a stub sheet (P-176/P-186 harness pattern) |
| `tests/test_s1_base_policy_p186.py` | sha256 `86b39c745226839ec589818c421445f8adbce6605902953fc5f99ca693cb4670` | sha256 `f4d8173240f025bb332b49449e3e2c345b90391ec432f3e14a2f17b6e223ef77` | version-token literals `v1.9.3` → `v1.10.0`; selftest count pin 116 → 120 (text only) |
| `tests/test_s1_board_fresh_guard.py` | sha256 `48c39896ac6a9fa3ec89d53c9893600a5cc230ed19680056e70c52f473174688` | sha256 `04ce81803f01832c0b64447befc3fa36b9ebbb9c8b6bf30d779d635eace53d10` | version-token literals `v1.9.3` → `v1.10.0`; selftest count pin 116 → 120 (text only) |
| `tests/test_s1_criteria_v2_p204.py` | sha256 `0ca4ec1e493c1ea6b2f4cc694db10dbaa045b74477774bdc3c0bb173cefd439b` | sha256 `86757fd87da4b3d01b0dbb2cfc5ef76fe2fec6f808f0e4aed2574d5b966149e5` | version-token literals `v1.9.3` → `v1.10.0`; selftest count pin 116 → 120 (text only) |
| `tests/test_s1_day_key_p176.py` | sha256 `4d90950880f3c7868592ac69376f69c2bed55c39721cfecf8f73c89aab737428` | sha256 `19bd6f726f8f5221b91610d039cb713ea6d3bd98e3cd5810a60e209fab43aa36` | version-token literals `v1.9.3` → `v1.10.0`; selftest count pin 116 → 120 (text only) |
| `.github/workflows/ci.yml` | — | — | +1 lean-list line |

## S2 — Why (measured in the brief, mechanism confirmed in code)
Shadow_History, October: 12 scored / 4 excluded (10-05, 10-08, 10-12, 10-15), all four `reason=fresh-floor`, zero `no-challenger`; challenger turnover 50–100 %/day; 16-Oct `[S1-FRESH] … new=[AX,CRC,EPRT,GLPI,VICI]`. In `basket_return_fresh` pairing walks the **base row's** prices, so a seat with no prior price is in none of fresh / stale / nopair — yet `_chal_frac = n_fresh / len(chal_syms)` counts it in the denominator. Five new seats of ten cap fresh coverage at 50 % < 60 % even when every pairable leg is fresh. (The 16-Oct brief's C1 — withheld-board starvation — was the wrong loop; corrected in v2 §1.)

**One finding from building the fixture, for the sitting:** the scorer also prices the base row's *departed* seats (`need |= set(p["symbols"])`), and a departed seat that still prices is counted as a fresh leg (the P-186 `fresh=5/3` asymmetry). The starvation therefore bites when departed seats are **unpriced** (delisted, blocked, provider miss) or the base row carries fewer priced seats; the structural fixture below leaves them unpriced (`nopair`). The live 16-Oct `[S1-FRESH]` line's `nopair=` list is the read-back that confirms which case the four October days were.

## S3 — What (default legacy = byte-identical)
`TFB_S1_NEW_SEAT_DAY0 = legacy (default) | spot` (unknown → legacy, never raises).
Under **spot**, for the equal-weight baskets (champion, challenger, EQW; benchmark and seed days untouched): a seat in today's basket with **no prev price** and a bar **strictly newer** than the base row's date (the same honesty rule a paired leg must meet) is paired at its own day-0 spot — return 0, counted fresh. The basket return is the equal-weight mean over paired legs including those zeros (bought at today's close, earned nothing today). stale / nopair semantics untouched. Read-back only when armed **and** a day-0 seat exists: `day0=[…]` on the `[S1-FRESH]` line (verdict, S1_Gate meta cell, `_Run_Log` details JSON `freshness.day0`) and ` day0=N` on the Shadow_History note. New pure helpers `day0_spot_seats`, `blend_day0`; reader `_new_seat_day0_mode`; selftest +4 (120/120).

## S4 — Audits
| Check | Result |
|---|---|
| `python scripts/run_shadow_scorer.py --selftest` | **120/120** (was 116/116) |
| New harness: T1 legacy → `DAY_EXCLUDED_INFRA fresh=5/10 stale=0 reason=fresh-floor`, `nopair=[5 departed] new=[5 new]`, no `day0` anywhere · T2 spot → day **scored**, return = 5-leg mean ÷ 10 (exact), note `n=10 day0=5`, `fresh=10/10 … day0=[AX.US,CRC.US,EPRT.US,GLPI.US,VICI.US]` on verdict + S1_Gate + `_Run_Log` JSON, new seats' spot stored in Prices JSON (pairable tomorrow), champion/benchmark rows as legacy · T3 `legacy`/junk/`lastscored`/empty → history, gate and log byte-identical to legacy · T4 already-scored shape (no new seats) under spot → byte-identical · T5 full turnover → legacy `fresh=0/10` excluded, spot scores flat `0.0`, `n=10 day0=10` · T6 new seat whose bar is not newer than the base row → not day-0, day still excluded | **9 passed** |
| Existing scorer suites (regret read failure, board-fresh guard, shape guard, P-176 day key, P-204 criteria v2, calibration freshness, P-201 zero baseline, P-186 base policy) | **93 passed** after updating the four version-token literals (they hard-code `vX.Y.Z` in `[S1-…]` tokens; both failed on the bump alone, both pass on the base file) |
| ruff `E9,F63,F7,F82,F401,F841` | PASS |
| Byte-identity | legacy: `day0 == []` everywhere → every `if`-guarded addition is skipped; the P-186 dual-tree leg (`S1_BASE`) is the standing proof and is unchanged in shape |

## S5 — Delivery
`scripts/run_shadow_scorer.py` · `tests/test_s1_day0_seat_pairing_t6.py` (NEW) · four scorer test files (version-token literals) · `.github/workflows/ci.yml` (+1) · this sheet. No ENV change (the Variable is Emad's, §8).

## S6 — Arming / read-back
1. Merge: nothing changes (default legacy; the next shadow_scorer run's verdict is byte-identical save the version token).
2. Decision at the sitting: `TFB_S1_NEW_SEAT_DAY0=spot` as a repository Variable on shadow_scorer.yml (map it like `TFB_S1_CRITERIA_V2`, `${{ vars.TFB_S1_NEW_SEAT_DAY0 || 'legacy' }}` — one line, operator item). Read-back on the first turnover day: `day_scored`, `[S1-FRESH] … day0=[…]`, note `n=10 day0=k`, S-1 counter advances where legacy would have excluded.
3. Replay of the four October days: the real rows cannot be re-scored (append-only history; the fixture export strips Prices JSON), so the proof is the structural harness above plus the live `nopair=` read-back on the next turnover day.
4. Rollback: unset the Variable.

## Known limits
- `spot` dilutes a turnover day's return toward zero by construction (10 legs, 5 of them 0); the alternative — divide by the base row's pairable legs (P-186 `lastscored`) — leaves the return undiluted but needs its own decision. Both are measurable now; neither is armed.
- The cockpit half of T6 (`turnover=N/10 new` status token, `_Selection_Log` column) is not in this commit: it lands with the Apps Script bump that also carries T1's table and T5's token, because the JS harness pins the `.gs` source hash and must be extended once per version.
