# TFB Commit Sheet — 16_Decision_Top10.gs v1.14.0: investability-reason table (T1), earnings hold (T5), turnover token + SelLog column (T6) — cockpit halves

Date: 2026-10-17 (Saturday) · Lane: GAS (paste; A4 class once proven in repo) · Brief: Codex Brief 2026-10-17 v2 §3 T1 / T5 / T6 (cockpit halves) · Builder halves: opportunity_builder v1.27.0 (T1) / v1.28.0 (T5), run_shadow_scorer v1.10.0 (T6 scorer half)

## S0/S1 — Base pin
| File | Base | Delivered | Change |
|---|---|---|---|
| `apps_script/16_Decision_Top10.gs` | sha256 `2e0967dd76bb260832eba1053ce62b682d3dc9ecd4426f9580c363663860ecca` (v1.13.9, 8,397 ln) | sha256 `71343eb11491e691b7afe58c0fa091ae704ac70394a6e0947f009a761b3e23bb` (v1.14.0, 8,596 ln) | **+202 / −4** — 11 enumerated edits, each occurring exactly once; the 4 removed lines are re-emitted (header version line, `var DT10_VERSION`, the SelLog header list tail, the SelLog row tail) |
| `tests/js/dt10_v1140_history.js` | — | sha256 `f256d7059938a653efb7738f9fbb3f0104a28347632fcfdffd0ea6387941bc92` | NEW — the v1.14.0 reversal layer, **generated from the same edit list** that produced the source; `revertV1140(delivered)` → v1.13.9 → `feedHistorySource` → the pinned v1.13.8 digest `9d11832a…` |
| `tests/js/dt10_v1140_reasons_earnings_turnover.test.js` | — | sha256 `17729542336f91c0c8102ce69303f666c8a3cbcd13cc438c80f57353aa944ca1` | NEW, 12 cases, full source in `node:vm` |
| `tests/js/decision_input_truth.test.js` · `tests/js/dt10_feed_cohort_binding.test.js` · `tests/js/decision_source_readiness.test.js` | — | — | version pins 1.13.9 → 1.14.0 (incl. the self-test string, the run-log details and the run-log regex), census 191/103 → 199/104; both history callers chained through `revertV1140` |
| `.github/workflows/ci.yml` | — | — | +1 `node tests/js/dt10_v1140_reasons_earnings_turnover.test.js` |

## S2 — What (ES5 only; three Script Property kill switches, value `'1'`)
| Item | Change | Default | Kill |
|---|---|---|---|
| **T1** | New table **INVESTABILITY — BY ENGINE REASON** (`Engine Reason · Candidates · Share % · Example Symbols`) directly under DATA GAPS, read from the builder's `meta.investability_reasons` (no GAS recomputation — the fixture candidates carry no `engine_gate`); the section is skipped when the block is absent (older backend) or empty | ON | `DT10_INVEST_REASONS_LEGACY` |
| **T5** | `dt10ApplyEarningsHold_` after `dt10FinalizeBoard_`: every seat the builder marked `sizing_hold === 'earnings'` (only under `TFB_T10_EARNINGS_GATE=enforce`) gets the P-145 dash vocabulary via `dt10EarningsSuspend_` (entry/stop/TP/ticket/shares/gain `—`, `funds_from` blanked on a copy, advisor note `EARNINGS HOLD (Nd to print) — sizing suspended …`, `_earn_hold=true`); the seat is **kept** (a hold is a pause — clocks untouched); status token `earn-held n` | ON (inert until the builder marker exists) | `DT10_EARN_HOLD_LEGACY` |
| **T6** | Status token `turnover=N/M new` = seats of the final board whose stability clock started today (`_stab_days === 1`: confirmed, fast-track and displacement entrants alike; epoch-keyed, so stable across same-day reruns — unlike the stab note's `N new`, which counts confirmed entrants only and empties on a rerun); blank when stability is OFF/frozen. `_Selection_Log` column 34 **New Today** (`YES`/`NO`/blank), header healed on existing sheets (the v1.3.1 col-32 pattern); positional withhold indexes (≤ 27) untouched | ON (column always; token switchable) | `DT10_TURNOVER_TOKEN_LEGACY` |
Functions added 8 (`dt10InvestReasonsLegacy_`, `dt10InvestReasonRows_`, `dt10EarnHoldLegacy_`, `dt10EarningsSuspend_`, `dt10ApplyEarningsHold_`, `dt10TurnoverLegacy_`, `dt10TurnoverNote_`, `dt10SelLogNewToday_`), var added 1 (`DT10_INVREASON_HEADERS`), removed 0. `dt10SelfTest` prints the three switch states.

**Why default ON (brief protocol):** all three are display / read-back surfaces — no selection, funding or clock path changes; the only sizing effect (T5) follows a builder marker that is itself dark behind `TFB_T10_EARNINGS_GATE`. The brief's T6 wording is "measure and publish".

## S3 — Audits
| Check | Result |
|---|---|
| Hash chain: `feedHistorySource(revertV1140(delivered))` == pinned v1.13.8 digest; each of the 11 edits occurs exactly once (mutation: a second `var DT10_VERSION` line is refused) | PASS |
| Added lines: ES5 guard (`let/const/=>/`/class/.../.includes(`), no smart quotes, LF only | PASS (the first attempt tripped the guard on `=>` and the word "class" inside *comments*; scrubbed, regenerated) |
| `new vm.Script(source)` | PASS |
| Census 199 functions / 104 vars, the 8 + 1 designed names present and callable | PASS |
| New harness: T1 rows shape and `[]` on 7 odd shapes (incl. a *string* `rows`, caught in review); kill switch spelling; table sits between DATA GAPS and CANDIDATES · T5 dash vocabulary, copy-on-write `funds_from`, `?d`/`0d` notes, `earn-held 2` with unmarked and `sizing_hold:'other'` seats untouched, no seat removed, legacy inert, hook + token wired · T6 token counts/blank/string days, legacy silences the token only, order `stab | turnover | earn`, SelLog 34 headers, row width 34, `YES/NO/''`, header heal line, withhold indexes unchanged | **12/12** |
| Existing harnesses on the delivered source: `decision_input_truth` 20 passed / 3 skipped (dual-tree need `--base`), `dt10_feed_cohort_binding` 21/21, `decision_source_readiness` 73, `decision_board_funding` 64, `apps_script_syntax` 4 sources, `pf_ibkr_evidence` 14/14, `pf_source_receipt` 8/8, `portfolio_reconciliation_inputs` 20, `test_dt10_calendar_completeness.js` 25/25; `tests/test_release_decision_feed.py` 86 passed / 10 skipped (goldens need `TFB_TEST_SUPERSEDE_BASE_DIR`) | PASS |
| Dual-tree legs (`--base` / `DT10_BASE`) | **not re-targeted**: they prove v1.13.9-vs-v1.13.3 and v1.13.8-vs-v1.13.7 against base files outside the repo; run against v1.13.9 they fail by construction (they expect the older base). The v1.14.0 proof is the hash chain above plus the pure-function harness. |

## S4 — Delivery / paste
`apps_script/16_Decision_Top10.gs` (paste as one file; run `dt10SelfTest` — the report gains `v1.14.0 switches: invest reasons ON | earn hold ON | turnover token ON`) · the three new/changed JS harness files · `ci.yml` · this sheet. Builder side: T1/T5 need the Render deploy of opportunity_builder ≥ 1.28.0 (the table is skipped, not broken, on an older backend).

## S5 — Read-back
1. Next cockpit run: status line carries `… | stab[strict]: … | turnover=N/10 new | … | earn ⚠k/10 …`; `_Selection_Log` header gains `New Today` in col 34 and new rows carry YES/NO.
2. With the 1.28.0 backend deployed: a second table under DATA GAPS whose counts sum to the Investability row (466 on the 17-Oct board).
3. `earn-held n` appears only after `TFB_T10_EARNINGS_GATE=enforce` is armed (T5 §8 item) and the data path carries earnings dates.
4. Rollback: paste v1.13.9, or set the three properties to `'1'` (the SelLog column stays, blank-tolerant).

## Known limits
- The new table adds rows above the CANDIDATES grid (cockpit target rows shift by table height + 3); `DT10_INVEST_REASONS_LEGACY=1` restores the previous layout exactly.
- Status-line byte-identity with v1.13.9 holds only under the kill switches (the token is on by default); `_Selection_Log` width differs by the one appended column regardless.
