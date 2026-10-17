# TFB Commit Sheet — L4 Evidence gate on qualification, observe first (opportunity_builder v1.30.0, 16_Decision_Top10 v1.18.0)

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §2 L4 · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `c44c3e2`

## 1. What ships

"This is the first place outcomes can change a decision." Gate **`TFB_T10_EVIDENCE_GATE = off (default) | observe | enforce`**, floors `TFB_T10_EVIDENCE_MIN_HIT` (55) and `TFB_T10_EVIDENCE_MIN_N` (100).

| Piece | What |
|---|---|
| **Transport** | The backend cannot read the workbook. The cockpit already reads `_Forecast_Scorecard` (v1.17.0 strip); with Script Property **`DT10_EVIDENCE_SCORECARD_SEND = on`** it sends the `Investability x Recommendation` rows as `body.evidence_scorecard = [{horizon, cohort, n, hit}]` (pure `dt10EvidenceScorecardRows_`, ≤ 400 rows, largest n first); the route forwards it in `upstream_meta`; the builder indexes it per build. Status line gains ` \| scorecard sent (N cohorts)`. Property off → request byte-identical to v1.17.0. |
| **Gate** `Evidence (Scorecard)` | Appends after Earnings Proximity (NON_CRITICAL). Cohort = Entry Investability (from the engine gate block) × Recommendation (normalised, e.g. STRONG_BUY) at the **plan horizon** (`period_months` → 1M / 3M / 6M / 1Y), falling back to the longest published horizon that carries the cohort, then ALL. **observe:** always PASSED; rows whose cohort is below the floors or has no outcomes gain one countable tag on Failure Reason: `[evidence-observe] cohort=INVESTABLE|ACCUMULATE@1M hit=48.0% n=400 BELOW` / `… no outcomes yet`. **enforce:** the gate FAILS for those rows (verdict follows the existing chain; the seat goes to the next name). **No scorecard in the request** → PASSED `no scorecard` in both modes and counted (absent evidence is not evidence against). |
| **Read-back** | `meta["evidence_gate"] = {mode, min_hit, min_n, horizon, scorecard_rows, evaluated, no_scorecard, no_cohort, would_fail, failed}` on the certified and withheld paths, only when armed. Off is byte-identical to v1.29.0 save the version stamp. |

## 2. Acceptance (brief: "[evidence-observe] tags on the full audit set; zero verdict change in observe")

- `test_observe_tags_below_floor_rows_and_keeps_verdicts`: every verdict equal to the off build; the ok cohort (INVESTABLE|BUY 63.3 % n=671) carries no tag; below-hit, below-n and no-cohort rows carry exactly one tag each; the WATCHLIST row keeps its Investability reason and appends the tag; seats unchanged; `would_fail 4, failed 0`.
- `test_enforce_fails_below_floor_rows_and_removes_seats`: the three weak cohorts fail the gate (`first_fail = Evidence (Scorecard)`), the board keeps only the evidenced seat; `failed 4`.
- `test_no_scorecard_passes_and_is_counted`, `test_floors_and_horizon_follow_env_and_criteria` (period 3 → 3M; fallback 1Y for BUY), `test_withheld_path_carries_tags_and_read_back`.

## 3. Decisions (flag for Emad)

- **Observe is the delivery; enforce is Emad's decision on the Scorecard** (brief). Arming order: `TRACK_FORECAST_SCORECARD` writes the tab (L3) → `DT10_EVIDENCE_SCORECARD_SEND=on` → `TFB_T10_EVIDENCE_GATE=observe` for one cycle → read the tags and `would_fail` → enforce.
- **A cohort with no outcomes yet fails under enforce** (n=0 < MIN_N, the letter of the brief). Outcomes accrue from Performance_Log regardless of selection, so a new cohort is not starved; the first cycles under enforce would nonetheless remove every unseen cohort — read the `no_cohort` count in observe first.
- **Plan horizon 12M has no matured outcomes** (1Y records are rare): the fallback to the longest published horizon is what the gate will mostly use; the horizon used is named in every tag (`@1M`).
- **Cohort key uses the engine's Recommendation** (the row's `recommendation_detailed`, normalised), matching the tracker's Entry Recommendation vocabulary.

## 4. Protocol evidence

- **Anchored edits:** builder 11 (`scratchpad/edit_l4_builder.py`), route 1, GAS 7 (`scratchpad/edit_l4_gas.py`); every anchor `count == 1`; reversal layer `tests/js/dt10_v1180_history.js` → v1.17.0 → … → pinned v1.13.8 digest (V2).
- **Zero-removal proof:** builder removed lines = `Version:` and `OPPORTUNITY_BUILDER_VERSION` (rewritten); GAS removed = `Version:`, `DT10_VERSION` and the status-line concatenation (rewritten with `scorecardNote`, `''` when off). Functions added: builder 8, GAS 2; removed 0. Census 217 → 219 / 106.
- **ES5:** every added GAS line passes the V3 guard.
- **Dark by default:** ENV off, Script Property off; no ENV, Script Property or cell changed by this PR.
- **Harness:** pytest `tests/test_ob_evidence_gate_l4.py` 23/23 + T1/T2/T5/decision suites 220 passed; `node tests/js/dt10_v1180_evidence_send.test.js` 5/5; the ten other GAS harnesses pass (pins 1.18.0 / 219 / 106; `scorecard rows core: ok`); ruff clean; `ci.yml` lean list + JS step updated.
