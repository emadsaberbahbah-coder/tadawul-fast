# Commit Sheet - `core/analysis/opportunity_builder.py` v1.28.0 [RANK-CHALLENGER] - 2026-10-27

**Scope:** program register item **A2 / loop piece L4** (project doc `TFB_Self_Correcting_Recommendation_Program_2026-10-27.md`). The backend now computes a second, pre-registered order of the same INVEST pool and publishes it next to the live order, so the nightly scorecard can judge both on the same days before anything changes what is funded. ENV-gated (`TFB_OPP_RANK_MODE`, default `legacy` = v1.27.2 byte for byte on every payload, candidate dict and log line).
**Files:** `core/analysis/opportunity_builder.py` (1.27.2 -> 1.28.0; zero removals), `tests/test_opp_rank_challenger_a2.py` (new, 36 legs), `scripts/verify_deployment.py` (manifest pin 1.27.2 -> 1.28.0 only), `tests/test_ob_conviction_sizing.py` + `tests/test_venue_calendar_ext.py` (exact `== "1.27.2"` pins -> version-tuple floors, house rule), `.github/workflows/ci.yml` (1.1.40: the new harness in the blocking lean lane), this sheet.

## S1 - WHY (evidence)

| Source | Finding |
|---|---|
| October review (71 first seats, 1-26 Oct, IBKR closes) | Engine ROI at seat vs realised excess: Spearman **-0.38**; board ranks 1-3 beat the market **38 %** of the time vs **70 %** for ranks 7-10; names at the ROI cap (>= 34 %) 43 % vs 70 % |
| First nightly scorecard (`_Board_Scorecard`, 27 Oct 02:12 Riyadh, 258 seat episodes since 1 Sep, vs sector ETF / venue index) | Engine ROI vs excess Spearman **-0.17**, verdict `RANKING_NEUTRAL`; ROI >= 34 hit **34 %** vs **53 %** below (October alone 35 % vs 61 %) |
| Mechanism | The live order is `opportunity_score`; 50 of its 100 weight points (ann_roi 20, risk_reward 18, margin_of_safety 12) measure the distance to the analyst target / intrinsic value. A large gap to a consensus target is widest when fundamentals are deteriorating and analysts lag (Yale Insights on consensus target prices). FRST.US, the first funded ticket (26 Oct), was seated two sessions after a 22 % EPS miss on a lagging consensus target. |

Nothing in the repository could test an alternative order on live data: the board only ever records the order it used. This build adds the challenger as a measurement, not as a decision.

## S2 - WHAT (only when `TFB_OPP_RANK_MODE` is `shadow` or `enforce`)

- `normalize_candidate`: attaches `momentum_score` and `target_revision_dir` to the candidate dict (new `_FIELD_ALIASES` entries `momentum_score`, `target_revision`). Under `legacy` nothing is attached (the v1.23.0 P-181 precedent).
- `_build`, step 2: after the live sort of the INVEST pool, `_rank_challenger(audit, invest, crit, mode)` returns the challenger order of the same records, a symbol -> rank map and the meta block. The live list, `_select_and_size`, tickets, near-miss, alerts, kpis and the audit-grid order are untouched (harness P4). `_rank_apply_shadow` adds `shadow_rank` to every INVEST `candidates_rows` record and every selected ticket. `meta.rank_challenger` = `{version "1.0", mode, builder, rule, pool, required_roi_pct, cap_engine_roi_pct 34, cap_first_free 5, rel_band_pts 5, strength_deadband 5, tiers {below_cap, at_cap, below_floor}, coverage {invest_with_momentum, scanned_with_momentum, scanned, sectors_with_median, revision_known}, overlap_top5, champion[10], challenger[10]}`; each list row carries shadow_rank, live_rank, symbol, market, sector, rel, conf, engine_roi_pct, momentum, strength, strength_ref, cap, floor_ok, revision. One INFO line per armed build: `[RANK-CHALLENGER v1.28.0] mode=shadow pool=.. below_cap=.. at_cap=.. below_floor=.. overlap_top5=.. momentum=k/n top5=...`.
- The allocate replay (`board_funding_stage=allocate`) never ranks: it re-runs a frozen subset of the research pool, and GAS copies allocated ticket keys over the research seat, so a replay-pool rank would overwrite the research one (P12). The withheld single pass (portfolio inputs unverified) returns before ranking - for both orders alike.
- `enforce` is **reserved**: it behaves exactly like shadow and sets `meta.rank_challenger.enforce_reserved = true` (P11). A real enforce must also move the GAS cockpit's display order (it sorts seats by its own smoothed `opportunity_score`) and the allocate replay's funding order; it ships with the promotion PR.
- A raising challenger never fails a build: legacy selection, `meta.rank_challenger = {version, mode, error}`, no `shadow_rank` anywhere, the log line carries `error=<Class>` (P13).

## S3 - The challenger order (pre-registered 27 Oct, before any shadow data)

1. **Floor:** engine 12M ROI >= criteria `required_roi_pct` (missing engine ROI fails). Floor failures sort after every floor pass.
2. Among floor passes, ascending key: reliability band desc (`floor(rel / 5)`); sector-relative strength desc (`momentum_score` minus the median of its sector across the scanned pool when the sector has >= 3 values, else the pool median; +1 above +5 points, -1 below -5, 0 inside or unknown - the deadband is exclusive); target-revision direction desc (+1 raised / -1 cut / 0 unknown; no producer yet, inert until register item A8); the **smaller** engine ROI first; symbol.
3. **Cap rule:** names with engine ROI >= 34 % never take ranks 1-5 while a floor-passing name below the cap is available (the first five below-cap names go first, then both groups continue in key order).

Promotion (program doc section 4, unchanged): >= 40 scored seats per order and >= 20 trading days; challenger hit rate at +5 sessions vs the sector/venue reference >= champion + 10 points; mean excess >= champion; worst-seat and churn guardrails; inconclusive after 40 trading days -> keep the champion. Changing any constant above (`RANK_*`) is a new challenger and restarts the window.

In-sample sanity check only (October first seats, IBKR prices, momentum not available, so strength reads neutral): Spearman of the challenger key vs excess **+0.15** to date / **+0.17** at +5 sessions; the live rank **-0.15** / **-0.09**; top third by the challenger 72 % hit at +5 (mean +1.2 pp) vs 50 % (-0.4 pp) by live rank. Design evidence, not proof - the design came from this sample; the shadow window is the test.

## S4 - Tests

- `python tests/test_opp_rank_challenger_a2.py` -> **RESULT 36 PASS / 0 FAIL**; under pytest -> 1 passed. Legs: P1 surface + gate parsing; **P2 golden** - with the gate unset, `legacy` and an unknown word, the single-pass, withheld, research, allocate and empty payloads and every `normalize_candidate` dict are byte-identical to the untouched v1.27.2 base (`git show 53c8544e:...`; `OB_BASE` fallback; SKIP line on a shallow checkout), with zero `[RANK-CHALLENGER]` lines; P3 the base ignores the gate; P4 shadow is selection-neutral on every surface; P5 hand-checked 7-row pool (challenger F B G A D E vs champion A D F G B E, tiers 3/2/1, ticket ranks A4 D5 F1 G3 B2, row detail, coverage); P6 cap rule (top-band cap names held at 6-7; fill behind 3 below-cap names); P7 sector vs pool median, exclusive deadband; P8 in-band key order (revision, then smaller ROI, then symbol); P9 floor incl. criteria-driven `required_roi_pct`; P10 revision parsing table; P11 enforce reserved; P12 research carries / allocate never ranks and funds what legacy funds; P13 exception safety; P14 one log line with the counts; P15 per-build read.
- Opportunity-builder suites (`tests/test_ob_*.py`, `test_opportunity_builder*.py`, `test_opportunity_blocked_invariant.py`, `test_board_funding_route.py`, `test_top10_selector.py`) -> 280 passed after the two exact version pins became tuple floors.
- The ci.yml lean-lane list (159 files) -> **4,909 passed, 35 skipped**. `python -m compileall main.py core routes scripts tests` PASS. `tests/test_verify_manifest_pins.py`, `tests/test_render_env_contract.py` PASS (no contract row changes: the gate is not added to `main.py` `_OB_GATE_ENVS` or the verifier FLAGS in this PR).
- AST inventory vs 53c8544e: functions 202 -> 208 (added `_env_rank_mode`, `_rank_revision_dir`, `_rank_median`, `_rank_challenger`, `_rank_apply_shadow`, nested `_row`), zero removals; module constants added `RANK_*`, `_RANK_REVISION_UP/DOWN`.

## S5 - Rollback
- `TFB_OPP_RANK_MODE` unset / `legacy` -> v1.27.2 behaviour on the next build (read per call, no restart).
- `git revert` restores v1.27.2 exactly (the two test pin edits are compatible with both versions).

## S6 - Known limits
- `shadow_rank` reaches the sheet only through `16_Decision_Top10` (A2 part 2, GAS: `_Selection_Log` "Shadow Rank" column and a daily champion/challenger top-5 log); until that paste, the order is visible in the payload and the Render log line.
- `momentum_score` is a short-horizon blend (RSI peak at 55, 1-day and 5-day change, 52-week position, volume ratio); its coverage is reported every build and a name without it is neutral. Sector-relative strength therefore means "stronger than its sector this week", not over months.
- `TFB_OPP_RANK_MODE` is in the board-funding policy fingerprint (`TFB_OPP_*`): flipping it between a research pass and its allocate pass seconds apart fails that one replay closed. Arm outside a sync.
- Not yet registered in `main.py` `_OB_GATE_ENVS` (health JSON) or the verifier FLAGS; arming is verified from the log line and `meta.rank_challenger`. One hygiene pass later.
- Enforce is reserved (S2).

## S7 - Operator steps
1. Review and squash-merge (Render auto-deploys; the gate defaults to legacy, so nothing changes on the board).
2. After the deploy is live and outside a sync: set `TFB_OPP_RANK_MODE=shadow` on the Render service (one ENV arming).
3. Verify on the next board build: Render log `[RANK-CHALLENGER v1.28.0] mode=shadow pool=...` and the payload's `meta.rank_challenger` (pool, tiers, coverage.invest_with_momentum, overlap_top5).
4. Paste `16_Decision_Top10` v1.17.0 (A2 part 2) so the sheet logs both orders; the scorecard compares them nightly from then on (evaluation window from the first logged day; promotion only by the section 4 rule).
