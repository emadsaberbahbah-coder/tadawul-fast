# TFB Commit Sheet - core/analysis/opportunity_builder.py v1.27.0 [conviction-weighted seat sizing + binding absolute cash buffer]

Date: 2026-10-19 (Monday, 12:00 to 13:30 Riyadh). Base: `origin/main` 137cd80 (#796), stacked in PR #797 after the release automation commit.
Delivered: `core/analysis/opportunity_builder.py` 1.26.2 -> 1.27.0, `tests/test_ob_conviction_sizing.py` (NEW, S1-S10),
`tests/test_venue_calendar_ext.py` (pin 1.27.0), `scripts/verify_deployment.py` (pin 1.27.0), `.github/workflows/ci.yml` 1.1.23, this sheet.
Not edited: gates, ranking, `max_selected`, diversification caps, the percent cash floor, funding plans, rotation, capital calls, the research stage, the frozen-snapshot replay contract, the ticket schema, every Apps Script file.

## Why (owner request, 19 Oct)

The Top_10 export shows every sized seat at the per-seat cap: `_size_one` gives each seat
`min(MaxWeight% x budget, remaining)`, so the first seats take the cap and the rest get the scraps
("investment is distributed with almost the same value"). The owner wants:

1. cash distributed by opportunity: expected ROI with its risk, not equal tickets;
2. a small absolute cash buffer (USD 500 to 1,000) and otherwise everything invested;
3. when cash is short, a sell-to-fund proposal out of the holding with the worst expected ROI.

Item 3 already exists (`FUNDABLE_BY_ROTATION`: a held equity worse by >= `TFB_OPP_ROTATION_EDGE_PP` (8 pp) of engine
12M ROI after `TFB_OPP_ROTATION_COST_PCT` (1.1%), one proposal per run, `TFB_OPP_ROTATION_EXCLUDE` 5023.SR,
`TFB_OPP_ROTATION_MIN_HELD_DAYS` 7, TP1 proximity 3%). The owner's example (new 3M plan 20% vs held -10%) clears it by
more than 30 pp. Item 2 existed as `TFB_OPP_CASH_FLOOR_SAR` but, as this build found, it did not bind the sizing budget
without a percent floor. Item 1 did not exist.

## What

### Conviction sizing: `TFB_OPP_SIZING_MODE = cap (DEFAULT) | conviction`

- `cap`: v1.26.2 byte-identical; none of the new code runs inside the seat loop; `meta.sizing == {"mode": "cap"}`.
- `conviction`, before the seat loop: the sizing cohort is the first `max_selected` candidates the loop would consider
  (rank order; replay eligibility; sector and market caps mirrored by `_sizing_cohort`). Each gets a conviction weight
  `_conviction_weight` = plan ROI % (`roi_pct`, the figure the Required ROI gate tests; a missing or non-positive value
  counts as the required minimum, never zero) x risk multiplier (Low 1.0, Medium 0.7, High 0.4, Unknown 0.7) x
  reliability factor (0.5 to 1.0), raised to `TFB_OPP_SIZING_TILT` (default 1.0 = proportional; 0 = equal split;
  up to 3; invalid -> 1.0). `_conviction_targets` water-fills the post-floor budget: targets proportional to weight,
  each at most the per-seat cap, the surplus of capped seats re-spread; a seat whose target would fall below the
  minimum ticket floor is released (lowest weight first) and the budget re-spread.
- In the loop a seat is sized as `_size_one(cand, criteria, budget_base, min(remaining, target))`, so every existing
  deferral (venue lot, minimum ticket, post-action sector weight, price cross-check) and the funding ledger apply
  unchanged. A released cohort seat is deferred with a countable reason
  (`Conviction sizing: weighted share below the minimum ticket floor - budget released to higher-conviction seats`).
- After the loop, the lot-rounding remainder tops up funded seats in conviction order, within the per-seat cap and the
  post-action sector cap, through the same `_size_one` / `_funds_from` calls. `meta.sizing.topup_sar` records it.
- `meta.sizing = {mode, tilt, cap_sar, budget_sar, cohort, weights, targets, topup_sar}`.
- The research stage is never sized under either mode; the allocate replay sizes only the eligible seats.
- Both env names are in the `TFB_OPP_` family, so the board-funding replay fingerprint binds them (S9): changing the
  mode between research and allocation invalidates the frozen snapshot, as every other policy read does.

### Absolute cash buffer binds the sizing budget

`_cash_floor_pct_ctx`: with `TFB_OPP_CASH_FLOOR_SAR > 0`, no percent floor, and an EXPLICIT
`TFB_OPP_CASH_FLOOR_MODE=enforce`, the context mode is `enforce`, so `_select_and_size` takes the reserve out of
`remaining` as it already did under the percent floor. v1.16.0 only kept the absolute reserve out of the funding labels;
`remaining` still sized over it. SAR set with the mode unset stays byte-identical (mode off, no alert, no meta;
S3b pins it). The `cash_floor` alert text names the absolute reserve instead of printing `None%`.

## Audits

| Battery | Result |
| --- | --- |
| `tests/test_ob_conviction_sizing.py` S1 default identity, S2 spread by conviction and cap, S2b tilt 0 equal, S3/S3b buffer binding and v1.16.0 identity, S4 water-filling, S5/S5b weights and tilt parsing, S6 cohort mirrors the loop, S7 top-up, S8 research never sized, S9 fingerprint family, S10 frozen replay | 13 passed |
| Builder neighbours: `test_ob_board_funding` (incl. the replay-fingerprint guard), `test_ob_research_diversify`, `test_opportunity_builder`, `test_top10_selector`, `test_venue_calendar_ext`, `test_verify_manifest_pins`, `test_learning_guards`, `test_executable_quote_conflicts`, `test_ob_portfolio_certification`, `test_sheet_payload_digest`, `test_ob_settle_unresolved`, `test_recent_fixes`, `test_ob_quote_evidence`, `test_ob_cash_floor_pct` | 485 passed |
| `tests/test_board_funding_route.py` (real route, fastapi) | see the PR CI receipt (local run recorded in the PR) |
| ruff E9/F63/F7/F82, py_compile | clean |

## Arming (owner; Render ENV, read per call, no restart beyond the env redeploy)

Already set on 19 Oct 12:00 Riyadh (this session, reversible): `TFB_OPP_CASH_FLOOR_SAR=3750` (USD 1,000 at the 3.75 peg),
`TFB_OPP_CASH_FLOOR_MODE=enforce`, `TFB_OPP_CASH_FLOOR_PCT=` (blank = off). On the deployed 1.26.2 the SAR reserve only
labels funding; it binds sizing from the first deploy that carries 1.27.0.

After merge and deploy:

1. `TFB_OPP_SIZING_MODE=conviction` (and optionally `TFB_OPP_SIZING_TILT=1.5` for a stronger tilt toward the best
   seats). Read-back: Top_10 B2 `meta.sizing` through the `/sheet-rows` payload, or the board tickets no longer equal.
2. Portfolio page control `PF: Target Cash %`: lower from 10 to about 2 so the two pages agree on the buffer
   (that control is a sheet input, not an env).
3. Rotation stays as configured; to make the sell-to-fund proposal trigger on a smaller edge, lower
   `TFB_OPP_ROTATION_EDGE_PP` (8 -> 5) after a week of observing the `rotation_proposal` alerts.

Rollback: unset `TFB_OPP_SIZING_MODE` (cap, byte-identical); unset `TFB_OPP_CASH_FLOOR_MODE` to return the absolute
reserve to label-only; or revert.

## Known limits

- Conviction weights use the plan ROI the gate already tests; they do not forecast beyond the engine's own figures.
- The cohort is a mirror of the loop's cheap pre-checks; a seat the loop later defers for a reason the mirror cannot see
  (venue lot, price cross-check) frees budget that the top-up redistributes to funded seats, not to a new seat.
- A per-seat cap of 15% with ten seats cannot spend more than 150% of NAV; the cap, not the weights, binds first on a
  small board. The owner may raise `TFB_OPP_MAX_WEIGHT_PCT` deliberately.
