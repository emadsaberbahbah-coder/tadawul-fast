# TFB Commit Sheet — core/analysis/portfolio_actions.py v1.12.0
**Build:** F-1a — SINGLE FORECAST BASIS (ADD leg) · **Date:** 2026-09-14 · **Session:** Monday AM, build #1

## 1. Decision provenance
- F-1 opened on Emad's GO ("go F-1"); decision memo delivered same session with measured evidence (four ROI bases live on the 09-14 export; CWBC 3.4%-vs-13.1% discriminator; 2222.SR cockpit discriminator; realized holding median 10.5 days; calibration only at 1W/2W).
- Operator pick: **"go with the best option" → Option B, translated threshold** (Claude's stated recommendation in the memo: plan-3M becomes the single decision basis; PF add threshold translated 12%-annual → 3.0%/3M).
- F-1a scope = THIS module's ADD-qualification ROI leg only. Deliberate cuts, each a separate operator decision: EXIT/TRIM valuation thresholds, VF-conflict guard, dd guard (loss-control legs); cockpit qualification basis (**F-1b**, opportunity_builder); engine-roi3m fallback (**F-1b feeder** — expected_roi_3m is not plumbed into this module; a TP1-less holding fails CLOSED with a disclosed DATA_GAP, never a stand-in number); board fallback horizon (**F-1c**, run_shadow_board — untouched to keep the S-1 lane byte-clean).

## 2. Identity
| | |
|---|---|
| Base (live-pinned) | core/analysis/portfolio_actions.py **v1.11.1**, sha256 `54a2f12f5f670f1c9cb0bae6284a2726687396feab707f72620feb87e196996a`, 3,125 lines — fetched from HEAD `abce739c788bd8b6f8b798986216d980adb4ff67` (byte-identical to the 09-12 delivered sha, no drift) |
| Delivered | **v1.12.0**, sha256 `a764aeb8a2095ef9fdbc69b896919cda4128a4031ad54609cb364ae63c358086`, 3,250 lines |
| Dependency (harness) | opportunity_builder **v1.19.6**, sha256 `ea1c7c84…` — live-fetched, matches the landed sha |

## 3. Gate & controls
- `TFB_FORECAST_BASIS` = **legacy** (default, v1.11.1 byte-identical) | **observe** (decisions unchanged; one countable `[f1-observe] plan 3M ROI …` tag per holding via the post-decision seam; ` - FLIP` strictly marks ADD-ROI-leg basis disagreement; positive read-back even on a zero-flip book) | **plan3m** (the ADD ROI leg runs `(TP1/price − 1)·100` vs `add_roi_3m_pct`).
- New control `add_roi_3m_pct` default **3.0** (= (1.12^0.25 − 1)·100), env `TFB_PF_ADD_ROI_3M_PCT`, float-coerced, meta-echoed.
- Env read at call time (fund-unit-sentry seam) — no boot proof needed; **rollback = unset the env** (or set `legacy`).

## 4. Edits (9 anchored, count==1 asserted)
E1 header WHY-block + `PORTFOLIO_ACTIONS_VERSION = "1.12.0"` · E3 DEFAULT_CONTROLS `add_roi_3m_pct` · E4 `_CONTROLS_FLOAT` tuple · E5 `_env_overrides` entry · E6 three helpers beside the dd-mode seam (`_env_forecast_basis`, `_f1_plan_roi_pct`, `_apply_f1_observe_tag`) · E7 step-5 `add_ok` ROI-leg switch · E8 plan3m ADD message branch · E9 step-6 binding-fact ladder head (plan-basis wording + `[f1:DATA_GAP]`) · E10 post-decision observe seam beside `_apply_drawdown_guard`.

## 5. Audit evidence
- `py_compile` PASS (base + delivered). AST functions **84 → 87**: added exactly the 3 helpers, **removed 0**. Smart-quote scan clean.
- Dual-tree REAL-module harness **F1–F5 PASS ×3, identical digest `42f642217208516e`** (tests/test_pf_f1_plan_basis_dualtree.py, sha `f3d6cb8272ae33ff`):
  - **F1** legacy identity — unit tuples base==new; integration on the REAL 7-row 09-14 My_Portfolio export under the LIVE panel controls **reproduces today's 07:15 run**: 7×HOLD, live-verbatim reason heads ("Upside 3.4% below add threshold 12.0%", "Upside 2.5% …", "PRECEDENCE (§4.7): engine verdict WATCHLIST"), exactly 2 "pending confirmation" (CARE+DDI); canon-identical base↔new on both the panel path and the defaults path.
  - **F2** observe on the real book: **7 tags / 0 FLIP / 0 DATA_GAP**, one tag per row, decisions canon-identical to legacy.
  - **F3** golden-negative flip pair: legacy-pass/plan-fail (roi 13, plan 2.0) → HOLD `[f1:plan3m]`; legacy-fail/plan-pass (roi 8, plan 5.0) → ADD "Plan 3M ROI 5.0% >= 3.0%"; both directions proven against the env-clean control run.
  - **F4** TP1-less holding fails CLOSED (`[f1:DATA_GAP]`); observe discloses the gap.
  - **F5** `TFB_PF_ADD_ROI_3M_PCT=7` honored in controls and in the verdict text.
- **Harness discoveries worth keeping** (adjudicated, no module defect):
  1. Each holding's reason renders at TWO payload sites (`action_reason` + embedded in `advisor_note`) — raw string counts double; count per action row.
  2. `controls=None` defaults (max_position 15, cash 0 → holdings-only weights) render TRIM-to-cap on today's real book; the live panel (20% / cash 23,242.50) renders 7×HOLD — integration goldens must carry the panel.
  3. The module's plan level `tp1` is the OB-normalized derivation (≈0.91× the 12M target at this snapshot), a few tenths off the page-rendered TP1 SAR from the 07:15 run (price-snapshot timing) — same contract, evidence note only.

## 6. Destinations
- `core/analysis/portfolio_actions.py` (full file, replaces v1.11.1)
- `tests/test_pf_f1_plan_basis_dualtree.py`
- `docs/evidence/TFB_Commit_Sheet_portfolio_actions_v1.12.0_2026-09-14.md`

## 7. Arming & read-back (sequenced — NOT today)
Today's Render slot is consumed (`TFB_EQ_ROI_UNIT_SENTRY=observe`). Earliest F-1a arming = **tomorrow's slot**: `TFB_FORECAST_BASIS=observe` → positive read-back = **7 `[f1-observe]` tags** in the next Portfolio_Decision run's Advisor Notes (FLIP expected 0 on the current book). `plan3m` flip is a separate later sitting after observe evidence. Commit itself is behavior-identical (default legacy, harness F1 proof).
