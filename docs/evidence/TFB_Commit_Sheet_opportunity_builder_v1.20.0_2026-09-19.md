# TFB Commit Sheet — core/analysis/opportunity_builder.py v1.20.0 — 2026-09-19
## F-1b: cockpit qualification on the plan-3M basis (gate TFB_FORECAST_BASIS legacy|observe|plan3m) + P-141 ticket-gain basis

## Base pin (S1)
- Repo cloned at HEAD `f27b2da6f15e8e9146ef25e3ba2088d1e1ad15e3` (2026-09-19 13:44 Riyadh, #518). `core/analysis/opportunity_builder.py` sha256 `ea1c7c840510d8fe…` = the 2026-09-12 v1.19.6 delivery, **zero drift**; 5,516 lines (wc -l).
- Existing battery `tests/test_opportunity_builder.py`: 27/27 PASS on base. `tests/test_ob_ann_roi_annualized.py` errors at collection on base AND delivered (it expects a pristine v1.19.5 copy on disk — a build-time dual-tree harness, pre-existing, not a regression).

## Delivered
- `core/analysis/opportunity_builder.py` sha256 `5f14a9be07e8e3de9c9739ec6f50a0af9578e971f570e43a8e76710bf317b0cb`, 5,666 lines (+150).
- 5 anchored edits, each asserted count==1: E1 WHY block + `OPPORTUNITY_BUILDER_VERSION` 1.20.0; E2 three helpers (`_f1b_basis`, `_f1b_required_roi_3m`, `_f1b_plan_eval`); E3 `evaluate_gates` ROI / Annualized ROI plan3m branch (legacy branch carried verbatim); E4 `_build_ticket` gain seam; E5 observe post-pass in `_build` after selection/deferrals, before kpis.
- AST functions 147 → 150 (+3, **0 removed**); py_compile PASS; 162 added lines, 0 smart quotes; the 13 non-ASCII characters are em dashes in comments/docstrings, the file's existing convention (305 base lines use it).
- `tests/test_ob_f1b_plan_basis.py` (NEW, 228 lines) sha256 `08adf8faec5e1390bead061a069e41ad40462ea257a205910f61e3a10288f456`.
- Version note: the parked P-108 "Truthful Headline" spec had reserved v1.20.0; on un-park it re-numbers to the next free version.

## Root (F-1 memo 2026-09-14; second specimen on the 2026-09-19 board)
- `evaluate_gates` judged the ROI gate on the 12M VALUATION upside (`roi_pct = ref/price − 1`) and the Annualized ROI gate on that upside compounded as if it were a 3-month return, while the board renders the TP1 execution plan (`_audit_align_plan_roi`, live "plan" basis). The TP1 ladder is the midpoint to the reference (`tp1 = price + 0.5*(ref − price)`), so the plan ROI is exactly half the valuation upside — the "18 of 27 INVEST rows below the panel's 12%" reading is structural, not noise.
- Ticket gain = `suggested × ann/100` with ann = plan ROI compounded over the period (the v1.0.23 reproducibility identity) — the P-141 inflation (TSM 13,976 × 82.2% = 11,488 vs payoff 2,263; today 19,474 on two suspended seats).

## Fix (three modes, read per call, no restart; env shared with F-1a)
- **legacy** (unset/anything else): byte-identical to v1.19.6 — proven on the full export (H1).
- **observe**: gates/verdicts/selection/deferrals/kpis untouched; ONE `[f1b-observe] plan3m X% vs Y%/3M` tag per audit row appended to `failure_reason` (blank on INVEST rows before), `- FLIP` when the ROI+Ann outcome would change under the translated floor, `- FLIP(strict)` under the strict 12%/period reading, `plan3m DATA_GAP` when no ladder; each ticket note gains `[f1b-observe] gain ann-basis X vs plan payoff Y`.
- **plan3m**: ROI gate = TP1 plan ROI ≥ `_f1b_required_roi_3m` (default `required_roi_pct × months/12` = **3.0** — the F-1a translation; `TFB_T10_REQ_ROI_3M_PCT` overrides, e.g. `12` = strict); Annualized ROI gate = `_ann_from_plan_roi(plan)` ≥ `required_ann_roi_pct`; both required strings stamped `(plan3m)`; ticket `exp_gain_12m_sar = suggested × plan ROI/100` (TP1 payoff; the v1.0.23 gain≡ann identity is deliberately broken in this mode, `ann_roi_pct` display unchanged); kpis.expected_gain = Σ ticket gains automatically. No ladder → legacy gates evaluate (never invents a plan).
- Deliberate cuts: engine `roi_3m` fallback (the F-1b feeder) not plumbed — every row with a valuation reference has a ladder, rows without one fail the Valuation MAJOR gate anyway; selection order, sector caps, funding, rotation untouched in every mode; the "Exp. Gain 12M" column LABEL is GAS-side (unchanged).

## Evidence
- Frozen-clock dual-tree harness (both trees in one process, wall clock pinned, inputs = the full 2026-09-19 export: 9,791 rows, panel-exact criteria, the 6 live holdings, FX table derived from the board's own price/price-SAR pairs; repo defaults, no Render env — local INVEST set 70 vs 27 live): **H1** legacy A vs B **0 diffs** (only the two version strings differ); base ignores the env in every mode; **H2** observe ×3 identical digest `e6b0975601a090c1`, diffs vs legacy confined to `candidates_rows[].failure_reason` (9,791 tags) + `selected[].advisor_note` (4 tickets); gates/verdicts/selection/deferral/near-miss/alerts/kpis byte-identical; tag anatomy: FLIP(translated) 629 (583 first-fail ROI + 46 FX), FLIP(strict) 1,249, DATA_GAP 4,626 (no-ladder rows, MF/CFX-heavy); **H3** plan3m ×3 identical digest `930aa468878d4ebe`: translated floor → INVEST 70 → 70 (0 verdict changes — the R/R ≥ 2 gate, computed on the unchanged valuation ROI, already dominates); strict 12 → INVEST 70 → 37 (33 INVEST→WATCH); gate strings render `16.5 >= 3% (plan3m)` / `84.1 >= 10% (plan3m)` beside legacy `33.1 >= 12%` / `213.2 >= 10%` on the same row; **H4** gains: legacy/observe 25,827 SAR (12,341 / 13,361 / 94 / 31) → plan3m **4,823** (2,401 / 2,400 / 16 / 6), identity gain = suggested × plan ROI/100 holds on every ticket; **H5** selection order identical plan3m vs legacy. Harness digest `d82defae1179a6e9`.
- Repo battery `tests/test_ob_f1b_plan_basis.py` T1–T7 **PASS ×3, identical digest `cc2c55ca3ff4aa5b`** (synthetic CRC-class / low-upside / no-target rows through the real `normalize_candidate`, `evaluate_gates` and `build_opportunity_payload`); golden-negative proven — the same file fails at T1 against base v1.19.6.
- `tests/test_opportunity_builder.py` 27/27 PASS on the delivered file.

## Gate / env — READ-BACK RIDES THE EXISTING ARMING
- `TFB_FORECAST_BASIS=observe` is already live on Render (F-1a, 09-16). **No ENV change**: this deploy activates the observe tags on the next Top_10 build. Read-back = the CANDIDATES FULL AUDIT "Failure Reason" column carries `[f1b-observe] …` on every row (500 rendered), with countable `- FLIP` / `- FLIP(strict)` / `DATA_GAP`; any ticket note carries `gain ann-basis X vs plan payoff Y`. Zero change to seats, verdicts, sizing, KPIs.
- **plan3m flip = a separate sitting** after ≥1 observe read-back, with ONE decision left open on purpose: translated floor (3.0%/3M, F-1a-consistent; on today's snapshot it changes no verdict) vs strict (12%/3M; would cut the local INVEST set 70→37) — the FLIP(strict) count on the live board is the evidence for that call. The flip also switches the ticket gain to the TP1 payoff (P-141) — expect "Exp. Gain 12M" to fall ~4–5× on the same tickets.
- Rollback: env unset (no deploy) or `git revert`.

## Deploy
- Destinations: `core/analysis/opportunity_builder.py`, `tests/test_ob_f1b_plan_basis.py`, `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.20.0_2026-09-19.md`.
- Render auto-deploy OFF — Manual Deploy; verify `/health` (route stamp `builder v1.20.0` on the next cockpit status line, `[advanced_analysis …] opportunity builder bound (… v1.20.0)` in the boot log).

## Residuals / vNEXT
- F-1b feeder (engine `roi_3m` plumbed as `engine_roi_3m_pct` for a TP1-less plan) — moot until a row with a reference lacks a ladder.
- F-1c board fallback horizon (S-1 lane) — separate sitting, byte-clean here.
- R/R gate basis (`rr = roi_pct/stop_pct` on the VALUATION ROI) is why the translated floor changes nothing; if plan3m is adopted, the R/R gate's basis is the next coherence question (register as F-1d).
- The "Exp. Gain 12M" header label is GAS-side; under plan3m the number is a plan-horizon payoff.
