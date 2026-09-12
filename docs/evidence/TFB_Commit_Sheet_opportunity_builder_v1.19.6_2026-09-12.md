# Commit Sheet — opportunity_builder v1.19.6 "P-127: THE AUDIT GRID'S ANN ROI IS NOW ANNUALIZED"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete) · **Register item:** **P-127** (external F07, adjudicated 09-11) — plus the HEAD audit that found it still open.

## HEAD audit that triggered this build (recorded)
The adjudicated external patch was **never integrated**: at HEAD, P-125's `_QTY_KEYS` still lacks `positionqty`, P-129's `_as_roi_fraction` still double-divides explicit large percents, P-130's twin `except TypeError` sites stand, and P-127 is live on the cockpit daily. P-127 was built first because the operator reads it every morning; **P-125 / P-129 / P-130 remain open** — small, latent, and buildable on request (or via integrating the external patch for those three files).

## Identity
| | |
|---|---|
| Destination | `core/analysis/opportunity_builder.py` |
| Base (pinned) | v1.19.5 · SHA256 `a1c343ca79c29db0b150…` — live-fetched, byte-identical to the upload |
| Delivered | v1.19.6 · SHA256 `ea1c7c840510d8fed4eb65459e556b4b0ecb5bbc…` · re-verify at HEAD after commit |
| Tests | `tests/test_ob_ann_roi_annualized.py` (O1–O4, dual-tree, real module) |

## Root cause, on source
`_audit_align_plan_roi` (v1.14.0 ROI-TRUTH-2, the **live default** since `primary_roi_basis == "plan"`) set `rec["ann_roi_pct"] = _p` — a raw copy of the TP1 plan ROI — while the ticket path 750 lines later annualizes with `(1+roi/100)^(365/days)−1` over `period_months × DAYS_PER_MONTH`. One grid, two formulas: the 09-12 board rendered `Ann ROI == ROI(TP1)` on every qualified row (11.0/11.0, 16.2/16.2, 13.8/13.8) while KRP's sized seat showed the true 67.7%. The operator was reading a 3-month figure in an "Ann" column.

## What it does
New pure `_ann_from_plan_roi(plan_roi_pct, crit)` — the ticket path's exact compound formula on the exact same horizon source — now feeds the align step's `ann_roi_pct`. `roi_pct`, gates, verdict, score, selection: **byte-untouched** (display truth only, the doctrine this block already carries). None-TP1 rows keep `None` + the `TP1_UNAVAILABLE(DATA_GAP)` note; fail-soft returns the input on any fault so a wrong-but-labelled number never replaces a blank.

**Default ON with kill switch** `TFB_OPP_ANN_LEGACY_COPY=1` (restores the v1.19.5 copy byte-for-byte, proven O1) — the file's own ROI-TRUTH precedent ships display-truth corrections as the live default with a kill; stated here so you can veto.

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` · AST zero-removal (added `_ann_from_plan_roi` only) · smart-quote scan | PASS |
| **O1** kill switch = v1.19.5 copy byte-identical (`ann == roi == 13.8`) | PASS ×3 |
| **O2** the live KRP case: 13.8% plan → **67.7%** annualized via the ticket-path formula over 91 d; the base tree still copies 13.8 (defect reproduced) | PASS ×3 |
| **O3** no TP1 ladder → `None/None` + DATA_GAP note preserved | PASS ×3 |
| **O4** non-plan basis inert; helper edges (None / −100 / 0) fail-soft | PASS ×3 |
| Triple-run digest | `9352cedf2b87efe2` identical ×3 |

## Deploy
Commit the full file; re-verify SHA at HEAD. **Read-back on the next Top_10 build:** the ALL QUALIFIED grid's `Ann ROI %` stops equalling `ROI %(TP1)` and instead shows the compounded figure (13.8 → ~67.7 class), matching the FEED table's formula — the two-formula display closes. Rollback: `TFB_OPP_ANN_LEGACY_COPY=1` on Render or `git revert`.
https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/analysis/opportunity_builder.py

## Queue after this build
Open-and-buildable on your word: **P-125** (qty alias), **P-129** (double-division), **P-130** (resolver retry label) — or integrate the external patch for those three. GAS: the Reformat-owning file for the percent-format extension. Everything else: read-backs + the F-1/F-3/F-4/F-5 decisions.
