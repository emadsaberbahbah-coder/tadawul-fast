# Commit Sheet — scoring v5.11.2 "P-129: EXPLICIT-PERCENT ROI DOUBLE-DIVISION CLOSED"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete) · **Register item:** **P-129** (external adjudication 09-11; re-proven on HEAD before building).

## Identity
| | |
|---|---|
| Destination | `core/scoring.py` |
| Base (pinned) | v5.11.1 · SHA256 `a0f559dbe8b59b5487a1…` — live-fetched, byte-identical to the session upload |
| Delivered | v5.11.2 · SHA256 `c85f80b054998ec69b080119b56c33651e393353…` · re-verify at HEAD after commit |
| Tests | `tests/test_scoring_roi_parse.py` (P1–P4, dual-tree, real module) |

## Root cause, re-proven on HEAD source
`_safe_float` explicitly strips a trailing `%` and divides once (`float(s[:-1])/100`). `_as_roi_fraction` then applied the bare-number points heuristic (`abs>1 → /100`) to that **already-divided** value — so any explicit percent string beyond 100% divided twice: `"200%" → 0.02`, `"▲ 150%" → 0.015`, `"-150%" → −0.015`. These reads feed `expected_roi_1m/3m/12m` at the forecast-derivation and gate sites (L1386–1407, 1698–1699) — threshold comparisons on such rows were distorted 100×. Bare numeric inputs (the engine path's floats: `250.0 → 2.5`, `34.0 → 0.34`, `0.105 → 0.105`) were and remain **correct**.

## What it does
In-place fix, zero functions added: when the **original input** is an explicit `%` string, the parser trusts the single division `_safe_float` performed and never re-divides. Applied to **two** functions under one kill switch: `_as_roi_fraction` (the adjudicated site) and `_as_upside_fraction` (found by this build's same-class sweep — `"300%"` was reading as 0.03 into the upside consumers). Bare-number heuristics are byte-untouched, and the known ≤1.0 bare-number ambiguity is **explicitly out of scope**.

**Default ON with kill switch** `TFB_SCORING_ROI_PARSE_LEGACY=1` (v5.11.1 byte-identical, proven P1) — the narrow-correctness precedent: the affected population is exactly explicit-% strings with |value| > 100%, provably parsed wrong; an observe mode has nothing to observe in a pure parser. Veto stays one env var away.

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` · AST (removed NONE, added NONE — in-place) · smart-quote scan | PASS |
| Same-class sweep across every `/100` heuristic in the file | **Found + fixed:** `_as_upside_fraction` (same defect, real unclamped consumer). **Reviewed + accepted unchanged:** `_as_pct_position_fraction` and the confidence read — both clamp into [0,1] and only misread inputs that are already nonsense (>150–200% position/confidence), so a change there buys nothing |
| **P1** kill switch: 14-case battery identical to v5.11.1, defect preserved (`"200%" → 0.02`) | PASS ×3 |
| **P2** default: `"200%"→2.0`, `"▲ 150%"→1.5`, `"-150%"→−1.5`; `"50%"`/`"75%"` and every bare numeric unchanged | PASS ×3 |
| **P3** defect reproduced on the base tree, closed on the revised tree | PASS ×3 |
| **P4** None / `"n/a"` / junk / bool edges identical both trees | PASS ×3 |
| **P5** upside: `"300%"→3.0`, `"▲ 260%"→2.6` (base 0.03/0.026); `"40%"` and bare 300.0/1.8 unchanged; kill restores base | PASS ×3 |
| Triple-run digest | `17406faea0012cb1` identical ×3 |

## Deploy
Commit the full file; re-verify SHA at HEAD. Behavior shift is confined to explicit-% strings beyond ±100% — engine-path floats are untouched, so expect **no visible board change** unless a sheet-fed row carries such a string; the fix is insurance at the decision core's input boundary. Rollback: `TFB_SCORING_ROI_PARSE_LEGACY=1` on Render or `git revert`.
https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/scoring.py

## Queue after this build
**P-125** (portfolio_actions `_QTY_KEYS` + `positionqty`) and **P-130** (investment_advisor TypeError retry label) remain the last two adjudicated-open items — both small, on your word. Then: the GAS Reformat file (percent display formats), the read-backs, and F-1/F-3/F-4/F-5.
