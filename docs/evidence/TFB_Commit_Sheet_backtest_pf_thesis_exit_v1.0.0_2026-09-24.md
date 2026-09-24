# TFB Commit Sheet — scripts/backtest_pf_thesis_exit.py v1.0.0 + .github/workflows/thesis_exit_backtest.yml [P-169 EVIDENCE: THESIS-FAILURE EXIT REPLAY]

Date: 2026-09-24 (Riyadh) · Author: Claude (One-Pass Script Protocol) · Operator: Emad
Register: P-169 (exit-gate asymmetry / thesis-failure exit) — this is the EVIDENCE step; the live rule (portfolio_actions v1.13.0, `TFB_PF_THESIS_EXIT`) is a separate build that must not be enforced without this replay.

## 1. Delivered (S5) — two NEW files, no existing file touched, no Render deploy
| File | Repo path | SHA-256 (prefix) | Lines |
|---|---|---|---|
| backtest_pf_thesis_exit.py | scripts/backtest_pf_thesis_exit.py | `c9e680151b12996b…` | 639 |
| thesis_exit_backtest.yml | .github/workflows/thesis_exit_backtest.yml | `ac23a3d5d9fd846f…` | 67 (workflow_dispatch only; no schedule; `contents: read`) |
| this sheet | docs/evidence/TFB_Commit_Sheet_backtest_pf_thesis_exit_v1.0.0_2026-09-24.md | — | — |

## 2. Why (pinned 09-24)
- Entry gate: overall ≥ 68, reliability ≥ 70. Hold gate: none. On the 09-24 book SBAC 57.8 / YUM 59.3 / CWBC 63.9 all read HOLD; the only exits are broker stops (−7…−10%) and engine EXIT, and the 09-19 SBAC SELL / YUM REDUCE were capped to HOLD by the reliability-floor artifact (P-115b / F-7).
- 16 of the 31 closed lots were stop-outs; closed total return +963 SAR is carried by one +2,650 winner. "Exit sooner" must therefore be measured, not assumed.

## 3. What the script measures (READ-ONLY)
Rule: per lot from its Buy Date, walk the symbol's `Signal_History` snapshots (the tracker's one-per-symbol-per-day store: Overall Score, Price, Recommendation, Investability, Reliability); N consecutive snapshots with Overall < S → exit priced at the NEXT snapshot (no look-ahead); unscored snapshots neither break nor extend a streak; a streak completing on the last snapshot = "still held". Grid S ∈ {60, 65, 68} × N ∈ {3, 5, 7}; default S<60, N=5.
Per lot: coverage (snapshots in window / calendar days), actual price-only P&L (sell price for closed, current price for active) vs rule price-only P&L in SAR (ledger FX), Δ, whipsaw (price ≥ +3% above the exit within 10 later snapshots), and for closed lots the count of sub-S snapshots before the actual exit. Sukuk lots exempt. Dividends/fees/slippage are NOT modelled in either leg (the ledger's Total Return incl. dividends is shown beside them).
Output: markdown evidence (job summary) + JSON. Without `Signal_History` the verdict is printed as NOT DECIDABLE — never a silent zero.

Sources: `--export-dir` (browser TSVs: `_Portfolio_CostBasis` required; `Signal_History` required for the replay) or `--live` (gspread, read-only scope, the acceptance script's env names: `DEFAULT_SPREADSHEET_ID` + `GOOGLE_SHEETS_CREDENTIALS(_B64)` / `GOOGLE_APPLICATION_CREDENTIALS`).

## 4. Evidence (S4)
- `--selftest` **PASS 9/9 ×3**: streak completion priced at the next snapshot (no look-ahead); reset on a ≥ S day; unscored days ignored; "still held" when no later price exists; holding-window clipping (Buy/Sell dates); whipsaw detection + grid aggregation; **four real lot shapes from the 09-24 ledger verbatim** (YUM 24 @ 144.01, SBAC 21 @ 181.01, VEL stopped 09-16 @ 16.97, EPRT stopped 09-11 @ 28.955) with synthetic scores — SBAC fires on the 6th sub-60 snapshot at 175.5 → Δ +392 SAR vs the actual −828; YUM with no coverage → no evidence (Δ None), sukuk excluded; NOT-DECIDABLE path; ledger parsing of parenthesised negatives / em-dash blanks / the FX arrow header.
- Real 09-24 export run: ledger parsed 37 lots (6 active, 31 closed; 15 winners / 16 losers; closed +965 whole-SAR rounding; active price-only −1,660 SAR); `Signal_History` not in a browser export → **NOT DECIDABLE**, exit 0 — the expected outcome; the live replay is the dispatch below.
- py_compile PASS; smart quotes 0; 21 non-ASCII characters, all in docstring/markdown labels (→ — ▲ ▼); LF-only.

## 5. How to get the real evidence (one click)
GitHub → Actions → "🧪 TFB P-169 Thesis-Exit Backtest (read-only)" → Run workflow (defaults S=60, N=5; re-run with other values to read the grid). The job summary prints the markdown; the artifact `tfb-thesis-exit-backtest-<run_id>` holds the JSON. Read-back to record: lots with coverage, fired count, net Δ SAR, whipsaws, and the grid — these numbers decide whether portfolio_actions v1.13.0 is built with S=60/N=5, another cell, or not at all.

## 6. Limits (disclosed)
- Signal_History covers only the symbols the tracker snapshots; lots without coverage are reported as "no snapshots in window" and count as no evidence.
- Scores in Signal_History carry the F-7 pass-dependence (cold/warm) — the same swing that capped the 09-19 SELL/REDUCE; N consecutive snapshots is the mitigation, the settle pass (F-7 enforce) is the fix.
- Price-only comparison; no dividends, fees, FX drift or slippage in either leg.

## 7. Next
- Dispatch the workflow → paste the summary → adjudicate the grid.
- portfolio_actions v1.13.0 (P-169 rule, observe first) on that evidence — 09-25 build #1.
- F-8 regime overlay after the spec addendum.
