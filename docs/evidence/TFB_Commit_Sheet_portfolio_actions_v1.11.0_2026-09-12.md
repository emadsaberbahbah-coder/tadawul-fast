# Commit Sheet — portfolio_actions v1.11.0 "POST-MORTEM BUILD #1"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete; S6 observe cycle pending arming)
**Register items closed by this build (once armed):** F-2 (drawdown/time exit) · P-122 (fee-aware, exact-cost funding)

## Identity
| | |
|---|---|
| Destination | `core/analysis/portfolio_actions.py` |
| Base (pinned) | v1.10.0 · SHA256 `b3f16c2cdef401bb78dfdee9b82bb3819309ad31144ca6be5fd346f047e934d2` |
| Base pin method | **Live-fetched from GitHub main at build time** — byte-identical to the session upload (protocol S1 satisfied) |
| Delivered | v1.11.0 · SHA256 `24e627a274d036f873769d5846a6e8b149d480c7cee70c7a9f77039cff6437d1` · 3,106 lines (base 2,981) |
| Tests delivered | `test_pf_dd_fee_guards_local_dualtree.py` (dual-tree harness; see Test Notes) |

## What it does
**Feature A — [F-2] Drawdown/time guard.** `TFB_PF_DD_EXIT = off | observe | enforce` (default **off**).
New seam `_apply_drawdown_guard()` runs after `_apply_reduce_policy` in the per-entry pipeline (same seam pattern as v1.8.1 de-minimis; `decide_action` byte-identical).
- Trigger (a): unrealized return ≤ −`TFB_PF_DD_EXIT_PCT` (default **8.0**), basis `pnl_sar/cost_sar` (currency-safe).
- Trigger (b): return < 0 after > `TFB_PF_DD_TIME_D` days (default **45**), buy date pulled off the raw sheet row via new `_buy_date_from_row()` (tokens: buydate/buy_date/purchasedate/entrydate/dateacquired/datebought), stamped as internal `cand["dd_buy_date"]` — `normalize_candidate` carries no date, which is why the time leg needed this.
- `observe`: appends a visible `[dd-observe] … would EXIT under enforce` tag to `action_reason`; action/proceeds untouched.
- `enforce`: ADD/HOLD/TRIM → **EXIT**, proceeds = full market value (feeds the funding pass like any EXIT). Never overrides an existing EXIT/BLOCK. Fail-soft on missing basis or unparseable date.

**Feature B — [P-122] Fee-aware, exact-cost funding.** `TFB_PF_FEE_FUNDING = 1` (default **off**), `TFB_PF_FEE_SAR` (default **9.0**).
Armed: the per-ticket fee is subtracted from the fundable budget **before** sizing, and the cash ledger (`cash_left/proceeds_left/remaining/sector room`) is debited with the **exact** gross cost + fee instead of `round(...,0)`. Display KPI (`Adds Funded`/suggested) stays the rounded figure. Off path is verbatim v1.10.0 arithmetic (`_charge == suggested`).

## Edits (anchored, count==1 asserted, zero removals)
E1 header version line · E2 version constant + changelog WHY block · E3 helpers `_env_dd_guard_mode` + `_apply_drawdown_guard` · E4 seam call after reduce-policy · E5 funding-block gated rewrite · E6 `_buy_date_from_row` helper · E7 `dd_buy_date` stamp in `normalize_holding` · E8 guard reads `dd_buy_date` (engine `buy_date` fallback).

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` | PASS |
| AST zero-removal proof | PASS — removed: NONE; added: `_apply_drawdown_guard`, `_env_dd_guard_mode`, `_buy_date_from_row`; class set unchanged |
| Smart-quote / NBSP scan | CLEAN |
| **G1** off/off vs pinned base, real module, deep-equal (timestamp+version neutralized) | **PASS ×3** |
| **G2** observe is log-only: actions identical; tags on −10% row and negative-134-day row; none on winner | **PASS ×3** |
| **G3** enforce: dd row + time row → EXIT with proceeds = market value (3,375 SAR fixture); winner untouched | **PASS ×3** |
| **G4** fee boundary (the DDI class): deployable 755 SAR, price 375 → off 2 sh / on 1 sh; ledger charged exact+fee | **PASS ×3** |
| Triple-run digest | `9676dfdaa5eef549` — identical across all three runs |

## Test notes (honest scope)
- Harness executes the **REAL module** (real `normalize_holding`, real `fund_adds`, real guard). G4 forces only the **routing** of one fixture row to ADD (`decide_action` + add-confirmation + de-minimis bypassed for that row alone) so the untouched funding loop could be exercised at a controlled boundary; all funding arithmetic under test is production code.
- Fixture discoveries recorded for future harnesses: cash enters via controls key `cash_available_sar` (panel label "PF: Cash Available SAR"), the cash-floor basis is **holdings + cash**, and Advisory mode tops deployable up with TRIM/EXIT proceeds — G4 pins `rebalance_mode: "New Cash Only"` to hold the boundary exact.
- Not covered here: FastAPI route layer, GAS rendering of the new tag text, live Redis/env interplay. Deploy is behavior-identical until armed, so those are S6 observe-cycle items.

## Overlap note
External package F02 (adjudicated P-122) fixes the rounded debit but models **no fees** (their own limitation note). This build supersedes that file if both land: exact-cost **and** fee, env-gated. If their patch is integrated first, take this v1.11.0 full file over their `portfolio_actions.py` — the other 7 modules in their patch are unaffected.

## Deploy + arming plan (Emad executes; one ENV per evidence run)
1. **Commit** the full file to `core/analysis/portfolio_actions.py` on main (byte-identical paste; re-verify SHA `24e627a2…` at HEAD). Render auto-deploys; boot log should bind **v1.11.0**. Nothing is armed — G1 guarantees behavior-identical.
   https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/analysis/portfolio_actions.py
2. **Arming run 1 (observe):** Render → Environment → add `TFB_PF_DD_EXIT=observe`. https://dashboard.render.com
   *Positive read-back:* today's book has no position beyond −8% or negative past 45 days, so zero tags is the correct-but-weak signal. For a **positive** proof in one cycle, set `TFB_PF_DD_EXIT_PCT=2` alongside for that single refresh — YUM (≈ −2.7%) must carry `[dd-observe]` in the next Portfolio_Decision export — then restore the var to unset (default 8.0). Two vars in one run is acceptable here only because the second exists purely to make the first observable; call it out in the log line.
3. **Arming run 2 (separate evidence run):** `TFB_PF_FEE_FUNDING=1`. Read-back = the next funded ADD's ledger line: unallocated slack reflects exact cost + 9 SAR (no more 1-SAR fictions), and any boundary ticket shrinks vs the rounded sizing.
4. **Enforce decision** on `TFB_PF_DD_EXIT=enforce` only after ≥1 week of observe tags reviewed — same doctrine as the fund-unit sentry.

**Rollback:** `git revert` of the commit (single-file), or disarm by removing the env vars — off-state is proven byte-identical.

## Explicitly not in this build (queued)
P-101 root fix (fraction-scale ROI writer in `enriched_quote`) = Build #2 candidate · F-6 horizon contradiction · cockpit stale cash cell (P-134, GAS-side) · Copper-row schema quarantine · F-1/F-3/F-4/F-5 model-design items (decision pending on program direction).
