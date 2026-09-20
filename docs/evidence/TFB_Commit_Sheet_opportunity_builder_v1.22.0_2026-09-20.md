# TFB Commit Sheet — core/analysis/opportunity_builder.py v1.22.0 [CASH-FLOOR-PCT] (P-148)

**Date:** 2026-09-20 (Sunday, Riyadh) · **Item:** P-148 — the board's deployable ignores the portfolio's 10% cash floor (adjudicated 2026-09-19, re-confirmed on the 09-20 export and by the red-team review P1-05) · **Build #2 of 2026-09-20** on Emad's "done, let's move to the next" · **Engineer:** Claude · **Protocol:** One-Pass S0→S6.

## S0 / S1 — Freeze and pin

- HEAD verified before the build: `996e6bfa0f016f46b91ee0e5dd650862c87dfc40` (tree page `currentOid`); the three v1.21.0 files byte-identical at branch AND commit (opportunity_builder sha `9ad67f5f…`, test `63567ed3…`, sheet `47a99b07…`) — v1.21.0 commit leg CLOSED. Render deploy proof for 1.21.0 not received; superseded by this file (contains 1.21.0 verbatim) — one Manual Deploy + `/health` showing **1.22.0** proves both.
- Base = v1.21.0 at that commit, 6,048 lines, sha256 `9ad67f5fd2be2f2bd36a3e7303219d27e90ae2b6bbf601266dade48e4965c9d2` (zero drift vs delivery).

## S2 — Design (gate `TFB_OPP_CASH_FLOOR_PCT`, read per call, no restart)

| State | Behaviour |
|---|---|
| unset / blank / invalid | Byte-identical v1.21.0 (measured: off diff paths = 2, both the version string). |
| `TFB_OPP_CASH_FLOOR_PCT=10`, `TFB_OPP_CASH_FLOOR_MODE` unset (= observe) | floor_sar = max(absolute `TFB_OPP_CASH_FLOOR_SAR`, 10% × NAV), NAV = holdings value + cash (the PF page's basis; pending proceeds excluded). Selection, sizing, funding, KPIs untouched. ONE countable `cash_floor` alert: floor, deployable before → after, how many sized seats (and SAR) would lose funding; `meta.cash_floor`. |
| `TFB_OPP_CASH_FLOOR_MODE=enforce` | Reserve taken from cash BEFORE sizing: `cash_left`, the reported `deployable` AND the sizing budget `remaining` honour it (v1.16.0 only shrank the first two — the sizing gap that made the board oversize). Tail seats fall into the existing "Unfunded … capital exhausted" / min-ticket semantics; `funds_from` can never name the reserve. Deployable KPI = cash − floor. |

Deliberate cuts: `budget_base` (per-position cap denominator) unchanged — a reserve does not shrink NAV; the absolute-only path (pct unset) stays byte-identical; the PF panel's Target Cash % is not plumbed into the request — the pct is an ENV mirror of that panel until the cockpit sends it.

## S3 — Build (anchored edits, every replacement asserted `count == 1`)

| # | Site | Change |
|---|---|---|
| E1 | header | v1.22.0 WHY block + `OPPORTUNITY_BUILDER_VERSION = "1.22.0"` |
| E2 | before `_sector_cap_basis` | helpers `_env_cash_floor_pct`, `_env_cash_floor_mode`, `_cash_floor_pct_ctx`, `_cash_floor_finalize`, `_cash_floor_alert_text`; state `_LAST_CASH_FLOOR` |
| E3 | `_select_and_size` reserve block | context + stricter-of floor + `remaining` honours the reserve under enforce |
| E4 | `_select_and_size` return | `_cash_floor_finalize` (observe read-back count, displayed-rounded ticket basis — the file's own reproducibility contract) |
| E5 | alerts | `cash_floor` alert before the `price_xcheck` alert |
| E6 | meta | `meta["cash_floor"]` only when armed |

Delivered: **6,204 lines**, sha256 `529a08644db49dbc43d103f8a3dcf9f8e16cf7d67bef44cb9770522b7b7c907b`. `py_compile` PASS. AST defs 166 → 171 (**+5, 0 removed**). Smart quotes 0. Net-new non-ASCII outside comments 0.

## S4 — Internal audits ×3

**Repo harness** `tests/test_ob_cash_floor_pct.py` (241 lines, sha256 `8a197c480e2d1abe32caf036124471aa47488b4e92ee454ac3662c028dd97a68`), REAL module: T1 env readers · T2 off identity · T3 observe (selection/sizing/funding/KPIs/near-miss identical; alert + meta; the would-lose-funding count recomputed independently from the off tickets) · T4 enforce (deployable 30,000 → 21,000 on the fixture; Σ suggested ≤ post-floor; unallocated identity; funds_from ≤ post-floor cash) · T5 stricter-of (absolute 12,000 beats pct 9,000 and is disclosed) · T6 NAV basis (holdings sum; cash-only fallback disclosed) · T7 idempotence · T8 wiring. **ALL PASS ×3.**

`tests/test_ob_price_xcheck.py` re-run on this file: T8's exact version pin loosened to a `>= (1, 21, 0)` floor (sha now `fef262ce…`) — **ALL PASS ×3** (the v1.21.0 battery stays green on the v1.22.0 file).

**Dual-tree replay on the REAL 2026-09-20 export** (9,791 rows, frozen clock, live wallet `cash 24,763.73` + `holdings 68,957`):

| Run | Result |
|---|---|
| off: base v1.21.0 vs delivered | diff paths = 2 (`version`, `meta.versions.opportunity_builder`); identical otherwise |
| observe, pct 10 | NAV **93,721** → floor **9,372.07** → deployable **24,764 → 15,392** — the PF page's own `cash_floor=9372` / Deployable 15,392 reproduced to the riyal on the same wallet; selection + KPIs == off; alert "1 of 10 sized seat(s) would lose funding (9,371 SAR)" |
| enforce, pct 10 | deployable KPI 15,392; tickets 14,058 + 1,332 = 15,390; unallocated 2 |
| Digest (off/obs/enf) | `19a6ac6fc7ed7a95` **identical ×3** |

Harness discovery kept: the observe count must use the DISPLAYED (rounded) ticket size — unrounded internals gave 8,973 vs 8,972 recomputed from the sheet; fixed pre-delivery (same principle as `exp_gain`).

## S5 — Delivery (full files, convention paths)

- `core/analysis/opportunity_builder.py` (v1.22.0 — contains v1.21.0 verbatim)
- `tests/test_ob_cash_floor_pct.py` (new)
- `tests/test_ob_price_xcheck.py` (one assertion loosened, see S4)
- `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.22.0_2026-09-20.md` (this sheet)

## ENV (Emad applies; Render lane; ONE per evidence run)

| Var | Default | Purpose |
|---|---|---|
| `TFB_OPP_CASH_FLOOR_PCT` | unset (off) | **the arming**: `10` mirrors the PF panel |
| `TFB_OPP_CASH_FLOOR_MODE` | observe | `enforce` after a clean observe read-back |
| `TFB_OPP_CASH_FLOOR_SAR` | unset | existing absolute reserve (v1.16.0); stricter-of when both set |

Arming sequence proposed (one ENV per evidence run): (1) today — `TFB_T10_PRICE_XCHECK=observe` after the deploy proof; (2) next slot — `TFB_OPP_CASH_FLOOR_PCT=10` (observe by default); (3) enforce flips as separate sittings.

## S6 — Read-back

Positive read-back for this build on the first armed board: ALERTS shows one `cash_floor` row reading "Cash floor 10% of NAV ≈93,7xx SAR = ≈9,37x SAR (observe): deployable would fall 24,764 SAR -> 15,392 SAR; …", KPI Deployable unchanged (observe), no ticket changed; under enforce the KPI reads 15,392 = the PF page.

## Rollback

Unset `TFB_OPP_CASH_FLOOR_PCT` (no deploy) or `git revert`.
