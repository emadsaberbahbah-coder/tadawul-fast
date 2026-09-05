# TFB Improvement Register — Update 2026-09-05 (Day 6 of the 10-day program)
Provisional IDs continue from **P-61** (main's last update, 09-02, ends at P-60). Every number below is provisional until Emad confirms. Evidence cutoff: morning export (`_Status` ML 11:20:30 · GM 07:47:39 · MF 11:41:17 · CFX 11:22:45 +03; runs [33943210923](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/33943210923) / [33954820433](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/33954820433)) + IBKR live read + Render deploy log 13:04 Riyadh.

## Closed today (evidenced)
| Item | Evidence |
|---|---|
| `scripts/run_shadow_scorer.py` v1.7.1 — `[S1-PRICE-ERRS]` read-back line (stdout + S1_Gate 4th cell + `_Run_Log` JSON), log-only, no ENV | on main `98272fd87af01edc` byte-identical to delivery; selftest 72→78/78 ×3; lean CI 232 passed. **Dispatch `dry_run=true` read-back still pending** |
| `core/analysis/top10_selector.py` v4.31.0 [BC-6] — fast-track seats stay sizing-withheld until confirmed (**P-61**) + `tests/test_top10_selector.py` (+5 tests) | on main `98ea0c722283d8fe` / `f3d0f6d89d581423`; new tests fail 5/20 on v4.30.0, pass 20/20 ×3 on v4.31.0; replay of the 09-05 board reproduces 03:40→07:46; membership/state byte-identical minus additive `ft`; lean CI 237 passed. **Live cockpit read-back pending (before Run 1)** |
| Arming plan Run 0 (inert) — `TFB_ENGINE_TARGET_KLG_REDIS=1` | `/health` 13:04: `engine_target_klg_redis: true`, `tgt_lkg_redis_state: idle`, `engine_target_klg: false`, `env_combo: ok` → PASS |
| P-34 cure holding | JHG.US row blank (no graft), ID-FIREWALL selftest 9/9; write-survival delta 0/0/0/0 (ML/GM/CFX/MF) |
| Broker reconciliation | IBKR positions 6/6 exact (EPRT 100 · HCI 30 · OTIS 55 · PFS 5 · SBAC 21 · YUM 24); cash $1,015.97 vs 3,825 SAR (FX display only); SHG.US closed 09-03 @84.34, +$693 net |
| ChatGPT blind cross-check adjudicated | `docs/evidence/claims_chatgpt_2026-09-05.json` — 15 accepted, 5 rejected/modified, 2 partial, 1 hypothesis |

## Open — new today
| ID | Item | Evidence / note | Phase |
|---|---|---|---|
| P-61 | Stability: fast-track seat has no persisted memory → next run labels it ACTIVE (day 1) → sizing released on an unconfirmed seat | `_Selection_Log` AEFES.IS 03:40 FAST-TRACK / 07:46 ACTIVE / 08:08 EXECUTABLE 3,824 SAR, Confirm Days 3, ci 1. **Built v4.31.0, read-back pending** | 1 |
| P-62 | `_Selection_Log` dedup keys on membership only — output-state transitions (HELD→WITHHELD→EXECUTABLE) unlogged ("SelLog: unchanged") | 08:08 run vs 07:46 rows | GAS `16_Decision_Top10.gs` (paste needed) |
| P-63 (extends P-52) | Holdings symbols bare (YUM/SBAC/OTIS/PFS) take the degraded engine path (`quote_exchange_missing` → no target → synthetic); GM carries bare+`.US` twins YUM/OTIS/FCX with different forecasts (YUM 4.20% vs YUM.US 15.01%) | explains 3 of 4 MP↔GM forecast splits; HCI.US split (28.8%→7.55%) is the R-6 case. Fix engine-side; do **not** rename ledger keys before `21_Portfolio_Ledger.gs` review | 1 |
| P-64 (→ P-41) | Cash floor split: PD floor 9,001 → Deployable 0; Top_10 Deployable 3,825, ticket 3,824 | both boards 09-05; policy decision (Target Cash %) then one shared floor | 3 |
| P-65 | Top_10 sizing divides by the 2-dp display price, no fee / entry-ceiling buffer | 2,693 × 1.42065 = 3,825.81 > 3,825 cash; 3,850.99 at the 1.43 ceiling; max 2,674 sh | 3 |
| P-66 (→ P-48) | Required ROI 12% / Annualized 10% not enforced in the INVEST verdict (32 / 10 of 92 rows below); reason label "ranked below the Max Selected cut" on ranks 3–5, 9; ALL QUALIFIED 92 vs Passed 89 (3 holdings mislabelled) | 08:08 board audit | 3 |
| P-67 | Morning cockpit trigger (`tfbMorningCockpitRefresh`, 08:07) writes no `_Run_Log` row; only the 4-hourly wrapper logs | `_Status` 08:08 vs `_Run_Log` gap | GAS (paste needed) |
| P-68 (hypothesis) | When the xprovider supplies the price (`quote_current_price_missing`), prev-close/change stay on the stale EODHD bar → change fields mix providers | AEFES prev close 17.99 vs public 18.21 (+2.56% vs +1.32%) | 1 |
| P-69 | Ledger conventions: sell-side commission not loaded (SHG 84.34 gross, $2.29); HCI buy price 191.41 = fill, not broker average 191.49 (others match to 3 dp). SHG FX 3.8090 = the known `TFB_FX_LOOKUP` wrong USD row → Run 2 | `_Portfolio_CostBasis` + IBKR trades | ops |
| P-70 | Universe hygiene: CFX list row for copper shifted one column (symbol "Copper Futures", stale since 08-13, HTTP 422); 114 GM dead symbols (no price/name, all BLOCKED, masked by `fresh_cov=100%`); MUAA.US (matured 2012); 18 CFX crypto/exotic FX unpriced; taxonomy: 294 CFX rows and all 2,474 MF rows "Equity" | pages 09-05 | 1 |
| P-71 | `tests/test_shadow_scorer_shape_guard.py` fails against main's own scorer (72 checks, no SG markers; expects ≥80 + 8 SG); no workflow runs it (dormant) | repo main | housekeeping |
| P-72 | IRM buy limit 120 GTC absent from IBKR live orders while IRM = 116.86; all 10 conditional orders read status REPLACED | IBKR read 09-05 | operator |
| P-73 | EPRT broker stop 28.95 = 52W low, 2.3% below price (system stop 27.28); HCI stop 179.00 4.6% below | IBKR orders | operator |
| P-74 | Data Quality ignores bar age (2210.SR bar 04-29, DQ 100; blocked by a separate path); no Price-As-Of column in the 115-col schema; 118 rows `price_bar_stale` | pages 09-05 | W |

## Adjudicated external claims (ChatGPT Morning Review 09-05) — see the JSON
Accepted (verified exactly): source→decision override via `Require Investable=No` (PD applies §4.7, T10 does not); stability bypass (mechanism now proven, P-61); cash invariant (P-41); ticket math (P-65); 92/89/80 (P-66); Unknown news 440/500, sector 500/500; INVESTABLE 58 = 27 synthetic, 14 GM rel<50 (min 37.9); copper row field-shift; taxonomy; 118 stale bars; 08:08 run unlogged (P-67); mixed timestamp formats; YUM/YUM.US (P-63).
Rejected: "unexplained cash moves" (all four tie to ledger trades within 1–19 SAR); "buy fees 0/33" (commission-loaded prices, proven from broker fills); "percent-unit ×100 risk" (export rendering, builder reads numeric); "no outcome feedback" (Performance_Log); reliability-floor gate (contradicts H-28). Partial: MF clustering (Quality 96.2% ✓, DQ 53.6% not 98.7%, Rel 48.4% not 88.9%); sell fees (accepted for sells only). Not adopted: readiness 4.7/10 (reviewer rubric).

## Pending read-backs (operator) — in this order
1. `shadow_scorer.yml` → `dry_run=true` → paste `[S1-PRICE-ERRS v1.7.1]`.
2. After the post-commit Render deploy: one manual Top_10 cockpit refresh → AEFES.IS Stability = "FAST-TRACK (day 1, 1/3 confirmed)", output not EXECUTABLE for it.
3. Run 1 `TFB_T10_VENUE_ALLOWLIST=US,SR,T,HK,L,PA,AS,BR,DE,MI,MC,LS,VI,SW,TO,AX,OL,SI,MX` → A1 PASS.
4. Run 2 `TFB_OPP_FX_SANITY=1` + `TFB_FX_LOOKUP` USD cell → A2a/A2b PASS.
5. `docs/evidence/TFB_Commit_Sheet_run_shadow_scorer_v1.7.1_2026-09-05.md` still not on main (optional).

## Claude errors logged today
1. Morning verdict "no ENV arming today" issued without reading `docs/evidence/TFB_Arming_Plan_2026-09-05.md` already on main — the plan (Runs 1–2 today) stands.
2. "R-6 needs building" — already built (engine v5.137.0 `TFB_ENGINE_TARGET_KLG`); Run 4 arms it.
3. Proposed numbering P-50…P-59 collided with the register (main at P-60); corrected to P-61+.
4. SHG FX 3.8090 presented as new — already documented in the builder (wrong `TFB_FX_LOOKUP` USD row) and scheduled for Run 2.
5. P-45 explanation first attributed all four MP↔GM forecast splits to route flapping; three are deterministic bare-symbol spelling (ChatGPT's YUM/YUM.US finding) — corrected.
6. Stability defect first placed in GAS/builder; it lives in `top10_selector.py` — corrected before the build.

## Operator decisions pending
HCI 23.5% > 20% cap: trim ≈5 sh or raise the cap · EPRT stop 28.95 vs system 27.28 · AVGO rotation not actionable (capped 35% target, intrinsic −10.9%, R/R 2.01, Rel 70.4) · capital policy (deposit 7,083 SAR vs rotation-only) · confirm P-61…P-74 numbering · Require Investable stays No until P-61/P-66 land (flipping it now empties the board and hides the defect).
