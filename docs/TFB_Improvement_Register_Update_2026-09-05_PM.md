# TFB Improvement Register — Update 2026-09-05 PM (Day 6, afternoon addendum)
Continues the morning update (P-61…P-74 provisional). New provisional IDs P-75…P-79. Evidence: Apps Script execution logs 14:31–15:33, board exports 14:36 / 14:54 / 15:03 / 15:10 / 15:33, Render deploys 14:28 / 15:09 Riyadh, shadow_scorer run #52 (dry-run).

## Closed today (evidenced live)
| Item | Evidence |
|---|---|
| **P-61** fast-track seat stays sizing-withheld until confirmed — **live fix is GAS `16_Decision_Top10.gs` v1.11.2** (the cockpit path), not `top10_selector` v4.31.0 (correct in its own path) | 14:33 "4 fast-track" → 14:36 "4 ft-carried", seats read `FAST-TRACK (day 1, 0/3 confirmed)`, Ticket/Shares `—`, output HELD; banner "0 EXECUTABLE TICKETS + 4 FAST-TRACK (SIZING SUSPENDED)" |
| **P-62** `_Selection_Log` logs output-state transitions (`OUT~<state>` token) | 15:03 `SelLog: +9`; 15:33 `SelLog: EMPTY_BOARD logged` |
| **P-76** layout builder keeps operator inputs (v1.11.3) | 14:52 `layout: preserved 27 operator input(s)` |
| **P-77** suspended seats disclose no funding (v1.11.3) | 14:54 board: Funds From `—` on both fast-track seats (was "Cash 19,544 SAR") |
| **P-71** dormant shape-guard test → real defect, fixed (scorer v1.7.2) | run #52: 19 non-symbols priced daily (meta rows of Shadow_Board swept into the EQW basket) = the 38 "price errors"; test now passes; selftest 87/87 |
| EODHD rescue arming (09-03) — **proven real** (scorer v1.7.1 read-back) | `gate=on token=yes`, 19 EODHD calls; 0 rescued because all 19 misses were junk tokens |
| Arming plan **Run 0** | `/health`: `engine_target_klg_redis: true`, `tgt_lkg_redis_state: idle`, master gate OFF |
| Arming plan **Run 1** `TFB_T10_VENUE_ALLOWLIST` | audit disallowed-venue rows 44 (08:08) → 0 (from 14:36); INVEST 92 → 57, all on allow-listed suffixes; venue floors visible in NEAR MISS (2222.SR 3,816 < 4,000; BYG.L 27,700 …). No "Eligibility (Venue)" first-fail rows because the gate is early and the audit window is depth-ordered (expectation corrected) |
| Stability state reset (operator, 15:31) | 15:33 board: `QUALIFIED_UNFUNDED`, 0 seats, 57 qualified, capital call 11,056 SAR — the truthful state at 3,825 SAR |
| Cash Available 100,000 → **3,825** restored; Require Investable Yes → **No** restored | 15:03 panel; Passed back to 57 |

## Open — new this afternoon
| ID | Item | Evidence / note | Phase |
|---|---|---|---|
| P-75 | Every `docs/` commit redeploys the Render web service (10:04Z, 11:28Z…); a restart mid-GM-leg cuts requests | Render Build Filters / Ignored Paths: `docs/**`, `**/*.md` (settings, not ENV) | ops |
| P-78 | Rotation-proposal text quotes the holding in native currency labelled SAR ("exit of PFS 119 SAR"; PFS = 445 SAR / $118.60) | 14:36 & 15:03 alerts; touches Run 2 (`TFB_OPP_FX_SANITY`) | 3 |
| **P-79** | **S-1 evidence clock is structurally stalled**: the shadow board publishes 0 eligible names most days (BROKER_UNTRADABLE / MODEL_SCREEN_FAIL) → CHALLENGER basket empty → fresh coverage 0/0 → every trading day excluded (labelled infra until v1.7.2's `reason=no-challenger`). EODHD cannot help. Decision: align the challenger's tradable universe with `TFB_T10_VENUE_ALLOWLIST` / `DERAYAH_MARKETS`, or re-scope criterion 1 | run #52 CHALLENGER `''`; `_Run_Log` SHADOW-BOARD `eligible=0` 09-01/03/04 | plan |
| P-80 (cosmetic) | `stab[strict]` note counts grace-held seats that still carry `ft=true` under "ft-carried" (e.g. 15:03 "9 grace, 3 ft-carried") — double label; exclude grace-held from the ft-carried count in v1.11.4 | 15:03 status line | GAS |
| P-81 (residual) | `OK` survives the shape guard (2-letter upper-case token from "risk: OK" carried in old history); leaves after the first clean EQW write | run #52 replay | scorer |

## Claude errors logged this afternoon
7. The live 03:40→07:46→08:08 defect was attributed to `top10_selector`; it lives in the GAS stability layer (`dt10StabCore_`). The Python fix stands for its own path; the cockpit fix is v1.11.2.
8. Run 1 read-back was predicted as `hard-exit …` + "Eligibility (Venue)" first-fail rows; the grace doctrine treats absence as jitter and the depth-ordered 500-row audit hides early-gate failures. The measurable fingerprint is the venue composition of the audit (44 → 0).
9. The 11:28Z Render deploy was called a docs commit; Render also redeploys on ENV changes — it most likely carried Run 1 (board composition changed by 14:36).

## Pending read-backs (operator)
1. `shadow_scorer.yml` dry-run on v1.7.2 → `[S1-SHAPE-GUARD v1.7.2] on dropped=7 …`, `[S1-PRICE-ERRS v1.7.2] n≤2`, `excluded_reason=no-challenger`.
2. `_Run_Log` row for `tfbMorningCockpitRefresh` (P-67) — evening export.
3. Run 2 (`TFB_OPP_FX_SANITY=1` + `TFB_FX_LOOKUP` USD cell 3.7528) → A2a/A2b.
4. Acceptance 06:45Z tomorrow: A1 PASS expected (no seat on an unmapped venue).

## Operator decisions pending (unchanged + one new)
HCI trim vs cap · EPRT stop · capital policy (deposit ≥ 11,056 SAR for RPC/AVGO/VTOL vs rotation-only) · Require Investable = Yes as a separate later arming (H-INV-ON) · **P-79 challenger scope**.
