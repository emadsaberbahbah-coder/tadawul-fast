# TFB Improvement Register — Update 2026-09-09 (Day 10)
Provisional IDs P-110…P-111 (renumber on merge if the Drive register has advanced past P-109). Evidence: 20-file workbook export 09:03–09:05 Riyadh, red-team audit PDF (repo pin `20b2f4d`), CostBasis refinalize exports 09:5x + 10:01:24, GitHub main SHAs, Render deploy 08:54:01Z (11:54 Riyadh), harness runs ×3 (py3.12.3) + parity (py3.11.15).

## Closed today (evidenced live)
| Item | Evidence |
|---|---|
| **Ledger fee finalization** — HCI sell corrected 186.27→186.35 + fee 2.29 → realized **(579)**; PFS fee 2.29 → **(20)**; both ⓘ notes cleared; DDI entry corrected to broker fills 12.72 + 5.25 fees (cost 2,918.13); SHG +2,650 confirmed net | 10:01:24 CostBasis export; read-back matched predictions to the SAR; closed book re-summed **+2,395 SAR** over 28 trades |
| **P-111 → downgraded to note** (SelLog "+5 gap") — all 6 writes present on the live tab (5 selections 09:04:53 incl. HCI/VEL fast-track + GLNG `EXIT:hard` 09:04:55; 1,198→1,204). Export raced the write. **Practice rule: pull workbook exports ≥2 min after a decision run** | fresh `_Selection_Log` export, same morning |
| Red-team "health-check path empty" **contradicted on the repo side** — `render.yaml` L25 declares `healthCheckPath: /readyz` | repo pin; live-field confirmation still in Pending read-backs |
| **Three-way exact agreement** on portfolio arithmetic (75,140.35 / +334.84 / 826.96 / 80 outcomes) across ledger, panel, and independent re-execution | strongest ledger-integrity evidence to date |

## Open — new / updated today
| ID | Item | Evidence / note | Phase |
|---|---|---|---|
| **P-110** | **Cross-event-loop provider lifecycle (EODHD)** — module-global singleton held loop-bound asyncio primitives (sem, locks, single-flight futures, httpx client); sync entry points (`top10_selector.build_top10_rows` → `asyncio.run`) create a new loop per call → `RuntimeError: … bound to a different event loop` + `Future exception was never retrieved`. Defect surface **wider than red-team reported**: `_INSTANCE_LOCK` and `_HEALTH_LOCK` were themselves loop-bound. **Impact includes silent degradation**: on 2nd+ loops, cross-loop errors inside secondary single-flight fetches degrade into `last_error_class=RuntimeError` error patches (the word "loop" never reaches the sheet) — quiet enriched-quote data loss, not just log noise. **FIX SHIPPED v4.18.0** (loop-aware singleton + thread-lock health guard + single-flight exception observance): main SHA `efbd7c14…`, deployed 08:54:01Z, clean boot py3.11.9, zero ENV, rollback = `git revert`. Evidence: golden-negative reproduced the exact production stack (L2243 sem via `_sf.do` L1966); fixed build 3 loops × 32 tasks, 0 loop errors, rebuilds=2 exact, health counters continuous, T02=0; harness ×3 byte-identical + py3.11 parity PASS. Artifacts: `docs/evidence/TFB_Commit_Sheet_eodhd_provider_v4.18.0_2026-09-09.md`, `tests/test_harness_loopguard.py`. Before-exhibit: Render record `f000f42a-4dfc-4c95-8b11-b3ebdf3a495d` (05:13:13Z). **Status: DEPLOYED-OBSERVING.** Closure = `EODHDClient v4.18.0 initialized` in logs + 0 hits of both signatures across one scheduler tick + one manual run | observe |
| **P-102 v2** | Forecast/ROI reconciliation **refined**: 174 rows (ML 64 / GM 109 / CFX 1 / MF 0) under the rounding + dual-encoding identity; 4 carry INVEST: 1150.SR, 2222.SR, 2286.SR, PHP.L. Spot re-executions: SISE.IS implied 26.89% vs displayed 48.34%; ISCTR.IS 0.84% vs 11.21%; 1150.SR 26.87% vs 26.27%. Supersedes the earlier row count; **not double-counted with P-101** | 3 |
| P-101 (residual) | 138 unformatted fraction-scale ROI cells remain post-reformat (GM 93 / CFX 29 / MF 16; samples AREN.US 0.3, DCBG.US 0.025222 — mostly dead/stripped rows). Read-back NOT bare=0 | GAS |
| P-99 (still open) | My_Portfolio 5023.SR Buy Date `2025-01-10` vs ledger truth `2025-11-23` (cost/P&L correct; date cell not carried by sync). Designated operator fix, 1 cell | sheet |
| (fixture) | Copper Futures junk row pinned as **T07**: px 6.60 / prev 17.07 / +9,530% / fc12 0.01 / stamp 2026-08-13; final action DO_NOT_INVEST correct but raw Recommendation col = BUY | test |
| (unnumbered IR) | PR-28 preview-service ownership · cash-snapshot time-field hygiene · Render Health Check Path live field (see read-backs) | ops |

## S-1 status (unchanged, structural)
Still **3/28 scored days**; latest scored night excluded `reason=no-challenger` (chal fresh 0/0; shadow authority as_of 2026-03-31); criterion 6 rollback drill PENDING on sheet. Root cause = **P-79 challenger scope** (operator decision pending). Positives held: net alpha +8.12% (chal +7.16 / champ +1.39 / bench −0.96), calibration 3.31pp in band (n=3,477), Brier 0.2667.

## Claude errors logged today
10. The P-110 golden-negative initially watched the wrong channel (returned patch text) while the defect degraded silently into `identity_mismatch`/RuntimeError patches — three detector revisions were needed before the white-box check (sem bound to a closed loop) + `last_error_class` + asyncio-logger capture made it fire. The silent-degradation behavior it exposed is itself the P-110 impact refinement.
11. Rebuild-log expectation was first set to 3 at the measuring point; correct is 2 (the first build on a fresh loop is not a rebuild).

## Pending read-backs (operator)
1. **First provider-touching run post-deploy** (scheduler tick or manual Top-10/refresh): `EODHDClient v4.18.0 initialized` present; `bound to a different event loop` = 0; `Future exception was never retrieved` = 0 → **P-110 CLOSED**.
2. Render **Settings → Health Check Path** live field (repo says `/readyz`).
3. Decision Diagnostics re-run (stale since 2026-08-11).
4. P-99 cell fix → export.
5. Three `plAddManualDividend` entries for 5023.SR (2026-02-23 / 05-23 / 08-23, 212.50 each) replacing the typed 637.50 cell.

## Operator decisions pending (unchanged + one new)
HCI **re-entry stance** (engine re-qualified it FAST-TRACK day 1, score 75.2, one day after the full exit — exclude to free the seat if no) · DDI ADD (8,284 SAR) vs Top-10 funding HCI+VEL (~21,190) contending for the same 21,255 cash · floors (a) stay-red vs **(b)-versioned** · CRC orphan GTC legs (53sh TP 63.55 / STP 50.80) · P-79 challenger scope.
