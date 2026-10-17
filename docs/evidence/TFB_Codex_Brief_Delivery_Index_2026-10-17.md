# TFB Codex Brief v2 (2026-10-17) — delivery index for branch `claude/new-session-twoifp`

Base: main `11ef134`. 22 commits, 95 files, +11,562 / −128 lines (19 commits for the Codex brief + 3 for the Decision Pages v2 plan packages, §B below). Every behaviour change ships dark (default off / byte-identical) unless marked **default on**; no ENV, Script Property or workbook cell was changed; every commit has a sheet in `docs/evidence/`.

| Item | Status | Commit(s) | Gate / kill | Sheet |
|---|---|---|---|---|
| L1 audit lane | delivered | fa42a55 | cron 47 20 UTC, single-writer wait | track_performance v1.1.0 |
| L2 backtest depth | diagnosis + fix | 4e88ae6 | — (loud empty verdict) | L2 diagnosis + v6.46.0 |
| L3 Forecast Scorecard | delivered (**default on**, display) | c44c3e2 | `TRACK_FORECAST_SCORECARD=0`, `DT10_MODEL_EVIDENCE_LEGACY=1` | L3 |
| L4 evidence gate | delivered (observe first) | 8d5417b | `TFB_T10_EVIDENCE_GATE`, `DT10_EVIDENCE_SCORECARD_SEND` | L4 |
| L5 Signal_History enforce + alpha-since | delivered (dark) | 644f9cd | `TRACK_DEDUP_SIGNAL_HISTORY=enforce` (Emad) | L5 |
| L6 target ROI unit | **prepared only** | 59cd767 (note) | decision D-L6 | L6/L7 note |
| L7 risk fields | **prepared only**; (a) is `TRACK_RISK_BLANK=1` (Emad) | 59cd767 (note) | decision D-L7 | L6/L7 note |
| T1 investability reasons | delivered (**default on**, display) | 1615775, 91c3377, 9f46633 | `TFB_T10_INVEST_REASONS_LEGACY`, `DT10_INVEST_REASONS_LEGACY` | v1.27.0 + DT10 v1.14.0 |
| T2 cluster + carried-seat cap | delivered (**default on**, brief's instruction) | fb918c6 | `TFB_T10_CLUSTER_LEGACY`, `DT10_SEAT_SECTOR_CAP_LEGACY` | T2 |
| T5 earnings gate | delivered (dark) | 8a82e2d, 9f46633 | `TFB_T10_EARNINGS_GATE`, `DT10_EARN_HOLD_LEGACY` | v1.28.0 |
| T6 day-0 pairing + turnover | delivered (dark / display) | d6a9548, 9f46633 | `TFB_S1_NEW_SEAT_DAY0=spot`, `DT10_TURNOVER_TOKEN_LEGACY` | v1.10.0 |
| T7 research levels | delivered (dark) | ed9792c | `DT10_RESEARCH_LEVELS=on` | T7 |
| C1 / C3 uncertified view + cash fallback | delivered (dark) | 08e4218 | `TFB_PF_UNCERTIFIED_VIEW=on`, `PD_UNCERTIFIED_CONTEXT=on` | C1/C3 |
| C4 switch advisory | delivered (rides the live scan; GAS column dark) | 5dd40a0 | `TFB_PF_SWITCH_ADVISORY_LEGACY`, `PD_SWITCH_ADVISORY_COL=on` | C4 |
| C6 run order | **not done** (verification needs #792 merged + chain armed) | — | — | — |
| G1 D3 prepare | delivered (prepare, byte-identical) | 59cd767 | file edit on GO | G1 |
| G2 / G3 | Emad merges (#781/#782/#784) | — | — | — |
| G4 GM histogram | diagnosis delivered | 4066d8b | — | G4 |
| G5 run verdict + G10 acceptance tally | delivered | cf8e875 | — | G5/G10 |
| G7 grid design note | delivered (note) | 4066d8b | decision D-G7 | G7 |
| G9 provider-target baseline | delivered | 55a1574 | `--seed` (remove line = rollback) | G9 |
| G11 hygiene | disposition note (2 code items prepared, 3 operator) | 4066d8b | — | G11 |

Follow-ups outside this branch: T5 route-side Calendar_Events merge (earnings dates onto Top-10 rows), G4 row-telemetry read (artifact host blocked from this session), G11 items 2 and 5 as small PRs, L6/L7 builds on D-L6 / D-L7.

## B. Decision Pages v2 Execution Plan (2026-10-17) — Codex packages on this branch

Plan file: `TFB_Execution_Plan_Decision_Pages_v2_2026-10-17.pdf` (this folder). Owner split per the plan: A1–A5 Claude (IDE paste), B1–B4 Codex (this branch), C1–C5 Emad.

| Package | Status | Commit | File / version | Gate (Emad) | Sheet |
|---|---|---|---|---|---|
| B1 Top_10 v2 layout | delivered (dark) | 58f8f01 | `16_Decision_Top10.gs` **v1.19.0** (not v1.14.0: taken by the brief layers) | `T10_V2_LAYOUT=1` | B1 |
| B2 IBKR history plumbing | delivered (no gate; inert until the Flex query carries NAV / Cash Report / Trades — plan C3) | bf035c6 | new `26_IBKR_History.gs` v1.0.0 + `10_My_Portfolio.gs` v1.5.17 hook | — | B2 |
| B3 sparkline (cockpit half) | delivered (dark) | 45f51fe | `10_My_Portfolio.gs` v1.5.16 | `PD_SPARKLINE=on` | B3 |
| B3 number formats / `_Data_Audit` heading | **not in repo** (`04_Format.gs`, `99_Data_Audit.gs` live only in the IDE) | — | — | — | B3 §1 |
| B4 Codex brief v2.1 | delivered above (§A) | — | — | — | — |
| A1 menu v2.19.0 | Claude's; append block + commit sheet archived here (`01_Menu_v2.19.0_append_2026-10-17.gs`; note: 519 lines / sha `7fd04f6061f7` received vs 517 / `71b690a2fd00` pinned on the sheet — a trailing-newline difference, verify in the IDE) | — | `01_Menu.gs` (not repo-managed) | — | A1 sheet (PDF) |
| A5 broker reconciliation input | CSV archived here and used as the B2 validation fixture (`tests/fixtures/ibkr_closed_trades_90d_2026-10-17.csv`) | 45f51fe | — | — | B2 §3 |

**Version reconciliation for the plan's authors (A3 / B1):** the plan names base versions 16 v1.13.9 and 10 v1.5.13 (= main). This branch already carries 16 v1.14.0 → v1.19.0 and 10 v1.5.14 → v1.5.17. Any Portfolio_Decision v2 renderer (plan A3 "v1.6.0 on top of v1.5.13") must be built on **v1.5.17** from this branch, or it drops C1/C3 (uncertified view), C4 (switch advisory column), B3 (sparkline) and the B2 hook. The IDE paste order is unchanged: one paste of each branch file.

