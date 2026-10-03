# TFB Commit Sheet — scripts/tfb_export_audit.py v1.0.0 [NEW: offline six-gate export audit] + tests/test_export_audit_v1.py

Date: 2026-10-03 (Saturday) · Lane: GitHub/Python — operator tool, no workflow, zero sheet writes · Build #1 of the day · Protocol: One-Pass · File status: script **NEW** · harness **NEW**

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | none (new file). Repo HEAD at build time `eedcd4d7bf9ad4b2eef806a4133a248e7e911c04` per the 10-03 audit PDF's live read — **not fetched by this session** (repository access was refused). No existing file is touched. Collision check owed before upload: `ls scripts/ \| grep -i audit`. |
| Delivered | `scripts/tfb_export_audit.py` sha256 `eabdd9a6f6debb5c5a736bae932383fc02eaab4a69ba73a6d2d14785cceb8186`, **1,431 lines**, `SCRIPT_VERSION = "1.0.0"`, 42 functions / 2 classes · `py_compile` PASS · **2 non-ASCII characters** (the "·" separators in the ledger status-line regex and its synthetic twin; they mirror the workbook's own format) · 0 smart quotes · dependency: openpyxl only |
| Harness | `tests/test_export_audit_v1.py` sha256 `15822e7487926d79db8417cedee360dcb4ce88df3a799fb7a2cc23a60cd48977`, 80 lines; runs under pytest or directly |
| Export audited | `_Market Share Deepseek-V3.xlsx` export of 13:50 Riyadh, sha256 `c7d962d595b7d9882d2768a507705064e342ac249b9f3378ee3148c058d850d8` — the same file the independent 10-03 PDF audited |

## S2 — Design (WHY on the file header)
Every export is supposed to pass the six-gate audit, but the gates were ad-hoc scripts (two sessions could disagree on the same file) and the in-workbook validator samples 1,500 rows per page (P-190). This script runs the full population of every tab in one pass, prints the same numbers every time, and produces the Monitoring-Sheet tables directly.
- **Read-only and standalone.** One `.xlsx` in; never opens the live Sheet; no provider calls; no writes. Sharing/ACL, Render, GitHub and broker state are outside an export and are reported **NOT_CHECKED, never PASS**.
- **Gates:** (a) row counts vs `--expect`, duplicate/blank symbols, cross-page duplicates, missing names, invalid prices, |day move| > 25 %, price outside the 52W band, freshness on the as-of date, stuck rows > 5 days, schema widths 115/122, profit-margin unit errors, canned-confidence concentration, **crypto wrong-instrument names** (curated map of ~170 bases; unmapped bases counted, not judged), zero forecast at positive price, **forecast-price vs Expected-ROI pair disagreement** (tolerance max(0.1 pp, 4-dp rounding of the forecast price)), Horizon Days vs Invest Period Label (WARN "systematic" when > 50 % of a page), `_PIT_Fundamentals` timestamps in the reliability field · (b) INVEST rows vs the cockpit's DQ/reliability screen, SELL-class rows carrying INVEST, sector-cap panel values vs `--sector-cap 3/40` · (c) version stamps from every surface vs `--manifest` · (d) Performance_Log matured win rate, duplicate matured keys, all-zero risk columns, Signal_History multi-version days, `_S1_Calibration` state, Brier from `_Status`, S-1 verdict, **criterion 5 PASS on an empty `_Corporate_Actions`**, hypotheses without verdict · (e) `_Run_Log` ERROR rows, EODHD daily max vs `--eodhd-target` inside the window (today excluded as partial), HTTP-402 rows, identity-guard refusal rows, **run start lateness vs `--cron "17 4,12,20"`** (runs = clusters of sync rows with no 90-min gap; the first row rarely carries the run id), Dashboard_Audit scope cap · Book: active lots × My_Portfolio prices × FX (USD 3.75) vs Portfolio_Decision KPI (sold holdings still shown, stale KPI cash, NAV within 100 SAR), `_Cash_Snapshot` duplicate dates and notes unchanged while the balance moved, fills in the window vs `_Trade_Notes` · (f) RAG per lane (DATA, BOOK, DECISION, PIPELINE, MODEL, GOVERNANCE), ordered fix list, JSON + Markdown, `[EXPORT-AUDIT v1.0.0] … digest=` read-back line, exit 1 on any FAIL.
- **Conventions:** as-of = most common Last Updated (UTC) date unless `--asof`; naive timestamps are workbook-local (`--tz-offset 3`); `--now` for replays; `--since-days 7`.
- **Cockpit race check:** Top 10 "Last run" vs the four `_Status` page stamps — inside the run window = FAIL with the seconds-before-completion count; the "aged:" banner is read from the panel rows.

## S4 — Audits (REAL functions; the only stub is a synthetic workbook written to a temp file and loaded through the same openpyxl path)
| Battery | Result |
|---|---|
| P1 selftest — defects workbook (33 planted defects, each asserted by exact count and status) + clean workbook (zero FAIL; cockpit after sync; on-time run +3 min; Brier PASS; notes cover fills; BOOK and MODEL green) + determinism + Markdown render | **46/46 ×3, cases-digest `3de969895c97a7d6`** |
| P2 CLI contract — `--selftest` rc 0; no args rc 2; missing file rc 2 | PASS ×3 |
| P3 real export — 18 reference counts (below) + win rate 48.43 + Brier 0.2811 + holdings 46,065.31 / cash 46,398.75 + YUM as the sold holding shown + ACL NOT_CHECKED + determinism | **PASS ×3, digest `c516b3b5caf8a745`** |
| CLI on the export with the versions manifest ×3 | overall RED, **22 FAIL**, digest `a6e2618b5ec90020` ×3; Markdown sha `0e6909c34165952d…` ×3 |

Two logic defects were found by the selftest and fixed before delivery: blank forecast cells were being read as zero (false positives on every page), and run-start detection depended on a run id the first `_Run_Log` row of a run does not carry. Ragged rows from openpyxl's read-only mode are padded at load.

### Real-export results (export of 2026-10-03 13:50, `--now 2026-10-03T11:05Z`)
| Lane | RAG | FAIL | WARN |
|---|---|---|---|
| DATA | RED | 7 | 21 |
| BOOK | RED | 2 | 2 |
| DECISION | RED | 3 | 1 |
| PIPELINE | RED | 3 | 2 |
| MODEL | RED | 5 | 2 |
| GOVERNANCE | RED | 2 | 3 |

Reference counts reproduced exactly (PDF and hand check): invalid price 135/13/54 · missing names 134/49 · margin unit errors 2,483 GM / 156 ML · INVEST 144 vs 22 eligible · forecast/ROI pairs **35** (1 ML + 34 GM) · PIT timestamps 78 · duplicate matured keys 27 · risk columns all zero 3 · cockpit race 76 s · caps off policy 2 · cross-page duplicates 5 · dead tabs 9 · win rate 48.43 % · Brier 0.2811 · NAV 92,464.06 vs KPI 92,482 · KPI cash stale by 12,232.75.
New beyond the PDF: **7 crypto wrong-instrument names** (SUI = Salmonation, GRT = Golden Ratio Token, APT = Apricot Finance, UNI = UNICORN Token, IMX = Impermax, STX = Stox, ARB = ARbit; the PDF listed 4) · the horizon mismatch is universe-wide (6,473 of 6,609 GM rows), so it is a column-semantics decision (P-194), not a holdings defect · run lateness per run: 10-01 +6h12m / +3h41m, 10-02 +6h19m / +5h39m / +3h35m, 10-03 +5h40m (first `_Run_Log` row; GitHub's run start was 2 min earlier) · EODHD over 90k on **6 of 7** full counter dates in the window · 13 cash-note staleness events · 4 fills in 7 days vs 0 trade notes.

## S5 — Delivery
`scripts/tfb_export_audit.py` (NEW) · `tests/test_export_audit_v1.py` (NEW) · this sheet (NEW) · `audit_2026-10-03.md` (evidence, not for commit)

## S6 — Operator steps (one action each)
1. Upload `tfb_export_audit.py` into `scripts/`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/scripts — and `test_export_audit_v1.py` into `tests/`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests
2. Commit message: `tfb_export_audit v1.0.0 [NEW] offline six-gate export audit + harness`
3. Use: File → Download → Microsoft Excel from the workbook, then `python scripts/tfb_export_audit.py export.xlsx --md audit.md --json audit.json --manifest versions.json` (manifest = `{"data_engine_v2":"5.151.0","opportunity_builder":"1.23.0","portfolio_actions":"1.14.0","run_dashboard_sync":"6.64.0","run_shadow_scorer":"1.9.1","track_performance":"6.41.0","run_shadow_board":"1.5.0","route":"4.16.0"}`). Paste `audit.md` into the day's Monitoring Sheet. Read-back = the `[EXPORT-AUDIT v1.0.0] … digest=` line; the same export must give the same digest on any machine.
4. Not wired to any workflow in v1.0.0 (observe-equivalent by construction). A CI step over the `workbook_backup` artifact is the candidate v1.1.0 after one week of hand runs.
Rollback: delete the two files; nothing depends on them.

## Known limits (stated, not hidden)
- Sharing/ACL, Render health, GitHub run state and broker balances are outside an export → NOT_CHECKED.
- Run start = first `_Run_Log` row of the run, about 2 minutes after the GitHub run start.
- Crypto map is curated; a base not in it is counted as unmapped, never judged.
- `--expect` defaults to the 10-03 universe (255 / 6,609 / 453 / 2,474); pass new counts when the universe changes (P-174).
- Price validity does not distinguish retired or blocked instruments (same caveat as the PDF).
- FX: USD 3.75 and SAR only; other currencies are reported as unpriced lots.

## Script-change register from the 10-03 reconciliation (what still needs the live files)
| # | Finding | Fix type | File | Status |
|---|---|---|---|---|
| 1 | Crypto wrong-instrument names ×7; 134 + 49 missing names | Revise | `core/identity_guard.py` v1.3.0 + provider name pin | needs live file at HEAD |
| 2 | Zero forecast prices at sub-cent; 35 forecast/ROI pair mismatches (P-158) | Revise | `core/data_engine_v2.py` 5.151.0 — significant-figure rounding; preserve forecast + ROI atomically | needs live file |
| 3 | Horizon Days 365 vs label 3M, universe-wide | Decision (P-194) then revise | `run_dashboard_sync.py` writer or label semantics | Emad's call first |
| 4 | Coverage/freshness gates FAIL while `_Status`/digest say SUCCESS (P-108/P-162) | Revise | `run_dashboard_sync.py` 6.64.0 decision-feed stamp + digest consume audit results | needs live file + decision word |
| 5 | Identity-guard refusals never reach `_Run_Log` (P-187) | Revise | `run_dashboard_sync.py` summary row | needs live file |
| 6 | S-1 criterion 5 PASS on an empty `_Corporate_Actions` | Revise | `scripts/run_shadow_scorer.py` 1.9.1 → NOT_EVALUABLE | needs live file |
| 7 | `/health` omits A1/A2 gate keys | Revise | `main.py` 8.14.0 (fixed key list ~line 1960) | needs live file |
| 8 | Cockpit ran inside the sync run (aged:GM) | Revise | `16_Decision_Top10.gs` hold check — in the pending v1.11.12 build | blocked on the paste of live v1.11.11 |
| 9 | 144 INVEST vs 22 eligible (P-193) | Decision (F-series) then `scoring.py` or cockpit label | — | Emad's call |
| 10 | Schedule 3.5–6 h late; EODHD > 90k on 6/7 days (P-170) | Arm | PAT `TFB_GH_DISPATCH_TOKEN` + dispatcher; then `daily_sync.yml` to 2 slots | config |
| 11 | ADD guards, dedup, risk blanks, margins, validator rows | Arm | `TFB_PF_ADD_LOSER_VETO`, `TFB_PF_CONFIRM_SESSION`, `TRACK_DEDUP_MATURED`, `TFB_MARGIN_PUBLISH`, `VALIDATE_MAX_ROWS` | ENV, Emad's hand |
| 12 | Caps 2/30 → 3/40, SBAC date, duplicate cash rows, dead tabs, sharing | Sheet | — | Emad's hand |
| 13 | Ad-hoc audits; validator samples 1,500 rows | **Add** | `scripts/tfb_export_audit.py` v1.0.0 | **delivered (this sheet)** |
