# TFB Commit Sheet — scripts/tfb_acceptance.py v1.0.7 [P-166 ACCEPTANCE STRICTNESS: EXECUTABLE BASIS + STAMP COVERAGE + NA HARDENING]

Date: 2026-09-24 (Riyadh) · Author: Claude (One-Pass Script Protocol) · Operator: Emad
Register: P-166 (tfb_acceptance non-strict — "board fills" counted the qualified KPI, NA parse failures never moved the verdict)

## 1. Base pin (S1)
| | |
|---|---|
| File | scripts/tfb_acceptance.py |
| Base | VERSION 1.0.6, sha `7f7a3180b97b437a…`, 1,027 lines — live-fetched from main (raw, cache-busted) |
| CI | `.github/workflows/acceptance_check.yml` runs `--selftest` then `--live --json artifacts/acceptance.json --board-min 5` daily 06:45Z, non-strict (strict only as a dispatch input) — unchanged by this build |

## 2. Delivered (S5)
| File | Repo path | SHA-256 (prefix) | Lines |
|---|---|---|---|
| tfb_acceptance.py | scripts/tfb_acceptance.py | `8fb5c8d97f7614e1…` | 1,231 (+212 / −8; every removed line is a replaced line — version, one `row()` line, two renamed D10-1 lines, the tally line, the selftest summary line, the overall line, the strict line) |
| this sheet | docs/evidence/TFB_Commit_Sheet_tfb_acceptance_v1.0.7_2026-09-24.md | — | — |

No new test file: the script's embedded `--selftest` is its battery (CI runs it before the live pass).

## 3. Defect pinned at source (v1.0.6)
- `check_board` measured **D10-1 "board fills"** as the Top_10 KPI `Passed` (the QUALIFIED set). On the real 2026-09-24 export it reads **PASS = 20** while the cockpit's own status line says `SEAT-CHECK kpi 2 funded vs board 0 exec +2 suspended +1 grace` and the banner says `0 EXECUTABLE TICKETS`. The acceptance instrument was certifying names that cannot be executed.
- `main` computed `overall = FAIL if (FAIL or read errors) else WARN/PASS` — a check that reports **NA because a section/field was not found** (A1 "board section not found", D10-5 "no guard counters", …) or a crashed check (`ERR`) never moved the verdict; only a recorded Source read error did. A malformed board could yield an overall PASS.
- `check_pages` G1 freshness used a rolling 24h window (99.8% today) while the decision feed withheld on the sync's own `fresh_cov=88.8%` — the instrument and the gate measured different things.

## 4. Change (S2/S3) — 13 anchored edits, each anchor asserted count==1
| # | Site | Change |
|---|---|---|
| E1/E2 | VERSION, docstring | 1.0.7; WHAT-IT-MEASURES gains D10-1b/1c, G1b, NA hardening |
| E3 | `class Check` | `na_kind` property: design (D10-6) / crash (ERR) / parse (any other NA); `row()` emits it |
| E4 | `_top10` | keeps the FULL status line (`last_run_full`; the SEAT-CHECK segment sits past col 160) and the output banner |
| E5 | `check_board` (+ `_top10_execution`, 3 regexes) | D10-1 renamed **"board qualified"** (same measure); **D10-1b** executable tickets from SEAT-CHECK (banner fallback): ≥1 PASS; 0 & WITHHELD → WARN (feed-blocked, not testable); 0 & HELD/EXECUTABLE → FAIL; unparsable → NA(parse). **D10-1c** funded-KPI parity: `Fundable Now − exec` = 0 PASS, >0 FAIL "(P-108)", <0 WARN |
| E6/E7 | `check_pages` (+ `_stamp_fresh_cov`) | **G1b-<page>** the STATUS-STAMP `fresh_cov=` ≥ 95% PASS / else FAIL / absent NA(parse) — G1's 24h window untouched |
| E8–E12 | `render`, `main` (+ `_legacy_na`, `_na_split`, `_overall_verdict`) | overall = FAIL on any FAIL, any read error, or any HARD NA (parse/crash); WARN on any WARN; else PASS. Design NA never moves it. Tally line adds `NA_hard= NA_design=`; JSON adds `na_hard`, `na_design`, `legacy_na`, per-check `na_kind`. `--strict` also exits 1 on hard NA. **Kill: `TFB_ACCEPTANCE_LEGACY_NA=1`** restores the v1.0.6 arithmetic (default ON — the OFF state IS the defect; P-127/P-130/P-145 precedent) |
| E13 | `_selftest` | 8th fixture set: the REAL 2026-09-24 08:08:29 status line + KPI strip + banner (HELD → D10-1b FAIL 0 / D10-1c FAIL 2), WITHHELD → WARN, executable → PASS/parity PASS, banner fallback, NA kinds, G1b on the real GM 88.8% / ML 100% stamps, overall arithmetic incl. the legacy switch |

Proofs: `py_compile` PASS; AST 42 → 48 names (+`_top10_execution`, `_stamp_fresh_cov`, `_legacy_na`, `_na_split`, `_overall_verdict`, `Check.na_kind`; **0 removed**); non-ASCII delta 0 (29 → 29); smart quotes 0.

## 5. Evidence (S4) — REAL artifact, dual version, ×3
| Leg | Result | Digest (×3) |
|---|---|---|
| Selftest v1.0.7 | **PASS 8/8** ×3 (the v1.0.6 seven fixtures unchanged + the P-166 set) | — |
| Real export (18 TSVs of 2026-09-24), v1.0.6 base | 36 checks; D10-1 PASS 20; overall FAIL only because Performance_Log is not in a browser export (read error) | `bfd67183…` |
| Real export, v1.0.7 | the 36 v1.0.6 checks carried with **identical verdict, measured and evidence (36/36)**; new rows: **D10-1b FAIL 0** (`exec=0 suspended=2 grace=1 output=HELD source=seat-check`), **D10-1c FAIL 2** (`kpi_funded=2 exec=0 gain_kpi=22,076 SAR — P-108`), **G1b-Global_Markets FAIL 88.8**, G1b ML/CFX/MF PASS 100 / 98.5 / 99.9; tally PASS=31 WARN=6 FAIL=3 NA=2 NA_hard=1 (D10-3b unreadable) NA_design=1 | `caafc7d5…` |
| Legacy kill on the real export | `TFB_ACCEPTANCE_LEGACY_NA=1` → v1.0.6 arithmetic (overall still FAIL here only via the read error), `legacy_na:true` in JSON, `legacy_na=1` on the tally line | — |
| `--strict` | base exit 1 / v1.0.7 exit 1 (read error), semantics unchanged for the daily non-strict job | — |

Run-to-run digest stability: the only field that moves between runs is the base's own rolling 24h `fresh24h=` count in G1 (identical in v1.0.6); digests above neutralise that field and the clock-age check.

## 6. Read-back (next `acceptance_check` run, 06:45Z, or a `workflow_dispatch`)
`artifacts/acceptance.json`: `version "1.0.7"`, rows `D10-1b`, `D10-1c`, `G1b-*` present, `na_hard`/`na_design`/`legacy_na` fields, per-check `na_kind`; the step summary tally line ends `NA_hard=… NA_design=…`. Expected on a normal morning: D10-1b FAIL 0 while the board keeps suspending every seat (the honest Day-10 measurement), D10-1c FAIL while P-108 stays parked, G1b-Global_Markets FAIL until the GM leg fits its time budget or the replay lands before the stamp is read. The daily job stays green (non-strict); only the JSON/summary verdicts change.

## 7. Deliberate cuts / disclosures
- D10-1's measure (qualified ≥ N) is kept as research capacity; it is renamed, not removed — every historical `acceptance.json` remains comparable on `id`.
- The executable count is read from the cockpit's own disclosure lines, not recomputed from the board rows: on a cockpit older than v1.11.6 (no SEAT-CHECK) the banner is the fallback, and with neither the check is NA(parse) — hard by design.
- "No qualified executable opportunity" remains a valid board output; D10-1b FAIL on a HELD day states that the Day-10 criterion is unmet, not that the board malfunctioned (the evidence names suspended/grace counts).
- Not changed: board_min default 5, the CI workflow (non-strict), G1's 24h window, the live-mode reader.

## 8. Register / next
- P-166 BUILT — awaiting commit (2 files; no Render deploy: the script runs in GitHub Actions).
- Still owed: the cron cut (`0 4,12,20 * * *`) — the line at HEAD still reads `0 */4 * * *`; Render deploy proof for v5.150.0 + Slot A arming; P-168 needs the live 16_Decision_Top10 source.
