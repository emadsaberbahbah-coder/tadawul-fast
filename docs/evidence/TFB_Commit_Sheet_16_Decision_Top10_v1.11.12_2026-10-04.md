# TFB Commit Sheet — apps_script/16_Decision_Top10.gs v1.11.12 [P-181b / P-149 / P-195 / SYNC-INFLIGHT — COCKPIT TRUTH] + tests/test_dt10_v11112_cockpit_truth.js + tests/test_export_audit_v1.py (D7 rebuild)

Date: 2026-10-04 (Sunday) · Lane: Apps Script (GAS) · Build #1 of the day (slot B1 taken by the paste, per the 09:45 map) · Protocol: One-Pass · Runtime: ES5

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | your paste `New_Text_Document.txt` = `16_Decision_Top10.gs` **v1.11.11**, sha256 `0112efbdb1e779025095181dc74ca4a3e4c55c9804935997f9ed21def402ee17`, 5,732 lines CRLF, 114 top-level functions — **byte-identical to the 09-26 delivery** (sha `0112efbd…`), so the editor's source of truth = the delivered v1.11.11; the live `epoch=2026-10-04/feed(frozen)` status token proves it is what runs |
| Delivered | `16_Decision_Top10.gs` **v1.11.12**, sha256 `f30bddd3fbeb6bc334d22a5edd67ec7858abb7e5add2c5e8e42f081277ec1164`, 6,138 lines CRLF (no trailing newline, as the base), 126 functions (**+12, 0 removed**), `node --check` PASS, 0 smart quotes, non-ASCII count unchanged (491), ES5 only in the delta (arrows appear only inside comments) |
| Harness | `tests/test_dt10_v11112_cockpit_truth.js`, sha256 `63e7dadc…b2637e327`, 294 lines; dual-tree (base v1.11.11 vs delivered) in a vm context with service stubs; embeds the **real 2026-10-04 Global_Markets header (115 cols) and the RDN / NVDA / DDI / BHF rows** and the **real 2026-10-03 13:05 race stamps** |
| D7 rebuild | `tests/test_export_audit_v1.py` **v1.0.1**, sha256 `09f5837a8b806123350da9b6cdef5c328ae7811c37544098dc335098e9a1d1d5`, 79 lines — replaces the byte-copy of the script sitting at HEAD under this name; runs the REAL `scripts/tfb_export_audit.py` as a subprocess: **4/4 PASS ×3** (selftest `46/46 … 3de969895c97a7d6`, CLI rc contract, determinism, not-a-copy guard) |
| Repo | HEAD `2efc442` (unchanged since 10-03 16:57); `apps_script/` holds only `11_Manual_Refresh_Coordinator.gs` and `24_Sync_Dispatch.gs` — this delivery is the **first mirror** of `16_Decision_Top10.gs` |

## S2 — Root causes (pinned on source + the 10-02 … 10-04 exports)
1. **P-181b** — `DT10_POOL_FIELDS` (the fixed projection the cockpit POSTs as `rows`) never carried `52W High / 52W Low / 52W Position % / Percent Change`. opportunity_builder v1.23.0's timing gate reads aliases `52whigh / 52wlow / 52wposition / percentchange` (`_FIELD_ALIASES`, L3171–3180) and `_w52_eval` returns `unknown` without them → `[w52-observe] n/a (no 52W / change fields)` on 500/500 rows. Same class as v1.6.1 (Last Updated) and v1.8.1 (Forecast Source): a gate starved by the pool contract.
2. **P-149 (GAS half)** — `dt10QualToRow_` prints the real reason for a `structural_block` candidate only when `DT10_V188_SEAT_TRUTH` is true; it has been `false` since v1.8.8 (2026-08-11, "the operator flips it deliberately"). Never flipped → AER.US / KRP.US read "ranked below the Max Selected cut" on today's board (export verified). The same toggle gates the G-a KPI cell.
3. **P-195** — nothing records a cockpit run killed at the GAS 6-minute limit (10-03 17:06:29 "kicking off", no completion/error row).
4. **SYNC-INFLIGHT** — the `_Sync_Control` lease covers only the ~3-minute page write; a cockpit run during the 40-minute Global_Markets leg (10-03 13:05:04 vs GM completion 13:48:28) builds on a mixed epoch. The first (Market_Leaders) and last (Global_Markets) legs' `_Status` stamps carry `run=<id>`; a differing id with the first leg newer = a run between its legs.

## S3 — Change (20 anchored edits, each `count == 1`; CRLF preserved)
| # | Site | Edit |
|---|---|---|
| E1 | header | `Version: 1.11.12` + the v1.11.12 WHY/WHAT block (inserted before the v1.11.11 block) |
| E2 | `var DT10_VERSION` | `'1.11.12'` |
| E3 | `DT10_PANEL` | `'T10: Max Per Sector'` built-in default `2 → 3` (re-seeds only; the live cell is an operator input preserved by P-76 — type 3) |
| E4 | `DT10_V188_SEAT_TRUTH` | `true`; NEW `dt10SeatTruthOn_()` (kill: Script Property `DT10_SEAT_TRUTH_LEGACY=1`); the two reads (`dt10QualToRow_` reason chain, `dt10RenderPayload_` KPI cell) routed through it |
| E5 | `DT10_POOL_FIELDS` | + `52W High`, `52W Low`, `52W Position %`, `Percent Change`; NEW `DT10_P181B_SENDS`, `dt10W52FieldsLegacy_()` (kill `DT10_P181B_W52_LEGACY=1`), `dt10PoolFieldsActive_(legacy)`; `dt10MapHeaderCols_` and `dt10PoolRowFromSheetRow_` iterate the active list |
| E6 | constants | `DT10_P195_MARKER_PROP`, `DT10_P195_LIMIT_MIN=7`, `DT10_INFLIGHT_PROP`, `DT10_INFLIGHT_FIRST_KEY`, `DT10_INFLIGHT_LAST_KEY`, `DT10_INFLIGHT_MAX_MIN=90` |
| E7 | pure cores + I/O (before `dt10IsFundingAlert_`) | NEW `dt10FeedStampParse_`, `dt10SyncInflightCore_`, `dt10SyncInflightMode_`, `dt10SyncInflight_`, `dt10RunMarkerCore_`, `dt10RunMarkerLegacy_`, `dt10RunMarkerCheck_`, `dt10RunMarkerClear_`, `dt10RunLogRow_` |
| E8 | `refreshDecisionTop10` after `dt10ReadPanel_` | marker check (ABORTED_PREV row when a stale marker exists, then store ours) → in-flight check: `enforce` + active ⇒ status `HELD`, `_Status` HELD, one `_Run_Log` row, marker cleared, **return before the POST**; `observe` + active ⇒ token `inflight=…(observe)` on the final status line; inactive ⇒ byte-identical run |
| E9 | the NETWORK ERROR / HTTP / DEGRADED returns and the normal end | `dt10RunMarkerClear_()` |
| E10 | `tfbMorningCockpitRefresh` catch | `dt10RunMarkerClear_()` (a logged failure is not an abort) |
| E11 | `dt10SelfTest` | + `w52 projection core`, `sync inflight core`, `sync in-flight now` (live), `run marker core`, `seat truth` lines |

Defaults: P-181b fields **ON** (the OFF state is the blind gate; kill restores v1.11.11 byte-for-byte), seat truth **ON** (display only; kill), marker **ON** (log-only; kill), in-flight **observe** (`DT10_SYNC_INFLIGHT` = off | observe | enforce). Selection, gates, clocks, tickets, KPIs from the backend: untouched.

## S4 — Audits (REAL functions, dual-tree, ×3 identical)
| Battery | Result |
|---|---|
| Harness `test_dt10_v11112_cockpit_truth.js` T1–T6 | **37/37 PASS ×3, cases-digest `3f7956937ba012cf`** — T1 projection on the real GM header: delivered rows = base rows + exactly the four fields (RDN 19.39 % / NVDA 94.66 % / DDI 98.30 % / BHF 24.72 %), kill = base byte-identical · T2 AER.US: base "ranked below the Max Selected cut", delivered "Portfolio: held vs exclude holdings (Include Portfolio Holdings = No)", KPI cell "0 exec + 6 grace / 10" for today's board, kill = base · T3 in-flight core on the 13:05 race (active, 6 min) / clean 10-04 (inactive) / 121 min (stale) / blank / last-newer / same-run · T4 marker: the 17:06 run judged ABORTED (244 min) at 21:10, 2-min marker = concurrent, one WARN `ABORTED_PREV` row, kill = no writes · **T5 the REAL orchestrator**: enforce + race ⇒ no POST, `status: HELD`, one HELD row, marker cleared; observe + race ⇒ proceeds to the POST; enforce + clean ⇒ proceeds; **base on the same race posts regardless** · T6 default 2→3, versions, P-144 resolver unchanged, zero removals, ES5 delta |
| Repo harness `tests/test_dt10_p144_epoch_key.js` on the delivered tree | every delivered-side check PASS incl. **T6 = the REAL `dt10SelfTest()` printing all prior `… core: ok` lines plus the five new ones**; the three FAILs are the harness's base-side assertions that require v1.11.10 as base (we only hold v1.11.11) — expected, not a regression |
| End-to-end P-181b through the REAL Python builder v1.23.0 (`normalize_candidate` → `_w52_eval`, `TFB_T10_W52_TIMING=observe`) | delivered RDN row → `pos 19.4% \| 1d +4.3%` (pass); NVDA 94.7 % **fail_high**; DDI 98.3 % **fail_high**; BHF 24.7 % pass; the base-shaped row → `unknown=True, 'n/a'` = the live defect reproduced |
| D7 harness `tests/test_export_audit_v1.py` | 4/4 PASS ×3 against the real script |

## S5 — Delivery (4 files)
`apps_script/16_Decision_Top10.gs` (paste + first repo mirror) · `tests/test_dt10_v11112_cockpit_truth.js` · `tests/test_export_audit_v1.py` (overwrite the mis-paste) · `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.12_2026-10-04.md` (this sheet)

## S6 — Operator steps (one action each)
1. Apps Script editor → file `16_Decision_Top10` → select all → paste the delivered file → save. Then run **`dt10SelfTest`** → the log must show `w52 projection core: ok (4 fields; legacy filter drops 4)`, `sync inflight core: ok (…)`, `run marker core: ok (17:06 run = aborted 244m; 2m = concurrent)`, `seat truth (P-149 / KPI cell): ON`, and the prior lines (`epoch key core: ok`, `outage pause core: ok`, …). Reply with the `sync in-flight now:` line.
2. Panel: type **3** into `T10: Max Per Sector` (row 9) — the default only governs re-seeds.
3. One manual cockpit refresh (menu) after the next sync lands — **read-back**: audit-grid rows show `[w52-observe] pos N% | 1d ±x%` (not `n/a`), `meta.timing_gate.evaluated > 0`; ALL QUALIFIED AER.US / KRP.US read `Portfolio: held vs exclude holdings …`; KPI cell 3 reads `E exec + G grace / 10`; status line carries no `inflight=` token on a clean epoch.
4. Repo mirror (your commit): upload the four files — https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/apps_script (the .gs), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests (both tests; the D7 file overwrites), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/docs/evidence (this sheet). Commit message: `16_Decision_Top10 v1.11.12 [P-181b/P-149/P-195/SYNC-INFLIGHT cockpit truth] + harness; test_export_audit_v1 v1.0.1 (D7 rebuild)`.
5. Arming (later, one per evidence run): `DT10_SYNC_INFLIGHT = enforce` only after one observed `inflight=` token or at the Saturday sitting; Render `TFB_T10_W52_TIMING=enforce` is the Saturday decision once the observe read-back names the would-suspend seats (NVDA/DDI-class).

Rollback: re-paste v1.11.11 (sha `0112efbd…`), or per feature: `DT10_P181B_W52_LEGACY=1`, `DT10_SEAT_TRUTH_LEGACY=1`, `DT10_P195_MARKER_LEGACY=1`, `DT10_SYNC_INFLIGHT=off`.

## Known limits / deliberate cuts
- The in-flight detector keys on first/last legs only (ML / GM); a run that writes GM before ML (never observed) would read as complete. Weekly-skipped MF/CFX pages do not enter the test by design (D-G).
- The ABORTED_PREV row names the previous run, not the cause (the GAS Executions page has it); a run killed while another run's marker is fresh (< 7 min) is overwritten silently.
- `15_Lists_Config`'s `TFB_PANEL_DEFAULTS` may still reseed `Max Per Sector = 2`; that file is not in hand.
- Gate-row consumption (`TFB Gate Coverage/Freshness` → WITHHELD) waits for tomorrow's producers (B2/B3) → v1.11.13.
- The repo P-168 harness needs the 09-26 export TSVs (`../exp/*.tsv`), not available here; its core is covered by the real `dt10SelfTest` run (`outage pause core: ok`).
