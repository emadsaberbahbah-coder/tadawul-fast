# TFB Commit Sheet — apps_script/16_Decision_Top10.gs v1.11.13 [P-202 HOTFIX — ONE PROPERTY READ PER EXECUTION] + tests/test_dt10_v11113_prop_memo.js (+ fixture)

Date: 2026-10-05 (Monday) · Lane: Apps Script (GAS) · Build #1 of the day (B1, on "We can start build") · Protocol: One-Pass · Runtime: ES5

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `apps_script/16_Decision_Top10.gs` at HEAD `184407c` (the first repo mirror, LF-normalised by the web upload, sha `a7153d76…`) re-encoded CRLF without a trailing newline = **v1.11.12 as delivered on 10-04, sha256 `f30bddd3fbeb6bc334d22a5edd67ec7858abb7e5add2c5e8e42f081277ec1164` exact**, 6,138 lines, 126 top-level functions. The editor's live source is this file: the three `ABORTED_PREV` rows of 10-05 stamp `{"version":"1.11.12"}` and the 10-04 21:34 audit grid shows the P-181b fields evaluated |
| Delivered | `16_Decision_Top10.gs` **v1.11.13**, sha256 `c59c69da9cfe6f26a33af604fb33fd51598af0703019be9280f0437676d7d2a9`, 324,517 bytes, 6,227 lines CRLF (no trailing newline), 128 functions (**+2, 0 removed**: `dt10PropMemoGet_`, `dt10PropMemoReset_`), `node --check` PASS, 0 smart quotes, non-ASCII count unchanged (491), ES5 only in the delta |
| Harness | `tests/test_dt10_v11113_prop_memo.js`, sha256 `a3329ecf…2df688`, 221 lines + fixture `tests/fixtures/dt10_pages_2026-10-05.json` (sha256 `b557c5a0…e9dea4`): the real 2026-10-05 headers (115 cols) and the first 10 rows of each of the four source pages (40 valid symbol rows) |
| Repo | HEAD `184407c` (10-04 16:49 Riyadh), zero drift since the mirror commit |

## S2 — Root cause (pinned on source; Monitoring Sheet #8 §2)
v1.11.12 edit E5 routed the pool projection through `dt10PoolFieldsActive_()`, and `dt10PoolRowFromSheetRow_` — called **once per sheet row** by `dt10CollectPoolRows_` — calls it with no argument, which calls `dt10W52FieldsLegacy_()`, which calls `PropertiesService.getScriptProperties().getProperty('DT10_P181B_W52_LEGACY')`. One service call per pool row: 255 + 6,609 + 453 + 2,474 = **9,791 rows → ≈ 9,800 service calls per run** (+4 in `dt10MapHeaderCols_`). At 10–30 ms each that is +100 … +300 s on a GAS side that already took 55–185 s (7-day median 84 s); the only v1.11.12 run that completed took **301.9 s** (10-04 21:30, backend 25.3 s); the next four (00:30 manual, 01:31, 05:35, 08:07 morning trigger) crossed the 6-minute ceiling — alive at +5 m 10 s (05:40 `document lock busy`), dead by +8 m 46 s (01:40 lock free). `dt10SeatTruthOn_` had the same shape at a smaller scale (once per qualified row). `DT10_P181B_W52_LEGACY='1'` cannot help: it changes the value returned, not the number of reads. The 10-04 harness stubbed `PropertiesService`, so the per-row service cost was invisible — owned.

## S3 — Change (8 anchored edits, each `count == 1`; CRLF preserved)
| # | Site | Edit |
|---|---|---|
| E1 | header | `Version: 1.11.13`; v1.11.13 WHY/WHAT block inserted before the v1.11.12 block |
| E2 | `var DT10_VERSION` | `'1.11.13'` |
| E3 | before `dt10W52FieldsLegacy_` | NEW `var DT10_PROP_MEMO_ = {}` + `dt10PropMemoGet_(key, reader)` (reader runs once per key per execution) + `dt10PropMemoReset_()` |
| E4 | `dt10W52FieldsLegacy_` | the existing try/catch read carried verbatim inside the memo reader → **one read per execution** |
| E5 | `dt10PoolFieldsActive_(legacy)` | no-argument form returns ONE cached list per execution (`pool_fields_active`); the explicit-argument form (`true`/`false`, used by the self-test) stays pure and unmemoized |
| E6 | `dt10SeatTruthOn_` | same memo pattern (`DT10_SEAT_TRUTH_LEGACY`), `DT10_V188_SEAT_TRUTH` short-circuit kept first |
| E7 | `refreshDecisionTop10` entry | `dt10PropMemoReset_()` right after `t0` — fresh reads per run |
| E8 | `dt10SelfTest` | new line `property memo core: ok (pool fields memoized; keys=N; reads=1/execution)` after `w52 projection core` |

Behaviour: for any property value the pool, body, board, KPIs and status line are **byte-identical** to v1.11.12; only the service-call count changes. No ENV, no YAML, no arming, no Script Property added. Rollback = re-paste v1.11.12 (`f30bddd3…`) or v1.11.11 (`0112efbd…`).

## S4 — Audits (REAL functions, dual-tree, ×3 identical)
| Battery | Result |
|---|---|
| `tests/test_dt10_v11113_prop_memo.js` T1–T7 | **22/22 PASS ×3, cases-digest `f90c6992489dd009`** — T1 real pages (40 rows): pool deep-equal base ↔ delivered incl. the four P-181b fields; reads `DT10_P181B_W52_LEGACY` **base 44 (= rows + 4 header maps) / delivered 1** · T2 kill property `'1'`: both trees drop the four fields identically; reads 44 / 1 · T3 seat truth: value identical, reads 3 / 1; kill honoured through the memo · T4 memo semantics: stable within an execution even if the property changes mid-run; after `dt10PropMemoReset_` the new value is read; explicit-argument form unmemoized · T5 `refreshDecisionTop10` resets the memo at entry (source position + behaviour) · **T6 the REAL `dt10SelfTest`** prints every prior `… core: ok` line (epoch key, outage pause, grace sizing, funding containment, cash source, w52 projection, sync inflight, run marker) **plus `property memo core: ok`**, zero FAIL; property reads inside the self-test: W52 flag 2 (pre/post its deliberate reset), seat truth 1 · T7 versions, +2/0 function census, CRLF-no-trailing |
| Repo harness `tests/test_dt10_v11112_cockpit_truth.js` on the delivered tree | every delivered-side check PASS (projection, seat-truth text, in-flight core, marker, **the REAL orchestrator enforce/observe paths**); the 9 FAILs are base-side assertions that require v1.11.11 as base (blindness, rank-cut text, version `1.11.12`, "+12 functions") — expected with a v1.11.12 base, not a regression (same pattern as the P-144 harness on 10-04) |
| Static | `node --check` PASS · functions 126 → 128 (+2, 0 removed) · CRLF 6,226 line breaks, 0 bare LF · 0 smart quotes · non-ASCII 491 → 491 · 103 added / 14 re-wrapped lines, no `let`/`const`/arrow/template in code lines |

Live-scale arithmetic (not simulated here): 9,791 valid rows on the 10-05 export → v1.11.12 made ≈ 9,795 `getProperty` calls per run; v1.11.13 makes **1** (+1 for seat truth, +≈ 20 unrelated reads that already existed per run).

## S5 — Delivery (4 files)
`apps_script/16_Decision_Top10.gs` (paste + repo mirror) · `tests/test_dt10_v11113_prop_memo.js` · `tests/fixtures/dt10_pages_2026-10-05.json` · `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.13_2026-10-05.md` (this sheet)

## S6 — Operator steps (one action each)
1. Apps Script editor → file `16_Decision_Top10` → select all → paste the delivered file → save. Run **`dt10SelfTest`** → the log must show `property memo core: ok (pool fields memoized; keys=2; reads=1/execution)` and every prior `… core: ok` line. Reply with that line.
2. Menu → one manual cockpit refresh (the panel `Cash Available` first, rule B.1). **Read-back:** a `decision-cockpit refresh completed` row in `_Run_Log` with `durationMs` back in the 60–200 s band, the status cell leaving `running…`, the board rebuilt on the EXECUTABLE 03:16 feed (ML 255/255), `[w52-observe]` still evaluated on the audit grid, and **no `ABORTED_PREV` row at the next auto run (13:30)**.
3. Repo mirror (your commit): https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/apps_script (the .gs), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests (the .js), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests/fixtures (the .json), https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/docs/evidence (this sheet). Commit message: `16_Decision_Top10 v1.11.13 [P-202 hotfix: one Script Property read per execution] + harness + fixture`.
4. If the first run after the paste still fails to complete: paste v1.11.11 (`0112efbd…`) and send the Apps Script → Executions row (status + duration) — the cause is then outside this file.

## Known limits / deliberate cuts
- The GAS-side budget stays structurally thin (≈ 1.2 M cells read and ≈ 9.8 k rows uploaded per run; pre-v1.11.12 outliers of 240–340 s exist). Levers deferred to operator decisions: **D-N** drop Mutual_Funds / Commodities_FX from the cockpit pool (−30 % rows, −32 % cells, 1 + 0 INVESTABLE rows affected) and the backend-side pool read (W-C). Neither is in this hotfix.
- The other per-run Script Property readers (`dt10CashSourceMode_`, `dt10RunMarkerLegacy_`, `dt10SyncInflightMode_`, earnings tag, selection-log flags …) read once or a handful of times per run and are left untouched.
- The harness proves the call-count mechanism on 40 real rows; the live-scale count (≈ 9,800) is arithmetic from the export's row counts, not a measured timing.
