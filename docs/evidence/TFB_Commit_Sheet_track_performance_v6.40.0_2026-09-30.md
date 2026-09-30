# TFB Commit Sheet — scripts/track_performance.py v6.40.0 [P-180 FORCED COVERAGE: ACTIVE LOTS FIRST, CLOSED LOTS SCOPED]

Date: 2026-09-30 (Wednesday) · Lane: GitHub/Python (daily_sync.yml track step) · **Build #4 of the day — one over the three-lane cap, pulled forward on Emad's "let's write the next scripts"; disclosed here** · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `scripts/track_performance.py` v6.39.0 at HEAD `0cdb24f` — sha `7128eb413a3b5443…` (9,133 lines) = the 09-21 P-158 delivery; zero drift |
| Runtime facts | `TFB_TRACK_FORCE_DECISION_SYMBOLS` default ON (v6.16.0); `TFB_TRACK_FORCE_MAX` default 40; ledger tab `_Portfolio_CostBasis`; the track step runs in GitHub Actions (daily_sync.yml), not Render |
| Environment golden | base embedded selftest `PASS 14/14` (run through the real `PerformanceTrackerApp._track_selftest_`) |

## S2 — Root (pinned on source + the 2026-09-30 ledger export)
`_load_costbasis_symbols` (v6.16.0/v6.20.0) reads column A of the **whole** ledger — every closed lot's symbol joins the forced-coverage priority set for life. Today: 39 lots → **36 unique symbols = 6 Active + 30 closed-only**; `missing` = 36 − covered < cap 40 (four names of headroom). Each new closed name adds one; at 41 the cut is `sorted(missing)[: 40]` — **alphabetical** — so the ACTIVE holdings that sort late (YUM, KRP.US, DDI.US, CWBC.US) are the first to drop out of the daily Signal_History snapshot, blinding P-169's thesis-exit evidence on exactly the positions it watches (harness V3 golden-negative: base drops all four). Meanwhile 30 dead symbols are force-fetched from the backend three times a day. This is the register candidate opened on 2026-09-28 ("closed lots consume the cap"), numbered **P-180** (provisional).

## S3 — Change (7 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | v6.40.0 WHY block + `SCRIPT_VERSION = "6.40.0"` |
| E2 | after `_force_rows_page` | `_force_scope()` (`TFB_TRACK_FORCE_SCOPE` = **all** default \| `active`; junk → all) and PURE `_force_plan(active, closed, extras, covered, cap, scope)` → `{forced, cut, skipped_closed, scope, active, closed, extras}`: `missing` sorted exactly as v6.39.0; **below the cap the list is unchanged**; over the cap every active/pinned symbol is kept and the cap applies to the closed remainder in sorted order |
| E3 | before `_ensure_project_root_on_path` | PURE `_extract_costbasis_by_status(values)` (header row = the row holding both `Symbol` and `Status`; SG-1 shape validation; Active-in-any-lot wins; no header → `([], [])`) and `_load_costbasis_symbols_by_status(sid)` (same SA-credential pattern as the v6.16.0 loader; **one** `get_all_values` read; `None` on any problem) |
| E4a | `_augment_with_decision_symbols` | by-status loader first; `all` scope unions active + closed (= v6.39.0 set), `active` scope unions active only; **`None` → the v6.16.0/v6.20.0 loader path verbatim** |
| E4b | same | when by-status succeeded: `_force_plan` decides the fetch list; `[v6.40.0 COVERAGE] scope= active= closed= extras= forced= cut= skipped_closed=` info line (+ a warning naming the cut when the cap binds); the v6.16.0 branch and its log line are kept verbatim for the fallback |
| E5 | `_track_selftest_` | total 14 → **16**: case 15 (ledger matrix with title/legend rows, duplicate 1050.SR Active+Inactive, junk `TRUTH:` row → active/closed split + below-cap plan == sorted legacy) and case 16 (cap binds: YUM/pinned kept, `C38/C39` cut; `active` scope skips closed; covered dropped; env parser words) |

Behaviour contract: **`all` (default) is byte-identical to v6.39.0 whenever the forced set fits the cap — today's state — and protective only when the cap binds.** The ordering rule has no switch (the OFF state IS the defect: an active holding cut for a dead one). `active` is an opt-in operator arming (≈ 30 fewer forced backend fetches per run today; closed lots still snapshot when the cockpit itself lists them). Fail-open everywhere: any read problem → the old path.

Deliberate cuts: no change to Performance_Log recording, checkpoints, calibration, the unit sentry or the P-158 tables; no change to `TFB_TRACK_FORCE_MAX` (40) or the fetch page; the ledger read stays one API call (get_all_values replaces col_values(1) on the new path).

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `305f2cb9de6c4a566d41a3bebf89bfccff11752abfb9ca929f0da6f8ab0a6cb3` (9,356 lines) |
| `py_compile` | PASS |
| AST | functions 233 → 237 (+4 listed above, **0 removed**) |
| Line audit | 7 base lines not verbatim = version constant · the 4-line legacy loader call (now the fallback branch, same text re-indented) · `if len(missing) > cap` (now `elif`) · `total = 14` |
| Non-ASCII | 0 new characters vs base; 0 smart quotes |
| Embedded selftest | delivered `PASS 16/16` (base 14/14) via the real `_track_selftest_` |
| Harness `tests/test_track_force_coverage_p180.py` (REAL `PerformanceTrackerApp._augment_with_decision_symbols`; seams: the two ledger loaders + a recording fake backend; dual-tree via `TP_BASE`; real ledger export via `TP_LEDGER_TSV`) | **V1–V7 PASS ×3, digest `8fafab57f6780bbf` ×3** |
| V2 | today's 36-symbol ledger, 3 cockpit rows (DDI covered): 35 forced, sorted; **delivered fetch list == base fetch list, same order** |
| V3 | 6 active + 40 closed at cap 40: delivered fetches 40 with all six active (YUM, KRP, DDI, CWBC included) and cuts `C38/C39`; **base drops YUM, DDI.US, CWBC.US, KRP.US** (alphabetical cut) |
| V4 | `TFB_TRACK_FORCE_SCOPE=active` + pinned `NVDA.US,PNFP.US`: fetch = active ∪ pinned − covered; closed skipped |
| V5 | by-status loader `None` → fetch list == base (legacy path proven) |
| V6 | covered actives never refetched; pinned `ZZZ.US` kept; closed fill the cap |
| V7 | `_extract_costbasis_by_status` on the real 09-30 `_Portfolio_CostBasis` export → active = 5023.SR, YUM, DDI.US, CWBC.US, AER.US, KRP.US; closed = 30; zero junk |

## S5 — Delivery
| File | Destination |
|---|---|
| `scripts/track_performance.py` | repo (full file) |
| `tests/test_track_force_coverage_p180.py` | repo `tests/` (new; `TP_BASE=<v6.39.0 file>` enables the dual-tree legs, `TP_LEDGER_TSV=<ledger export>` enables V7) |
| `docs/evidence/TFB_Commit_Sheet_track_performance_v6.40.0_2026-09-30.md` | repo `docs/evidence/` |

## S6 — Arming / read-back (GitHub lane)
1. Commit the three files. No ENV change: the next track step (14:00Z schedule inside daily_sync / track_performance.yml) runs v6.40.0 in `all` scope — read-back = the job log line `[v6.40.0 COVERAGE] scope=all active=6 closed=30 extras=0 forced=<n> cut=- skipped_closed=0` followed by the unchanged `[v6.16.0 COVERAGE]` line, and a Signal_History day that still carries all six holdings.
2. Optional arming (separate sitting, cost/hygiene): `TFB_TRACK_FORCE_SCOPE: "active"` in the track step's env — read-back = `skipped_closed=30`, forced count falls by ~30 per run, Signal_History keeps every active holding.
3. Rollback: `git revert` (the default path is already the v6.39.0 semantics below the cap).
