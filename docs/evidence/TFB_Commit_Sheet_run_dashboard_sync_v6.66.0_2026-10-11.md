# TFB Commit Sheet - scripts/run_dashboard_sync.py v6.66.0 [WP-2c: whole-page Rank (Overall), gate TFB_RANK_OVERALL_PAGE] + .github/workflows/daily_sync.yml (mapping) + tests/test_sync_rank_overall_page.py + scripts/harness_wp2bc.py (shared)

Date: 2026-10-11 (Sunday) - Lane: GitHub / Python (daily_sync.yml env; one arming per evidence run) - Build: WP-2bc, framework section 4 WP-2 item (c) - Protocol: One-Pass - File status: sync **REVISED** 6.65.0 -> 6.66.0 - workflow **REVISED** (env mapping only) - test **NEW** - harness **NEW** (shared) - this sheet **NEW**

## S0/S1 - Base pin
| Item | Value |
|---|---|
| Base commit | `18a84168aac1d462dd58494239ac91f8cef7178c` (wave 1 landed: WP-6 Warnings compaction made this file 6.65.0) |
| Base file | `scripts/run_dashboard_sync.py` sha256 `8afeda49317cd046fa1a8d73c472fb21513e6e2a80ba1ce203f313ba00366d46`, **14,317 lines**, `SCRIPT_VERSION = "6.65.0"`, AST walk-all **346 defs / 8 classes**; module level 312 / 31 nested / 169 names |
| Base workflow | `.github/workflows/daily_sync.yml` sha256 `43e619d3dc94d819326690572bca42ad3df0fbc04b272e73b077a1e2bb5dd06b`, 2,030 lines |
| Pristine copies | `git show 18a8416:<path>` at `<scratchpad>/base_wp2bc/scripts/run_dashboard_sync.py` and `<scratchpad>/base_wp2bc/.github/workflows/daily_sync.yml` (sha256 identical) |
| Delivered sync | sha256 `58540b7b153408cc74648ee184e99eef52f8e4f7c645a0cbcd86181194a7d2c6`, **14,620 lines** (+303; 2 lines edited in place: banner, `SCRIPT_VERSION`), AST walk-all **352 / 8** (+6 incl. the nested `_rank_overall_page_core._cell` / 0), module level 317 / 32 / 176 (+5 defs, +7 constants), **0 removals**; `py_compile` PASS (both interpreters); ruff E9/F63/F7/F82 PASS; 0 smart quotes; non-ASCII 566 = base |
| Delivered workflow | sha256 `0d102b498a7f53dfb1a40fbb169175825c0cc9d7510f237a41a47ee6f3513a5e`, 2,050 lines (+20, two identical 10-line blocks; 0 lines removed); YAML parses (`yaml.safe_load`); `TFB_RANK_OVERALL_PAGE` present in exactly `sync-dashboard` and `recover-missing-market-pages` |
| Delivered test | `tests/test_sync_rank_overall_page.py` sha256 `2d55df54189e75264129a565b9d42dc2c7832d0c655796e3f1341b4c67828436`, 517 lines, 52 collected cases (pattern `tests/test_sync_sheet_presentation.py`; `python tests/...` runs `pytest.main`) |
| Shared harness | `scripts/harness_wp2bc.py` sha256 `9972d99def34ce358c2394157213f92a6f7bf82cf2bd8c02079fdd77546529a4`, 636 lines - a **RECORDING TRANSPORT around the REAL `SheetsWriter.write_table`** (the RecorderSheets precedent of `scripts/harness_sync_6410.py`; the transport only records update / append calls and applies RAW null-skip to an in-memory grid); no predicate, stats dict or writer is re-implemented - not a fake-object harness |
| Export audited | 2026-10-10 export (run 38071484709), identified by content (live rows ML 255 / GM 6,609 / CFX 453 / MF 2,474); openpyxl `read_only=True, data_only=True` under `python -I` |

## S2 - Design (WHY; full paragraph `v6.66.0 - WP-2c [RANK-PAGE]` in the module header above the wave-1 v6.65.0 paragraph)
**Evidence (re-measured; harness export leg + WP-0):** distinct Rank (Overall) values ML 25 / GM 25 / CFX 22 / MF 14 on 253 / 6,168 / 92 / 106 ranked rows = each exactly 1..N, a WITHIN-BATCH rank. WP-0 **DQ-41 FAIL, count 6,533**. Mechanism: `TFB_SYNC_SYMBOL_BATCH_SIZE: "25"` (both market jobs) + the "analysis" gateway, so `routes/analysis_sheet_rows.py _apply_global_rank_overall` ranks each 25-symbol request; the complete page exists only at `SheetsWriter.write_table`. Eligibility (numeric Overall Score AND no exact `low_data_trust` token AND no `f7_settle_failed:` / `settle_unresolved:` prefix) reproduces today's withholding exactly: eligible == ranked on every page, 0 eligible-but-unranked, 0 ranked-but-ineligible. Blank-string Rank cells today ML 2 / GM 441 / CFX 361 / MF 2,368 (the route already writes "" for ineligible rows).

**Design (challenge C6 preferred option):** PURE CORE `_rank_overall_page_core(hdr, matrix, mode) -> (matrix, stats)` beside `_ohlc_fill_guard_core` (env-free, no I/O, never raises on malformed cells); env wrapper `_rank_overall_page_apply` called in `write_table` AFTER the fill guard and BEFORE the copy-only presentation; `core/sheet_presentation.py` NOT touched (no pin change, its copy-only contract intact). Order: Overall Score desc, Symbol asc on exact ties (deterministic; GM's largest tie 8 rows at 45.49).
- **off** (default; any word other than observe/enforce): the same matrix object, no log, no `_Run_Log` row.
- **observe**: same object, values untouched; one INFO line `[RANK-PAGE v6.66.0] page=.. mode=observe rows=.. eligible=.. distinct_before=.. max_before=.. changed=.. withheld(low_trust/settle/no_score)=..` and one `_Run_Log` row, Status **OBSERVE** (a new status word in the run-log vocabulary; FG-2 uses OK/SUSPECT, WP-6 WOULD_COMPACT - C10).
- **enforce**: a NEW matrix, int rank 1..N on eligible rows, "" on every other row's Rank cell (None would be null-skipped by the RAW in-place update and leave the stale batch rank); every other cell verbatim; idempotent. `_Run_Log` Status OK.
- **Fail-open** in both armed modes (exception or failed postcondition "ranks are exactly 1..eligible"): today's within-batch ranks are published, WARNING logged, one `_Run_Log` Status ERROR row.
- **Telemetry:** FW-3 channel, honors the existing **`TFB_SYNC_OHLC_RUNLOG`** kill switch (C10, same lever as FG-2); its own append failure is annotated and NOT counted into `_RUNLOG_APPEND_FAILS` (the WP-6 rule: telemetry never flips the exit code).
- **WP-6 interplay:** `_wc_token_movable` is False for `settle_unresolved:*`, `rank_skipped_settle_unresolved`, `f7_settle_failed:*` (test Y2); the WP-6 reader census `test_t12` stays green (1 passed).
- **Workflow (C3):** `TFB_RANK_OVERALL_PAGE: ${{ vars.TFB_RANK_OVERALL_PAGE || 'off' }}` after `TFB_SYNC_SYMBOL_BATCH_SIZE: "25"` in BOTH job env blocks (`sync-dashboard`, `recover-missing-market-pages`); test Y1 asserts the two declarations and their owning jobs.

## S3 - Anchored edits (count == 1 each; base line numbers)
| # | Base line | Edit |
|---|---|---|
| 1 | :6 banner `(v6.65.0)` | -> `(v6.66.0)` |
| 2 | :10 above `v6.65.0 - WP-6 ...` | insert the `v6.66.0 - WP-2c [RANK-PAGE]` paragraph (prior paragraphs verbatim) |
| 3 | :1998 `SCRIPT_VERSION = "6.65.0"` | -> `"6.66.0"` + 3-line pointer comment |
| 4 | :3898-3899 fill guard / presentation in `write_table` | + `matrix, _rp_stats = _rank_overall_page_apply(sheet_name, hdr, matrix)` between them |
| 5 | :3953-3961 FG-2 run-log block | + the RANK-PAGE run-log block after it |
| 6 | :9167 end of `_append_runlog_ohlc_fillguard` | + RANK-PAGE section: 7 constants, `_rank_page_score`, `_rank_overall_page_core`, `_rank_overall_page_mode`, `_rank_overall_page_apply`, `_append_runlog_rank_page` |
| 7 | daily_sync.yml :553 and :1581 | + 9 comment lines + the mapping line, each job |

## S4 - Audits
| Battery | Command | Result |
|---|---|---|
| Compile / lint | `py_compile` (both interpreters), `ruff check --select E9,F63,F7,F82` | PASS / PASS |
| AST zero-removal | pristine base copy vs delivered | removed `[]` / `[]` / `[]`; added 5 defs + nested `_cell` + 7 constants; **PASS** |
| New tests x3 | the four WP-2bc files (see engine sheet) | `69 passed` x3 per interpreter, outcome digest `9701dfe236d11870` x6; this file alone `52 passed` (direct run too) |
| Discriminates against the base | this file in the pristine base tree | `29 failed, 1 passed, 22 errors` (the pass is Y2: WP-6 keeps the tokens, true on both) |
| WP-6 / FG / P-162 one process | `pytest -q tests/test_ohlc_fillguard.py tests/test_sync_fetchfail_truth_p162.py tests/test_sync_warnings_compact_wp6.py tests/test_de_f7_scoring_settle.py` + the 4 new files | `120 passed, 1 skipped` (venv311 and venv_full) |
| Existing sync / workflow suites | `tests/test_sync_sheet_presentation.py`, `tests/test_sync_warnings_compact_wp6.py` (12 passed, 1 skipped = T13 export), `tests/test_scheduled_portfolio_refresh.py tests/test_final_publication_workflow.py tests/test_track_flags_mapped.py` (55 passed) | green |
| Harness x3 | `python scripts/harness_wp2bc.py` (3 x venv311, 3 x venv_full with the pristine-copy base tree) | **WP2BC-DIGEST `948d431f6e69d0eab3a598bc6436f91a5ca6b9619356cf7c5a3ef753a614553c` x6**; L2 core `ffad7f10d8e17242`, L3 wiring `619abd0f5198d646` |
| Byte-identity, gate unset (L5) | REAL `write_table` over the recording transport on 4 pages x (special, 60-row three-batch) matrices, delivered tree vs pristine base tree | sync off-digest base `568087e0847f2c7c` == delivered `568087e0847f2c7c`; `off`/`1`/`on`/`garbage`/unset all identical |
| L5 discriminates | mutant tree whose `_rank_overall_page_mode()` defaults to enforce | sync off-digest DIFF -> RESULT FAIL (as designed) |
| Privacy grep | new file + diff additions | 0 hits |

### DQ-41 before / after on the 2026-10-10 export (pure core enforce over the export matrices; harness export leg, not in the digest)
| Page | ranked | distinct before | blank-string Rank | eligible | elig-not-ranked / ranked-not-elig | distinct after | rows changed | page top |
|---|---|---|---|---|---|---|---|---|
| Market_Leaders | 253 | 25 | 2 | 253 | 0 / 0 | **253** | 252 | 2223.SR 76.85 |
| Global_Markets | 6,168 | 25 | 441 | 6,168 | 0 / 0 | **6,168** | 6,167 | 0939.HK 85.71 |
| Commodities_FX | 92 | 22 | 361 | 92 | 0 / 0 | **92** | 91 | ZL=F 76.66 |
| Mutual_Funds | 106 | 14 | 2,368 | 106 | 0 / 0 | **106** | 105 | JPXN.US 75.08 |
**WP-0 `dq_41_rank` (its own function, on the book with the four market pages page-ranked): FAIL count 6,533 -> PASS count 0.** WP-0 run on the unmodified export before and after the build: digest `a92e4b7c9a54807f` both (DQ-41 FAIL 6,533; the export itself only moves after arming).

## S5 - Delivery
`scripts/run_dashboard_sync.py` (REVISED 6.66.0) - `.github/workflows/daily_sync.yml` (REVISED, mapping only) - `tests/test_sync_rank_overall_page.py` (NEW) - `scripts/harness_wp2bc.py` (NEW, shared) - this sheet.

### Pins required - `scripts/verify_deployment.py` (NOT edited here; relative to SCRIPT_VERSION 1.0.48; proven on a scratch tree: `tests/test_verify_manifest_pins.py` 3 passed, ci lean list + new files 2663 passed)
1. After line 88 (`# off); manifest only.`) add:
```
# v1.0.49: pin data_engine_v2 5.153.0 (WP-2b TFB_SETTLE_UNRESOLVED), run_dashboard_sync
# 6.66.0 (WP-2c TFB_RANK_OVERALL_PAGE) and opportunity_builder 1.25.1 (settle_unresolved
# reader); register TFB_SCORING_SETTLE / TFB_SETTLE_UNRESOLVED / TFB_RANK_OVERALL_PAGE as
# value flags (kill=False; ARMED only on the word enforce; the rank gate workflow-scoped).
```
2. Line 89: replace the prefix `SCRIPT_VERSION = "1.0.48"  # v1.0.48 (2026-10-11) - MANIFEST PIN ONLY:` with `SCRIPT_VERSION = "1.0.49"  # v1.0.49 (2026-10-11) - WP-2bc pins + three mode flags (header note).   # v1.0.48 (2026-10-11) - MANIFEST PIN ONLY:` (rest of the line kept).
3. Line 112: `     "1.25.0", "opportunity builder"),` -> `     "1.25.1", "opportunity builder"),`
4. Line 132: `    ("core.data_engine_v2", "__version__", "5.152.0", "data engine"),` -> `    ("core.data_engine_v2", "__version__", "5.153.0", "data engine"),`
5. Line 148: `    ("run_dashboard_sync", "6.65.0", "dashboard sync"),` -> `    ("run_dashboard_sync", "6.66.0", "dashboard sync"),`
6. After line 220 (`TFB_WARNINGS_COMPACT` FLAGS entry) add:
```
    ("TFB_SCORING_SETTLE", "off", "F-7 settle seam: observe tags / enforce settled row (off|observe|enforce)", False),
    ("TFB_SETTLE_UNRESOLVED", "off", "non-converged rows publish pass-1 scores as WATCHLIST, WP-2b (off|observe|enforce)", False),
    ("TFB_RANK_OVERALL_PAGE", "off", "sync publishes Rank (Overall) as a whole-page rank, WP-2c (off|observe|enforce)", False),
```
7. Line 282: `                "TFB_ADV_ENGINE_CALL_TIMEOUT_S", "TFB_OPP_BUILD_TIMEOUT_S"}` -> two lines:
```
                "TFB_ADV_ENGINE_CALL_TIMEOUT_S", "TFB_OPP_BUILD_TIMEOUT_S",
                "TFB_SCORING_SETTLE", "TFB_SETTLE_UNRESOLVED", "TFB_RANK_OVERALL_PAGE"}  # v1.0.49 (WP-2bc)
```
8. Line 295: `                    "TFB_WARNINGS_COMPACT"}  # v1.0.47 (WP-6): daily_sync / page_refresh_recovery env only` -> two lines:
```
                    "TFB_WARNINGS_COMPACT",  # v1.0.47 (WP-6): daily_sync / page_refresh_recovery env only
                    "TFB_RANK_OVERALL_PAGE"}  # v1.0.49 (WP-2c): daily_sync / page_refresh_recovery env only
```
9. Line 529: `        elif name == "TFB_WARNINGS_COMPACT":` -> two lines:
```
        elif name in ("TFB_WARNINGS_COMPACT", "TFB_SCORING_SETTLE", "TFB_SETTLE_UNRESOLVED",
                      "TFB_RANK_OVERALL_PAGE"):  # v1.0.49 (WP-2bc): same exact-word rule
```
Read-back on the scratch tree: unset -> all three `off` / not armed; `TFB_SCORING_SETTLE=enforce` and `TFB_RANK_OVERALL_PAGE=enforce` -> armed; `TFB_SETTLE_UNRESOLVED=observe` -> not armed; scopes render / render / workflow.

### ci.yml lines required (`.github/workflows/ci.yml`; relative to VERSION 1.1.5)
Header: `# VERSION 1.1.5 (2026-10-11) -- WAVE-1 SUITE ENUMERATION` -> `# VERSION 1.1.6 (2026-10-11) -- WAVE-2 SUITE ENUMERATION` and, above the `# v1.1.5:` note:
```
# v1.1.6: enumerate the WP-2bc suites in the lean job: tests/test_de_f7_settle_unresolved.py
# (engine settle_unresolved rule), tests/test_sync_rank_overall_page.py (whole-page rank at
# the sync write boundary), tests/test_analysis_sheet_rows_rank_withheld.py (route rank
# withholds engine-withheld rows), tests/test_ob_settle_unresolved.py (builder reader).
#
```
Lean "decision-layer suites" list, after `            tests/test_de_margin_publish_p152.py \`:
```
            tests/test_de_f7_settle_unresolved.py \
            tests/test_sync_rank_overall_page.py \
            tests/test_analysis_sheet_rows_rank_withheld.py \
            tests/test_ob_settle_unresolved.py \
```

### Workflow line the integrator must add outside this WP's file list (open item)
`.github/workflows/page_refresh_recovery.yml` job `recover-page` also runs `scripts/run_dashboard_sync.py` with `TFB_SYNC_SYMBOL_BATCH_SIZE: '25'`; after its `TFB_WARNINGS_COMPACT_KEEP` line (base :192) add `      TFB_RANK_OVERALL_PAGE: ${{ vars.TFB_RANK_OVERALL_PAGE || 'off' }}` so a recovery write after arming cannot republish within-batch ranks.

## S6 - Operator steps (one action each)
1. Merge with the pins / ci.yml lines above (+ the page_refresh_recovery.yml line). Nothing is armed by the merge: the Variable is unset -> `'off'`.
2. Next scheduled run read-back (off): no `[RANK-PAGE` log line, no `_Run_Log` RANK-PAGE row, Rank (Overall) identical to today (DQ-41 FAIL 6,533 unchanged).
3. **Arm observe:** repo Settings -> Variables -> `TFB_RANK_OVERALL_PAGE = observe` (no commit). Read-back: per market page per write one `[RANK-PAGE v6.66.0] page=<page> mode=observe ...` line and one `_Run_Log` row Status OBSERVE; expected `distinct_before` 25 / 25 / 22 / 14, `changed` ~252 / 6,167 / 91 / 105, `withheld(low_trust/settle/no_score)` matching today's blank cells (+/- input drift); sheet values unchanged.
4. **Arm enforce** after a positive read-back: `TFB_RANK_OVERALL_PAGE = enforce`. Read-back: `_Run_Log` Status OK rows; next export WP-0 DQ-41 **PASS, count 0**; distinct = ranked = 253 / 6,168 / 92 / 106 (+/- drift); Rank 1 on 2223.SR / 0939.HK / ZL=F / JPXN.US (+/- drift); blank Rank cells unchanged in number. Then arm the engine's `TFB_SETTLE_UNRESOLVED` (engine sheet).
5. Silence telemetry only (keep the gate): `TFB_SYNC_OHLC_RUNLOG=0` (also silences FG-2 lines).
6. **Rollback:** set the Variable to `off` or delete it (byte-identical next run) or `git revert`; an enforce-written page keeps its page ranks until the next write overwrites them with within-batch ranks.

## Integration 2026-10-11 (applied by the integrator in the same landing)
- `scripts/verify_deployment.py` 1.0.49 and `.github/workflows/ci.yml` 1.1.6 carry the pins and the four suite lines reported above.
- `.github/workflows/page_refresh_recovery.yml` job `recover-page` now maps `TFB_RANK_OVERALL_PAGE: ${{ vars.TFB_RANK_OVERALL_PAGE || 'off' }}` (the open item this sheet raised); `tests/test_sync_rank_overall_page.py::test_y4_recovery_job_maps_the_gate_too` pins it and fails without the mapping.
- `scripts/tfb_workbook_dq_audit.py` v1.0.1 counts DQ-37 non-converged rows by the prefixes `f7_settle_failed` or `settle_unresolved:` (never the substring), so arming `TFB_SETTLE_UNRESOLVED=enforce` cannot turn DQ-37 into a false PASS; see `TFB_Commit_Sheet_tfb_workbook_dq_audit_v1.0.1_2026-10-11.md`.

## Known limits (stated, not hidden)
- Exact ties are ordered by Symbol, while the engine / route break ties by arrival order: DQ-41 trend comparisons must use the page rule.
- KLG / persistence-preserved rows keep their (possibly stale) Overall Score and receive a page rank, as they receive a batch rank today; the rank reflects what is published, not freshness.
- The page rank is computed per write: a recovery write of one page re-ranks only that page (pages are ranked independently by design).
- `low_data_trust` is matched as an exact token (the route matches a substring); 0 rows differ on the export.
- `_Run_Log` grows by one row per market page per write when armed (4 per run, more when recovery writes).
