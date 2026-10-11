# Audit framework wave 1 — integration record and arming plan (2026-10-11)

Source: `TFB_Workbook_Audit_Framework_and_Findings_v1.0_2026-10-11.pdf` (owner's
check catalogue DQ-01..DQ-75 and work packages WP-0..WP-12), measured against the
2026-10-10 workbook export (run 38071484709). Base commit of the wave:
`a6668c2` on `claude/pensive-hypatia-smnxqe`; published head after the wave: `741ab66`.

Every package was built in its own git worktree from a challenged build spec, then
reviewed by an independent agent that re-ran the tests three times, re-proved the
AST zero-removal and the default-off byte-identity with its own scripts, scanned for
forbidden edits and for portfolio figures (public repository), and either accepted
the build or sent it back to a fixer. All five packages ended in `accept`.

## What landed

| WP | File(s) and version | Gate (default off) | Observe read-back | Closes / measures |
|---|---|---|---|---|
| WP-0 | `scripts/tfb_workbook_dq_audit.py` v1.0.0 (new), `tests/test_workbook_dq_audit.py`, `tests/fixtures/dq_baseline_2026-10-10.json`, `scripts/backup_workbook.py` 1.5.0, `.github/workflows/workbook_backup.yml` v1.2.0 | repo Variable `TFB_DQ_AUDIT_MODE` off/observe/enforce; `TFB_BACKUP_LOCAL_COPY` derived from it | `[DQ-AUDIT v1.0.0] … checks=81 … regressions=N digest=…` on the nightly backup + artifact `tfb-dq-audit-<run_id>` (report + scorecard only, never the xlsx) | measurement harness for all 75 checks; 10-10 baseline: 44 FAIL, 11 decision-critical, digest `a92e4b7c9a54807f` |
| WP-7 | `scripts/track_performance.py` 6.44.0, `track_performance.yml`, `daily_sync.yml` (tracker step), `tests/test_tp_dedup_signal_p191.py`, `tests/test_track_flags_mapped.py` | repo Variable `TRACK_DEDUP_SIGNAL_HISTORY` off/observe/enforce (enforce is measure-only in 6.44.0); `TRACK_DEDUP_MATURED`, `TRACK_RISK_BLANK` now mapped in both lanes | job log `[v6.44.0 DEDUP-SIGNAL] mode=observe would_skip=k/built …`; `_Run_Log` token ` \| dedup_signal observe …`; `selftest=PASS 22/22` | DQ-61 measurement (Signal_History same-day repeats: 21,670 of 29,471 rows); kill-switch-without-mapping class |
| WP-8 | `apps_script/16_Decision_Top10.gs` v1.13.4, `tests/js/decision_input_truth.test.js`, `tests/fixtures/dt10_candidates_2026-10-10.json` | Script Properties `DT10_INPUT_TRUTH`, `DT10_CAND_TAB`, `DT10_VINTAGE_COLS` off/observe/enforce; `DT10_POLICY_REF` | B2 tokens `gates: … NOT EVALUABLE · panel: … · cal(earn-dated): … · cands … · vintage …` + one `INPUT-TRUTH` `_Run_Log` row | DQ-14, 47, 48, 49, 50, 52 disclosure (P-196) |
| WP-6 | `scripts/run_dashboard_sync.py` 6.65.0, `daily_sync.yml` (both jobs), `page_refresh_recovery.yml`, `tests/test_sync_warnings_compact_wp6.py` | repo Variable `TFB_WARNINGS_COMPACT` off/observe/enforce, `TFB_WARNINGS_COMPACT_KEEP` additive | boot `[WARNINGS-COMPACT v6.65.0] selftest=PASS requested=… effective=…`; four `… mode=observe status=WOULD_COMPACT …` lines; `_Run_Log` rows with `chars_removed` | DQ-43 (Warnings length: 9.4 M chars → 3.4 M under enforce, every gate token kept) |
| WP-3ab | `core/data_engine_v2.py` 5.152.0, `tests/test_de_roi12_ceiling_tuple_noise_wp3ab.py`, `tests/fixtures/wp3ab_tuple_rows_2026-10-10.json`, `scripts/harness_archive/wp3ab_dualtree_replay.py` | Render env `TFB_ROI12_SOFTCAP_GATE`, `TFB_TUPLE_NOISE_RELEASE` off/observe/enforce | tags `r12_ceiling:observe:hi/:band`, `tuple_noise:12m:(round\|drift\|precision_limited):observe`; `/health engine_gates`; boot `[v5.152.0 GUARDS] … r12_ceiling=… tuple_noise=…` | DQ-32 (1,238 blank ROI-12M with a forecast), DQ-33 (153 ROI-12M soft-cap breaches), DQ-31, DQ-38 |

Integrator-owned files changed alongside: `scripts/verify_deployment.py` 1.0.45 → 1.0.48
(pins track_performance 6.44.0, backup_workbook 1.5.0, run_dashboard_sync 6.65.0,
data_engine_v2 5.152.0; `TFB_WARNINGS_COMPACT` registered as a workflow-scoped value
flag that reads ARMED only on the word `enforce`), `.github/workflows/ci.yml` 1.1.5
(every new suite enumerated: lean job, heavy job for the openpyxl harness, native job
for the Apps Script harness; the three seam-distance contracts WP-3ab builds on were
never enumerated before and are now).

## Integration findings (not in any single builder's scope)

- **Cross-package reader census.** WP-6's census test scans the tree for readers of the
  Warnings heads it may compact and tripped on WP-0's audit script, which counts
  `sheet_horizon_label`, `fundamentals_lkg`, `rel_path` and `fund_cache` in the exported
  Warnings column. The script is declared in the census as an offline export reader with
  exactly that head set (a new head fails the test again) and both commit sheets record the
  consequence: the first enforce keep list must carry those heads until the audit reads
  the JSONL telemetry sink.
- **Whole-tree test order.** Running every `tests/` file in one pytest process (what the
  lean CI job does) exposed two environment leaks that single-file runs cannot see:
  another suite exports its own `GITHUB_RUN_ID` first, and `tests/test_ohlc_fillguard.py`
  left `TFB_SYNC_OHLC_FILL_GUARD=1`/`enforce` behind while the pattern-file environment
  dictionary pinned a misspelt variable that reads nothing. The WP-6 helper now pins the
  real names and the run id inside every block; the fill-guard suite restores its
  variables after each test.
- **Full-requirements collection.** `tests/test_yf_loopguard_p203.py` re-executes the
  Yahoo fundamentals provider under a fresh module name; with prometheus_client installed
  the duplicate metric registration aborted collection of the whole tree. The loader now
  releases the earlier collectors first (commit `c44968d`).

## Gates on the published head (`741ab66`)

| Gate | Result |
|---|---|
| compileall (main.py, core, routes, scripts, tests) | OK |
| ruff E9, F63, F7, F82 (whole tree) | OK |
| pytest, lean CI pins (Python 3.11) | 3259 passed, 15 skipped (baseline before the wave: 3229) |
| pytest, full requirements + openpyxl (Python 3.11) | 3347 passed, 9 skipped (baseline: 3306) |
| `tests/test_verify_manifest_pins.py` + workflow audit tests | 24 passed |
| `scripts/audit_repository_workflows.py` | errors=0, warnings=2 (pre-existing `TEMPORARY_OVERRIDE_ACTIVE` in daily_sync.yml) |
| `scripts/verify_deployment.py --selftests` | OK |
| node suites (`tests/js/*.test.js`, `tests/*.js`) | all PASS |

## Arming plan (one lever per evidence run; never two in the same sitting)

**Phase A — deploy and read back with nothing armed.** Deploy engine 5.152.0 to Render
(`/health engine_gates` shows `roi12_softcap: off`, `tuple_noise_release: off`). Paste
`16_Decision_Top10` v1.13.4 into the script editor, run `dt10SelfTest`, do one refresh
with every property unset (B2 identical in shape, footer row unchanged). Next tracker run
must show `[PERF-VERDICT v6.44.0] … selftest=PASS 22/22` with no dedup token; next sync
leg must show `[WARNINGS-COMPACT v6.65.0] … requested=off effective=off`. Nothing on any
sheet changes in this phase.

**Phase B — observe, in this order (GitHub Settings → Variables unless stated).**

1. `TFB_DQ_AUDIT_MODE = observe` first, so every later arming is measured by the nightly
   audit. Read back the `[DQ-AUDIT v1.0.0]` line and the artifact.
2. `TRACK_DEDUP_SIGNAL_HISTORY = observe` (optionally `TRACK_DEDUP_MATURED = observe` in the
   same sitting). Read back the `[v6.44.0 DEDUP-SIGNAL]` job-log line and the `_Run_Log`
   token. Do not set `TRACK_RISK_BLANK=1` until `scripts/tfb_export_audit.py` learns that
   blank is not zero.
3. `TFB_WARNINGS_COMPACT = observe`. Read back four `WOULD_COMPACT` lines whose
   `chars_removed` is within 5 % of ML 146,147 / GM 4,290,925 / CFX 209,386 / MF 1,329,350;
   market cells, `_Status` and cockpit counts unchanged.
4. Script Properties `DT10_INPUT_TRUTH`, `DT10_CAND_TAB`, `DT10_VINTAGE_COLS = observe`.
   Read back the B2 tokens, one `INPUT-TRUTH` `_Run_Log` row and its `durationMs`.
5. Render env `TFB_ROI12_SOFTCAP_GATE = observe` and `TFB_TUPLE_NOISE_RELEASE = observe`.
   Read back on the next export (the WP-0 harness counts them): about 1,190
   `tuple_noise:12m:(round|drift):observe` rows, 21 `precision_limited`, 153 or more
   `r12_ceiling:observe:hi`, about 498 `:band`. Confirm beforehand that
   `fc_tuple_coherent` and `margin_publish` read `observe` and `engine_target_klg` is on.

**Phase C — enforce, each in its own sitting after a positive observe read-back and the
owner decision it depends on.**

- `TFB_ROI12_SOFTCAP_GATE = enforce` after the owner chooses parity (every un-capped
  provider-target leg above 0.30) or narrow (above 0.35 only). Expected: DQ-33 → 0, DQ-31
  unchanged at 0, investability and recommendations unchanged.
- `TFB_TUPLE_NOISE_RELEASE = enforce` (Saturday sitting). Expected: DQ-32 ≤ 48, DQ-38 → 0.
- `TFB_WARNINGS_COMPACT = enforce` with `TFB_WARNINGS_COMPACT_KEEP =
  f7_settle,eq_roi_backfill,margin_publish,rel_path,fund_cache,fundamentals_lkg,sheet_horizon_label`
  until the audit reads the JSONL sink. Expected: GM mean about 400 chars / 10 tags.
- `DT10_*` one gate per run; before `DT10_CAND_TAB = enforce`, edit
  `.github/workflows/intraday_refresh.yml` `TFB_IQR_SYMBOL_PAGES` to include
  `_T10_Candidates`.
- `TFB_DQ_AUDIT_MODE = enforce` so a decision-critical regression reddens the nightly
  backup; regenerate the baseline fixture (`--baseline-out`) after each arming that
  legitimately moves counts.
- WP-7 `enforce` is measure-only in 6.44.0; the Signal_History write change is v6.45.0 and
  waits for the idempotency decision below.

Never arm: `TFB_EQ_ROI_UNIT_SENTRY=enforce` (the sentry is tag-only in the engine and
would scale by 100 in enriched_quote).

## Owner decisions carried from the builders' reports

- Signal_History idempotency policy (last-of-day upsert recommended; first-of-day skip would
  freeze the 04:xx pre-market run) and whether `enforce` should keep meaning measure-only.
- Sizing-policy source of truth for the cockpit (`DT10_POLICY_REF`): panel 2/30/20 vs seed
  2/30/15 vs built-in 3 vs the 2026-09-10 decision 3/40 vs the PDF programme.
- Engine version number 5.152.0 for WP-3ab, and the (b) trigger scope (parity vs narrow).
- Whether the News and Sector gates stay "not evaluable" (no wired input) or get wired.
- `scripts/intraday_quote_refresh.py` page list and the `_T10_Candidates` tab; the USX
  "Subunit Of USD" row in `LC_FX_Table`.
- Raising the sync-logs artifact retention from 7 to 30 days so the JSONL sink can be
  trended.

## New findings surfaced during the wave (not acted on)

- Signal_History `Analyst Rating` holds the literal string `None` on 15,072 of 29,471
  rows (`_safe_str(None)`); a value change, so it needs its own gate.
- `docs/reviews/decision-publication-update-20261010.md` still instructs installing
  16_Decision_Top10 v1.13.3; v1.13.4 supersedes it.
- The 2026-10-11 export (run 38095726629) that the framework measured is still needed to
  reconcile its §2.5 figures line by line; the committed baseline is the 2026-10-10 export.

## Next

Wave 2 builds WP-2 (b)(c): `TFB_SETTLE_UNRESOLVED` (engine 5.153.0, with the
`_portfolio_decision` HOLD guard, the opportunity-builder settlement regex and the
per-request ranker's withheld-rank rule) and `TFB_RANK_OVERALL_PAGE` (sync 6.66.0, pure
core at the publish boundary, DQ-41), on this published head.
