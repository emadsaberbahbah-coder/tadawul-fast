# Audit framework waves 1 and 2 — integration record and arming plan (2026-10-11)

Source: `TFB_Workbook_Audit_Framework_and_Findings_v1.0_2026-10-11.pdf` (owner's
check catalogue DQ-01..DQ-75 and work packages WP-0..WP-12), measured against the
2026-10-10 workbook export (run 38071484709). Base commit of the wave:
`a6668c2` on `claude/pensive-hypatia-smnxqe`; published head after wave 1: `741ab66`;
after wave 2 (WP-2 b/c, section below): `454e6e2`.

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

## Gates on the published head (`741ab66`, wave 1)

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

## Wave 2 — WP-2 (b)(c), published on top of wave 1

| WP | File(s) and version | Gate (default off) | Observe read-back | Closes / measures |
|---|---|---|---|---|
| WP-2b | `core/data_engine_v2.py` 5.153.0, `core/analysis/opportunity_builder.py` 1.25.1, `routes/analysis_sheet_rows.py` 4.8.2, `tests/test_de_f7_settle_unresolved.py`, `tests/test_ob_settle_unresolved.py`, `tests/test_analysis_sheet_rows_rank_withheld.py` | Render env `TFB_SETTLE_UNRESOLVED` off/observe/enforce (effective only while `TFB_SCORING_SETTLE` is observe or enforce) | each non-converged row (46 on 2026-10-10) gains one `settle_unresolved:observe:<reason>:p<n>:ov=..:rc=..` part; every value unchanged; `/health engine_gates.settle_unresolved`; boot `[v5.153.0 GUARDS] … settle_unresolved=…` | DQ-37 posture: under enforce the 46 rows publish their pass-1 scores as WATCHLIST, never funded (HOLD guard in `_portfolio_decision`), never ranked (engine, per-request route and page rank all withhold them), and the opportunity builder still fails them MAJOR on Scoring Settlement |
| WP-2c | `scripts/run_dashboard_sync.py` 6.66.0, `daily_sync.yml` (both market jobs), `page_refresh_recovery.yml` (recover-page), `tests/test_sync_rank_overall_page.py`, `scripts/harness_wp2bc.py` | repo Variable `TFB_RANK_OVERALL_PAGE` off/observe/enforce | per page per write one `[RANK-PAGE v6.66.0] page=… mode=observe …` line and one `_Run_Log` row (Status OBSERVE); expected `distinct_before` 25 / 25 / 22 / 14 and `changed` about 252 / 6,167 / 91 / 105 | DQ-41: Rank (Overall) becomes one rank over the whole page instead of a rank inside each 25-row batch; on the 2026-10-10 rows enforce gives distinct ranks = ranked rows (253 / 6,168 / 92 / 106) and WP-0 DQ-41 PASS, count 0 |

Integrator-owned changes in the same landing: `scripts/verify_deployment.py` 1.0.49 (pins engine
5.153.0, sync 6.66.0, opportunity builder 1.25.1; registers `TFB_SCORING_SETTLE`,
`TFB_SETTLE_UNRESOLVED` and `TFB_RANK_OVERALL_PAGE` as value flags; `TFB_SCORING_SETTLE` reads
ARMED on observe or enforce because observe already withholds rows, the other two only on enforce;
the rank gate is workflow-scoped), `.github/workflows/ci.yml` 1.1.6, and
`scripts/tfb_workbook_dq_audit.py` 1.0.1.

### Wave-2 integration findings

- **CI job environments.** The new route test imports `routes/`, which needs FastAPI. The lean CI
  job installs numpy and pytest only, so listing the test there would have aborted the whole lean
  pytest step on its first collection error. The test runs in the contract job instead. The gate
  that missed it ran the lean suites in an environment that had FastAPI; the integration gate now
  replays every `ci.yml` job in an environment built from that job's own install step, and the
  replay of the already-published wave-1 head is green.
- **Recovery job mapping.** `page_refresh_recovery.yml` (job `recover-page`) also runs the sync with
  batch size 25. It now maps `TFB_RANK_OVERALL_PAGE` from the same Variable; without it a recovery
  write after arming would republish within-batch ranks. `test_y4` pins the mapping.
- **DQ-37 receipt.** Under `TFB_SETTLE_UNRESOLVED=enforce` the engine no longer writes the
  `f7_settle_failed` receipt that WP-0 v1.0.0 counted, so DQ-37 would have read zero non-converged
  rows while the 46 rows were still unresolved. WP-0 1.0.1 counts the tag prefixes
  `f7_settle_failed` or `settle_unresolved:`, never the bare substring, which the enforce-only
  margin receipt and the provider token `name_unresolved` also contain. An INVESTABLE row among
  them is listed first in the DQ-37 examples. The 2026-10-10 numbers are unchanged and the
  committed baseline stays valid; the nightly line now reads `[DQ-AUDIT v1.0.1]`.
- **Independent review of the integrator's own commits.** Three reviewers, each finding checked by
  two refuters, confirmed four defects in the integration layer, all fixed before publication:
  the verifier printed an explicit `TFB_SCORING_SETTLE=observe` as DEFAULT; it reported that flag
  unarmed on observe although observe already withholds rows; the DQ-37 INVESTABLE example shared
  a six-slot cap that the rc-flip examples fill first on a real export; and two evidence sentences
  were wrong. A re-review then checked the fixes.
- **Test-order independence.** Two older problems surfaced when the lean list and the whole tree
  ran in reverse: three suites imported a shared helper by a bare name that only resolved after an
  earlier file had changed `sys.path`, and the Yahoo loop-guard harness left its private metric
  collectors registered. `tests/conftest.py` now puts `tests/` on the path and the harness releases
  its collectors. Activating the P-143 seam contract under pytest (it had run only as a script,
  so CI checked nothing) then showed the F-7 suites leaving the keep-last-good gate ON and stored
  12-month targets behind; both suites now restore them. The tree passes forward, reversed and in a
  fixed-seed shuffle, and the integration gate runs all three plus the per-job CI replay.
- **Reviewer note, not a defect.** Under rank enforce the `_Status` stamp's `payload_sha8` is computed
  before `write_table` applies the page rank, as it already is for the OHLC fill guard; it fingerprints
  the projected rows, not the written cells.

### Wave-2 arming (extends phases B and C above)

1. Phase B: repo Variable `TFB_RANK_OVERALL_PAGE = observe`. Read back the four `[RANK-PAGE v6.66.0]`
   lines and `_Run_Log` rows per write.
2. Phase B: Render `TFB_SETTLE_UNRESOLVED = observe` (production already runs
   `TFB_SCORING_SETTLE=observe`). Read back exactly one `settle_unresolved:observe:` part on each
   non-converged row, 0 of them INVESTABLE, every value unchanged; DQ-37 non-converged count unchanged.
3. Phase C: `TFB_RANK_OVERALL_PAGE = enforce` first. Read back WP-0 DQ-41 PASS, count 0; Rank 1 on the
   page tops (2223.SR / 0939.HK / ZL=F / JPXN.US on the 2026-10-10 rows, ± drift).
4. Phase C: record `/health` `TFB_OPP_INVESTABILITY_GATE` in the read-back, then
   `TFB_SETTLE_UNRESOLVED = enforce`. Read back `f7_settle_failed:non_converged` 46 → 0,
   `settle_unresolved:enforce:` 0 → 46, blank Overall Score on them 46 → 0, Rank (Overall) still blank
   on all of them, BLOCKED among them ≤ 3 (decided by the base gate chain), and DQ-37 non-converged
   still 46.
5. Out of scope here: `TFB_SCORING_SETTLE=enforce` (WP-12 #1). `TFB_SCORING_SETTLE_MAX_PASSES` must stay
   at 4 or more.

### Gates on the published head after wave 2 (`454e6e2`)

| Gate | Result |
|---|---|
| compileall, ruff E9/F63/F7/F82 | OK |
| pytest, lean CI pins (Python 3.11) | 3335 passed, 15 skipped |
| pytest, full requirements + openpyxl, forward / reversed / fixed-seed shuffle | 3424 passed, 9 skipped in each order |
| CI replay: every `ci.yml` job in an environment built from its own install step | green (lean 2662 + 99 + 14 passed, contract 225, heavy suites, node suites) |
| manifest pins, workflow audit, verifier self-tests | pass (workflow audit warnings=2, pre-existing) |

## Next

- Arm in the order above, one lever per evidence run, and regenerate the WP-0 baseline after each
  arming that legitimately moves counts.
- Open work packages from the framework not built in these waves: WP-1 (portfolio page), WP-4/5
  (universe hygiene), WP-9/10/11, WP-12 (the enforce sittings themselves), plus the deferred WP-7
  follow-ups (Signal_History write policy v6.45.0, Performance_Log Target Hit, PIT numeric storage).
- The 2026-10-11 export the framework measured is still needed to reconcile its figures line by line.
